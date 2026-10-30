import contextlib
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alphavantage_client import AccessError
from sources_alphavantage import collect


class FakeClient:
    """An already-cached, offline client; never loads credentials or sends I/O."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.ledger = {"days": {"2026-10-21": len(responses)}}

    def get(self, function, symbol):
        self.calls.append((function, symbol))
        result = self.responses[symbol]
        if isinstance(result, Exception):
            raise result
        return result


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.destination = self.root / "supplementary/alphavantage/earnings_estimates.parquet"

    def cached(self, symbol="TSM", collected_at="2026-10-21T02:00:00+00:00", value="21.5"):
        payload = {
            "symbol": symbol,
            "estimates": [{"date": "2027-12-31", "horizon": "fiscal year",
                           "eps_estimate_average": value,
                           "eps_estimate_average_90_days_ago": "19.5",
                           "eps_estimate_revision_down_trailing_7_days": None}],
        }
        raw = json.dumps(payload, sort_keys=True).encode()
        sha = hashlib.sha256(raw).hexdigest()
        path = self.root / "archive/raw" / f"{sha}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        receipt = {"sha256": sha, "retrieved_at": collected_at}
        return payload, receipt

    def run_collect(self, responses, symbols=None):
        client = FakeClient(responses)
        with contextlib.redirect_stdout(io.StringIO()):
            report = collect(root=self.root, symbols=list(responses) if symbols is None else symbols,
                             client=client)
        return report, client

    def test_same_cached_response_is_byte_idempotent(self):
        response = self.cached()
        first, _ = self.run_collect({"TSM": response})
        original = self.destination.read_bytes()
        second, client = self.run_collect({"TSM": response}, symbols=["TSM", "TSM"])
        self.assertEqual(self.destination.read_bytes(), original)
        self.assertEqual(first["parquet_sha256"], second["parquet_sha256"])
        self.assertEqual(second["total_rows"], 2)
        self.assertEqual(second["observation_days"], 1)
        self.assertEqual(second["succeeded"][0]["collected_at"], response[1]["retrieved_at"])
        self.assertEqual(client.calls, [("EARNINGS_ESTIMATES", "TSM")])
        frame = pd.read_parquet(self.destination)
        self.assertEqual(set(frame.raw_sha256), {response[1]["sha256"]})
        self.assertEqual(set(frame.collected_at), {pd.Timestamp(response[1]["retrieved_at"])})
        metadata = json.loads(pq.read_schema(self.destination).metadata[b"alt_data_supplementary"])
        self.assertFalse(metadata["public_catalog_member"])
        self.assertIn("unspecified", metadata["units"])
        self.assertEqual(second["public_catalog_additions"], 0)
        self.assertFalse(second["schedule_installed"])

    def test_all_calls_failing_preserve_existing_parquet_exactly(self):
        self.run_collect({"TSM": self.cached()})
        original = self.destination.read_bytes()
        report, client = self.run_collect({
            "TSM": AccessError("network_error; authenticated URL omitted"),
            "ASML": AccessError("network_error; authenticated URL omitted"),
        })
        self.assertEqual(self.destination.read_bytes(), original)
        self.assertEqual(report["succeeded"], [])
        self.assertEqual([f["symbol"] for f in report["failures"]], ["TSM", "ASML"])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(json.loads((self.root / "logs/alphavantage_latest.json").read_text()), report)

    def test_all_calls_failing_initially_create_no_parquet(self):
        report, _ = self.run_collect({"TSM": AccessError("network_error; authenticated URL omitted")})
        self.assertFalse(self.destination.exists())
        self.assertEqual(len(report["failures"]), 1)

    def test_later_collection_appends_observation_clock_and_raw_provenance(self):
        first = self.cached()
        self.run_collect({"TSM": first})
        second = self.cached(collected_at="2026-10-22T02:00:00+00:00", value="22.0")
        report, _ = self.run_collect({"TSM": second})
        frame = pd.read_parquet(self.destination)
        self.assertEqual(report["total_rows"], 4)
        self.assertEqual(report["total_symbols"], 1)
        self.assertEqual(report["observation_days"], 2)
        self.assertEqual(frame.fiscal_period_end.nunique(), 1)
        self.assertEqual(frame.collected_at.nunique(), 2)
        self.assertEqual(set(frame.raw_sha256), {first[1]["sha256"], second[1]["sha256"]})
        values = frame.loc[frame.metric.eq("eps_estimate_average")].sort_values("collected_at").value.tolist()
        self.assertEqual(values, [21.5, 22.0])

    def test_same_observation_key_conflicting_value_leaves_previous_file(self):
        self.run_collect({"TSM": self.cached()})
        original = self.destination.read_bytes()
        previous_report = (self.root / "logs/alphavantage_latest.json").read_bytes()
        with self.assertRaisesRegex(ValueError, "Conflicting values"):
            self.run_collect({"TSM": self.cached(value="22.0")})
        self.assertEqual(self.destination.read_bytes(), original)
        self.assertEqual((self.root / "logs/alphavantage_latest.json").read_bytes(), previous_report)

    def test_same_observation_key_conflicting_provenance_leaves_previous_file(self):
        self.run_collect({"TSM": self.cached()})
        original = self.destination.read_bytes()
        payload, receipt = self.cached()
        # An otherwise ignored provider field changes the original response body.
        # Keeping the first hash silently would discard this conflicting provenance.
        payload["provider_revision"] = "another response at the same timestamp"
        raw = json.dumps(payload, sort_keys=True).encode()
        receipt["sha256"] = hashlib.sha256(raw).hexdigest()
        (self.root / "archive/raw" / f"{receipt['sha256']}.bin").write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            self.run_collect({"TSM": (payload, receipt)})
        self.assertEqual(self.destination.read_bytes(), original)

    def test_invalid_estimate_response_cannot_replace_previous_data(self):
        self.run_collect({"TSM": self.cached()})
        original = self.destination.read_bytes()
        report, _ = self.run_collect({"TSM": self.cached(value="NaN")})
        self.assertEqual(self.destination.read_bytes(), original)
        self.assertEqual(report["succeeded"], [])
        self.assertIn("eps_estimate_average", report["failures"][0]["error"])

    def test_quota_response_stops_remaining_requests_without_losing_success(self):
        report, client = self.run_collect({
            "TSM": self.cached(),
            "ASML": AccessError("local_daily_quota_exhausted"),
            "BABA": self.cached(symbol="BABA"),
        })
        self.assertEqual(client.calls, [("EARNINGS_ESTIMATES", "TSM"), ("EARNINGS_ESTIMATES", "ASML")])
        self.assertEqual(report["total_rows"], 2)
        self.assertEqual([s["symbol"] for s in report["succeeded"]], ["TSM"])
        self.assertEqual([f["symbol"] for f in report["failures"]], ["ASML"])
        self.assertEqual(pd.read_parquet(self.destination).symbol.unique().tolist(), ["TSM"])


if __name__ == "__main__":
    unittest.main()
