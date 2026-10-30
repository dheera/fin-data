import copy
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alphavantage_estimates import COLUMNS, parse_estimates


SHA = "a" * 64
COLLECTED_AT = "2026-10-20T19:30:45.123456789-07:00"


def response(**metrics):
    return {
        "symbol": "TSM",
        "estimates": [{"date": "2027-12-31", "horizon": "fiscal year", **metrics}],
    }


def parse(payload, **kwargs):
    return parse_estimates(payload, kwargs.get("symbol", "TSM"),
                           kwargs.get("retrieved_at", COLLECTED_AT),
                           kwargs.get("raw_sha256", SHA))


class ParseEstimatesTests(unittest.TestCase):
    def test_fiscal_dates_are_targets_and_lookbacks_remain_metrics(self):
        payload = response(eps_estimate_average="21.9251", eps_estimate_average_90_days_ago="19.6630")
        payload["estimates"].append({"date": "2024-12-31", "horizon": "fiscal quarter",
                                     "revenue_estimate_average": "855653116680.00"})
        frame = parse(payload)
        self.assertEqual(frame.columns.tolist(), list(COLUMNS))
        self.assertEqual(set(frame.collected_at), {pd.Timestamp("2026-10-21T02:30:45.123456789Z")})
        self.assertEqual(set(frame.fiscal_period_end), {pd.Timestamp("2024-12-31", tz="UTC"),
                                                       pd.Timestamp("2027-12-31", tz="UTC")})
        self.assertEqual(set(frame.metric), {"eps_estimate_average", "eps_estimate_average_90_days_ago",
                                             "revenue_estimate_average"})
        self.assertEqual(frame.loc[frame.metric.eq("revenue_estimate_average"), "value"].item(),
                         855653116680.0)
        self.assertEqual(str(frame.collected_at.dtype), "datetime64[ns, UTC]")
        self.assertEqual(str(frame.fiscal_period_end.dtype), "datetime64[ns, UTC]")
        self.assertEqual(str(frame.value.dtype), "float64")
        self.assertEqual(set(frame.raw_sha256), {SHA})

    def test_nulls_are_absent_but_explicit_zero_and_negative_eps_survive(self):
        frame = parse(response(eps_estimate_average="-1.2", eps_estimate_high=None,
                               eps_estimate_low="None", eps_estimate_analyst_count=" ",
                               eps_estimate_average_7_days_ago="-", eps_estimate_average_30_days_ago=" none ",
                               eps_estimate_revision_down_trailing_7_days="0.0000"))
        self.assertEqual(dict(zip(frame.metric, frame.value)), {
            "eps_estimate_average": -1.2, "eps_estimate_revision_down_trailing_7_days": 0.0,
        })

    def test_year_and_quarter_on_same_date_remain_distinct(self):
        payload = response(eps_estimate_average="1.5")
        payload["estimates"].append({"date": "2027-12-31", "horizon": "fiscal quarter",
                                     "eps_estimate_average": "0.5"})
        frame = parse(payload)
        self.assertEqual(dict(zip(frame.horizon, frame.value)), {"fiscal year": 1.5, "fiscal quarter": 0.5})

    def test_identical_and_conflicting_duplicate_records_rejected(self):
        for second_value in ("1.5", "4.0"):
            with self.subTest(second_value=second_value):
                payload = response(eps_estimate_average="1.5")
                duplicate = copy.deepcopy(payload["estimates"][0])
                duplicate["eps_estimate_average"] = second_value
                payload["estimates"].append(duplicate)
                with self.assertRaisesRegex(ValueError, "duplicate fiscal"):
                    parse(payload)

    def test_invalid_known_numeric_values_rejected(self):
        for value in ("NaN", "Infinity", float("nan"), float("inf"), "1e999", True,
                      [], {}, "1,200", "1_200", "null", "unexpected"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "eps_estimate_average"):
                parse(response(eps_estimate_average=value))

    def test_explicit_numeric_literals_and_exponents_supported(self):
        frame = parse(response(eps_estimate_average=2.5, eps_estimate_analyst_count=12,
                               revenue_estimate_average="+2.5e9"))
        self.assertEqual(dict(zip(frame.metric, frame.value)), {
            "eps_estimate_average": 2.5, "eps_estimate_analyst_count": 12.0,
            "revenue_estimate_average": 2_500_000_000.0,
        })

    def test_extension_fields_are_not_accidentally_numeric_series(self):
        frame = parse(response(eps_estimate_average="1.0", currency="USD", arbitrary=12,
                               eps_estimate_surprise_future="999", revenue_unknown="broken"))
        self.assertEqual(frame.metric.tolist(), ["eps_estimate_average"])

    def test_symbol_mismatch_or_missing_rejected(self):
        for symbol in ("IBM", "", None, 123):
            with self.subTest(symbol=symbol):
                payload = response(eps_estimate_average="1.0")
                payload["symbol"] = symbol
                with self.assertRaisesRegex(ValueError, "symbol does not match"):
                    parse(payload)
        self.assertEqual(parse(response(eps_estimate_average="1"), symbol=" tsm ").symbol.tolist(), ["TSM"])

    def test_provider_errors_are_clear_without_echoing_response(self):
        for field in ("Error Message", "Information", "Note"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, field) as caught:
                    parse({field: "sensitive request text must not be echoed"})
                self.assertNotIn("sensitive", str(caught.exception))

    def test_malformed_response_shapes_rejected(self):
        for payload in ([], None, {"symbol": "TSM"}, {"symbol": "TSM", "estimates": {}},
                        {"symbol": "TSM", "estimates": [None]}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                parse(payload)

    def test_malformed_targets_and_horizons_rejected(self):
        for date in ("2026-02-30", "2026-1-01", "2026-01-01T12:00:00Z", "NaT", None):
            with self.subTest(date=date):
                payload = response(eps_estimate_average="1")
                payload["estimates"][0]["date"] = date
                with self.assertRaisesRegex(ValueError, "date|target"):
                    parse(payload)
        for horizon in (None, "quarter", 1, []):
            with self.subTest(horizon=horizon):
                payload = response(eps_estimate_average="1")
                payload["estimates"][0]["horizon"] = horizon
                with self.assertRaisesRegex(ValueError, "horizon"):
                    parse(payload)

    def test_naive_or_invalid_collection_time_rejected(self):
        for collected in ("2026-10-20", "2026-10-20T19:30:00", "NaT", "broken", None):
            with self.subTest(collected=collected), self.assertRaisesRegex(ValueError, "timezone-aware"):
                parse(response(), retrieved_at=collected)

    def test_raw_digest_validated(self):
        for digest in ("", "x" * 64, "a" * 63, None):
            with self.subTest(digest=digest), self.assertRaisesRegex(ValueError, "SHA-256"):
                parse(response(), raw_sha256=digest)
        self.assertEqual(parse(response(eps_estimate_average=1), raw_sha256="A" * 64).raw_sha256.item(), SHA)

    def test_empty_response_and_all_missing_metrics_have_typed_schema(self):
        for payload in ({"symbol": "TSM", "estimates": []}, response(eps_estimate_average=None)):
            with self.subTest(payload=payload):
                frame = parse(payload)
                self.assertTrue(frame.empty)
                self.assertEqual(frame.columns.tolist(), list(COLUMNS))
                self.assertEqual(str(frame.collected_at.dtype), "datetime64[ns, UTC]")
                self.assertEqual(str(frame.fiscal_period_end.dtype), "datetime64[ns, UTC]")
                self.assertEqual(str(frame.value.dtype), "float64")


if __name__ == "__main__":
    unittest.main()
