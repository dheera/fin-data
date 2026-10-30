import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

import requests

from alphavantage_client import AccessError, Client


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.session = Mock()
        self.response = Mock(status_code=200, content=b'{"symbol":"TSM","estimates":[]}', headers={})
        self.session.get.return_value = self.response
        self.client = Client(self.root, key="PRIVATE_TEST_KEY", session=self.session,
                             now=lambda: datetime(2026, 10, 21, 1, tzinfo=timezone.utc), sleep=lambda _: None)

    def test_cached_response_preserves_clock_and_never_logs_secret(self):
        first, receipt = self.client.get("EARNINGS_ESTIMATES", "TSM")
        second, cached = self.client.get("EARNINGS_ESTIMATES", "TSM")
        self.assertEqual(first, second)
        self.assertEqual(receipt, cached)
        self.assertEqual(self.session.get.call_count, 1)
        self.assertEqual(self.client.ledger["days"]["2026-10-21"], 1)
        self.assertFalse(self.session.get.call_args.kwargs["allow_redirects"])
        for p in self.root.rglob("*"):
            if p.is_file():
                self.assertNotIn(b"PRIVATE_TEST_KEY", p.read_bytes())

    def test_quota_prevents_request(self):
        self.client.ledger["days"]["2026-10-21"] = 25
        with self.assertRaisesRegex(AccessError, "quota"):
            self.client.get("EARNINGS_ESTIMATES", "TSM")
        self.session.get.assert_not_called()

    def test_request_spacing_survives_a_new_client_process(self):
        self.client.get("EARNINGS_ESTIMATES", "TSM")
        pause = Mock()
        restarted = Client(self.root, key="PRIVATE_TEST_KEY", session=self.session,
                           now=lambda: datetime(2026, 10, 21, 1, 0, 5, tzinfo=timezone.utc),
                           sleep=pause)
        restarted.get("EARNINGS_ESTIMATES", "ASML")
        pause.assert_called_once()
        self.assertAlmostEqual(pause.call_args.args[0], 8.0)
        self.assertEqual(restarted.ledger["days"]["2026-10-21"], 2)
        self.assertEqual(self.session.get.call_count, 2)

    def test_network_error_consumes_quota_and_hides_url(self):
        self.session.get.side_effect = requests.RequestException("url?apikey=PRIVATE_TEST_KEY")
        with self.assertRaises(AccessError) as caught:
            self.client.get("EARNINGS_ESTIMATES", "TSM")
        self.assertNotIn("PRIVATE_TEST_KEY", str(caught.exception))
        self.assertEqual(self.client.ledger["days"]["2026-10-21"], 1)
        self.assertEqual(self.session.get.call_count, 1)

    def test_credential_echo_not_archived(self):
        self.response.content = b'{"Information":"PRIVATE_TEST_KEY invalid"}'
        with self.assertRaisesRegex(AccessError, "response_echoed"):
            self.client.get("EARNINGS_ESTIMATES", "TSM")
        self.assertFalse((self.root / "archive/raw").exists())

    def test_http_200_rate_message_not_treated_as_data_or_retried(self):
        self.response.content = b'{"Information":"API rate limit reached"}'
        for _ in range(2):
            with self.assertRaisesRegex(AccessError, "rate_limited"):
                self.client.get("EARNINGS_ESTIMATES", "TSM")
        self.assertEqual(self.session.get.call_count, 1)

    def test_initial_probe_accounted_and_reused(self):
        raw = self.response.content
        digest = hashlib.sha256(raw).hexdigest()
        (self.root / "archive/raw").mkdir(parents=True)
        (self.root / "archive/raw" / f"{digest}.bin").write_bytes(raw)
        (self.root / "logs").mkdir()
        receipt = dict(url="https://www.alphavantage.co/query", parameters={"function":"EARNINGS_ESTIMATES","symbol":"TSM"},
                       sha256=digest,retrieved_at="2026-10-21T00:00:00+00:00",status_code=200)
        (self.root / "logs/alphavantage_initial_probe.json").write_text(json.dumps(receipt))
        for _ in range(2):
            client = Client(self.root, key="PRIVATE_TEST_KEY", session=self.session,
                            now=lambda:datetime(2026, 10, 21, 1, tzinfo=timezone.utc))
            _, actual = client.get("EARNINGS_ESTIMATES", "TSM")
            self.assertEqual(actual["retrieved_at"], receipt["retrieved_at"])
            self.assertEqual(client.ledger["days"]["2026-10-21"], 1)
        self.session.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
