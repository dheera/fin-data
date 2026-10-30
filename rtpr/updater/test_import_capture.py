import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from updater.article_metadata import enrich_article
from updater.dataset import Dataset, records, verify
from updater.import_capture import import_capture, write_rows


class CaptureImportTests(unittest.TestCase):
    def test_cross_file_body_links_metadata_clocks_and_repeat(self):
        with tempfile.TemporaryDirectory() as directory:
            root, source = Path(directory) / "rtpr", Path(directory) / "capture"
            root.mkdir(); source.mkdir()
            clock = {"receipt_utc": "2026-10-03T21:59:59.123456789Z", "receipt_unix_ns": 1791064799123456789,
                     "receipt_monotonic_ns": 123456789}
            fetched = {"receipt_utc": "2026-10-03T22:00:00.123456789Z", "receipt_unix_ns": 1791064800123456789,
                       "receipt_monotonic_ns": 1123456789}
            reference = {"type": "alert", "article_url": "https://rtpr.io/a/new", "article_published_at": "2026-10-03T21:59:59Z",
                         "ticker": "TEST", "rules": [{"rule_name": "All articles"}], "extra": {"source": 7}}
            text = "# Exact headline\nTEST · Test Wire · October 3, 2026\nArticle ID: new\nAll the text, 包括中文."
            alert = {"kind": "rtpr_frame", **clock, "message": reference, "frame_type": "TEXT"}
            body = {"kind": "article_body", **fetched, "article_id": "/a/new", "article_published_at": reference["article_published_at"],
                    "text": text, "body_sha256": hashlib.sha256(text.encode()).hexdigest(), "alert_receipt": clock,
                    "capture_transport": "websocket", "extra_body_metadata": [1, {"a": "b"}]}
            write_rows(source / "hour1.jsonl.gz", [alert, {"kind": "kalshi_frame", **clock, "message": {"seq": 99}}])
            write_rows(source / "hour2.jsonl.gz", [body, {"kind": "jev_result", **fetched, "response": "example"}])
            first = import_capture(root, source, min_free_gib=0)
            self.assertEqual(first["new_capture_events"], 2)
            self.assertEqual(first["new_article_versions"], 1)
            article = next(records(root / "articles/2026-10-03.jsonl.gz"))[1]
            self.assertEqual(article["text"], text)
            self.assertEqual(article["reference"], reference)
            events = [row for _, row in records(root / "events/2026-10-03.jsonl.gz")]
            self.assertEqual(len(events), 2)
            self.assertEqual(events[0]["message"], reference)
            self.assertEqual(events[0]["receipt_unix_ns"], clock["receipt_unix_ns"])
            self.assertEqual(events[0]["title_available_at"], fetched["receipt_utc"])
            self.assertEqual(events[1]["extra_body_metadata"], body["extra_body_metadata"])
            self.assertTrue(all("text" not in row for row in events))
            self.assertTrue(all(row["article_version_id"] == article["article_version_id"] for row in events))
            self.assertFalse((root / "markets").exists())
            self.assertFalse((root / "models").exists())
            self.assertEqual(verify(root)["articles"], 1)
            again = import_capture(root, source, min_free_gib=0)
            self.assertEqual(again["new_capture_events"], 0)
            self.assertEqual(again["historical_fetch_events_added"], 0)
            self.assertEqual(len(list(records(root / "events/2026-10-03.jsonl.gz"))), 2)
            write_rows(source / "hour1.jsonl.gz", [alert, alert])
            with self.assertRaisesRegex(ValueError, "journal changed"):
                import_capture(root, source, min_free_gib=0)

    def test_historical_fetch_event_uses_saved_receipt_not_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root, source = Path(directory) / "rtpr", Path(directory) / "capture"
            root.mkdir(); source.mkdir()
            text = "Historic headline\nTEST · Wire · May 1, 2026\nArticle ID: old\nComplete article"
            article = enrich_article({"kind": "rtpr_historical_article", "article_id": "/a/old",
                                      "article_published_at": "2026-05-01T00:00:00Z", "receipt_utc": "2026-10-01T01:00:00Z",
                                      "receipt_unix_ns": 1790816400000000000, "capture_transport": "historical_rest",
                                      "body_format": "text", "text": text, "body_sha256": hashlib.sha256(text.encode()).hexdigest()})
            path = Path(directory) / "article.gz"
            write_rows(path, [article])
            with Dataset(root) as dataset:
                dataset.ingest(path, "original", "history"); dataset.commit()
            write_rows(source / "empty-news.jsonl.gz", [{"kind": "coinbase_heartbeat", "receipt_utc": "2026-10-03T00:00:00Z"}])
            result = import_capture(root, source, min_free_gib=0)
            self.assertEqual(result["historical_fetch_events_added"], 1)
            event = next(records(root / "events/2026-10-01.jsonl.gz"))[1]
            self.assertEqual(event["receipt_unix_ns"], article["receipt_unix_ns"])
            self.assertEqual(event["capture_transport"], "historical_rest")
            self.assertEqual(event["record_origin"], "saved_article_fetch_metadata")
            self.assertEqual(verify(root)["articles"], 1)


if __name__ == "__main__":
    unittest.main()
