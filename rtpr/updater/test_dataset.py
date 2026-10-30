import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from updater.dataset import Dataset, records, verify, partition
from updater.article_metadata import enrich_article


def article(identity="test", published="2026-10-03T00:00:00.123456789Z"):
    text = "# 完整新闻\n" + "Full release. " * 1000
    return enrich_article({"kind": "rtpr_historical_article", "article_id": "/a/" + identity,
            "article_published_at": published, "receipt_utc": "2026-10-03T00:01:01.987654321Z",
            "receipt_unix_ns": 1790985661987654321, "receipt_monotonic_ns": 123456789123456,
            "body_sha256": hashlib.sha256(text.encode()).hexdigest(), "body_characters": len(text),
            "body_format": "md", "text": text, "capture_transport": "websocket"})


def page(rows):
    return {"kind": "rtpr_historical_feed_page", "receipt_utc": "2026-10-03T00:01:01Z",
            "page": {"articles": [{"article_url": "https://rtpr.io" + row["article_id"],
                                    "article_published_at": row["article_published_at"]} for row in rows]}}


def source(root, name, rows):
    path = root / name
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


class DatasetTests(unittest.TestCase):
    def test_dates_exact_records_incremental_append_and_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = article("first", "2026-10-02T23:59:59.999999999Z")
            second = article("second")
            incoming = source(root, "one.gz", [first, second, page([first, second])])
            output = root / "dataset"
            with Dataset(output) as dataset:
                dataset.ingest(incoming, "history/one", "history")
                dataset.commit()
            third = article("third")
            topup = source(root, "two.gz", [third, page([third])])
            with Dataset(output) as dataset:
                self.assertFalse(dataset.ingest(incoming, "history/one", "history"))
                dataset.ingest(topup, "history/two", "history")
                dataset.commit()
            rows = [row for _, row in records(output / "articles/2026-10-03.jsonl.gz")]
            self.assertEqual(rows, [second, third])
            self.assertEqual(verify(output)["articles"], 3)
            self.assertEqual(verify(output)["article_days"], 2)

    def test_utc_partition_uses_timezone_but_does_not_modify_clock(self):
        row = {"kind": "rtpr_frame", "receipt_utc": "2026-10-03T00:15:00+02:00"}
        self.assertEqual(partition(row, "capture"), "events/2026-10-02.jsonl.gz")
        self.assertEqual(row["receipt_utc"], "2026-10-03T00:15:00+02:00")

    def test_bad_body_or_duplicate_aborts_without_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bad = {**article(), "text": "truncated"}
            incoming = source(root, "bad.gz", [bad])
            with self.assertRaisesRegex(ValueError, "checksum"):
                with Dataset(root / "dataset") as dataset:
                    dataset.ingest(incoming, "bad", "history")
                    dataset.commit()
            self.assertFalse((root / "dataset/manifest.json").exists())
            duplicate = source(root, "duplicate.gz", [article(), article()])
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                with Dataset(root / "dataset") as dataset:
                    dataset.ingest(duplicate, "duplicate", "history")
                    dataset.commit()
            self.assertFalse((root / "dataset/manifest.json").exists())

    def test_changed_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = source(root, "input.gz", [article(), page([article()])])
            with Dataset(root / "dataset") as dataset:
                dataset.ingest(incoming, "history/same", "history")
                dataset.commit()
            source(root, "input.gz", [article("changed")])
            with Dataset(root / "dataset") as dataset:
                with self.assertRaisesRegex(ValueError, "source changed"):
                    dataset.ingest(incoming, "history/same", "history")

    def test_interrupted_commit_recovers_without_double_counting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = source(root, "input.gz", [article(), page([article()])])
            with Dataset(root / "dataset") as dataset:
                dataset.ingest(incoming, "history/same", "history")
                with mock.patch.object(dataset, "recover", side_effect=RuntimeError("interrupted")):
                    with self.assertRaises(RuntimeError):
                        dataset.commit()
            with Dataset(root / "dataset") as dataset:
                self.assertFalse(dataset.ingest(incoming, "history/same", "history"))
                dataset.commit()
            self.assertEqual(verify(root / "dataset")["articles"], 1)

    def test_event_types_and_source_spans_preserve_original_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [{"kind": kind, "receipt_utc": "2026-10-03T01:00:00.123456789Z", "receipt_unix_ns": 42}
                    for kind in ["rtpr_frame", "jev_result", "kalshi_frame", "spot_ticker", "rtpr_frame"]]
            incoming = source(root, "capture.gz", rows)
            with Dataset(root / "dataset") as dataset:
                dataset.ingest(incoming, "capture/original-run", "capture")
                dataset.commit()
            ledger = json.loads((root / "dataset/.state/inputs.json").read_text())
            spans = ledger["capture/original-run"]["partitions"]
            self.assertEqual(spans["events/2026-10-03.jsonl.gz"], {"first_row": 1, "rows": 2})
            self.assertEqual(verify(root / "dataset")["records"], 5)


if __name__ == "__main__":
    unittest.main()
