import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from updater.dataset import add_record, blank_stats, file_hash, records, verify
from updater.migrate import migrate


class MigrationTests(unittest.TestCase):
    def test_fields_body_variants_and_precise_timestamps_survive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / ".state"
            state.mkdir()
            text = "A headline\nTEST · Wire · Oct 3, 2026\nArticle ID: new\n----\nThe whole original release."
            pub = "2026-10-03T20:00:00Z"
            clock = {"receipt_utc": "2026-10-03T20:00:01.123456789Z", "receipt_unix_ns": 1791057601123456789, "receipt_monotonic_ns": 123456789}
            reference = {"article_url": "https://rtpr.io/a/new", "article_published_at": pub, "ticker": "TEST", "arbitrary": [1, 2]}
            article = {"kind": "rtpr_historical_article", "article_id": "/a/new", "article_published_at": pub,
                       "reference": reference, "text": text, "body_format": "text", **clock,
                       "body_sha256": hashlib.sha256(text.encode()).hexdigest(), "body_characters": len(text)}
            md = "# " + text
            body = {**article, "kind": "article_body", "text": md, "body_sha256": hashlib.sha256(md.encode()).hexdigest(),
                    "body_characters": len(md), "body_format": "md", "alert_receipt": clock, "future_metadata": {"a": 7}}
            alert = {"kind": "rtpr_frame", **clock, "message": {**reference, "type": "alert", "dispatched_at_ms": 42}}
            page = {"kind": "rtpr_historical_feed_page", **clock, "page": {"articles": [reference]}}
            files, inputs = {}, {}
            for relative, rows in {"articles/2026-10-03.jsonl.gz": [article], "events/2026-10-03/news.jsonl.gz": [alert, body],
                                   "metadata/feed_pages/2026-10-03.jsonl.gz": [page]}.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                stats = blank_stats()
                with gzip.open(path, "wb") as handle:
                    for row in rows:
                        line = json.dumps(row).encode()
                        handle.write(line + b"\n")
                        add_record(stats, line, row)
                files[relative] = {**stats, "sha256": file_hash(path), "bytes": path.stat().st_size}
                inputs[relative] = {"stats": stats, "sha256": file_hash(path), "bytes": path.stat().st_size}
            (root / "manifest.json").write_text(json.dumps({"schema_version": 1, "files": files}))
            (state / "inputs.json").write_text(json.dumps(inputs))
            result = migrate(root, min_free_gib=0)
            self.assertEqual(result["articles"], 1)
            self.assertEqual(result["article_versions"], 2)
            articles = [row for _, row in records(root / "articles/2026-10-03.jsonl.gz")]
            self.assertEqual({row["text"] for row in articles}, {text, md})
            self.assertEqual(articles[0]["reference"], reference)
            events = [row for _, row in records(root / "events/2026-10-03.jsonl.gz")]
            self.assertTrue(all("text" not in row for row in events))
            self.assertEqual(events[0]["message"], alert["message"])
            self.assertEqual(events[1]["future_metadata"], {"a": 7})
            self.assertEqual(events[1]["receipt_unix_ns"], clock["receipt_unix_ns"])
            self.assertEqual(events[0]["title"], "A headline")
            self.assertEqual(verify(root)["missing_articles"], 0)
            self.assertTrue(migrate(root)["already_migrated"])


if __name__ == "__main__":
    unittest.main()
