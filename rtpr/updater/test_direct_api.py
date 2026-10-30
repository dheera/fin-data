import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from updater.article_metadata import enrich_article
from updater.dataset import Dataset, records, verify
from updater.direct_api import DirectUpdater


def ref(name, published="2026-10-03T20:00:00Z"):
    return {"article_url": "https://rtpr.io/a/" + name, "article_published_at": published, "ticker": "TEST"}


def text_body(name="new"):
    return ("Complete headline for " + name + "\nTEST (NASDAQ) · Test Wire · Oct 3, 2026\nArticle ID: " + name
            + "\n----------------\nThe full body, Unicode included: 完整新闻.\n")


class Response(io.BytesIO):
    def __init__(self, body, status=200, content_type="application/json", headers=None):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        super().__init__(body)
        self.code = status
        self.headers = {"Content-Type": content_type, "X-Test-Metadata": "retained", **(headers or {})}


class Opener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        if not self.responses:
            raise AssertionError("Unexpected extra request")
        return self.responses.pop(0)


class DirectApiTests(unittest.TestCase):
    def test_full_metadata_title_and_body_then_idempotent_delta(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = {**ref("new"), "future_metadata": {"languages": ["en", "zh"], "score": 17}}
            page = {"articles": [reference], "next_token": None, "provider_extra": {"edition": 5}}
            opener = Opener([Response(page), Response(text_body(), content_type="text/plain; charset=utf-8")])
            with mock.patch("updater.direct_api.time.sleep"), Dataset(root) as dataset:
                result = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0, opener=opener).run()
            self.assertEqual(result["new_articles"], 1)
            article = next(records(root / "articles/2026-10-03.jsonl.gz"))[1]
            event_path = next((root / "events").glob("*.jsonl.gz"))
            event = next(records(event_path))[1]
            self.assertEqual(article["reference"], reference)
            self.assertEqual(article["title"], "Complete headline for new")
            self.assertEqual(article["text"], text_body())
            self.assertIn(["X-Test-Metadata", "retained"], article["response_headers"])
            self.assertNotIn("text", event)
            self.assertEqual(event["article_version_id"], article["article_version_id"])
            self.assertEqual(verify(root)["articles"], 1)
            repeat = Opener([Response(page)])
            with mock.patch("updater.direct_api.time.sleep"), Dataset(root) as dataset:
                again = DirectUpdater(dataset, "fake-key", min_free_gib=0, opener=repeat).run()
            self.assertEqual(again["new_articles"], 0)
            self.assertEqual(len(repeat.requests), 1)
            self.assertEqual(verify(root)["article_versions"], 1)

    def test_pause_after_index_resumes_and_does_not_lose_body(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            page = {"articles": [ref("new")], "next_token": None}
            with mock.patch("updater.direct_api.time.sleep"), Dataset(root) as dataset:
                result = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0,
                                       max_requests=1, opener=Opener([Response(page)])).run()
            self.assertEqual(result["status"], "paused")
            self.assertEqual(result["pending_articles"], 1)
            with mock.patch("updater.direct_api.time.sleep"), Dataset(root) as dataset:
                result = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0,
                                       opener=Opener([Response(page), Response(text_body(), content_type="text/plain")])).run()
            self.assertEqual(result["status"], "complete")
            self.assertEqual(verify(root)["articles"], 1)

    def test_empty_page_with_cursor_is_followed(self):
        with tempfile.TemporaryDirectory() as directory:
            opener = Opener([Response({"articles": [], "next_token": "next"}),
                             Response({"articles": [ref("new")], "next_token": None}),
                             Response(text_body(), content_type="text/plain")])
            with mock.patch("updater.direct_api.time.sleep"), Dataset(directory) as dataset:
                result = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0, opener=opener).run()
            self.assertEqual(result["requests"], 3)
            self.assertIn("next_token=next", opener.requests[1].full_url)

    def test_rate_limit_cooldown_survives_restart_and_secrets_are_not_saved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            opener = Opener([Response({}, status=429, headers={"Retry-After": "30"})])
            with Dataset(root) as dataset:
                result = DirectUpdater(dataset, "sensitive-test-key", since="2026-10-03", min_free_gib=0,
                                       max_requests=1, opener=opener).run()
            state = json.loads((root / ".state/api.json").read_text())
            self.assertEqual(result["status"], "paused")
            self.assertGreater(state["learned_interval"], 1.1)
            self.assertGreater(state["cooldown_until"], 0)
            self.assertNotIn("sensitive-test-key", json.dumps(state))

    def test_pending_response_recovery_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reference = ref("new")
            body = text_body()
            row = enrich_article({"kind": "rtpr_article", "article_id": "/a/new", "article_published_at": reference["article_published_at"],
                                  "reference": reference, "text": body, "body_format": "text", "body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                                  "receipt_utc": "2026-10-03T20:00:01.123456789Z"})
            with Dataset(root) as dataset:
                updater = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0)
                updater.stage([{"row": row, "stream": "history"}])
                pending = next(updater.pending.glob("*.gz"))
                dataset.ingest_bundle(pending, "api/" + pending.name)
                dataset.commit()  # Simulate dying before the pending-file acknowledgement.
            with Dataset(root) as dataset:
                updater = DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0)
                updater.flush()
                self.assertEqual(len(dataset.index), 1)
                self.assertFalse(list(updater.pending.glob("*.gz")))
            self.assertEqual(verify(root)["article_versions"], 1)

    def test_second_writer_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory, Dataset(directory):
            with self.assertRaisesRegex(ValueError, "writer lock"):
                Dataset(directory)

    def test_expired_cursor_restarts_listing_without_dropping_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch("updater.direct_api.time.sleep"), Dataset(directory) as dataset:
                DirectUpdater(dataset, "fake-key", since="2026-10-03", min_free_gib=0, max_requests=1,
                              opener=Opener([Response({"articles": [], "next_token": "expired"})])).run()
            opener = Opener([Response({}, status=400), Response({"articles": [ref("new")], "next_token": None}),
                             Response(text_body(), content_type="text/plain")])
            with mock.patch("updater.direct_api.time.sleep"), Dataset(directory) as dataset:
                result = DirectUpdater(dataset, "fake-key", min_free_gib=0, opener=opener).run()
            self.assertEqual(result["status"], "complete")
            self.assertIn("next_token=expired", opener.requests[0].full_url)
            self.assertNotIn("next_token", opener.requests[1].full_url)
            self.assertEqual(verify(Path(directory))["articles"], 1)


if __name__ == "__main__":
    unittest.main()
