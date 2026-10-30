"""Resumable RTPR delta fetcher. Standard library; no SSH, repo, or SQLite."""
from __future__ import annotations

import datetime as dt
import email.utils
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib

from .article_metadata import article_id, enrich_article
from .dataset import atomic_json, durable_file, sync_directory, utc_now


def stamp():
    wall = time.time_ns()
    utc = dt.datetime.fromtimestamp(wall // 10**9, dt.timezone.utc)
    return {"receipt_utc": utc.strftime("%Y-%m-%dT%H:%M:%S") + f".{wall % 10**9:09d}Z",
            "receipt_unix_ns": wall, "receipt_monotonic_ns": time.monotonic_ns()}


def published(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("RTPR publication timestamps require a timezone")
    return parsed.astimezone(dt.timezone.utc)


def reference_key(reference):
    return article_id(reference["article_url"]) + "\n" + reference["article_published_at"]


def read_key(path=None):
    if path is not None:
        content = Path(path).expanduser().read_text().strip()
        if content.startswith("{"):
            value = json.loads(content)
            content = value.get("api_key")
    else:
        content = os.environ.get("RTPR_API_KEY", "").strip()
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Set RTPR_API_KEY or supply --api-key-file with a private JSON profile containing api_key")
    return content.strip()


def retry_delay(value, default=60):
    try:
        return max(1, float(value))
    except (ValueError, TypeError):
        try:
            return max(1, email.utils.parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return default


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Paused(Exception):
    pass


class RequestFailure(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__("RTPR returned HTTP " + str(status))


class DirectUpdater:
    def __init__(self, dataset, key, *, interval=1.1, overlap_hours=24, since=None,
                 min_free_gib=1, max_requests=None, opener=None):
        self.dataset, self.key = dataset, key
        self.interval, self.overlap_hours, self.since = interval, overlap_hours, since
        self.min_free_gib, self.max_requests = min_free_gib, max_requests
        self.state_path = dataset.state_dir / "api.json"
        self.pending = dataset.state_dir / "api_pending"
        self.pending.mkdir(mode=0o700, exist_ok=True)
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.state.setdefault("todo", {})
        self.state.setdefault("failures", {})
        self.state.setdefault("cooldown_until", 0)
        self.state.setdefault("learned_interval", interval)
        self.opener = opener or urllib.request.build_opener(NoRedirect())
        self.requests = 0
        self.last_request = 0
        self.last_flush = time.monotonic()
        self.known = {(r["article_id"], r["article_published_at"]) for r in dataset.index.values()}
        self.initial_count = len(self.known)
        self.fetched = 0

    def save(self):
        atomic_json(self.state_path, self.state)

    def guard(self):
        if shutil.disk_usage(self.dataset.root).free < self.min_free_gib * 1024**3:
            raise Paused("free_space_reserve")
        if self.max_requests is not None and self.requests >= self.max_requests:
            raise Paused("request_budget")

    def clean(self, value):
        """Keep arbitrary provider fields; redact only authentication material."""
        if isinstance(value, dict):
            return {k: ("[REDACTED]" if k.lower() in ("api_key", "apikey", "authorization", "x-api-key", "set-cookie") else self.clean(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [self.clean(v) for v in value]
        if isinstance(value, str):
            value = value.replace(self.key, "[REDACTED]")
            if value.startswith("https://rtpr.io/a/"):
                url = urllib.parse.urlsplit(value)
                pairs = [(k, "[REDACTED]" if k.lower() in ("sig", "apikey", "api_key") else v)
                         for k, v in urllib.parse.parse_qsl(url.query, keep_blank_values=True)]
                return urllib.parse.urlunsplit((url.scheme, url.netloc, url.path, urllib.parse.urlencode(pairs), url.fragment))
        return value

    def request(self, url, feed=False):
        failures = 0
        while True:
            self.guard()
            delay = max(self.state["cooldown_until"] - time.time(),
                        max(self.interval, self.state["learned_interval"]) - (time.monotonic() - self.last_request), 0)
            if delay:
                time.sleep(delay)
            headers = {"Accept-Encoding": "identity", "User-Agent": "RTPR-portable-delta/2.0"}
            headers.update({"Authorization": "Bearer " + self.key} if feed else {"X-API-Key": self.key})
            started = stamp()
            self.last_request = time.monotonic()
            self.requests += 1
            try:
                try:
                    response = self.opener.open(urllib.request.Request(url, headers=headers), timeout=45)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    body = response.read()
                    received = stamp()
                    status = response.code
                    raw_headers = [[k, "[REDACTED]" if k.lower() in ("set-cookie", "authorization", "x-api-key") else self.clean(v)] for k, v in response.headers.items()]
                    content_type = response.headers.get("Content-Type", "")
                    encoding = response.headers.get("Content-Encoding", "identity").lower()
                    retry = response.headers.get("Retry-After")
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
                failures += 1
                if failures >= 8:
                    raise ValueError("Repeated RTPR network errors; checkpoint retained") from None
                self.state["cooldown_until"] = time.time() + min(300, 2**failures)
                self.save()
                continue
            self.state["last_http_status"] = status
            if status in (401, 403):
                self.save()
                raise ValueError("RTPR authentication or subscription rejected (HTTP " + str(status) + ")")
            if status == 429 or status >= 500:
                failures += 1
                if status == 429:
                    self.state["learned_interval"] = min(600, max(self.interval, self.state["learned_interval"]) * 2)
                self.state["cooldown_until"] = time.time() + retry_delay(retry, min(900, 60 * 2**(failures-1)))
                self.save()
                if failures >= 8:
                    raise ValueError("Repeated RTPR throttling/server errors; cooldown and checkpoint retained")
                continue
            if status != 200:
                raise RequestFailure(status)
            if encoding == "gzip":
                body = gzip.decompress(body)
            elif encoding == "deflate":
                body = zlib.decompress(body)
            elif encoding not in ("identity", ""):
                raise ValueError("Unsupported RTPR content encoding")
            return body, {"request_started": started, **received, "http_status": status,
                          "content_type": content_type, "response_headers": raw_headers,
                          "capture_transport": "historical_rest", "capture_host": socket.gethostname()}

    def stage(self, rows):
        token = str(time.time_ns()) + "-" + uuid.uuid4().hex[:12]
        target = self.pending / (token + ".json.gz")
        temporary = target.with_suffix(".tmp")
        body = json.dumps({"records": rows}, ensure_ascii=False, separators=(",", ":")).encode()
        temporary.write_bytes(gzip.compress(body, mtime=0))
        durable_file(temporary)
        os.replace(temporary, target)
        sync_directory(self.pending)

    def queue_page(self, page):
        for reference in page["articles"]:
            key = reference_key(reference)
            published(reference["article_published_at"])
            pair = (article_id(reference["article_url"]), reference["article_published_at"])
            if pair not in self.known:
                self.state["todo"].setdefault(key, reference)

    def flush(self):
        files = sorted(self.pending.glob("*.json.gz"))
        if not files:
            return
        # Reconstruct queued references if a crash occurred between staging a page
        # and checkpointing its cursor. No captured page can silently lose bodies.
        for path in files:
            with gzip.open(path, "rt") as handle:
                bundle = json.load(handle)
            for item in bundle["records"]:
                row = item["row"]
                if row["kind"] == "rtpr_historical_feed_page":
                    self.queue_page(row["page"])
            self.dataset.ingest_bundle(path, "api/" + path.name)
        self.dataset.commit()
        self.known = {(r["article_id"], r["article_published_at"]) for r in self.dataset.index.values()}
        self.state["todo"] = {k: r for k, r in self.state["todo"].items()
                              if (article_id(r["article_url"]), r["article_published_at"]) not in self.known}
        self.state["failures"] = {k: v for k, v in self.state["failures"].items() if k in self.state["todo"]}
        self.save()
        for path in files:
            path.unlink()
        sync_directory(self.pending)
        self.last_flush = time.monotonic()

    def start_run(self):
        if self.state.get("phase") == "index":
            return  # Resume an incomplete index without losing its cursor.
        latest = max((r["article_published_at"] for r in self.dataset.index.values()), default=None)
        if self.since:
            cutoff = published(self.since + "T00:00:00Z" if len(self.since) == 10 else self.since)
        elif latest:
            cutoff = published(latest) - dt.timedelta(hours=self.overlap_hours)
        else:
            raise ValueError("An empty dataset requires --since YYYY-MM-DD to set the initial backfill boundary")
        self.state.update(phase="index", started=stamp(), cursor=None, seen_cursors=[],
                          cutoff=cutoff.isoformat(), pages=0, latest_publication_seen=None)
        self.save()

    def fetch_index(self):
        cursor_reset = False
        while self.state["phase"] == "index":
            parameters = {"limit": 500}
            if self.state.get("cursor"):
                parameters["next_token"] = self.state["cursor"]
            try:
                body, response = self.request("https://api.rtpr.io/feed/articles?" + urllib.parse.urlencode(parameters), feed=True)
            except RequestFailure as exc:
                if exc.status == 400 and self.state.get("cursor") and not cursor_reset:
                    # An expired cursor can be replayed from the head. Keep all
                    # already queued references and committed body identities.
                    cursor_reset = True
                    self.state.update(cursor=None, seen_cursors=[])
                    self.save()
                    continue
                raise ValueError("RTPR feed request failed with HTTP " + str(exc.status)) from None
            page = self.clean(json.loads(body.decode("utf-8")))
            if not isinstance(page.get("articles"), list) or "next_token" not in page:
                raise ValueError("Unexpected RTPR feed response; checkpoint preserved")
            cursor = page["next_token"]
            if cursor is not None and (not isinstance(cursor, str) or not cursor or cursor in self.state["seen_cursors"]):
                raise ValueError("RTPR feed cursor did not advance")
            dates = []
            for reference in page["articles"]:
                reference_key(reference)
                dates.append(published(reference["article_published_at"]))
            record = {"kind": "rtpr_historical_feed_page", **response, "page": page}
            self.stage([{"stream": "history", "row": record}])
            self.queue_page(page)
            self.state.update(cursor=cursor, pages=self.state["pages"] + 1)
            if cursor:
                self.state["seen_cursors"].append(cursor)
            if dates:
                latest = max(dates).isoformat()
                self.state["latest_publication_seen"] = max(self.state["latest_publication_seen"] or latest, latest)
            if cursor is None or (dates and max(dates) < published(self.state["cutoff"])):
                self.state["phase"] = "bodies"
            self.save()
            if self.state["pages"] % 10 == 0:
                print(json.dumps({"phase": "index", "pages": self.state["pages"], "pending_articles": len(self.state["todo"])}), flush=True)

    def fetch_bodies(self):
        for key, reference in list(self.state["todo"].items()):
            identity = article_id(reference["article_url"])
            if (identity, reference["article_published_at"]) in self.known:
                self.state["todo"].pop(key, None)
                continue
            try:
                body, response = self.request("https://rtpr.io" + identity + "?format=text")
            except RequestFailure as exc:
                self.state["failures"][key] = {"http_status": exc.status, "checked_utc": utc_now()}
                self.save()
                continue
            if "text/plain" not in response["content_type"] or not body:
                raise ValueError("RTPR returned an unexpected or empty article body")
            text = body.decode("utf-8").replace(self.key, "[REDACTED]")
            article = enrich_article({"kind": "rtpr_article", **response,
                                      "fetch_started": response["request_started"], "article_id": identity,
                                      "article_published_at": reference["article_published_at"], "reference": reference,
                                      "body_format": "text", "text": text, "body_sha256": hashlib.sha256(text.encode()).hexdigest(),
                                      "body_characters": len(text), "historical_version_guaranteed": False})
            event = {k: v for k, v in article.items() if k != "text"}
            event.update(kind="article_fetched", title_available_at=response["receipt_utc"])
            self.stage([{"stream": "history", "row": article}, {"stream": "capture", "row": event}])
            self.known.add((identity, reference["article_published_at"]))
            self.state["todo"].pop(key, None)
            self.state["failures"].pop(key, None)
            self.fetched += 1
            self.save()
            if self.fetched % 50 == 0 or time.monotonic() - self.last_flush >= 60:
                self.flush()
                print(json.dumps({"phase": "bodies", "fetched": self.fetched, "pending_articles": len(self.state["todo"])}), flush=True)

    def run(self):
        status, reason = "complete", None
        try:
            self.flush()
            self.start_run()
            self.fetch_index()
            self.fetch_bodies()
        except Paused as exc:
            status, reason = "paused", str(exc)
        finally:
            self.flush()
            self.save()
        if status == "complete":
            status = "incomplete" if self.state["todo"] else "complete"
            self.state.update(phase=status, finished=stamp())
            self.save()
        return {"status": status, "reason": reason, "destination": str(self.dataset.root),
                "requests": self.requests, "new_articles": len(self.known) - self.initial_count,
                "pending_articles": len(self.state["todo"]), "articles": self.dataset.manifest.get("articles", 0),
                "article_versions": self.dataset.manifest.get("article_versions", 0),
                "latest_publication_seen": self.state.get("latest_publication_seen"),
                "effective_request_interval": max(self.interval, self.state["learned_interval"])}
