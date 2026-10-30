"""Local, lossless RTPR JSONL dataset, partitioned by UTC date.

Article bodies are stored once per content version; events reference them.
Imports are checkpointed by source checksum; a recoverable commit prevents a
retry from duplicating records after a process interruption.
"""
from __future__ import annotations

import collections
import datetime as dt
import errno
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import uuid

from .article_metadata import article_id, index_entry, version_id

DEFAULT_ROOT = Path(__file__).resolve().parents[1]
MODULUS = 1 << 256


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_relative(value):
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts or "\n" in value or "\r" in value:
        raise ValueError("Unsafe dataset path")
    return path


def utc_date(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("A timezone is required for date partitioning")
    return parsed.astimezone(dt.timezone.utc).date().isoformat()


def article_key(row):
    return version_id(row)


def partition(row, stream):
    if stream == "history":
        if row["kind"] in ("rtpr_historical_article", "rtpr_article"):
            return f"articles/{utc_date(row['article_published_at'])}.jsonl.gz"
        if row["kind"] == "rtpr_historical_feed_page":
            return f"metadata/feed_pages/{utc_date(row['receipt_utc'])}.jsonl.gz"
        raise ValueError("Unexpected historical record kind")
    if stream != "capture":
        raise ValueError("Unknown record stream")
    kind = row["kind"]
    if kind == "kalshi_frame":
        group = "raw"
    elif kind.startswith("kalshi_"):
        group = "prediction_markets"
    elif kind.startswith("jev_"):
        return f"models/{utc_date(row['receipt_utc'])}.jsonl.gz"
    elif kind.startswith(("rtpr_", "article_")):
        return f"events/{utc_date(row['receipt_utc'])}.jsonl.gz"
    elif kind.startswith(("spot_", "coinbase_", "pyth_")):
        group = "spot"
    else:
        return f"metadata/collector/{utc_date(row['receipt_utc'])}.jsonl.gz"
    return f"markets/{utc_date(row['receipt_utc'])}/{group}.jsonl.gz"


def blank_stats():
    return {"rows": 0, "record_sha256_sum": "0" * 64, "kinds": {}, "full_text_characters": 0}


def add_record(stats, line, row):
    stats["rows"] += 1
    digest = int.from_bytes(hashlib.sha256(line).digest(), "big")
    stats["record_sha256_sum"] = f"{(int(stats['record_sha256_sum'], 16) + digest) % MODULUS:064x}"
    kind = row["kind"]
    stats["kinds"][kind] = stats["kinds"].get(kind, 0) + 1
    stats["full_text_characters"] += len(row.get("text", ""))


def merge_stats(left, right):
    return {"rows": left["rows"] + right["rows"],
            "record_sha256_sum": f"{(int(left['record_sha256_sum'], 16) + int(right['record_sha256_sum'], 16)) % MODULUS:064x}",
            "kinds": dict(collections.Counter(left["kinds"]) + collections.Counter(right["kinds"])),
            "full_text_characters": left["full_text_characters"] + right["full_text_characters"]}


def records(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rb") as handle:
        for line in handle:
            line = line.rstrip(b"\r\n")
            if not line:
                raise ValueError("Empty line in a record file")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("Expected a JSON object")
            yield line, row


def check_body(row):
    if "text" in row and "body_sha256" in row:
        if hashlib.sha256(row["text"].encode("utf-8")).hexdigest() != row["body_sha256"]:
            raise ValueError("Full-text checksum mismatch")
        if "body_characters" in row and len(row["text"]) != row["body_characters"]:
            raise ValueError("Full-text length mismatch")


def restrict_mode(path, mode):
    """Tighten permissions when the filesystem allows it.

    Some network filesystems (for example NFS exports backed by Samba or with
    squashed ownership) reject changing the mode even for the owner. The mode
    is hardening only, so EPERM, ENOTSUP and EINVAL mean "not applicable here".
    """
    try:
        os.chmod(path, mode)
    except OSError as exc:
        if exc.errno not in (errno.EPERM, errno.ENOTSUP, errno.EINVAL):
            raise


def durable_file(path):
    restrict_mode(path, 0o600)
    with Path(path).open("rb") as handle:
        os.fsync(handle.fileno())


def check_state_matches(manifest, index):
    """The hidden .state/ directory is written in the same commit as manifest.json.

    After a copy that skipped hidden files (file managers hide .state/ by
    default) the data is intact but the index and checkpoints are older than
    the manifest. Say so, instead of failing later with a bare mismatch.
    """
    if len(index) != manifest.get("article_versions", 0):
        raise ValueError("The .state/ directory does not match manifest.json: article index has %d versions, manifest has %d. "
                         "Copy the hidden .state/ directory from the same source as the data (show hidden files in the file manager)."
                         % (len(index), manifest.get("article_versions", 0)))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    durable_file(path)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in (errno.EINVAL, errno.ENOTSUP):
                raise
    finally:
        os.close(fd)


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    write_json(temporary, value)
    os.replace(temporary, path)
    sync_directory(path.parent)


class WriterLock:
    """Atomic directory creation works across clients of a mounted share."""
    def __init__(self, state_dir):
        self.path = Path(state_dir) / "writer.lock"
        try:
            self.path.mkdir(mode=0o700)
        except FileExistsError:
            raise ValueError(self.held_message()) from None
        self.token = uuid.uuid4().hex
        atomic_json(self.path / "owner.json", {"hostname": socket.gethostname(), "pid": os.getpid(), "started_utc": utc_now(), "token": self.token})

    def held_message(self):
        owner_file = self.path / "owner.json"
        try:
            owner = json.loads(owner_file.read_text()) if owner_file.exists() else {}
        except (OSError, ValueError):
            owner = {}
        held_by = "held by pid {pid} on host {hostname} since {started_utc}".format(
            pid=owner.get("pid", "?"), hostname=owner.get("hostname", "?"), started_utc=owner.get("started_utc", "?"))
        if owner.get("hostname") not in (None, socket.gethostname()):
            held_by += " (not this machine; after copying a dataset from another machine this lock is stale)"
        return ("Dataset already has a writer lock: " + str(self.path) + ", " + held_by
                + ". Make sure that writer has stopped, then rerun with --recover-lock.")

    def close(self):
        owner = self.path / "owner.json"
        if owner.exists() and json.loads(owner.read_text()).get("token") == self.token:
            owner.unlink()
            self.path.rmdir()


def recover_lock(root):
    path = Path(root) / ".state/writer.lock"
    if not path.exists():
        return
    owner_file = path / "owner.json"
    owner = json.loads(owner_file.read_text()) if owner_file.exists() else {}
    if owner.get("hostname") == socket.gethostname():
        try:
            os.kill(owner["pid"], 0)
        except ProcessLookupError:
            pass
        else:
            raise ValueError("Refusing to remove a lock belonging to a running local process")
    # This option is an explicit operator assertion that a foreign writer is stopped.
    if owner_file.exists():
        owner_file.unlink()
    path.rmdir()


class Dataset:
    def __init__(self, root=DEFAULT_ROOT):
        self.root = Path(root).expanduser().resolve()
        self.state_dir = self.root / ".state"
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        restrict_mode(self.root, 0o700)
        self.lock = WriterLock(self.state_dir)
        self.stage = self.state_dir / "transaction"
        try:
            self.recover()
            self.manifest = self.read("manifest.json", {"schema_version": 2, "files": {}})
            if self.manifest["schema_version"] != 2:
                raise ValueError("This dataset needs --migrate before direct API updates")
        except BaseException:
            self.lock.close()
            raise
        self.inputs = self.read(".state/inputs.json", {})
        index_path = self.state_dir / "article_index.json.gz"
        if index_path.exists():
            with gzip.open(index_path, "rt") as handle:
                self.index = json.load(handle)
        else:
            self.index = {}
        try:
            check_state_matches(self.manifest, self.index)
        except ValueError:
            self.lock.close()
            raise
        self.stage.mkdir(mode=0o700)
        self.writers = {}
        self.delta = {}
        self.blobs = {}
        self.new_inputs = {}
        self.article_keys = set(self.index)

    def read(self, relative, default):
        path = self.root / relative
        return json.loads(path.read_text()) if path.exists() else default

    def recover(self):
        plan = self.stage / "commit.json"
        if plan.exists():
            for entry in json.loads(plan.read_text()):
                relative = safe_relative(entry["path"])
                source, dest = self.stage / "ready" / relative, self.root / relative
                candidate = source if source.exists() else dest
                if not candidate.is_file() or file_hash(candidate) != entry["sha256"]:
                    raise ValueError("Interrupted import has an invalid file: " + str(relative))
                if source.exists():
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(source, dest)
                    sync_directory(dest.parent)
        if self.stage.exists():
            shutil.rmtree(self.stage)

    def imported(self, source_id, digest):
        old = self.new_inputs.get(source_id, self.inputs.get(source_id))
        if old and old["sha256"] != digest:
            raise ValueError("Previously imported immutable source changed: " + source_id)
        return old is not None

    def load_article_keys(self):
        if not self.index and any(p.startswith("articles/") for p in self.manifest["files"]):
            self.article_keys = set()
            for rel in self.manifest["files"]:
                if rel.startswith("articles/"):
                    for _, row in records(self.root / rel):
                        key = article_key(row)
                        if key in self.article_keys:
                            raise ValueError("Duplicate article version in existing dataset")
                        self.article_keys.add(key)
                        self.index[key] = index_entry(row, rel)

    def ingest(self, path, source_id, stream, expected=None):
        path = Path(path)
        digest = file_hash(path)
        if expected and (digest != expected["sha256"] or path.stat().st_size != expected["bytes"]):
            raise ValueError("Source checksum mismatch: " + source_id)
        if self.imported(source_id, digest):
            return False
        source_stats = blank_stats()
        spans = {}
        for line, row in records(path):
            relative = partition(row, stream)
            check_body(row)
            if relative.startswith("articles/"):
                self.load_article_keys()
                key = article_key(row)
                if key in self.article_keys:
                    raise ValueError("Duplicate article version in incoming archive")
                self.article_keys.add(key)
                self.index[key] = index_entry(row, relative)
            if relative not in self.writers:
                target = self.stage / "delta" / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                self.writers[relative] = gzip.GzipFile(filename=str(target), mode="wb", compresslevel=6, mtime=0)
                self.delta[relative] = blank_stats()
            current_row = self.manifest["files"].get(relative, {}).get("rows", 0) + self.delta[relative]["rows"]
            spans.setdefault(relative, {"first_row": current_row + 1, "rows": 0})["rows"] += 1
            self.writers[relative].write(line + b"\n")
            add_record(self.delta[relative], line, row)
            add_record(source_stats, line, row)
        if expected and stream == "history" and source_id.startswith("history/articles/"):
            if source_stats["rows"] != expected["rows"]:
                raise ValueError("Source article count mismatch")
        if file_hash(path) != digest:
            raise ValueError("Source changed during import")
        self.new_inputs[source_id] = {"sha256": digest, "bytes": path.stat().st_size,
                                     "stream": stream, "stats": source_stats, "partitions": spans}
        return True

    def ingest_bundle(self, path, source_id):
        """Import a durable API response bundle, including text-free event rows."""
        path = Path(path)
        digest = file_hash(path)
        if self.imported(source_id, digest):
            return False
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            bundle = json.load(handle)
        combined, spans = blank_stats(), {}
        for item in bundle["records"]:
            row, stream = item["row"], item["stream"]
            relative = partition(row, stream)
            check_body(row)
            if relative.startswith("articles/"):
                key = article_key(row)
                if key in self.article_keys:
                    # A retry may include a body already committed before a crash.
                    continue
                self.article_keys.add(key)
                self.index[key] = index_entry(row, relative)
            line = json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()
            if relative not in self.writers:
                target = self.stage / "delta" / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                self.writers[relative] = gzip.GzipFile(filename=str(target), mode="wb", compresslevel=6, mtime=0)
                self.delta[relative] = blank_stats()
            current = self.manifest["files"].get(relative, {}).get("rows", 0) + self.delta[relative]["rows"]
            spans.setdefault(relative, {"first_row": current + 1, "rows": 0})["rows"] += 1
            self.writers[relative].write(line + b"\n")
            add_record(self.delta[relative], line, row)
            add_record(combined, line, row)
        self.new_inputs[source_id] = {"sha256": digest, "bytes": path.stat().st_size, "stats": combined, "partitions": spans}
        return True

    def add_blob(self, path, source_id, destination):
        path = Path(path)
        relative = str(safe_relative(destination))
        if relative.startswith(".state/") or relative in ("manifest.json", "README.txt"):
            raise ValueError("Reserved dataset path")
        digest = file_hash(path)
        if self.imported(source_id, digest):
            return False
        existing = self.blobs.get(relative, self.manifest["files"].get(relative))
        if existing and existing["sha256"] != digest:
            raise ValueError("A different artifact already has this destination: " + relative)
        target = self.stage / "ready" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        durable_file(target)
        if file_hash(target) != digest:
            raise ValueError("Artifact copy checksum mismatch")
        self.blobs[relative] = {"sha256": digest, "bytes": target.stat().st_size}
        self.new_inputs[source_id] = {"sha256": digest, "bytes": path.stat().st_size, "destination": relative}
        return True

    def commit(self):
        for writer in self.writers.values():
            writer.close()
        self.writers.clear()
        if not self.new_inputs:
            return self.manifest
        files = dict(self.manifest["files"])
        for relative, stats in self.delta.items():
            target = self.stage / "ready" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("wb") as dest:
                old = self.root / relative
                if old.exists():
                    if relative not in files or file_hash(old) != files[relative]["sha256"]:
                        raise ValueError("Existing partition checksum mismatch: " + relative)
                    with old.open("rb") as source:
                        shutil.copyfileobj(source, dest)
                with (self.stage / "delta" / relative).open("rb") as source:
                    shutil.copyfileobj(source, dest)
                dest.flush()
                os.fsync(dest.fileno())
            restrict_mode(target, 0o600)
            total = merge_stats(files.get(relative, blank_stats()), stats)
            files[relative] = {**total, "sha256": file_hash(target), "bytes": target.stat().st_size}
        files.update(self.blobs)
        article_files = {k: v for k, v in files.items() if k.startswith("articles/")}
        manifest = {"schema_version": 2, "updated_utc": utc_now(),
                    "articles": len({(r["article_id"], r["article_published_at"]) for r in self.index.values()}),
                    "article_versions": sum(v["rows"] for v in article_files.values()),
                    "article_dates": [min(article_files)[9:19], max(article_files)[9:19]] if article_files else [],
                    "article_days": len(article_files), "compressed_article_bytes": sum(v["bytes"] for v in article_files.values()),
                    "files": dict(sorted(files.items()))}
        self.inputs.update(self.new_inputs)
        ready = self.stage / "ready"
        write_json(ready / ".state/inputs.json", self.inputs)
        index_path = ready / ".state/article_index.json.gz"
        index_path.write_bytes(gzip.compress(json.dumps(self.index, ensure_ascii=False, separators=(",", ":")).encode(), mtime=0))
        durable_file(index_path)
        write_json(ready / "manifest.json", manifest)
        readme = ready / "README.txt"
        if (self.root / "README.txt").exists():
            shutil.copyfile(self.root / "README.txt", readme)
        else:
            readme.write_text(README)
        durable_file(readme)
        paths = sorted(set(self.delta) | set(self.blobs)) + ["README.txt", ".state/article_index.json.gz", ".state/inputs.json", "manifest.json"]
        plan = [{"path": p, "sha256": file_hash(ready / p)} for p in paths]
        write_json(self.stage / "commit.tmp", plan)
        os.replace(self.stage / "commit.tmp", self.stage / "commit.json")
        sync_directory(self.stage)
        self.recover()
        self.manifest = manifest
        self.new_inputs = {}
        self.delta = {}
        self.blobs = {}
        return manifest

    def close(self):
        for writer in self.writers.values():
            writer.close()
        if self.stage.exists() and not (self.stage / "commit.json").exists():
            shutil.rmtree(self.stage)
        self.lock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def verify(root=DEFAULT_ROOT):
    """Read every output record, verify hashes/counts and match all article refs."""
    root = Path(root).expanduser()
    manifest = json.loads((root / "manifest.json").read_text())
    inputs = json.loads((root / ".state/inputs.json").read_text())
    articles, references, publications, event_versions = set(), set(), set(), set()
    news_events, websocket_alerts, fetched_events = 0, 0, 0
    expected_index = {}
    output_stats, source_stats = blank_stats(), blank_stats()
    for source in inputs.values():
        if "stats" in source:
            source_stats = merge_stats(source_stats, source["stats"])
    for relative, expected in manifest["files"].items():
        path = root / safe_relative(relative)
        if path.stat().st_size != expected["bytes"] or file_hash(path) != expected["sha256"]:
            raise ValueError("Dataset file checksum mismatch: " + relative)
        if "rows" not in expected:
            continue
        actual = blank_stats()
        for line, row in records(path):
            check_body(row)
            stream = "history" if relative.startswith(("articles/", "metadata/feed_pages/")) else "capture"
            if partition(row, stream) != relative:
                raise ValueError("Record is in the wrong UTC date partition")
            add_record(actual, line, row)
            if relative.startswith("articles/"):
                key = article_key(row)
                if key in articles:
                    raise ValueError("Duplicate article version")
                articles.add(key)
                expected_index[key] = index_entry(row, relative)
                publications.add((row["article_id"], row["article_published_at"]))
                if row.get("article_version_id") != key:
                    raise ValueError("Article version ID mismatch")
            elif relative.startswith(("events/", "models/")):
                if row.get("article_version_id"):
                    event_versions.add(row["article_version_id"])
                if relative.startswith("events/") and "text" in row:
                    raise ValueError("Full article text belongs in articles, not news events")
                if relative.startswith("events/"):
                    news_events += 1
                    if row["kind"] == "article_fetched":
                        fetched_events += 1
                        if not row.get("article_version_id"):
                            raise ValueError("Article fetch event is missing its body reference")
                    message = row.get("message", {})
                    if row["kind"] == "rtpr_frame" and message.get("type") == "alert":
                        websocket_alerts += 1
                        references.add((article_id(message["article_url"]), message["article_published_at"]))
                        if not row.get("article_version_id"):
                            raise ValueError("WebSocket article alert is missing its body reference")
                    if row["kind"] == "rtpr_reconciliation_page":
                        for ref in row["page"]["articles"]:
                            references.add((article_id(ref["article_url"]), ref["article_published_at"]))
            elif relative.startswith("metadata/feed_pages/"):
                for ref in row["page"]["articles"]:
                    references.add((ref["article_url"].split("rtpr.io", 1)[1].split("?", 1)[0], ref["article_published_at"]))
        if actual != {k: expected[k] for k in actual}:
            raise ValueError("Record integrity mismatch: " + relative)
        output_stats = merge_stats(output_stats, actual)
    with gzip.open(root / ".state/article_index.json.gz", "rt") as handle:
        index = json.load(handle)
    check_state_matches(manifest, index)
    if output_stats != source_stats:
        raise ValueError("Imported source records do not match output records")
    if references - publications:
        raise ValueError("Article body/reference coverage differs")
    if event_versions - articles:
        raise ValueError("Event references a missing article version")
    if len(publications) != manifest["articles"] or len(articles) != manifest["article_versions"]:
        raise ValueError("Article count differs from manifest")
    if index != expected_index:
        raise ValueError("Article index does not match the actual records")
    return {"verified_utc": utc_now(), "articles": len(publications), "article_versions": len(articles), "article_days": manifest["article_days"],
            "article_dates": manifest["article_dates"], "verified_files": len(manifest["files"]),
            "verified_source_files": len(inputs), "records": output_stats["rows"],
            "news_events": news_events, "websocket_alerts": websocket_alerts, "article_fetch_events": fetched_events,
            "missing_articles": 0, "duplicate_article_versions": 0, "record_integrity_mismatches": 0,
            "method": "Every compressed file checksum, every JSON record, full-text hashes, date partitions, unique article versions, body/reference coverage, and source/output record fingerprint totals checked."}


README = """RTPR: one consolidated news dataset

This folder contains RTPR news only. Historical article downloads, recorded
WebSocket news and REST updates use the same articles/ and events/ partitions.
There are no separate history/download/observations datasets to combine.
The delta updater writes new data into these same files.

Market prices/order books, Jev requests/results and the original mixed collector
archive live separately in ~/data/news_market_capture/. They are not required
to read or update this RTPR dataset. The server collector on trade-columbus3 was
stopped and disabled on 2026-10-03; its data was removed after a complete local
SHA-256-verified copy. No live service is installed by update.py.

Layout

articles/YYYY-MM-DD.jsonl.gz
    Full article text and metadata, partitioned by UTC publication date.
    One row per unique content/format version. article_version_id is its exact
    lookup key. article_id + article_published_at identifies a release.
    Original Markdown and plain-text captures can be separate versions of the
    same release. Text is complete; it is never truncated to a model limit.

events/YYYY-MM-DD.jsonl.gz
    News events partitioned by UTC local receipt date. Includes WebSocket
    frames/alerts, connections/heartbeats, REST reconciliation responses,
    fetch errors and article_fetched completions. Full bodies are not stored
    here: article_version_id points to articles/. Article-related records have
    titles when available; control records naturally have no article/title.
    Historical download receipts were incorporated into this same table from
    their saved fetch metadata, using their original download timestamps.

metadata/feed_pages/YYYY-MM-DD.jsonl.gz
    Complete historical/API listing responses, all ticker references, cursors,
    unknown provider fields, request metadata and receipt timestamps.

metadata/migration/ and metadata/snapshots/
    Provenance manifests and import ledgers from the earlier layout. Old paths
    in these audit documents are historical references, not dependencies or
    separate datasets you need to join. They contain no market message stream.

reports/YYYY-MM-DD/
    Coverage checks, integrity validation, import proofs and latency reports.

manifest.json
    Current file hashes/counts and article totals. "articles" counts distinct
    article ID/publication pairs; "article_versions" includes format/body
    variants. These counts deliberately need not equal the event count.

.state/
    Portable JSON/gzip checkpoints, article index, source import ledger,
    pending API responses and a single-writer lock. Copy this hidden directory.

update.py, update-config.json, updater/
    Self-contained Python 3.10+ updater and bundled standard-library helpers.
    Linux/macOS/WSL. No repository checkout, SSH connection, rsync, SQLite,
    external Python packages or running server are required.

Data and clocks

Article records share article_id, article_published_at, article_version_id,
text, body_sha256, body_characters, body_format, title and receipt fields.
Additional original metadata is preserved. Source record kinds are retained
where applicable; article storage and lookup keys are the same for all sources.
Event message/page objects retain all provider fields, including future/unknown
fields; repeated ticker listings remain in the full saved pages and alerts.

RTPR currently supplies ticker, publication timestamp and article URL in its
feed/alerts. The title comes from the fetched document header. document_metadata
keeps the parsed header and original header lines, while text retains the whole
unaltered response. If a provider reference supplies title/headline, it is kept.
Authentication material such as API keys/signatures/cookies is redacted.

article_published_at is the source publication clock. receipt_utc,
receipt_unix_ns and receipt_monotonic_ns preserve local receipt precision.
Historical REST receipt times mean when we downloaded a body/page, not when the
news first arrived in a live feed. capture_transport distinguishes websocket,
rest_reconciliation and historical_rest where that field was recorded. Legacy
WebSocket alerts are also identifiable by kind=rtpr_frame and message.type=alert.
Monotonic timestamps are comparable only within the same originating run/host.

article_fetched events derived from old article download metadata carry
record_origin=saved_article_fetch_metadata. They are real saved fetch receipts,
not newly measured HTTP calls or reconstructed historical WebSocket arrivals.
An alert's title_source=article_lookup and title_available_at describe a later
body lookup; do not treat that title/text as information available at alert
arrival. Keep source publication and receipt clocks separate in latency work.

Coverage

The original fully downloaded API history begins on 2026-05-01. A full index
rescan and body reconciliation were run again on 2026-10-03. Saved live news
receipts cover 2026-09-29 through shutdown at about 22:45 UTC on 2026-10-03.
All recorded RTPR news events in the stopped collector have been merged here.
All fetched content versions are retained. Exact counts, latest API scan status
and pending bodies are available with --status; dated validation reports record
the checked coverage at consolidation time.

Verified consolidation snapshot (2026-10-03 23:13 UTC): 184,800 releases,
186,061 content/format versions and 213,059 news events. Latest stored publication:
2026-10-03 23:08:16.141 UTC. Full validation found zero missing bodies or duplicate
article versions. reports/2026-10-03/consolidation-coverage.json and
consolidation-validation.json hold the detailed evidence; future updates will
advance the live counts in manifest.json without rewriting this snapshot note.

"Complete" means all captured news plus full text for the provider references
we downloaded and audited. REST cannot reconstruct WebSocket arrival times from
before/after the actual capture period. It also cannot guarantee that text
retrieved later equals the version available at its original publication.
This dataset makes those distinctions explicit instead of inventing timestamps.

Update from RTPR

From this folder:
    python3 update.py --api-key-file ../api_keys/rtpr.json

Or set RTPR_API_KEY in the updater process environment and run:
    python3 update.py

On this machine the existing private account profile is:
    python3 ~/data/rtpr/update.py --api-key-file ~/arbitrage/api_keys/rtpr.json

The key-file profile accepts {"api_key": "..."}. No credential is bundled in
the shared dataset. Supply a private profile/environment on the next machine.
Only the key is external; no code is imported from its directory or the repo.

Each run scans from the latest feed backwards through a 24-hour overlap before
the newest stored publication, follows pagination, fetches every unseen
article ID/publication pair and appends full bodies and article_fetched events
into the same dated files above. A long gap between runs is handled by scanning
all the intervening pages. All listing response metadata is also saved.
Known bodies are not downloaded again. Retries and interrupted runs resume from
checkpoints, without duplicating bodies or replaying committed capture events.

Default request spacing is 1.1 seconds. HTTP 429/Retry-After and learned cooldowns
persist across restarts. Unavailable article bodies remain queued; a completed
run with unresolved bodies reports status=incomplete and exits with code 2.
A paused bounded run is resumable. Always inspect its reported status and pending
count when scheduling updates.

For an explicit complete index rescan back to the archive start:
    python3 update.py --since 2026-05-01 --api-key-file ../api_keys/rtpr.json

An empty dataset requires --since YYYY-MM-DD. The normal overlap catches recent
late arrivals, not arbitrarily old inserted references or silent body edits to
an already known article ID/publication pair. --since extends the index scan;
it does not refetch every known body merely to detect silent revisions.

Useful options:
    --max-requests N       Pause after N HTTP attempts; next call resumes.
    --overlap-hours 24     Recent publication window to rescan.
    --request-interval 1.1 Seconds between requests; minimum 1.
    --min-free-gib 1       Free disk reserve; updater pauses below this.
    --status              Local counts/checkpoint status; no networking.
    --verify              Check every file/record, body hash and article link.

Relocation and a mounted network drive

Copy the whole rtpr/ folder while its writer is idle, including .state/.
Default output is the folder containing update.py, wherever it is copied.
CLI --destination, --config, --import-capture and --api-key-file paths are
relative to the current working directory. api_key_file inside a config file
is relative to that config file. ~ and paths containing spaces are supported.

From a different working directory, for example:
    python3 ../rtpr/update.py --destination ../rtpr --api-key-file ../api_keys/rtpr.json

Run one writer at a time. Atomic lock-directory creation and file replacement
are used on the mounted drive; this requires the share to support those
filesystem operations correctly. Checkpoints use files, not a shared database.
Readers can use committed data. If a writer crashed, confirm that it stopped
before using --recover-lock. Never clear a lock while another host is writing.
The updater has no scheduler dependency; your other machine can call it on its
own schedule. No recurring process is installed by this bundle.

Reading

Gzip JSONL: one JSON object per line. Files can have multiple gzip members.
Sort by the relevant timestamp for time-series analysis; append/import order
is not guaranteed chronological. Historical and live rows share the same files.

Run Python from this folder:
    from updater.read import Reader
    data = Reader('.')
    for event in data.events('2026-10-03'):
        if event.get('article_version_id'):
            article = data.article(event['article_version_id'])
            print(event['receipt_utc'], article['title'])

Optional joined rows are produced in memory:
    for row in data.events('2026-10-03', with_articles=True):
        pass  # row['event'], row['article']

There is no duplicated joined table. Instantiate a fresh Reader after an update
to see new data; a Reader uses the committed manifest from when it was created.

Offline capture import (normally already done)

    python3 update.py --import-capture /path/to/sealed/collector/archive

This adds only RTPR news to the same article/event tables. It preserves clocks,
all news fields and complete bodies; markets and model records stay outside this
dataset. Imports are checkpointed by original immutable journal/checksum and
refuse changed previously imported files. Repeating an import adds no duplicate
news events or article versions. The original mixed journals are retained as a
verified archival backup in the separate news_market_capture folder; normal
RTPR analysis and delta updates do not depend on that backup.

Validation tests:
    python3 -m unittest discover -s updater -p 'test_*.py'
"""
