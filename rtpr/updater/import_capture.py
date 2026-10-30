"""Import recorded RTPR news from sealed mixed collector journals, without markets.

The original journals remain in the separate news_market_capture archive. This
module preserves every news message/metadata field, moves bodies to articles,
and links alerts to the exact body fetched for that receipt when available.
"""
from __future__ import annotations

import collections
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile

from .article_metadata import article_id, enrich_article, version_id
from .dataset import Dataset, check_body, file_hash, records, utc_now, write_json

KIND = re.compile(rb'^\{\s*"kind"\s*:\s*"([^"\\]+)"')


def encoded(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()


def write_rows(path, rows):
    with gzip.GzipFile(filename=str(path), mode="wb", mtime=0) as handle:
        for row in rows:
            handle.write(encoded(row) + b"\n")


def news_rows(path, counts):
    # Collector kind is the first field. Avoid decoding millions of unrelated
    # price messages, while falling back to full decoding for any other layout.
    with gzip.open(path, "rb") as handle:
        for line in handle:
            match = KIND.match(line)
            row = None
            if match:
                kind = match[1].decode()
            else:
                row = json.loads(line)
                kind = row["kind"]
            counts[kind] += 1
            if kind.startswith(("rtpr_", "article_")):
                yield row if row is not None else json.loads(line)


def fetch_identity(row):
    return (row["article_version_id"], row.get("receipt_utc"), row.get("receipt_unix_ns"))


def backfill_fetch_events(dataset, work):
    """Expose existing historical download receipts as text-free fetch events.

These are derived from the original saved fetch metadata. No live arrival or
new HTTP request is invented, and existing fetch completions are not doubled.
"""
    seen = set()
    for relative in dataset.manifest["files"]:
        if relative.startswith("events/"):
            for _, row in records(dataset.root / relative):
                if row["kind"] == "article_fetched" and row.get("article_version_id"):
                    seen.add(fetch_identity(row))
    count = 0

    def missing():
        nonlocal count
        for relative in sorted(dataset.manifest["files"]):
            if not relative.startswith("articles/"):
                continue
            for _, article in records(dataset.root / relative):
                identity = fetch_identity(article)
                if identity in seen:
                    continue
                seen.add(identity)
                row = {k: v for k, v in article.items() if k != "text"}
                row.update(kind="article_fetched", original_kind=article["kind"],
                           record_origin="saved_article_fetch_metadata",
                           title_available_at=article.get("receipt_utc"))
                count += 1
                yield row

    path = work / "historical-fetch-events.jsonl.gz"
    write_rows(path, missing())
    if count:
        dataset.ingest(path, "fetch-event-backfill/" + file_hash(path), "capture")
        dataset.commit()
    return count


def import_capture(root, source, min_free_gib=1):
    root, source = Path(root).resolve(), Path(source).resolve()
    paths = sorted(source.glob("*.jsonl.gz"))
    if not paths:
        raise ValueError("No sealed collector journals found in " + str(source))
    if list(source.glob("*.part")):
        raise ValueError("Seal or stop the source collector before importing")
    if shutil.disk_usage(root).free < sum(p.stat().st_size for p in paths) * 4 + min_free_gib * 1024**3:
        raise ValueError("Insufficient free space for a validated news import")
    legacy = {}
    for p in (root / "metadata/migration").glob("*/original-inputs.json"):
        for key, value in json.loads(p.read_text()).items():
            if key.startswith("capture/"):
                legacy[key[8:]] = value["sha256"]
    proof = {"started_utc": utc_now(), "source": str(source), "source_files": [],
             "new_source_files": 0, "previously_imported_source_files": 0,
             "new_article_versions": 0, "new_capture_events": 0,
             "news_fields_preserved": True, "original_receipt_clocks_preserved": True}
    with Dataset(root) as dataset, tempfile.TemporaryDirectory(prefix="news-import-", dir=dataset.state_dir) as temporary:
        work = Path(temporary)
        spools = []
        alert_references = {}
        body_links = {}
        new_articles = {}
        for path in paths:
            digest = file_hash(path)
            source_id = "capture-news-v2/" + path.name
            previous = dataset.inputs.get(source_id)
            known = previous.get("original_source", {}).get("sha256") if previous else legacy.get(path.name)
            if known is not None:
                if known != digest:
                    raise ValueError("Previously imported collector journal changed: " + path.name)
                proof["previously_imported_source_files"] += 1
                continue
            counts = collections.Counter()
            spool = work / path.name

            def selected():
                for row in news_rows(path, counts):
                    message = row.get("message", {})
                    if row["kind"] == "rtpr_frame" and message.get("type") == "alert":
                        identity = article_id(message["article_url"])
                        alert_references[(identity, row["receipt_unix_ns"])] = message
                    yield row

            write_rows(spool, selected())
            if file_hash(path) != digest:
                raise ValueError("Collector journal changed during import")
            entry = {"path": path.name, "sha256": digest, "bytes": path.stat().st_size,
                     "record_kinds": dict(counts),
                     "news_records": sum(v for k, v in counts.items() if k.startswith(("rtpr_", "article_")))}
            proof["source_files"].append(entry)
            spools.append((spool, source_id, entry))
            if len(spools) % 20 == 0:
                print(json.dumps({"phase": "extract_news", "source_files": len(spools)}), flush=True)

        # All body versions are registered before linking alerts, including when
        # an alert and its fetch lie on opposite sides of an hourly boundary.
        for spool, _, entry in spools:
            for _, row in records(spool):
                if row["kind"] != "article_body":
                    continue
                check_body(row)
                arrival = row.get("alert_receipt", {}).get("receipt_unix_ns")
                reference = alert_references.get((row["article_id"], arrival),
                            {"ticker": row.get("ticker"), "article_published_at": row["article_published_at"],
                             "article_url": "https://rtpr.io" + row["article_id"]})
                article = enrich_article({**row, "kind": "rtpr_article", "reference": reference,
                                          "body_format": row.get("body_format", "md"),
                                          "source_journal": entry["path"], "source_file_sha256": entry["sha256"]})
                key = article["article_version_id"]
                if key not in dataset.index:
                    new_articles.setdefault(key, article)
                body_links[(row["article_id"], arrival)] = (key, row["receipt_utc"])
        if new_articles:
            path = work / "new-body-versions.jsonl.gz"
            write_rows(path, new_articles.values())
            dataset.ingest(path, "capture-bodies-v2/" + file_hash(path), "history")
        proof["new_article_versions"] = len(new_articles)
        by_publication, by_id = {}, {}
        for key, row in dataset.index.items():
            by_publication.setdefault((row["article_id"], row["article_published_at"]), key)
            by_id.setdefault(row["article_id"], key)
        for spool, source_id, entry in spools:
            count = 0

            def linked():
                nonlocal count
                for _, original in records(spool):
                    row = dict(original)
                    kind, key, available = row["kind"], None, None
                    if kind == "article_body":
                        key = version_id(row)
                        del row["text"]
                        row.update(kind="article_fetched", original_kind=kind)
                        available = row["receipt_utc"]
                    elif kind == "rtpr_frame" and row.get("message", {}).get("type") == "alert":
                        message = row["message"]
                        identity = article_id(message["article_url"])
                        row.setdefault("article_id", identity)
                        row.setdefault("article_published_at", message.get("article_published_at"))
                        row.setdefault("ticker", message.get("ticker"))
                        row.setdefault("capture_transport", "websocket")
                        exact = body_links.get((identity, row["receipt_unix_ns"]))
                        if exact:
                            key, available = exact
                        else:
                            key = by_publication.get((identity, message.get("article_published_at")))
                    elif row.get("article_id"):
                        key = by_publication.get((row["article_id"], row.get("article_published_at"))) or by_id.get(row["article_id"])
                    if key:
                        metadata = dataset.index[key]
                        row.update(article_version_id=key, title=metadata["title"],
                                   title_source="document_header" if kind == "article_body" else "article_lookup",
                                   title_available_at=available or metadata["text_available_at"])
                        row.setdefault("article_published_at", metadata["article_published_at"])
                    row.setdefault("source_journal", entry["path"])
                    row.setdefault("source_file_sha256", entry["sha256"])
                    for field, value in original.items():
                        if field not in ("kind", "text") and row.get(field) != value:
                            raise ValueError("Import changed original news metadata: " + field)
                    if "text" in row:
                        raise ValueError("Unexpected full text outside an article_body event")
                    count += 1
                    yield row

            path = work / ("linked-" + spool.name)
            write_rows(path, linked())
            dataset.ingest(path, source_id, "capture")
            dataset.new_inputs[source_id]["original_source"] = entry
            proof["new_capture_events"] += count
        proof["new_source_files"] = len(spools)
        dataset.commit()
        proof["historical_fetch_events_added"] = backfill_fetch_events(dataset, work)
        proof["finished_utc"] = utc_now()
        path = work / "proof.json"
        write_json(path, proof)
        digest = file_hash(path)
        dataset.add_blob(path, "capture-import-proof/" + digest,
                         "reports/" + proof["finished_utc"][:10] + "/capture-import-" + digest[:12] + ".json")
        dataset.commit()
    return {k: v for k, v in proof.items() if k != "source_files"}
