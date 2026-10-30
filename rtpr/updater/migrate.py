"""One-time, validated migration: article versions own bodies; events reference them."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil

from .article_metadata import article_id, enrich_article, version_id
from .dataset import (Dataset, WriterLock, atomic_json, durable_file, file_hash,
                      records, sync_directory, utc_now, verify, write_json)


def encoded(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.GzipFile(filename=str(path), mode="wb", mtime=0) as handle:
        for row in rows:
            handle.write(encoded(row) + b"\n")
    durable_file(path)


def migrate(root, min_free_gib=1):
    root = Path(root).resolve()
    state = root / ".state"
    lock = WriterLock(state)
    work = state / "migration"
    try:
        plan_path = state / "migration-commit.json"
        if plan_path.exists():
            return finish_migration(root, work, plan_path)
        old = json.loads((root / "manifest.json").read_text())
        if old["schema_version"] == 2:
            return {"already_migrated": True, "articles": old["articles"], "article_versions": old["article_versions"]}
        required = sum(value["bytes"] for value in old["files"].values()) * 2 + min_free_gib * 1024**3
        if shutil.disk_usage(root).free < required:
            raise ValueError("Not enough free space to validate the migration before replacing files")
        if work.exists():
            shutil.rmtree(work)
        output, incoming = work / "output", work / "incoming"
        incoming.mkdir(parents=True)
        proof = {"started_utc": utc_now(), "source_schema": 1, "destination_schema": 2,
                 "original_article_records": 0, "original_body_events": 0, "body_events_linked": 0,
                 "all_original_article_fields_preserved": True, "all_original_event_metadata_preserved": True,
                 "unchanged_files_verified": 0, "files": []}
        new_articles = {}
        body_links, model_links = {}, {}
        with Dataset(output) as dataset:
            # The metadata/page/market files are unchanged byte for byte.
            for relative, info in old["files"].items():
                source = root / relative
                if source.stat().st_size != info["bytes"] or file_hash(source) != info["sha256"]:
                    raise ValueError("Original file failed checksum verification: " + relative)
                proof["files"].append({"source": relative, "sha256": info["sha256"], "bytes": info["bytes"]})
                if relative.startswith("articles/") or relative.endswith(("/news.jsonl.gz", "/models.jsonl.gz")):
                    continue
                destination = relative
                if relative.startswith("events/"):
                    _, day, name = relative.split("/")
                    destination = (f"metadata/collector/{day}.jsonl.gz" if name == "collector.jsonl.gz"
                                   else f"markets/{day}/" + ("raw.jsonl.gz" if name == "prediction_market_raw.jsonl.gz" else name))
                target = output / destination
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                durable_file(target)
                if file_hash(target) != info["sha256"]:
                    raise ValueError("Migration copy checksum mismatch")
                dataset.manifest["files"][destination] = info
                entry = {"sha256": info["sha256"], "bytes": info["bytes"], "original_path": relative}
                if "rows" in info:
                    entry["stats"] = {key: info[key] for key in ("rows", "record_sha256_sum", "kinds", "full_text_characters")}
                    entry["partitions"] = {destination: {"first_row": 1, "rows": info["rows"]}}
                else:
                    entry["destination"] = destination
                dataset.inputs["migration/unchanged/" + relative] = entry
                proof["unchanged_files_verified"] += 1
            for relative in sorted(old["files"]):
                if not relative.startswith("articles/"):
                    continue
                def converted():
                    for _, original in records(root / relative):
                        row = enrich_article(original)
                        if any(row[key] != value for key, value in original.items()):
                            raise ValueError("Migration changed an original article field")
                        proof["original_article_records"] += 1
                        yield row
                path = incoming / relative
                write_rows(path, converted())
                dataset.ingest(path, "migration/enriched/" + relative, "history")
                path.unlink()
            print(json.dumps({"phase": "migrate_article_metadata", "articles": proof["original_article_records"]}), flush=True)
            # Register body variants from the live logs, then remove only their text fields.
            for relative in sorted(old["files"]):
                if not relative.endswith("/news.jsonl.gz"):
                    continue
                for _, row in records(root / relative):
                    if row["kind"] != "article_body":
                        continue
                    proof["original_body_events"] += 1
                    reference = {"ticker": row.get("ticker"), "article_published_at": row["article_published_at"],
                                 "article_url": "https://rtpr.io" + row["article_id"]}
                    article = enrich_article({**row, "kind": "rtpr_article", "reference": reference, "body_format": "md"})
                    key = article["article_version_id"]
                    if key not in dataset.index:
                        new_articles.setdefault(key, article)
                    body_links[(row["article_id"], row["alert_receipt"]["receipt_unix_ns"])] = key
                    model_links[(row["article_id"], row["body_sha256"])] = key
            if new_articles:
                path = incoming / "live-article-versions.jsonl.gz"
                write_rows(path, new_articles.values())
                dataset.ingest(path, "migration/live-article-versions", "history")
                path.unlink()
            by_publication = {}
            by_id = {}
            for key, row in dataset.index.items():
                by_publication.setdefault((row["article_id"], row["article_published_at"]), key)
                by_id.setdefault(row["article_id"], key)
            for relative in sorted(old["files"]):
                if not relative.endswith(("/news.jsonl.gz", "/models.jsonl.gz")):
                    continue
                def converted_events():
                    for _, original in records(root / relative):
                        row = dict(original)
                        kind = row["kind"]
                        key = None
                        if kind == "article_body":
                            key = model_links[(row["article_id"], row["body_sha256"])]
                            if dataset.index[key]["body_sha256"] != hashlib.sha256(row["text"].encode()).hexdigest():
                                raise ValueError("Migrated event body does not match its article version")
                            del row["text"]
                            row.update(kind="article_fetched", original_kind=kind)
                            proof["body_events_linked"] += 1
                        elif kind.startswith("jev_"):
                            key = model_links.get((row.get("article_id"), row.get("body_sha256")))
                        elif kind == "rtpr_frame" and row.get("message", {}).get("type") == "alert":
                            message = row["message"]
                            identity = article_id(message["article_url"])
                            row.setdefault("article_id", identity)
                            key = body_links.get((identity, row["receipt_unix_ns"])) or by_publication.get((identity, message.get("article_published_at")))
                        elif row.get("article_id"):
                            key = by_id.get(row["article_id"])
                        if key:
                            metadata = dataset.index[key]
                            row.update(article_version_id=key, title=metadata["title"],
                                       title_source="article_lookup", title_available_at=metadata["text_available_at"])
                            if kind == "article_body":
                                row.update(title_source="document_header", title_available_at=original["receipt_utc"])
                        for field, value in original.items():
                            if field not in ("kind", "text") and row.get(field) != value:
                                raise ValueError("Migration changed original event metadata")
                        yield row
                path = incoming / relative
                write_rows(path, converted_events())
                dataset.ingest(path, "migration/linked/" + relative, "capture")
                path.unlink()
            day = utc_now()[:10]
            dataset.add_blob(root / "manifest.json", "migration/original-manifest", f"metadata/migration/{day}/original-manifest.json")
            dataset.add_blob(state / "inputs.json", "migration/original-inputs", f"metadata/migration/{day}/original-inputs.json")
            proof.update(added_body_versions=len(new_articles), finished_utc=utc_now())
            write_json(incoming / "proof.json", proof)
            dataset.add_blob(incoming / "proof.json", "migration/schema-v2-proof", f"reports/{day}/schema-v2-migration.json")
            dataset.commit()
        print(json.dumps({"phase": "verify_migration", "article_body_events": proof["body_events_linked"]}), flush=True)
        validation = verify(output)
        write_json(incoming / "validation.json", validation)
        with Dataset(output) as dataset:
            dataset.add_blob(incoming / "validation.json", "migration/schema-v2-validation", f"reports/{day}/schema-v2-validation.json")
            dataset.commit()
        manifest = json.loads((output / "manifest.json").read_text())
        paths = sorted(manifest["files"]) + [".state/article_index.json.gz", ".state/inputs.json", "manifest.json"]
        plan = {"files": [{"path": p, "sha256": file_hash(output / p)} for p in paths],
                "remove": sorted(set(old["files"]) - set(manifest["files"])), "validation": validation}
        atomic_json(plan_path, plan)
        return finish_migration(root, work, plan_path)
    finally:
        lock.close()


def finish_migration(root, work, plan_path):
    plan = json.loads(plan_path.read_text())
    for entry in plan["files"]:
        source, target = work / "output" / entry["path"], root / entry["path"]
        candidate = source if source.exists() else target
        if file_hash(candidate) != entry["sha256"]:
            raise ValueError("Interrupted migration has a checksum mismatch")
        if source.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(source, target)
            sync_directory(target.parent)
    for relative in plan["remove"]:
        path = root / relative
        if path.exists():
            path.unlink()
    for path in sorted((p for p in (root / "events").rglob("*") if p.is_dir()), reverse=True):
        if not any(path.iterdir()):
            path.rmdir()
    plan_path.unlink()
    shutil.rmtree(work)
    return {"migrated": True, **plan["validation"]}
