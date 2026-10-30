#!/usr/bin/env python3
"""Load, search, publish and audit the alternative-data library."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from expansion_common import (
    ROOT, DATA_SCHEMA, atomic_bytes, coverage, normalize_frame, parquet_bytes, qualifies_daily,
    qualifies_research, raw_provenance_hashes,
)


def read_metadata(path):
    return json.loads(pq.read_schema(path).metadata[b"alt_data"])


def _collection_clock():
    return pd.Timestamp.now(tz="UTC").isoformat()


def publish(frame, meta, root=ROOT):
    root = Path(root)
    path = root / "data" / meta["category"] / (meta["series_id"] + ".parquet")
    meta = dict(meta, path=str(path.relative_to(root)))
    meta.update(coverage(frame))
    meta["daily_eligible"] = qualifies_daily(meta)
    meta["research_eligible"] = qualifies_research(meta)
    # Track when a numeric version became usable in this local library. This is
    # deliberately distinct from any claim about the source's first release.
    if meta.get("batch", 0) >= 8:
        meta["locally_available_at"] = _collection_clock()
    if path.exists():
        old = read_metadata(path)
        if old.get("content_sha256") == meta.get("content_sha256") and old.get("locally_available_at"):
            meta["locally_available_at"] = old["locally_available_at"]
        if old.get("content_sha256") != meta.get("content_sha256"):
            # Preserve every prior publicly consumed version, including original metadata.
            # Repeated A -> B -> A values are distinct local availability events,
            # even when a cached acquisition reused A's retrieval metadata.
            version_clock = old.get("locally_available_at", old["retrieved_at"])
            revision_name = version_clock.replace(":", "") + "_" + old["content_sha256"] + ".parquet"
            revision = root / "archive/revisions" / meta["series_id"] / revision_name
            if not revision.exists():
                atomic_bytes(revision, path.read_bytes())
    atomic_bytes(path, parquet_bytes(frame, meta))
    return meta


def publish_legacy(rows, root=ROOT, short_history=False):
    last = rows[-1]
    frame = normalize_frame([{"date": r["timestamp"], "value": r["value"]} for r in rows])
    meta = {
        "series_id": last["series_id"], "title": last["title"], "provider": last["source"],
        "category": last["category"], "unit": last["unit"], "frequency": last["frequency"],
        "source_url": last["source_url"], "geography": last["geography"],
        "hypothesis": "Existing dataset; evaluate the linked methodology for the relevant economic exposure.",
        "limitations": last["methodology"], "retrieved_at": str(last["retrieved_at"]),
        "raw_sha256": sorted({r["raw_sha256"] for r in rows if r["raw_sha256"]}),
        "batch": 0, "status": "short_history" if short_history else "existing",
        "native_frequency": last["frequency"], "dataset_identity": last["series_id"],
        "content_sha256": hashlib.sha256(
            frame["date"].astype("int64").to_numpy().tobytes() + frame["value"].to_numpy().tobytes()
        ).hexdigest(),
    }
    return publish(frame, meta, root)


def build_catalog(root=ROOT):
    root = Path(root)
    paths = sorted((root / "data").glob("*/*.parquet"))
    with ThreadPoolExecutor(max_workers=8) as pool:
        items = list(pool.map(read_metadata, paths))
    if not items:
        raise ValueError("No data files")
    if len({x["series_id"] for x in items}) != len(items):
        raise ValueError("Duplicate IDs across categories")
    # Keep the public catalog flat and easy to search with pandas, DuckDB or a text editor.
    for x in items:
        x["age_days"] = int((pd.Timestamp.now(tz="UTC").normalize() - pd.Timestamp(x["end"]).normalize()).days)
        x["stale_over_45_days"] = x["age_days"] > 45
        x.setdefault("native_frequency", x["frequency"])
        x.setdefault("finer_frequency_available", bool(x["median_interval_seconds"] and x["median_interval_seconds"] < 86400))
        x["raw_sha256"] = json.dumps(x["raw_sha256"])
        for key, value in list(x.items()):
            if isinstance(value, (dict, list)):
                x[key] = json.dumps(value, sort_keys=True)
    frame = pd.DataFrame(items).sort_values(["category", "series_id"]).reset_index(drop=True)
    ordered = ["series_id", "title", "category", "provider", "frequency", "native_frequency",
               "finer_frequency_available", "unit", "geography_scope", "geography", "country_codes",
               "start", "end", "rows", "path", "batch", "daily_eligible", "research_eligible",
               "age_days", "stale_over_45_days", "expected_publication_lag",
               "source_url", "hypothesis", "limitations"]
    # Provider-specific metadata can use the same name for a numbered source
    # column and a named source column. Keep mixed optional fields searchable
    # as text in the flat catalog; their original types remain in each footer.
    # Core catalog fields keep their declared numeric/boolean/text semantics.
    for column in frame.select_dtypes(include="object"):
        if column in ordered:
            continue
        values = frame[column].dropna()
        types = set(values.map(type))
        if str in types and len(types) > 1:
            frame[column] = frame[column].map(
                lambda value: None if pd.isna(value) else value if isinstance(value, str) else json.dumps(value)
            )
    frame = frame[[x for x in ordered if x in frame] + [x for x in frame if x not in ordered]]
    atomic_bytes(root / "catalog.parquet", frame.to_parquet(index=False, compression="zstd"))
    atomic_bytes(root / "catalog.csv", frame.to_csv(index=False).encode())
    return frame


def catalog(root=ROOT):
    return pd.read_parquet(Path(root) / "catalog.parquet")


def load(series_id, root=ROOT):
    """Return one UTC date/value frame, without filling gaps or changing frequency."""
    selected = catalog(root).query("series_id == @series_id")
    if len(selected) != 1:
        raise KeyError(series_id)
    return pd.read_parquet(Path(root) / selected.iloc[0]["path"])


def load_collected_asof(series_id, as_of, root=ROOT):
    """Load the latest numeric version actually published locally by `as_of`.

    Tracking starts with batch 8. Older source dates downloaded today do not
    become available yesterday. This does not reconstruct historical releases.
    """
    root = Path(root)
    selected = catalog(root).query("series_id == @series_id")
    if len(selected) != 1:
        raise KeyError(series_id)
    cutoff = pd.Timestamp(as_of)
    if cutoff.tzinfo is None:
        raise ValueError("as_of must include an explicit timezone")
    # Read the current inode BEFORE listing archives. Since publish archives a
    # predecessor before replacing current, a too-new current version guarantees
    # its predecessors exist by the subsequent directory listing.
    with (root / selected.iloc[0]["path"]).open("rb") as stream:
        parquet = pq.ParquetFile(stream)
        meta = json.loads(parquet.schema_arrow.metadata[b"alt_data"])
        clock = meta.get("locally_available_at")
        if clock and pd.Timestamp(clock) <= cutoff:
            return parquet.read().to_pandas()
    best_clock, best_frame = None, None
    for path in (root / "archive/revisions" / series_id).glob("*.parquet"):
        # One open inode supplies BOTH metadata and values; atomic replacement
        # cannot swap future values into a previously inspected clock.
        with path.open("rb") as stream:
            parquet = pq.ParquetFile(stream)
            meta = json.loads(parquet.schema_arrow.metadata[b"alt_data"])
            clock = meta.get("locally_available_at")
            if clock:
                clock = pd.Timestamp(clock)
                if clock <= cutoff and (best_clock is None or clock > best_clock):
                    best_clock, best_frame = clock, parquet.read().to_pandas()
    if best_frame is not None:
        return best_frame
    raise LookupError(f"No locally tracked version of {series_id} was available by {cutoff.isoformat()}")


def panel(series_ids, root=ROOT):
    """Outer-join exact observation timestamps; never silently aggregate or forward-fill."""
    return pd.concat(
        [load(sid, root).set_index("date")["value"].rename(sid) for sid in series_ids], axis=1
    ).sort_index()


def publish_staged(count, batch, root=ROOT, groups=None, selection=None, selection_policy=None,
                   workers=1):
    """Prioritize current data, then diversify across provider and category."""
    root = Path(root)
    if count <= 0 or batch <= 0:
        raise ValueError("Count and batch must be positive")
    if not 1 <= workers <= 16:
        raise ValueError("Publication workers must be between 1 and 16")
    if selection is not None and len(selection) != count:
        raise ValueError("Selection must contain exactly the requested number of distinct IDs")
    current = catalog(root) if (root / "catalog.parquet").exists() else pd.DataFrame()
    if ((len(current) and (current["batch"] == batch).any())
            or (root / "logs" / f"batch_{batch}.json").exists()):
        raise ValueError(f"Batch {batch} already exists; refusing to add another batch under the same number")
    existing = set(current["series_id"]) if len(current) else set()
    selected_ids = set(existing)
    selected_identities = set(current["dataset_identity"].dropna()) if "dataset_identity" in current else set()
    selected_content = set(current["content_sha256"].dropna()) if "content_sha256" in current else set()
    queues = defaultdict(list)
    rejected = []
    paths = [path for path in sorted((root / "archive/staging").glob("*/*.parquet"))
             if (groups is None or path.parent.name in groups)
             and (selection is None or path.stem in selection)]

    def inspect_candidate(path):
        table = pq.read_table(path, columns=["value"])
        meta = json.loads(table.schema.metadata[b"alt_data"])
        variable = table.column("value").to_pandas().nunique() >= 2
        return path, meta, variable

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for path, meta, variable in pool.map(inspect_candidate, paths):
            if selection is not None and selection.get(meta["series_id"]) != meta["content_sha256"]:
                raise ValueError("Selected candidate changed after selection: " + meta["series_id"])
            if meta["series_id"] in existing:
                continue
            if not qualifies_research(meta):
                rejected.append({"series_id": meta["series_id"], "reason": "insufficient daily/weekly history"})
                continue
            if not variable:
                rejected.append({"series_id": meta["series_id"], "reason": "constant history"})
                continue
            queues[(meta["provider"], meta["category"])].append((path, meta))
    # Prefer recent, well-covered histories within each provider/category.
    cutoff = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=45)

    def stale(item):
        return pd.Timestamp(item[1]["end"]).normalize() < cutoff

    for queue in queues.values():
        queue.sort(key=lambda item: (
            stale(item),
            -item[1]["day_coverage"], -item[1]["observed_days"], item[1]["series_id"],
        ))
    selected = []
    for stale_phase in (False, True):
        while len(selected) < count and any(q and stale(q[0]) == stale_phase for q in queues.values()):
            for key in sorted(queues):
                if not queues[key] or len(selected) >= count or stale(queues[key][0]) != stale_phase:
                    continue
                path, meta = queues[key].pop(0)
                identity = meta.get("dataset_identity", meta["series_id"])
                if (meta["series_id"] in selected_ids or identity in selected_identities
                        or meta["content_sha256"] in selected_content):
                    rejected.append({"series_id": meta["series_id"], "reason": "duplicate ID, identity or numeric history"})
                    continue
                selected_ids.add(meta["series_id"])
                selected_identities.add(identity)
                selected_content.add(meta["content_sha256"])
                selected.append((path, meta))
    if len(selected) != count:
        raise ValueError(f"Batch {batch}: need {count}, only {len(selected)} distinct validated candidates; rejected {rejected}")
    report = {"batch": batch, "requested": count, "groups": sorted(groups) if groups else None,
              "selection_policy": selection_policy or "Prefer end dates within 45 days, then older histories; provider/category round-robin within each freshness tier, with coverage and history depth as tie-breakers.",
              "started_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "series": [], "rejected": rejected}
    def publish_one(item):
        path, meta = item
        frame = normalize_frame(pd.read_parquet(path))
        actual_hash = hashlib.sha256(
            frame["date"].astype("int64").to_numpy().tobytes()
            + frame["value"].to_numpy().tobytes()
        ).hexdigest()
        if actual_hash != meta["content_sha256"] or (
                selection is not None and actual_hash != selection[meta["series_id"]]):
            raise ValueError("Numeric history changed before publication: " + meta["series_id"])
        meta.update(batch=batch, status="ready")
        result = publish(frame, meta, root)
        return {k: result[k] for k in ("series_id", "provider", "category", "rows", "start", "end", "frequency", "path")}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(publish_one, selected):
            report["series"].append(result)
            if count >= 1000 and len(report["series"]) % 5000 == 0:
                print(f"Published {len(report['series'])}/{count}", flush=True)
    build_catalog(root)
    report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(root / "logs" / f"batch_{batch}.json", json.dumps(report, indent=2).encode())
    return report


def validate(root=ROOT, verify_raw=True, expected_new=None):
    root = Path(root)
    frame = catalog(root)
    hashes = set()
    manifest_cache = {}
    contents = {}
    identities = {}
    failures = []
    def check_series(meta):
        try:
            path = root / meta["path"]
            table = pq.read_table(path)
            if not table.schema.remove_metadata().equals(DATA_SCHEMA):
                raise ValueError("not the date/value schema")
            original = table.to_pandas()
            data = normalize_frame(original)
            if not original.equals(data):
                raise ValueError("dates not sorted")
            embedded = json.loads(table.schema.metadata[b"alt_data"])
            actual = coverage(data)
            if any(embedded[k] != value for k, value in actual.items()):
                raise ValueError("embedded coverage disagrees with actual values")
            if any(meta[k] != embedded[k] for k in (
                    "series_id", "path", "rows", "frequency", "start", "end", "content_sha256",
                    "batch", "category", "unit", "provider")):
                raise ValueError("catalog disagrees with Parquet")
            actual_hash = hashlib.sha256(
                data["date"].astype("int64").to_numpy().tobytes() + data["value"].to_numpy().tobytes()
            ).hexdigest()
            if actual_hash != meta["content_sha256"]:
                raise ValueError("content checksum mismatch")
            if meta.get("batch", 0) > 0:
                if not qualifies_research(embedded):
                    raise ValueError("new data does not meet daily/weekly history requirements")
                if data["value"].nunique() < 2:
                    raise ValueError("constant history")
            raw_hashes = embedded["raw_sha256"]
            if not raw_hashes:
                raise ValueError("no raw provenance")
            return meta, embedded, None
        except Exception as exc:
            return meta, None, str(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        checked = pool.map(check_series, frame.to_dict("records"))
        for index, (meta, embedded, error) in enumerate(checked, 1):
            try:
                if error:
                    raise ValueError(error)
                if meta.get("batch", 0) > 0:
                    actual_hash = embedded["content_sha256"]
                    if actual_hash in contents:
                        raise ValueError(f"duplicate history of {contents[actual_hash]}")
                    contents[actual_hash] = meta["series_id"]
                    identity = embedded.get("dataset_identity", meta["series_id"])
                    if identity in identities:
                        raise ValueError(f"duplicate dataset identity of {identities[identity]}")
                    identities[identity] = meta["series_id"]
                hashes.update(raw_provenance_hashes(embedded, root, manifest_cache))
            except Exception as exc:
                failures.append({"series_id": meta["series_id"], "error": str(exc)})
            if len(frame) > 5000 and index % 5000 == 0:
                print(f"Validated series {index} / {len(frame)}", flush=True)
    if verify_raw:
        def check_raw(digest):
            path = root / "archive/raw" / (digest + ".bin")
            actual = None
            if path.exists():
                with path.open("rb") as stream:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
            return digest, actual == digest

        with ThreadPoolExecutor(max_workers=4) as pool:
            for digest, valid in pool.map(check_raw, sorted(hashes)):
                if not valid:
                    failures.append({"raw_sha256": digest, "error": "raw missing or checksum mismatch"})
    new = frame[frame["batch"] > 0]
    for batch, members in new.groupby("batch"):
        receipt_path = root / "logs" / f"batch_{batch}.json"
        if not receipt_path.exists():
            failures.append({"batch": int(batch), "error": "missing batch receipt"})
            continue
        receipt = json.loads(receipt_path.read_text())
        recorded = [x["series_id"] for x in receipt["series"]]
        if (receipt["batch"] != batch or receipt["requested"] != len(members)
                or len(recorded) != len(set(recorded)) or set(recorded) != set(members.series_id)):
            failures.append({"batch": int(batch), "error": "batch receipt differs from catalog membership"})
    report = {
        "checked_at": pd.Timestamp.now(tz="UTC").isoformat(), "total_series": len(frame),
        "new_series": len(new), "observations": int(frame["rows"].sum()),
        "new_observations": int(new["rows"].sum()), "new_providers": int(new["provider"].nunique()),
        "new_categories": int(new["category"].nunique()),
        "new_intraday_series": int((new["median_interval_seconds"] < 86400).sum()),
        "batch_counts": {str(k): int(v) for k, v in new["batch"].value_counts().sort_index().items()},
        "raw_responses_verified": len(hashes) if verify_raw else 0,
        "failures": failures,
    }
    completion = root / "logs/expansion_complete.json"
    if expected_new is None and completion.exists():
        expected_new = json.loads(completion.read_text())["new_series"]
    if expected_new is not None:
        if len(new) != expected_new:
            failures.append({"error": f"Expected {expected_new} new series; found {len(new)}"})
    if completion.exists():
        expected_batches = json.loads(completion.read_text()).get("batch_counts", {})
        for batch, count in expected_batches.items():
            if report["batch_counts"].get(batch) != count:
                failures.append({"error": f"Batch {batch}: expected {count}, found {report['batch_counts'].get(batch)}"})
    atomic_bytes(root / "logs/validation.json", json.dumps(report, indent=2).encode())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    ls = sub.add_parser("list")
    ls.add_argument("--search", default="")
    ls.add_argument("--new", action="store_true")
    ls.add_argument("--batch", type=int)
    sub.add_parser("catalog")
    audit = sub.add_parser("validate")
    audit.add_argument("--expected-new", type=int)
    pub = sub.add_parser("publish")
    pub.add_argument("--count", type=int, required=True)
    pub.add_argument("--batch", type=int, required=True)
    pub.add_argument("--group", action="append", help="Restrict publication to these staging groups; repeat as needed")
    pub.add_argument("--selection", type=Path, help="Audited JSON plan with exact series IDs and numeric checksums")
    pub.add_argument("--workers", type=int, default=1, help="Parallel atomic file writes (1–16)")
    args = parser.parse_args()
    if args.command == "list":
        frame = catalog(args.root)
        if args.new:
            frame = frame[frame["batch"] > 0]
        if args.batch is not None:
            frame = frame[frame["batch"] == args.batch]
        if args.search:
            text = frame[["series_id", "title", "provider", "category", "geography"]].fillna("").agg(" ".join, axis=1)
            frame = frame[text.str.contains(args.search, case=False, regex=False)]
        print(frame[["series_id", "frequency", "rows", "start", "end"]].to_string(index=False))
        print(f"{len(frame)} datasets")
    elif args.command == "catalog":
        print(f"Catalog: {len(build_catalog(args.root))} datasets")
    elif args.command == "publish":
        plan = json.loads(args.selection.read_text()) if args.selection else None
        selection = {row["series_id"]: row["content_sha256"] for row in plan["series"]} if plan else None
        result = publish_staged(args.count, args.batch, args.root, groups=args.group,
                                selection=selection,
                                selection_policy=plan["selection_policy"] if plan else None,
                                workers=args.workers)
        print(f"Published batch {args.batch}: {len(result['series'])} datasets")
    else:
        result = validate(args.root, expected_new=args.expected_new)
        print(json.dumps(result, indent=2))
        return int(bool(result["failures"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
