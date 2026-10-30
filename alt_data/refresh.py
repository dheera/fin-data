#!/usr/bin/env python3
"""Refresh acquired datasets, merge real history, rebuild the catalog and verify checksums."""
import argparse
import fcntl
import json
import subprocess
import sys

import pandas as pd

from expansion_common import ROOT, atomic_bytes, normalize_frame
from library import build_catalog, catalog, publish, read_metadata, validate

COLLECTOR_GROUPS = ["legacy", "environment", "digital", "activity", "macro",
                    "physical2", "activity2", "digital2", "macro2",
                    "physical3", "activity3", "digital3", "research3",
                    "energy4", "economy4", "digital4", "research4",
                    "knowledge5", "commerce5", "realworld5", "health5",
                    "asia6", "emerging6", "supply6", "intent6", "expectations6",
                    "foreign7", "grid7", "attention7", "forecast7",
                    "openaq10", "cams10", "energy11", "corporate11", "physical11", "commerce11", "smap11"]


def promote_existing(root=ROOT, groups=None, eligible_ids=None):
    """Refresh only selected datasets. Keep older observations outside rolling source windows."""
    import hashlib
    selected = catalog(root).set_index("series_id")
    updated = []
    plans = []
    for path in sorted((root / "archive/staging").glob("*/*.parquet")):
        if groups is not None and path.parent.name not in groups:
            continue
        if eligible_ids is not None and path.stem not in eligible_ids:
            continue
        if path.stem not in selected.index:
            continue
        meta = read_metadata(path)
        sid = meta["series_id"]
        if eligible_ids is not None and sid not in eligible_ids:
            continue
        if sid not in selected.index:
            continue
        previous = read_metadata(root / selected.loc[sid, "path"])
        for key in ("category", "unit", "provider", "frequency"):
            if previous[key] != meta[key]:
                raise ValueError(f"{sid}: {key} changed from {previous[key]!r} to {meta[key]!r}; review source definition before publishing")
        if previous.get("dataset_identity", sid) != meta.get("dataset_identity", sid):
            raise ValueError(f"{sid}: underlying dataset identity changed")
        retracted = pd.to_datetime(meta.get("retracted_dates", []), utc=True, errors="raise")
        if len(retracted) and pd.read_parquet(path, columns=["date"])["date"].isin(retracted).any():
            raise ValueError(f"{sid}: observation conflicts with an explicit source retraction")
        plans.append((path, meta, previous))
    # Check all identities/units before publishing any candidate.
    for path, meta, previous in plans:
        sid = meta["series_id"]
        old = pd.read_parquet(root / previous["path"])
        incoming = pd.read_parquet(path)
        # Ordinary omissions are ambiguous; explicit source withdrawals are not.
        # OFR final disclosure edits can withdraw an earlier preliminary value.
        retracted = pd.to_datetime(meta.get("retracted_dates", []), utc=True, errors="raise")
        if len(retracted):
            old = old[~old["date"].isin(retracted)]
        combined = pd.concat([old, incoming], ignore_index=True).drop_duplicates("date", keep="last")
        combined = normalize_frame(combined)
        meta["batch"] = previous["batch"]
        meta["status"] = previous["status"]
        meta["raw_sha256"] = sorted(set(meta["raw_sha256"]) | set(previous["raw_sha256"]))
        if meta.get("raw_manifest_sha256") or previous.get("raw_manifest_sha256"):
            meta["raw_manifest_sha256"] = sorted(set(meta.get("raw_manifest_sha256", []))
                                                | set(previous.get("raw_manifest_sha256", [])))
        meta["content_sha256"] = hashlib.sha256(
            combined["date"].astype("int64").to_numpy().tobytes() + combined["value"].to_numpy().tobytes()
        ).hexdigest()
        publish(combined, meta, root)
        updated.append(sid)
    return updated


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", choices=["all"] + COLLECTOR_GROUPS, default="all")
    parser.add_argument("--refresh", action="store_true", help="Bypass response caches")
    parser.add_argument("--publish-only", action="store_true", help="Use already downloaded candidates without network calls")
    args = parser.parse_args()
    lock = open(ROOT / ".refresh.lock", "a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.exit(2, "Another refresh is running\n")
    report = {"started_at": pd.Timestamp.now(tz="UTC").isoformat(), "collectors": [], "failures": []}
    eligible = set()
    groups = set(COLLECTOR_GROUPS) - {"legacy"} if args.group == "all" else {args.group}
    if not args.publish_only:
        for group in COLLECTOR_GROUPS:
            if args.group not in {"all", group}:
                continue
            command = [sys.executable, str(ROOT / ("collect.py" if group == "legacy" else f"sources_{group}.py"))]
            staged_before = {p.stem: p.stat().st_mtime_ns for p in (ROOT / "archive/staging" / group).glob("*.parquet")}
            if args.refresh:
                command.append("--refresh")
            result = subprocess.run(command, cwd=ROOT)
            eligible.update(
                p.stem for p in (ROOT / "archive/staging" / group).glob("*.parquet")
                if p.stat().st_mtime_ns != staged_before.get(p.stem)
            )
            report["collectors"].append({"group": group, "returncode": result.returncode})
            if result.returncode:
                report["failures"].append(group)
    with open(ROOT / ".collect.lock", "a") as publish_lock:
        fcntl.flock(publish_lock, fcntl.LOCK_EX)
        report["updated"] = promote_existing(ROOT, groups=groups, eligible_ids=None if args.publish_only else eligible)
        build_catalog(ROOT)
        report["validation"] = validate(ROOT)
    report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(ROOT / "logs/latest_refresh.json", json.dumps(report, indent=2).encode())
    print(f"Published {len(report['updated'])} refreshed datasets; failed groups: {report['failures']}")
    return int(bool(report["failures"] or report["validation"]["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
