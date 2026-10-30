#!/usr/bin/env python3
"""One-time, resumable migration. Moves originals intact and creates date/value views."""
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from expansion_common import ROOT, atomic_bytes
from library import build_catalog, publish_legacy


def migrate(root=ROOT):
    root = Path(root)
    manifest_path = root / "logs/layout_migration.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"moves": [], "original_series": []}
    mappings = [
        ("data/raw", "archive/raw"),
        ("data/series", "archive/legacy_observations"),
        ("data/vintages", "archive/revisions"),
        ("data/snapshots/series", "archive/legacy_short_history"),
        ("data/snapshots/vintages", "archive/revisions"),
        ("data/runs", "logs/legacy"),
        ("samples", "archive/samples"),
    ]
    for src, dst in mappings:
        source, target = root / src, root / dst
        if not source.exists():
            continue
        target.mkdir(parents=True, exist_ok=True)
        for path in sorted(source.iterdir()):
            if path.is_dir():
                raise ValueError(f"Unexpected nested directory: {path}")
            destination = target / path.name
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if destination.exists():
                if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
                    raise ValueError(f"Conflicting destination: {destination}")
                path.unlink()  # byte-identical duplicate only
            else:
                path.rename(destination)
            manifest["moves"].append({"from": str(path.relative_to(root)), "to": str(destination.relative_to(root)),
                                      "sha256": digest})
            atomic_bytes(manifest_path, json.dumps(manifest, indent=2).encode())
        source.rmdir()
    for name in ("inventory.json", "snapshot_inventory.json", "latest_run.json", ".collect.lock"):
        source = root / "data" / name
        if source.exists():
            target = root / "logs/legacy" / name
            if target.exists():
                raise ValueError(f"Refusing to replace {target}")
            source.rename(target)
    snapshots = root / "data/snapshots"
    if snapshots.exists() and not list(snapshots.iterdir()):
        snapshots.rmdir()
    ids = set()
    for folder, short in [("archive/legacy_observations", False), ("archive/legacy_short_history", True)]:
        for path in sorted((root / folder).glob("*.parquet")):
            if path.stem in ids:
                raise ValueError(f"Duplicate original identifier: {path.stem}")
            ids.add(path.stem)
            rows = pq.read_table(path).to_pylist()
            publish_legacy(rows, root, short_history=short)
    manifest["original_series"] = sorted(ids)
    manifest["complete"] = True
    atomic_bytes(manifest_path, json.dumps(manifest, indent=2).encode())
    frame = build_catalog(root)
    print(f"Migrated {len(frame)} datasets; all original bytes retained under archive/")


if __name__ == "__main__":
    migrate()
