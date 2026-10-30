#!/usr/bin/env python3
"""Fetch RTPR deltas directly into the dataset beside this script.

No repository, SSH host, external Python packages or database server required.
CLI relative paths use the working directory; config key paths use the config
file's directory. Copy the whole dataset including .state/ to another machine.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import sys

sys.dont_write_bytecode = True
SCRIPT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_ROOT))

from updater.dataset import Dataset, recover_lock, verify
from updater.direct_api import DirectUpdater, read_key
from updater.migrate import migrate

DEFAULT_CONFIG = {"api_key_file": None, "request_interval_s": 1.1, "overlap_hours": 24, "min_free_gib": 1}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--destination", type=Path, default=SCRIPT_ROOT, help="Default: directory containing update.py")
    parser.add_argument("--config", type=Path, help="Default: update-config.json beside update.py")
    parser.add_argument("--api-key-file", "--key-file", type=Path, help="Private JSON profile with api_key, or plaintext key; otherwise RTPR_API_KEY")
    parser.add_argument("--request-interval", type=float, help="Seconds between API requests; minimum 1, default 1.1")
    parser.add_argument("--overlap-hours", type=float, help="Revisit this publication window on each delta run; default 24")
    parser.add_argument("--since", help="Initial or explicit scan boundary, YYYY-MM-DD or timezone-aware ISO timestamp")
    parser.add_argument("--min-free-gib", type=float, help="Preserve this much free disk space; default 1 GiB")
    parser.add_argument("--max-requests", type=int, help="Pause safely after this many HTTP attempts; next invocation resumes")
    parser.add_argument("--recover-lock", action="store_true", help="Remove a stale lock only after confirming the previous writer has stopped")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="Local counts and checkpoint status, without networking")
    action.add_argument("--verify", action="store_true", help="Check all local file hashes, records, article versions and links")
    action.add_argument("--migrate", action="store_true", help="Migrate an old dataset to articles plus events referencing bodies")
    action.add_argument("--import-capture", type=Path, help="Import RTPR events and full bodies from sealed local collector journals; excludes markets and models")
    args = parser.parse_args(argv)
    args.destination = args.destination.expanduser().resolve()
    if args.max_requests is not None and args.max_requests <= 0:
        parser.error("--max-requests must be positive")
    return args


def settings(args):
    path = args.config.expanduser().resolve() if args.config else SCRIPT_ROOT / "update-config.json"
    config = dict(DEFAULT_CONFIG)
    if path.exists():
        supplied = json.loads(path.read_text())
        if not isinstance(supplied, dict) or set(supplied) - set(config):
            raise ValueError("Config accepts api_key_file, request_interval_s, overlap_hours and min_free_gib")
        config.update(supplied)
    elif args.config:
        raise ValueError("Config file does not exist: " + str(path))
    key_file = args.api_key_file
    if key_file is None and config["api_key_file"]:
        key_file = Path(config["api_key_file"]).expanduser()
        if not key_file.is_absolute():
            key_file = path.parent / key_file
    if key_file is not None:
        key_file = key_file.expanduser().resolve()
    interval = args.request_interval if args.request_interval is not None else config["request_interval_s"]
    overlap = args.overlap_hours if args.overlap_hours is not None else config["overlap_hours"]
    minimum = args.min_free_gib if args.min_free_gib is not None else config["min_free_gib"]
    if not all(isinstance(v, (int, float)) and math.isfinite(v) for v in (interval, overlap, minimum)):
        raise ValueError("Numeric settings must be finite numbers")
    if interval < 1 or overlap < 0 or minimum < 0:
        raise ValueError("Request interval must be at least 1 second; overlap and free-space reserve must be nonnegative")
    return {"key_file": key_file, "interval": interval, "overlap_hours": overlap, "min_free_gib": minimum}


def local_status(root):
    manifest = json.loads((root / "manifest.json").read_text())
    checkpoint = root / ".state/api.json"
    api = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    return {"destination": str(root), **{k: v for k, v in manifest.items() if k != "files"},
            "event_dates": sorted({Path(p).name[:10] for p in manifest["files"] if p.startswith("events/")}),
            "news_events": sum(v.get("rows", 0) for p, v in manifest["files"].items() if p.startswith("events/")),
            "api_update_phase": api.get("phase"), "pending_articles": len(api.get("todo", {})),
            "last_api_update_finished": api.get("finished", {}).get("receipt_utc"),
            "latest_publication_seen": api.get("latest_publication_seen")}


def main(argv=None):
    args = parse_args(argv)
    if args.recover_lock:
        recover_lock(args.destination)
    if args.status:
        result = local_status(args.destination)
    elif args.verify:
        with Dataset(args.destination):
            result = verify(args.destination)
    else:
        config = settings(args)
        if args.migrate:
            result = migrate(args.destination, min_free_gib=config["min_free_gib"])
        elif args.import_capture:
            from updater.import_capture import import_capture
            result = import_capture(args.destination, args.import_capture.expanduser(), min_free_gib=config["min_free_gib"])
        else:
            key = read_key(config.pop("key_file"))
            with Dataset(args.destination) as dataset:
                result = DirectUpdater(dataset, key, **config, since=args.since, max_requests=args.max_requests).run()
    print(json.dumps(result, indent=2), flush=True)
    return 2 if result.get("status") == "incomplete" else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted; saved API responses and checkpoints will resume on the next run.", file=sys.stderr)
        raise SystemExit(130)
    except (ValueError, OSError) as exc:
        print("Update failed: " + str(exc), file=sys.stderr)
        raise SystemExit(1)
