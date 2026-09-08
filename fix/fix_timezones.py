#!/usr/bin/env python3

# One-off: make bar timestamps timezone-consistent.
#
#   ./fix/fix_timezones.py us_indices/day_aggs   --naive-is utc --to America/New_York
#   ./fix/fix_timezones.py global_crypto/minute_aggs --to UTC
#
# Background (measured 2026-09-01):
# - us_indices/day_aggs was written with a naive window_start until 2025-02-10 and
#   tz-aware America/New_York from 2025-03-10 on. The naive values are UTC clock
#   times: naive "2024-11-25 06:00:00" is the same instant as the newer files'
#   "2024-11-25 01:00:00-05:00". So naive values must be LOCALIZED TO UTC and then
#   CONVERTED to New York - localizing them straight to New York would move every
#   bar by 4-5 hours (that bug is exactly what us_indices/day_aggs_by_ticker has).
# - global_crypto/minute_aggs was written as America/New_York for most files and
#   UTC for 181 of them; crypto is UTC everywhere else, so it is converted to UTC.
#
# Converting a tz-aware column never changes the instant, only its label. Files
# already in the target timezone are skipped. Each file is rewritten to a temp
# file and os.replace()d, so an interrupted run leaves valid parquet everywhere.

import os
import sys
import argparse
import multiprocessing as mp
from glob import glob

import pandas as pd
import pyarrow.parquet as pq

TS_COLUMNS = ("window_start", "sip_timestamp", "timestamp")
TMP_SUFFIX = ".tzfix.tmp"


def timestamp_column(schema):
    for name in TS_COLUMNS:
        if name in schema.names:
            return name
    return None


def needs_fix(path, target):
    """-> (column, current_tz_or_None) if the file should be rewritten, else None."""
    schema = pq.ParquetFile(path).schema_arrow
    col = timestamp_column(schema)
    if col is None:
        return None
    tz = getattr(schema.field(col).type, "tz", None)
    if tz == target:
        return None
    return col, tz


def fix_file(job):
    path, target, naive_is, dry_run = job
    try:
        check = needs_fix(path, target)
        if check is None:
            return ("skipped", path, None)
        col, tz = check
        if tz is None and naive_is is None:
            return ("naive-left-alone", path, None)
        if dry_run:
            return ("would-fix", path, f"{tz or 'naive'} -> {target}")

        df = pd.read_parquet(path)
        index_names = [n for n in df.index.names if n is not None]
        flat = df.reset_index() if index_names else df
        series = flat[col]
        if series.dt.tz is None:
            series = series.dt.tz_localize(naive_is).dt.tz_convert(target)
        else:
            series = series.dt.tz_convert(target)
        flat[col] = series
        out = flat.set_index(index_names) if index_names else flat

        tmp = f"{path}.{os.getpid()}{TMP_SUFFIX}"
        out.to_parquet(tmp, compression="snappy")
        check_after = needs_fix(tmp, target)
        if check_after is not None:
            os.remove(tmp)
            return ("failed", path, "timezone not applied")
        if pq.ParquetFile(tmp).metadata.num_rows != pq.ParquetFile(path).metadata.num_rows:
            os.remove(tmp)
            return ("failed", path, "row count changed")
        try:
            os.chmod(tmp, os.stat(path).st_mode & 0o7777)
            gid = os.stat(path).st_gid
            if gid in os.getgroups():
                os.chown(tmp, -1, gid)
        except OSError:
            pass
        os.replace(tmp, path)
        return ("fixed", path, f"{tz or 'naive'} -> {target}")
    except Exception as e:
        return ("failed", path, repr(e))


def main():
    parser = argparse.ArgumentParser(description="Normalize parquet bar timestamps to one timezone.")
    parser.add_argument("directory")
    parser.add_argument("--to", required=True, help="target timezone, e.g. UTC or America/New_York")
    parser.add_argument("--naive-is", default=None,
                        help="timezone that naive values are already expressed in "
                             "(they are localized to it, then converted to --to). "
                             "Omit to leave naive files alone.")
    parser.add_argument("--glob", default="*.parquet")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    files = sorted(glob(os.path.join(args.directory, args.glob)))
    if not files:
        print(f"no files matching {args.glob} in {args.directory}")
        return 1
    print(f"{args.directory}: {len(files)} files -> {args.to}"
          + (f" (naive treated as {args.naive_is})" if args.naive_is else ""), flush=True)

    jobs = [(f, args.to, args.naive_is, args.dry_run) for f in files]
    counts = {}
    failures = []
    examples = {}
    ctx = mp.get_context("spawn")
    with ctx.Pool(args.workers) as pool:
        for status, path, detail in pool.imap_unordered(fix_file, jobs, chunksize=8):
            counts[status] = counts.get(status, 0) + 1
            if status == "failed":
                failures.append((path, detail))
            elif detail and status not in examples:
                examples[status] = f"{os.path.basename(path)}: {detail}"

    for status, n in sorted(counts.items()):
        print(f"  {status:18s} {n:6d}" + (f"   e.g. {examples[status]}" if status in examples else ""))
    for path, detail in failures[:10]:
        print(f"  FAILED {path}: {detail}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
