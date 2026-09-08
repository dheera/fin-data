#!/usr/bin/env python3

# One-off: re-encode existing us_options_opra/quotes parquet files into the
# format csv2parquet_options_quotes.py now writes (zstd-3, delta-encoded
# timestamps, 1M-row row groups) without going back to the CSV flat files.
#
#   ./reencode_options_quotes.py us_options_opra/quotes
#   ./reencode_options_quotes.py us_options_opra/quotes --dates 2026-02-02 --verify full
#
# Rows, schema and row order are preserved exactly - this is a pure re-encode,
# ~45% smaller (11 -> 6.2 bytes/row on options quotes). The parquet settings
# come from csv2parquet_options_quotes.py so the two stay in sync.
#
# Safety:
# - Each file is written to <file>.reencode.tmp and os.replace()d into place,
#   so an interrupted run leaves every parquet valid (a mix of old and new
#   format, both readable) and can simply be rerun.
# - The temp file is verified (row count, schema, and by default the first and
#   last rows) before it replaces the original; on any failure the original is
#   left alone and the file is reported.
# - Files already in the target format are skipped; a day whose files are all
#   done gets a .reencoded marker so a rerun skips it without reading ~6000
#   parquet footers over NFS.
#
# Files are rewritten in place on the NFS mount, largest first across the whole
# job, so the workers stay busy instead of queueing behind one 6 GB SPXW file.
#
# To stop a run, kill the whole process group (the pool's worker processes do
# not die with the parent):  kill -- -$(ps -o pgid= -p <pid> | tr -d ' ')
# Temp files carry the writing pid, so a leftover one is never removed while
# its process is alive, and two runs over the same directory cannot collide.

import os
import re
import sys
import grp
import time
import argparse
import importlib.util
import multiprocessing as mp
from collections import deque
from glob import glob

import pyarrow as pa
import pyarrow.parquet as pq

TMP_SUFFIX = ".reencode.tmp"          # <file>.<pid>.reencode.tmp
TMP_GLOB = "*.reencode.tmp"
TMP_RE = re.compile(r"\.(\d+)" + re.escape(TMP_SUFFIX) + r"$")
MARKER = ".reencoded"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BATCH_ROWS = 1 << 18
SPOT_ROWS = 50_000


def load_converter():
    """Import csv2parquet_options_quotes.py (next to this script) for the
    parquet settings and row group size, so both write identical files."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "csv2parquet_options_quotes.py")
    spec = importlib.util.spec_from_file_location("csv2parquet_options_quotes", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONV = load_converter()
ROW_GROUP_ROWS = CONV.ROW_GROUP_ROWS


def parquet_opts_for(schema, compression, zstd_level):
    """Writer options from the converter, restricted to columns this file has."""
    opts = dict(CONV.make_parquet_opts(compression, zstd_level))
    names = set(schema.names)
    encoding = opts.get("column_encoding")
    if encoding:
        encoding = {k: v for k, v in encoding.items() if k in names}
        if encoding:
            opts["column_encoding"] = encoding
        else:
            opts.pop("column_encoding")
    if isinstance(opts.get("use_dictionary"), list):
        opts["use_dictionary"] = [c for c in opts["use_dictionary"] if c in names]
    return opts


def is_target_format(metadata, compression):
    """True if the file is already encoded the way we would write it."""
    if metadata.num_row_groups == 0:
        return True
    want = "ZSTD" if compression == "zstd" else "SNAPPY"
    rg = metadata.row_group(0)
    for j in range(rg.num_columns):
        col = rg.column(j)
        if col.compression != want:
            return False
        if compression == "zstd" and col.path_in_schema == "sip_timestamp":
            if not any("DELTA_BINARY_PACKED" in str(e) for e in col.encodings):
                return False
    return True


class Window:
    """Pulls fixed-size row windows out of a stream of record batches."""

    def __init__(self, batches, schema):
        self.batches = batches
        self.schema = schema
        self.buf = deque()
        self.rows = 0

    def take(self, n):
        while self.rows < n:
            batch = next(self.batches, None)
            if batch is None:
                break
            if batch.num_rows:
                self.buf.append(batch)
                self.rows += batch.num_rows
        if not self.buf:
            return self.schema.empty_table()
        table = pa.Table.from_batches(list(self.buf), schema=self.schema)
        self.buf.clear()
        self.rows = 0
        if table.num_rows > n:
            rest = table.slice(n)
            self.buf.extend(rest.to_batches())
            self.rows = rest.num_rows
            table = table.slice(0, n)
        return table


def tail_rows(path, n):
    """Last n rows of a parquet file."""
    pf = pq.ParquetFile(path)
    groups = []
    rows = 0
    for i in range(pf.metadata.num_row_groups - 1, -1, -1):
        groups.insert(0, i)
        rows += pf.metadata.row_group(i).num_rows
        if rows >= n:
            break
    if not groups:
        return pf.schema_arrow.empty_table()
    table = pf.read_row_groups(groups)
    return table.slice(max(0, table.num_rows - n))


def verify_full(src_path, tmp_path, schema):
    """Compare both files row by row in bounded memory."""
    src = Window(pq.ParquetFile(src_path).iter_batches(batch_size=BATCH_ROWS), schema)
    dst = Window(pq.ParquetFile(tmp_path).iter_batches(batch_size=BATCH_ROWS), schema)
    while True:
        a = src.take(BATCH_ROWS)
        b = dst.take(BATCH_ROWS)
        if a.num_rows == 0 and b.num_rows == 0:
            return None
        if not a.equals(b):
            return "content differs"


def reencode_file(job):
    path, opts = job
    t0 = time.time()
    # The pid keeps two concurrent runs over the same directory (or a run and
    # an orphaned worker of a previous one) from writing and deleting each
    # other's temp files.
    tmp = f"{path}.{os.getpid()}{TMP_SUFFIX}"
    try:
        pf = pq.ParquetFile(path)
        metadata = pf.metadata
        schema = pf.schema_arrow
        size_before = os.path.getsize(path)
        if not opts["force"] and is_target_format(metadata, opts["compression"]):
            return {"path": path, "status": "skipped", "rows": metadata.num_rows,
                    "before": size_before, "after": size_before, "secs": 0.0}
        if opts["dry_run"]:
            return {"path": path, "status": "would-convert", "rows": metadata.num_rows,
                    "before": size_before, "after": size_before, "secs": 0.0}

        head = None
        tail = deque()
        tail_rows_kept = 0
        rows = 0
        buffered = []
        n_buffered = 0
        writer = pq.ParquetWriter(tmp, schema,
                                  **parquet_opts_for(schema, opts["compression"], opts["zstd_level"]))
        try:
            for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                if batch.num_rows == 0:
                    continue
                rows += batch.num_rows
                if opts["verify"] == "spot":
                    if head is None or head.num_rows < SPOT_ROWS:
                        piece = pa.Table.from_batches([batch], schema=schema)
                        head = piece if head is None else pa.concat_tables([head, piece])
                        if head.num_rows > SPOT_ROWS:
                            head = head.slice(0, SPOT_ROWS)
                    tail.append(batch)
                    tail_rows_kept += batch.num_rows
                    while len(tail) > 1 and tail_rows_kept - tail[0].num_rows >= SPOT_ROWS:
                        tail_rows_kept -= tail.popleft().num_rows
                buffered.append(batch)
                n_buffered += batch.num_rows
                if n_buffered >= ROW_GROUP_ROWS:
                    table = pa.Table.from_batches(buffered, schema=schema)
                    buffered, n_buffered = [], 0
                    full = (table.num_rows // ROW_GROUP_ROWS) * ROW_GROUP_ROWS
                    writer.write_table(table.slice(0, full), row_group_size=ROW_GROUP_ROWS)
                    rest = table.slice(full)
                    if rest.num_rows:
                        buffered = rest.to_batches()
                        n_buffered = rest.num_rows
            if n_buffered:
                writer.write_table(pa.Table.from_batches(buffered, schema=schema),
                                   row_group_size=ROW_GROUP_ROWS)
        finally:
            writer.close()

        new = pq.ParquetFile(tmp)
        problem = None
        if new.metadata.num_rows != rows or rows != metadata.num_rows:
            problem = f"row count {new.metadata.num_rows} != {metadata.num_rows}"
        elif new.schema_arrow != schema:
            problem = "schema changed"
        elif opts["verify"] == "full":
            problem = verify_full(path, tmp, schema)
        elif opts["verify"] == "spot" and rows:
            if not new.read_row_groups([0]).slice(0, head.num_rows).equals(head):
                problem = "first rows differ"
            else:
                kept = pa.Table.from_batches(list(tail), schema=schema)
                kept = kept.slice(max(0, kept.num_rows - SPOT_ROWS))
                if not tail_rows(tmp, kept.num_rows).equals(kept):
                    problem = "last rows differ"
        if problem:
            os.remove(tmp)
            return {"path": path, "status": "failed", "error": problem, "rows": rows,
                    "before": size_before, "after": size_before, "secs": time.time() - t0}

        size_after = os.path.getsize(tmp)
        try:  # keep the original's permissions, and its group if we belong to it
            os.chmod(tmp, os.stat(path).st_mode & 0o7777)
            gid = os.stat(path).st_gid
            if gid in os.getgroups():
                os.chown(tmp, -1, gid)
        except OSError:
            pass
        os.replace(tmp, path)
        return {"path": path, "status": "converted", "rows": rows,
                "before": size_before, "after": size_after, "secs": time.time() - t0}
    except Exception as e:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        return {"path": path, "status": "failed", "error": repr(e), "rows": 0,
                "before": 0, "after": 0, "secs": time.time() - t0}


def day_dirs(root, dates):
    out = []
    for d in sorted(os.listdir(root)):
        if not DATE_RE.match(d) or not os.path.isdir(os.path.join(root, d)):
            continue
        if dates and d not in dates:
            continue
        out.append(os.path.join(root, d))
    return out


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def clean_stale(day):
    """Remove temp files left by dead runs, never one a live process is writing."""
    for p in glob(os.path.join(day, TMP_GLOB)):
        m = TMP_RE.search(p)
        if m and pid_alive(int(m.group(1))):
            print(f"  leaving {os.path.basename(p)} alone: pid {m.group(1)} is still running",
                  flush=True)
            continue
        try:
            os.remove(p)
        except OSError:
            pass


def gb(n):
    return n / 2**30


def main():
    parser = argparse.ArgumentParser(
        description="One-off: re-encode existing options-quotes parquet files into the "
                    "format csv2parquet_options_quotes.py writes (zstd + delta timestamps).")
    parser.add_argument("quotes_dir", type=str, help="e.g. us_options_opra/quotes")
    parser.add_argument("--workers", type=int, default=8,
                        help="files re-encoded concurrently (default 8; the job is NFS-bound)")
    parser.add_argument("--dates", type=str, default=None,
                        help="comma-separated day dirs to convert (default: all)")
    parser.add_argument("--verify", choices=["spot", "full", "rows"], default="spot",
                        help="spot (default): row count, schema, first and last 50k rows; "
                             "full: compare every row (slower); rows: row count and schema only")
    parser.add_argument("--compression", choices=["zstd", "snappy"], default="zstd")
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--force", action="store_true",
                        help="re-encode even files that are already in the target format")
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be converted, write nothing")
    args = parser.parse_args()

    opts = {
        "compression": args.compression,
        "zstd_level": args.zstd_level,
        "verify": args.verify,
        "force": args.force,
        "dry_run": args.dry_run,
    }
    dates = set(args.dates.split(",")) if args.dates else None
    days = day_dirs(args.quotes_dir, dates)
    if not days:
        print(f"no day directories found in {args.quotes_dir}")
        return 1
    print(f"{len(days)} day dirs, {args.workers} workers, verify={args.verify}, "
          f"compression={args.compression}", flush=True)

    t_start = time.time()
    totals = {"converted": 0, "skipped": 0, "failed": 0, "before": 0, "after": 0, "rows": 0}
    failures = []
    ctx = mp.get_context("spawn")
    with ctx.Pool(args.workers) as pool:
        for day in days:
            name = os.path.basename(day)
            if os.path.exists(os.path.join(day, MARKER)) and not args.force:
                print(f"{name}: already re-encoded, skipping", flush=True)
                continue
            clean_stale(day)
            files = sorted(glob(os.path.join(day, "*.parquet")), key=os.path.getsize, reverse=True)
            if not files:
                print(f"{name}: no parquet files", flush=True)
                continue
            t0 = time.time()
            day_stats = {"converted": 0, "skipped": 0, "failed": 0,
                         "before": 0, "after": 0, "rows": 0}
            for res in pool.imap_unordered(reencode_file, [(f, opts) for f in files], chunksize=4):
                status = res["status"]
                key = "converted" if status == "would-convert" else status
                day_stats[key] = day_stats.get(key, 0) + 1
                day_stats["before"] += res["before"]
                day_stats["after"] += res["after"]
                day_stats["rows"] += res["rows"]
                if status == "failed":
                    failures.append((res["path"], res.get("error")))
                    print(f"  FAILED {os.path.basename(res['path'])}: {res.get('error')}", flush=True)
            for k in totals:
                totals[k] += day_stats.get(k, 0)
            saved = day_stats["before"] - day_stats["after"]
            print(f"{name}: {day_stats['converted']} converted, {day_stats['skipped']} skipped, "
                  f"{day_stats['failed']} failed, {day_stats['rows']/1e9:.2f}B rows, "
                  f"{gb(day_stats['before']):.1f} -> {gb(day_stats['after']):.1f} GiB "
                  f"(saved {gb(saved):.1f} GiB), {(time.time()-t0)/60:.1f} min "
                  f"[total {(time.time()-t_start)/60:.1f} min]", flush=True)
            if not day_stats["failed"] and not args.dry_run:
                try:
                    open(os.path.join(day, MARKER), "w").close()
                except OSError:
                    pass

    saved = totals["before"] - totals["after"]
    print(f"\n{totals['converted']} converted, {totals['skipped']} skipped, "
          f"{totals['failed']} failed, {totals['rows']/1e9:.2f}B rows, "
          f"{gb(totals['before']):.1f} -> {gb(totals['after']):.1f} GiB "
          f"(saved {gb(saved):.1f} GiB) in {(time.time()-t_start)/60:.1f} min")
    if failures:
        print(f"{len(failures)} files failed and were left untouched:")
        for path, err in failures[:20]:
            print(f"  {path}: {err}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
