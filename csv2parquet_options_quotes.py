#!/usr/bin/env python3

# Stream-converts Polygon OPRA options quotes CSV.gz flat files (100-170 GB
# compressed / ~1 TB uncompressed / ~10^10 rows per day) to per-underlying
# Parquet files.
#
#   ./csv2parquet_options_quotes.py us_options_opra/quotes_v1 us_options_opra/quotes
#
# Measured 2026-08-30 on the 7950X / 10G-NFS box: ~25 min per day file with one
# worker (11.8 GB peak RSS), 3 days per 33 min with --workers 3; the old script
# took ~2.5 h per day and ~85 GB RAM.
#
# Output: <out_dir>/<date>/<date>-<underlying>.parquet with the same schema as
# before (expiry, type, strike, sip_timestamp, ask_*, bid_*). A day is written
# to <out_dir>/<date>.tmp and renamed into place when complete, so an existing
# date dir means done.
#
# Row order (--order):
#   contract  (default) every contract's rows are contiguous and time-ordered,
#             contracts sorted by (expiry, type, strike). This is the order of
#             the flat files themselves, so it needs no sorting, spilling or
#             buffering, and row-group min/max stats on expiry/strike let
#             readers pull one expiry or strike range out of a 10+ GB SPXW file
#             without scanning all of it, e.g.
#               pq.read_table(f, filters=[("expiry", "=", 260202), ("strike", ">=", 690)])
#             Caveat: a flat file is 2-3 ticker-sorted partitions concatenated
#             (disjoint contract sets), so a file holds 2-3 sorted runs rather
#             than one; each contract is still in exactly one contiguous run.
#             The days converted before this rewrite are in this order too:
#             the old script's final sort never ran on them.
#   time      globally sorted by sip_timestamp (what the old script intended).
#             Costs a sort; big underlyings are spilled to local disk, see below.
#
# How it stays fast and memory-bounded:
# - Decompression: ISA-L igzip (pip install isal; ~1.2 GB/s on one core), else
#   rapidgzip (parallel, ~1.8 GB/s but 7-10 cores), else unpigz, else gzip.
# - Parsing: the decompressed stream is cut into large blocks (--block-mb) that
#   are parsed by pyarrow's multi-threaded read_csv. The streaming open_csv
#   reader used previously is effectively single-threaded (~0.16 GB/s) and was
#   the main bottleneck.
# - Ticker parsing happens once per distinct contract per block, not per row.
# - Within a partition the flat files are sorted by option ticker, so all rows
#   of an underlying are contiguous. In contract order the underlying's parquet
#   file simply stays open (a few thousand open files) and later partitions are
#   appended. In time order a reappearing underlying gets a segment file that
#   is merged (memory-bounded) at the end, which roughly doubles the work.
# - With --order time, underlyings with more than --spill-rows rows (SPXW, SPX,
#   NDXP, SPY, ... a few dozen per day) are bucketed by 5-minute window into
#   Arrow IPC files under --spill-dir (local disk, keep it OFF the NFS mount),
#   then each bucket is sorted and appended to the final parquet file. Peak
#   memory is a few GB per worker instead of ~3x the largest underlying
#   (~80 GB for SPXW).

import os
import re
import sys
import gzip
import time
import queue
import shutil
import argparse
import datetime
import subprocess
import threading
import traceback
import multiprocessing as mp
from glob import glob
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq

TICKER_RE = re.compile(r"^O:(.+?)(\d{6})([CP])(\d{8})$")
NY_TZ = ZoneInfo("America/New_York")
NY_TS = pa.timestamp("ns", tz="America/New_York")

CSV_COLUMN_TYPES = {
    "ticker": pa.string(),
    "sip_timestamp": pa.int64(),
    "ask_exchange": pa.int16(),
    "ask_price": pa.float32(),
    "ask_size": pa.int32(),
    "bid_exchange": pa.int16(),
    "bid_price": pa.float32(),
    "bid_size": pa.int32(),
}
CSV_COLUMNS = list(CSV_COLUMN_TYPES)
QUOTE_COLUMNS = ["ask_exchange", "ask_price", "ask_size", "bid_exchange", "bid_price", "bid_size"]

# Internal schema keeps sip_timestamp as int64 epoch ns (cheapest to sort and
# bucket); it is cast to a tz-aware timestamp when the parquet is written.
WORK_SCHEMA = pa.schema([
    ("expiry", pa.int32()),
    ("type", pa.string()),
    ("strike", pa.float64()),
    ("sip_timestamp", pa.int64()),
    ("ask_exchange", pa.int16()),
    ("ask_price", pa.float32()),
    ("ask_size", pa.int32()),
    ("bid_exchange", pa.int16()),
    ("bid_price", pa.float32()),
    ("bid_size", pa.int32()),
])
TS_INDEX = WORK_SCHEMA.get_field_index("sip_timestamp")
OUT_SCHEMA = WORK_SCHEMA.set(TS_INDEX, pa.field("sip_timestamp", NY_TS))

ROW_GROUP_ROWS = 1 << 20
MIN_PARQUET_FILES = 100   # sanity guard before --delete-original truncates a source
BUCKET_NS = 5 * 60 * 10**9
N_BUCKETS = 24 * 12  # one day of 5-minute buckets
HOUR_NS = 3600 * 10**9
IPC_OPTIONS = pa.ipc.IpcWriteOptions(compression="lz4")


# ----------------------------------------------------------------------------
# input: decompression + block splitting + parallel parsing
# ----------------------------------------------------------------------------

class Decompressor:
    """Opens a .csv.gz for reading with the fastest available decompressor."""

    def __init__(self, path, threads):
        self.proc = None
        self.file = None
        # ISA-L igzip: ~1.2 GB/s on ONE core (pip install isal). rapidgzip is
        # faster in isolation (~1.8 GB/s) but burns 7-10 cores for it, which
        # starves the parsers when several files are converted at once.
        try:
            import isal.igzip  # noqa: F401
            self.name = "isal-igzip"
            self.proc = subprocess.Popen([sys.executable, "-m", "isal.igzip", "-d", "-c", path],
                                         stdout=subprocess.PIPE)
            self.stream = self.proc.stdout
            return
        except ImportError:
            pass
        exe = shutil.which("rapidgzip")
        if exe is None and os.path.exists(os.path.expanduser("~/.local/bin/rapidgzip")):
            exe = os.path.expanduser("~/.local/bin/rapidgzip")
        if exe:
            self.name = "rapidgzip"
            self.proc = subprocess.Popen([exe, "-d", "-c", "-P", str(min(threads, 8)), path],
                                         stdout=subprocess.PIPE)
            self.stream = self.proc.stdout
            return
        try:
            import rapidgzip
            self.name = "rapidgzip(module)"
            self.file = rapidgzip.open(path, parallelization=min(threads, 8))
            self.stream = self.file
            return
        except ImportError:
            pass
        exe = shutil.which("unpigz") or shutil.which("pigz")
        if exe:
            self.name = os.path.basename(exe)
            self.proc = subprocess.Popen([exe, "-dc", path], stdout=subprocess.PIPE)
            self.stream = self.proc.stdout
            return
        self.name = "gzip"
        self.file = gzip.open(path, "rb")
        self.stream = self.file

    def close(self, ok=True):
        if self.proc is not None:
            if not ok and self.proc.poll() is None:
                self.proc.kill()
            self.proc.stdout.close()
            rc = self.proc.wait()
            if ok and rc != 0:
                raise RuntimeError(f"{self.name} exited with status {rc}")
        elif self.file is not None:
            self.file.close()


PIPE_READ = 1 << 20
F_SETPIPE_SZ = 1031  # linux fcntl; larger pipe = fewer context switches with the decompressor


class BufferRing:
    """Preallocated block buffers shared between the reader thread and the
    parse workers. Reading the decompressor's pipe in ~1 MB pieces straight
    into reused buffers runs at ~1.7 GB/s; allocating/joining fresh 128 MB
    blocks per read costs page faults and ran at ~0.7 GB/s."""

    def __init__(self, n, block_size):
        self.block_size = block_size
        self.bufs = [bytearray(block_size) for _ in range(n)]
        self.views = [memoryview(b) for b in self.bufs]
        self.free = queue.Queue()
        for i in range(n):
            self.free.put(i)

    def acquire(self, stop):
        while not stop.is_set():
            try:
                return self.free.get(timeout=1)
            except queue.Empty:
                pass
        return None

    def release(self, i):
        self.free.put(i)


def stream_filler(stream):
    """Return fill(view) -> bytes read into the memoryview (0 at EOF)."""
    try:
        fd = stream.fileno()
        os.fstat(fd)
        try:
            import fcntl
            fcntl.fcntl(fd, F_SETPIPE_SZ, PIPE_READ)
        except (OSError, ImportError):
            pass
        return lambda view: os.readv(fd, [view])
    except (AttributeError, OSError, ValueError):
        if hasattr(stream, "readinto"):
            return stream.readinto

        def fill(view):
            b = stream.read(len(view))
            view[:len(b)] = b
            return len(b)
        return fill


def reader_thread(stream, ring, out_q, stop, initial):
    """Fills ring buffers with newline-terminated blocks and queues
    (buffer index, length); None at EOF, or an Exception on failure."""
    try:
        fill = stream_filler(stream)
        carry = initial
        bs = ring.block_size
        eof = False
        while not eof and not stop.is_set():
            i = ring.acquire(stop)
            if i is None:
                return
            mv = ring.views[i]
            got = len(carry)
            mv[:got] = carry
            while got < bs:
                k = fill(mv[got:got + PIPE_READ])
                if k == 0:
                    eof = True
                    break
                got += k
            if eof:
                if got == 0 or not bytes(mv[:got]).strip():
                    ring.release(i)
                    break
                if mv[got - 1] != 10:  # no trailing newline
                    if got == bs:
                        raise ValueError("last line does not fit in a block")
                    mv[got] = 10
                    got += 1
                cut = got - 1
            else:
                tail = bytes(mv[max(0, got - 4096):got])
                j = tail.rfind(b"\n")
                cut = got - len(tail) + j if j >= 0 else bytes(mv[:got]).rfind(b"\n")
                if cut < 0:
                    raise ValueError(f"no newline in a {bs}-byte block")
            carry = bytes(mv[cut + 1:got])
            item = (i, cut + 1)
            while not stop.is_set():
                try:
                    out_q.put(item, timeout=1)
                    break
                except queue.Full:
                    pass
    except Exception as e:  # propagate to the consumer
        out_q.put(e)
    finally:
        out_q.put(None)


def parse_block(ring, item, column_names, csv_block_size):
    """ring block -> (underlying ChunkedArray, WORK_SCHEMA table); runs in the parse pool."""
    i, length = item
    try:
        table = pacsv.read_csv(
            pa.BufferReader(pa.py_buffer(ring.views[i][:length])),
            read_options=pacsv.ReadOptions(column_names=column_names, use_threads=True,
                                           block_size=csv_block_size),
            convert_options=pacsv.ConvertOptions(column_types=CSV_COLUMN_TYPES,
                                                 include_columns=CSV_COLUMNS),
        )
    finally:
        ring.release(i)
    return transform(table)


def iter_parsed_tables(decomp, block_size, parse_workers):
    """Yield (underlying array, WORK_SCHEMA table) pairs in file order."""
    stream = decomp.stream
    header = b""
    while b"\n" not in header:
        c = stream.read(1 << 16)
        if not c:
            break
        header += c
    if not header.strip():
        return  # empty file
    nl = header.find(b"\n")
    if nl < 0:
        raise ValueError(f"CSV has no newline after header: {header[:200]!r}")
    column_names = header[:nl].decode().strip().split(",")
    missing = [c for c in CSV_COLUMNS if c not in column_names]
    if missing:
        raise ValueError(f"CSV is missing columns {missing}; header: {column_names}")
    initial = header[nl + 1:]  # start of the data, probably ending mid-line

    stop = threading.Event()
    q = queue.Queue(maxsize=4)
    ring = BufferRing(q.maxsize + parse_workers + 3, block_size)
    t = threading.Thread(target=reader_thread, args=(stream, ring, q, stop, initial), daemon=True)
    t.start()
    csv_block_size = 8 << 20
    pending = deque()
    try:
        with ThreadPoolExecutor(parse_workers) as pool:
            eof = False
            while True:
                if not eof and len(pending) < parse_workers + 1:
                    item = q.get()
                    if item is None:
                        eof = True
                    elif isinstance(item, Exception):
                        raise item
                    else:
                        pending.append(pool.submit(parse_block, ring, item, column_names, csv_block_size))
                        continue
                if not pending:
                    break
                yield pending.popleft().result()
    finally:
        stop.set()
        for f in pending:
            f.cancel()


# ----------------------------------------------------------------------------
# transform: OPRA ticker -> (underlying, expiry, type, strike)
# ----------------------------------------------------------------------------

def transform(table):
    """CSV table -> (underlying ChunkedArray, WORK_SCHEMA table). Rows whose
    ticker does not parse are dropped."""
    tickers = table.column("ticker")
    uniq = pc.unique(tickers)
    unds, exps, typs, strikes = [], [], [], []
    for t in uniq.to_pylist():
        m = TICKER_RE.match(t) if t is not None else None
        if m:
            unds.append(m.group(1))
            exps.append(int(m.group(2)))
            typs.append(m.group(3))
            strikes.append(int(m.group(4)) / 1000.0)
        else:
            unds.append(None)
            exps.append(None)
            typs.append(None)
            strikes.append(None)
    idx = pc.index_in(tickers, uniq)
    und = pc.take(pa.array(unds, pa.string()), idx)
    columns = [
        pc.take(pa.array(exps, pa.int32()), idx),
        pc.take(pa.array(typs, pa.string()), idx),
        pc.take(pa.array(strikes, pa.float64()), idx),
        table.column("sip_timestamp"),
    ] + [table.column(c) for c in QUOTE_COLUMNS]
    out = pa.table(columns, schema=WORK_SCHEMA)
    if None in unds:
        mask = pc.is_valid(und)
        out = out.filter(mask)
        und = und.filter(mask)
    return und, out


def split_by_underlying(und, table):
    """Yield (underlying, sub-table) in order of first appearance."""
    uniq = pc.unique(und)
    if len(uniq) == 1:
        yield uniq[0].as_py(), table
        return
    for u in uniq.to_pylist():
        yield u, table.filter(pc.equal(und, u))


# ----------------------------------------------------------------------------
# output: one Sink per underlying
# ----------------------------------------------------------------------------

def safe_name(s):
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)


def parquet_writer(path, parquet_opts):
    return pq.ParquetWriter(path, OUT_SCHEMA, **parquet_opts)


def make_parquet_opts(compression, zstd_level):
    if compression == "snappy":
        return {"compression": "snappy"}
    # Timestamps are ~2/3 of the file; delta encoding + zstd roughly halves
    # them. Prices/sizes compress better plain+zstd than dictionary-encoded.
    return {
        "compression": "zstd",
        "compression_level": zstd_level,
        "column_encoding": {"sip_timestamp": "DELTA_BINARY_PACKED"},
        "use_dictionary": ["expiry", "type", "strike", "ask_exchange", "bid_exchange"],
    }


class WriterPool:
    """Parquet encoding is single-threaded in pyarrow (~9 M rows/s), slower
    than the parse pipeline, so files are encoded on a small pool of threads.
    Each output file is pinned to one thread (row groups must be written in
    order); a new file goes to the thread with the least queued rows, so a
    huge underlying (SPXW) keeps encoding while the next files use the other
    threads. The main thread only routes batches."""

    def __init__(self, parquet_opts, n_threads, max_queued=4):
        self.parquet_opts = parquet_opts
        self.queues = [queue.Queue(maxsize=max_queued) for _ in range(n_threads)]
        self.queued_rows = [0] * n_threads
        self.assigned = {}
        self.error = None
        self.lock = threading.Lock()
        self.threads = [threading.Thread(target=self._run, args=(i,), daemon=True)
                        for i in range(n_threads)]
        for t in self.threads:
            t.start()

    def write(self, path, table):
        self._check()
        with self.lock:
            i = self.assigned.get(path)
            if i is None:
                i = self.assigned[path] = min(range(len(self.queues)), key=lambda k: self.queued_rows[k])
            self.queued_rows[i] += table.num_rows
        self.queues[i].put(("write", path, table))

    def close_file(self, path):
        self._check()
        with self.lock:
            i = self.assigned.pop(path, None)
        if i is None:  # nothing was written: create an empty file
            parquet_writer(path, self.parquet_opts).close()
            return
        self.queues[i].put(("close", path, None))

    def _check(self):
        if self.error is not None:
            raise RuntimeError("parquet writer thread failed") from self.error

    def _run(self, i):
        q = self.queues[i]
        writers = {}
        try:
            while True:
                op, path, table = q.get()
                if op == "stop":
                    break
                if op == "write":
                    w = writers.get(path)
                    if w is None:
                        w = writers[path] = parquet_writer(path, self.parquet_opts)
                    w.write_table(table, row_group_size=ROW_GROUP_ROWS)
                    with self.lock:
                        self.queued_rows[i] -= table.num_rows
                else:
                    w = writers.pop(path, None)
                    if w is not None:
                        w.close()
        except Exception as e:
            self.error = e
            while True:  # drain so producers never block forever
                try:
                    if q.get_nowait()[0] == "stop":
                        break
                except queue.Empty:
                    break
        finally:
            for w in writers.values():
                try:
                    w.close()
                except Exception:
                    pass

    def finish(self):
        for q in self.queues:
            q.put(("stop", None, None))
        for t in self.threads:
            t.join()
        self._check()


class Sink:
    """Collects all rows of one underlying and writes them, sorted by
    sip_timestamp, to a single parquet file. Small underlyings are sorted in
    memory; big ones are spilled into time buckets on local disk first."""

    def __init__(self, underlying, path, spill_root, spill_rows, day_start_ns, writer):
        self.u = underlying
        self.path = path
        self.writer = writer
        self.spill_root = spill_root
        self.spill_rows = spill_rows
        self.day_start_ns = day_start_ns
        self.tables = []
        self.rows = 0
        self.spill_dir = None
        self.writers = None
        self.edges = None

    def add(self, table):
        self.rows += table.num_rows
        if self.writers is None:
            self.tables.append(table)
            if self.rows > self.spill_rows:
                self._start_spill()
        else:
            self._spill(table)

    def _start_spill(self):
        self.spill_dir = os.path.join(self.spill_root, safe_name(self.u))
        if os.path.exists(self.spill_dir):
            shutil.rmtree(self.spill_dir)
        os.makedirs(self.spill_dir)
        self.writers = [None] * N_BUCKETS
        tables, self.tables = self.tables, []
        day_start = self.day_start_ns
        if day_start is None:
            first = min(pc.min(t.column(TS_INDEX)).as_py() for t in tables if t.num_rows)
            day_start = (first // HOUR_NS) * HOUR_NS
        self.edges = day_start + BUCKET_NS * np.arange(1, N_BUCKETS, dtype=np.int64)
        for t in tables:
            self._spill(t)

    def _spill(self, table):
        if table.num_rows == 0:
            return
        table = table.sort_by("sip_timestamp")
        ts = table.column(TS_INDEX).to_numpy()
        cuts = np.searchsorted(ts, self.edges)
        starts = np.concatenate(([0], cuts))
        ends = np.concatenate((cuts, [len(ts)]))
        for k in np.flatnonzero(ends > starts):
            w = self.writers[k]
            if w is None:
                w = self.writers[k] = pa.ipc.new_stream(
                    os.path.join(self.spill_dir, f"{k:03d}.arrow"), WORK_SCHEMA, options=IPC_OPTIONS)
            w.write_table(table.slice(int(starts[k]), int(ends[k] - starts[k])))

    def finalize(self):
        """Sort and hand the rows to the writer pool. Returns the row count."""
        if self.writers is None:
            if self.tables:
                self._write(pa.concat_tables(self.tables).sort_by("sip_timestamp"))
        else:
            for w in self.writers:
                if w is not None:
                    w.close()
            # Buckets are time-partitioned, so sorted(bucket k) followed by
            # sorted(bucket k+1) is globally sorted; the tail that does not
            # fill a row group is carried into the next bucket so row
            # groups stay full-sized.
            carry = None
            for k, w in enumerate(self.writers):
                if w is None:
                    continue
                p = os.path.join(self.spill_dir, f"{k:03d}.arrow")
                with pa.OSFile(p, "rb") as f:
                    bucket = pa.ipc.open_stream(f).read_all().sort_by("sip_timestamp")
                os.remove(p)
                if carry is not None:
                    bucket = pa.concat_tables([carry, bucket])
                n_full = (bucket.num_rows // ROW_GROUP_ROWS) * ROW_GROUP_ROWS
                if n_full:
                    self._write(bucket.slice(0, n_full))
                carry = bucket.slice(n_full) if bucket.num_rows > n_full else None
            if carry is not None:
                self._write(carry)
            shutil.rmtree(self.spill_dir, ignore_errors=True)
        self.writer.close_file(self.path)
        self.tables = None
        self.writers = None
        return self.rows

    def _write(self, table):
        table = table.set_column(TS_INDEX, OUT_SCHEMA.field(TS_INDEX),
                                 pc.cast(table.column(TS_INDEX), NY_TS))
        self.writer.write(self.path, table)


CONTRACT_SORT_KEYS = [("expiry", "ascending"), ("type", "ascending"),
                      ("strike", "ascending"), ("sip_timestamp", "ascending")]
FLUSH_ROWS = 4 * ROW_GROUP_ROWS


def contract_keys(table):
    """int64 key identifying the contract of each row: (expiry, type, strike)."""
    expiry = table.column("expiry").to_numpy().astype(np.int64)
    is_put = pc.equal(table.column("type"), "P").to_numpy(zero_copy_only=False)
    strike = np.rint(table.column("strike").to_numpy() * 1000).astype(np.int64)
    return (expiry * 2 + is_put) * 10**8 + strike


class ContractSink:
    """Writes one underlying in contract order: rows grouped by
    (expiry, type, strike), time-ordered within each contract. The flat files
    already come in this order, so this is a streaming write with no sorting
    and no spill; the order is verified per flush and repaired with a sort if
    it is ever violated. Because the file is globally ordered by contract,
    each row group covers a narrow (expiry, type, strike) range and parquet
    row-group statistics let readers skip straight to an expiry or strike."""

    def __init__(self, underlying, path, writer):
        self.u = underlying
        self.path = path
        self.writer = writer
        self.tables = []
        self.buffered = 0
        self.rows = 0
        self.sorted_flushes = 0

    def add(self, table):
        if table.num_rows == 0:
            return
        self.tables.append(table)
        self.buffered += table.num_rows
        self.rows += table.num_rows
        if self.buffered >= FLUSH_ROWS:
            self._flush(final=False)

    def _flush(self, final):
        table = pa.concat_tables(self.tables) if len(self.tables) > 1 else self.tables[0]
        n = table.num_rows
        keys = contract_keys(table)
        if final:
            write_len = n
        else:
            # Hold back the trailing contract (it may continue in the next
            # block) and any partial row group.
            not_last = np.flatnonzero(keys != keys[-1])
            complete = not_last[-1] + 1 if len(not_last) else 0
            write_len = (complete // ROW_GROUP_ROWS) * ROW_GROUP_ROWS
            if write_len == 0:
                self.tables = [table]
                return
        out = table.slice(0, write_len)
        keys = keys[:write_len]
        ts = out.column(TS_INDEX).to_numpy()
        boundary = keys[1:] != keys[:-1]
        run_starts = keys[np.concatenate(([True], boundary))]
        contiguous = len(np.unique(run_starts)) == len(run_starts)
        time_ordered = not ((ts[1:] < ts[:-1]) & ~boundary).any()
        if not (contiguous and time_ordered):
            out = out.sort_by(CONTRACT_SORT_KEYS)
            self.sorted_flushes += 1
        out = out.set_column(TS_INDEX, OUT_SCHEMA.field(TS_INDEX),
                             pc.cast(out.column(TS_INDEX), NY_TS))
        self.writer.write(self.path, out)
        rest = table.slice(write_len)
        self.tables = [rest] if rest.num_rows else []
        self.buffered = rest.num_rows

    def suspend(self):
        """Flush everything buffered but keep the file open: the underlying
        will continue in a later input partition."""
        if self.tables:
            self._flush(final=True)

    def finalize(self):
        if self.tables:
            self._flush(final=True)
        self.writer.close_file(self.path)
        if self.sorted_flushes:
            print(f"  WARNING {self.u}: input was not in contract order, {self.sorted_flushes} flushes sorted",
                  flush=True)
        return self.rows


def merge_segments(paths, out_path, order, spill_root, day_start_ns, parquet_opts, writer):
    """Merge several parquet segments of one underlying into one file. In time
    order this is memory-bounded via the spill path; in contract order (where
    it should never happen) it is done in memory with a full sort."""
    u = os.path.basename(out_path)
    if order == "contract":
        table = pa.concat_tables([pq.read_table(p) for p in paths]).sort_by(CONTRACT_SORT_KEYS)
        pq.write_table(table, out_path, row_group_size=ROW_GROUP_ROWS, **parquet_opts)
        return table.num_rows
    sink = Sink(u, out_path, spill_root, 0, day_start_ns, writer)
    for p in paths:
        pf = pq.ParquetFile(p)
        for batch in pf.iter_batches(batch_size=ROW_GROUP_ROWS):
            t = pa.Table.from_batches([batch])
            t = t.set_column(TS_INDEX, WORK_SCHEMA.field(TS_INDEX),
                             pc.cast(t.column(TS_INDEX), pa.int64()))
            sink.add(t)
    return sink.finalize()


# ----------------------------------------------------------------------------
# per-file driver
# ----------------------------------------------------------------------------

def release_source(path):
    """Free a converted .csv.gz but KEEP a zero-byte file in its place:
    download_flatfiles.py skips any object whose local file exists, so
    unlinking it would make the next download re-fetch ~100 GB."""
    with open(path, "wb"):
        pass


def day_start_ns_for(base_name):
    try:
        d = datetime.datetime.strptime(base_name[:10], "%Y-%m-%d")
    except ValueError:
        return None
    return int(d.replace(tzinfo=NY_TZ).timestamp()) * 10**9


def process_file(input_filename, output_dir, opts):
    base_name = os.path.basename(input_filename).replace(".csv.gz", "")
    date_dir = os.path.join(output_dir, base_name)
    tmp_dir = date_dir + ".tmp"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir)
    spill_root = os.path.join(opts["spill_dir"], f"{base_name}.{os.getpid()}")
    if os.path.exists(spill_root):
        shutil.rmtree(spill_root)
    os.makedirs(spill_root, exist_ok=True)
    day_start = day_start_ns_for(base_name)

    def path_for(u, segment):
        if segment == 0:
            return os.path.join(tmp_dir, f"{base_name}-{u}.parquet")
        return os.path.join(tmp_dir, f"{base_name}-{u}.seg{segment}.parquet")

    t0 = time.time()
    last_report = t0
    total_rows = 0
    n_files = 0
    segments = {}  # underlying -> number of segments written
    non_contiguous = []
    current = None
    pending = None
    finalizer = ThreadPoolExecutor(1)
    parquet_opts = make_parquet_opts(opts["compression"], opts["zstd_level"])
    writer = WriterPool(parquet_opts, opts["writer_threads"])

    def submit_finalize(sink):
        nonlocal pending
        if pending is not None:
            pending.result()
        pending = finalizer.submit(sink.finalize)

    contract_order = opts["order"] == "contract"
    sinks = {}      # contract order: underlying -> open ContractSink (kept across partitions)
    reappeared = set()
    decomp = Decompressor(input_filename, opts["threads"])
    ok = False
    try:
        for und, table in iter_parsed_tables(decomp, opts["block_mb"] << 20, opts["parse_workers"]):
            total_rows += table.num_rows
            for u, part in split_by_underlying(und, table):
                if current is None or u != current.u:
                    if contract_order:
                        if current is not None:
                            current.suspend()
                        current = sinks.get(u)
                        if current is None:
                            current = sinks[u] = ContractSink(u, path_for(u, 0), writer)
                            n_files += 1
                        else:
                            reappeared.add(u)
                    else:
                        if current is not None:
                            submit_finalize(current)
                        seg = segments.get(u, 0)
                        if seg:
                            non_contiguous.append(u)
                        segments[u] = seg + 1
                        n_files += 1
                        current = Sink(u, path_for(u, seg), spill_root, opts["spill_rows"],
                                       day_start, writer)
                current.add(part)
            now = time.time()
            if now - last_report > 60:
                last_report = now
                el = now - t0
                print(f"  {base_name}: {el/60:5.1f} min, {total_rows/1e9:.2f}B rows, "
                      f"{total_rows/1e6/el:.1f}M rows/s, {n_files} underlyings, at {current.u if current else '-'}",
                      flush=True)
        if contract_order:
            for sink in sinks.values():
                sink.finalize()
            segments = {u: 1 for u in sinks}
            if reappeared:
                print(f"  {base_name}: {len(reappeared)} of {len(sinks)} underlyings span several "
                      f"input partitions (appended; contracts stay contiguous)", flush=True)
        else:
            if current is not None:
                submit_finalize(current)
            if pending is not None:
                pending.result()
        writer.finish()
        ok = True
    finally:
        finalizer.shutdown(wait=False)
        if not ok:
            for q in writer.queues:
                q.put(("stop", None, None))
        decomp.close(ok)

    if non_contiguous:
        writer = WriterPool(parquet_opts, opts["writer_threads"])
        merged_paths = []
        for u in sorted(set(non_contiguous)):
            n = segments[u]
            print(f"  {base_name}: WARNING {u} is not contiguous ({n} segments), merging", flush=True)
            paths = [path_for(u, s) for s in range(n)]
            merged = path_for(u, 0) + ".merge"
            merge_segments(paths, merged, opts["order"], spill_root, day_start, parquet_opts, writer)
            merged_paths.append((paths, merged))
        writer.finish()
        for paths, merged in merged_paths:
            for p in paths:
                os.remove(p)
            os.replace(merged, paths[0])

    shutil.rmtree(spill_root, ignore_errors=True)
    os.rename(tmp_dir, date_dir)
    if opts["delete_original"]:
        release_source(input_filename)
    el = time.time() - t0
    return base_name, total_rows, len(segments), el, decomp.name


def raise_fd_limit():
    """Contract order keeps one parquet file per underlying open until the end
    of the input (~6000 files), so make sure the fd limit allows it."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = min(hard, 1 << 17) if hard != resource.RLIM_INFINITY else 1 << 17
        if soft < want:
            resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except (ImportError, ValueError, OSError):
        pass


def worker_main(job):
    input_filename, output_dir, opts = job
    pa.set_cpu_count(opts["threads"])
    raise_fd_limit()
    try:
        return process_file(input_filename, output_dir, opts)
    except Exception as e:
        return input_filename, e, traceback.format_exc()


def main():
    ncpu = os.cpu_count() or 8
    parser = argparse.ArgumentParser(
        description="Stream-convert OPRA option quote CSV.gz files to per-underlying Parquet files.")
    parser.add_argument("input_dir", type=str)
    parser.add_argument("output_dir", type=str)
    parser.add_argument("--workers", type=int, default=3,
                        help="files processed concurrently (default 3: ~5 GB RAM and ~90 MB/s of NFS "
                             "read each; per-file speed is capped by parquet encoding of the giant "
                             "underlyings, so aggregate throughput comes from this)")
    parser.add_argument("--threads", type=int, default=None,
                        help="decompression/parsing threads per file (default: cpus / workers)")
    parser.add_argument("--writer-threads", type=int, default=3,
                        help="parquet encoding threads per file (encoding is ~9M rows/s per thread)")
    parser.add_argument("--parse-workers", type=int, default=2,
                        help="CSV blocks parsed concurrently per file (each block is itself parsed multi-threaded)")
    parser.add_argument("--block-mb", type=int, default=128,
                        help="CSV block size in MB handed to each parse (default 128)")
    parser.add_argument("--order", choices=["contract", "time"], default="contract",
                        help="row order within each output file: 'contract' (default) = grouped by "
                             "(expiry, type, strike), time-ordered within each contract (the input order: "
                             "no sort, no spill, ~no memory; row-group stats let readers skip to an "
                             "expiry/strike); 'time' = globally sorted by sip_timestamp")
    parser.add_argument("--compression", choices=["zstd", "snappy"], default="zstd",
                        help="parquet codec: zstd (default; ~35%% smaller than snappy incl. delta-encoded "
                             "timestamps, same read speed) or snappy (what the old script wrote)")
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--spill-rows", type=int, default=25_000_000,
                        help="underlyings with more rows than this are spilled to --spill-dir (default 25M ~ 1 GB)")
    parser.add_argument("--spill-dir", type=str, default="/tmp/csv2parquet_options_quotes",
                        help="local (non-NFS) scratch dir for spilled buckets; needs ~50 GB per worker")
    parser.add_argument("--delete-original", action="store_true",
                        help="truncate the .csv.gz to zero bytes once its day dir is complete, "
                             "including days converted by an earlier run. The empty file stays so "
                             "download_flatfiles.py does not re-fetch it; the parquet files then "
                             "become the only copy of that data")
    parser.add_argument("--oldest-first", action="store_true",
                        help="process oldest dates first (default newest first)")
    args = parser.parse_args()

    threads = args.threads or max(2, ncpu // args.workers)
    opts = {
        "threads": threads,
        "parse_workers": args.parse_workers,
        "writer_threads": args.writer_threads,
        "block_mb": args.block_mb,
        "order": args.order,
        "compression": args.compression,
        "zstd_level": args.zstd_level,
        "spill_rows": args.spill_rows,
        "spill_dir": args.spill_dir,
        "delete_original": args.delete_original,
    }
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.spill_dir, exist_ok=True)

    csv_files = sorted(glob(os.path.join(args.input_dir, "*.csv.gz")), reverse=not args.oldest_first)
    jobs = []
    for f in csv_files:
        base_name = os.path.basename(f).replace(".csv.gz", "")
        day_dir = os.path.join(args.output_dir, base_name)
        if os.path.exists(day_dir):
            # Already converted. With --delete-original the source is removed
            # here as well: the day is skipped before any processing, so a
            # rerun would otherwise never delete a source converted earlier.
            if args.delete_original and os.path.getsize(f) > 0:
                n = len(glob(os.path.join(day_dir, "*.parquet")))
                if n >= MIN_PARQUET_FILES:
                    print(f"{base_name}: already converted ({n} files), freeing {f}", flush=True)
                    release_source(f)
                else:
                    print(f"{base_name}: already converted but only {n} parquet files, "
                          f"keeping {f}", flush=True)
            continue
        jobs.append((f, args.output_dir, opts))
    print(f"{len(jobs)} of {len(csv_files)} files to process, {args.workers} workers x {threads} threads",
          flush=True)

    failures = 0
    t0 = time.time()

    def report(res):
        nonlocal failures
        if isinstance(res[1], Exception):
            failures += 1
            print(f"FAILED {res[0]}: {res[1]!r}\n{res[2]}", flush=True)
        else:
            base_name, rows, n_und, el, dec = res
            print(f"DONE {base_name}: {rows/1e9:.2f}B rows, {n_und} underlyings, "
                  f"{el/60:.1f} min ({rows/1e6/el:.1f}M rows/s, {dec}); total {(time.time()-t0)/60:.1f} min",
                  flush=True)

    if args.workers <= 1:
        for job in jobs:
            report(worker_main(job))
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(args.workers) as pool:
            for res in pool.imap_unordered(worker_main, jobs):
                report(res)

    print(f"Processing complete ({failures} failures). Parquet files saved in {args.output_dir}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
