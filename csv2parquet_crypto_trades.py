#!/usr/bin/env python3

# Converts polygon global crypto trades CSV.gz flat files to one Parquet file per day.
#
# ./csv2parquet_crypto_trades.py global_crypto/trades_v1 global_crypto/trades [--workers 4] [--delete-original]
#
# Output: <out_dir>/<date>.parquet, rows ordered by (ticker, participant_timestamp).
# The flat files already arrive in that order (verified), so normally nothing is
# sorted; if a file is not in order it is sorted in memory (stable) instead.
#
# Schema:
#   ticker                 string   e.g. "X:BTC-USD" (same form as global_crypto/*_aggs)
#   participant_timestamp  timestamp[ns, tz=UTC]
#   price                  double
#   size                   double   (fractional coin amounts)
#   exchange               int16    polygon crypto exchange id (1 Coinbase, 2 Bitfinex, 6 Bitstamp, 23 Kraken, ...)
#   conditions             int8     1 = sell side, 2 = buy side (aggressor); null if absent
#   id                     int64    exchange trade id
#
# zstd, delta-encoded timestamps, dictionary-encoded ticker/exchange/conditions,
# 1M-row row groups. Because rows are ticker-ordered, a filter on ticker prunes row
# groups via min/max stats:  pd.read_parquet(path, filters=[("ticker", "==", "X:BTC-USD")])
#
# Each day is written to <date>.parquet.tmp and renamed into place after the row
# count is verified, so an existing <date>.parquet means done.

import os
import sys
import gzip
import argparse
import subprocess
import concurrent.futures
from glob import glob

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

CSV_COLUMNS = ["ticker", "conditions", "exchange", "id", "participant_timestamp", "price", "size"]
CSV_TYPES = {
    "ticker": pa.string(),
    "conditions": pa.int8(),
    "exchange": pa.int16(),
    "id": pa.int64(),
    "participant_timestamp": pa.int64(),
    "price": pa.float64(),
    "size": pa.float64(),
}
UTC_TS = pa.timestamp("ns", tz="UTC")
OUT_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("participant_timestamp", UTC_TS),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("exchange", pa.int16()),
    ("conditions", pa.int8()),
    ("id", pa.int64()),
])
SORT_KEYS = [("ticker", "ascending"), ("participant_timestamp", "ascending")]
ROW_GROUP_ROWS = 1_000_000


def read_csv_gz(path, column_types):
    """Read a whole CSV.gz into an Arrow table. Uses ISA-L igzip in a subprocess when
    available (~4x faster than Python gzip), else gzip.open. Raises on truncated input."""
    read_opts = pacsv.ReadOptions(block_size=1 << 26)
    conv_opts = pacsv.ConvertOptions(column_types=column_types, strings_can_be_null=False)
    try:
        import isal.igzip  # noqa: F401
    except ImportError:
        with gzip.open(path, "rb") as f:
            return pacsv.read_csv(f, read_options=read_opts, convert_options=conv_opts)
    proc = subprocess.Popen([sys.executable, "-m", "isal.igzip", "-d", "-c", path],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    table, parse_error = None, None
    try:
        table = pacsv.read_csv(proc.stdout, read_options=read_opts, convert_options=conv_opts)
    except pa.ArrowInvalid as e:
        parse_error = e
    finally:
        proc.stdout.close()
        err = proc.stderr.read()
        proc.wait()
    # A truncated gzip usually surfaces first as a ragged last CSV line; report the
    # decompressor failure instead, since that is the real cause.
    if proc.returncode != 0:
        last = (err.decode(errors="replace").strip().splitlines() or [""])[-1]
        raise EOFError(f"igzip exited {proc.returncode}: {last}")
    if parse_error is not None:
        raise parse_error
    return table


def is_sorted(table, ticker_col, ts_col):
    """True if rows are ordered by (ticker, timestamp)."""
    if table.num_rows < 2:
        return True
    tk = table.column(ticker_col).combine_chunks()
    ts = table.column(ts_col).combine_chunks()
    prev_tk, next_tk = tk.slice(0, len(tk) - 1), tk.slice(1)
    if pc.any(pc.less(next_tk, prev_tk)).as_py():
        return False
    same = pc.equal(next_tk, prev_tk)
    back = pc.and_(same, pc.less(ts.slice(1), ts.slice(0, len(ts) - 1)))
    return not pc.any(back).as_py()


def parquet_opts(zstd_level):
    return dict(
        compression="zstd",
        compression_level=zstd_level,
        use_dictionary=["ticker", "exchange", "conditions"],
        column_encoding={"participant_timestamp": "DELTA_BINARY_PACKED"},
        sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1)],
    )


def process_file(csv_file, out_dir, zstd_level):
    date_str = os.path.basename(csv_file).split(".")[0]
    final_path = os.path.join(out_dir, f"{date_str}.parquet")
    tmp_path = final_path + ".tmp"

    try:
        raw = read_csv_gz(csv_file, CSV_TYPES)
    except (EOFError, gzip.BadGzipFile, OSError) as e:
        return f"SKIP truncated/corrupt gzip {csv_file}: {e}"
    except pa.ArrowInvalid as e:
        if "Empty CSV file" in str(e):
            return f"SKIP empty {csv_file}"
        raise

    if raw.column_names != CSV_COLUMNS:
        raise ValueError(f"{csv_file}: unexpected columns {raw.column_names}")

    table = pa.table({
        "ticker": raw.column("ticker"),
        "participant_timestamp": pc.cast(raw.column("participant_timestamp"), UTC_TS),
        "price": raw.column("price"),
        "size": raw.column("size"),
        "exchange": raw.column("exchange"),
        "conditions": raw.column("conditions"),
        "id": raw.column("id"),
    }, schema=OUT_SCHEMA)
    del raw

    note = ""
    if not is_sorted(table, "ticker", "participant_timestamp"):
        table = table.sort_by(SORT_KEYS)
        note = " (input was unsorted; sorted)"

    pq.write_table(table, tmp_path, row_group_size=ROW_GROUP_ROWS, **parquet_opts(zstd_level))
    written = pq.ParquetFile(tmp_path).metadata.num_rows
    if written != table.num_rows:
        os.remove(tmp_path)
        raise RuntimeError(f"{tmp_path}: wrote {written} rows, expected {table.num_rows}")
    os.replace(tmp_path, final_path)
    return f"Saved {final_path}: {table.num_rows:,} rows{note}"


def main():
    parser = argparse.ArgumentParser(
        description="Convert crypto trade CSV.gz flat files to one ticker-ordered Parquet file per day.")
    parser.add_argument("in_dir", help="Dir of input <date>.csv.gz files (e.g. global_crypto/trades_v1)")
    parser.add_argument("out_dir", help="Output dir for <date>.parquet files (e.g. global_crypto/trades)")
    parser.add_argument("--delete-original", action="store_true",
                        help="Once a day's parquet exists, truncate its CSV.gz to 0 bytes "
                             "(the empty file keeps download_flatfiles.py from refetching it)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel days (default 4; ~1 GB RAM each)")
    parser.add_argument("--zstd-level", type=int, default=3)
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    for stale in glob(os.path.join(args.out_dir, "*.parquet.tmp")):
        os.remove(stale)

    todo = []
    for csv_file in sorted(glob(os.path.join(args.in_dir, "*.csv.gz")), reverse=True):
        if os.path.getsize(csv_file) == 0:
            continue
        date_str = os.path.basename(csv_file).split(".")[0]
        if os.path.exists(os.path.join(args.out_dir, f"{date_str}.parquet")):
            if args.delete_original:
                print(f"Deleting original {csv_file}")
                os.remove(csv_file)
                open(csv_file, "a").close()
            continue
        todo.append(csv_file)

    failures = skipped = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_file, f, args.out_dir, args.zstd_level): f for f in todo}
        for fut in tqdm(concurrent.futures.as_completed(futures), total=len(futures), unit="day"):
            try:
                msg = fut.result()
                skipped += msg.startswith("SKIP")
                tqdm.write(msg)
            except Exception as exc:
                failures += 1
                tqdm.write(f"FAILED {futures[fut]}: {exc!r}")

    print(f"Done: {len(todo) - failures - skipped}/{len(todo)} days converted into {args.out_dir} "
          f"({skipped} skipped, {failures} failed)")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
