#!/usr/bin/env python3
"""Compact legacy per-ticker stock TQ files into one parquet file per date."""
import argparse
import concurrent.futures
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

NUMERIC = ("last", "last_size", "volume", "bid", "bid_size", "ask", "ask_size")
SCHEMA = pa.schema([
    pa.field("ticker", pa.string()),
    pa.field("window_start", pa.timestamp("ns", tz="America/New_York")),
    *(pa.field(column, pa.float64()) for column in NUMERIC),
])


def compact_date(date_dir, output_dir, compression):
    date_dir = Path(date_dir)
    date = date_dir.name
    output = Path(output_dir) / f"{date}.parquet"
    if output.exists():
        return date, "current", 0
    temporary = output.with_suffix(".parquet.tmp")
    output.parent.mkdir(parents=True, exist_ok=True)
    writer, rows = None, 0
    try:
        for source in sorted(date_dir.glob(f"{date}-*.parquet")):
            ticker = source.name[len(date) + 1:-8]
            try:
                frame = pd.read_parquet(source)
                if "window_start" not in frame.columns:
                    frame = frame.reset_index()
                if "window_start" not in frame.columns:
                    raise ValueError("missing window_start")
                frame["ticker"] = ticker
                frame["window_start"] = pd.to_datetime(frame["window_start"])
                for column in NUMERIC:
                    frame[column] = frame[column].astype("float64")
                table = pa.Table.from_pandas(
                    frame[["ticker", "window_start", *NUMERIC]], preserve_index=False
                ).cast(SCHEMA, safe=False)
                if writer is None:
                    writer = pq.ParquetWriter(temporary, SCHEMA, compression=compression)
                writer.write_table(table)
                rows += len(frame)
            except Exception as exc:
                print(f"{date} {ticker}: {exc}", flush=True)
    finally:
        if writer is not None:
            writer.close()
    if rows:
        os.replace(temporary, output)
        return date, "written", rows
    temporary.unlink(missing_ok=True)
    return date, "empty", 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default="us_stocks_sip/tq_aggs")
    parser.add_argument("--output-dir", default="us_stocks_sip/tq_aggs_v2")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--compression", default="zstd")
    args = parser.parse_args()
    dates = sorted(entry.path for entry in os.scandir(args.input_dir)
                   if entry.is_dir() and len(entry.name) == 10
                   and entry.name[4] == "-" and entry.name[7] == "-")
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(compact_date, date, args.output_dir, args.compression)
                   for date in dates]
        for future in concurrent.futures.as_completed(futures):
            date, status, rows = future.result()
            print(f"{date}: {status} ({rows:,} rows)", flush=True)


if __name__ == "__main__":
    main()
