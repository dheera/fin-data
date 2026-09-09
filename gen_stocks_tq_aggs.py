#!/usr/bin/env python3
"""Write stock trade/quote aggregates as one Parquet file per date.

The old per-ticker layout created tens of millions of NFS-hostile small files.
Each output now contains ticker and window_start columns at
``OUTPUT_DIR/YYYY-MM-DD.parquet``.
"""
import argparse
import concurrent.futures
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

QUOTE_COLUMNS = ["sip_timestamp", "bid_price", "bid_size", "ask_price", "ask_size"]
TRADE_COLUMNS = ["sip_timestamp", "price", "size"]


def aggregate_symbol(quote_path, trade_path, ticker, interval):
    quotes = pd.read_parquet(quote_path, columns=QUOTE_COLUMNS).rename(
        columns={"bid_price": "bid", "ask_price": "ask"})
    quotes["sip_timestamp"] = pd.to_datetime(quotes["sip_timestamp"])
    quotes = quotes.set_index("sip_timestamp").resample(interval).last()
    quotes = quotes[["bid", "bid_size", "ask", "ask_size"]]
    trades = pd.read_parquet(trade_path, columns=TRADE_COLUMNS)
    trades["sip_timestamp"] = pd.to_datetime(trades["sip_timestamp"])
    trades = trades.set_index("sip_timestamp").resample(interval).agg(
        last=("price", "last"), last_size=("size", "last"), volume=("size", "sum"))
    frame = pd.concat((trades, quotes), axis=1).sort_index().ffill()
    valid = (frame["last"].notna() & frame["bid"].notna() & frame["bid"].ne(0)
             & frame["ask"].notna() & frame["ask"].ne(0)
             & frame["bid_size"].notna() & frame["bid_size"].ne(0)
             & frame["ask_size"].notna() & frame["ask_size"].ne(0))
    if not valid.any():
        return None
    frame = frame.loc[valid.idxmax():].reset_index(names="window_start")
    frame.insert(0, "ticker", ticker)
    # Parquet row groups in a date file must share one schema.  Input feeds
    # vary between integer and floating sizes/volumes, so normalize all quote
    # and trade numeric fields before handing frames to the common writer.
    for column in ("last", "last_size", "volume", "bid", "bid_size", "ask", "ask_size"):
        frame[column] = frame[column].astype("float64")
    return frame


def process_date(date_path, trades_root, output_root, interval, compression, force, symbol_workers):
    quote_dir = Path(date_path)
    date = quote_dir.name
    output = Path(output_root) / f"{date}.parquet"
    inputs, newest = [], 0.0
    for quote in sorted(quote_dir.glob(f"{date}-*.parquet")):
        ticker = quote.name[len(date) + 1:-8]
        trade = Path(trades_root) / date / f"{date}-{ticker}.parquet"
        if trade.exists():
            newest = max(newest, quote.stat().st_mtime, trade.stat().st_mtime)
            inputs.append((quote, trade, ticker))
    if not inputs:
        return date, "empty", 0
    if not force and output.exists() and output.stat().st_mtime >= newest:
        return date, "current", 0
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".parquet.tmp")
    writer, rows = None, 0
    try:
        # Threads let Arrow/Pandas read and aggregate several symbols at once,
        # while this process remains the sole writer of the date-level file.
        with concurrent.futures.ThreadPoolExecutor(max_workers=symbol_workers) as pool:
            futures = {
                pool.submit(aggregate_symbol, quote, trade, ticker, interval): ticker
                for quote, trade, ticker in inputs
            }
            for future in concurrent.futures.as_completed(futures):
                ticker = futures[future]
                try:
                    frame = future.result()
                except Exception as exc:
                    print(f"{date} {ticker}: {exc}", flush=True)
                    continue
                if frame is None or frame.empty:
                    continue
                table = pa.Table.from_pandas(frame, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(temporary, table.schema, compression=compression)
                writer.write_table(table)
                rows += len(frame)
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
    parser.add_argument("--quotes-dir", default="us_stocks_sip/quotes")
    parser.add_argument("--trades-dir", default="us_stocks_sip/trades")
    parser.add_argument("--output-dir", default="us_stocks_sip/tq_aggs")
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--symbol-workers", type=int, default=4,
                        help="Concurrent symbol reads within each date worker")
    parser.add_argument("--days", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--compression", default="zstd")
    args = parser.parse_args()
    dates = sorted((entry.path for entry in os.scandir(args.quotes_dir)
                    if entry.is_dir() and len(entry.name) == 10
                    and entry.name[4] == "-" and entry.name[7] == "-"), reverse=True)
    if args.start_date:
        dates = [path for path in dates if Path(path).name >= args.start_date]
    if args.end_date:
        dates = [path for path in dates if Path(path).name <= args.end_date]
    if args.days is not None:
        dates = dates[:args.days]
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(process_date, date, args.trades_dir, args.output_dir,
                                   f"{args.interval}s", args.compression, args.force,
                                   args.symbol_workers)
                   for date in dates]
        for future in concurrent.futures.as_completed(futures):
            date, status, rows = future.result()
            print(f"{date}: {status} ({rows:,} rows)", flush=True)


if __name__ == "__main__":
    main()
