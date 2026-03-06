#!/usr/bin/env python3
"""
Read the past N calendar days of day_aggs (YYYY-MM-DD.parquet per day; missing
non-trading days are expected) and write a single Parquet indexed by (date, ticker).
Date is derived from window_start; no separate date column is added.
"""
import os
import argparse
import pandas as pd
from glob import glob
from tqdm import tqdm
from datetime import datetime, timedelta
import concurrent.futures

NY_TZ = "America/New_York"


def get_recent_files(input_dir, period_days):
    """
    Finds Parquet files with YYYY-MM-DD.parquet names in input_dir and returns
    those within the most recent period_days (calendar days), sorted oldest first.
    """
    all_files = glob(os.path.join(input_dir, "*.parquet"))
    files_with_date = []
    for file in all_files:
        basename = os.path.basename(file)
        date_str = basename.replace(".parquet", "")
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            files_with_date.append((file, file_date))
        except ValueError:
            continue
    if not files_with_date:
        return []
    max_date = max(d for _, d in files_with_date)
    cutoff = max_date - timedelta(days=period_days)
    recent = [f for f, d in files_with_date if d >= cutoff]
    recent.sort(key=lambda f: datetime.strptime(os.path.basename(f).replace(".parquet", ""), "%Y-%m-%d"))
    return recent


def read_file(file):
    """Read one day's Parquet and return DataFrame with (ticker, window_start) normalized to NY time."""
    try:
        df = pd.read_parquet(file)
        if isinstance(df.index, pd.MultiIndex) and set(["ticker", "window_start"]).issubset(df.index.names):
            win = df.index.get_level_values("window_start")
            if win.tz is None:
                win = pd.to_datetime(win).tz_localize(NY_TZ, ambiguous="infer", nonexistent="shift_forward")
            else:
                win = win.tz_convert(NY_TZ)
            df.index = pd.MultiIndex.from_arrays(
                [df.index.get_level_values("ticker"), win], names=["ticker", "window_start"]
            )
        else:
            if df.index.name == "window_start" and "ticker" in df.columns:
                df = df.reset_index()
            if "ticker" not in df.columns or "window_start" not in df.columns:
                return None
            df["window_start"] = pd.to_datetime(df["window_start"])
            if df["window_start"].dt.tz is None:
                df["window_start"] = df["window_start"].dt.tz_localize(NY_TZ, ambiguous="infer", nonexistent="shift_forward")
            else:
                df["window_start"] = df["window_start"].dt.tz_convert(NY_TZ)
            df = df.set_index(["ticker", "window_start"])
        return df
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Build a single day_aggs Parquet from the last N calendar days, indexed by (date, ticker)."
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing YYYY-MM-DD.parquet day_aggs (e.g. day_aggs or us_stocks_sip/day_aggs).",
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=730,
        metavar="N",
        help="Number of calendar days to include (default: 730). Non-trading days may be missing in filenames.",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output Parquet path (default: <input_dir>/last<N>.parquet).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Max workers for reading files (default: CPU count).",
    )
    args = parser.parse_args()

    files = get_recent_files(args.input_dir, args.recent_days)
    if not files:
        print("No day_aggs files found in the given window.")
        return 1

    out_path = args.output or os.path.join(args.input_dir, f"last{args.recent_days}.parquet")

    print(f"Reading {len(files)} day_aggs files (last {args.recent_days} calendar days)...")
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        dfs = list(tqdm(executor.map(read_file, files), total=len(files), desc="Reading"))
    dfs = [d for d in dfs if d is not None]
    if not dfs:
        print("No data read.")
        return 1

    combined = pd.concat(dfs)
    # Index by (date, ticker); date from window_start (no extra column)
    combined = combined.reset_index()
    combined = combined.dropna(subset=["ticker"])
    combined["date"] = pd.to_datetime(combined["window_start"]).dt.date
    combined = combined.drop(columns=["window_start"])
    combined = combined.set_index(["date", "ticker"]).sort_index()
    combined.to_parquet(out_path, compression="snappy")
    print(f"Wrote {out_path} ({len(combined):,} rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
