#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm
from datetime import datetime, timedelta
import concurrent.futures

# New York timezone identifier
NY_TZ = "America/New_York"

def get_recent_files(input_dir, period_days=365):
    """
    Finds all Parquet files in input_dir whose filenames follow the "YYYY-MM-DD.parquet" format,
    and returns a chronologically sorted list of files whose dates are within the most recent period_days.
    """
    all_files = glob(os.path.join(input_dir, "*.parquet"))
    files_with_date = []
    for file in all_files:
        basename = os.path.basename(file)
        date_str = basename.replace(".parquet", "")
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            files_with_date.append((file, file_date))
        except Exception:
            print(f"Skipping file {file}: cannot parse date from filename.")
    if not files_with_date:
        return []
    # Determine cutoff based on the maximum date in the filenames
    max_date = max(date for _, date in files_with_date)
    cutoff = max_date - timedelta(days=period_days)
    recent_files = [file for file, date in files_with_date if date >= cutoff]
    # Sort files in chronological order (oldest first)
    recent_files.sort(key=lambda f: datetime.strptime(os.path.basename(f).replace(".parquet", ""), "%Y-%m-%d"))
    return recent_files

def get_latest_window_starts(output_dir):
    """
    For each per-ticker Parquet file in output_dir, read its index (assumed to be 'window_start')
    and return a dictionary {ticker: latest_window_start}. This is used to skip duplicates.
    """
    latest_windows = {}
    existing_files = glob(os.path.join(output_dir, "*.parquet"))
    for file in existing_files:
        ticker = os.path.basename(file).replace(".parquet", "")
        try:
            # Read only the 'window_start' column for efficiency
            df = pd.read_parquet(file, columns=["window_start"])
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        if not df.empty:
            latest_windows[ticker] = df.index.max()
    return latest_windows

def read_file(file):
    """
    Reads a Parquet file and ensures that the DataFrame is indexed by ['ticker', 'window_start'].
    Handles cases where the file already has window_start as the index with ticker as a column.
    Also converts the 'window_start' timestamps to New York time.
    """
    try:
        df = pd.read_parquet(file)
        # Case 1: Already a MultiIndex with both 'ticker' and 'window_start'
        if isinstance(df.index, pd.MultiIndex) and set(["ticker", "window_start"]).issubset(df.index.names):
            win_vals = df.index.get_level_values("window_start")
            if win_vals.tz is None:
                win_vals = pd.to_datetime(win_vals).tz_localize(NY_TZ, ambiguous='infer', nonexistent='shift_forward')
            else:
                win_vals = win_vals.tz_convert(NY_TZ)
            df.index = pd.MultiIndex.from_arrays(
                [df.index.get_level_values("ticker"), win_vals],
                names=["ticker", "window_start"]
            )
        else:
            # If the index is named "window_start" and "ticker" is a column, reset it.
            if df.index.name == "window_start" and "ticker" in df.columns:
                df = df.reset_index()
            # Now ensure both 'ticker' and 'window_start' are present as columns.
            if "ticker" not in df.columns or "window_start" not in df.columns:
                raise KeyError("Missing required columns 'ticker' and/or 'window_start'")
            df["window_start"] = pd.to_datetime(df["window_start"])
            if df["window_start"].dt.tz is None:
                df["window_start"] = df["window_start"].dt.tz_localize(NY_TZ, ambiguous='infer', nonexistent='shift_forward')
            else:
                df["window_start"] = df["window_start"].dt.tz_convert(NY_TZ)
            df = df.set_index(["ticker", "window_start"])
        return df
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return None

def process_ticker(task):
    """
    For a given ticker, combines new rows (if any) with an existing per-ticker Parquet file.
    Writes the combined DataFrame back to disk using snappy compression.
    """
    ticker, group, output_dir, latest_window = task
    # Remove ticker from the index so that only window_start remains
    group = group.reset_index(level=0, drop=True)
    # Skip rows already present (based on window_start)
    if latest_window is not None:
        new_group = group[group.index > latest_window]
    else:
        new_group = group

    if new_group.empty:
        return None

    output_path = os.path.join(output_dir, f"{ticker}.parquet")
    if os.path.exists(output_path):
        try:
            existing_df = pd.read_parquet(output_path)
            combined_df = pd.concat([existing_df, new_group])
            combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
            combined_df = combined_df.sort_index()
        except Exception as e:
            print(f"Error processing existing file {output_path}: {e}")
            combined_df = new_group
    else:
        combined_df = new_group

    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, output_path, compression="snappy")
    return ticker

def process_aggs(input_dir, output_dir, agg_type="day", period_days=365):
    """
    Process aggregate Parquet files (day or minute) by:
      - Selecting recent files (based on period_days)
      - Reading all files concurrently into memory with a process pool (using tqdm)
      - Converting all 'window_start' timestamps to New York time
      - Concatenating all data and grouping by ticker, then
      - For each ticker, appending only new rows (based on 'window_start') to each ticker's output file,
        processing these groups concurrently.
    """
    os.makedirs(output_dir, exist_ok=True)
    recent_files = get_recent_files(input_dir, period_days=period_days)
    if not recent_files:
        print("No recent files found in the input directory.")
        return

    print("Reading all Parquet files concurrently...")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dfs = list(tqdm(executor.map(read_file, recent_files),
                        total=len(recent_files),
                        desc="Reading Parquet Files"))
    # Filter out any failed reads
    dfs = [df for df in dfs if df is not None]
    if not dfs:
        print("No dataframes could be read.")
        return

    # Concatenate all the data and sort the MultiIndex
    all_df = pd.concat(dfs)
    all_df = all_df.sort_index()

    # Get the latest window_start per ticker from existing output files
    latest_windows = get_latest_window_starts(output_dir)

    # Prepare tasks for each ticker group
    ticker_tasks = []
    for ticker, group in all_df.groupby(level=0):
        lw = latest_windows.get(ticker, None)
        ticker_tasks.append((ticker, group, output_dir, lw))

    print("Processing ticker groups concurrently...")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(process_ticker, ticker_tasks),
                  total=len(ticker_tasks),
                  desc="Processing Ticker Groups"))
    print(f"Processing complete. Files saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Concatenate daily aggregate (day or minute) Parquet files into per-ticker files for a specified period."
    )
    parser.add_argument("input_dir", type=str,
                        help="Path to the input directory containing daily aggregate Parquet files.")
    parser.add_argument("output_dir", type=str,
                        help="Path to the output directory for per-ticker aggregate Parquet files.")
    parser.add_argument("--agg_type", type=str, choices=["day", "minute"], default="day",
                        help="Type of aggregation files to process (day or minute). Default is day.")
    parser.add_argument("--period_days", type=int, default=730,
                        help="Number of days to include (default: 730).")
    args = parser.parse_args()

    process_aggs(args.input_dir, args.output_dir, agg_type=args.agg_type, period_days=args.period_days)

