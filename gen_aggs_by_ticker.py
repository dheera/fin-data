#!/usr/bin/env python3

"""
Unified Script to Reorganize Aggregate Parquet Files by Ticker

This script processes daily aggregate files (either day or minute aggregates)
and creates/updates per-ticker Parquet files that contain all available data
within a specified lookback period (default: 1 year).

Usage Examples:
  For day aggregates:
    python process_aggs.py /path/to/day_aggs /path/to/day_aggs_by_ticker --agg_type day

  For minute aggregates:
    python process_aggs.py /path/to/minute_aggs /path/to/minute_aggs_per_ticker --agg_type minute

The logic is nearly identical for both cases—the minute aggregate files may have
multiple rows per ticker per day while the day aggregate files generally have one.

*** Example of input: one file per day for all tickers ***

/fin :) print-parquet us_stocks_sip/day_aggs/2025-01-02.parquet
                             volume        open       close        high         low  transactions
ticker window_start
A      2025-01-02 05:00:00   927400  135.210007  133.429993  135.729996  132.869995         20210
AA     2025-01-02 05:00:00  2553338   38.165001   37.990002   39.040001   37.900002         33107
AAA    2025-01-02 05:00:00    13720   25.129999   25.100000   25.139000   25.060101            71
AAAU   2025-01-02 05:00:00  2130005   26.160000   26.285000   26.299999   26.149000          3266
AACG   2025-01-02 05:00:00    17613    0.832000    0.880000    0.905400    0.820001           147
...                             ...         ...         ...         ...         ...           ...
ZWS    2025-01-02 05:00:00   580435   37.439999   36.900002   37.590000   36.730000         11220
ZXIET  2025-01-02 05:00:00     2000  100.000000  100.000000  100.000000  100.000000             1
ZYME   2025-01-02 05:00:00   517644   14.660000   14.780000   15.040000   14.420000          6304
ZYXI   2025-01-02 05:00:00    84810    8.120000    7.810000    8.120000    7.770000          1510
ZZZ    2025-01-02 05:00:00     3437   27.330000   27.177500   27.400000   27.070000            81
[10870 rows x 6 columns]

*** Example of output: one file per ticker for all time ***

/fin :) print-parquet us_stocks_sip/day_aggs_by_ticker/NVDA.parquet
                        volume        open       close        high         low  transactions
window_start                                                                                
2003-09-10 04:00:00   11480051   20.150000   19.320000   20.690001   19.160000         24116
2003-09-11 04:00:00   21728296   19.580000   19.070000   20.150000   18.299999         48120
2003-09-12 04:00:00    9659462   18.910000   19.350000   19.770000   18.700001         23350
2003-09-15 04:00:00    4946863   19.510000   19.030001   19.613001   18.959999         12812
2003-09-16 04:00:00    5171418   19.070000   19.620001   19.679001   19.070000         13435
...                        ...         ...         ...         ...         ...           ...
2025-02-04 05:00:00  242321420  116.959999  118.650002  121.199997  116.699997       1841195
2025-02-05 05:00:00  260517576  121.760002  124.830002  125.000000  120.760002       1968061
2025-02-06 05:00:00  248146032  127.419998  128.679993  128.770004  125.209999       1855949
2025-02-07 05:00:00  226630821  129.220001  129.839996  130.369995  125.000000       1778758
2025-02-10 05:00:00  211358778  130.089996  133.570007  135.000000  129.960007       1658255
[5390 rows x 6 columns]
"""

import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm
from datetime import datetime, timedelta

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
        except Exception as e:
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

def process_aggs(input_dir, output_dir, agg_type="day", period_days=365, batch_size=16):
    """
    Process aggregate Parquet files (day or minute) by:
      - Selecting recent files (based on period_days)
      - Reading and concatenating them in batches
      - Grouping by ticker and dropping the ticker column (the per-ticker files use 'window_start' as index)
      - Appending only new rows (based on 'window_start') to each ticker's output file
    """
    os.makedirs(output_dir, exist_ok=True)
    recent_files = get_recent_files(input_dir, period_days=period_days)
    if not recent_files:
        print("No recent files found in the input directory.")
        return

    latest_windows = get_latest_window_starts(output_dir)
    progress = tqdm(total=len(recent_files), desc="Processing Aggregate Files", unit="file")

    for i in range(0, len(recent_files), batch_size):
        batch_files = recent_files[i:i+batch_size]
        dfs = []
        for file in batch_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
        if not dfs:
            progress.update(len(batch_files))
            continue

        # Concatenate the batch into one DataFrame
        batch_df = pd.concat(dfs)
        
        # Ensure the DataFrame is indexed by ['ticker', 'window_start']
        if batch_df.index.nlevels < 2:
            # If not, assume 'ticker' and 'window_start' are columns
            batch_df = batch_df.reset_index().set_index(["ticker", "window_start"])

        # (Optional) If processing day aggregates and you need to handle options tickers,
        # you could insert extra logic here similar to the original day aggs script.

        # Group by ticker (the first index level)
        for ticker, group in batch_df.groupby(level=0):
            # Remove ticker from the index so that only window_start remains
            group = group.reset_index(level=0, drop=True)
            # Skip rows that are already present (based on window_start)
            if ticker in latest_windows:
                new_group = group[group.index > latest_windows[ticker]]
            else:
                new_group = group

            if new_group.empty:
                continue

            output_path = os.path.join(output_dir, f"{ticker}.parquet")
            if os.path.exists(output_path):
                try:
                    existing_df = pd.read_parquet(output_path)
                    # Combine the existing data with the new data and remove duplicate indices
                    combined_df = pd.concat([existing_df, new_group])
                    combined_df = combined_df[~combined_df.index.duplicated(keep="last")]
                    combined_df = combined_df.sort_index()
                except Exception as e:
                    print(f"Error processing existing file {output_path}: {e}")
                    combined_df = new_group
            else:
                combined_df = new_group

            # Write the combined DataFrame back to a Parquet file (using snappy compression)
            table = pa.Table.from_pandas(combined_df)
            pq.write_table(table, output_path, compression="snappy")
            # Update the latest window_start for this ticker
            latest_windows[ticker] = combined_df.index.max()

        progress.update(len(batch_files))
    progress.close()
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
    parser.add_argument("--batch_size", type=int, default=16,
                        help="Number of files to process in each batch (default: 16).")
    args = parser.parse_args()

    process_aggs(args.input_dir, args.output_dir, agg_type=args.agg_type,
                 period_days=args.period_days, batch_size=args.batch_size)

