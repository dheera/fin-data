#!/usr/bin/env python3

# converts a bunch of day aggs like this, filenames are indexed by date:

#/fin :) print-parquet us_stocks_sip/day_aggs/2025-01-02.parquet

#                             volume        open       close        high         low  transactions
#ticker window_start
#A      2025-01-02 05:00:00   927400  135.210007  133.429993  135.729996  132.869995         20210
#AA     2025-01-02 05:00:00  2553338   38.165001   37.990002   39.040001   37.900002         33107
#AAA    2025-01-02 05:00:00    13720   25.129999   25.100000   25.139000   25.060101            71
#AAAU   2025-01-02 05:00:00  2130005   26.160000   26.285000   26.299999   26.149000          3266
#AACG   2025-01-02 05:00:00    17613    0.832000    0.880000    0.905400    0.820001           147
#...                             ...         ...         ...         ...         ...           ...
#ZWS    2025-01-02 05:00:00   580435   37.439999   36.900002   37.590000   36.730000         11220
#ZXIET  2025-01-02 05:00:00     2000  100.000000  100.000000  100.000000  100.000000             1
#ZYME   2025-01-02 05:00:00   517644   14.660000   14.780000   15.040000   14.420000          6304
#ZYXI   2025-01-02 05:00:00    84810    8.120000    7.810000    8.120000    7.770000          1510
#ZZZ    2025-01-02 05:00:00     3437   27.330000   27.177500   27.400000   27.070000            81
#[10870 rows x 6 columns]

# into a aggs by ticker for all time, filenames are indexed by ticker:

# /fin :) print-parquet us_stocks_sip/day_aggs_by_ticker/NVDA.parquet
#                        volume        open       close        high         low  transactions
#window_start                                                                                
#2003-09-10 04:00:00   11480051   20.150000   19.320000   20.690001   19.160000         24116
#2003-09-11 04:00:00   21728296   19.580000   19.070000   20.150000   18.299999         48120
#2003-09-12 04:00:00    9659462   18.910000   19.350000   19.770000   18.700001         23350
#2003-09-15 04:00:00    4946863   19.510000   19.030001   19.613001   18.959999         12812
#2003-09-16 04:00:00    5171418   19.070000   19.620001   19.679001   19.070000         13435
#...                        ...         ...         ...         ...         ...           ...
#2025-02-04 05:00:00  242321420  116.959999  118.650002  121.199997  116.699997       1841195
#2025-02-05 05:00:00  260517576  121.760002  124.830002  125.000000  120.760002       1968061
#2025-02-06 05:00:00  248146032  127.419998  128.679993  128.770004  125.209999       1855949
#2025-02-07 05:00:00  226630821  129.220001  129.839996  130.369995  125.000000       1778758
#2025-02-10 05:00:00  211358778  130.089996  133.570007  135.000000  129.960007       1658255
#
#[5390 rows x 6 columns]

import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm

def get_latest_dates(output_dir):
    """
    Reads existing Parquet files and determines the latest timestamp for each ticker.
    Returns a dictionary of {ticker: latest_timestamp}.
    """
    latest_dates = {}

    # Get all existing per-ticker Parquet files
    existing_files = glob(os.path.join(output_dir, "*.parquet"))

    for file in existing_files:
        ticker = os.path.basename(file).replace(".parquet", "")

        # Read only the first column for efficiency
        df = pd.read_parquet(file, columns=["window_start"])
        
        window_start_max = df.index.get_level_values("window_start").max()
        print(window_start_max)
        if not df.empty:
            latest_dates[ticker] = df["window_start"].max()
            print(f"{ticker}: {latest_dates[ticker]}")
    return latest_dates

def process_parquets(input_dir, output_dir, batch_size=16):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get sorted list of Parquet files for chronological processing
    parquet_files = sorted(glob(os.path.join(input_dir, "*.parquet")))

    # Get latest dates for tickers already in the output directory
    latest_dates = get_latest_dates(output_dir)

    # Use tqdm for progress tracking
    progress = tqdm(total=len(parquet_files), desc="Processing Parquet Files", unit="file")

    is_tickers_options = None

    # Process files in batches of batch_size
    for i in range(0, len(parquet_files), batch_size):
        batch_files = parquet_files[i:i+batch_size]

        # Read batch of Parquet files
        dfs = [pd.read_parquet(file) for file in batch_files]

        # Concatenate batch
        df = pd.concat(dfs)

        if is_tickers_options is None:
            is_tickers_options = df.reset_index()['ticker'].iloc[0].startswith("O:")
            print(f"Are tickrs options? {is_tickers_options}")
    
        if is_tickers_options:
            # Vectorized extraction using regex
            ticker_pattern = r"O:(?P<underlying>.+?)(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})"

            df.reset_index(inplace=True)
            df[['underlying', 'expiry', 'type', 'strike']] = df['ticker'].str.extract(ticker_pattern)

            # Convert extracted columns to correct types
            df['expiry'] = df['expiry'].astype(int)
            df['strike'] = df['strike'].astype(int) / 1000  # Convert strike to float
            # df.set_index(["underlying", "window_start"], inplace=True)
            df.set_index(['underlying', 'expiry', 'type', 'strike', 'window_start'], inplace=True)

        # Process each ticker separately
        for ticker, group in df.groupby(level=0):  # Assuming ticker is in index
            output_path = os.path.join(output_dir, f"{ticker}.parquet")

            # Remove ticker index
            group = group.reset_index(level=0, drop=True)

            # Skip data already present in the output file
            if ticker in latest_dates:
                group = group[group["window_start"] > latest_dates[ticker]]

            if group.empty:
                continue  # Skip writing if no new data

            # Convert to Arrow Table
            table = pa.Table.from_pandas(group)

            # Append to existing Parquet file or create a new one
            if os.path.exists(output_path):
                existing_table = pq.read_table(output_path)  # Read existing data
                combined_table = pa.concat_tables([existing_table, table])  # Merge with new data
                pq.write_table(combined_table, output_path, compression="snappy")  # Write back
            else:
                pq.write_table(table, output_path, compression="snappy")  # Create new file

        # Update progress bar
        progress.update(len(batch_files))

    progress.close()
    print(f"Processing complete. Parquet files saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reorganize day aggregate Parquet files by ticker in a memory-efficient way, skipping duplicate data.")
    parser.add_argument("input_dir", type=str, help="Path to the input directory containing daily Parquet files.")
    parser.add_argument("output_dir", type=str, help="Path to the output directory for per-ticker Parquet files.")

    args = parser.parse_args()
    process_parquets(args.input_dir, args.output_dir)

