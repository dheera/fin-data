#!/usr/bin/env python3

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
            df.set_index(["underlying", "window_start"], inplace=True)

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

