#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm
import pytz

def process_option_trades(input_dir, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get sorted list of CSV.GZ files
    csv_files = sorted(glob(os.path.join(input_dir, "*.csv.gz")))

    # Use tqdm for progress tracking
    progress = tqdm(total=len(csv_files), desc="Processing Option Trade Files", unit="file")

    for file in csv_files:
        # Extract date from filename (assuming it's in YYYY-MM-DD format somewhere in the name)
        base_name = os.path.basename(file).replace(".csv.gz", "")
        date_dir = os.path.join(output_dir, base_name)
        os.makedirs(date_dir, exist_ok=True)

        if os.path.exists(date_dir) and len(os.listdir(date_dir)) > 3000:
            print(f"output dir {date_dir} exists, skipping")
            continue

        # Read CSV file
        try:
            df = pd.read_csv(file)
        except pd.errors.EmptyDataError:
            print(f"input file {file} has no data, skipping")
            continue

        print(f"processing {file}")

        # Parse option tickers
        # df[['underlying', 'expiry', 'type', 'strike']] = df['ticker'].apply(lambda x: pd.Series(parse_option_ticker(x)))

        # Vectorized extraction using regex
        ticker_pattern = r"O:(?P<underlying>.+?)(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})"
        df[['underlying', 'expiry', 'type', 'strike']] = df['ticker'].str.extract(ticker_pattern)

        # Convert extracted columns to correct types
        df['expiry'] = df['expiry'].astype(int)
        df['strike'] = df['strike'].astype(int) / 1000  # Convert strike to float


        # Convert timestamp to NY timezone
        df['sip_timestamp'] = pd.to_datetime(df['sip_timestamp'], unit='ns', utc=True).dt.tz_convert('America/New_York')

        # Remove original ticker column
        df.drop(columns=['ticker'], inplace=True)

        for underlying, group in df.groupby('underlying'):
            print(f"saving {underlying}")
            output_file = os.path.join(date_dir, f"{base_name}-{underlying}.parquet")
            group.set_index(['sip_timestamp'], inplace=True)
            group.to_parquet(output_file, engine="pyarrow", compression="snappy")

        # Update progress bar
        progress.update(1)

    progress.close()
    print(f"Processing complete. Parquet files saved in {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert option trade CSV files to per-day cleaned Parquet files.")
    parser.add_argument("input_dir", type=str, help="Path to the input directory containing option trade CSV files.")
    parser.add_argument("output_dir", type=str, help="Path to the output directory for per-day Parquet files.")

    args = parser.parse_args()
    process_option_trades(args.input_dir, args.output_dir)

