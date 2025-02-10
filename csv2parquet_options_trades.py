#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm
import pytz

def parse_option_ticker(ticker):
    """
    Parses the option ticker format (e.g., O:A250117C00125000)
    and extracts expiry (yymmdd), type (C/P), and strike price.
    """
    try:
        # Example: O:A250117C00125000
        core = ticker.split(":")[1]  # Remove the "O:" prefix
        
        expiry = int(core[1:7])  # yymmdd as integer
        opt_type = core[7]  # "C" or "P"
        strike = int(core[8:]) / 1000  # Convert to float (e.g., 00125000 → 125.00)

        return expiry, opt_type, strike
    except Exception as e:
        print(f"Error parsing ticker: {ticker} - {e}")
        return None, None, None

def process_option_trades(input_dir, output_dir, batch_size=16):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Get sorted list of CSV files for chronological processing
    csv_files = sorted(glob(os.path.join(input_dir, "*.csv")))

    # Use tqdm for progress tracking
    progress = tqdm(total=len(csv_files), desc="Processing Option Trade Files", unit="file")

    # Process files in batches
    for i in range(0, len(csv_files), batch_size):
        batch_files = csv_files[i:i+batch_size]

        # Read batch of CSV files
        dfs = [pd.read_csv(file) for file in batch_files]

        # Concatenate batch
        df = pd.concat(dfs)

        # Parse option tickers
        df[['expiry', 'type', 'strike']] = df['ticker'].apply(lambda x: pd.Series(parse_option_ticker(x)))

        # Convert timestamp to NY timezone
        df['sip_timestamp'] = pd.to_datetime(df['sip_timestamp'], unit='ns', utc=True).dt.tz_convert('America/New_York')

        # Remove original ticker column
        df.drop(columns=['ticker'], inplace=True)

        # Save cleaned data to Parquet
        output_path = os.path.join(output_dir, f"options_{i}.parquet")
        df.to_parquet(output_path, engine="pyarrow", compression="snappy")

        # Update progress bar
        progress.update(len(batch_files))

    progress.close()
    print(f"Processing complete. Parquet files saved to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert option trade CSV files to cleaned Parquet format.")
    parser.add_argument("input_dir", type=str, help="Path to the input directory containing option trade CSV files.")
    parser.add_argument("output_dir", type=str, help="Path to the output directory for cleaned Parquet files.")
    
    args = parser.parse_args()
    process_option_trades(args.input_dir, args.output_dir)

