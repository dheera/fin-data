#!/usr/bin/env python3

# converts polygon csvs to parquets

# ./csv2parquet.py --in_dir us_stocks_sip/minute_aggs_v1/ --out_dir us_stocks_sip/minute_aggs_parquet/
# ./csv2parquet.py --in_dir us_options_opra/minute_aggs_v1/ --out_dir us_options_opra/minute_aggs_parquet/
# ./csv2parquet.py --in_dir us_indices/minute_aggs_v1/ --out_dir us_indices/minute_aggs_parquet/

import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

class AggDataPreprocessor:
    def __init__(self, in_dir, out_dir, delete_original):
        self.in_dir = Path(in_dir)
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.delete_original = delete_original

    def reindex_and_store_all(self):
        """
        Load all CSV files in the directory, reindex data, and store them in Parquet format.
        """
        files = sorted(self.in_dir.glob("*.csv.gz"))  # Sort files alphabetically
        for file in files:
            # skip placeholder zero-sized files (they exist to prevent re-download)
            if os.path.getsize(file) == 0:
                continue

            print(f"Processing file: {file.name}")

            out_path = self.out_dir / file.parts[-1].replace(".csv.gz", ".parquet")
            if os.path.exists(out_path):
                print(f"Exists, skipping: {out_path}")
            else:
                self.reindex_and_store(file, out_path)

            if delete_original:
                print("Deleting original {file.name}")
                os.remove(file)
                Path(file).touch()

    def reindex_and_store(self, file, out_path):
        """
        Load a single CSV file, reindex data, and store it in Parquet format.

        Args:
            file (Path): Path to the CSV file.
        """
        df = pd.read_csv(file)

        is_options = df.reset_index()['ticker'].iloc[0].startswith("O:")
        
        # Convert timestamp to datetime index
        df['window_start'] = pd.to_datetime(df['window_start'], unit='ns')
        for field in ['open', 'close', 'high', 'low']:
            df[field] = df[field].astype('float32')
        for field in ['volume', 'transactions']:
            if field in df.columns:
                df[field] = df[field].astype('int32')

        df["window_start"] = pd.to_datetime(df["window_start"], utc=True).dt.tz_convert("America/New_York")

        if is_options:
            df.reset_index(inplace=True)
            ticker_pattern = r"O:(?P<underlying>.+?)(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})"
            df[['underlying', 'expiry', 'type', 'strike']] = df['ticker'].str.extract(ticker_pattern)

            # Convert extracted columns to correct types
            df['expiry'] = df['expiry'].astype(int)
            df['strike'] = df['strike'].astype(int) / 1000  # Convert strike to float
            df.set_index(["underlying", "ticker", "window_start"], inplace=True)
        else:
            df.set_index(['ticker', 'window_start'], inplace=True)

        pq.write_table(pa.Table.from_pandas(df), out_path)

# Example Usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reindex stock data and store it in Parquet format.")
    parser.add_argument("in_dir", type=str, help="Path to the directory containing CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    parser.add_argument("--delete-original", action="store_true", help="Delete original files (default: False)")

    args = parser.parse_args()

    preprocessor = AggDataPreprocessor(args.in_dir, args.out_dir, args.delete_original)
    preprocessor.reindex_and_store_all()

