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

class StockDataPreprocessor:
    def __init__(self, in_dir, out_dir):
        self.in_dir = Path(in_dir)
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def reindex_and_store_all(self):
        """
        Load all CSV files in the directory, reindex data, and store them in Parquet format.
        """
        files = sorted(self.in_dir.glob("*.csv.gz"))  # Sort files alphabetically
        for file in files:
            print(f"Processing file: {file.name}")
            self.reindex_and_store(file)

    def reindex_and_store(self, file):
        """
        Load a single CSV file, reindex data, and store it in Parquet format.

        Args:
            file (Path): Path to the CSV file.
        """
        date_path = self.out_dir / file.parts[-1].replace(".csv.gz", ".parquet")
        if os.path.exists(date_path):
            print(f"Exists, skipping: {date_path}")
            return

        df = pd.read_csv(file)

        # Convert timestamp to datetime index
        df['window_start'] = pd.to_datetime(df['window_start'], unit='ns')
        for field in ['open', 'close', 'high', 'low']:
            df[field] = df[field].astype('float32')
        for field in ['volume', 'transactions']:
            if field in df.columns:
                df[field] = df[field].astype('int32')
        df.set_index(['ticker', 'window_start'], inplace=True)

        pq.write_table(pa.Table.from_pandas(df), date_path)

# Example Usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reindex stock data and store it in Parquet format.")
    parser.add_argument("in_dir", type=str, help="Path to the directory containing CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    args = parser.parse_args()

    preprocessor = StockDataPreprocessor(args.in_dir, args.out_dir)
    preprocessor.reindex_and_store_all()

