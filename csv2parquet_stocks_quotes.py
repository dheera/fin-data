#!/usr/bin/env python3

from tqdm import tqdm
from glob import glob
import pandas as pd
import gzip
import csv
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import pytz
import os
import numpy as np
import sys
import argparse

def convert_timestamp_series(sip_timestamps):
    """Vectorized conversion of nanosecond SIP timestamps to America/New_York timezone."""
    utc_times = pd.to_datetime(sip_timestamps, unit='ns', utc=True)
    return utc_times.dt.tz_convert('America/New_York')

def process_file(input_filename, output_dir):
    """Reads a gzipped CSV in a streaming fashion and writes Parquet files by ticker."""
    current_ticker = None
    chunk_size = 500000  # Preallocated chunk size
    chunks = []
    current_size = 0

    dtype_mapping = np.dtype([
        ('sip_timestamp', np.int64),
        ('ask_exchange', np.int16), ('ask_price', np.float32), ('ask_size', np.int32),
        ('bid_exchange', np.int16), ('bid_price', np.float32), ('bid_size', np.int32)
    ])

    with gzip.open(input_filename, 'rt') as f:
        reader = csv.DictReader(f)
        buffer = np.zeros(chunk_size, dtype=dtype_mapping)  # Preallocated structured NumPy array
        for row in reader:
            ticker = row['ticker']

            if ticker != current_ticker or current_size >= chunk_size:
                df = pd.DataFrame(buffer[:current_size])
                chunks.append(df)

                if ticker != current_ticker:
                    final_df = pd.concat(chunks, ignore_index=True)
                    final_df.sort_values(by='sip_timestamp', inplace=True)  # Sorting before saving
                    save_parquet(final_df, input_filename, output_dir, current_ticker)
                    chunks.clear()

                buffer = np.zeros(chunk_size, dtype=dtype_mapping)  # Reset buffer
                current_size = 0
                current_ticker = ticker

            buffer[current_size] = (int(row['sip_timestamp']), int(row['ask_exchange']),
                                    float(row['ask_price']), int(row['ask_size']), int(row['bid_exchange']),
                                    float(row['bid_price']), int(row['bid_size']))

            current_size += 1

        if current_size > 0:
            df = pd.DataFrame(buffer[:current_size])
            chunks.append(df)
            final_df = pd.concat(chunks, ignore_index=True)
            final_df.sort_values(by='sip_timestamp', inplace=True)  # Sorting before saving
            save_parquet(final_df, input_filename, output_dir, current_ticker)

def save_parquet(df, input_filename, output_dir, ticker):
    """Writes collected data to a Parquet file for a specific ticker."""
    if df.empty:
        return

    df['sip_timestamp'] = convert_timestamp_series(df['sip_timestamp'])  # Vectorized timestamp conversion

    date_str = os.path.basename(input_filename).split('.')[0]  # Extract date from filename
    output_filename = os.path.join(output_dir, date_str, f"{date_str}-{ticker}.parquet")

    os.makedirs(os.path.join(output_dir, date_str), exist_ok=True)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename)
    print(f"Saved {output_filename}")

    del df  # Free memory after writing file

# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reindex a single day of stock quotes and store it as separate Parquets per ticker.")
    parser.add_argument("in_dir", type=str, help="Path to dir for input CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    csv_files = sorted(glob(os.path.join(args.in_dir, "*.csv.gz")))
    for csv_file in tqdm(csv_files):
        date_str = os.path.basename(csv_file).split('.')[0]  # Extract date from filename
        out_dir = os.path.join(args.out_dir, date_str)
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 9000:
            print(f"output dir {out_dir} already exists, skipping")
            continue
        print(f"processing {csv_file}")
        process_file(csv_file, args.out_dir)

