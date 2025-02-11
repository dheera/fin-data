#!/usr/bin/env python3

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
        ('participant_timestamp', np.int64),
        ('price', np.float32),
        ('size', np.int32),
        ('exchange', np.int16),
        ('sequence_number', np.int64),
        ('conditions', 'U20'),  # String with a max length of 20
        ('correction', np.int8),
        ('id', np.int64),
        ('tape', np.int8),
        ('trf_id', np.int64),
        ('trf_timestamp', np.int64)
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

            buffer[current_size] = (
                int(row['sip_timestamp']), int(row['participant_timestamp']),
                float(row['price']), int(row['size']), int(row['exchange']),
                int(row['sequence_number']), row['conditions'], int(row['correction']),
                int(row['id']), int(row['tape']), int(row['trf_id']), int(row['trf_timestamp'])
            )

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
    df['participant_timestamp'] = convert_timestamp_series(df['participant_timestamp'])

    date_str = os.path.basename(input_filename).split('.')[0]  # Extract date from filename
    output_filename = os.path.join(output_dir, date_str, f"{date_str}-{ticker}.parquet")

    os.makedirs(os.path.join(output_dir, date_str), exist_ok=True)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename)
    print(f"Saved {output_filename}")

    del df  # Free memory after writing file

# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reindex a single day of stock trades and store it as separate Parquets per ticker.")
    parser.add_argument("in_dir", type=str, help="Path to dir for input CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    csv_files = sorted(glob(os.path.join(args.in_dir, "*.csv.gz")))
    for csv_file in tqdm(csv_files):
        date_str = os.path.basename(args.csv_file).split('.')[0]  # Extract date from filename
        out_dir = os.path.join(args.out_dir, date_str)
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) > 9000:
            print(f"output dir {out_dir} already exists, skipping")
            continue
        print(f"processing {args.infile}")
        process_file(args.in_file, args.out_dir)

