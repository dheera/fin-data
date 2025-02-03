#!/usr/bin/env python3

import pandas as pd
import gzip
import csv
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import pytz
import os

def parse_ticker(ticker):
    """Extracts underlying, expiry, type, and strike price from an option ticker."""
    parts = ticker.split(":")
    if len(parts) != 2:
        return None
    
    option = parts[1]
    underlying = ''.join(filter(str.isalpha, option[:-15]))  # Extract underlying (e.g., AAPL, NVDA, etc.)
    expiry = option[-15:-9]    # Extract expiry (e.g., 250221 for 2025-02-21)
    opt_type = option[-9]      # Extract option type ('C' or 'P')
    strike = int(option[-8:]) / 1000  # Convert strike price to float (e.g., 115000 -> 115.0)
    
    return underlying, expiry, opt_type, strike

def convert_timestamp(sip_timestamp):
    """Converts nanosecond SIP timestamp to America/New_York timezone."""
    utc_time = datetime.utcfromtimestamp(sip_timestamp / 1e9).replace(tzinfo=pytz.utc)
    ny_time = utc_time.astimezone(pytz.timezone('America/New_York'))
    return ny_time

def process_file(input_filename):
    """Reads a gzipped CSV in a streaming fashion and writes Parquet files by underlying."""
    current_underlying = None
    data = []
    
    with gzip.open(input_filename, 'rt') as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = parse_ticker(row['ticker'])
            if not parsed:
                continue
            
            underlying, expiry, opt_type, strike = parsed
            row['expiry'] = expiry
            row['type'] = opt_type
            row['strike'] = strike
            row['sip_timestamp'] = convert_timestamp(int(row['sip_timestamp']))
            del row['ticker']  # Drop original ticker column
            del row['sequence_number']  # Drop sequence number column
            
            if current_underlying is None:
                current_underlying = underlying
            
            if underlying != current_underlying:
                # Save the existing data for the previous underlying
                save_parquet(data, input_filename, current_underlying)
                data.clear()
                current_underlying = underlying
            
            data.append(row)
        
        if data:
            save_parquet(data, input_filename, current_underlying)  # Save last chunk

def save_parquet(data, input_filename, underlying):
    """Writes collected data to a Parquet file for a specific underlying."""
    if not data:
        return
    
    df = pd.DataFrame(data)
    df['sip_timestamp'] = pd.to_datetime(df['sip_timestamp'])
    
    date_str = os.path.basename(input_filename).split('.')[0]  # Extract date from filename
    output_filename = f"{date_str}-{underlying}.parquet"
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename)
    print(f"Saved {output_filename}")

# Example usage
process_file("2025-01-31.csv.gz")

