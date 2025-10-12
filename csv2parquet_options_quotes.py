#!/usr/bin/env python3
import argparse
import os
import glob
import gzip
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

def process_file(input_filename, output_filename, tickers):
    """
    Uses PyArrow to stream-read a gzipped CSV file, filter rows based on a ticker list,
    parse the tickers (extracting underlying, expiry, opt_type, and strike), and write the 
    resulting rows to a Parquet file. If the CSV is empty, the file is skipped.
    """
    # Define column types for CSV reading.
    column_types = {
        "ticker": pa.string(),
        "sip_timestamp": pa.int64(),
        "ask_exchange": pa.int16(),
        "ask_price": pa.float32(),
        "ask_size": pa.int32(),
        "bid_exchange": pa.int16(),
        "bid_price": pa.float32(),
        "bid_size": pa.int32()
    }
    
    # Build an initial regex pattern to filter tickers.
    # Matches tickers starting with "O:" followed by one of the specified tickers.
    # Use a non-capturing group here to avoid issues.
    pattern = f"^O:(?:{'|'.join(tickers)})"
    
    writer = None
    total_filtered_rows = 0

    with gzip.open(input_filename, 'rb') as f:
        try:
            reader = pacsv.open_csv(
                f,
                convert_options=pacsv.ConvertOptions(column_types=column_types)
            )
        except pa.ArrowInvalid as e:
            if "Empty CSV file" in str(e):
                print(f"Skipping empty file: {input_filename}")
                return
            else:
                raise

        # Process record batches one at a time.
        for batch in reader:
            # Apply initial filter on the 'ticker' column.
            mask = pc.match_substring_regex(batch.column("ticker"), pattern)
            filtered_batch = batch.filter(mask)
            if filtered_batch.num_rows == 0:
                continue

            # --- Ticker Parsing ---
            # The original ticker format is: O:<UNDERLYING><expiry><C/P><strike>
            # Example: "O:AAPL210917C00145000" should yield:
            # underlying: "AAPL", expiry: 210917, opt_type: "C", strike: 145.0

            # Use named capture groups as required by PyArrow.
            underlying = pc.extract_regex(filtered_batch.column("ticker"), r"O:(?P<underlying>[A-Z]+)")
            expiry_str = pc.extract_regex(filtered_batch.column("ticker"), r"O:[A-Z]+(?P<expiry>\d{6})")
            opt_type = pc.extract_regex(filtered_batch.column("ticker"), r"O:[A-Z]+\d{6}(?P<opt_type>[CP])")
            strike_str = pc.extract_regex(filtered_batch.column("ticker"), r"O:[A-Z]+\d{6}[CP](?P<strike>\d{8})")
            
            # Convert expiry to int32.
            expiry = pc.cast(expiry_str, pa.int32())
            # Convert strike to int32, then divide by 1000 and cast to float32.
            strike_int = pc.cast(strike_str, pa.int32())
            strike = pc.divide(strike_int, 1000.0)
            strike = pc.cast(strike, pa.float32())

            # --- Build a new table with parsed ticker columns and other fields ---
            other_cols = ["sip_timestamp", "ask_exchange", "ask_price", "ask_size",
                          "bid_exchange", "bid_price", "bid_size"]
            cols = [
                ("underlying", underlying),
                ("expiry", expiry),
                ("opt_type", opt_type),
                ("strike", strike)
            ]
            for col in other_cols:
                cols.append((col, filtered_batch.column(col)))
            
            new_table = pa.Table.from_arrays(
                [col for _, col in cols],
                names=[name for name, _ in cols]
            )
            
            if writer is None:
                writer = pq.ParquetWriter(output_filename, new_table.schema)
            writer.write_table(new_table)
            total_filtered_rows += new_table.num_rows

    if writer is not None:
        writer.close()
        print(f"Saved {output_filename} with {total_filtered_rows} rows.")
    else:
        print(f"No matching rows found in {input_filename}.")

def main():
    parser = argparse.ArgumentParser(
        description="Stream gzipped CSV files with PyArrow, parse tickers, and write to Parquet."
    )
    parser.add_argument("in_dir", type=str, help="Directory containing input CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Directory to store output Parquet files.")
    parser.add_argument(
        "--tickers",
        type=str,
        default="SPX,SPXW,VIX,VIXW,XSP,XSPW,SPXL,QQQ,TQQQ,SQQQ,NVDA,TSM,AAPL,MSFT,AMZN,META,TSLA,FBTC,IBIT,BITO,BITX,MSTR",
        help="Comma-separated list of tickers to filter on (e.g., 'SPX,VIX,AAPL')."
    )
    args = parser.parse_args()

    tickers_list = args.tickers.split(",")
    os.makedirs(args.out_dir, exist_ok=True)
    csv_files = sorted(glob.glob(os.path.join(args.in_dir, "*.csv.gz")))

    for csv_file in tqdm(csv_files, desc="Processing files"):
        base_name = os.path.basename(csv_file).split('.')[0]
        output_filename = os.path.join(args.out_dir, f"{base_name}.parquet")
        process_file(csv_file, output_filename, tickers_list)

if __name__ == "__main__":
    main()

