#!/usr/bin/env python3

import os
import argparse
import gzip
import shutil
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq
from glob import glob
from tqdm import tqdm

TICKER_PATTERN = r"O:(?P<underlying>.+?)(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})"
CHUNK_ROWS = 10_000_000


def process_file(input_filename, date_dir, base_name):
    column_types = {
        "ticker": pa.string(),
        "sip_timestamp": pa.int64(),
        "ask_exchange": pa.int16(),
        "ask_price": pa.float32(),
        "ask_size": pa.int32(),
        "bid_exchange": pa.int16(),
        "bid_price": pa.float32(),
        "bid_size": pa.int32(),
    }

    temp_dir = os.path.join(date_dir, ".parts")
    os.makedirs(temp_dir, exist_ok=True)

    part_counts = {}
    chunk_buffers = {}
    chunk_total = 0

    with gzip.open(input_filename, "rb") as f:
        try:
            reader = pacsv.open_csv(
                f,
                read_options=pacsv.ReadOptions(block_size=1 << 26),  # 64MB
                convert_options=pacsv.ConvertOptions(column_types=column_types),
            )
        except pa.ArrowInvalid as e:
            if "Empty CSV file" in str(e):
                print(f"Skipping empty file: {input_filename}")
                return
            raise

        for batch in reader:
            parsed = pc.extract_regex(batch.column("ticker"), TICKER_PATTERN)

            valid_mask = pc.is_valid(parsed.field("underlying"))
            batch = batch.filter(valid_mask)
            parsed = parsed.filter(valid_mask)
            if batch.num_rows == 0:
                continue

            underlying_col = parsed.field("underlying")
            table = pa.table({
                "expiry": pc.cast(parsed.field("expiry"), pa.int32()),
                "type": parsed.field("type"),
                "strike": pc.divide(
                    pc.cast(parsed.field("strike"), pa.float64()), 1000.0
                ),
                "sip_timestamp": pc.cast(
                    batch.column("sip_timestamp"), pa.timestamp("ns", tz="America/New_York")
                ),
                "ask_exchange": batch.column("ask_exchange"),
                "ask_price": batch.column("ask_price"),
                "ask_size": batch.column("ask_size"),
                "bid_exchange": batch.column("bid_exchange"),
                "bid_price": batch.column("bid_price"),
                "bid_size": batch.column("bid_size"),
            })

            for u in pc.unique(underlying_col).to_pylist():
                mask = pc.equal(underlying_col, u)
                subset = table.filter(mask)
                chunk_buffers.setdefault(u, []).append(subset)
                chunk_total += subset.num_rows

            if chunk_total >= CHUNK_ROWS:
                for u, tables in chunk_buffers.items():
                    combined = pa.concat_tables(tables)
                    part_num = part_counts.get(u, 0)
                    pq.write_table(
                        combined,
                        os.path.join(temp_dir, f"{u}-{part_num:06d}.parquet"),
                        compression="snappy",
                    )
                    part_counts[u] = part_num + 1
                chunk_buffers = {}
                chunk_total = 0

        # flush remaining
        for u, tables in chunk_buffers.items():
            combined = pa.concat_tables(tables)
            part_num = part_counts.get(u, 0)
            pq.write_table(
                combined,
                os.path.join(temp_dir, f"{u}-{part_num:06d}.parquet"),
                compression="snappy",
            )
            part_counts[u] = part_num + 1

    # merge parts per underlying: read all parts, sort, write final file
    for u in tqdm(sorted(part_counts.keys()), desc="  Merging", unit="sym"):
        parts = sorted(glob(os.path.join(temp_dir, f"{u}-*.parquet")))
        table = pa.concat_tables([pq.read_table(p) for p in parts])
        table = table.take(
            pc.sort_indices(table, sort_keys=[("sip_timestamp", "ascending")])
        )
        output_file = os.path.join(date_dir, f"{base_name}-{u}.parquet")
        pq.write_table(table, output_file, compression="snappy")
        print(f"  {u}: {table.num_rows} rows")

    shutil.rmtree(temp_dir)


def main():
    parser = argparse.ArgumentParser(
        description="Stream-convert option quote CSV.gz files to per-underlying Parquet files."
    )
    parser.add_argument("input_dir", type=str)
    parser.add_argument("output_dir", type=str)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    csv_files = sorted(glob(os.path.join(args.input_dir, "*.csv.gz")))

    for file in tqdm(sorted(csv_files, reverse=True), desc="Processing files", unit="file"):
        base_name = os.path.basename(file).replace(".csv.gz", "")
        date_dir = os.path.join(args.output_dir, base_name)
        os.makedirs(date_dir, exist_ok=True)

        if os.path.exists(date_dir) and len(os.listdir(date_dir)) > 100:
            print(f"output dir {date_dir} exists, skipping")
            continue

        print(f"processing {file}")
        process_file(file, date_dir, base_name)

    print(f"Processing complete. Parquet files saved in {args.output_dir}")


if __name__ == "__main__":
    main()
