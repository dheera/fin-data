#!/usr/bin/env python3

# Stream-converts polygon futures quotes CSV.gz flat files (multi-GB per day)
# to per-product-root Parquet files.
#
# ./csv2parquet_futures_quotes.py us_futures_cme/quotes_v1 us_futures_cme/quotes
#
# Same ticker handling as csv2parquet_futures_trades.py: outrights (ESU6) get
# type 'O' and expiry YYYYMM; listed spreads/combos (6AU6-6AQ6, KE:BF H7-K7-N7,
# SR3:DF ..., 6A:XF:AUDUSD:U6, FYT 03-02 U6) get type 'C' and expiry 0,
# filed under their product root. Filter type == 'O' for outright quote series.
#
# Output: <out_dir>/<date>/<date>-<root>.parquet sorted by timestamp. Files are
# streamed in batches and spilled to <date>.tmp/.parts, then merged per root;
# the date dir is renamed into place only when complete, so an existing date
# dir means done.

import os
import re
import gzip
import shutil
import argparse
from glob import glob

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq
from tqdm import tqdm

MONTH_CODES = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
               'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}
OUTRIGHT_RE = re.compile(r'^([A-Z0-9]+?)([FGHJKMNQUVXZ])(\d{1,2})$')

COLUMN_TYPES = {
    "ticker": pa.string(),
    "timestamp": pa.int64(),
    "sequence_number": pa.int64(),
    "report_sequence": pa.int64(),
    "ask_timestamp": pa.int64(),
    "ask_price": pa.float64(),
    "ask_size": pa.int32(),
    "bid_timestamp": pa.int64(),
    "bid_price": pa.float64(),
    "bid_size": pa.int32(),
    "exchange": pa.int16(),
    "session_end_date": pa.string(),
}

CHUNK_ROWS = 10_000_000
NY_TS = pa.timestamp('ns', tz='America/New_York')


def parse_outright(ticker, session_year):
    m = OUTRIGHT_RE.match(ticker)
    if not m:
        return None
    root, month_code, year_digits = m.groups()
    if len(year_digits) == 2:
        year = 2000 + int(year_digits)
    else:
        year = session_year - session_year % 10 + int(year_digits)
        if year < session_year:
            year += 10
    return root, year * 100 + MONTH_CODES[month_code]


def classify_ticker(ticker, session_year):
    """-> (root, type 'O'|'C', expiry YYYYMM; 0 = spread/combo with no single expiry)"""
    parsed = parse_outright(ticker, session_year)
    if parsed:
        return parsed[0], 'O', parsed[1]
    head = re.split(r'[: ]', ticker, 1)[0]
    if '-' in head:  # calendar spread like 6AU6-6AQ6: root comes from first leg
        leg = head.split('-', 1)[0]
        parsed = parse_outright(leg, session_year)
        return (parsed[0] if parsed else leg), 'C', 0
    return head, 'C', 0


def transform_batch(batch, session_year, cache):
    """Parse tickers and convert timestamps; returns (root_column, table)."""
    tickers = batch.column('ticker')
    uniq = pc.unique(tickers)
    roots, types, expiries = [], [], []
    for t in uniq.to_pylist():
        if t not in cache:
            cache[t] = classify_ticker(t, session_year)
        root, typ, expiry = cache[t]
        roots.append(root)
        types.append(typ)
        expiries.append(expiry)
    idx = pc.index_in(tickers, uniq)
    root_col = pc.take(pa.array(roots, pa.string()), idx)

    table = pa.table({
        'ticker': tickers,
        'type': pc.take(pa.array(types, pa.string()), idx),
        'expiry': pc.take(pa.array(expiries, pa.int32()), idx),
        'timestamp': pc.cast(batch.column('timestamp'), NY_TS),
        'sequence_number': batch.column('sequence_number'),
        'report_sequence': batch.column('report_sequence'),
        'ask_timestamp': pc.cast(batch.column('ask_timestamp'), NY_TS),
        'ask_price': batch.column('ask_price'),
        'ask_size': batch.column('ask_size'),
        'bid_timestamp': pc.cast(batch.column('bid_timestamp'), NY_TS),
        'bid_price': batch.column('bid_price'),
        'bid_size': batch.column('bid_size'),
        'exchange': batch.column('exchange'),
    })
    return root_col, table


def flush(chunk_buffers, part_counts, temp_dir):
    for root, tables in chunk_buffers.items():
        combined = pa.concat_tables(tables)
        part_num = part_counts.get(root, 0)
        safe_root = re.sub(r'[^A-Za-z0-9]', '_', root)
        pq.write_table(
            combined, os.path.join(temp_dir, f"{safe_root}-{part_num:06d}.parquet"),
            compression='snappy')
        part_counts[root] = part_num + 1


def process_file(input_filename, output_dir):
    date_str = os.path.basename(input_filename).split('.')[0]
    session_year = int(date_str[:4])
    final_dir = os.path.join(output_dir, date_str)
    tmp_dir = final_dir + '.tmp'
    shutil.rmtree(tmp_dir, ignore_errors=True)
    temp_parts = os.path.join(tmp_dir, '.parts')
    os.makedirs(temp_parts, exist_ok=True)

    part_counts = {}
    chunk_buffers = {}
    chunk_total = 0
    root_of = {}  # safe_root -> root
    cache = {}

    try:
        with gzip.open(input_filename, 'rb') as f:
            reader = pacsv.open_csv(
                f,
                read_options=pacsv.ReadOptions(block_size=1 << 26),  # 64MB
                convert_options=pacsv.ConvertOptions(column_types=COLUMN_TYPES),
            )
            for batch in reader:
                root_col, table = transform_batch(batch, session_year, cache)
                for root in pc.unique(root_col).to_pylist():
                    subset = table.filter(pc.equal(root_col, root))
                    chunk_buffers.setdefault(root, []).append(subset)
                    chunk_total += subset.num_rows
                    root_of[re.sub(r'[^A-Za-z0-9]', '_', root)] = root
                if chunk_total >= CHUNK_ROWS:
                    flush(chunk_buffers, part_counts, temp_parts)
                    chunk_buffers = {}
                    chunk_total = 0
    except (EOFError, gzip.BadGzipFile) as e:
        print(f"Truncated/corrupt gzip, skipping {input_filename}: {e}")
        shutil.rmtree(tmp_dir)
        return
    except pa.ArrowInvalid as e:
        if "Empty CSV file" in str(e):
            print(f"Skipping empty file: {input_filename}")
            shutil.rmtree(tmp_dir)
            return
        raise

    flush(chunk_buffers, part_counts, temp_parts)
    chunk_buffers = {}

    # merge parts per root: read all parts, sort, write final file
    for safe_root in tqdm(sorted(root_of.keys()), desc="  Merging", unit="root"):
        parts = sorted(glob(os.path.join(temp_parts, f"{safe_root}-*.parquet")))
        table = pa.concat_tables([pq.read_table(p) for p in parts])
        table = table.take(
            pc.sort_indices(table, sort_keys=[('timestamp', 'ascending')]))
        pq.write_table(
            table, os.path.join(tmp_dir, f"{date_str}-{safe_root}.parquet"),
            compression='snappy')

    shutil.rmtree(temp_parts)
    os.rename(tmp_dir, final_dir)
    print(f"Saved {final_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream-convert futures quote CSV.gz files to per-root Parquet files.")
    parser.add_argument("in_dir", type=str, help="Path to dir of input CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    parser.add_argument("--delete-original", action="store_true", help="Delete original files (default: False)")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    csv_files = sorted(glob(os.path.join(args.in_dir, "*.csv.gz")), reverse=True)

    for csv_file in tqdm(csv_files, desc="Processing files", unit="file"):
        if os.path.getsize(csv_file) == 0:
            continue
        date_str = os.path.basename(csv_file).split('.')[0]
        if os.path.exists(os.path.join(args.out_dir, date_str)):
            if args.delete_original:
                print(f"Deleting original {csv_file}")
                os.remove(csv_file)
                open(csv_file, "a").close()
            continue
        print(f"processing {csv_file}")
        process_file(csv_file, args.out_dir)

    print(f"Processing complete. Parquet files saved in {args.out_dir}")
