#!/usr/bin/env python3

# Converts polygon futures trades CSV.gz flat files to per-product-root Parquet files.
#
# ./csv2parquet_futures_trades.py us_futures_cme/trades_v1 us_futures_cme/trades
#
# Futures tickers come in several forms:
#   ESU6              outright: root ES, month U (Sep), year 6 -> expiry 202609
#   ASTU26            outright with 2-digit year
#   6AU6-6AQ6         exchange-listed calendar spread (root 6A)
#   KE:BF H7-K7-N7    butterfly (root KE)
#   SR3:DF ...        condor / pack / other rate strategy (root SR3)
#   6A:XF:AUDUSD:U6   FX inverted-quote instrument (root 6A)
#   BZ:C1 HO V6-BZ V6 inter-commodity spread (filed under first root, BZ)
#   FYT 03-02 U6      CBOT inter-commodity ratio spread (root FYT)
#
# Output: <out_dir>/<date>/<date>-<root>.parquet with all rows for that product
# root (outrights of every expiry plus its listed spreads/combos), sorted by
# timestamp. Outrights get type 'O' and expiry YYYYMM; spreads/combos get type
# 'C' and expiry 0 (their prices are differentials, not levels — filter
# type == 'O' for outright price series). The raw ticker column is kept.
#
# Single-digit contract years are resolved to the current decade, rolling
# forward if in the past (digit 6 in a 2026 file -> 2026; digit 5 -> 2035).

import os
import re
import gzip
import shutil
import argparse
import concurrent.futures
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
    "price": pa.float64(),
    "size": pa.int32(),
    "correction": pa.int16(),
    "exchange": pa.int16(),
    "session_end_date": pa.string(),
}


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


def add_parsed_columns(table, session_year, cache):
    """Append root/type/expiry columns derived from the ticker column."""
    tickers = table.column('ticker')
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
    table = table.append_column('root', pc.take(pa.array(roots, pa.string()), idx))
    table = table.append_column('type', pc.take(pa.array(types, pa.string()), idx))
    table = table.append_column('expiry', pc.take(pa.array(expiries, pa.int32()), idx))
    return table


def process_file(input_filename, output_dir):
    date_str = os.path.basename(input_filename).split('.')[0]
    session_year = int(date_str[:4])
    final_dir = os.path.join(output_dir, date_str)
    tmp_dir = final_dir + '.tmp'
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir, exist_ok=True)

    print(f"processing {input_filename}")
    try:
        with gzip.open(input_filename, 'rb') as f:
            table = pacsv.read_csv(
                f, convert_options=pacsv.ConvertOptions(column_types=COLUMN_TYPES))
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

    table = add_parsed_columns(table, session_year, {})
    table = table.set_column(
        table.schema.get_field_index('timestamp'), 'timestamp',
        pc.cast(table.column('timestamp'), pa.timestamp('ns', tz='America/New_York')))
    table = table.drop_columns(['session_end_date'])  # redundant with dir name

    for root in pc.unique(table.column('root')).to_pylist():
        subset = table.filter(pc.equal(table.column('root'), root))
        subset = subset.take(
            pc.sort_indices(subset, sort_keys=[('timestamp', 'ascending')]))
        safe_root = re.sub(r'[^A-Za-z0-9]', '_', root)
        pq.write_table(
            subset, os.path.join(tmp_dir, f"{date_str}-{safe_root}.parquet"),
            compression='snappy')

    os.rename(tmp_dir, final_dir)
    print(f"Saved {final_dir} ({table.num_rows} rows)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert a dir of futures trades CSV.gz files to per-root Parquet files.")
    parser.add_argument("in_dir", type=str, help="Path to dir of input CSV.gz files.")
    parser.add_argument("out_dir", type=str, help="Path to the directory to store Parquet files.")
    parser.add_argument("--delete-original", action="store_true", help="Delete original files (default: False)")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker processes to use")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    csv_files = sorted(glob(os.path.join(args.in_dir, "*.csv.gz")), reverse=True)

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for csv_file in tqdm(csv_files):
            if os.path.getsize(csv_file) == 0:
                continue
            date_str = os.path.basename(csv_file).split('.')[0]
            if os.path.exists(os.path.join(args.out_dir, date_str)):
                if args.delete_original:
                    print(f"Deleting original {csv_file}")
                    os.remove(csv_file)
                    open(csv_file, "a").close()
                continue
            futures.append(executor.submit(process_file, csv_file, args.out_dir))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Task generated an exception: {exc}")
