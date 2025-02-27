#!/usr/bin/env python3

import os
import glob
import argparse
import pandas as pd
import concurrent.futures

def drop_incomplete_intervals(group):
    # For each option group (identified by expiry, type, strike), drop all rows until we have complete data.
    condition = (
        group['last'].notna() &
        group['bid'].notna() & (group['bid'] != 0) &
        group['ask'].notna() & (group['ask'] != 0) &
        group['bid_size'].notna() & (group['bid_size'] != 0) &
        group['ask_size'].notna() & (group['ask_size'] != 0)
    )
    if condition.any():
        first_valid_idx = group.index[condition.argmax()]
        return group.loc[first_valid_idx:]
    else:
        return group.iloc[0:0]  # return empty DataFrame if no valid row

def process_options_day(underlying, date_str, quotes_file, trades_file, output_file, interval):
    # Process quotes file.
    try:
        quotes = pd.read_parquet(quotes_file)
    except Exception as e:
        print(f"Error reading quotes file {quotes_file}: {e}")
        return

    # Rename columns for consistency.
    quotes = quotes.rename(columns={'bid_price': 'bid', 'ask_price': 'ask'})
    quotes['sip_timestamp'] = pd.to_datetime(quotes['sip_timestamp'])
    # Reset index so that (expiry, type, strike) become columns.
    quotes = quotes.reset_index()

    # Group by option identifier and resample on sip_timestamp.
    quotes_agg = quotes.groupby(['expiry', 'type', 'strike']).resample(interval, on='sip_timestamp').last()
    quotes_agg = quotes_agg[['bid', 'bid_size', 'ask', 'ask_size']]

    # Process trades file.
    if os.path.exists(trades_file):
        try:
            trades = pd.read_parquet(trades_file)
        except Exception as e:
            print(f"Error reading trades file {trades_file}: {e}")
            return
        trades['sip_timestamp'] = pd.to_datetime(trades['sip_timestamp'])
        trades = trades.reset_index()
        trades_agg = trades.groupby(['expiry', 'type', 'strike']).resample(interval, on='sip_timestamp').agg(
            last=('price', 'last'),
            last_size=('size', 'last'),
            volume=('size', 'sum')
        )
    else:
        print(f"Trades file does not exist for underlying {underlying} on {date_str}. Skipping.")
        return

    # Merge aggregated trades and quotes on the multi-index (expiry, type, strike, resampled time).
    merged = pd.merge(trades_agg, quotes_agg, left_index=True, right_index=True, how='outer')
    merged = merged.sort_index()

    # Forward-fill selected columns per option group, but do NOT forward-fill volume.
    ffill_cols = ['last', 'last_size', 'bid', 'bid_size', 'ask', 'ask_size']
    merged[ffill_cols] = merged.groupby(level=[0,1,2])[ffill_cols].ffill()
    # Ensure that volume remains 0 (i.e. no forward fill) when there is no new trade.
    merged['volume'] = merged['volume'].fillna(0)

    # Drop leading intervals (per option) that don't have complete data.
    merged = merged.groupby(level=[0, 1, 2], group_keys=False).apply(drop_incomplete_intervals)
    if merged.empty:
        print(f"No complete data found for underlying {underlying} on {date_str}. Skipping.")
        return

    # Rename the resampled time level to 'window_start'.
    index_names = list(merged.index.names)
    index_names[-1] = 'window_start'
    merged.index.set_names(index_names, inplace=True)

    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        merged.to_parquet(output_file)
        print(f"Wrote {output_file}")
    except Exception as e:
        print(f"Error writing file {output_file}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Aggregate option trade and quote data over intervals.")
    parser.add_argument("--quotes_dir", type=str, default="us_options_opra/quotes", help="Directory with options quotes data")
    parser.add_argument("--trades_dir", type=str, default="us_options_opra/trades", help="Directory with options trades data")
    parser.add_argument("--output_dir", type=str, default="us_options_opra/tq_aggs", help="Output directory")
    parser.add_argument("--interval", type=int, default=10, help="Aggregation interval in seconds")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes to use")
    parser.add_argument("--underlying", type=str, default="", help="Comma-separated list of underlyings to process. If not specified, process all.")
    args = parser.parse_args()

    # Prepare a set of underlyings to filter, if provided.
    underlying_filter = None
    if args.underlying:
        underlying_filter = {u.strip() for u in args.underlying.split(',')}

    # Get date directories from the quotes directory.
    date_dirs = glob.glob(os.path.join(args.quotes_dir, "*"))
    date_dirs = sorted(date_dirs, reverse=True)  # Process most recent dates first

    interval_str = f"{args.interval}s"

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for date_path in date_dirs:
            date_str = os.path.basename(date_path)  # e.g. "2025-01-02"
            output_date_dir = os.path.join(args.output_dir, date_str)
            processed_files = set(os.listdir(output_date_dir)) if os.path.exists(output_date_dir) else set()

            quotes_files = glob.glob(os.path.join(date_path, f"{date_str}-*.parquet"))
            if not quotes_files:
                continue

            for quotes_file in quotes_files:
                filename = os.path.basename(quotes_file)
                # The file format is "YYYY-MM-DD-UNDERLYING.parquet" where UNDERLYING is the underlying identifier.
                underlying = filename[len(date_str)+1:-8]
                # If an underlying filter is provided, only process matching files.
                if underlying_filter is not None and underlying not in underlying_filter:
                    continue

                if filename in processed_files:
                    print(f"Output for {filename} already exists. Skipping.")
                    continue

                trades_file = os.path.join(args.trades_dir, date_str, f"{date_str}-{underlying}.parquet")
                output_file = os.path.join(args.output_dir, date_str, f"{date_str}-{underlying}.parquet")

                futures.append(
                    executor.submit(process_options_day, underlying, date_str, quotes_file, trades_file, output_file, interval_str)
                )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Task generated an exception: {exc}")

if __name__ == "__main__":
    main()

