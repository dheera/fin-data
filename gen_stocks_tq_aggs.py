#!/usr/bin/env python3

import os
import glob
import argparse
import pandas as pd
import concurrent.futures

def aggregate_interval(df, time_col, agg_columns, interval):
    # Convert time column to datetime, sort the values, and set as index.
    df[time_col] = pd.to_datetime(df[time_col])
    #df = df.sort_values(time_col)
    df.set_index(time_col, inplace=True)
    # Resample using the specified interval.
    agg_df = df.resample(interval).last()
    if isinstance(agg_columns, list):
        agg_df = agg_df[agg_columns]
    elif isinstance(agg_columns, dict):
        agg_df = agg_df[list(agg_columns.keys())]
        agg_df = agg_df.rename(columns=agg_columns)
    return agg_df

def process_day(ticker, date_str, quotes_file, trades_file, output_file, interval):
    # Process quotes file.
    try:
        quotes = pd.read_parquet(quotes_file)
    except Exception as e:
        print(f"Error reading quotes file {quotes_file}: {e}")
        return

    # Rename columns for consistency: bid_price -> bid, ask_price -> ask.
    quotes = quotes.rename(columns={'bid_price': 'bid', 'ask_price': 'ask'})
    quotes_agg = aggregate_interval(quotes, 'sip_timestamp',
                                    ['bid', 'bid_size', 'ask', 'ask_size'], interval)

    # Process trades file.
    if os.path.exists(trades_file):
        try:
            trades = pd.read_parquet(trades_file)
        except Exception as e:
            print(f"Error reading trades file {trades_file}: {e}")
            return
        # Use named aggregation to get the last trade price, last trade size, and sum of sizes as volume.
        trades['sip_timestamp'] = pd.to_datetime(trades['sip_timestamp'])
        #trades = trades.sort_values('sip_timestamp').set_index('sip_timestamp')
        trades = trades.set_index('sip_timestamp')
        trades_agg = trades.resample(interval).agg(
            last=('price', 'last'),
            last_size=('size', 'last'),
            volume=('size', 'sum')
        )
    else:
        print(f"Trades file does not exist for {ticker} on {date_str}. Skipping.")
        return

    # Merge the aggregated trades and quotes data on the time index.
    merged = pd.merge(trades_agg, quotes_agg, left_index=True, right_index=True, how='outer')
    merged = merged.sort_index()

    # Forward-fill missing data to propagate the last observed values.
    merged = merged.ffill()

    # Drop leading intervals until we have complete valid data.
    condition = (
        merged['last'].notna() &
        merged['bid'].notna() & (merged['bid'] != 0) &
        merged['ask'].notna() & (merged['ask'] != 0) &
        merged['bid_size'].notna() & (merged['bid_size'] != 0) &
        merged['ask_size'].notna() & (merged['ask_size'] != 0)
    )
    if condition.any():
        first_valid_index = merged.index[condition.argmax()]
        merged = merged.loc[first_valid_index:]
    else:
        print(f"No complete data found for {ticker} on {date_str}. Skipping.")
        return

    # Rename index to window_start.
    merged.index.rename('window_start', inplace=True)

    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        merged.to_parquet(output_file)
        print(f"Wrote {output_file}")
    except Exception as e:
        print(f"Error writing file {output_file}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Aggregate trade and quote data over intervals.")
    parser.add_argument("--quotes_dir", type=str, default="us_stocks_sip/quotes", help="Directory with quotes data")
    parser.add_argument("--trades_dir", type=str, default="us_stocks_sip/trades", help="Directory with trades data")
    parser.add_argument("--output_dir", type=str, default="us_stocks_sip/tq_aggs", help="Output directory")
    parser.add_argument("--interval", type=int, default=10, help="Aggregation interval in seconds")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker processes to use")
    args = parser.parse_args()

    # Get date directories from the quotes directory.
    date_dirs = glob.glob(os.path.join(args.quotes_dir, "*"))
    date_dirs = sorted(date_dirs, reverse=True)  # Process most recent dates first

    interval_str = f"{args.interval}s"

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for date_path in date_dirs:
            date_str = os.path.basename(date_path)  # e.g. "2024-11-27"
            output_date_dir = os.path.join(args.output_dir, date_str)
            processed_files = set(os.listdir(output_date_dir)) if os.path.exists(output_date_dir) else set()

            quotes_files = glob.glob(os.path.join(date_path, f"{date_str}-*.parquet"))
            if not quotes_files:
                continue

            for quotes_file in quotes_files:
                filename = os.path.basename(quotes_file)
                if filename in processed_files:
                    print(f"Output for {filename} already exists. Skipping.")
                    continue

                ticker = filename[len(date_str)+1:-8]
                trades_file = os.path.join(args.trades_dir, date_str, f"{date_str}-{ticker}.parquet")
                output_file = os.path.join(args.output_dir, date_str, f"{date_str}-{ticker}.parquet")

                futures.append(
                    executor.submit(process_day, ticker, date_str, quotes_file, trades_file, output_file, interval_str)
                )

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Task generated an exception: {exc}")

if __name__ == "__main__":
    main()

