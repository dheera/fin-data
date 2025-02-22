#!/usr/bin/env python3
#!/usr/bin/env python3
import os
import glob
import argparse
import pandas as pd

def aggregate_interval(df, time_col, agg_columns, interval):
    # Convert time column to datetime, sort the values, and set as index.
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.sort_values(time_col)
    df.set_index(time_col, inplace=True)
    # Resample using the specified interval and take the last record in each bin.
    agg_df = df.resample(interval).last()
    if isinstance(agg_columns, list):
        agg_df = agg_df[agg_columns]
    elif isinstance(agg_columns, dict):
        agg_df = agg_df[list(agg_columns.keys())]
        agg_df = agg_df.rename(columns=agg_columns)
    return agg_df

def process_day(ticker, date_str, quotes_file, trades_file, output_file, interval):
    # Process quotes file
    try:
        quotes = pd.read_parquet(quotes_file)
    except Exception as e:
        print(f"Error reading quotes file {quotes_file}: {e}")
        return

    # Rename columns for consistency: bid_price -> bid, ask_price -> ask.
    quotes = quotes.rename(columns={'bid_price': 'bid', 'ask_price': 'ask'})
    quotes_agg = aggregate_interval(quotes, 'sip_timestamp',
                                    ['bid', 'bid_size', 'ask', 'ask_size'], interval)

    # Process trades file if it exists; otherwise, skip this ticker.
    if os.path.exists(trades_file):
        try:
            trades = pd.read_parquet(trades_file)
        except Exception as e:
            print(f"Error reading trades file {trades_file}: {e}")
            return
        trades_agg = aggregate_interval(trades, 'sip_timestamp',
                                        {'price': 'last', 'size': 'last_size'}, interval)
    else:
        print(f"Trades file does not exist for {ticker} on {date_str}. Skipping.")
        return

    # Merge the aggregated trades and quotes on the time index.
    merged = pd.merge(trades_agg, quotes_agg, left_index=True, right_index=True, how='outer')
    merged = merged.sort_index()

    # Forward-fill missing data to propagate the last observed values.
    merged = merged.ffill()

    # Define condition for a valid row:
    # - Trade values must be non-null.
    # - Quote values must be non-null and nonzero.
    condition = (
        merged['last'].notna() &
        merged['bid'].notna() & (merged['bid'] != 0) &
        merged['ask'].notna() & (merged['ask'] != 0) &
        merged['bid_size'].notna() & (merged['bid_size'] != 0) &
        merged['ask_size'].notna() & (merged['ask_size'] != 0)
    )
    if condition.any():
        # Find the first valid index and restrict the DataFrame from that point onward.
        first_valid_index = merged.index[condition.argmax()]
        merged = merged.loc[first_valid_index:]
    else:
        print(f"No complete data found for {ticker} on {date_str}. Skipping.")
        return

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
    parser.add_argument("--output_dir", type=str, default="aggregated", help="Output directory")
    parser.add_argument("--interval", type=int, default=10, help="Aggregation interval in seconds")
    args = parser.parse_args()

    # Get date directories from the quotes directory.
    date_dirs = glob.glob(os.path.join(args.quotes_dir, "*"))
    date_dirs = sorted(date_dirs, reverse=True)  # Process most recent dates first

    # Use lowercase 's' for seconds to avoid FutureWarning.
    interval_str = f"{args.interval}s"

    for date_path in date_dirs:
        date_str = os.path.basename(date_path)  # e.g., "2024-11-27"
        # Find all ticker files for this date.
        quotes_files = glob.glob(os.path.join(date_path, f"{date_str}-*.parquet"))
        if not quotes_files:
            continue

        for quotes_file in quotes_files:
            filename = os.path.basename(quotes_file)
            # Confirm the filename starts with the date plus a dash.
            if not filename.startswith(date_str + "-"):
                print(f"Skipping file with unexpected name format: {quotes_file}")
                continue

            # Extract the ticker by removing the date and the trailing ".parquet".
            # For example: "2024-11-27-AACT.WS.parquet" becomes "AACT.WS"
            ticker = filename[len(date_str)+1:-8]

            # Build matching trades file path assuming the same structure.
            trades_file = os.path.join(args.trades_dir, date_str, f"{date_str}-{ticker}.parquet")
            output_file = os.path.join(args.output_dir, date_str, f"{date_str}-{ticker}.parquet")
            if os.path.exists(output_file):
                print(f"Output for {ticker} on {date_str} already exists. Skipping.")
                continue

            process_day(ticker, date_str, quotes_file, trades_file, output_file, interval_str)

if __name__ == "__main__":
    main()

