import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta, datetime

def parse_expiry_date(expiry_int):
    """
    Converts an expiry represented as an int (in YYYYMMDD format) to a datetime object.
    """
    exp_str = str(expiry_int)
    return datetime.strptime(exp_str, "%y%m%d")

def choose_expiry(filtered_df, target_date):
    """
    Given a dataframe with an 'expiry' column (as an int in YYYYMMDD format) and a target date,
    returns the expiry (as int) that is closest to the target_date and not in the past.
    If all expiries are in the past, returns the nearest anyway.
    """
    expiries = filtered_df['expiry'].unique()
    expiry_dates = [(exp, parse_expiry_date(exp)) for exp in expiries]
    # Only consider expiries that are at or after the target_date if possible
    future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
    if future_expiries:
        chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
    else:
        chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
    return chosen[0]

def plot_option_spread(timestamp, underlying=None, expiry=None, dte=None, nearest=False, tolerance=timedelta(minutes=5)):
    """
    Loads options data from a parquet file, filters it to the given timestamp
    (and optionally by underlying and expiry), and plots the option spread for calls and puts
    in different colors.

    Command-line arguments:
      timestamp (str or pd.Timestamp): The minute timestamp to filter on.
      underlying (str, optional): Filter by underlying symbol.
      expiry (int, optional): Option expiry in YYYYMMDD format.
      dte (int, optional): Days-to-expiry. If provided, the chain expiry closest to
                           (timestamp.date + dte days) will be used.
      nearest (bool, optional): If True, search for the nearest available timestamp within tolerance if an exact match isn't found.
      tolerance (timedelta, optional): Maximum time difference to consider for nearest timestamp.
    """
    # Mapping for underlyings with alternatives
    underlying_map = {
        "SPX": ["SPX", "SPXW"],
        "XSP": ["XSP", "XSPW"],
        "VIX": ["VIX", "VIXW"]
    }

    timestamp = pd.to_datetime(timestamp)
    date_str = timestamp.strftime("%Y-%m-%d")
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date_str}.parquet"

    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return

    # Load data from parquet file
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])

    # Filter for the exact timestamp
    filtered = df[df['window_start'] == timestamp]

    # If no exact match and nearest=True, search for the closest timestamp within tolerance
    if filtered.empty and nearest:
        df['time_diff'] = (df['window_start'] - timestamp).abs()
        nearest_rows = df[df['time_diff'] <= tolerance]
        if not nearest_rows.empty:
            min_diff = nearest_rows['time_diff'].min()
            filtered = df[df['time_diff'] == min_diff]
            print(f"No exact match found. Using nearest timestamp: {filtered.iloc[0]['window_start']}")
        else:
            print("No data found within the specified tolerance for the given timestamp.")
            return
    elif filtered.empty:
        print("No data found for the given timestamp.")
        return

    # Optional filtering for underlying (handles mapped alternatives)
    if underlying is not None:
        if underlying.upper() in underlying_map:
            candidates = underlying_map[underlying.upper()]
            filtered = filtered[filtered['underlying'].isin(candidates)]
        else:
            filtered = filtered[filtered['underlying'] == underlying]

    if filtered.empty:
        print("No data found after filtering by underlying.")
        return

    # Determine expiry if not provided or if dte is provided
    if expiry is None:
        if dte is not None:
            target_expiry_date = timestamp.date() + timedelta(days=dte)
        else:
            target_expiry_date = timestamp.date()
        target_dt = datetime.combine(target_expiry_date, datetime.min.time())
        expiry = choose_expiry(filtered, target_dt)
        print(f"Selected expiry: {expiry} based on target date {target_dt.strftime('%Y-%m-%d')}")
    else:
        try:
            expiry = int(expiry)
        except ValueError:
            print("Expiry should be an integer in YYYYMMDD format.")
            return

    # Filter by expiry
    filtered = filtered[filtered['expiry'] == expiry]
    if filtered.empty:
        print("No data found for the given expiry filter.")
        return

    # Separate calls and puts
    calls = filtered[filtered['type'] == 'C']
    puts = filtered[filtered['type'] == 'P']

    plt.figure(figsize=(10, 6))
    if not calls.empty:
        calls = calls.sort_values('strike')
        plt.plot(calls['strike'], calls['close'], marker='o', linestyle='-', label='Calls', color='blue')
    if not puts.empty:
        puts = puts.sort_values('strike')
        plt.plot(puts['strike'], puts['close'], marker='o', linestyle='-', label='Puts', color='red')

    plt.xlabel("Strike")
    plt.ylabel("Option Price (Close)")
    plot_time = filtered.iloc[0]['window_start']
    title_str = f"Option Spread at {plot_time.strftime('%Y-%m-%d %H:%M:%S')}"
    if underlying:
        title_str += f" - {underlying}"
    plt.title(title_str)
    plt.legend()
    plt.grid(True)
    plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot an options spread from minute aggregates.")
    # Date is required; time is optional with default as noon NYC time.
    parser.add_argument("date", help="Date for the options data (YYYY-MM-DD)")
    parser.add_argument("--time", default="12:00:00-05:00", help="Time for the options data (default: 12:00:00-05:00)")
    parser.add_argument("--underlying", help="Underlying symbol to filter (e.g., SPX, NVDA)")
    parser.add_argument("--expiry", help="Expiry as an integer in YYYYMMDD format")
    parser.add_argument("--dte", help="Days-to-expiry. The expiry closest to (date + dte) will be selected.", type=int)
    parser.add_argument("--nearest", help="If set, search for the nearest timestamp if an exact match is not found", action="store_true")
    parser.add_argument("--tolerance", help="Tolerance in minutes for the nearest timestamp search (default 5)", type=int, default=5)
    
    args = parser.parse_args()
    
    timestamp_str = f"{args.date} {args.time}"
    plot_option_spread(
        timestamp=timestamp_str,
        underlying=args.underlying,
        expiry=args.expiry,
        dte=args.dte,
        nearest=args.nearest,
        tolerance=timedelta(minutes=args.tolerance)
    )

if __name__ == "__main__":
    main()

