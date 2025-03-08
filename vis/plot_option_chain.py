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
    Loads options data from a parquet file for the given timestamp (and optionally by underlying and expiry),
    and plots the option spread for calls and puts in different colors.
    
    This function blocks until the plot window is closed.
    
    Parameters:
      timestamp (str or pd.Timestamp): The minute timestamp to filter on.
      underlying (str, optional): Filter by underlying symbol.
      expiry (int, optional): Option expiry in YYYYMMDD format.
      dte (int, optional): Days-to-expiry. If provided, the chain expiry closest to (timestamp.date + dte days) will be used.
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

def animate_option_spread(date, underlying=None, expiry=None, dte=None, interval=0.5, nearest=False, tolerance=timedelta(minutes=5)):
    """
    Animates the option spread over the course of the day.
    
    This function loads the parquet file for the given date once, applies optional underlying filtering,
    and (if expiry is not provided) determines a single expiry to use for all frames. Then it iterates through
    all unique timestamps (sorted in ascending order) and updates the plot for calls and puts.
    
    The x-axis (strike) and y-axis (option price) limits are fixed based on the min and max values for the whole day.
    
    Parameters:
      date (str): Date in YYYY-MM-DD format. The parquet file is assumed to be at /fin/us_options_opra/minute_aggs/{date}.parquet.
      underlying (str, optional): Filter by underlying symbol.
      expiry (int, optional): Option expiry in YYYYMMDD format. If not provided, will be determined using dte or the earliest available date.
      dte (int, optional): Days-to-expiry. The expiry closest to (date + dte) will be selected.
      interval (float, optional): Pause interval (in seconds) between frames.
      nearest (bool, optional): If True, search for the nearest timestamp if an exact match is not found for a given frame.
      tolerance (timedelta, optional): Tolerance for nearest timestamp search.
    """
    date_str = date
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date_str}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return

    # Load data once for the day
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])
    
    # Optional filtering for underlying
    underlying_map = {
        "SPX": ["SPX", "SPXW"],
        "XSP": ["XSP", "XSPW"],
        "VIX": ["VIX", "VIXW"]
    }
    if underlying is not None:
        if underlying.upper() in underlying_map:
            candidates = underlying_map[underlying.upper()]
            df = df[df['underlying'].isin(candidates)]
        else:
            df = df[df['underlying'] == underlying]
    
    if df.empty:
        print("No data found for the given underlying and date.")
        return

    # Determine a fixed expiry for the animation if not provided
    # We choose expiry based on the first available timestamp of the day.
    first_time = df['window_start'].min()
    if expiry is None:
        if dte is not None:
            target_expiry_date = first_time.date() + timedelta(days=dte)
        else:
            target_expiry_date = first_time.date()
        target_dt = datetime.combine(target_expiry_date, datetime.min.time())
        expiry = choose_expiry(df, target_dt)
        print(f"Animating with expiry: {expiry} based on target date {target_dt.strftime('%Y-%m-%d')}")
    else:
        try:
            expiry = int(expiry)
        except ValueError:
            print("Expiry should be an integer in YYYYMMDD format.")
            return

    # Filter data to the chosen expiry chain
    df = df[df['expiry'] == expiry]
    if df.empty:
        print("No data available for the chosen expiry.")
        return

    # Compute global axis limits based on the entire day's data
    x_min = df['strike'].mean() - df['strike'].std()
    x_max = df['strike'].mean() + df['strike'].std()
    y_min = df['close'].min()
    y_max = df['close'].max() * 0.1

    # Get unique timestamps sorted in ascending order
    timestamps = sorted(df['window_start'].unique())
    if not timestamps:
        print("No timestamps found for animation.")
        return

    plt.ion()  # Turn on interactive mode
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for ts in timestamps:
        # Filter data for the current timestamp
        frame = df[df['window_start'] == ts]
        # If no exact match and nearest flag is set, search for the nearest timestamp within tolerance.
        if frame.empty and nearest:
            df['time_diff'] = (df['window_start'] - ts).abs()
            nearest_rows = df[df['time_diff'] <= tolerance]
            if not nearest_rows.empty:
                min_diff = nearest_rows['time_diff'].min()
                frame = df[df['time_diff'] == min_diff]
                print(f"For timestamp {ts}, using nearest available time: {frame.iloc[0]['window_start']}")
            else:
                continue  # Skip if no data within tolerance

        ax.clear()
        # Plot fixed axis limits
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min, y_max)
        
        # Separate calls and puts
        calls = frame[frame['type'] == 'C'].sort_values('strike')
        puts = frame[frame['type'] == 'P'].sort_values('strike')
        if not calls.empty:
            ax.plot(calls['strike'], calls['close'], marker='o', linestyle='-', label='Calls', color='blue')
        if not puts.empty:
            ax.plot(puts['strike'], puts['close'], marker='o', linestyle='-', label='Puts', color='red')
        ax.set_xlabel("Strike")
        ax.set_ylabel("Option Price (Close)")
        title_str = f"Option Spread at {ts.strftime('%Y-%m-%d %H:%M:%S')}"
        if underlying:
            title_str += f" - {underlying}"
        ax.set_title(title_str)
        ax.legend()
        ax.grid(True)
        plt.pause(interval)
    
    plt.ioff()
    plt.show(block=False)
    print("Animation complete.")

def main():
    parser = argparse.ArgumentParser(description="Plot or animate an options spread from minute aggregates.")
    parser.add_argument("date", help="Date for the options data (YYYY-MM-DD)")
    parser.add_argument("--time", default="12:00:00-05:00", help="Time for the options data (default: 12:00:00-05:00)")
    parser.add_argument("--underlying", help="Underlying symbol to filter (e.g., SPX, NVDA)")
    parser.add_argument("--expiry", help="Expiry as an integer in YYYYMMDD format")
    parser.add_argument("--dte", help="Days-to-expiry. The expiry closest to (date + dte) will be selected.", type=int)
    parser.add_argument("--nearest", help="If set, search for the nearest timestamp if an exact match is not found", action="store_true")
    parser.add_argument("--tolerance", help="Tolerance in minutes for the nearest timestamp search (default 5)", type=int, default=5)
    parser.add_argument("--animate", help="If set, animate the spread over the course of the day", action="store_true")
    parser.add_argument("--interval", help="Animation interval in seconds (default 0.5)", type=float, default=0.05)
    
    args = parser.parse_args()
    
    if args.animate:
        animate_option_spread(
            date=args.date,
            underlying=args.underlying,
            expiry=args.expiry,
            dte=args.dte,
            interval=args.interval,
            nearest=args.nearest,
            tolerance=timedelta(minutes=args.tolerance)
        )
    else:
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

