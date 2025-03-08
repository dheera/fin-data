#!/usr/bin/env python3

"""
Plot/Animate Options Spread from Minute Aggregates

This script loads options data stored in Parquet files organized by date (YYYY-MM-DD.parquet)
from the directory /fin/us_options_opra/minute_aggs/. Each file contains minute-aggregated options data 
with a multi-index that includes columns such as 'underlying', 'expiry', 'type', 'strike', and 'window_start'.
Expiry values are assumed to be integers in YYYYMMDD format.

Example of expected data (after resetting index):

   underlying  expiry type  strike              window_start        ticker  volume   open  close   high    low  transactions
0       NVDA  20250221    C   250.0 2025-02-14 12:00:00-05:00  O:NVDA20250221C00250000      100   10.0   10.5   10.7   10.0             5
1       NVDA  20250221    P   250.0 2025-02-14 12:00:00-05:00  O:NVDA20250221P00250000       80    9.8    9.5    9.9    9.3             4
...
(Additional rows with various strikes and window_start timestamps)

Features:
- Static plot: Plot the option spread (calls and puts in different colors) at a specific timestamp.
- Animated plot: Animate the spread over the day. The x-axis (strike) and y-axis (option price) limits are fixed based on the min and max values for the whole day.
- Command-line interface using argparse:
  - Required "date" (YYYY-MM-DD) and optional "time" (default: 12:00:00-05:00).
  - Optional underlying filter (e.g., NVDA, SPX) with support for alternatives (e.g., SPX filters SPX and SPXW).
  - Expiry can be specified as an integer (YYYYMMDD) or determined based on a days-to-expiry (dte) parameter.
  - Option to animate over the day with a configurable frame interval.

Example command-line usage for static plot:
    python3 plot_chain.py 2025-02-14 --time "12:00:00-05:00" --underlying NVDA --dte 7

Example command-line usage for animation:
    python3 plot_chain.py 2025-02-14 --underlying NVDA --animate --interval 0.5

"""

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta, datetime

def parse_expiry_date(expiry_int):
    """
    Convert an expiry value in YYYYMMDD integer format to a datetime object.
    
    Parameters:
        expiry_int (int): Expiry date as an integer in YYYYMMDD format.
        
    Returns:
        datetime: The expiry date as a datetime object.
    """
    exp_str = str(expiry_int)
    return datetime.strptime(exp_str, "%Y%m%d")

def choose_expiry(filtered_df, target_date):
    """
    Given a dataframe with an 'expiry' column (integers in YYYYMMDD format) and a target date,
    returns the expiry that is closest to the target date (and not in the past if possible).
    
    Parameters:
        filtered_df (pd.DataFrame): DataFrame containing an 'expiry' column.
        target_date (datetime): The target date to compare against.
        
    Returns:
        int: The chosen expiry as an integer.
    """
    expiries = filtered_df['expiry'].unique()
    # Create a list of tuples: (expiry integer, corresponding datetime)
    expiry_dates = [(exp, parse_expiry_date(exp)) for exp in expiries]
    # Filter for expiries that are on or after the target date, if any exist
    future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
    if future_expiries:
        chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
    else:
        chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
    return chosen[0]

def plot_option_spread(timestamp, underlying=None, expiry=None, dte=None, nearest=False, tolerance=timedelta(minutes=5)):
    """
    Load options data from the day's parquet file for a given timestamp, filter by underlying and expiry,
    and plot the option spread (calls in blue, puts in red) for that moment.
    
    This function displays a static plot and blocks until the window is closed.
    
    Parameters:
        timestamp (str or pd.Timestamp): The timestamp to filter on.
        underlying (str, optional): Underlying symbol filter.
        expiry (int, optional): Option expiry in YYYYMMDD format.
        dte (int, optional): Days-to-expiry; if provided, selects the expiry closest to (timestamp.date + dte).
        nearest (bool, optional): If True, use the nearest timestamp within tolerance if exact match is not found.
        tolerance (timedelta, optional): Tolerance for nearest timestamp search.
    """
    # Mapping for underlyings with alternative symbols (e.g., SPX -> SPX and SPXW)
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

    # Load the day's parquet file and reset index for easier filtering
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])

    # Filter rows that match the exact timestamp
    filtered = df[df['window_start'] == timestamp]

    # If no exact match and 'nearest' flag is set, try to find the closest timestamp within tolerance
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

    # Filter by underlying, using mapped alternatives if applicable
    if underlying is not None:
        if underlying.upper() in underlying_map:
            candidates = underlying_map[underlying.upper()]
            filtered = filtered[filtered['underlying'].isin(candidates)]
        else:
            filtered = filtered[filtered['underlying'] == underlying]

    if filtered.empty:
        print("No data found after filtering by underlying.")
        return

    # Determine expiry if not provided (or if dte is provided)
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

    # Filter by the chosen expiry
    filtered = filtered[filtered['expiry'] == expiry]
    if filtered.empty:
        print("No data found for the given expiry filter.")
        return

    # Separate calls and puts data
    calls = filtered[filtered['type'] == 'C']
    puts = filtered[filtered['type'] == 'P']

    # Create the plot
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
    Animate the option spread over the course of a day.
    
    This function loads the day's parquet file once, applies optional underlying filtering,
    and (if expiry is not provided) selects a fixed expiry chain to use for all frames.
    It then determines global x-axis (strike) and y-axis (option price) limits based on the entire day's data.
    The plot is updated for each unique timestamp (in ascending order) with a pause defined by 'interval'.
    The interactive mode is used so the animation does not block.
    
    Parameters:
        date (str): Date in YYYY-MM-DD format.
        underlying (str, optional): Underlying symbol filter.
        expiry (int, optional): Option expiry in YYYYMMDD format.
        dte (int, optional): Days-to-expiry for selecting the expiry chain.
        interval (float, optional): Pause interval (seconds) between frames.
        nearest (bool, optional): If True, use the nearest timestamp within tolerance if an exact match is not found.
        tolerance (timedelta, optional): Tolerance for nearest timestamp search.
    """
    date_str = date
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date_str}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return

    # Load data for the whole day and reset index for filtering
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])
    
    # Filter by underlying using mapped alternatives if provided
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

    # Determine a fixed expiry for the animation if not provided, using the earliest timestamp's date
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

    # Filter data to only include the chosen expiry chain
    df = df[df['expiry'] == expiry]
    if df.empty:
        print("No data available for the chosen expiry.")
        return

    # Compute fixed x (strike) and y (close price) axis limits for the entire day
    x_min = df['strike'].mean() - df['strike'].std()
    x_max = df['strike'].mean() + df['strike'].std()
    y_min = max(df['close'].mean() - df['close'].std(), -0.1)
    y_max = df['close'].mean() + df['close'].std()

    # Get unique timestamps (frames) sorted in ascending order
    timestamps = sorted(df['window_start'].unique())
    if not timestamps:
        print("No timestamps found for animation.")
        return

    plt.ion()  # Enable interactive mode for non-blocking animation
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for ts in timestamps:
        # Filter the data for the current timestamp (frame)
        frame = df[df['window_start'] == ts]
        # If exact match is not found and nearest flag is set, try to get nearest frame within tolerance
        if frame.empty and nearest:
            df['time_diff'] = (df['window_start'] - ts).abs()
            nearest_rows = df[df['time_diff'] <= tolerance]
            if not nearest_rows.empty:
                min_diff = nearest_rows['time_diff'].min()
                frame = df[df['time_diff'] == min_diff]
                print(f"For timestamp {ts}, using nearest available time: {frame.iloc[0]['window_start']}")
            else:
                continue  # Skip this frame if no data within tolerance

        ax.clear()
        # Set fixed axis limits for each frame
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min, y_max)
        
        # Separate and sort calls and puts data for plotting
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
    # Set up the command-line argument parser
    parser = argparse.ArgumentParser(description="Plot or animate an options spread from minute aggregates.")
    parser.add_argument("date", help="Date for the options data (YYYY-MM-DD)")
    parser.add_argument("--time", default="12:00:00-05:00", help="Time for the options data (default: 12:00:00-05:00)")
    parser.add_argument("--underlying", help="Underlying symbol to filter (e.g., SPX, NVDA)")
    parser.add_argument("--expiry", help="Expiry as an integer in YYYYMMDD format")
    parser.add_argument("--dte", help="Days-to-expiry. The expiry closest to (date + dte) will be selected.", type=int)
    parser.add_argument("--nearest", help="If set, search for the nearest timestamp if an exact match is not found", action="store_true")
    parser.add_argument("--tolerance", help="Tolerance in minutes for nearest timestamp search (default 5)", type=int, default=5)
    parser.add_argument("--animate", help="If set, animate the spread over the course of the day", action="store_true")
    parser.add_argument("--interval", help="Animation interval in seconds (default 0.5)", type=float, default=0.05)
    
    args = parser.parse_args()
    
    # If animation flag is set, call the animation function; otherwise, produce a static plot.
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

