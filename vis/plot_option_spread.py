"""
Plot Expiration Payoff for an Option Spread Trade (Sign Convention Fixed)

This script loads minute-aggregated options data stored as Parquet files in
/fin/us_options_opra/minute_aggs/YYYY-MM-DD.parquet. It supports trades like:

  - iron-condor
  - iron-butterfly
  - vertical-call-credit
  - vertical-call-debit
  - vertical-put-credit
  - vertical-put-debit

Sign Convention:
  - Short leg => pos = +1  (adds to initial credit, costs intrinsic at expiration)
  - Long  leg => pos = -1  (subtracts from initial credit, gains intrinsic at expiration)

Thus, the final expiration payoff for each leg is: pos * (-intrinsic).
The total payoff is: initial_credit + sum_over_legs( pos * -intrinsic ).

Features:
  - Static plot of the expiration payoff at a specified date/time.
  - Animated version that updates the payoff curve over each minute's data.
  - A vertical dashed red line indicates the estimated underlying price (based on the call/put cross).

Example:
    python3 plot_option_spread.py --underlying SPXW --date 2025-02-18 --trade-type iron-butterfly --center 6125 --width 20 --dte 2 --time "09:30-0500" --interval 1
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta, datetime

# -------------------------------------------------------------------
# 1. Expiry date parsing (%y%m%d) and selection
# -------------------------------------------------------------------

def parse_expiry_date(expiry_int):
    """
    Convert an expiry (int) in %y%m%d format to a datetime object.
    
    Parameters:
      expiry_int (int): Expiry date as an integer in %y%m%d format.
    
    Returns:
      datetime: Parsed expiry date.
    """
    exp_str = str(expiry_int)
    return datetime.strptime(exp_str, "%y%m%d")

def choose_expiry(df, target_date):
    """
    Given a DataFrame with an 'expiry' column (in %y%m%d ints) and a target date,
    select the expiry closest to the target date (preferring expiries not in the past).
    
    Parameters:
      df (pd.DataFrame): DataFrame containing an 'expiry' column.
      target_date (datetime): Target date for expiry selection.
    
    Returns:
      int: Chosen expiry as an integer in %y%m%d format.
    """
    expiries = df['expiry'].unique()
    expiry_dates = [(exp, parse_expiry_date(exp)) for exp in expiries]
    future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
    if future_expiries:
        chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
    else:
        chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
    return chosen[0]

# -------------------------------------------------------------------
# 2. Loading the chain data
# -------------------------------------------------------------------

def load_chain(date, underlying, expiry, dte=None):
    """
    Load the option chain data for the given day from the parquet file,
    filter by underlying and expiry (or choose expiry using dte if not provided).
    
    Parameters:
      date (str): Date in YYYY-MM-DD format.
      underlying (str): Underlying symbol.
      expiry (int or None): Expiry in %y%m%d format.
      dte (int or None): Days-to-expiry for selecting the expiry.
    
    Returns:
      pd.DataFrame or None: Filtered option chain DataFrame, or None if not found.
    """
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return None

    df = pd.read_parquet(parquet_file)
    df = df.reset_index()

    # After setting window_start as datetime:
    df['window_start'] = pd.to_datetime(df['window_start'])

    # Create a complete minute index from the earliest to the latest timestamp in the data
    full_index = pd.date_range(df['window_start'].min(), df['window_start'].max(), freq='T')

    # Set window_start as the index and forward fill for each option (group by type and strike)
    df = df.set_index('window_start')
    df = df.groupby(['type', 'strike'], group_keys=False).apply(lambda g: g.reindex(full_index).ffill())
    df = df.reset_index().rename(columns={'index': 'window_start'})

    # Filter by underlying (exact match)
    df = df[df['underlying'] == underlying]
    if df.empty:
        print("No data found for the given underlying and date.")
        return None

    # Determine expiry if not provided
    if expiry is None:
        first_time = df['window_start'].min()
        if dte is not None:
            target_expiry_date = first_time.date() + timedelta(days=dte)
        else:
            target_expiry_date = first_time.date()
        target_dt = datetime.combine(target_expiry_date, datetime.min.time())
        expiry = choose_expiry(df, target_dt)
        print(f"Chosen expiry: {expiry}")
    else:
        try:
            expiry = int(expiry)
        except ValueError:
            print("Expiry should be an integer in %y%m%d format.")
            return None

    df = df[df['expiry'] == expiry]
    if df.empty:
        print("No data for the chosen expiry.")
        return None
    return df

# -------------------------------------------------------------------
# 3. Estimating the underlying price at a given timestamp
# -------------------------------------------------------------------

def estimate_underlying_at_time(df, trade_time):
    """
    Estimate the underlying price at the given trade_time by:
      1. Computing OHLC4 = (open + high + low + close) / 4 for each option.
      2. Merging calls and puts by strike.
      3. Finding where the difference in OHLC4 (call - put) crosses zero between strikes.
      4. If no sign crossing is found, pick the strike with the minimal absolute difference.
    
    Parameters:
      df (pd.DataFrame): Option chain data. Must have columns:
                        ['type', 'strike', 'open', 'high', 'low', 'close', 'window_start'].
      trade_time (pd.Timestamp): The timestamp at which to estimate the underlying.
    
    Returns:
      float or None: Estimated underlying price, or None if not possible.
    """
    # Filter to the exact trade_time
    frame = df[df['window_start'] == trade_time].copy()
    if frame.empty:
        return None
    
    # We need both calls and puts in the same time slice
    calls = frame[frame['type'] == 'C'].copy()
    puts  = frame[frame['type'] == 'P'].copy()
    if calls.empty or puts.empty:
        return None
    
    # Compute OHLC4 for each row
    for subset in (calls, puts):
        subset['ohlc4'] = (subset['open'] + subset['high'] + subset['low'] + subset['close']) / 4.0
    
    # Merge calls and puts on 'strike'
    merged = pd.merge(calls[['strike','ohlc4']], puts[['strike','ohlc4']], on='strike',
                      suffixes=('_call','_put'))
    if merged.empty:
        return None
    
    # Sort by strike
    merged = merged.sort_values('strike').reset_index(drop=True)
    # Compute the difference in OHLC4
    merged['diff'] = merged['ohlc4_call'] - merged['ohlc4_put']
    
    # We look for a sign change in consecutive strikes
    # i.e. diff[i] and diff[i+1] have opposite signs => zero crossing
    diffs = merged['diff'].values
    strikes = merged['strike'].values
    
    crossing_index = None
    for i in range(len(diffs) - 1):
        if diffs[i] == 0:
            # Perfect match
            return strikes[i]
        if diffs[i] * diffs[i+1] < 0:
            # Sign crossing between i and i+1
            crossing_index = i
            break
    
    if crossing_index is not None:
        # Linear interpolation:
        # We want to find strike X s.t. diff(X) = 0 between strikes[i] and strikes[i+1].
        # Let x0 = strikes[i], x1 = strikes[i+1]
        # Let y0 = diffs[i],    y1 = diffs[i+1]
        # We solve y(X) = 0 => X = x0 - y0 * ( (x1 - x0) / (y1 - y0) )
        i2 = crossing_index + 1
        x0, x1 = strikes[crossing_index], strikes[i2]
        y0, y1 = diffs[crossing_index], diffs[i2]
        if (y1 - y0) != 0:
            x_star = x0 - y0 * ( (x1 - x0) / (y1 - y0) )
            return x_star
        else:
            # degenerate case: same diff => no actual sign crossing
            pass
    
    # If no sign crossing, pick the strike with the minimal absolute difference
    idx_min = merged['diff'].abs().idxmin()
    return merged.loc[idx_min, 'strike']

# -------------------------------------------------------------------
# 4. Defining trades with sign convention: short => +1, long => -1
# -------------------------------------------------------------------

def get_trade_legs(trade_type, center, width):
    """
    Return a dictionary of legs with (option_type, strike, pos).
    Short => pos=+1, Long => pos=-1.
    
    Examples:
      iron_butterfly:
        short_call: (C, center, +1)
        short_put:  (P, center, +1)
        long_call:  (C, center+width, -1)
        long_put:   (P, center-width, -1)
    """
    legs = {}
    if trade_type == "iron_butterfly":
        # short center => pos=+1
        legs["short_call"] = ("C", center, +1)
        legs["short_put"]  = ("P", center, +1)
        # long wings => pos=-1
        legs["long_call"]  = ("C", center + width, -1)
        legs["long_put"]   = ("P", center - width, -1)

    elif trade_type == "iron_condor":
        legs["short_call"] = ("C", center + width/2, +1)
        legs["long_call"]  = ("C", center + 3*width/2, -1)
        legs["short_put"]  = ("P", center - width/2, +1)
        legs["long_put"]   = ("P", center - 3*width/2, -1)

    elif trade_type == "vertical_call_credit":
        legs["short_call"] = ("C", center, +1)
        legs["long_call"]  = ("C", center + width, -1)

    elif trade_type == "vertical_call_debit":
        legs["long_call"]  = ("C", center, -1)
        legs["short_call"] = ("C", center + width, +1)

    elif trade_type == "vertical_put_credit":
        legs["short_put"] = ("P", center, +1)
        legs["long_put"]  = ("P", center - width, -1)

    elif trade_type == "vertical_put_debit":
        legs["long_put"]  = ("P", center, -1)
        legs["short_put"] = ("P", center - width, +1)

    else:
        raise ValueError(
            "Invalid trade type. Supported: iron-condor, iron-butterfly, "
            "vertical-call-credit, vertical-call-debit, vertical-put-credit, vertical-put-debit."
        )
    return legs

# -------------------------------------------------------------------
# 5. Lookup leg price
# -------------------------------------------------------------------

def lookup_leg_price(df, leg, trade_time):
    """
    Return the OHLC4 price for a given leg at trade_time.
    If no row is found, return NaN.
    """
    opt_type, strike, pos = leg
    # Filter by type & strike
    tol = 0.001
    frame = df[
        (df['type'] == opt_type) &
        (abs(df['strike'] - strike) < tol) &
        (df['window_start'] == trade_time)
    ]
    if frame.empty:
        return np.nan
    
    # Compute OHLC4
    row = frame.iloc[0]
    ohlc4 = (row['open'] + row['high'] + row['low'] + row['close']) / 4.0
    return ohlc4

# -------------------------------------------------------------------
# 6. Compute expiration payoff
# -------------------------------------------------------------------

def compute_expiration_payoff(legs, underlying_prices, initial_credit):
    """
    For each leg: short => pos=+1 => at expiration cost = intrinsic, so payoff = -intrinsic
                  long  => pos=-1 => at expiration gain = +intrinsic, so payoff = +intrinsic
    => Leg payoff at expiration = pos * (-intrinsic).
    
    Then total payoff = initial_credit + sum(pos * -intrinsic).
    """
    payoff = []
    for S in underlying_prices:
        leg_payoff = 0
        for (opt_type, strike, pos) in legs.values():
            if opt_type == "C":
                intrinsic = max(S - strike, 0)
            else:
                intrinsic = max(strike - S, 0)
            # pos=+1 => short => final payoff = -intrinsic
            # pos=-1 => long => final payoff = +intrinsic
            # => sum = pos * -intrinsic
            leg_payoff += pos * -intrinsic
        payoff.append(initial_credit + leg_payoff)
    return np.array(payoff)

# -------------------------------------------------------------------
# 7. Static plot of expiration payoff (with vertical dashed line)
# -------------------------------------------------------------------

def plot_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, nearest, tolerance):
    """
    Plot the expiration payoff curve at the given trade_time. 
    Also draw a vertical dashed red line for the estimated underlying price.
    """
    chain = load_chain(date, underlying, expiry, dte)
    if chain is None:
        return

    trade_time = pd.to_datetime(f"{date} {time_str}")
    print(f"Trade entry time: {trade_time}")

    # Build the trade legs with the new sign convention
    legs = get_trade_legs(trade_type, center, width)
    print("Trade legs:")
    for name, leg in legs.items():
        print(f"  {name}: {leg}")

    # Sum up the initial credit
    initial_credit = 0
    for leg in legs.values():
        price = lookup_leg_price(chain, leg, trade_time)
        if np.isnan(price):
            print("Error: Could not retrieve price for all legs. Exiting.")
            return
        initial_credit += leg[2] * price  # short => +1 => add, long => -1 => subtract
    print(f"Initial trade credit: {initial_credit:.2f}")

    # Estimate underlying at trade_time
    underlying_est = estimate_underlying_at_time(chain, trade_time)

    # Build a range of underlying prices for expiration
    S_range = np.linspace(center - 3*width, center + 3*width, 200)
    payoff = compute_expiration_payoff(legs, S_range, initial_credit)

    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(S_range, payoff, 'm-', label='Payoff Curve')
    if underlying_est is not None:
        plt.axvline(x=underlying_est, color='red', linestyle='--', label='Estimated Underlying')
    plt.xlabel("Underlying Price at Expiration")
    plt.ylabel("Trade P&L")
    plt.title("Expiration Payoff Curve")
    plt.grid(True)
    plt.legend()
    plt.show()

# -------------------------------------------------------------------
# 8. Animated plot of expiration payoff
# -------------------------------------------------------------------

def animate_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, nearest, tolerance, interval):
    """
    Animate the expiration payoff curve over the course of the day.
    Recompute initial credit at each timestamp. 
    Plot a vertical dashed line for the estimated underlying.
    """
    chain = load_chain(date, underlying, expiry, dte)
    if chain is None:
        return

    timestamps = sorted(chain['window_start'].unique())
    if not timestamps:
        print("No timestamps found.")
        return

    legs = get_trade_legs(trade_type, center, width)
    print("Trade legs:")
    for name, leg in legs.items():
        print(f"  {name}: {leg}")

    # Underlying price range for the payoff
    S_range = np.linspace(center - 3*width, center + 3*width, 200)

    plt.ion()
    fig, ax = plt.subplots(figsize=(10, 6))

    start_time = pd.to_datetime(f"{date} {time_str}")
    
    for ts in timestamps:
        current_time = pd.to_datetime(ts)
        if current_time < start_time:
            continue
        
        # Recompute the initial credit at current_time
        current_credit = 0
        for leg in legs.values():
            price = lookup_leg_price(chain, leg, current_time)
            if np.isnan(price):
                continue
            current_credit += leg[2] * price
        
        # Expiration payoff
        payoff = compute_expiration_payoff(legs, S_range, current_credit)
        # Estimate underlying
        underlying_est = estimate_underlying_at_time(chain, current_time)

        ax.clear()
        ax.plot(S_range, payoff, 'm-', label='Payoff Curve')
        if underlying_est is not None:
            ax.axvline(x=underlying_est, color='red', linestyle='--', label='Estimated Underlying')
        ax.set_xlabel("Underlying Price at Expiration")
        ax.set_ylabel("Trade P&L")
        ax.set_title(
            f"Expiration Payoff Curve\n"
            f"Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}  |  Credit: {current_credit:.2f}"
        )
        ax.grid(True)
        ax.legend()
        plt.pause(interval)

    plt.ioff()
    plt.show()

# -------------------------------------------------------------------
# 9. Main (CLI) 
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot or animate Expiration Payoff for an Option Spread Trade (with corrected sign convention).")
    parser.add_argument("--trade-type", required=True,
                        choices=["iron-condor", "iron-butterfly",
                                 "vertical-call-credit", "vertical-call-debit",
                                 "vertical-put-credit", "vertical-put-debit"],
                        help="Type of trade (hyphenated).")
    parser.add_argument("--center", required=True, type=float, help="Center strike for the trade.")
    parser.add_argument("--width", required=True, type=float, help="Width of the spread.")
    parser.add_argument("--date", required=True, help="Date for the options data (YYYY-MM-DD).")
    parser.add_argument("--time", default="12:00:00-05:00", help="Trade entry time (default: 12:00:00-05:00).")
    parser.add_argument("--underlying", required=True, help="Underlying symbol (e.g., SPXW).")
    parser.add_argument("--expiry", help="Expiry as an integer in %y%m%d format (optional).")
    parser.add_argument("--dte", type=int, help="Days-to-expiry for selecting the expiry chain (optional).")
    parser.add_argument("--nearest", action="store_true", help="Use nearest timestamp matching if exact match not found.")
    parser.add_argument("--tolerance", type=int, default=5, help="Tolerance in minutes for nearest timestamp search (default 5).")
    parser.add_argument("--animate", action="store_true", help="Animate the expiration payoff curve over the day.")
    parser.add_argument("--interval", type=float, default=0.05, help="Animation interval in seconds (default 0.5).")
    
    args = parser.parse_args()
    
    # Convert trade type from hyphenated to underscore for internal usage
    trade_type = args.trade_type.replace("-", "_")
    
    if args.animate:
        animate_expiration_payoff(trade_type, args.center, args.width,
                                  args.date, args.time, args.underlying,
                                  args.expiry, args.dte,
                                  args.nearest, timedelta(minutes=args.tolerance),
                                  args.interval)
    else:
        plot_expiration_payoff(trade_type, args.center, args.width,
                               args.date, args.time, args.underlying,
                               args.expiry, args.dte,
                               args.nearest, timedelta(minutes=args.tolerance))

if __name__ == "__main__":
    main()

