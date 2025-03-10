"""
Plot Expiration Payoff for an Option Spread Trade (with OHLC4 pricing, forward/backward-filled data, and fallback lookup)

This script loads minute-aggregated options data stored as Parquet files in
/fin/us_options_opra/minute_aggs/YYYY-MM-DD.parquet. It supports trades such as:

  - iron-condor
  - iron-butterfly
  - vertical-call-credit
  - vertical-call-debit
  - vertical-put-credit
  - vertical-put-debit

Sign Convention:
  - Short leg => pos = +1  (you receive premium; costs intrinsic at expiration)
  - Long  leg => pos = -1  (you pay premium; gains intrinsic at expiration)

Thus, the expiration payoff is computed as:
  payoff(S) = initial_credit + sum( pos * (-intrinsic(S)) )
with intrinsic for a call = max(S - strike, 0) and for a put = max(strike - S, 0).

Prices for each option are computed using OHLC4 = (open+high+low+close)/4,
which is used both for estimating the underlying price and for leg valuation.
Data are forward- and backward-filled to a complete minute index.

A vertical dashed red line is drawn at the estimated underlying price (computed by
merging call and put OHLC4 prices and interpolating between strikes if needed).

Expiry dates in the data are assumed to be stored as integers in the format %y%m%d.

Example static usage:
    python3 plot_option_spread.py --trade-type iron-butterfly --center 6120 --width 20 --date 2025-02-20 --time "12:00:00-05:00" --underlying SPXW --dte 0

Example animated usage:
    python3 plot_option_spread.py --trade-type iron-butterfly --center 6120 --width 20 --date 2025-02-20 --time "12:00:00-05:00" --underlying SPXW --dte 0 --animate --interval 1
"""

import os
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta, datetime

# -------------------------------------------------------------------
# 1. Expiry Date Parsing and Selection
# -------------------------------------------------------------------

def parse_expiry_date(expiry_int):
    exp_str = str(expiry_int)
    return datetime.strptime(exp_str, "%y%m%d")

def choose_expiry(df, target_date):
    expiries = df['expiry'].unique()
    expiry_dates = [(exp, parse_expiry_date(exp)) for exp in expiries]
    future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
    if future_expiries:
        chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
    else:
        chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
    return chosen[0]

# -------------------------------------------------------------------
# 2. Loading the Option Chain Data with Forward and Backward Filling
# -------------------------------------------------------------------

def fill_forward(group, full_index):
    group = group[~group.index.duplicated(keep='first')]
    return group.reindex(full_index).ffill().bfill()

def load_chain(date, underlying, expiry, dte=None):
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return None

    # Read data and reset the index.
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    # If 'window_start' column doesn't exist, assume the first column is the time column.
    if 'window_start' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'window_start'})
    # Convert to datetime and remove timezone info.
    df['window_start'] = pd.to_datetime(df['window_start']).dt.tz_localize(None)

    df = df[df['underlying'] == underlying]
    if df.empty:
        print("No data found for the given underlying and date.")
        return None

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

    full_index = pd.date_range(df['window_start'].min(), df['window_start'].max(), freq='T')
    df = df.set_index('window_start')
    df = df.groupby(['type', 'strike'], group_keys=False).apply(lambda g: fill_forward(g, full_index))
    df.index.name = 'window_start'
    df = df.reset_index()
    return df

# -------------------------------------------------------------------
# 3. Estimating Underlying Price Using OHLC4 and Interpolation
# -------------------------------------------------------------------

def estimate_underlying_at_time_ohlc4(df, trade_time):
    frame = df[df['window_start'] == trade_time].copy()
    if frame.empty:
        return None
    frame['ohlc4'] = (frame['open'] + frame['high'] + frame['low'] + frame['close']) / 4.0
    calls = frame[frame['type'] == 'C'][['strike', 'ohlc4']]
    puts  = frame[frame['type'] == 'P'][['strike', 'ohlc4']]
    if calls.empty or puts.empty:
        return None
    merged = pd.merge(calls, puts, on='strike', suffixes=('_call', '_put'))
    if merged.empty:
        return None
    merged = merged.sort_values('strike').reset_index(drop=True)
    merged['diff'] = merged['ohlc4_call'] - merged['ohlc4_put']
    diffs = merged['diff'].values
    strikes = merged['strike'].values
    for i in range(len(diffs) - 1):
        if diffs[i] == 0:
            return strikes[i]
        if diffs[i] * diffs[i+1] < 0:
            x0, x1 = strikes[i], strikes[i+1]
            y0, y1 = diffs[i], diffs[i+1]
            if (y1 - y0) != 0:
                return x0 - y0 * ((x1 - x0) / (y1 - y0))
    idx_min = merged['diff'].abs().idxmin()
    return merged.loc[idx_min, 'strike']

# -------------------------------------------------------------------
# 4. Trade Setup: Defining Legs with Correct Sign Convention
# -------------------------------------------------------------------

def get_trade_legs(trade_type, center, width):
    legs = {}
    if trade_type == "iron_butterfly":
        legs["short_call"] = ("C", center, +1)
        legs["short_put"]  = ("P", center, +1)
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
        raise ValueError("Invalid trade type. Supported types: iron-condor, iron-butterfly, vertical-call-credit, vertical-call-debit, vertical-put-credit, vertical-put-debit.")
    return legs

# -------------------------------------------------------------------
# 5. Lookup Leg Price Using OHLC4 with Fallback
# -------------------------------------------------------------------

def lookup_leg_price_ohlc4(df, leg, trade_time):
    """
    Return the OHLC4 price for the given leg at trade_time.
    If an exact match is not found, use the price from the closest available time.
    Ensure trade_time is tz-naive.
    """
    if trade_time.tzinfo is not None:
        trade_time = trade_time.tz_localize(None)
    opt_type, strike, pos = leg
    tol = 0.001
    group = df[(df['type'] == opt_type) & (np.abs(df['strike'] - strike) < tol)]
    if group.empty:
        return np.nan
    exact = group[group['window_start'] == trade_time]
    if not exact.empty:
        row = exact.iloc[0]
        print(row)
        return (row['open'] + row['high'] + row['low'] + row['close']) / 4.0
    group = group.copy()
    group['time_diff'] = (pd.to_datetime(group['window_start']) - trade_time).abs()
    idx = group['time_diff'].idxmin()
    row = group.loc[idx]
    print(row)
    return (row['open'] + row['high'] + row['low'] + row['close']) / 4.0

# -------------------------------------------------------------------
# 6. Compute Expiration Payoff
# -------------------------------------------------------------------

def compute_expiration_payoff(legs, underlying_prices, initial_credit):
    """
    Compute the expiration payoff:
      payoff(S) = initial_credit + sum( pos * (-intrinsic(S)) )
    where intrinsic for a call = max(S - strike, 0) and for a put = max(strike - S, 0).
    """
    payoff = []
    for S in underlying_prices:
        leg_payoff = 0
        for (opt_type, strike, pos) in legs.values():
            if opt_type == "C":
                intrinsic = max(S - strike, 0)
            else:
                intrinsic = max(strike - S, 0)
            leg_payoff += pos * -intrinsic
        payoff.append(initial_credit + leg_payoff)
    return np.array(payoff)

# -------------------------------------------------------------------
# 7. Static Plot of Expiration Payoff (with Vertical Dashed Line)
# -------------------------------------------------------------------

def plot_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, nearest, tolerance):
    chain = load_chain(date, underlying, expiry, dte)
    if chain is None:
        return

    trade_time = pd.to_datetime(f"{date} {time_str}")
    print(f"Trade entry time: {trade_time}")

    legs = get_trade_legs(trade_type, center, width)
    print("Trade legs:")
    for name, leg in legs.items():
        print(f"  {name}: {leg}")

    initial_credit = 0
    for leg in legs.values():
        price = lookup_leg_price_ohlc4(chain, leg, trade_time)
        if np.isnan(price):
            print("Error: Could not retrieve price for all legs. Exiting.")
            return
        initial_credit += leg[2] * price
    print(f"Initial trade credit: {initial_credit:.2f}")

    underlying_est = estimate_underlying_at_time_ohlc4(chain, trade_time)

    S_range = np.linspace(center - 3*width, center + 3*width, 200)
    payoff = compute_expiration_payoff(legs, S_range, initial_credit)

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
# 8. Animated Plot of Expiration Payoff
# -------------------------------------------------------------------

def animate_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, nearest, tolerance, interval):
    chain = load_chain(date, underlying, expiry, dte)
    if chain is None:
        return

    # Filter timestamps to those >= trade entry time
    all_times = pd.to_datetime(chain['window_start'].unique())
    start_time = pd.to_datetime(f"{date} {time_str}").tz_localize(None)
    valid_times = sorted([t for t in all_times if t >= start_time])
    if not valid_times:
        print("No timestamps found after the trade entry time.")
        return

    legs = get_trade_legs(trade_type, center, width)
    print("Trade legs:")
    for name, leg in legs.items():
        print(f"  {name}: {leg}")

    S_range = np.linspace(center - 3*width, center + 3*width, 200)
    plt.ion()
    fig, ax = plt.subplots(figsize=(10, 6))

    for current_time in valid_times:
        if current_time.tzinfo is not None:
            current_time = current_time.tz_localize(None)

        current_credit = 0
        for leg in legs.values():
            price = lookup_leg_price_ohlc4(chain, leg, current_time)
            if np.isnan(price):
                continue
            current_credit += leg[2] * price

        payoff = compute_expiration_payoff(legs, S_range, current_credit)
        underlying_est = estimate_underlying_at_time_ohlc4(chain, current_time)

        ax.clear()
        ax.plot(S_range, payoff, 'm-', label='Payoff Curve')
        if underlying_est is not None:
            ax.axvline(x=underlying_est, color='red', linestyle='--', label='Estimated Underlying')
        ax.set_xlabel("Underlying Price at Expiration")
        ax.set_ylabel("Trade P&L")
        try:
            title_str = f"Expiration Payoff Curve\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')} | Credit: {current_credit:.2f}"
        except Exception:
            title_str = f"Expiration Payoff Curve | Credit: {current_credit:.2f}"
        ax.set_title(title_str)
        ax.grid(True)
        ax.legend()
        plt.pause(interval)

    plt.ioff()
    plt.show()

# -------------------------------------------------------------------
# 9. Command-Line Interface
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Plot or animate Expiration Payoff for an Option Spread Trade (with OHLC4 pricing and forward/backward-filled data).")
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
    parser.add_argument("--interval", type=float, default=0.5, help="Animation interval in seconds (default 0.5).")
    
    args = parser.parse_args()
    trade_type = args.trade_type.replace("-", "_")
    
    if args.animate:
        animate_expiration_payoff(trade_type, args.center, args.width,
                                  args.date, args.time, args.underlying,
                                  args.expiry, args.dte, args.nearest,
                                  timedelta(minutes=args.tolerance), args.interval)
    else:
        plot_expiration_payoff(trade_type, args.center, args.width,
                               args.date, args.time, args.underlying,
                               args.expiry, args.dte, args.nearest,
                               timedelta(minutes=args.tolerance))

if __name__ == "__main__":
    main()

