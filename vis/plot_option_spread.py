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
import argparse
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from option_price_provider import OptionPriceProvider
from option_spread import OptionSpread

def plot_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, tolerance):
    # Create the price provider instance.
    price_provider = OptionPriceProvider(date, underlying, expiry, dte)
    trade_time = pd.to_datetime(f"{date} {time_str}")
    print(f"Trade entry time: {trade_time}")

    # Create the option spread instance.
    spread = OptionSpread(trade_type, center, width, price_provider)
    initial_credit = spread.get_initial_credit(trade_time)
    print(f"Initial trade credit: {initial_credit:.2f}")

    underlying_est = price_provider.estimate_underlying(trade_time)
    S_range = np.linspace(center - 3*width, center + 3*width, 200)
    payoff = spread.compute_expiration_payoff(S_range, trade_time)

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

def animate_expiration_payoff(trade_type, center, width, date, time_str, underlying, expiry, dte, tolerance, interval):
    price_provider = OptionPriceProvider(date, underlying, expiry, dte)
    all_times = pd.to_datetime(price_provider.data['window_start'].unique())
    start_time = pd.to_datetime(f"{date} {time_str}").tz_localize(None)
    valid_times = sorted([t for t in all_times if t >= start_time])
    if not valid_times:
        print("No timestamps found after the trade entry time.")
        return

    spread = OptionSpread(trade_type, center, width, price_provider)
    S_range = np.linspace(center - 3*width, center + 3*width, 200)
    
    plt.ion()
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for current_time in valid_times:
        if current_time.tzinfo is not None:
            current_time = current_time.tz_localize(None)
        try:
            current_credit = spread.get_initial_credit(current_time)
        except ValueError as e:
            print(e)
            continue

        payoff = spread.compute_expiration_payoff(S_range, current_time)
        underlying_est = price_provider.estimate_underlying(current_time)
        
        ax.clear()
        ax.plot(S_range, payoff, 'm-', label='Payoff Curve')
        if underlying_est is not None:
            ax.axvline(x=underlying_est, color='red', linestyle='--', label='Estimated Underlying')
        ax.set_xlabel("Underlying Price at Expiration")
        ax.set_ylabel("Trade P&L")
        title_str = f"Expiration Payoff Curve\nTime: {current_time.strftime('%Y-%m-%d %H:%M:%S')} | Credit: {current_credit:.2f}"
        ax.set_title(title_str)
        ax.grid(True)
        ax.legend()
        plt.pause(interval)
    
    plt.ioff()
    plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot or animate Expiration Payoff for an Option Spread Trade")
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
    parser.add_argument("--tolerance", type=int, default=5, help="Tolerance in minutes (default 5).")
    parser.add_argument("--animate", action="store_true", help="Animate the expiration payoff curve over the day.")
    parser.add_argument("--interval", type=float, default=0.5, help="Animation interval in seconds (default 0.5).")
    
    args = parser.parse_args()
    trade_type = args.trade_type.replace("-", "_")
    if args.animate:
        animate_expiration_payoff(trade_type, args.center, args.width,
                                  args.date, args.time, args.underlying,
                                  args.expiry, args.dte, args.tolerance, args.interval)
    else:
        plot_expiration_payoff(trade_type, args.center, args.width,
                               args.date, args.time, args.underlying,
                               args.expiry, args.dte, args.tolerance)

if __name__ == "__main__":
    main()

