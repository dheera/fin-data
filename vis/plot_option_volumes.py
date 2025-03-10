#!/usr/bin/env python3
"""
Animated Options Volume with Normalized Cumulative Histogram, Underlying Price Estimation,
and Weighted Statistics (Mean, Median, Mode)

This script loads minute-aggregated options data from a Parquet file for a given date.
It filters by underlying and expiry (or days-to-expiry), then for every minute in the file:
  - Top subplot: Plots the current minute’s total volume (calls+puts) at each strike as a bar chart,
    with a vertical dashed line showing the estimated underlying price (determined by where the
    call and put "close" prices cross, with linear interpolation if needed).
  - Bottom subplot: Plots a cumulative histogram of volumes (aggregated from the start of the day
    to the current minute) normalized so that the total sums to 1. It also calculates and overlays:
       - Weighted mean and weighted standard deviation of the strikes.
       - Weighted median of the strikes.
       - Mode (the strike with the highest cumulative volume).

Both subplots use constant x-axis limits computed as:
    x_min = mean(strike) - std(strike)
    x_max = mean(strike) + std(strike)

Animation updates occur at an interval of 0.05 seconds.

Usage example:
    python3 animated_volume_cumhist_normalized.py 2025-02-14 --underlying NVDA --dte 7
"""

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np

def parse_expiry_date(expiry_int):
    return datetime.strptime(str(expiry_int), "%Y%m%d")

def choose_expiry(filtered_df, target_date):
    expiries = filtered_df['expiry'].unique()
    expiry_dates = [(exp, parse_expiry_date(exp)) for exp in expiries]
    future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
    if future_expiries:
        chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
    else:
        chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
    return chosen[0]

def load_data(date_str):
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date_str}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return None
    df = pd.read_parquet(parquet_file)
    df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])
    return df

def precompute_axes(df, underlying=None, expiry=None, dte=None):
    """
    Compute global x-axis limits using:
        x_min = mean(strike) - std(strike)
        x_max = mean(strike) + std(strike)
    Also returns the (not normalized) maximum minute volume across strikes for information.
    """
    underlying_map = {
        "SPX": ["SPX", "SPXW"],
        "XSP": ["XSP", "XSPW"],
        "VIX": ["VIX", "VIXW"]
    }
    if underlying is not None:
        if underlying.upper() in underlying_map:
            df = df[df['underlying'].isin(underlying_map[underlying.upper()])]
        else:
            df = df[df['underlying'] == underlying]
    
    if expiry is None:
        first_time = df['window_start'].min()
        if dte is not None:
            target_date = first_time.date() + timedelta(days=dte)
        else:
            target_date = first_time.date()
        target_dt = datetime.combine(target_date, datetime.min.time())
        expiry = choose_expiry(df, target_dt)
    else:
        expiry = int(expiry)
    df = df[df['expiry'] == expiry]

    if df.empty:
        print("No data found after applying global filters.")
        return None, None, None

    x_mean = df['strike'].mean()
    x_std = df['strike'].std()
    x_min = x_mean - x_std
    x_max = x_mean + x_std

    grouped = df.groupby(['window_start', 'strike'])['volume'].sum().reset_index()
    y_max = grouped['volume'].max() * 1.1  # headroom for current minute plot

    return df, (x_min, x_max), y_max

def estimate_underlying_price(price_pivot):
    """
    Estimate the underlying price based on the crossing of call and put close prices.
    Returns the strike where the crossing occurs (using linear interpolation if needed).
    """
    if 'C' not in price_pivot.columns or 'P' not in price_pivot.columns:
        return None
    price_pivot = price_pivot.sort_index()
    strikes = price_pivot.index.values.astype(float)
    call_prices = price_pivot['C'].values
    put_prices = price_pivot['P'].values
    diff = call_prices - put_prices

    for i in range(len(diff) - 1):
        if diff[i] * diff[i+1] < 0:
            s0, s1 = strikes[i], strikes[i+1]
            d0, d1 = diff[i], diff[i+1]
            underlying_price = s0 + (s1 - s0) * (0 - d0) / (d1 - d0)
            return underlying_price

    idx = np.argmin(np.abs(diff))
    return strikes[idx]

def animate_volume_and_cumhist(date, time_str="12:00:00-05:00", underlying=None, expiry=None, dte=None,
                               tolerance=timedelta(minutes=5), interval=0.05):
    date_str = date
    df = load_data(date_str)
    if df is None:
        return

    df_global, (x_min, x_max), y_max = precompute_axes(df, underlying, expiry, dte)
    if df_global is None:
        return

    timestamps = sorted(df_global['window_start'].unique())
    if not timestamps:
        print("No timestamps found for animation.")
        return

    plt.ion()
    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(10, 12))
    plt.subplots_adjust(hspace=0.3)

    for ts in timestamps:
        # --- Top subplot: Current Minute Volume and Underlying Price ---
        frame = df[df['window_start'] == ts]
        if underlying is not None:
            underlying_map = {
                "SPX": ["SPX", "SPXW"],
                "XSP": ["XSP", "XSPW"],
                "VIX": ["VIX", "VIXW"]
            }
            if underlying.upper() in underlying_map:
                frame = frame[frame['underlying'].isin(underlying_map[underlying.upper()])]
            else:
                frame = frame[frame['underlying'] == underlying]
        if expiry is None:
            if dte is not None:
                target_date = ts.date() + timedelta(days=dte)
            else:
                target_date = ts.date()
            target_dt = datetime.combine(target_date, datetime.min.time())
            chosen_expiry = choose_expiry(frame, target_dt)
        else:
            chosen_expiry = int(expiry)
        frame = frame[frame['expiry'] == chosen_expiry]
        if frame.empty:
            continue

        vol_by_strike = frame.groupby('strike')['volume'].sum()
        strikes = vol_by_strike.index.values
        volumes = vol_by_strike.values

        price_pivot = frame.pivot_table(index='strike', columns='type', values='close')
        est_underlying = estimate_underlying_price(price_pivot)

        ax_top.clear()
        ax_top.set_xlim(x_min, x_max)
        ax_top.set_ylim(0, y_max)
        ax_top.bar(strikes, volumes, width=1.0, color='grey', edgecolor='black', alpha=0.7)
        if est_underlying is not None:
            ax_top.axvline(est_underlying, color='blue', linestyle='--', linewidth=2,
                           label=f"Underlying ~ {est_underlying:.2f}")
        ax_top.set_xlabel("Strike")
        ax_top.set_ylabel("Volume")
        top_title = f"Volume at {ts.strftime('%Y-%m-%d %H:%M:%S')}"
        if underlying:
            top_title += f" - {underlying}"
        ax_top.set_title(top_title)
        ax_top.legend()
        ax_top.grid(True)

        # --- Bottom subplot: Normalized Cumulative Volume ---
        cum_frame = df_global[df_global['window_start'] <= ts]
        cum_vol = cum_frame.groupby('strike')['volume'].sum()
        total_cum = cum_vol.sum()
        if total_cum > 0:
            norm_cum_vol = cum_vol / total_cum
        else:
            norm_cum_vol = cum_vol

        # Compute weighted mean and standard deviation.
        strikes_arr = norm_cum_vol.index.values.astype(float)
        weights = norm_cum_vol.values
        weighted_mean = np.sum(strikes_arr * weights)
        weighted_std = np.sqrt(np.sum(weights * (strikes_arr - weighted_mean)**2))

        # Compute weighted median:
        cum_weights = np.cumsum(weights)
        median_index = np.searchsorted(cum_weights, 0.5)
        weighted_median = strikes_arr[median_index] if median_index < len(strikes_arr) else np.nan

        # Compute mode as the strike with the maximum cumulative volume.
        mode_strike = cum_vol.idxmax()

        ax_bottom.clear()
        ax_bottom.set_xlim(x_min, x_max)
        ax_bottom.set_ylim(0, 1)
        ax_bottom.bar(norm_cum_vol.index.values, norm_cum_vol.values,
                      width=1.0, color='green', edgecolor='black', alpha=0.7)
        # Overlay weighted statistics.
        ax_bottom.axvline(weighted_mean, color='red', linestyle='-', linewidth=2,
                          label=f"Mean: {weighted_mean:.2f}")
        ax_bottom.axvline(weighted_mean - weighted_std, color='red', linestyle='--', linewidth=1,
                          label=f"Std: {weighted_std:.2f}")
        ax_bottom.axvline(weighted_mean + weighted_std, color='red', linestyle='--', linewidth=1)
        ax_bottom.axvline(weighted_median, color='magenta', linestyle='-', linewidth=2,
                          label=f"Median: {weighted_median:.2f}")
        ax_bottom.axvline(mode_strike, color='orange', linestyle='-', linewidth=2,
                          label=f"Mode: {mode_strike:.2f}")
        ax_bottom.set_xlabel("Strike")
        ax_bottom.set_ylabel("Normalized Cumulative Volume")
        ax_bottom.set_title("Normalized Cumulative Volume (Start of Day to Current)")
        ax_bottom.legend()
        ax_bottom.grid(True)

        plt.pause(interval)

    plt.ioff()
    plt.show(block=False)
    print("Animation complete.")

def main():
    parser = argparse.ArgumentParser(
        description="Animate current minute volume with normalized cumulative histogram, weighted stats (mean, median, mode), and estimated underlying price."
    )
    parser.add_argument("date", help="Date for the options data (YYYY-MM-DD)")
    parser.add_argument("--time", default="12:00:00-05:00", help="Start time for the options data (default: 12:00:00-05:00)")
    parser.add_argument("--underlying", help="Underlying symbol to filter (e.g., NVDA, SPX)")
    parser.add_argument("--expiry", help="Expiry as an integer in YYYYMMDD format")
    parser.add_argument("--dte", help="Days-to-expiry. The expiry closest to (date + dte) will be selected.", type=int)
    parser.add_argument("--tolerance", help="Tolerance in minutes for nearest timestamp search (default: 5)", type=int, default=5)
    parser.add_argument("--interval", help="Animation interval in seconds (default: 0.05)", type=float, default=0.05)
    args = parser.parse_args()

    animate_volume_and_cumhist(
        date=args.date,
        time_str=args.time,
        underlying=args.underlying,
        expiry=args.expiry,
        dte=args.dte,
        tolerance=timedelta(minutes=args.tolerance),
        interval=args.interval
    )

if __name__ == "__main__":
    main()

