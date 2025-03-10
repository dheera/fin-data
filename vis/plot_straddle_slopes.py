#!/usr/bin/env python3
"""
Script to estimate the underlying price via call-put intersection at opening,
and then plot straddle (call + put) prices vs time for strikes around the estimated underlying.
Linear regressions (from open until a cutoff time, default 10:00) are computed for each straddle,
with the regression slope displayed on the subplot titles.
"""

import os
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

def load_data(date, underlying, expiry=None):
    """Load the parquet file for the given date and filter by underlying (and optionally expiry)."""
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date}.parquet"
    print(parquet_file)
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
    
    df = pd.read_parquet(parquet_file).reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])
    # Filter by underlying
    df = df[df['underlying'] == underlying]
    
    if expiry is not None:
        try:
            # Note: Expiries are stored as yymmdd ints
            expiry = int(expiry)
        except ValueError:
            raise ValueError("Expiry should be an integer in yymmdd format.")
        df = df[df['expiry'] == expiry]
    
    if df.empty:
        raise ValueError("No data available after filtering by underlying (and expiry if provided).")
    return df

def estimate_underlying(df):
    """
    Estimate the underlying price from the opening data by finding the strike where the absolute
    difference between call and put close prices is minimal.
    Duplicate entries (if any) are aggregated via the mean.
    """
    # Get the first time stamp of the day
    first_time = df['window_start'].min()
    opening = df[df['window_start'] == first_time]
    if opening.empty:
        raise ValueError("No opening data found.")
    
    # Group by strike and average if duplicates exist
    call_open = opening[opening['type'] == 'C'].groupby('strike')['close'].mean()
    put_open  = opening[opening['type'] == 'P'].groupby('strike')['close'].mean()
    
    # Find common strikes for both calls and puts
    common_strikes = call_open.index.intersection(put_open.index)
    if common_strikes.empty:
        raise ValueError("No common strikes between calls and puts at opening.")
    
    diffs = (call_open.loc[common_strikes] - put_open.loc[common_strikes]).abs()
    est_strike = diffs.idxmin()  # strike with minimum difference
    return est_strike

def create_full_grid(df):
    """
    For the entire day, create a minute-frequency time grid and pivot call and put data,
    filling forward and backward missing values.
    Returns:
      full_time_index: DatetimeIndex of the full grid.
      strikes: sorted numpy array of strikes available.
      straddle: DataFrame of straddle prices (call + put) with time as index and strikes as columns.
    """
    # Create full time index (minute frequency) based on the data range
    start_time = df['window_start'].min()
    end_time = df['window_start'].max()
    full_time_index = pd.date_range(start=start_time, end=end_time, freq='T')
    
    strikes = np.sort(df['strike'].unique())
    
    # Separate calls and puts; use pivot_table to aggregate duplicates via mean.
    call_df = df[df['type'] == 'C'][['window_start', 'strike', 'close']]
    put_df  = df[df['type'] == 'P'][['window_start', 'strike', 'close']]
    
    call_pivot = pd.pivot_table(call_df, values='close', index='window_start', columns='strike', aggfunc='mean').reindex(full_time_index)
    put_pivot  = pd.pivot_table(put_df, values='close', index='window_start', columns='strike', aggfunc='mean').reindex(full_time_index)
    
    # Fill missing data: forward-fill then backward-fill
    call_pivot = call_pivot.ffill().bfill()
    put_pivot  = put_pivot.ffill().bfill()
    
    # Compute straddle price: call + put
    straddle = call_pivot + put_pivot
    
    return full_time_index, strikes, straddle

def plot_straddles(date, underlying, expiry, N, reg_cutoff):
    """
    Estimate the underlying price at the opening by finding the intersection of call and put.
    Then for N strikes on each side (total 2N+1), plot the straddle prices vs time on synchronized subplots.
    For each subplot, perform a linear regression on the straddle prices from opening until reg_cutoff time,
    and display the regression slope in the subplot title.
    """
    # Load data and filter
    df = load_data(date, underlying, expiry)
    
    # Estimate the underlying price using opening data
    underlying_est = estimate_underlying(df)
    print(f"Estimated underlying price at open: {underlying_est}")
    
    # Create a full time grid and compute straddle prices
    full_time_index, strikes, straddle = create_full_grid(df)
    
    # Convert full_time_index to naive datetimes (drop timezone) for regression comparisons
    full_time_index_naive = full_time_index.tz_localize(None)
    
    # Choose strikes around the estimated underlying
    strike_diffs = np.abs(strikes - underlying_est)
    center_idx = np.argmin(strike_diffs)
    lower_idx = max(0, center_idx - N)
    upper_idx = min(len(strikes), center_idx + N + 1)
    selected_strikes = strikes[lower_idx:upper_idx]
    
    # Create figure with 2N+1 subplots (vertical layout)
    n_plots = len(selected_strikes)
    fig, axes = plt.subplots(n_plots, 1, figsize=(12, 2.5 * n_plots), sharex=True)
    
    if n_plots == 1:
        axes = [axes]
    
    # Determine the regression cutoff time as a naive datetime (using the first day's date)
    open_time_naive = full_time_index_naive[0]
    reg_cutoff_time = datetime.combine(open_time_naive.date(), datetime.strptime(reg_cutoff, "%H:%M").time())
    
    # For each selected strike, plot the straddle price vs time and perform linear regression for times <= reg_cutoff_time.
    for ax, strike in zip(axes, selected_strikes):
        s_series = straddle[strike]
        ax.plot(full_time_index_naive, s_series, label=f"Strike {strike}")
        ax.set_ylabel("Straddle Price")
        
        # Select data for regression: times from open until reg_cutoff_time
        mask = full_time_index_naive <= reg_cutoff_time
        if np.sum(mask) < 2:
            reg_slope = np.nan
        else:
            x_vals = np.array([(t - open_time_naive).total_seconds() / 60 for t in full_time_index_naive[mask]])
            y_vals = s_series[mask].values
            coeffs = np.polyfit(x_vals, y_vals, 1)
            reg_slope = coeffs[0]
            reg_line = np.polyval(coeffs, x_vals)
            ax.plot(full_time_index_naive[mask], reg_line, linestyle='--', color='red')
        
        ax.set_title(f"Strike {strike} - Regression Slope: {reg_slope:.4f} per minute")
        ax.legend(loc="upper left")
        ax.grid(True)
    
    axes[-1].xaxis.set_major_formatter(DateFormatter('%H:%M'))
    plt.xlabel("Time")
    plt.suptitle(f"Straddle Prices vs Time for Underlying {underlying} on {date}", fontsize=16, y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.show()

def main():
    parser = argparse.ArgumentParser(description="Plot straddle prices vs time for a range of strikes, and perform linear regressions on the opening to cutoff period.")
    parser.add_argument("--date", default="2025-01-29", help="Date for the options data (YYYY-MM-DD). Default: 2025-01-28")
    parser.add_argument("--underlying", default="SPXW", help="Underlying option symbol. Default: SPXW")
    parser.add_argument("--expiry", default=250129, help="Expiry as an integer in yymmdd format. Optional.")
    parser.add_argument("--N", default=12, type=int, help="Number of strikes on each side of the estimated underlying strike. Total plotted = 2N+1. Default: 5")
    parser.add_argument("--reg_cutoff", default="10:30", help="Time (HH:MM) until which to perform linear regression. Default: 10:00")
    
    args = parser.parse_args()
    
    try:
        plot_straddles(args.date, args.underlying, args.expiry, args.N, args.reg_cutoff)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()

