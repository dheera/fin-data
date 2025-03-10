import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np
from mpl_toolkits.mplot3d import Axes3D  # registers the 3D projection

def plot_straddle_surface(date, underlying=None, expiry=None, dte=None):
    """
    For a given date (YYYY-MM-DD), load the options data, fill forward and backward
    missing values for each strike and option type, compute the straddle price (call + put),
    and plot a 3D surface where:
        X axis = strike (limited to mean ± std)
        Y axis = time (minutes from start)
        Z axis = straddle price (limited from 0 to mean + std)
    """
    parquet_file = f"/fin/us_options_opra/minute_aggs/{date}.parquet"
    if not os.path.exists(parquet_file):
        print(f"Parquet file not found: {parquet_file}")
        return

    # Load the day's data and parse time
    df = pd.read_parquet(parquet_file).reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])

    # Filter by underlying if specified
    if underlying is not None:
        df = df[df['underlying'] == underlying]
    if df.empty:
        print("No data available after filtering underlying.")
        return

    # Filter by expiry if provided, or use dte logic if desired (not implemented here)
    if expiry is not None:
        try:
            expiry = int(expiry)
        except ValueError:
            print("Expiry should be an integer in YYYYMMDD format.")
            return
        df = df[df['expiry'] == expiry]

    if df.empty:
        print("No data available after expiry filtering.")
        return

    # Create a full time index for the day (minute frequency)
    start_time = df['window_start'].min()
    end_time = df['window_start'].max()
    full_time_index = pd.date_range(start=start_time, end=end_time, freq='T')

    # Extract the unique strikes and sort them
    strikes = np.sort(df['strike'].unique())

    # Separate calls and puts and pivot the data to a time-strike grid for the 'close' price
    call_df = df[df['type'] == 'C'][['window_start', 'strike', 'close']]
    put_df = df[df['type'] == 'P'][['window_start', 'strike', 'close']]

    call_pivot = call_df.pivot(index='window_start', columns='strike', values='close').reindex(full_time_index)
    put_pivot = put_df.pivot(index='window_start', columns='strike', values='close').reindex(full_time_index)

    # Fill missing data: forward-fill then backward-fill
    call_pivot = call_pivot.ffill().bfill()
    put_pivot = put_pivot.ffill().bfill()

    # Compute the straddle price: call + put at each time and strike
    straddle = call_pivot + put_pivot

    # Create a numeric time axis (minutes from start)
    time_numeric = np.array([(t - full_time_index[0]).total_seconds() / 60 for t in full_time_index])
    X, Y = np.meshgrid(strikes, time_numeric)
    Z = straddle.values

    # Compute new axis limits based on the mean and std
    strike_mean = np.mean(strikes)
    strike_std = np.std(strikes)
    x_min = strike_mean - 50 +100 #strike_std
    x_max = strike_mean + 300 +100#strike_std

    price_mean = np.nanmean(Z)
    price_std = np.nanstd(Z)
    z_min = 0
    z_max = price_mean + price_std/2

    # Create the interactive 3D surface plot
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')
    surf = ax.plot_surface(X, Y, Z, cmap='viridis', edgecolor='none', alpha=0.8)
    ax.set_xlabel('Strike')
    ax.set_ylabel('Time (minutes from start)')
    ax.set_zlabel('Straddle Price (Call + Put)')

    # Set the new axis limits
    ax.set_xlim(x_min, x_max)
    ax.set_zlim(z_min, z_max)

    fig.colorbar(surf, shrink=0.5, aspect=5)
    plt.title(f"3D Straddle Price Surface on {date}")
    plt.show()

# Example usage:
if __name__ == "__main__":
    # Replace with your date and filtering parameters as needed.
    plot_straddle_surface('2025-02-27', underlying='SPXW', expiry=250227)

