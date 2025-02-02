#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import parsedatetime  # Library to parse human-readable date/time strings

# Define trading hours in Eastern Time
MARKET_OPEN = 9 * 60 + 30  # 9:30 AM in minutes
MARKET_CLOSE = 16 * 60  # 4:00 PM in minutes

def parse_timestamp(timestamp_str):
    """Parses a timestamp from Unix time, ISO format, or a natural language string."""
    cal = parsedatetime.Calendar()
    try:
        # Try parsing as Unix timestamp (seconds since epoch)
        timestamp = datetime.utcfromtimestamp(float(timestamp_str)).replace(tzinfo=ZoneInfo("UTC"))
        return timestamp.astimezone(ZoneInfo("America/New_York"))

    except ValueError:
        pass

    try:
        # Try parsing as ISO format
        timestamp = datetime.fromisoformat(timestamp_str).replace(tzinfo=ZoneInfo("UTC"))
        return timestamp.astimezone(ZoneInfo("America/New_York"))
    except ValueError:
        pass
    
    try:
        # Try parsing as a natural language string
        time_struct, _ = cal.parse(timestamp_str)
        timestamp = datetime(*time_struct[:6], tzinfo=ZoneInfo("UTC"))
        return timestamp.astimezone(ZoneInfo("America/New_York"))
    except ValueError:
        pass

    return None

def load_stock_data(ticker, timestamp_et):
    """Loads the stock data from a parquet file based on the given timestamp."""
    date_str = timestamp_et.strftime("%Y-%m-%d")
    parquet_file = os.path.join("us_stocks_sip/matrix_512", f"{date_str}.parquet")
    
    if not os.path.exists(parquet_file):
        print(f"Error: Parquet file not found: {parquet_file}")
        return None
    
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    return df

def plot_stock_chart(ticker, timestamp_et, df):
    """Plots the stock chart around the specified timestamp."""
    # Define the time window (15 min before and after)
    window_start = timestamp_et - timedelta(minutes=15)
    window_end = timestamp_et + timedelta(minutes=15)
    df_window = df.loc[(df.index >= window_start) & (df.index <= window_end)]
    
    if df_window.empty:
        print(f"No trading data available for {ticker} around {timestamp_et}.")
        return
    
    try:
        plot_data = pd.DataFrame({
            "Open": df_window[f"{ticker}_open"],
            "High": df_window[f"{ticker}_high"],
            "Low": df_window[f"{ticker}_low"],
            "Close": df_window[f"{ticker}_close"],
            "Volume": df_window[f"{ticker}_volume"],
            "VWAP": df_window.get(f"{ticker}_vwap", None),
            "EMA12": df_window.get(f"{ticker}_ema12", None),
            "EMA26": df_window.get(f"{ticker}_ema26", None)
        })
    except KeyError as e:
        print(f"Missing expected column for ticker {ticker}: {e}")
        return
    
    add_plots = []
    if "VWAP" in plot_data:
        add_plots.append(mpf.make_addplot(plot_data['VWAP'], color='blue', width=1.0))
    if "EMA12" in plot_data:
        add_plots.append(mpf.make_addplot(plot_data['EMA12'], color='red', width=1.0))
    if "EMA26" in plot_data:
        add_plots.append(mpf.make_addplot(plot_data['EMA26'], color='green', width=1.0))
    
    title = f"{ticker} on {timestamp_et.strftime('%Y-%m-%d')} at {timestamp_et.strftime('%H:%M:%S')} ET"
    fig, axes = mpf.plot(plot_data, type='candle', style='charles', addplot=add_plots,
                          volume=True, title=title, returnfig=True)
    
    ax = axes[0]
    # ax.axvline(timestamp_et, color='magenta', linestyle='--', linewidth=2, label="News/Event Time")
    ax.legend()
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot stock chart around a specific time.")
    parser.add_argument("ticker", type=str, help="Stock ticker symbol (e.g., AAPL)")
    parser.add_argument("timestamp", type=str, help="Time in Unix (seconds), ISO format, or natural language (e.g., 'two days ago at 2:00pm ET')")
    args = parser.parse_args()
    
    timestamp_et = parse_timestamp(args.timestamp)
    df = load_stock_data(args.ticker, timestamp_et)
    if df is not None:
        plot_stock_chart(args.ticker, timestamp_et, df)
