#!/usr/bin/env python3

import sys
import os
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# --------------------------------------------------------------------------------------
# 1. Accept command-line arguments for input and output directories
# --------------------------------------------------------------------------------------
if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} <input_directory> <output_directory>")
    sys.exit(1)

stocks_dir = sys.argv[1]
output_dir = sys.argv[2]

# --------------------------------------------------------------------------------------
# Parameter for how many top stocks to keep
# --------------------------------------------------------------------------------------
TOP_STOCKS = 1024

# NYSE trading hours in Eastern Time
market_open = "09:30:00"
market_close = "16:00:00"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# --------------------------------------------------------------------------------------
# Helper function to compute technical indicators
# --------------------------------------------------------------------------------------
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame for a single ticker, containing:
        window_start, open, high, low, close, volume, ticker
    compute the following columns: ema12, ema26, macd, rsi, vwap.
    """
    # Sort by time to ensure calculations are in correct chronological order
    df = df.sort_values("window_start")

    # --- EMA12, EMA26, MACD ---
    df["ema12"] = df["close"].ewm(span=12, min_periods=1).mean()
    df["ema26"] = df["close"].ewm(span=26, min_periods=1).mean()
    df["macd"]  = df["ema12"] - df["ema26"]

    # --- RSI (14-period, using exponential moving average) ---
    period = 14
    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period-1, min_periods=1).mean()
    avg_loss = loss.ewm(com=period-1, min_periods=1).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1.0 + rs))

    # --- VWAP ---
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    cum_vp = (typical_price * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum()
    df["vwap"] = cum_vp / cum_vol

    # Forward/backward fill any boundary NaNs
    df = df.ffill().bfill()

    return df

# --------------------------------------------------------------------------------------
# Main processing loop
# --------------------------------------------------------------------------------------
stocks_files = sorted(Path(stocks_dir).glob("*.parquet"))
stocks_dates = {f.stem for f in stocks_files}

for date in tqdm(sorted(stocks_dates), desc="Processing Files"):
    stocks_file = Path(stocks_dir) / f"{date}.parquet"
    output_file = Path(output_dir) / f"{date}.parquet"

    if output_file.exists():
        print(f"Exists, skipping: {output_file}")
        continue

    # Read the data
    stocks_data = pd.read_parquet(stocks_file).reset_index()

    # Convert window_start to Eastern Time and restrict to NYSE trading hours
    stocks_data["window_start"] = (
        pd.to_datetime(stocks_data["window_start"], utc=True)
          .dt.tz_convert("America/New_York")
    )
    stocks_data = stocks_data[
        (stocks_data["window_start"].dt.time >= pd.to_datetime(market_open).time()) &
        (stocks_data["window_start"].dt.time <= pd.to_datetime(market_close).time())
    ]

    # Keep open, close, high, low, volume; drop anything else (e.g. transactions)
    drop_cols = set(stocks_data.columns) - {
        "window_start", "ticker", "open", "high", "low", "close", "volume"
    }
    stocks_data.drop(columns=list(drop_cols), inplace=True)

    # Calculate price_volume for ranking the top stocks
    stocks_data["price_volume"] = stocks_data["close"] * stocks_data["volume"]
    total_price_volume = stocks_data.groupby("ticker")["price_volume"].sum()
    top_stocks = total_price_volume.nlargest(TOP_STOCKS).index
    stocks_data = stocks_data[stocks_data["ticker"].isin(top_stocks)]

    # Create a complete range of trading minutes
    full_minutes = pd.date_range(
        start=stocks_data["window_start"].min(),
        end=stocks_data["window_start"].max(),
        freq="min"
    )
    full_minutes_df = pd.DataFrame({"window_start": full_minutes})

    # Merge, fill missing minutes, compute indicators for each stock
    filled_data = []
    for ticker, group in stocks_data.groupby("ticker"):
        merged_group = full_minutes_df.merge(group, on="window_start", how="left")

        # Forward/backward fill the core OHLC columns
        merged_group[["open","high","low","close"]] = (
            merged_group[["open","high","low","close"]].ffill().bfill()
        )
        merged_group["volume"] = merged_group["volume"].fillna(0)
        merged_group["ticker"] = ticker

        # Compute technical indicators
        merged_group = compute_indicators(merged_group)
        filled_data.append(merged_group)

    # Combine all tickers back together
    stocks_data = pd.concat(filled_data, ignore_index=True)

    # Pivot (actually pivot_table for safety in case of duplicates)
    # We'll keep these columns: open, high, low, close, volume, ema12, ema26, macd, rsi, vwap
    # index = each minute, columns = each ticker, values = these features
    pivoted_data = pd.pivot_table(
        stocks_data,
        index="window_start",
        columns="ticker",
        values=["open","high","low","close","volume","ema12","ema26","macd","rsi","vwap"],
        aggfunc="first"  # if duplicates exist, take the first row
    )

    # Flatten the MultiIndex columns: (feature, ticker) -> "TICKER_feature"
    pivoted_data.columns = [f"{ticker}_{feature}" for feature, ticker in pivoted_data.columns]

    # Check for NaNs
    if pivoted_data.isna().any().any():
        raise ValueError(f"NaNs found in final dataframe for date={date}.")

    # Save the processed data
    pivoted_data.to_parquet(output_file, index=True)
    print(f"Processed and saved: {output_file}")

print("All files processed.")
