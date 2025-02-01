#!/usr/bin/env python3

import sys
import os
import argparse
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# --------------------------------------------------------------------------------------
# 1. Parse command-line arguments using argparse
# --------------------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate minute-based matrices from stock parquet data."
    )
    parser.add_argument("input_dir", type=str,
        help="Directory containing input .parquet files.")
    parser.add_argument("output_dir", type=str,
        help="Directory to save the output .parquet files.")

    parser.add_argument("--no-indicators", action="store_true",
        help="If set, do NOT compute technical indicators (ema12, ema26, macd, rsi, vwap).")

    parser.add_argument("--top-stocks", type=int, default=1024,
        help="Number of top stocks to keep (by 'close * volume'). 0 means keep all stocks.")

    return parser.parse_args()

# --------------------------------------------------------------------------------------
# Helper function: compute technical indicators
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
    if "volume" in df.columns:
        typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
        cum_vp = (typical_price * df["volume"]).cumsum()
        cum_vol = df["volume"].cumsum()
        df["vwap"] = cum_vp / cum_vol
    else:
        # If there's no volume, we can't compute VWAP
        df["vwap"] = pd.NA

    # Forward/backward fill any boundary NaNs
    df = df.ffill().bfill()

    return df


# --------------------------------------------------------------------------------------
# Main function
# --------------------------------------------------------------------------------------
def main():
    args = parse_args()
    stocks_dir = args.input_dir
    output_dir = args.output_dir
    no_indicators = args.no_indicators
    top_stocks_num = args.top_stocks

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Gather all parquet files
    stocks_files = sorted(Path(stocks_dir).glob("*.parquet"))
    stocks_dates = {f.stem for f in stocks_files}

    for date in tqdm(sorted(stocks_dates), desc="Processing Files"):
        stocks_file = Path(stocks_dir) / f"{date}.parquet"
        output_file = Path(output_dir) / f"{date}.parquet"

        if output_file.exists():
            print(f"Exists, skipping: {output_file}")
            continue

        # ------------------------------------------------------------
        # 1. Load Data
        # ------------------------------------------------------------
        stocks_data = pd.read_parquet(stocks_file).reset_index()

        # Convert window_start to Eastern Time and restrict to NYSE trading hours
        stocks_data["window_start"] = (
            pd.to_datetime(stocks_data["window_start"], utc=True)
              .dt.tz_convert("America/New_York")
        )

        # NY trading hours
        market_open = "09:30:00"
        market_close = "16:00:00"

        stocks_data = stocks_data[
            (stocks_data["window_start"].dt.time >= pd.to_datetime(market_open).time()) &
            (stocks_data["window_start"].dt.time <= pd.to_datetime(market_close).time())
        ]

        # Keep only these core columns if they exist
        keep_cols = {"window_start","ticker","open","high","low","close","volume"}
        existing_cols = set(stocks_data.columns)
        # We'll drop anything not in keep_cols, but only if it exists in the DataFrame
        drop_cols = existing_cols - (keep_cols & existing_cols)
        stocks_data.drop(columns=list(drop_cols), inplace=True)

        # ------------------------------------------------------------
        # 2. (Optional) Select top N stocks if volume is present
        # ------------------------------------------------------------
        if top_stocks_num > 0 and "volume" in stocks_data.columns:
            stocks_data["price_volume"] = stocks_data["close"] * stocks_data["volume"]
            total_price_volume = stocks_data.groupby("ticker")["price_volume"].sum()
            top_stocks = total_price_volume.nlargest(top_stocks_num).index
            stocks_data = stocks_data[stocks_data["ticker"].isin(top_stocks)]
        elif top_stocks_num > 0 and "volume" not in stocks_data.columns:
            # We can't rank by volume if it doesn't exist. Print a warning and keep all.
            print(f"Warning: 'volume' not found. Cannot select top {top_stocks_num} stocks. Keeping all tickers.")
        # If top_stocks_num == 0, do nothing (we keep all tickers).

        # ------------------------------------------------------------
        # 3. Reindex to ensure a row for each trading minute
        # ------------------------------------------------------------
        if len(stocks_data) == 0:
            print(f"No data after filtering for {date}. Skipping.")
            continue

        full_minutes = pd.date_range(
            start=stocks_data["window_start"].min(),
            end=stocks_data["window_start"].max(),
            freq="min"
        )
        full_minutes_df = pd.DataFrame({"window_start": full_minutes})

        # ------------------------------------------------------------
        # 4. Merge, fill missing minutes; optionally compute indicators
        # ------------------------------------------------------------
        filled_data = []
        for ticker, group in stocks_data.groupby("ticker"):
            merged_group = full_minutes_df.merge(group, on="window_start", how="left")

            # Forward/backward fill the core OHLC columns if they exist
            for col in ["open","high","low","close"]:
                if col in merged_group.columns:
                    merged_group[col] = merged_group[col].ffill().bfill()

            # If volume column exists, fill missing with 0
            if "volume" in merged_group.columns:
                merged_group["volume"] = merged_group["volume"].fillna(0)

            merged_group["ticker"] = ticker

            # Compute indicators if not disabled
            if not no_indicators:
                merged_group = compute_indicators(merged_group)
            filled_data.append(merged_group)

        # Combine all tickers back together
        stocks_data = pd.concat(filled_data, ignore_index=True)

        # ------------------------------------------------------------
        # 5. Pivot to create a wide matrix
        # ------------------------------------------------------------
        # Base columns for pivot
        pivot_cols = ["open","high","low","close"]
        if "volume" in stocks_data.columns:
            pivot_cols.append("volume")

        # If we computed indicators, add them; note that VWAP can be NA if volume didn't exist.
        if not no_indicators:
            pivot_cols += ["ema12","ema26","macd","rsi","vwap"]

        # Restrict to the columns that actually exist in stocks_data
        pivot_cols = [c for c in pivot_cols if c in stocks_data.columns]

        pivoted_data = pd.pivot_table(
            stocks_data,
            index="window_start",
            columns="ticker",
            values=pivot_cols,
            aggfunc="first"
        )

        # Flatten the MultiIndex columns: (feature, ticker) -> "TICKER_feature"
        pivoted_data.columns = [f"{ticker}_{feature}" for feature, ticker in pivoted_data.columns]

        # ------------------------------------------------------------
        # 6. Final checks and save
        # ------------------------------------------------------------
        if pivoted_data.isna().any().any():
            raise ValueError(f"NaNs found in final dataframe for date={date}.")

        pivoted_data.to_parquet(output_file, index=True)
        print(f"Processed and saved: {output_file}")

    print("All files processed.")


if __name__ == "__main__":
    main()
