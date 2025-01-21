#!/usr/bin/env python3

# for each parquet it selects the top 2048 stocks, fills in missing minutes with previous price data, and pivots the parquet
# to a (minutes) x (2048*3) dataframe and saves it to a new parquet in the output dir.
# the new columns are STOCK_open STOCK_close STOCK_volume for each of the 2048 STOCKs, so 2048*3 columns
# also, restricts to trading hours

import os
import pandas as pd
from pathlib import Path
from tqdm import tqdm  # Import tqdm for progress bar

# input and output directories
stocks_dir = "us_stocks_sip/minute_aggs_parquet/"
output_dir = "us_stocks_sip/minute_aggs_matrix/"

# NYSE trading hours in Eastern Time
market_open = "09:30:00"
market_close = "16:00:00"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

stocks_files = sorted(Path(stocks_dir).glob("*.parquet"))
stocks_dates = {f.stem for f in stocks_files}

# Process each date
for date in tqdm(sorted(stocks_dates), desc="Processing Files"):
    stocks_file = Path(stocks_dir) / f"{date}.parquet"

    # Read the data
    stocks_data = pd.read_parquet(stocks_file)
    stocks_data = stocks_data.reset_index()
    
    # Convert window_start to Eastern Time and restrict to NYSE trading hours
    stocks_data["window_start"] = pd.to_datetime(stocks_data["window_start"], utc=True).dt.tz_convert("America/New_York")
    stocks_data = stocks_data[(stocks_data["window_start"].dt.time >= pd.to_datetime(market_open).time()) &
                                   (stocks_data["window_start"].dt.time <= pd.to_datetime(market_close).time())]

    # Drop unwanted columns
    stocks_data = stocks_data.drop(columns=["transactions", "high", "low"])
    
    # Calculate total `price * volume` for each stock
    stocks_data["price"] = stocks_data["close"]  # Use the closing price as the "price"
    stocks_data["price_volume"] = stocks_data["price"] * stocks_data["volume"]
    total_price_volume = stocks_data.groupby("ticker")["price_volume"].sum()

    # Select top 2048 stocks by total `price * volume`
    top_stocks = total_price_volume.nlargest(2048).index
    stocks_data = stocks_data[stocks_data["ticker"].isin(top_stocks)]

    # Create a complete range of trading minutes
    full_minutes = pd.date_range(
        start=stocks_data["window_start"].min(),
        end=stocks_data["window_start"].max(),
        freq="min"  # Replaced 'T' with 'min'
    )
    full_minutes = pd.DataFrame({"window_start": full_minutes})

    # Merge and fill missing minutes
    filled_data = []
    for ticker, group in stocks_data.groupby("ticker"):
        group = full_minutes.merge(group, on="window_start", how="left")
        group = group.ffill().bfill()  # Replace deprecated method
        group["volume"] = group["volume"].fillna(0)  # Use 0 for missing volume
        group["ticker"] = ticker  # Re-add the ticker column
        filled_data.append(group)
    stocks_data = pd.concat(filled_data)

    # Pivot the data
    pivoted_data = stocks_data.pivot(index="window_start", columns="ticker", values=["open", "close", "volume"])

    # Flatten MultiIndex columns
    pivoted_data.columns = [f"{stock}_{col}" for col, stock in pivoted_data.columns]

    # Save the processed data
    output_file = Path(output_dir) / f"{date}.parquet"
    pivoted_data.to_parquet(output_file, index=True)

    print(f"Processed and saved: {output_file}")

print("All files processed.")

