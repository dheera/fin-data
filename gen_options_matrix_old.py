#!/usr/bin/env python3
import sys
import os
import argparse
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# --------------------------------------------------------------------------------------
# Parse command-line arguments
# --------------------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate minute-based option matrices from OPRA parquet data with averaged price and volume, using 3-level columns (underlying/ticker/metric)."
    )
    parser.add_argument("input_dir", type=str,
                        help="Directory containing input option .parquet files.")
    parser.add_argument("output_dir", type=str,
                        help="Directory to save the output option .parquet files.")
    parser.add_argument("--stock-matrix-dir", type=str, default=None,
                        help="Directory containing daily stock matrix parquet files. For each day, the corresponding file is used to filter valid underlying tickers.")
    return parser.parse_args()

# --------------------------------------------------------------------------------------
# Helper: Load valid underlying tickers from a stock matrix file for a specific date
# --------------------------------------------------------------------------------------
def load_valid_tickers_for_date(stock_matrix_dir: str, date_str: str) -> set:
    stock_file = Path(stock_matrix_dir) / f"{date_str}.parquet"
    if not stock_file.exists():
        print(f"Stock matrix file {stock_file} not found. No ticker filtering for this day.")
        return None
    try:
        stock_df = pd.read_parquet(stock_file)
    except Exception as e:
        print(f"Error reading stock matrix file {stock_file}: {e}")
        return None

    if isinstance(stock_df.columns, pd.MultiIndex):
        valid_tickers = set(stock_df.columns.get_level_values(0).unique())
    else:
        valid_tickers = set(stock_df.columns.unique())
    return valid_tickers

# --------------------------------------------------------------------------------------
# Main function
# --------------------------------------------------------------------------------------
def main():
    args = parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir
    stock_matrix_dir = args.stock_matrix_dir

    os.makedirs(output_dir, exist_ok=True)

    # Process files in reverse sorted order.
    options_files = sorted(Path(input_dir).glob("*.parquet"), reverse=True)
    if not options_files:
        print("No .parquet files found in the input directory.")
        sys.exit(1)

    # Define the most liquid index options.
    index_options = {"VIX", "VIXW", "SPX", "SPXW", "XSP", "XSPW"}

    for file in tqdm(options_files, desc="Processing option files"):
        date_str = file.stem  # e.g., "2025-01-02"
        output_file = Path(output_dir) / f"{date_str}.parquet"
        if output_file.exists():
            print(f"Exists, skipping: {output_file}")
            continue

        try:
            df = pd.read_parquet(file).reset_index()
        except Exception as e:
            print(f"Error reading file {file}: {e}")
            continue

        # Convert window_start to Eastern Time.
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True).dt.tz_convert("America/New_York")

        # Restrict to NYSE trading hours (09:30 to 16:00 ET).
        market_open = pd.to_datetime("09:30:00").time()
        market_close = pd.to_datetime("16:00:00").time()
        df = df[(df["window_start"].dt.time >= market_open) & (df["window_start"].dt.time <= market_close)]
        if df.empty:
            print(f"No data after time filtering for {file}. Skipping.")
            continue

        # Keep only the needed columns.
        keep_cols = {"underlying", "ticker", "window_start", "open", "high", "low", "close", "volume"}
        df = df[[col for col in df.columns if col in keep_cols]]

        # If a stock matrix directory is provided, filter by valid underlying tickers.
        if stock_matrix_dir:
            valid_tickers = load_valid_tickers_for_date(stock_matrix_dir, date_str)
            if valid_tickers is not None:
                valid_tickers |= set(index_options)
                df = df[df["underlying"].isin(valid_tickers)]
                if df.empty:
                    print(f"No options data with valid underlying for {file}. Skipping.")
                    continue

        # Compute average price as the mean of open, high, low, and close.
        df["price"] = df[["open", "high", "low", "close"]].mean(axis=1)

        # ----------------------------------------------------------------------------------
        # Pivot the data so that rows are indexed by window_start and columns form a 3-level MultiIndex:
        # (underlying, ticker, metric) where metric is either "price" or "volume".
        pivoted = pd.pivot_table(
            df,
            index="window_start",
            columns=["underlying", "ticker"],
            values=["price", "volume"],
            aggfunc="first"
        )
        # The pivot produces a MultiIndex with levels (metric, underlying, ticker).
        # Reorder them to (underlying, ticker, metric).
        pivoted = pivoted.reorder_levels([1, 2, 0], axis=1)
        pivoted = pivoted.sort_index(axis=1)

        # ----------------------------------------------------------------------------------
        # Reindex to include every minute from market open to close.
        full_minutes = pd.date_range(
            start=pd.Timestamp(f"{date_str} 09:30:00", tz="America/New_York"),
            end=pd.Timestamp(f"{date_str} 16:00:00", tz="America/New_York"),
            freq="min"
        )
        pivoted = pivoted.reindex(full_minutes)

        # ----------------------------------------------------------------------------------
        # Drop columns that have non-null data for 5% or less of the minutes,
        # except if the ticker is one of the most liquid index options.
        non_null_proportion = pivoted.notnull().mean()
        ticker_level = pivoted.columns.get_level_values(1)
        valid_col_mask = (non_null_proportion > 0.05)
        pivoted = pivoted.loc[:, valid_col_mask]

        # ----------------------------------------------------------------------------------
        # Fill missing values in a vectorized way:
        # For price: forward fill then backward fill.
        price_cols = pivoted.columns[pivoted.columns.get_level_values(2) == "price"]
        pivoted.loc[:, price_cols] = pivoted.loc[:, price_cols].ffill().bfill()
        # For volume: fill missing values with 0.
        vol_cols = pivoted.columns[pivoted.columns.get_level_values(2) == "volume"]
        pivoted.loc[:, vol_cols] = pivoted.loc[:, vol_cols].fillna(0)

        # ----------------------------------------------------------------------------------
        # Final check.
        if pivoted.isna().any().any():
            print(f"Warning: NaNs remain in the final dataframe for {date_str}.")
        else:
            print(f"Successfully filled missing data for {date_str}.")

        pivoted.to_parquet(output_file, index=True)
        print(f"Processed and saved: {output_file}")

    print("All option files processed.")

if __name__ == "__main__":
    main()

