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
        description="For each date file, compute averaged option data (price & volume) and save one parquet per underlying."
    )
    parser.add_argument("input_dir", type=str,
                        help="Directory containing input option .parquet files.")
    parser.add_argument("output_dir", type=str,
                        help="Directory to save the output option parquet files organized by date.")
    parser.add_argument("--stock-matrix-dir", type=str, default=None,
                        help="Directory containing daily stock matrix parquet files. For each day, the corresponding file is used to filter valid underlying tickers.")
    return parser.parse_args()

# --------------------------------------------------------------------------------------
# Helper: Load valid underlying tickers from a stock matrix file for a specific date
# --------------------------------------------------------------------------------------
def load_valid_tickers_for_date(stock_matrix_dir: str, date_str: str) -> set:
    stock_file = Path(stock_matrix_dir) / f"{date_str}.parquet"
    if not stock_file.exists():
        print(f"Stock matrix file {stock_file} not found. No underlying filtering for this day.")
        return None
    try:
        stock_df = pd.read_parquet(stock_file)
    except Exception as e:
        print(f"Error reading stock matrix file {stock_file}: {e}")
        return None
    # Assume that the stock matrix columns represent valid underlying tickers.
    if isinstance(stock_df.columns, pd.MultiIndex):
        valid_underlyings = set(stock_df.columns.get_level_values(0).unique())
    else:
        valid_underlyings = set(stock_df.columns.unique())
    return valid_underlyings

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
    date_files = sorted(Path(input_dir).glob("*.parquet"), reverse=True)
    if not date_files:
        print("No .parquet files found in the input directory.")
        sys.exit(1)

    for file in tqdm(date_files, desc="Processing date files"):
        date_str = file.stem  # e.g. "2025-01-02"
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
            valid_underlyings = load_valid_tickers_for_date(stock_matrix_dir, date_str)
            if valid_underlyings is not None:
                valid_underlyings |= set({"SPX", "SPXW", "XSP", "XSPW", "VIX", "VIXW"})
                df = df[df["underlying"].isin(valid_underlyings)]
                if df.empty:
                    print(f"No options data with valid underlying for {file}. Skipping.")
                    continue

        # Compute average price.
        df["price"] = df[["open", "high", "low", "close"]].mean(axis=1)

        # Create the full minute index for the trading day.
        full_minutes = pd.date_range(
            start=pd.Timestamp(f"{date_str} 09:30:00", tz="America/New_York"),
            end=pd.Timestamp(f"{date_str} 16:00:00", tz="America/New_York"),
            freq="min"
        )

        # Group by underlying.
        for underlying, group_df in df.groupby("underlying"):
            # Pivot so that index is window_start and columns are ticker with metrics "price" and "volume".
            pivoted = pd.pivot_table(
                group_df,
                index="window_start",
                columns="ticker",
                values=["price", "volume"],
                aggfunc="first"
            )
            # The pivot produces a MultiIndex with levels (metric, ticker).
            # Swap levels so that we get (ticker, metric).
            pivoted.columns = pivoted.columns.swaplevel(0, 1)
            pivoted = pivoted.sort_index(axis=1)

            # Reindex rows to full minute grid.
            pivoted = pivoted.reindex(full_minutes)

            # For each ticker, drop if its "price" data is present for 5% or less of the minutes.
            tickers_to_keep = []
            for ticker in pivoted.columns.get_level_values(0).unique():
                price_series = pivoted.loc[:, (ticker, "price")]
                if price_series.notnull().mean() > 0.05:
                    tickers_to_keep.append(ticker)
            if not tickers_to_keep:
                print(f"All tickers dropped for underlying {underlying} on {date_str}. Skipping.")
                continue
            pivoted = pivoted.loc[:, pivoted.columns.get_level_values(0).isin(tickers_to_keep)]

            # --- Vectorized filling ---
            # Fill all price columns: forward fill then backward fill.
            price_cols = pivoted.columns[pivoted.columns.get_level_values(1) == "price"]
            pivoted.loc[:, price_cols] = pivoted.loc[:, price_cols].ffill().bfill()
            # Fill volume columns with 0.
            vol_cols = pivoted.columns[pivoted.columns.get_level_values(1) == "volume"]
            pivoted.loc[:, vol_cols] = pivoted.loc[:, vol_cols].fillna(0)

            # Create output directory for the date.
            date_dir = Path(output_dir) / date_str
            date_dir.mkdir(parents=True, exist_ok=True)

            # Save the pivoted DataFrame to a parquet file.
            # File name: {date_str}-{underlying}.parquet
            output_filename = f"{date_str}-{underlying}.parquet"
            output_path = date_dir / output_filename
            pivoted.to_parquet(output_path, index=True)
            print(f"Saved: {output_path}")

    print("All files processed.")

if __name__ == "__main__":
    main()

