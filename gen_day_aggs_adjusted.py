#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd

def adjust_day_aggs(day_aggs: pd.DataFrame, splits: pd.DataFrame, dividends: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust day aggregation data for splits and dividends in reverse,
    so that the most recent day's prices remain unchanged.

    The function normalizes dates in day_aggs to remove the time component,
    ensuring a proper merge with splits and dividends which only have dates.

    Parameters:
    - day_aggs: DataFrame containing daily price data. Expected columns include:
          * An index or column for date (here "window_start" which we rename to "date")
          * Price columns: "open", "high", "low", "close", plus others.
    - splits: DataFrame containing split events. Expected columns after processing:
          * "date" (derived from "execution_date")
          * "split_factor" computed as (split_to / split_from)
    - dividends: DataFrame containing dividend events. Expected columns after processing:
          * "date" (derived from "ex_dividend_date")
          * "dividend" (derived from "cash_amount")

    Returns:
    - DataFrame with additional adjusted price columns: "adj_open", "adj_high", "adj_low", "adj_close".
    """
    # --- Process the day_aggs DataFrame ---
    # If day_aggs doesn't have a "date" column, assume the index is the date column.
    if "date" not in day_aggs.columns:
        # Reset the index and rename "window_start" to "date".
        day_aggs = day_aggs.reset_index().rename(columns={"window_start": "date"})
    # Convert the "date" column to datetime and normalize it (set time to midnight) to remove time component.
    day_aggs["date"] = pd.to_datetime(day_aggs["date"]).dt.normalize()
    # Sort by date in ascending order.
    df = day_aggs.sort_values("date").reset_index(drop=True)

    # --- Process splits data ---
    if not splits.empty:
        # Convert the "execution_date" to datetime and normalize it.
        splits["date"] = pd.to_datetime(splits["execution_date"]).dt.normalize()
        # Compute the split factor as (split_to / split_from).
        splits["split_factor"] = splits["split_to"] / splits["split_from"]
        # Only keep the necessary columns for merging.
        splits = splits[["date", "split_factor"]]
        # Merge the splits data with the day_aggs data on "date".
        df = pd.merge(df, splits, on="date", how="left")
    else:
        # No splits: assign a default factor of 1.
        df["split_factor"] = 1.0
    # Fill missing split_factor values with 1 (i.e., no split effect).
    df["split_factor"] = df["split_factor"].fillna(1.0)

    # --- Process dividends data ---
    if not dividends.empty:
        # Reset the index (dividends are stored with a multi-index).
        dividends = dividends.reset_index()
        # Rename "ex_dividend_date" to "date" if present.
        if "ex_dividend_date" in dividends.columns:
            dividends = dividends.rename(columns={"ex_dividend_date": "date"})
        # Rename "cash_amount" to "dividend" if present.
        if "cash_amount" in dividends.columns:
            dividends = dividends.rename(columns={"cash_amount": "dividend"})
        # Convert the "date" column to datetime and normalize it.
        dividends["date"] = pd.to_datetime(dividends["date"]).dt.normalize()
        # Merge the dividends data with the main data on "date".
        df = pd.merge(df, dividends[["date", "dividend"]], on="date", how="left")
    else:
        # No dividends: assign a default dividend value of 0.
        df["dividend"] = 0.0
    # Fill missing dividend values with 0.
    df["dividend"] = df["dividend"].fillna(0.0)

    # --- Compute the event factor for each day ---
    # For splits: use 1 divided by the split_factor.
    # For dividends: use (close - dividend) / close (if close is nonzero).
    def compute_event_factor(row):
        # Dividend factor: avoid division by zero by defaulting to 1.
        div_factor = (row["close"] - row["dividend"]) / row["close"] if row["close"] != 0 else 1.0
        # Split factor adjustment.
        split_adj = 1.0 / row["split_factor"]
        # Combined event factor.
        return split_adj * div_factor

    df["event_factor"] = df.apply(compute_event_factor, axis=1)

    # --- Compute the cumulative reverse adjustment factor ---
    # Compute the cumulative product of event factors in reverse order so that the latest day is unchanged.
    df["cum_factor"] = (
        df["event_factor"][::-1].cumprod()[::-1]
        .shift(-1)
        .fillna(1)
    )

    # --- Adjust price columns using the cumulative factor ---
    for col in ["open", "high", "low", "close"]:
        df[f"adj_{col}"] = df[col] * df["cum_factor"]

    return df

def main():
    """
    Main function to process each ticker's day aggregation data.
    Reads day_aggs, splits, and dividends files for each ticker,
    applies reverse adjustments (so the latest day remains unchanged),
    and writes the adjusted data to a new parquet file.
    """
    # Define the base directory containing the us_stocks_sip data.
    base_dir = Path("us_stocks_sip")

    # Define the directories for day aggregates, splits, and dividends.
    day_aggs_dir = base_dir / "day_aggs_by_ticker"
    splits_dir = base_dir / "splits_by_ticker"
    dividends_dir = base_dir / "dividends_by_ticker"

    # Define the output directory for the adjusted day aggregates.
    output_dir = base_dir / "adjusted_day_aggs_by_ticker"
    output_dir.mkdir(exist_ok=True)

    # Process each ticker file in the day_aggs_by_ticker directory.
    for day_file in day_aggs_dir.glob("*.parquet"):
        ticker = day_file.stem  # Extract ticker from filename.
        print(f"Processing ticker: {ticker}")

        # Read the day aggregation data.
        try:
            day_aggs = pd.read_parquet(day_file)
        except Exception as e:
            print(f"Error reading {day_file}: {e}")
            continue

        # --- Load splits data for the ticker ---
        splits_file = splits_dir / f"{ticker}.parquet"
        if splits_file.exists():
            splits = pd.read_parquet(splits_file)
            if splits.empty:
                splits = pd.DataFrame(columns=["date", "split_factor"])
        else:
            splits = pd.DataFrame(columns=["date", "split_factor"])

        # --- Load dividends data for the ticker ---
        dividends_file = dividends_dir / f"{ticker}.parquet"
        if dividends_file.exists():
            dividends = pd.read_parquet(dividends_file)
            if dividends.empty:
                dividends = pd.DataFrame(columns=["date", "dividend"])
        else:
            dividends = pd.DataFrame(columns=["date", "dividend"])

        # Compute the adjusted day aggregates.
        adjusted_df = adjust_day_aggs(day_aggs, splits, dividends)

        # Write the adjusted DataFrame to a new parquet file.
        output_file = output_dir / f"{ticker}.parquet"
        try:
            adjusted_df.to_parquet(output_file)
            print(f"Saved adjusted data for {ticker} to {output_file}")
        except Exception as e:
            print(f"Error writing {output_file}: {e}")

if __name__ == "__main__":
    main()

