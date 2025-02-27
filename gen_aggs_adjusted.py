#!/usr/bin/env python3
"""
adjust_minute_aggs.py

This script adjusts minute-level aggregate stock price data for splits and dividends
in reverse order so that the most recent minute prices remain unchanged. It processes
each ticker's minute aggregate data by merging in daily splits and dividends data,
computing a daily cumulative reverse adjustment factor, and applying that factor to 
each minute's prices. The original "window_start" timestamps are preserved in the output.

The script expects the following directory structure (relative to a base directory, e.g., "us_stocks_sip"):

    us_stocks_sip/
        minute_aggs_by_ticker/         # Contains minute-level aggregate files (one per ticker)
            A.parquet
            ...
        splits_by_ticker/              # Contains splits data per ticker
            A.parquet
            ...
        dividends_by_ticker/           # Contains dividends data per ticker
            A.parquet
            ...
        adjusted_minute_aggs_by_ticker/ # Output directory for adjusted minute data

Example Inputs:
----------------
1. Minute Aggregate File ("us_stocks_sip/minute_aggs_by_ticker/A.parquet"):
   -------------------------------------------------------------------------------
   | window_start                 | open    | high    | low     | close   | volume | transactions |
   |------------------------------|---------|---------|---------|---------|--------|--------------|
   | 2025-02-14 09:30:00-05:00     | 136.99  | 137.00  | 136.01  | 136.85  | 18340  | 190          |
   | 2025-02-14 09:31:00-05:00     | 136.94  | 137.11  | 136.93  | 137.11  | 1275   | 43           |
   | ...                          | ...     | ...     | ...     | ...     | ...    | ...          |
   | 2025-02-14 15:59:00-05:00     | 136.50  | 136.70  | 136.50  | 136.65  | ...    | ...          |

2. Splits File ("us_stocks_sip/splits_by_ticker/A.parquet"):
   ------------------------------------------------------------
   | execution_date | split_from | split_to |
   |----------------|------------|----------|
   | 2025-02-14     | 1          | 2        |

3. Dividends File ("us_stocks_sip/dividends_by_ticker/A.parquet"):
   ---------------------------------------------------------------
   | ex_dividend_date | cash_amount |
   |------------------|-------------|
   | 2025-02-14       | 0.50        |

Processing:
-----------
- A temporary "date" column is derived from "window_start" by normalizing the timestamp.
- The daily summary is computed by taking the last minute's close price for each day.
- Splits and dividends data are merged based on this date.
- A daily event factor is calculated using:
      event_factor = (1/split_factor) * ((last_close - dividend) / last_close)
- The cumulative reverse adjustment factor is computed (in reverse order so that the latest day is unchanged)
  and then merged back to adjust each minute's price.
- Adjusted price columns ("adj_open", "adj_high", "adj_low", "adj_close") are computed by multiplying 
  the original prices with the cumulative factor.
- The original "window_start" values remain unchanged in the output.

Output:
-------
The adjusted file is written to "us_stocks_sip/adjusted_minute_aggs_by_ticker/A.parquet" with columns:

    window_start, open, high, low, close, volume, transactions, 
    adj_open, adj_high, adj_low, adj_close

Example Output (first few rows):
----------------------------------
   window_start                 open     high     low    close   adj_open  adj_high  adj_low  adj_close  volume  transactions
   2025-02-14 09:30:00-05:00     136.99   137.00  136.01  136.85    <calc>    <calc>    <calc>   <calc>    18340   190
   2025-02-14 09:31:00-05:00     136.94   137.11  136.93  137.11    <calc>    <calc>    <calc>   <calc>    1275    43
   ...

Note:
-----
- The script relies on daily splits and dividends events, merging them based on the normalized date.
- The adjustment is performed in reverse so that the most recent minute's prices are not altered.
- This method ensures that intraday timestamps ("window_start") remain as originally recorded.
"""

import os
from pathlib import Path
import pandas as pd

def adjust_minute_aggs(minute_aggs: pd.DataFrame, splits: pd.DataFrame, dividends: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust minute aggregate data for splits and dividends in reverse,
    so that the most recent minute prices remain unchanged.

    Since splits and dividends are daily events, this function:
      1. Extracts a daily date from the "window_start" timestamps (without modifying them),
      2. Computes a daily adjustment factor using the last minute's close price per day,
      3. Merges the daily factor back onto each minute row, and
      4. Computes adjusted price columns ("adj_open", "adj_high", "adj_low", "adj_close").

    The original "window_start" timestamps are preserved in the output.

    Parameters:
    - minute_aggs: DataFrame containing minute-level aggregate data with at least the following columns:
          * window_start (datetime): The original timestamp of the minute data.
          * open, high, low, close (float): Price columns.
    - splits: DataFrame containing splits data for the ticker with columns:
          * execution_date (datetime): The date of the split.
          * split_from (numeric) and split_to (numeric): To compute the split ratio (split_to / split_from).
    - dividends: DataFrame containing dividend data for the ticker with columns:
          * ex_dividend_date (datetime): The ex-dividend date.
          * cash_amount (float): Dividend per share.

    Returns:
    - DataFrame: The minute-level DataFrame with additional columns:
          * adj_open, adj_high, adj_low, adj_close: Adjusted price columns.
    """
    # Work on a copy to avoid modifying the input DataFrame.
    df = minute_aggs.copy()

    # Ensure that window_start is available as a column.
    if df.index.name == "window_start":
        df = df.reset_index()
    elif "window_start" not in df.columns:
        raise ValueError("DataFrame must have a 'window_start' column or index.")

    # Create a temporary "date" column by normalizing window_start (i.e. stripping the time component).
    df["date"] = pd.to_datetime(df["window_start"]).dt.normalize()

    # Sort by date and window_start to ensure correct daily ordering.
    df = df.sort_values(["date", "window_start"]).reset_index(drop=True)

    # Compute a daily summary: use the last minute's close price per day.
    daily_summary = df.groupby("date").agg(last_close=("close", "last")).reset_index()

    # --- Process splits data ---
    if not splits.empty:
        splits = splits.copy()
        # Convert execution_date to a normalized date.
        splits["date"] = pd.to_datetime(splits["execution_date"]).dt.normalize()
        # Compute the split factor (split_to divided by split_from).
        splits["split_factor"] = splits["split_to"] / splits["split_from"]
        splits = splits[["date", "split_factor"]]
    else:
        splits = pd.DataFrame(columns=["date", "split_factor"])
    # Merge splits into the daily summary and fill missing factors with 1.
    daily_summary = pd.merge(daily_summary, splits, on="date", how="left")
    daily_summary["split_factor"] = daily_summary["split_factor"].fillna(1.0)

    # --- Process dividends data ---
    if not dividends.empty:
        dividends = dividends.copy().reset_index()
        if "ex_dividend_date" in dividends.columns:
            dividends = dividends.rename(columns={"ex_dividend_date": "date"})
        if "cash_amount" in dividends.columns:
            dividends = dividends.rename(columns={"cash_amount": "dividend"})
        dividends["date"] = pd.to_datetime(dividends["date"]).dt.normalize()
        dividends = dividends[["date", "dividend"]]
    else:
        dividends = pd.DataFrame(columns=["date", "dividend"])
    # Merge dividends into the daily summary and fill missing dividend values with 0.
    daily_summary = pd.merge(daily_summary, dividends, on="date", how="left")
    daily_summary["dividend"] = daily_summary["dividend"].fillna(0.0)

    # --- Compute the daily event factor ---
    def compute_event_factor(row):
        if row["last_close"] == 0:
            div_factor = 1.0
        else:
            div_factor = (row["last_close"] - row["dividend"]) / row["last_close"]
        split_adj = 1.0 / row["split_factor"]
        return split_adj * div_factor

    daily_summary["event_factor"] = daily_summary.apply(compute_event_factor, axis=1)

    # --- Compute the cumulative reverse adjustment factor ---
    daily_summary = daily_summary.sort_values("date").reset_index(drop=True)
    daily_summary["cum_factor"] = (
        daily_summary["event_factor"][::-1].cumprod()[::-1]
        .shift(-1)
        .fillna(1)
    )

    # --- Merge the daily cumulative factor back onto the minute-level data ---
    df = pd.merge(df, daily_summary[["date", "cum_factor"]], on="date", how="left")

    # --- Adjust the price columns for each minute ---
    for col in ["open", "high", "low", "close"]:
        df[f"adj_{col}"] = df[col] * df["cum_factor"]

    # Drop the temporary "date" column.
    df = df.drop(columns=["date"])

    return df

def main():
    """
    Main function to process each ticker's minute aggregation data.
    For each ticker file (assumed to be in "us_stocks_sip/minute_aggs_by_ticker"),
    this script reads the minute aggregates, loads the corresponding splits and dividends data,
    applies the reverse adjustment, and writes the adjusted data to "adjusted_minute_aggs_by_ticker".
    """
    base_dir = Path("us_stocks_sip")

    # Define directories.
    minute_aggs_dir = base_dir / "minute_aggs_by_ticker"
    splits_dir = base_dir / "splits_by_ticker"
    dividends_dir = base_dir / "dividends_by_ticker"
    output_dir = base_dir / "adjusted_minute_aggs_by_ticker"
    output_dir.mkdir(exist_ok=True)

    # Process each ticker file.
    for minute_file in minute_aggs_dir.glob("*.parquet"):
        ticker = minute_file.stem
        print(f"Processing ticker: {ticker}")

        try:
            minute_aggs = pd.read_parquet(minute_file)
        except Exception as e:
            print(f"Error reading {minute_file}: {e}")
            continue

        # Load splits for this ticker.
        splits_file = splits_dir / f"{ticker}.parquet"
        if splits_file.exists():
            splits = pd.read_parquet(splits_file)
            if splits.empty:
                splits = pd.DataFrame(columns=["execution_date", "split_from", "split_to"])
        else:
            splits = pd.DataFrame(columns=["execution_date", "split_from", "split_to"])

        # Load dividends for this ticker.
        dividends_file = dividends_dir / f"{ticker}.parquet"
        if dividends_file.exists():
            dividends = pd.read_parquet(dividends_file)
            if dividends.empty:
                dividends = pd.DataFrame(columns=["ex_dividend_date", "cash_amount"])
        else:
            dividends = pd.DataFrame(columns=["ex_dividend_date", "cash_amount"])

        # Adjust the minute aggregates.
        adjusted_df = adjust_minute_aggs(minute_aggs, splits, dividends)

        # Save the adjusted data.
        output_file = output_dir / f"{ticker}.parquet"
        try:
            adjusted_df.to_parquet(output_file, index=False)
            print(f"Saved adjusted data for {ticker} to {output_file}")
        except Exception as e:
            print(f"Error writing {output_file}: {e}")

if __name__ == "__main__":
    main()

