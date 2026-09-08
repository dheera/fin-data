#!/usr/bin/env python3
"""
adjust_aggs.py

This script adjusts aggregate stock price data for splits and dividends in reverse so that
the most recent prices remain unchanged. It supports both day‐ and minute‐level data using a unified
approach: a normalized "date" is computed from either a "date" or "window_start" column, the last row
per day is used to compute a daily adjustment factor, and that factor is merged back onto every row for
that day. For day aggregates, grouping by date returns the original row; for minute aggregates it collapses
multiple rows per day.

Splits (with columns "execution_date", "split_from", "split_to") and dividends (with columns
"ex_dividend_date", "cash_amount") are merged based on the normalized date, and adjustments are computed
using the formula:

    event_factor = (1 / split_factor) * ((close - dividend) / close)

A cumulative reverse product of these daily event factors is computed so that the most recent day is
unadjusted (i.e. has a factor of 1). The computed cumulative factor is applied to adjust the price columns
("open", "high", "low", "close").

Default directory structure (relative to base directory "us_stocks_sip"):

    us_stocks_sip/
       day_aggs_by_ticker/           # Input day aggregates (one file per ticker)
       minute_aggs_by_ticker/        # Input minute aggregates (one file per ticker)
       splits_by_ticker/             # Splits data per ticker (columns: execution_date, split_from, split_to)
       dividends_by_ticker/          # Dividends data per ticker (columns: ex_dividend_date, cash_amount)
       adjusted_day_aggs_by_ticker/   # Output for adjusted day aggregates
       adjusted_minute_aggs_by_ticker/# Output for adjusted minute aggregates

Usage Examples:
---------------
1. Day Aggregates:
   Input Example (/fin :) print-parquet us_stocks_sip/day_aggs_by_ticker/TSL.parquet):
       volume       open      close       high        low  transactions
   window_start
   2006-12-19 05:00:00  6552700  26.000000  20.280001  26.500000  20.200001          7485
   2006-12-20 05:00:00  1251900  20.530001  18.900000  20.900000  18.820000          2419
   2006-12-21 05:00:00   349600  19.100000  19.219999  19.500000  19.000000           697
   2006-12-22 05:00:00   352100  19.400000  19.850000  20.340000  19.299999           583
   2006-12-26 05:00:00   148800  19.950001  19.980000  20.450001  19.850000           342
   ...                      ...        ...        ...        ...        ...           ...
   2025-02-10 05:00:00   178330  15.360000  15.050000  15.680100  15.030000          1799
   [3203 rows x 6 columns]

   Output Example (/fin :) print-parquet us_stocks_sip/adjusted_day_aggs_by_ticker/TSL.parquet):
       volume       open      close       high        low  transactions  adj_open  adj_high  adj_low  adj_close
   window_start
   2006-12-19 05:00:00  6552700  26.000000  20.280001  26.500000  20.200001          7485    ...       ...       ...       ...
   2006-12-20 05:00:00  1251900  20.530001  18.900000  20.900000  18.820000          2419    ...       ...       ...       ...
   ...
   2025-02-10 05:00:00   178330  15.360000  15.050000  15.680100  15.030000          1799    ...       ...       ...       ...

2. Minute Aggregates:
   Input Example (/fin :) print-parquet us_stocks_sip/minute_aggs_by_ticker/TSL.parquet):
                           volume     open   close    high      low  transactions
   window_start
   2024-02-27 07:16:00-05:00     100   8.3900   8.390   8.390   8.3900             1
   2024-02-27 08:00:00-05:00     336   8.3903   8.410   8.410   8.3903            12
   2024-02-27 08:03:00-05:00     107   8.2500   8.250   8.250   8.2500             6
   2024-02-27 08:16:00-05:00     373   8.3800   8.380   8.380   8.3800             1
   2024-02-27 08:50:00-05:00     250   8.5000   8.500   8.500   8.5000             1
   ...                           ...      ...     ...     ...      ...           ...
   2025-02-26 19:01:00-05:00     500  11.7500  11.750  11.750  11.7500             4
   2025-02-26 19:08:00-05:00    1065  11.7490  11.749  11.749  11.7490             2
   2025-02-26 19:15:00-05:00    1000  11.7490  11.749  11.749  11.7490             1
   2025-02-26 19:24:00-05:00    1029  11.7090  11.709  11.709  11.7090             3
   2025-02-26 19:41:00-05:00    1000  11.6890  11.689  11.689  11.6890             1
   [60920 rows x 6 columns]

   Output Example (/fin :) print-parquet us_stocks_sip/adjusted_minute_aggs_by_ticker/TSL.parquet):
                           volume     open   close    high      low  transactions  adj_open  adj_high  adj_low  adj_close
   window_start
   2024-02-27 07:16:00-05:00     100   8.3900   8.390   8.390   8.3900             1    ...       ...       ...       ...
   2024-02-27 08:00:00-05:00     336   8.3903   8.410   8.410   8.3903            12    ...       ...       ...       ...
   ...
   2025-02-26 19:41:00-05:00    1000  11.6890  11.689  11.689  11.6890             1    ...       ...       ...       ...

Usage:
------
Run the script with the desired aggregation type. For example:
  python adjust_aggs.py --agg_type day
or
  python adjust_aggs.py --agg_type minute

Additional options allow overriding default input/output directories and specifying the locations of splits
and dividends data.

The output files will contain the original data plus new columns:
  adj_open, adj_high, adj_low, adj_close
which represent the adjusted prices.

Note:
-----
- The "window_start" column is preserved if present.
- A temporary "date" column (normalized to New York time) is used for merging and is dropped in the final output.
"""

import os
from pathlib import Path
import pandas as pd
import argparse
import pandas as pd
import concurrent.futures

def to_ny_normalized(series):
    """
    Convert a datetime series to New York time, normalize to midnight, and remove timezone info.
    """
    series = pd.to_datetime(series)
    if series.dt.tz is None:
        # Calendar dates (splits.execution_date, dividends.ex_dividend_date) are legitimately
        # naive and all midnight - localizing those is fine. Warn only for naive values with an
        # intraday component, which are instants whose timezone was lost: assuming New York for
        # those silently shifts them (see fix/fix_timezones.py).
        if (series != series.dt.floor("D")).any():
            print("WARNING: to_ny_normalized(): naive intraday timestamps, assuming "
                  "America/New_York wall clock; normalize with ./fix/fix_timezones.py if they are UTC")
        series = series.dt.tz_localize("America/New_York")
    else:
        series = series.dt.tz_convert("America/New_York")
    # Normalize to midnight and then drop timezone info.
    return series.dt.normalize().dt.tz_localize(None)

def process_splits(splits: pd.DataFrame) -> pd.DataFrame:
    """Normalize splits data: compute split_factor and convert execution_date to New York normalized date."""
    if not splits.empty:
        splits = splits.copy()
        splits["date"] = to_ny_normalized(splits["execution_date"])
        splits["split_factor"] = splits["split_to"] / splits["split_from"]
        splits = splits[["date", "split_factor"]]
    else:
        splits = pd.DataFrame(columns=["date", "split_factor"])
    return splits

def process_dividends(dividends: pd.DataFrame) -> pd.DataFrame:
    """Normalize dividends data: rename and convert ex_dividend_date to New York normalized date."""
    if not dividends.empty:
        dividends = dividends.copy().reset_index()
        if "ex_dividend_date" in dividends.columns:
            dividends = dividends.rename(columns={"ex_dividend_date": "date"})
        if "cash_amount" in dividends.columns:
            dividends = dividends.rename(columns={"cash_amount": "dividend"})
        dividends["date"] = to_ny_normalized(dividends["date"])
        dividends = dividends[["date", "dividend"]]
    else:
        dividends = pd.DataFrame(columns=["date", "dividend"])
    return dividends

def compute_daily_cum_factor(daily: pd.DataFrame, close_col: str = "close") -> pd.DataFrame:
    """
    Given a daily DataFrame with columns "date", close_col, "split_factor", and "dividend",
    compute an event factor and then the cumulative reverse adjustment factor so that the latest day remains unchanged.
    """
    def compute_event_factor(row):
        if row[close_col] == 0:
            return 1.0
        div_factor = (row[close_col] - row["dividend"]) / row[close_col]
        return (1.0 / row["split_factor"]) * div_factor

    daily["event_factor"] = daily.apply(compute_event_factor, axis=1)
    daily = daily.sort_values("date").reset_index(drop=True)
    # Compute reverse cumulative product so that the latest day gets a factor of 1.
    daily["cum_factor"] = daily["event_factor"][::-1].cumprod()[::-1].shift(-1).fillna(1)
    return daily

def adjust_aggs_common(aggs: pd.DataFrame, splits: pd.DataFrame, dividends: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust aggregate data for splits and dividends using a unified logic:
      - Ensure a normalized "date" column exists. If absent, assume "window_start" exists.
      - Group by "date" to get the last row per day (used for computing the adjustment factor).
      - Merge processed splits and dividends.
      - Compute the daily cumulative reverse factor.
      - Merge the factor back onto all rows by "date" and compute adjusted price columns.
      - If the original data had a "window_start" column, drop the temporary "date" column.
    """
    # Determine the time column: use "date" if present, otherwise use "window_start".
    if "date" not in aggs.columns:
        if aggs.index.name == "window_start":
            aggs = aggs.reset_index()
        if "window_start" in aggs.columns:
            aggs["date"] = to_ny_normalized(aggs["window_start"])
        else:
            raise ValueError("Input data must have a 'date' or 'window_start' column.")
    else:
        aggs["date"] = to_ny_normalized(aggs["date"])

    # Sort and group by date to obtain the last row (using the 'close' as the reference price).
    aggs = aggs.sort_values(["date"]).reset_index(drop=True)
    daily = aggs.groupby("date").agg(last_close=("close", "last")).reset_index()

    # Process splits and dividends.
    splits = process_splits(splits)
    dividends = process_dividends(dividends)
    daily = pd.merge(daily, splits, on="date", how="left")
    daily["split_factor"] = daily["split_factor"].fillna(1.0)
    daily = pd.merge(daily, dividends, on="date", how="left")
    daily["dividend"] = daily["dividend"].fillna(0.0)

    # Compute cumulative factor using the day's last_close.
    daily = compute_daily_cum_factor(daily, close_col="last_close")

    # Merge the cumulative factor back onto the original DataFrame.
    aggs = pd.merge(aggs, daily[["date", "cum_factor"]], on="date", how="left")

    # Compute adjusted price columns.
    for col in ["open", "high", "low", "close"]:
        aggs[f"adj_{col}"] = aggs[col] * aggs["cum_factor"]

    # If the original data had "window_start", drop the temporary "date" column.
    if "window_start" in aggs.columns:
        aggs = aggs.drop(columns=["date"])

    return aggs

# Ensure adjust_aggs_common is imported or defined in this module.
# from your_module import adjust_aggs_common

def load_split_div_groups(splits_file, dividends_file):
    """
    Build per-ticker splits/dividends lookups from the single combined parquet files
    (us_stocks_sip/splits.parquet, dividends.parquet) -- which is what the pipeline
    (download_splits.py / download_dividends.py) actually produces. These files are
    MultiIndexed by (ticker, <date>); reset_index turns the index levels into columns.

    Returns (splits_groups, dividends_groups), each dict[ticker] -> DataFrame with the
    columns adjust_aggs_common needs (splits: execution_date, split_from, split_to;
    dividends: ex_dividend_date, cash_amount).
    """
    splits_groups, dividends_groups = {}, {}

    if Path(splits_file).exists():
        sp = pd.read_parquet(splits_file).reset_index()
        if "ticker" in sp.columns:
            for tk, g in sp.groupby("ticker"):
                splits_groups[str(tk)] = g[["execution_date", "split_from", "split_to"]]
    else:
        print(f"WARNING: splits file not found: {splits_file} -- prices will be split-UNadjusted")

    if Path(dividends_file).exists():
        dv = pd.read_parquet(dividends_file).reset_index()
        if "ticker" in dv.columns:
            for tk, g in dv.groupby("ticker"):
                dividends_groups[str(tk)] = g[["ex_dividend_date", "cash_amount"]]
    else:
        print(f"WARNING: dividends file not found: {dividends_file} -- prices will be div-UNadjusted")

    return splits_groups, dividends_groups


def process_ticker(file, splits, dividends, output_dir):
    """
    Process one ticker file: adjust pre-loaded aggregate data for the given splits and
    dividends frames, then write the adjusted data to the output directory.
    `splits` / `dividends` are per-ticker DataFrames (possibly empty).
    """
    ticker = file.stem
    result = f"Processing ticker: {ticker}\n"
    try:
        aggs = pd.read_parquet(file)
    except Exception as e:
        return f"Error reading {file}: {e}\n"

    if splits is None or splits.empty:
        splits = pd.DataFrame(columns=["execution_date", "split_from", "split_to"])
    if dividends is None or dividends.empty:
        dividends = pd.DataFrame(columns=["ex_dividend_date", "cash_amount"])

    try:
        adjusted = adjust_aggs_common(aggs, splits, dividends)
    except Exception as e:
        return f"Error processing {ticker}: {e}\n"

    output_file = output_dir / f"{ticker}.parquet"
    try:
        adjusted.to_parquet(output_file, index=False)
        result += f"Saved adjusted data for {ticker} to {output_file}\n"
    except Exception as e:
        result += f"Error writing {output_file}: {e}\n"

    return result

def main():
    parser = argparse.ArgumentParser(
        description="Adjust aggregate (day or minute) stock price data for splits and dividends in reverse "
                    "so that the most recent prices remain unchanged."
    )
    parser.add_argument("--agg_type", type=str, choices=["day", "minute"], default="day",
                        help="Type of aggregate data (default: day). Used only for selecting default directories.")
    parser.add_argument("--input_dir", type=str, default=None,
                        help="Input directory for aggregate files (default depends on agg_type).")
    parser.add_argument("--output_dir", type=str, default=None,
                        help="Output directory for adjusted files (default depends on agg_type).")
    parser.add_argument("--splits_file", type=str, default="us_stocks_sip/splits.parquet",
                        help="Combined splits parquet (default: us_stocks_sip/splits.parquet).")
    parser.add_argument("--dividends_file", type=str, default="us_stocks_sip/dividends.parquet",
                        help="Combined dividends parquet (default: us_stocks_sip/dividends.parquet).")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of worker processes to use (default: 4)")
    args = parser.parse_args()

    base_dir = Path("us_stocks_sip")
    if args.agg_type == "day":
        input_dir = Path(args.input_dir) if args.input_dir else base_dir / "day_aggs_by_ticker"
        output_dir = Path(args.output_dir) if args.output_dir else base_dir / "adjusted_day_aggs_by_ticker"
    else:
        input_dir = Path(args.input_dir) if args.input_dir else base_dir / "minute_aggs_by_ticker"
        output_dir = Path(args.output_dir) if args.output_dir else base_dir / "adjusted_minute_aggs_by_ticker"

    output_dir.mkdir(exist_ok=True, parents=True)

    files = list(input_dir.glob("*.parquet"))
    if not files:
        print("No files found in", input_dir)
        return

    # Load splits/dividends once from the single combined parquet files and group by ticker.
    print("Loading splits/dividends ...")
    splits_groups, dividends_groups = load_split_div_groups(args.splits_file, args.dividends_file)
    print(f"Loaded splits for {len(splits_groups)} tickers, dividends for {len(dividends_groups)} tickers.")

    empty = pd.DataFrame()

    # Process each ticker file concurrently, passing its pre-loaded splits/dividends.
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                process_ticker,
                file,
                splits_groups.get(file.stem, empty),
                dividends_groups.get(file.stem, empty),
                output_dir,
            )
            for file in files
        ]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()

