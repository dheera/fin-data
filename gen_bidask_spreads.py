#!/usr/bin/env python3
import argparse
import os
import glob
import datetime
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

def get_most_recent_date_dir(base_dir):
    """
    Scan the base directory for subdirectories that are valid dates (YYYY-MM-DD)
    and return the most recent one.
    """
    subdirs = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    valid_dates = []
    for d in subdirs:
        try:
            dt = datetime.datetime.strptime(d, "%Y-%m-%d")
            valid_dates.append((dt, d))
        except ValueError:
            continue
    if not valid_dates:
        raise ValueError(f"No valid date directories found in {base_dir}")
    valid_dates.sort(key=lambda x: x[0], reverse=True)
    return valid_dates[0][1]

def reject_outliers(series):
    """
    Remove data points outside of 3 standard deviations from the mean.
    """
    mean = series.mean()
    std = series.std()
    return series[(series >= mean - 3 * std) & (series <= mean + 3 * std)]

def process_file(filepath, date_str):
    """
    Process a single ticker file:
      - Extract ticker from filename.
      - Read the parquet file.
      - Compute the normalized bid-ask spread = (ask - bid) / last.
      - Split into premarket (04:00-09:29:59), day (09:30-15:59:59) and postmarket (16:00-20:00)
      - For each period, remove data points outside of 3 std and recompute mean and std.
      - Return a dictionary with ticker and stats for each period.
    """
    try:
        # Extract ticker from filename, e.g. "2025-02-26-NVDA.parquet" -> "NVDA"
        basename = os.path.basename(filepath)
        ticker = basename.replace(f"{date_str}-", "").replace(".parquet", "")

        df = pd.read_parquet(filepath)

        # Ensure the index is datetime
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)

        # Calculate bid-ask spread and its normalized version.
        df["spread"] = df["ask"] - df["bid"]
        df["spread_frac"] = (df["ask"] - df["bid"]) / df["last"]

        # Define time periods.
        premarket = df.between_time("04:00", "09:29:59")
        day = df.between_time("09:30", "15:59:59")
        postmarket = df.between_time("16:00", "20:00:00")

        # For each period, apply outlier rejection for both 'spread' and 'spread_frac'
        pre_spread = reject_outliers(premarket["spread"]) if not premarket.empty else pd.Series(dtype=float)
        pre_spread_frac = reject_outliers(premarket["spread_frac"]) if not premarket.empty else pd.Series(dtype=float)

        day_spread = reject_outliers(day["spread"]) if not day.empty else pd.Series(dtype=float)
        day_spread_frac = reject_outliers(day["spread_frac"]) if not day.empty else pd.Series(dtype=float)

        post_spread = reject_outliers(postmarket["spread"]) if not postmarket.empty else pd.Series(dtype=float)
        post_spread_frac = reject_outliers(postmarket["spread_frac"]) if not postmarket.empty else pd.Series(dtype=float)

        return {
            "ticker": ticker,
            "pre_mean": pre_spread.mean(),
            "pre_std": pre_spread.std(),
            "pre_mean_frac": pre_spread_frac.mean(),
            "pre_std_frac": pre_spread_frac.std(),
            "day_mean": day_spread.mean(),
            "day_std": day_spread.std(),
            "day_mean_frac": day_spread_frac.mean(),
            "day_std_frac": day_spread_frac.std(),
            "post_mean": post_spread.mean(),
            "post_std": post_spread.std(),
            "post_mean_frac": post_spread_frac.mean(),
            "post_std_frac": post_spread_frac.std(),
        }
    except Exception as e:
        print(f"Error processing file {filepath}: {e}")
        return None

def process_date(date_str):
    # Define input and output directories.
    input_dir = os.path.join("us_stocks_sip", "tq_aggs", date_str)
    output_dir = os.path.join("us_stocks_sip", "bidask_spreads")
    os.makedirs(output_dir, exist_ok=True)

    file_pattern = os.path.join(input_dir, f"{date_str}-*.parquet")
    file_list = glob.glob(file_pattern)

    results = []
    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_file, filepath, date_str) for filepath in file_list]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            result = future.result()
            if result is not None:
                results.append(result)

    # Create the summary DataFrame and set ticker as the index.
    summary_df = pd.DataFrame(results)
    summary_df.set_index("ticker", inplace=True)

    output_path = os.path.join(output_dir, f"{date_str}.parquet")
    summary_df.to_parquet(output_path)
    print(f"Saved summary to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Compute normalized bid-ask spread stats per stock for a given date with outlier rejection")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format. If not provided, uses the most recent date subdirectory.")
    args = parser.parse_args()

    if args.date:
        date_str = args.date
    else:
        base_dir = os.path.join("us_stocks_sip", "tq_aggs")
        date_str = get_most_recent_date_dir(base_dir)

    process_date(date_str)

if __name__ == "__main__":
    main()

