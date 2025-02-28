#!/usr/bin/env python3
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

def process_file(filepath, date_str):
    """
    Process a single ticker file:
      - Extract ticker from filename.
      - Read the parquet file.
      - Compute the normalized bid-ask spread = (ask - bid) / last.
      - Split into premarket (04:00-09:29:59), day (09:30-15:59:59) and postmarket (16:00-20:00)
      - Return a dictionary with ticker, pre_mean, pre_std, day_mean, day_std, post_mean, post_std.
    """
    try:
        # Extract ticker from filename, e.g. "2025-02-26-NVDA.parquet" -> "NVDA"
        basename = os.path.basename(filepath)
        ticker = basename.replace(f"{date_str}-", "").replace(".parquet", "")

        df = pd.read_parquet(filepath)

        # Ensure the index is datetime
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)

        # Calculate normalized bid-ask spread: (ask - bid) / last.
        df["bidask_spread"] = (df["ask"] - df["bid"]) / df["last"]

        # Define time periods.
        premarket = df.between_time("04:00", "09:29:59")
        day = df.between_time("09:30", "15:59:59")
        postmarket = df.between_time("16:00", "20:00:00")

        return {
            "ticker": ticker,
            "pre_mean": premarket["bidask_spread"].mean(),
            "pre_std": premarket["bidask_spread"].std(),
            "day_mean": day["bidask_spread"].mean(),
            "day_std": day["bidask_spread"].std(),
            "post_mean": postmarket["bidask_spread"].mean(),
            "post_std": postmarket["bidask_spread"].std()
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
    parser = argparse.ArgumentParser(description="Compute normalized bid-ask spread stats per stock for a given date")
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

