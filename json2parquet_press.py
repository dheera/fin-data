#!/usr/bin/env python3
import os
import json
import pandas as pd
import argparse

def aggregate_news_to_parquet(news_dir, output_dir):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Loop through each ticker folder inside the news directory
    for ticker in os.listdir(news_dir):
        ticker_path = os.path.join(news_dir, ticker)
        if os.path.isdir(ticker_path):
            print(f"Processing ticker: {ticker}")
            records = []
            # Process each JSON file in the ticker directory
            for file in os.listdir(ticker_path):
                if file.endswith(".json"):
                    file_path = os.path.join(ticker_path, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            record = json.load(f)
                            records.append(record)
                    except Exception as e:
                        print(f"Error reading {file_path}: {e}")

            # If there are records, convert to DataFrame and write as Parquet
            if records:
                df = pd.DataFrame(records)
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert("America/New_York")
                df = df.sort_values(by='timestamp')
                output_file = os.path.join(output_dir, f"{ticker}.parquet")
                df.to_parquet(output_file, index=False)
                print(f"Saved {output_file}")
            else:
                print(f"No valid records found for ticker: {ticker}")

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate news JSON files into Parquet files, one per ticker."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing subdirectories of news JSON files (one per ticker)."
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the Parquet files will be saved."
    )
    args = parser.parse_args()

    aggregate_news_to_parquet(args.input_dir, args.output_dir)

if __name__ == "__main__":
    main()

