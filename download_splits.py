#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import requests
import glob

with open("polygon.json", "r") as f:
    config = json.loads(f.read())

s = requests.Session()

def get_splits(ticker):
    r = s.get("https://api.polygon.io/v3/reference/splits",
        params = {
            "apiKey": config["api_key"],
            "ticker": ticker,
            "limit": 1000,
        }
    )
    splits = r.json()['results']
    print(splits)
    pd.DataFrame(splits).to_parquet(os.path.join("us_stocks_sip", "splits_by_ticker", f"{ticker}.parquet"))

if __name__ == "__main__":
    # download splits 1 ticker at a time, polygon doesn't have an API to get all of them
    os.makedirs("us_stocks_sip/splits_by_ticker", exist_ok = True)
    tickers = pd.read_parquet("us_stocks_sip/tickers.parquet")["ticker"].to_list()
    for ticker in tickers:
        get_splits(ticker)
    time.sleep(0.015)

    # Step 1: Read all parquet files from the splits directory we just downloaded
    files = glob.glob("us_stocks_sip/splits_by_ticker/*.parquet")
    dfs = [pd.read_parquet(file) for file in files]
    df = pd.concat(dfs, ignore_index=True)

    # Step 2: Convert execution_date to a date-only (time-less) format
    df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce").dt.date

    # Step 3: Remove the "id" field
    df.drop(columns=["id"], inplace=True)

    # Step 4: Set the multi-index by ticker and execution_date, then sort
    df.set_index(["ticker", "execution_date"], inplace=True)
    df.sort_index(inplace=True)
    
    # Step 5: Write the processed DataFrame to splits.parquet
    df.to_parquet("us_stocks_sip/splits.parquet")

