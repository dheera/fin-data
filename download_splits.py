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

def get_splits():
    url = "https://api.polygon.io/v3/reference/splits"
    results = []
    while url is not None:
        r = requests.get(
            url,
            params = {
                "apiKey": config["api_key"],
                "market": "stocks",
                "limit": 1000,
            }
        )
        response = r.json()
        results += response["results"]
        url = response.get("next_url", None)
        print(response)
        print(len(results))
        time.sleep(0.1)
    return results

if __name__ == "__main__":
    # download splits 1 ticker at a time, polygon doesn't have an API to get all of them
    #os.makedirs("us_stocks_sip/splits_by_ticker", exist_ok = True)
    #tickers = pd.read_parquet("us_stocks_sip/tickers.parquet")["ticker"].to_list()
    #for ticker in tickers:
    #    get_splits(ticker)
    #time.sleep(0.015)

    # Step 1: Read all parquet files from the splits directory we just downloaded
    #files = glob.glob("us_stocks_sip/splits_by_ticker/*.parquet")
    #dfs = [pd.read_parquet(file) for file in files]
    #df = pd.concat(dfs, ignore_index=True)

    splits = get_splits()
    df = pd.DataFrame(splits)

    df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce").dt.date
    df.drop(columns=["id"], inplace=True)
    df.set_index(["ticker", "execution_date"], inplace=True)
    df.sort_index(inplace=True)

    print(df)

    df.to_parquet("us_stocks_sip/splits.parquet")

