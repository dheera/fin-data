#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import requests
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
    os.makedirs("us_stocks_sip/splits_by_ticker", exist_ok = True)
    tickers = pd.read_parquet("us_stocks_sip/tickers.parquet")["ticker"].to_list()
    for ticker in tickers:
        get_splits(ticker)
    time.sleep(0.015)
