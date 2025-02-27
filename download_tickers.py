#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import requests
with open("polygon.json", "r") as f:
    config = json.loads(f.read())

def get_tickers():
    url = "https://api.polygon.io/v3/reference/tickers"
    results = []
    while url is not None:
        r = requests.get(
            url,
            params = {
                "apiKey": config["api_key"],
                "active": "true",
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
    tickers = get_tickers()
    pd.DataFrame(tickers).to_parquet("us_stocks_sip/tickers.parquet")
