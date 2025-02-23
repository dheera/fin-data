#!/usr/bin/env python3
import os
import time
import json
import requests

with open("polygon.json", "r") as f:
    config = json.loads(f.read())

os.makedirs("news", exist_ok=True)
tickers = ["NVDA", "TSM", "GOOG", "MSFT", "FBTC", "MSTR", "META", "CEG", "TSLA", "ORCL", "AMD", "AAPL", "GM", "ADBE", "SNOW"]

for ticker in tickers:
    print(f"Fetching news for {ticker}")
    r = requests.get(
        "https://api.polygon.io/v2/reference/news",
        params = {
            "ticker": ticker,
            "limit":"1000",
            "apiKey": config["api_key"],
        }
    )

    news_obj = json.loads(r.content.decode('utf-8'))["results"]

    with open(f"news/{ticker}.json", "w") as f:
        f.write(json.dumps(news_obj))
    time.sleep(2)