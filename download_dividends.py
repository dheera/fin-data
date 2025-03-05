#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import glob
import requests
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta

with open("polygon.json", "r") as f:
    config = json.loads(f.read())

def get_dividends(start=None):
    url = "https://api.polygon.io/v3/reference/dividends"
    results = []
    last_year = None
    while url is not None:
        r = requests.get(
            url,
            params = {
                "apiKey": config["api_key"],
                "limit": 1000,
                "order": "desc",
                "ex_dividend_date.gt": start,
                "sort": "ex_dividend_date",
            }
        )
        response = r.json()
        results += response["results"]
        url = response.get("next_url", None)
        print(f"Fetched {len(results)} dividends")
        time.sleep(0.1)

    return results

if __name__ == "__main__":
    file_path = "us_stocks_sip/dividends.parquet"
    if os.path.exists(file_path):
        timestamp = os.path.getmtime(file_path)
        last_modified = datetime.fromtimestamp(timestamp)
        months_ago = last_modified - relativedelta(months=2)

        print(f"Fetching dividends from {months_ago.strftime('%Y-%m-%d')}")
        dividends = get_dividends(start = months_ago.strftime("%Y-%m-%d"))

        df_old = pd.read_parquet(file_path)
        df = pd.DataFrame(dividends)

        df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"], errors="coerce").dt.date
        df["pay_date"] = pd.to_datetime(df["pay_date"], errors="coerce").dt.date
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce").dt.date
        df["declaration_date"] = pd.to_datetime(df["declaration_date"], errors="coerce").dt.date

        df.drop(columns=["id"], inplace=True)
        df.set_index(["ticker", "ex_dividend_date"], inplace=True)

        df = pd.concat([df_old, df])
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(inplace=True)

    else:
        print(f"Fetching all dividends")
        dividends = get_dividends()
        df = pd.DataFrame(dividends)
        df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"], errors="coerce").dt.date
        df["pay_date"] = pd.to_datetime(df["pay_date"], errors="coerce").dt.date
        df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce").dt.date
        df["declaration_date"] = pd.to_datetime(df["declaration_date"], errors="coerce").dt.date
        df.drop(columns=["id"], inplace=True)
        df.set_index(["ticker", "ex_dividend_date"], inplace=True)
        df.sort_index(inplace=True)

    print("Writing us_stocks_sip/dividends.parquet")
    print(df)
    df.to_parquet("us_stocks_sip/dividends.parquet")

