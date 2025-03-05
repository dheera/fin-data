#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import requests
import glob
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta


with open("polygon.json", "r") as f:
    config = json.loads(f.read())

s = requests.Session()

def get_splits(start=None):
    url = "https://api.polygon.io/v3/reference/splits"
    results = []
    while url is not None:
        r = requests.get(
            url,
            params = {
                "apiKey": config["api_key"],
                "market": "stocks",
                "execution_date.gt": start,
                "limit": 1000,
            }
        )
        response = r.json()
        results += response["results"]
        url = response.get("next_url", None)
        print(f"Fetched {len(results)} splits")
        time.sleep(0.1)
    return results

if __name__ == "__main__":
    file_path = "us_stocks_sip/splits.parquet"
    if os.path.exists(file_path):
        timestamp = os.path.getmtime(file_path)
        last_modified = datetime.fromtimestamp(timestamp)
        six_months_ago = last_modified - relativedelta(months=6)

        print(f"Fetching splits from {six_months_ago.strftime('%Y-%m-%d')}")
        splits = get_splits(start = six_months_ago.strftime("%Y-%m-%d"))

        df_old = pd.read_parquet(file_path)
        df = pd.DataFrame(splits)
        df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce").dt.date
        df.drop(columns=["id"], inplace=True)
        df.set_index(["ticker", "execution_date"], inplace=True)

        df = pd.concat([df_old, df])
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(inplace=True)

    else:
        print(f"Fetching all splits")
        splits = get_splits()
        df = pd.DataFrame(splits)
        df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce").dt.date
        df.drop(columns=["id"], inplace=True)
        df.set_index(["ticker", "execution_date"], inplace=True)

        df.sort_index(inplace=True)

    print("Writing us_stocks_sip/splits.parquet")
    df.to_parquet("us_stocks_sip/splits.parquet")


