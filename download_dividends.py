#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import requests
with open("polygon.json", "r") as f:
    config = json.loads(f.read())


def get_dividends():
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
                "sort": "ex_dividend_date",
            }
        )
        response = r.json()
        url = response.get("next_url", None)
        for result in response["results"]:
            if "ex_dividend_date" not in result:
                continue
            year = result["ex_dividend_date"][:4]

            if last_year is None:
                last_year = year

            print(f'{result["ticker"]} {result.get("pay_date")}')

            if year != last_year:
                pd.DataFrame(results).to_parquet(f"us_stocks_sip/dividends_by_year/{last_year}.parquet")
                results = []
                print(f"wrote {last_year}.parquet")
                time.sleep(2)
            
            results.append(result)
            last_year = year
        time.sleep(0.1)

    pd.DataFrame(results).to_parquet(f"us_stocks_sip/dividends_by_year/{last_year}.parquet")
    print(f"wrote {last_year.parquet}")

if __name__ == "__main__":
    os.makedirs("us_stocks_sip/dividends_by_year", exist_ok = True)
    get_dividends()
