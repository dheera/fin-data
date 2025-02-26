#!/usr/bin/env python3
import os
import time
import pandas as pd
import json
import glob
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

    files = glob.glob("us_stocks_sip/dividends_by_year/*.parquet")
    dfs = [pd.read_parquet(file) for file in files]
    df = pd.concat(dfs, ignore_index=True)
    
    # Step 2: Convert date fields to date-only (i.e., remove time component)
    date_columns = ["ex_dividend_date", "pay_date", "record_date", "declaration_date"]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Step 3: Remove the "id" column
    df.drop(columns=["id"], inplace=True)

    # Step 4: Set multi-index by ticker and ex_dividend_date, then sort the DataFrame
    df.set_index(["ticker", "ex_dividend_date"], inplace=True)
    df.sort_index(inplace=True)

    # Step 5: Write the combined DataFrame to 'dividends.parquet'
    df.to_parquet("us_stocks_sip/dividends.parquet")

    # Step 6: Create output directory for per-ticker files
    os.makedirs("us_stocks_sip/dividends_by_ticker", exist_ok=True)

    # Step 7: Write each ticker's dividends to a separate parquet file
    for ticker, group in df.groupby(level="ticker"):
        if "/" in ticker:
            continue
        output_file = os.path.join("us_stocks_sip/dividends_by_ticker", f"{ticker}.parquet")
        group.to_parquet(output_file)

