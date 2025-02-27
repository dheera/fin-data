#!/usr/bin/env python3
#!/usr/bin/env python3
import argparse
import os
import random
import time
import stealth_requests as requests
import pandas as pd
import re

def convert_weight(data):
    """
    Recursively traverse the data and, if a key 'weight' is found whose value is a string ending in '%',
    convert it to a float.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "weight" and isinstance(value, str) and value.endswith("%"):
                try:
                    data[key] = float(value.strip('%'))
                except ValueError:
                    pass
            else:
                convert_weight(value)
    elif isinstance(data, list):
        for item in data:
            convert_weight(item)
    return data

def fetch_etf_data(ticker):
    page_url = f"https://www.etf.com/{ticker}"
    url = "https://api-prod.etf.com/v2/fund/fund-details"
    payload = {
        "query": "topHoldings",
        "variables": {
            "fund_id": "559",  # using static value as per example; update if necessary
            "ticker": ticker,
            "fund_isin": ""
        }
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://www.etf.com/{ticker}",
        "Origin": "https://www.etf.com",
        "Content-Type": "application/json",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "sec-ch-ua": '"Chromium";v="115", "Not A;Brand";v="99", "Google Chrome";v="115"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }
    
    with requests.StealthSession() as session:
        response = session.get(page_url)
        html = response.content.decode('utf-8')
        pattern = re.compile(r'(\d+)===' + re.escape(ticker))
        match = pattern.search(html)
        payload["variables"]["fund_id"] = match.group(1)

        response = session.post(url, json=payload, headers=headers)
    for obj in response.json()["data"]["topHoldings"]["data"]:
        if obj["name"] == "all_holdings":
            return obj["data"]

    return None

def main():
    parser = argparse.ArgumentParser(description="Download ETF data and save as parquet files.")
    parser.add_argument("--output-dir", default="us_stocks_sip/etf_holdings", help="Output directory for parquet files")
    parser.add_argument("tickers", nargs="+", help="List of ETF tickers")
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    for i, ticker in enumerate(args.tickers):
        output_file = os.path.join(args.output_dir, f"{ticker}.parquet")

        #if os.path.exists(output_file):
        #    print("already exists, continuing")
        #    continue

        print(f"Fetching data for {ticker}...")
        data = fetch_etf_data(ticker)
        for datapoint in data:
            datapoint["weight"] = float(datapoint["weight"].strip("%")) / 100.0
        
        # Convert the data to a DataFrame with a single row and save as parquet
        df = pd.DataFrame(data)
        df.to_parquet(output_file, index=False)
        print(f"Saved data for {ticker} to {output_file}.")

        # Rename 'symbol' to 'ticker'
        df = df.rename(columns={"symbol": "ticker"})
        
        # Convert 'asOf' to a datetime/timestamp
        df["asOf"] = pd.to_datetime(df["asOf"])
        
        # Sort by 'asOf' in descending order so that the most recent date comes first
        df = df.sort_values("asOf", ascending=False)
        
        # Remove duplicates by keeping the first occurrence (the most recent row) for each ticker
        df = df.drop_duplicates(subset="ticker", keep="first")

        print(df)

        if i != len(args.tickers) - 1:
            # Random delay between 5 and 15 seconds
            delay = random.uniform(5, 15)
            print(f"Sleeping for {delay:.2f} seconds...")
            time.sleep(delay)
        
if __name__ == "__main__":
    main()

