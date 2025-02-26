#!/usr/bin/env python3

import os
import glob
import pandas as pd

# Step 1: Read all parquet files in the 'dividends' directory
files = glob.glob("us_stocks_sip/dividends/*.parquet")
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

