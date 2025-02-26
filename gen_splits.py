#!/usr/bin/env python3

import glob
import os
import pandas as pd

# Step 1: Read all parquet files from the splits directory
files = glob.glob("us_stocks_sip/splits/*.parquet")
dfs = [pd.read_parquet(file) for file in files]
df = pd.concat(dfs, ignore_index=True)

# Step 2: Convert execution_date to a date-only (time-less) format
df["execution_date"] = pd.to_datetime(df["execution_date"], errors="coerce").dt.date

# Step 3: Remove the "id" field
df.drop(columns=["id"], inplace=True)

# Step 4: Set the multi-index by ticker and execution_date, then sort
df.set_index(["ticker", "execution_date"], inplace=True)
df.sort_index(inplace=True)

# Step 5: Write the processed DataFrame to splits.parquet
df.to_parquet("us_stocks_sip/splits.parquet")

