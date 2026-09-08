import glob
import os
import pandas as pd

for file in sorted(glob.glob("minute_aggs/*.parquet"), reverse=True):
    print(file)
    df = pd.read_parquet(file)
    df.reset_index(inplace=True)
    df.set_index(['underlying', 'expiry', 'type', 'strike', 'window_start'], inplace=True)
    df.sort_index(inplace=True)
    df.to_parquet(file)
