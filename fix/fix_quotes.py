import glob
import os
import pandas as pd
import time

for file in glob.glob("quotes/*/*.parquet"):
    mtime = os.path.getmtime(file)
    if time.time() - mtime < 86400:
        continue
    print(file)
    df = pd.read_parquet(file)
    df.set_index(['expiry', 'type', 'strike'], inplace=True)
    df.sort_index(inplace=True)
    df.to_parquet(file)
