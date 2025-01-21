import os
import pandas as pd
from pathlib import Path

# combines stock and option data into single file

# Directories for options and stock data
stocks_dir = "us_stocks_sip/minute_aggs_parquet/"
output_dir = "us_stocks_sip/minute_aggs_matrix/"

# NYSE trading hours in Eastern Time
market_open = "09:30:00"
market_close = "16:00:00"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# List all files in the options directory (assumes filenames are dates)
stocks_files = sorted(Path(stocks_dir).glob("*.parquet"))

# Create a set of dates present in both directories
stocks_dates = {f.stem for f in stocks_files}

# Process each date
for date in sorted(stocks_dates):
    stocks_file = Path(stocks_dir) / f"{date}.parquet"
    
    # Read the data
    stocks_data = pd.read_parquet(stocks_file)
    stocks_data = stocks_data.reset_index()

    # Convert window_start to Eastern Time and restrict to NYSE trading hours
    stocks_data["window_start"] = pd.to_datetime(stocks_data["window_start"], utc=True).dt.tz_convert("America/New_York")
    stocks_data = stocks_data[(stocks_data["window_start"].dt.time >= pd.to_datetime(market_open).time()) &
                                   (stocks_data["window_start"].dt.time <= pd.to_datetime(market_close).time())]

    # ... more processing logic ...

    output_file = Path(output_dir) / f"{date}.parquet"
    combined_data.to_parquet(output_file, index=True)

    print(f"Processed and saved: {output_file}")

print("All files processed.")

