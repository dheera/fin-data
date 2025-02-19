#!/usr/bin/env python3

import argparse
import os
import glob
import pandas as pd

# NYSE trading hours (Eastern Time)
TRADING_HOURS = pd.date_range("09:30:00", "16:00:00", freq="min").time  # Every minute from 09:30 to 16:00

def main():
    parser = argparse.ArgumentParser(description="Aggregate minute option data into call/put volumes.")
    parser.add_argument("input_dir", help="Path to the input directory of daily Parquet files.")
    parser.add_argument("output_dir", help="Path to the output directory for aggregated Parquet files.")
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Get all parquet files in the input directory
    file_paths = sorted(glob.glob(os.path.join(args.input_dir, "*.parquet")), reverse=True)

    for file_path in file_paths:
        out_name = os.path.basename(file_path)
        out_path = os.path.join(args.output_dir, out_name)
        
        if os.path.exists(out_path):
            print(f"{out_path} exists, skipping")
            continue

        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(file_path)
        df.reset_index(inplace=True)
        # --------------------------------------------------
        # 1) Identify top-1024 underlyings by total volume
        # --------------------------------------------------
        total_volume_by_und = df.groupby('underlying')['volume'].sum()
        top_und = total_volume_by_und.nlargest(1024).index

        # Filter df to only those underlyings
        df = df[df['underlying'].isin(top_und)]

        # --------------------------------------------------
        # 2) Group by (window_start, underlying, type), summing all contract volumes
        # --------------------------------------------------
        grouped = df.groupby(['window_start', 'underlying', 'type'], as_index=False)['volume'].sum()
        grouped['volume'] = grouped['volume'].fillna(0)

        # --------------------------------------------------
        # 3) Pivot the DataFrame so each underlying has 2 columns (calls & puts)
        # --------------------------------------------------
        pivoted = grouped.pivot_table(
            index='window_start',
            columns=['underlying', 'type'],
            values='volume',
            fill_value=0
        )

        # Ensure all underlyings have both 'C' and 'P' columns
        all_underlying_types = pd.MultiIndex.from_product([top_und, ['C', 'P']], names=['underlying', 'type'])
        
        # Reindex to guarantee presence of all (underlying, type) pairs
        pivoted = pivoted.reindex(columns=all_underlying_types, fill_value=0)
        
        # Rename columns for clarity
        pivoted.rename(columns={'C': 'C_volume', 'P': 'P_volume'}, level=1, inplace=True)
        
        # Convert to int32
        pivoted = pivoted.astype('int32')

        # --------------------------------------------------
        # 4) Ensure All NYSE Trading Minutes Are Present
        # --------------------------------------------------
        # Extract the trading date from the dataset (assuming all timestamps are from the same day)
        trading_date = pivoted.index[0].date()

        # Create full datetime index for that trading day
        full_index = pd.date_range(start=f"{trading_date} 09:30:00", end=f"{trading_date} 16:00:00", freq="min", tz=pivoted.index.tz)

        # Reindex to ensure every minute is present, filling missing values with 0
        pivoted = pivoted.reindex(full_index, fill_value=0)

        # --------------------------------------------------
        # 5) Write the DataFrame to Parquet
        # --------------------------------------------------

        pivoted.to_parquet(out_path)
        print(f"Processed {file_path} -> {out_path}")

if __name__ == "__main__":
    main()

