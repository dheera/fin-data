#!/usr/bin/env python3

import os
import argparse
import pandas as pd
from tqdm import tqdm

def merge_stock_and_options(stock_dir, options_dir, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of files in both directories
    stock_files = set(os.listdir(stock_dir))
    options_files = set(os.listdir(options_dir))
    
    # Find common dates (files that exist in both directories)
    common_files = stock_files.intersection(options_files)
    
    for file in tqdm(sorted(common_files, reverse=True), desc="Processing files"):
        stock_path = os.path.join(stock_dir, file)
        options_path = os.path.join(options_dir, file)
        output_path = os.path.join(output_dir, file)
       
        if not os.path.exists(output_path):
            # Read parquet files
            stock_df = pd.read_parquet(stock_path)
            options_df = pd.read_parquet(options_path)
            
            # Extract tickers from multi-index columns in options
            option_tickers = set(options_df.columns.get_level_values(0))
            stock_tickers = set(stock_df.columns.get_level_values(0))
            
            # Find common tickers
            common_tickers = list(stock_tickers.intersection(option_tickers))
            
            if not common_tickers:
                print(f"Skipping {file}: No common tickers found")
                continue
            
            # Drop tickers that are not in common_tickers
            options_filtered = options_df[common_tickers]
            stocks_filtered = stock_df[common_tickers]
            
            # Merge stock and options data
            merged_df = stocks_filtered.join(options_filtered, how='inner')
            merged_df = merged_df.sort_index(axis=1)
            
            # Save to output directory
            merged_df.to_parquet(output_path)
            print(f"Processed {file} successfully")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge stock and options data")
    parser.add_argument("--stocks", required=True, help="Path to stocks directory")
    parser.add_argument("--options", required=True, help="Path to options directory")
    parser.add_argument("--output", required=True, help="Path to output directory")
    
    args = parser.parse_args()
    merge_stock_and_options(args.stocks, args.options, args.output)

