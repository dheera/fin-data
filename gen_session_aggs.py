#!/usr/bin/env python3

import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta, time
from tqdm import tqdm

# --- Define session boundaries (assumed Eastern Time) ---
PRE_MARKET_END = time(9, 30)    # premarket: before 09:30
INTRA_START = time(9, 30)       # intraday: starting at 09:30
INTRA_END = time(16, 0)         # intraday: up to 16:00
# postmarket: after 16:00

def vectorized_day_metrics(file_path, ticker_list, day_str):
    """
    Process one day of minute aggregated data in a vectorized manner.
    Returns a DataFrame with one row (indexed by the day)
    and columns a MultiIndex (ticker, metric).
    """
    # Load the day's data
    df = pd.read_parquet(file_path)
    
    # Reset index if needed and ensure proper datetime conversion
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    df['window_start'] = pd.to_datetime(df['window_start'])
    df['time'] = df['window_start'].dt.time
    
    # Filter only the tickers we care about
    df = df[df['ticker'].isin(ticker_list)]
    
    # Sort by ticker and time to ensure groupby('first') and ('last') work as expected.
    df = df.sort_values(['ticker', 'window_start'])
   
    print(df)

    # Create a "session" column
    df['session'] = np.select(
        [
            df['time'] < PRE_MARKET_END,
            (df['time'] >= INTRA_START) & (df['time'] <= INTRA_END),
            df['time'] > INTRA_END
        ],
        [
            'pre',
            'intra',
            'post'
        ],
        default='unknown'
    )
    
    # Group by ticker and session and compute vectorized metrics
    grouped = df.groupby(['ticker', 'session'])
    agg_df = grouped.agg(
        first_open=('open', 'first'),
        last_close=('close', 'last'),
        mean_close=('close', 'mean')
    ).reset_index()
    
    # Compute fractional change
    agg_df['f_change'] = (agg_df['last_close'] - agg_df['first_open']) / agg_df['first_open']
    
    # Create pivot tables for fractional change and average price by session
    fc_pivot = agg_df.pivot(index='ticker', columns='session', values='f_change')
    fc_pivot = fc_pivot.rename(columns={'pre': 'pre_change', 'intra': 'intra_change', 'post': 'post_change'})
    
    mean_pivot = agg_df.pivot(index='ticker', columns='session', values='mean_close')
    mean_pivot = mean_pivot.rename(columns={'pre': 'pre_mean', 'intra': 'intra_mean', 'post': 'post_mean'})
    
    # Combine the two sets of metrics
    metrics_df = pd.concat([fc_pivot, mean_pivot], axis=1)
    
    # Reindex to include all tickers even if they have no data (NaN will be filled in)
    metrics_df = metrics_df.reindex(ticker_list)
    
    # Turn the DataFrame into a single row with a MultiIndex for the columns: (ticker, metric)
    # First, stack the metrics so that each ticker/metric pair becomes an entry.
    metrics_series = metrics_df.stack()
    # Then convert to a one-row DataFrame.
    metrics_row = metrics_series.to_frame().T
    metrics_row.index = [day_str]
    
    # Ensure the columns have a MultiIndex (ticker, metric)
    metrics_row.columns = pd.MultiIndex.from_tuples(metrics_row.columns)
    return metrics_row

def main(start_date, end_date):
    # --- Read SPY constituents ---
    constituents_file = "us_stocks_sip/etf_holdings/SPY.parquet"
    constituents_df = pd.read_parquet(constituents_file)
    # Drop rows where symbol is missing
    constituents_df = constituents_df.dropna(subset=['symbol'])
    spy_constituents = constituents_df['symbol'].unique().tolist()

    print(spy_constituents)
    
    # Additional tickers to include
    extra_tickers = ['SPY', 'GLD', 'SLV', 'USO', 'UNG', 'VXX', 'UVXY', 'TLT']
    # Combined ticker list (removing duplicates)
    ticker_list = list(set(spy_constituents + extra_tickers))
    print(f"Processing {len(ticker_list)} tickers.")

    # --- Create date range ---
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    # --- Process each day using vectorized grouping ---
    results = {}
    data_dir = "us_stocks_sip/minute_aggs"  # Folder with minute aggregated parquet files

    for d in tqdm(dates):
        file_path = os.path.join(data_dir, f"{d}.parquet")
        if not os.path.exists(file_path):
            print(f"File for {d} not found; skipping.")
            continue
        
        day_df = vectorized_day_metrics(file_path, ticker_list, d)
        results[d] = day_df

    if not results:
        print("No data processed for the given dates.")
        return

    # Concatenate all days into one DataFrame (row index: date, columns: MultiIndex (ticker, metric))
    result_df = pd.concat(results.values())
    result_df.index.name = 'date'
    
    # Save to parquet
    output_file = "us_stocks_sip/session_aggs.parquet"
    result_df.to_parquet(output_file)
    print(f"Saved computed metrics to {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Compute session metrics for SPY constituents and extra tickers (vectorized over tickers)."
    )
    parser.add_argument('--start-date', required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', required=True, help='End date in YYYY-MM-DD format')
    args = parser.parse_args()
    
    main(args.start_date, args.end_date)

