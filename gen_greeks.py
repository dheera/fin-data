#!/usr/bin/env python3

# testing, this doesn't work yet

import pandas as pd
import numpy as np
import re
from py_vollib.black_scholes.greeks.analytical import delta, gamma, vega, theta, rho

df = pd.read_parquet("combined_data/2024-01-02.parquet")

import pandas as pd
import numpy as np
import re
from py_vollib.black_scholes.implied_volatility import implied_volatility

# Separate stock and option data
stocks = df[~df.index.get_level_values('ticker').str.startswith('O:')].reset_index()
options = df[df.index.get_level_values('ticker').str.startswith('O:')].reset_index()

# Parse option ticker details
def parse_option_ticker(ticker):
    match = re.match(r'O:(\w+)(\d{6})([CP])(\d+)', ticker)
    if match:
        stock, expiration, option_type, strike = match.groups()
        expiration_date = pd.Timestamp(f"20{expiration[:2]}-{expiration[2:4]}-{expiration[4:6]}", tz='US/Eastern')
        strike_price = int(strike) / 1000
        return stock, expiration_date, option_type, strike_price
    return None

# Parse options and avoid SettingWithCopyWarning
options_details = options['ticker'].map(parse_option_ticker).to_list()
options[['underlying', 'expiration', 'type', 'strike']] = pd.DataFrame(options_details, index=options.index)

# Calculate time to expiration
options['time_to_expiration'] = (
    (options['expiration'] - options['window_start']).dt.total_seconds() / (60 * 60 * 24 * 365)
)

# Map underlying prices to options
# Map underlying prices to options
underlying_prices = (
    stocks.groupby('ticker')['close']
    .last()  # Pick the last value in case of duplicates
)
options['underlying_price'] = options['underlying'].map(underlying_prices)

# Calculate implied volatility for each option
def calculate_implied_volatility(row, risk_free_rate=0.01):
    try:
        option_price = row['close']
        stock_price = row['underlying_price']
        strike_price = row['strike']
        time_to_expiration = row['time_to_expiration']
        option_type = 'c' if row['type'] == 'C' else 'p'
        return implied_volatility(option_price, stock_price, strike_price, time_to_expiration, risk_free_rate, option_type)
    except:
        return np.nan

options['implied_volatility'] = options.apply(calculate_implied_volatility, axis=1)

# Aggregate IV: Volume-weighted average
implied_volatility = (
    options.groupby(['underlying', 'window_start'])
    .apply(lambda group: np.average(group['implied_volatility'], weights=group['volume']) if group['volume'].sum() > 0 else np.nan)
    .rename('implied_volatility')
    .reset_index()
)

# Calculate put/call ratio
put_call_ratio = (
    options.groupby(['underlying', 'window_start', 'type'])['volume']
    .sum()
    .unstack(fill_value=0)
    .rename(columns={'P': 'puts', 'C': 'calls'})
    .assign(put_call_ratio=lambda df: df['puts'] / (df['calls'] + 1e-9))  # Avoid division by zero
    [['put_call_ratio']]
    .reset_index()
)

# Merge implied volatility and put/call ratio
metrics = pd.merge(implied_volatility, put_call_ratio, on=['underlying', 'window_start'], how='outer')

# Drop stocks that don't have options
stocks_with_options = metrics['underlying'].unique()
stocks = stocks[stocks['ticker'].isin(stocks_with_options)]

# Merge metrics with stock data
result = stocks.merge(
    metrics,
    left_on=['ticker', 'window_start'],
    right_on=['underlying', 'window_start'],
    how='left'
)

# Drop unnecessary columns and clean up
result.drop(columns=['underlying'], inplace=True)

# Write to Parquet
result.to_parquet('result.parquet', engine='pyarrow', index=False)

