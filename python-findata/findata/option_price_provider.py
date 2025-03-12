import os
import pandas as pd
import numpy as np
from datetime import timedelta, datetime

class OptionPriceProvider:
    def __init__(self, date, underlying, expiry=None, dte=None):
        """
        date: string YYYY-MM-DD for the Parquet file path
        underlying: underlying symbol (e.g., SPXW)
        expiry: (optional) integer in %y%m%d format. If not provided, dte is used.
        dte: (optional) days-to-expiry to help select an expiry.
        """
        self.date = date
        self.underlying = underlying
        self.expiry = expiry
        self.dte = dte
        self.data = None
        self.load_data()

    def load_data(self):
        parquet_file = f"/fin/us_options_opra/minute_aggs/{self.date}.parquet"
        if not os.path.exists(parquet_file):
            raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

        # Read the parquet file.
        df = pd.read_parquet(parquet_file)
        # If the file is written with a MultiIndex, reset the index to turn the index levels into columns.
        if "window_start" not in df.columns:
            df = df.reset_index()
        # Convert window_start to datetime and drop timezone info.
        df['window_start'] = pd.to_datetime(df['window_start']).dt.tz_localize(None)

        # Filter for the specified underlying.
        df = df[df['underlying'] == self.underlying]
        if df.empty:
            raise ValueError("No data found for the given underlying and date.")

        # Determine expiry if not provided.
        if self.expiry is None:
            first_time = df['window_start'].min()
            target_expiry_date = first_time.date() + timedelta(days=self.dte) if self.dte is not None else first_time.date()
            target_dt = datetime.combine(target_expiry_date, datetime.min.time())
            self.expiry = self.choose_expiry(df, target_dt)
            print(f"Chosen expiry: {self.expiry}")
        else:
            try:
                self.expiry = int(self.expiry)
            except ValueError:
                raise ValueError("Expiry should be an integer in %y%m%d format.")

        df = df[df['expiry'] == self.expiry]
        if df.empty:
            raise ValueError("No data for the chosen expiry.")

        # Create a continuous minute index.
        full_index = pd.date_range(df['window_start'].min(), df['window_start'].max(), freq='T')
        # Set the window_start column as the index, group by type and strike, and forward/backward fill.
        df = df.set_index('window_start')
        df = df.groupby(['type', 'strike'], group_keys=False).apply(lambda g: self.fill_forward(g, full_index))
        df.index.name = 'window_start'
        self.data = df.reset_index()

    @staticmethod
    def fill_forward(group, full_index):
        group = group[~group.index.duplicated(keep='first')]
        return group.reindex(full_index).ffill().bfill()

    @staticmethod
    def parse_expiry_date(expiry_int):
        exp_str = str(expiry_int)
        return datetime.strptime(exp_str, "%y%m%d")

    @classmethod
    def choose_expiry(cls, df, target_date):
        expiries = df['expiry'].unique()
        expiry_dates = [(exp, cls.parse_expiry_date(exp)) for exp in expiries]
        future_expiries = [(exp, ed) for exp, ed in expiry_dates if ed >= target_date]
        if future_expiries:
            chosen = min(future_expiries, key=lambda tup: abs((tup[1] - target_date).days))
        else:
            chosen = min(expiry_dates, key=lambda tup: abs((tup[1] - target_date).days))
        return chosen[0]

    def lookup_leg_price(self, leg, trade_time):
        """
        Return the OHLC4 price for the given leg at trade_time.
        leg: tuple (opt_type, strike, pos)
        trade_time: datetime at which to look up the price.
        """
        if trade_time.tzinfo is not None:
            trade_time = trade_time.tz_localize(None)
        opt_type, strike, pos = leg
        tol = 0.001
        group = self.data[(self.data['type'] == opt_type) & (np.abs(self.data['strike'] - strike) < tol)]
        if group.empty:
            return np.nan
        exact = group[group['window_start'] == trade_time]
        if not exact.empty:
            row = exact.iloc[0]
            return (row['open'] + row['high'] + row['low'] + row['close']) / 4.0
        group = group.copy()
        group['time_diff'] = (pd.to_datetime(group['window_start']) - trade_time).abs()
        idx = group['time_diff'].idxmin()
        row = group.loc[idx]
        return (row['open'] + row['high'] + row['low'] + row['close']) / 4.0

    def estimate_underlying(self, trade_time):
        """
        Estimate underlying price using OHLC4 and interpolation at the given trade_time.
        """
        frame = self.data[self.data['window_start'] == trade_time].copy()
        if frame.empty:
            return None
        frame['ohlc4'] = (frame['open'] + frame['high'] + frame['low'] + frame['close']) / 4.0
        calls = frame[frame['type'] == 'C'][['strike', 'ohlc4']]
        puts  = frame[frame['type'] == 'P'][['strike', 'ohlc4']]
        if calls.empty or puts.empty:
            return None
        merged = pd.merge(calls, puts, on='strike', suffixes=('_call', '_put'))
        if merged.empty:
            return None
        merged = merged.sort_values('strike').reset_index(drop=True)
        merged['diff'] = merged['ohlc4_call'] - merged['ohlc4_put']
        diffs = merged['diff'].values
        strikes = merged['strike'].values
        for i in range(len(diffs) - 1):
            if diffs[i] == 0:
                return strikes[i]
            if diffs[i] * diffs[i+1] < 0:
                x0, x1 = strikes[i], strikes[i+1]
                y0, y1 = diffs[i], diffs[i+1]
                if (y1 - y0) != 0:
                    return x0 - y0 * ((x1 - x0) / (y1 - y0))
        idx_min = merged['diff'].abs().idxmin()
        return merged.loc[idx_min, 'strike']

