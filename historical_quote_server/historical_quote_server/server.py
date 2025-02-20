#!/usr/bin/env python3

import os
import glob
import datetime
import json
import zmq
import pandas as pd
from cachetools import LRUCache

def dataframe_size(df):
    """Return the DataFrame memory usage in bytes."""
    return df.memory_usage(deep=True).sum()

class LoggingLRUCache(LRUCache):
    def popitem(self):
        key, value = super().popitem()
        print(f"DEBUG: Evicting file from cache: {key}")
        return key, value

class QuoteServer:
    def __init__(self, base_dir="/fin",
                 zmq_bind_address="tcp://*:5555",
                 max_cache_bytes=20 * 1024**3):
        """
        :param stock_base_dir: Directory for stock raw quote parquet files.
        :param options_base_dir: Directory for option raw quote parquet files.
        :param stock_agg_base_dir: Directory for stock minute agg parquet files.
        :param option_agg_base_dir: Directory for option minute agg parquet files.
        :param zmq_bind_address: ZeroMQ bind address.
        :param max_cache_bytes: Maximum cache size in bytes (default 20GB).
        """
        self.stock_base_dir = os.path.join(base_dir, "us_stocks_sip", "quotes")
        self.options_base_dir = os.path.join(base_dir, "us_options_opra", "quotes")
        self.indices_base_dir = os.path.join(base_dir, "us_indices", "values")
        self.stock_agg_base_dir = os.path.join(base_dir, "us_stocks_sip", "minute_aggs")
        self.option_agg_base_dir = os.path.join(base_dir, "us_options_opra", "minute_aggs")
        self.indices_agg_base_dir = os.path.join(base_dir, "us_indices", "minute_aggs")
        self.cache = LoggingLRUCache(maxsize=max_cache_bytes, getsizeof=dataframe_size)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(zmq_bind_address)
        print(f"QuoteServer bound to {zmq_bind_address}")

    # ----- File lookup methods for raw quotes -----
    def get_stock_parquet_path(self, ticker, request_date):
        """Return the stock raw quote parquet file for a given ticker and date."""
        dir_path = os.path.join(self.stock_base_dir, request_date.isoformat())
        if os.path.isdir(dir_path):
            pattern = os.path.join(dir_path, f"{request_date.isoformat()}-{ticker}*.parquet")
            files = glob.glob(pattern)
            if files:
                return files[0]
        return None

    def get_option_parquet_path(self, underlying, request_date):
        """Return the option raw quote parquet file for a given underlying and date."""
        dir_path = os.path.join(self.options_base_dir, request_date.isoformat())
        if os.path.isdir(dir_path):
            pattern = os.path.join(dir_path, f"{request_date.isoformat()}-{underlying}*.parquet")
            files = glob.glob(pattern)
            if files:
                return files[0]
        return None

    # ----- File lookup methods for minute aggs -----
    def get_index_minute_agg_path(self, request_date):
        """
        Return the index minute agg file for a given ticker and date.
        Assumes files are stored under self.stock_agg_base_dir with a filename pattern:
        {date}-{ticker}*.parquet
        """
        file_path = os.path.join(self.indices_agg_base_dir, f"{request_date.isoformat()}.parquet")
        if os.path.exists(file_path):
            return file_path
        return None

    def get_stock_minute_agg_path(self, request_date):
        """
        Return the stock minute agg file for a given ticker and date.
        Assumes files are stored under self.stock_agg_base_dir with a filename pattern:
        {date}-{ticker}*.parquet
        """
        file_path = os.path.join(self.stock_agg_base_dir, f"{request_date.isoformat()}.parquet")
        if os.path.exists(file_path):
            return file_path
        return None

    def get_option_minute_agg_path(self, request_date):
        """
        Return the option minute agg file for a given underlying and date.
        Assumes files are stored under self.option_agg_base_dir with a filename pattern:
        {date}-{underlying}*.parquet
        """
        file_path = os.path.join(self.option_agg_base_dir, f"{request_date.isoformat()}.parquet")
        if os.path.exists(file_path):
            return file_path
        return None

    def load_parquet_file(self, file_path):
        """Load a parquet file into a DataFrame. Assumes the file is already sorted by time."""
        print(f"DEBUG: Loading file: {file_path}")
        df = pd.read_parquet(file_path)
        #if "sip_timestamp" not in df.columns:
        #    raise ValueError(f"'sip_timestamp' column not found in {file_path}")
        # For raw quotes, we assume the file is already sorted.
        return df

    def get_quote_from_df(self, df, query_timestamp):
        """Return the as‑of row from a sorted DataFrame given the query timestamp."""
        query_ts = pd.to_datetime(query_timestamp)
        idx = df["sip_timestamp"].searchsorted(query_ts, side="right") - 1
        if idx >= 0:
            return df.iloc[idx].to_dict()
        return None

    # ----- Stock raw quote logic -----
    def get_stock_quote_logic(self, ticker, timestamp_str):
        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}

        request_date = query_ts.date()
        file_path = self.get_stock_parquet_path(ticker, request_date)
        if not file_path:
            return {"error": f"No stock data file found for ticker {ticker} on {request_date} (outside trading hours)."}

        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        start_ts = df.iloc[0]["sip_timestamp"]
        end_ts = df.iloc[-1]["sip_timestamp"]
        if query_ts < start_ts or query_ts > end_ts:
            return {"error": f"Timestamp {timestamp_str} is outside trading hours for {ticker} on {request_date}. Trading hours: {start_ts.isoformat()} to {end_ts.isoformat()}."}

        quote = self.get_quote_from_df(df, query_ts)
        if quote is None:
            return {"error": f"No quote found before {timestamp_str} in file {file_path}."}

        for key, value in quote.items():
            if pd.isna(value):
                quote[key] = None
            elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                quote[key] = value.isoformat()
            elif hasattr(value, "item"):
                quote[key] = value.item()
        quote["source_file"] = file_path
        return quote

    # ----- Option raw quote logic -----
    def get_option_quote_logic(self, underlying, expiry, option_type, strike, timestamp_str):
        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}

        request_date = query_ts.date()
        file_path = self.get_option_parquet_path(underlying, request_date)
        if not file_path:
            return {"error": f"No option data file found for underlying {underlying} on {request_date} (outside trading hours)."}

        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        try:
            # Use the multi-index (expiry, type, strike) to filter rows.
            df_filtered = df.loc[int(expiry), str(option_type), float(strike)]
            if isinstance(df_filtered, pd.Series):
                df_filtered = df_filtered.to_frame().T
        except KeyError:
            return {"error": f"No option data found for {underlying} {expiry} {option_type} {strike} on {request_date}."}

        if df_filtered.empty:
            return {"error": f"No option data found for {underlying} {expiry} {option_type} {strike} on {request_date}."}

        start_ts = df_filtered.iloc[0]["sip_timestamp"]
        end_ts = df_filtered.iloc[-1]["sip_timestamp"]
        if query_ts < start_ts or query_ts > end_ts:
            return {"error": f"Timestamp {timestamp_str} is outside trading hours for option {underlying} {expiry} {option_type} {strike} on {request_date}. Trading hours: {start_ts.isoformat()} to {end_ts.isoformat()}."}

        quote = self.get_quote_from_df(df_filtered, query_ts)
        if quote is None:
            return {"error": f"No option quote found before {timestamp_str} in file {file_path}."}

        for key, value in quote.items():
            if pd.isna(value):
                quote[key] = None
            elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                quote[key] = value.isoformat()
            elif hasattr(value, "item"):
                quote[key] = value.item()
        quote["source_file"] = file_path
        return quote
    
    # ----- Stock minute agg logic -----
    def get_index_minute_agg_logic(self, ticker, timestamp_str):
        """
        Process a stock minute agg request:
          - Loads the appropriate minute agg file (or retrieves it from cache).
          - Filters to the given ticker (from the multi-index: ticker, window_start).
          - Returns the as-of agg row (the last row with window_start <= query timestamp).
        """
        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}
        request_date = query_ts.date()
        file_path = self.get_index_minute_agg_path(request_date)
        if not file_path:
            return {"error": f"No index minute agg data file found for ticker {ticker} on {request_date}."}
        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        global_times = df.index.get_level_values(1)
        global_min = global_times.min()
        global_max = global_times.max()
        if query_ts < global_min or query_ts > global_max:
            return {"error": f"Timestamp {timestamp_str} is outside trading minutes. Aggregates available from {global_min.isoformat()} to {global_max.isoformat()}."}

        try:
            # Extract rows for the given ticker.
            df_ticker = df.loc[ticker]
            if not isinstance(df_ticker.index, pd.DatetimeIndex):
                df_ticker.index = pd.to_datetime(df_ticker.index, errors="coerce")
        except KeyError:
            return {"error": f"No minute agg data found for ticker {ticker} on {request_date}."}
        pos = df_ticker.index.searchsorted(query_ts, side="right") - 1
        if pos < 0:
            pos = 0
        elif pos >= len(df_ticker):
            pos = len(df_ticker) - 1
        row = df_ticker.iloc[pos].to_dict()
        row["window_start"] = df_ticker.index[pos].isoformat()
        row["source_file"] = file_path
        return row

    # ----- Stock minute agg logic -----
    def get_stock_minute_agg_logic(self, ticker, timestamp_str):
        """
        Process a stock minute agg request:
          - Loads the appropriate minute agg file (or retrieves it from cache).
          - Filters to the given ticker (from the multi-index: ticker, window_start).
          - Returns the as-of agg row (the last row with window_start <= query timestamp).
        """
        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}
        request_date = query_ts.date()
        file_path = self.get_stock_minute_agg_path(request_date)
        if not file_path:
            return {"error": f"No stock minute agg data file found for ticker {ticker} on {request_date}."}
        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        global_times = df.index.get_level_values(1)
        global_min = global_times.min()
        global_max = global_times.max()
        if query_ts < global_min or query_ts > global_max:
            return {"error": f"Timestamp {timestamp_str} is outside trading minutes. Aggregates available from {global_min.isoformat()} to {global_max.isoformat()}."}

        try:
            # Extract rows for the given ticker.
            df_ticker = df.loc[ticker]
            if not isinstance(df_ticker.index, pd.DatetimeIndex):
                df_ticker.index = pd.to_datetime(df_ticker.index, errors="coerce")
        except KeyError:
            return {"error": f"No minute agg data found for ticker {ticker} on {request_date}."}
        pos = df_ticker.index.searchsorted(query_ts, side="right") - 1
        if pos < 0:
            pos = 0
        elif pos >= len(df_ticker):
            pos = len(df_ticker) - 1
        row = df_ticker.iloc[pos].to_dict()
        row["window_start"] = df_ticker.index[pos].isoformat()
        row["source_file"] = file_path
        return row

    # ----- Option minute agg logic -----
    def get_option_minute_agg_logic(self, underlying, expiry, option_type, strike, timestamp_str):
        """
        Process an option minute agg request:
          - Loads the appropriate minute agg file (or retrieves it from cache).
          - Uses the multi-index (underlying, expiry, option_type, strike, window_start) to filter rows.
          - Returns the as-of agg row.
        """
        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}
        request_date = query_ts.date()
        file_path = self.get_option_minute_agg_path(request_date)
        if not file_path:
            return {"error": f"No option minute agg data file found for underlying {underlying} on {request_date}."}
        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        global_times = df.index.get_level_values(4)
        global_min = global_times.min()
        global_max = global_times.max()

        if query_ts < global_min or query_ts > global_max:
            return {"error": f"Timestamp {timestamp_str} is outside trading minutes. Aggregates available from {global_min.isoformat()} to {global_max.isoformat()}."}

        try:
            # Filter the multi-index for the specific option contract.
            df_option = df.loc[(str(underlying), int(expiry), str(option_type), float(strike))]
            if isinstance(df_option, pd.Series):
                df_option = df_option.to_frame().T
            if not isinstance(df_option.index, pd.DatetimeIndex):
                df_option.index = pd.to_datetime(df_option.index, errors="coerce")
        except KeyError:
            return {"error": f"No option minute agg data found for {underlying} {expiry} {option_type} {strike} on {request_date}."}
        if df_option.empty:
            return {"error": f"No option minute agg data found for {underlying} {expiry} {option_type} {strike} on {request_date}."}
        #if query_ts < df_option.index[0] or query_ts > df_option.index[-1]:
        #    return {"error": f"Timestamp {timestamp_str} is outside trading minutes for option {underlying} {expiry} {option_type} {strike} on {request_date}. Aggregates available from {df_option.index[0].isoformat()} to {df_option.index[-1].isoformat()}."}
        pos = df_option.index.searchsorted(query_ts, side="right") - 1
        if pos < 0:
            pos = 0 # return {"error": f"No option minute agg available before {timestamp_str}."}
        elif pos >= len(df_option):
            pos = len(df_option) - 1
        row = df_option.iloc[pos].to_dict()
        row["window_start"] = df_option.index[pos].isoformat()
        row["source_file"] = file_path
        return row

    # ----- Option ticker parser (for raw and minute agg endpoints) -----
    def parse_option_ticker(self, ticker_str):
        """
        Parse a standard options ticker string, e.g., "O:NVDA261218C00162000".
        Expected format: O:{underlying}{expiry}{option_type}{strike}
          - underlying: letters until a digit is encountered
          - expiry: 6 digits (YYMMDD)
          - option_type: single letter (C or P)
          - strike: 8 digits, with an implied decimal point before the last 3 digits.
        Returns a dict with underlying, expiry, option_type, strike.
        """
        if not ticker_str.startswith("O:"):
            return {"error": "Invalid option ticker format."}
        try:
            content = ticker_str[2:]
            i = 0
            while i < len(content) and content[i].isalpha():
                i += 1
            underlying = content[:i]
            expiry = content[i:i+6]
            i += 6
            option_type = content[i]
            i += 1
            strike_str = content[i:i+8]
            strike = int(strike_str) / 1000.0
            return {"underlying": underlying, "expiry": expiry, "option_type": option_type, "strike": strike}
        except Exception as e:
            return {"error": f"Failed to parse option ticker: {e}"}

    # ----- Request dispatch -----
    def process_request(self, request_data):
        """
        Dispatch incoming requests by endpoint.
          - "quote": stock raw quote.
          - "option_quote": option raw quote.
          - "minute_agg": stock minute agg.
          - "index_minute_agg": stock index agg.
          - "option_minute_agg": option minute agg.
        """
        endpoint = request_data.get("endpoint", "quote")
        if endpoint == "stock_quote":
            ticker = request_data.get("ticker")
            timestamp_str = request_data.get("timestamp")
            if not ticker or not timestamp_str:
                return {"error": "Missing 'ticker' or 'timestamp' in stock quote request."}
            return self.get_stock_quote_logic(ticker, timestamp_str)
        elif endpoint == "option_quote":
            if "ticker" in request_data and str(request_data["ticker"]).startswith("O:"):
                parsed = self.parse_option_ticker(request_data["ticker"])
                if "error" in parsed:
                    return parsed
                underlying = parsed["underlying"]
                expiry = parsed["expiry"]
                option_type = parsed["option_type"]
                strike = parsed["strike"]
            else:
                underlying = request_data.get("underlying")
                expiry = request_data.get("expiry")
                option_type = request_data.get("option_type")
                strike = request_data.get("strike")
            timestamp_str = request_data.get("timestamp")
            if not (underlying and expiry and option_type and strike and timestamp_str):
                return {"error": "Missing parameters in option quote request. Required: underlying, expiry, option_type, strike, timestamp."}
            return self.get_option_quote_logic(underlying, expiry, option_type, strike, timestamp_str)
        elif endpoint == "stock_minute_agg":
            ticker = request_data.get("ticker")
            timestamp_str = request_data.get("timestamp")
            if not ticker or not timestamp_str:
                return {"error": "Missing 'ticker' or 'timestamp' in minute agg request."}
            return self.get_stock_minute_agg_logic(ticker, timestamp_str)
        elif endpoint == "index_minute_agg":
            ticker = request_data.get("ticker")
            timestamp_str = request_data.get("timestamp")
            if not ticker or not timestamp_str:
                return {"error": "Missing 'ticker' or 'timestamp' in minute agg request."}
            return self.get_index_minute_agg_logic(ticker, timestamp_str)
        elif endpoint == "option_minute_agg":
            if "ticker" in request_data and str(request_data["ticker"]).startswith("O:"):
                parsed = self.parse_option_ticker(request_data["ticker"])
                if "error" in parsed:
                    return parsed
                underlying = parsed["underlying"]
                expiry = parsed["expiry"]
                option_type = parsed["option_type"]
                strike = parsed["strike"]
            else:
                underlying = request_data.get("underlying")
                expiry = request_data.get("expiry")
                option_type = request_data.get("option_type")
                strike = request_data.get("strike")
            timestamp_str = request_data.get("timestamp")
            if not (underlying and expiry and option_type and strike and timestamp_str):
                return {"error": "Missing parameters in option minute agg request. Required: underlying, expiry, option_type, strike, timestamp."}
            return self.get_option_minute_agg_logic(underlying, expiry, option_type, strike, timestamp_str)
        else:
            return {"error": f"Unknown endpoint: {endpoint}"}

    def start(self):
        """Run the server loop to handle incoming requests."""
        print("QuoteServer is running...")
        while True:
            try:
                message = self.socket.recv()
                request_data = json.loads(message.decode("utf-8"))
                response = self.process_request(request_data)
                self.socket.send_string(json.dumps(response))
            except Exception as e:
                error_response = {"error": str(e)}
                self.socket.send_string(json.dumps(error_response))

def main():
    server = QuoteServer()
    server.start()

if __name__ == "__main__":
    main()
