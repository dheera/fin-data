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
    def __init__(self, stock_base_dir, options_base_dir, zmq_bind_address="tcp://*:5555", max_cache_bytes=20 * 1024**3):
        """
        :param stock_base_dir: Directory for stock parquet files.
        :param options_base_dir: Directory for options parquet files.
        :param zmq_bind_address: ZeroMQ bind address.
        :param max_cache_bytes: Maximum cache size in bytes (default 20GB).
        """
        self.stock_base_dir = stock_base_dir
        self.options_base_dir = options_base_dir
        self.cache = LoggingLRUCache(maxsize=max_cache_bytes, getsizeof=dataframe_size)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(zmq_bind_address)
        print(f"QuoteServer bound to {zmq_bind_address}")

    def get_stock_parquet_path(self, ticker, request_date):
        """Return the stock parquet file for a given ticker and date."""
        dir_path = os.path.join(self.stock_base_dir, request_date.isoformat())
        if os.path.isdir(dir_path):
            pattern = os.path.join(dir_path, f"{request_date.isoformat()}-{ticker}*.parquet")
            files = glob.glob(pattern)
            if files:
                return files[0]
        return None

    def get_option_parquet_path(self, underlying, request_date):
        """Return the options parquet file for a given underlying and date."""
        dir_path = os.path.join(self.options_base_dir, request_date.isoformat())
        if os.path.isdir(dir_path):
            pattern = os.path.join(dir_path, f"{request_date.isoformat()}-{underlying}*.parquet")
            files = glob.glob(pattern)
            if files:
                return files[0]
        return None

    def load_parquet_file(self, file_path):
        """Load a parquet file into a DataFrame. Assumes the file is already sorted by sip_timestamp."""
        print(f"DEBUG: Loading file: {file_path}")
        df = pd.read_parquet(file_path)
        if "sip_timestamp" not in df.columns:
            raise ValueError(f"'sip_timestamp' column not found in {file_path}")
        # Since parquets are assumed sorted, we don't convert or re-sort.
        return df

    def get_quote_from_df(self, df, query_timestamp):
        """Return the as‑of quote from a sorted DataFrame given the query timestamp."""
        query_ts = pd.to_datetime(query_timestamp)
        idx = df["sip_timestamp"].searchsorted(query_ts, side="right") - 1
        if idx >= 0:
            return df.iloc[idx].to_dict()
        return None

    def get_stock_quote_logic(self, ticker, timestamp_str):
        """
        Process a stock quote request:
          - Loads the corresponding parquet file (or retrieves it from cache).
          - Checks that the query timestamp falls within trading hours.
          - Returns the as‑of quote.
        """
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

    def get_option_quote_logic(self, underlying, expiry, option_type, strike, timestamp_str):
        """
        Process an option quote request:
          - Loads the corresponding options parquet file.
          - Uses the multi-index (expiry, type, strike) to select the appropriate rows.
          - Checks that the query timestamp falls within trading hours.
          - Returns the as‑of quote.
        """
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

        # Use the multi-index (expiry, type, strike) to filter rows.
        try:
            df_filtered = df.loc[int(expiry), str(option_type), float(strike)]
            # If a single row is returned as a Series, convert it to a DataFrame.
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

    def process_request(self, request_data):
        """
        Dispatch incoming requests by endpoint.
          - "quote": stock quote.
          - "option_quote": option quote.
        """
        endpoint = request_data.get("endpoint", "quote")
        if endpoint == "quote":
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

if __name__ == "__main__":
    # Update these paths as needed.
    stock_base_dir = "/fin/us_stocks_sip/quotes"
    options_base_dir = "/fin/us_options_opra/quotes"
    server = QuoteServer(stock_base_dir=stock_base_dir, options_base_dir=options_base_dir)
    server.start()

