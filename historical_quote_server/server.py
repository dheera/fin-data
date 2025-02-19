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

class QuoteServer:
    def __init__(self, base_dir, zmq_bind_address="tcp://*:5555", max_cache_bytes=100 * 1024 * 1024):
        """
        :param base_dir: Root directory where the parquet files are stored.
        :param zmq_bind_address: The ZeroMQ bind address.
        :param max_cache_bytes: Maximum cache size in bytes.
        """
        self.base_dir = base_dir
        self.cache = LRUCache(maxsize=max_cache_bytes, getsizeof=dataframe_size)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(zmq_bind_address)
        print(f"QuoteServer bound to {zmq_bind_address}")

    def get_parquet_path(self, ticker, request_date):
        """
        Given a ticker and a date (datetime.date), return the path to the parquet file.
        If the file does not exist for that date (e.g. weekends/holidays), search backwards.
        """
        date_to_try = request_date
        min_date = datetime.date(2000, 1, 1)  # Arbitrary lower bound
        while date_to_try >= min_date:
            dir_path = os.path.join(self.base_dir, date_to_try.isoformat())
            if os.path.isdir(dir_path):
                pattern = os.path.join(dir_path, f"{date_to_try.isoformat()}-{ticker}*.parquet")
                files = glob.glob(pattern)
                if files:
                    return files[0]
            date_to_try -= datetime.timedelta(days=1)
        return None

    def load_parquet_file(self, file_path):
        """
        Load a parquet file into a Pandas DataFrame.
        Ensure that the 'sip_timestamp' column is parsed as datetime and sorted.
        """
        df = pd.read_parquet(file_path)
        if "sip_timestamp" not in df.columns:
            raise ValueError(f"'sip_timestamp' column not found in {file_path}")
        df["sip_timestamp"] = pd.to_datetime(df["sip_timestamp"], errors="coerce")
        df.sort_values("sip_timestamp", inplace=True)
        return df

    def get_quote_from_df(self, df, query_timestamp):
        """
        Retrieve the most recent (as-of) quote from the DataFrame given a query timestamp.
        """
        query_ts = pd.to_datetime(query_timestamp)
        idx = df["sip_timestamp"].searchsorted(query_ts, side="right") - 1
        if idx >= 0:
            return df.iloc[idx].to_dict()
        return None

    def process_request(self, request_data):
        """
        Dispatch incoming requests to the proper handler based on the 'endpoint' key.
        By default, the endpoint "quote" is used for equity quotes.
        """
        endpoint = request_data.get("endpoint", "quote")
        if endpoint == "quote":
            return self.handle_quote_request(request_data)
        else:
            return {"error": f"Unknown endpoint: {endpoint}"}

    def handle_quote_request(self, request_data):
        """
        Handle a 'quote' request. The expected keys in request_data are:
          - "ticker": Stock ticker symbol.
          - "timestamp": ISO‑formatted timestamp string.
        """
        ticker = request_data.get("ticker")
        timestamp_str = request_data.get("timestamp")
        if not ticker or not timestamp_str:
            return {"error": "Missing 'ticker' or 'timestamp' in request."}

        try:
            query_ts = pd.to_datetime(timestamp_str)
        except Exception as e:
            return {"error": f"Invalid timestamp format: {e}"}

        request_date = query_ts.date()
        file_path = self.get_parquet_path(ticker, request_date)
        if not file_path:
            return {"error": f"No data file found for ticker {ticker} on or before {request_date}"}

        # Use the cached DataFrame if available; otherwise load from disk.
        if file_path in self.cache:
            df = self.cache[file_path]
        else:
            try:
                df = self.load_parquet_file(file_path)
            except Exception as e:
                return {"error": f"Failed to load file {file_path}: {e}"}
            self.cache[file_path] = df

        quote = self.get_quote_from_df(df, query_ts)
        if quote is None:
            return {"error": f"No quote found before {timestamp_str} in file {file_path}"}

        # Convert non-serializable types to native Python types.
        for key, value in quote.items():
            if pd.isna(value):
                quote[key] = None
            elif isinstance(value, (pd.Timestamp, datetime.datetime)):
                quote[key] = value.isoformat()
            elif hasattr(value, "item"):
                quote[key] = value.item()

        quote["source_file"] = file_path
        return quote

    def start(self):
        """Run the server's main loop to listen for incoming requests."""
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
    BASE_DIR = "/fin/us_stocks_sip/quotes"  # Update this path as needed
    server = QuoteServer(base_dir=BASE_DIR)
    server.start()

