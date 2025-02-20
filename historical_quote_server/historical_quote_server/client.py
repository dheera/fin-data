#!/usr/bin/env python3

import json
import zmq
from datetime import datetime
import time

class QuoteClient:
    def __init__(self, server_address="tcp://localhost:5555", timeout=60000):
        """
        :param server_address: Address of the quote server.
        :param timeout: Timeout in milliseconds for server responses (default 60s).
        """
        self.server_address = server_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(server_address)
        self.socket.RCVTIMEO = timeout

    def send_request(self, request_data):
        """Send a JSON request to the server and return the response."""
        try:
            self.socket.send_string(json.dumps(request_data))
            response = self.socket.recv()
            return json.loads(response.decode("utf-8"))
        except zmq.error.Again:
            return {"error": "Request timed out."}
        except Exception as e:
            return {"error": str(e)}

    def _ensure_iso_timestamp(self, timestamp):
        """
        Convert the given timestamp to an ISO-formatted string.
        Accepts:
          - datetime objects (uses .isoformat())
          - Unix timestamps (int or float; converts using datetime.fromtimestamp())
          - ISO formatted strings (verifies by parsing)
        """
        if isinstance(timestamp, datetime):
            # If it's already a datetime object, just convert to ISO format.
            return timestamp.isoformat()
        elif isinstance(timestamp, (int, float)):
            # If it's a Unix timestamp, convert it to a datetime then to ISO format.
            return datetime.fromtimestamp(timestamp).isoformat()
        elif isinstance(timestamp, str):
            try:
                # Try to parse it as an ISO formatted string.
                dt = datetime.fromisoformat(timestamp)
                return dt.isoformat()
            except ValueError:
                raise ValueError("Timestamp string is not in a valid ISO format.")
        else:
            raise ValueError("Unsupported timestamp type. Must be a datetime, a Unix timestamp (int/float), or an ISO string.")


    def stock_quote(self, ticker=None, timestamp=None):
        """
        Request a stock raw quote for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "stock_quote",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)
    
    def index_value(self, ticker=None, timestamp=None):
        """
        Request a index raw quote for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "index_value",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def option_quote(self, ticker=None, underlying=None, expiry=None, option_type=None, strike=None, timestamp=None):
        """
        Request an option raw quote. Provide either:
          - A ticker string starting with "O:" (e.g., "O:NVDA261218C00162000"), OR
          - Separate parameters: underlying, expiry, option_type, strike.
        :param timestamp: ISO‑formatted timestamp string.
        """
        if ticker:
            parsed = self.parse_option_ticker(ticker)
            if "error" in parsed:
                return parsed
            underlying = parsed["underlying"]
            expiry = parsed["expiry"]
            option_type = parsed["option_type"]
            strike = parsed["strike"]
        if not (underlying and expiry and option_type and strike and timestamp):
            return {"error": "Missing parameters for option quote. Provide either a ticker or underlying, expiry, option_type, strike, and timestamp."}
        request_data = {
            "endpoint": "option_quote",
            "underlying": underlying,
            "expiry": expiry,
            "option_type": option_type,
            "strike": strike,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def parse_option_ticker(self, ticker_str):
        """
        Parse an options ticker string (e.g., "O:NVDA261218C00162000").
        Expected format: O:{underlying}{expiry}{option_type}{strike}
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
    
    def indices_minute_aggs(self, ticker=None, timestamp=None):
        """
        Request a index minute agg for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        if not ticker.startswith("I:"):
            ticker = f"I:{ticker}"

        request_data = {
            "endpoint": "indices_minute_aggs",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def index_day_aggs(self, ticker=None, timestamp=None):

        """
        Request a index day agg for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        if not ticker.startswith("I:"):
            ticker = f"I:{ticker}"

        request_data = {
            "endpoint": "indices_day_aggs",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def stocks_minute_aggs(self, ticker=None, timestamp=None):
        """
        Request a stock minute agg for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "stocks_minute_aggs",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)
    
    def stocks_day_aggs(self, ticker=None, timestamp=None):
        """
        Request a stock day agg for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "stocks_day_aggs",
            "ticker": ticker,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def options_minute_aggs(self, ticker=None, underlying=None, expiry=None, option_type=None, strike=None, timestamp=None):
        """
        Request an option minute agg. Provide either:
          - A ticker string starting with "O:" (e.g., "O:NVDA261218C00162000"), OR
          - Separate parameters: underlying, expiry, option_type, strike.
        :param timestamp: ISO‑formatted timestamp string.
        """
        if ticker:
            if not ticker.startswith("O:"):
                ticker = f"O:{ticker}"
            parsed = self.parse_option_ticker(ticker)
            if "error" in parsed:
                return parsed
            underlying = parsed["underlying"]
            expiry = parsed["expiry"]
            option_type = parsed["option_type"]
            strike = parsed["strike"]
        if not (underlying and expiry and option_type and strike and timestamp):
            return {"error": "Missing parameters for option minute agg request."}
        request_data = {
            "endpoint": "options_minute_aggs",
            "underlying": underlying,
            "expiry": expiry,
            "option_type": option_type,
            "strike": strike,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def options_day_aggs(self, ticker=None, underlying=None, expiry=None, option_type=None, strike=None, timestamp=None):
        """
        Request an option minute agg. Provide either:
          - A ticker string starting with "O:" (e.g., "O:NVDA261218C00162000"), OR
          - Separate parameters: underlying, expiry, option_type, strike.
        :param timestamp: ISO‑formatted timestamp string.
        """
        if ticker:
            if not ticker.startswith("O:"):
                ticker = f"O:{ticker}"
            parsed = self.parse_option_ticker(ticker)
            if "error" in parsed:
                return parsed
            underlying = parsed["underlying"]
            expiry = parsed["expiry"]
            option_type = parsed["option_type"]
            strike = parsed["strike"]
        if not (underlying and expiry and option_type and strike and timestamp):
            return {"error": "Missing parameters for option minute agg request."}
        request_data = {
            "endpoint": "options_day_aggs",
            "underlying": underlying,
            "expiry": expiry,
            "option_type": option_type,
            "strike": strike,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def option_chain(self, underlying=None, timestamp=None):
        if not (underlying and timestamp):
            return {"error": "Missing parameters for option chain request."}
        request_data = {
            "endpoint": "options_chains",
            "underlying": underlying,
            "timestamp": self._ensure_iso_timestamp(timestamp),
        }
        return self.send_request(request_data)

    def close(self):
        """Cleanly close the socket and context."""
        self.socket.close()
        self.context.term()

if __name__ == "__main__":
    client = QuoteClient()
    # Test stock raw quote:
    #stock_result = client.get_quote("RGTI", "2024-01-02T09:45:00-05:00")
    #print("Stock Quote Result:")
    #print(json.dumps(stock_result, indent=4))

    # Test option raw quote using ticker string:
    #option_result = client.get_option_quote(underlying="RGTI", expiry="250214", option_type="C", strike=15.0, timestamp="2025-01-02T09:30:00-05:00")
    #print("Option Quote Result (ticker):")
    #print(json.dumps(option_result, indent=4))

    # Test stock minute agg:
    stocks_minute = client.stocks_minute_aggs("RGTI", "2025-02-14T07:38:00-05:00")
    print("Stock Minute Aggregate Result:")
    print(json.dumps(stocks_minute, indent=4))

    stocks_day = client.stocks_day_aggs("RGTI", "2025-02-14T07:38:00-05:00")
    print("Stock Day Aggregate Result:")
    print(json.dumps(stocks_day, indent=4))

    indices_minute = client.indices_minute_aggs("I:VIX", "2025-01-03T07:38:00-05:00")
    print("Index Minute Aggregate Result:")
    print(json.dumps(indices_minute, indent=4))

    # Test option minute agg using separate parameters:
    options_minute = client.options_minute_aggs(underlying="RGTI", expiry="250117", option_type="C", strike=18.0, timestamp="2025-01-02T09:31:00-05:00")
    print("Option Minute Aggregate Result (parameters):")
    print(json.dumps(options_minute, indent=4))

    client.close()

