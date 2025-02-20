#!/usr/bin/env python3

import json
import zmq

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

    def get_quote(self, ticker, timestamp):
        """
        Request a stock raw quote for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "quote",
            "ticker": ticker,
            "timestamp": timestamp
        }
        return self.send_request(request_data)
    
    def get_index(self, ticker, timestamp):
        """
        Request a index raw quote for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "quote",
            "ticker": ticker,
            "timestamp": timestamp
        }
        return self.send_request(request_data)

    def get_option_quote(self, ticker=None, underlying=None, expiry=None, option_type=None, strike=None, timestamp=None):
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
            "timestamp": timestamp
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
    
    def get_index_minute_aggregate(self, ticker, timestamp):
        """
        Request a stock minute aggregate for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "index_minute_aggregate",
            "ticker": ticker,
            "timestamp": timestamp
        }
        return self.send_request(request_data)

    def get_minute_aggregate(self, ticker, timestamp):
        """
        Request a stock minute aggregate for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "minute_aggregate",
            "ticker": ticker,
            "timestamp": timestamp
        }
        return self.send_request(request_data)

    def get_option_minute_aggregate(self, ticker=None, underlying=None, expiry=None, option_type=None, strike=None, timestamp=None):
        """
        Request an option minute aggregate. Provide either:
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
            return {"error": "Missing parameters for option minute aggregate request."}
        request_data = {
            "endpoint": "option_minute_aggregate",
            "underlying": underlying,
            "expiry": expiry,
            "option_type": option_type,
            "strike": strike,
            "timestamp": timestamp
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

    # Test stock minute aggregate:
    stock_minute = client.get_minute_aggregate("RGTI", "2025-02-14T07:38:00-05:00")
    print("Stock Minute Aggregate Result:")
    print(json.dumps(stock_minute, indent=4))

    index_minute = client.get_index_minute_aggregate("I:VIX", "2025-01-03T07:38:00-05:00")
    print("Index Minute Aggregate Result:")
    print(json.dumps(index_minute, indent=4))

    # Test option minute aggregate using separate parameters:
    option_minute = client.get_option_minute_aggregate(underlying="RGTI", expiry="250117", option_type="C", strike=18.0, timestamp="2025-01-02T09:31:00-05:00")
    print("Option Minute Aggregate Result (parameters):")
    print(json.dumps(option_minute, indent=4))

    client.close()

