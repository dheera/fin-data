#!/usr/bin/env python3

import json
import zmq

class QuoteClient:
    def __init__(self, server_address="tcp://localhost:5555", timeout=5000):
        """
        :param server_address: Address of the quote server.
        :param timeout: Timeout in milliseconds for server responses.
        """
        self.server_address = server_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(server_address)
        self.socket.RCVTIMEO = timeout  # Set a receive timeout (ms)

    def send_request(self, request_data):
        """
        Send a JSON request to the server and return the JSON-decoded response.
        """
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
        Request an as-of quote for the given ticker and timestamp.
        :param ticker: Stock ticker symbol.
        :param timestamp: ISO‑formatted timestamp string.
        """
        request_data = {
            "endpoint": "quote",
            "ticker": ticker,
            "timestamp": timestamp
        }
        return self.send_request(request_data)

    def close(self):
        """Cleanly close the client socket and context."""
        self.socket.close()
        self.context.term()

if __name__ == "__main__":
    client = QuoteClient()
    # Example usage:
    ticker = "NVDA"
    timestamp = "2024-01-02T12:00:00-05:00"  # Adjust as needed.
    result = client.get_quote(ticker, timestamp)
    print("Received Response:")
    print(json.dumps(result, indent=4))
    client.close()

