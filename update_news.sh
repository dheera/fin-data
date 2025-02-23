#!/bin/bash

./download_nasdaq_news.py --output-dir us_stocks_sip/press_json/ NVDA AMD HOOD CEG TSM MSFT SMCI RGTI QUBT AMZN ASML AAPL TXN DASH META ANSS ADI AMAT INTU GOOG PLTR AVGO CDW ON KLAC MDB DDOG QCOM PANW CTSH MSTR APP LRCX CRWD

./json2parquet_press.py --input-dir us_stocks_sip/press_json/ --output-dir us_stocks_sip/press

