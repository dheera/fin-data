#!/bin/bash
./download_perpetuals_hyperliquid.py
./download_perpetuals_kalshi.py
./download_xstocks.py
./download_crypto_books.py
cd /data/fin/prediction_markets/kalshi && ./update_daily.sh
cd /data/fin/prediction_markets/polymarket_us && ./update_daily.sh
