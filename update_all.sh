#!/bin/bash
echo "Downloading aggs"
./download_flatfiles.py

sleep 1

echo "*** converting: stocks minute aggs"
./csv2parquet_aggs.py us_stocks_sip/minute_aggs_v1/ us_stocks_sip/minute_aggs/
echo "*** converting: stocks day aggs"
./csv2parquet_aggs.py us_stocks_sip/day_aggs_v1/ us_stocks_sip/day_aggs/
echo "*** converting: options minute aggs"
./csv2parquet_aggs.py us_options_opra/minute_aggs_v1/ us_options_opra/minute_aggs/
echo "*** converting: option day aggs"
./csv2parquet_aggs.py us_options_opra/day_aggs_v1/ us_options_opra/day_aggs/
echo "*** converting: indexes minute aggs"
./csv2parquet_aggs.py us_indices/minute_aggs_v1/ us_indices/minute_aggs/
echo "*** converting: indexes day aggs"
./csv2parquet_aggs.py us_indices/day_aggs_v1/ us_indices/day_aggs/

sleep 1

echo "*** converting stocks/quotes"
./csv2parquet_stocks_quotes.py us_stocks_sip/trades_v1 us_stocks_sip/quotes
echo "*** converting stocks/trades"
./csv2parquet_stocks_trades.py us_stocks_sip/trades_v1 us_stocks_sip/trades
#echo "*** converting options/quotes"
# ./csv2parquet_options_quotes.py us_options_opra/trades_v1 us_options_opra/quotes
echo "*** converting options/trades"
./csv2parquet_options_trades.py us_options_opra/trades_v1 us_options_opra/trades

sleep 1

echo "*** generating stocks/matrix"
./gen_matrix.py us_stocks_sip/minute_aggs/ us_stocks_sip/minute_aggs/ --top-stocks 1024
echo "*** generating indices/matrix"
./gen_matrix.py us_indices/minute_aggs/ us_indices/minute_aggs/ --top-stocks 0 --no-indicators

#sleep 1
#echo "Day aggs by ticker"
#./gen_aggs_by_ticker.py us_stocks_sip/day_aggs/ us_stocks_sip/day_aggs_by_ticker/
