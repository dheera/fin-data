#!/bin/bash
echo "Downloading aggs"
./download_flatfiles.py

sleep 1

echo "*** converting: crypto day aggs"
./csv2parquet_aggs.py global_crypto/day_aggs_v1/ global_crypto/day_aggs/ --utc
echo "*** converting: crypto minute aggs"
./csv2parquet_aggs.py global_crypto/minute_aggs_v1/ global_crypto/minute_aggs/ --utc
echo "*** converting: forex day aggs"
./csv2parquet_aggs.py global_forex/day_aggs_v1/ global_forex/day_aggs/ --utc
echo "*** converting: forex minute aggs"
./csv2parquet_aggs.py global_forex/minute_aggs_v1/ global_forex/minute_aggs/ --utc
echo "*** converting: stocks minute aggs"
./csv2parquet_aggs.py us_stocks_sip/minute_aggs_v1/ us_stocks_sip/minute_aggs/ --delete-original
echo "*** converting: stocks day aggs"
./csv2parquet_aggs.py us_stocks_sip/day_aggs_v1/ us_stocks_sip/day_aggs/ --delete-original
echo "*** converting: options minute aggs"
./csv2parquet_aggs.py us_options_opra/minute_aggs_v1/ us_options_opra/minute_aggs/ --delete-original
echo "*** converting: option day aggs"
./csv2parquet_aggs.py us_options_opra/day_aggs_v1/ us_options_opra/day_aggs/ --delete-original
echo "*** converting: indexes minute aggs"
./csv2parquet_aggs.py us_indices/minute_aggs_v1/ us_indices/minute_aggs/ --delete-original
echo "*** converting: indexes day aggs"
./csv2parquet_aggs.py us_indices/day_aggs_v1/ us_indices/day_aggs/ --delete-original

sleep 1

echo "*** converting stocks/quotes"
./csv2parquet_stocks_quotes.py us_stocks_sip/quotes_v1 us_stocks_sip/quotes --delete-original --workers 16
echo "*** converting stocks/trades"
./csv2parquet_stocks_trades.py us_stocks_sip/trades_v1 us_stocks_sip/trades --delete-original --workers 16

sleep 1

echo "*** fetching splits"
./download_splits.py

echo "*** fetching dividends"
./download_dividends.py

echo "*** generating stocks/day_aggs_by_ticker"
./gen_aggs_by_ticker.py --agg_type day us_stocks_sip/day_aggs us_stocks_sip/day_aggs_by_ticker/ --recent_days 10000

echo "*** generating stocks/adjusted_day_aggs_by_ticker"
./gen_aggs_adjusted.py --agg_type day --workers 16

echo "*** generating stocks/minute_aggs_by_ticker"
./gen_aggs_by_ticker.py --agg_type minute us_stocks_sip/minute_aggs us_stocks_sip/minute_aggs_by_ticker/2026/ --start_date 2026-01-01 --end_date 2026-12-31

./gen_aggs_by_ticker.py --agg_type minute us_stocks_sip/minute_aggs us_stocks_sip/minute_aggs_by_ticker/last730/ --recent_days 730

echo "*** generating stocks/adjusted_minute_aggs_by_ticker"
./gen_aggs_adjusted.py --agg_type minute --workers 16 --input_dir us_stocks_sip/minute_aggs_by_ticker/last730 --output_dir us_stocks_sip/adjusted_minute_aggs_by_ticker/last730

#sleep 1
#echo "*** converting options/quotes"
# ./csv2parquet_options_quotes.py us_options_opra/trades_v1 us_options_opra/quotes
#echo "*** converting options/trades"
./csv2parquet_options_trades.py us_options_opra/trades_v1 us_options_opra/trades

sleep 1

echo "*** generating stocks/matrix"
./gen_stocks_matrix.py us_stocks_sip/minute_aggs/ us_stocks_sip/minute_aggs_matrix/ --top-stocks 1024
# ./gen_stocks_matrix.py us_stocks_sip/minute_aggs/ us_stocks_sip/minute_aggs_matrix_2048 --top-stocks 2048
#echo "*** generating options/matrix"
#./gen_options_matrix.py us_stocks_sip/minute_aggs/ us_stocks_sip/minute_aggs_matrix/

echo "*** generating indices/matrix"
./gen_stocks_matrix.py us_indices/minute_aggs/ us_indices/minute_aggs_matrix/ --top-stocks 0 --no-indicators

echo "*** generating stocks/tq_aggs"
./gen_stocks_tq_aggs.py --quotes_dir us_stocks_sip/quotes --trades_dir us_stocks_sip/trades --output_dir us_stocks_sip/tq_aggs --workers 16

# python3 ./gen_aggs_by_ticker2.py --agg_type day us_indices/day_aggs us_indices/day_aggs_by_ticker/ --tickers I:SPX,I:VIX
# python3 ./gen_aggs_by_ticker2.py --agg_type minute us_indices/minute_aggs us_indices/minute_aggs_by_ticker/ --tickers I:SPX,I:VIX

