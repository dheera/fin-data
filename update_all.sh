#!/bin/bash
echo "Downloading aggs"
./download_flatfiles.py
sleep 1
echo "Converting to parquets: stocks minute aggs"
./csv2parquet_aggs.py us_stocks_sip/minute_aggs_v1/ us_stocks_sip/minute_aggs_parquet/
echo "Converting to parquets: stocks day aggs"
./csv2parquet_aggs.py us_stocks_sip/day_aggs_v1/ us_stocks_sip/day_aggs_parquet/
echo "Converting to parquets: options minute aggs"
./csv2parquet_aggs.py us_options_opra/minute_aggs_v1/ us_options_opra/minute_aggs_parquet/
echo "Converting to parquets: option day aggs"
./csv2parquet_aggs.py us_options_opra/day_aggs_v1/ us_options_opra/day_aggs_parquet/
echo "Converting to parquets: indexes minute aggs"
./csv2parquet_aggs.py us_indices/minute_aggs_v1/ us_indices/minute_aggs_parquet/
echo "Converting to parquets: indexes day aggs"
./csv2parquet_aggs.py us_indices/day_aggs_v1/ us_indices/day_aggs_parquet/
sleep 1
echo "Stock matrix"
./gen_matrix.py us_stocks_sip/minute_aggs_parquet/ us_stocks_sip/minute_aggs_matrix/ --top-stocks 1024
echo "Index matrix"
./gen_matrix.py us_indices/minute_aggs_parquet/ us_indices/minute_aggs_matrix/ --top-stocks 0 --no-indicators
