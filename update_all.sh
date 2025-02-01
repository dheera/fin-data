#!/bin/bash
echo "Downloading data"
./download_polygon.py
sleep 1
echo "Converting to parquets: stocks"
./csv2parquet.py us_stocks_sip/minute_aggs_v1/ us_stocks_sip/minute_aggs_parquet/
echo "Converting to parquets: options"
./csv2parquet.py us_options_opra/minute_aggs_v1/ us_options_opra/minute_aggs_parquet/
echo "Converting to parquets: indexes"
./csv2parquet.py us_indices/minute_aggs_v1/ us_indices/minute_aggs_parquet/
sleep 1
echo "Stock matrix"
./gen_matrix.py us_stocks_sip/minute_aggs_parquet/ us_stocks_sip/minute_aggs_matrix/
echo "Option matrix"
./gen_matrix.py us_options_opra/minute_aggs_parquet/ us_options_opra/minute_aggs_matrix/
echo "Index matrix"
./gen_matrix.py us_indices/minute_aggs_parquet/ us_indices/minute_aggs_matrix/ --top-stocks 0 --no-indicators
