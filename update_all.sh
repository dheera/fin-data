#!/bin/bash
./download_polygon.py
sleep 1
./csv2parquet.py us_stocks_sip/minute_aggs_v1/ us_stocks_sip/minute_aggs_parquet/
./csv2parquet.py us_options_opra/minute_aggs_v1/ us_options_opra/minute_aggs_parquet/
./csv2parquet.py us_indices/minute_aggs_v1/ us_indices/minute_aggs_parquet/
sleep 1
./gen_matrix.py us_stocks_sip/minute_aggs_parquet/ us_stocks_sip/minute_aggs_matrix/
./gen_matrix.py us_options_opra/minute_aggs_parquet/ us_options_opra/minute_aggs_matrix/
./gen_matrix.py us_indices/minute_aggs_parquet/ us_indices/minute_aggs_matrix/
