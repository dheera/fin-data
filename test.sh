#!/usr/bin/env python3

import os, sys

for year in range(2002, 2026):
  cmd = f"./gen_aggs_by_ticker.py --agg_type minute us_stocks_sip/minute_aggs us_stocks_sip/minute_aggs_by_ticker/{year}/ --start_date {year}-01-01 --end_date {year}-12-31"
  print(cmd)
  os.system(cmd)

