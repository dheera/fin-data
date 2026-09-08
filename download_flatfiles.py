#!/usr/bin/env python3
import os
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import time
import json

with open("polygon.json", "r") as f:
    config = json.loads(f.read())

session = boto3.Session(
   aws_access_key_id=config["s3_access_key_id"],
   aws_secret_access_key=config["s3_secret_access_key"],
)

s3 = session.client(
   's3',
   endpoint_url=config["s3_endpoint"],
   config=Config(signature_version='s3v4'),
)

bucket_name = config["s3_bucket"]

# Polygon serves ~2 MB/s per connection, so throughput scales with concurrency:
# 10 threads (default) ~20 MB/s, 32 ~60 MB/s, 64 ~130 MB/s peak, 128 ~150 MB/s peak
# on our 2 Gbps link. Diminishing returns past 128.
transfer_config = TransferConfig(max_concurrency=128)

def download(prefix, year_start = 2021, year_end = 2026):
    to_download = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket='flatfiles', Prefix=prefix):
        for obj in page['Contents']:
            for year in range(year_start, year_end):
                if f"/{year}/" in obj['Key']:
                    to_download.append(obj['Key'])

    os.makedirs(os.path.join(".", prefix), exist_ok=True)
    for object_key in to_download:
        local_file_name = object_key.split('/')[-1]
        local_file_path = os.path.join('.' , prefix , local_file_name)

        if os.path.exists(local_file_path):
            print(f"Exists, skipping: {object_key}")
            continue
    
        try:
            print(f"Downloading {object_key}")
            result = s3.download_file(bucket_name, object_key, local_file_path, Config=transfer_config)
        except Exception as e:
            print(e)
            if int(e.response.get('Error',{}).get('Code')) == 403:
                print("403 Unauthorized, ending early")
                return
            
        time.sleep(0.1)

# - 'global_crypto' for global cryptocurrency data
# - 'global_forex' for global forex data
# - 'us_indices' for US indices data
# - 'us_options_opra' for US options (OPRA) data
# - 'us_stocks_sip' for US stocks (SIP) data

year_start = 2026
year_end = 2027
market_list = [
  'us_indices',
  'global_forex',
  'global_crypto',
  'global_forex',
  'us_stocks_sip',
  'us_options_opra',
  'us_futures_cbot',
  'us_futures_cme',
  'us_futures_nymex',
  'us_futures_comex',
]
datatype_list = [
  'minute_aggs_v1',
  'day_aggs_v1',
  'quotes_v1',
  'trades_v1',
]

for market in market_list:
    for datatype in datatype_list:
        try:
            print(f"*** Fetching {market}/{datatype}/ ***")
            download(f"{market}/{datatype}/", year_start = year_start, year_end = year_end)
            time.sleep(0.2)
        except KeyError:
            print(f"warning: f{market} does not have {datatype}")

