#!/usr/bin/env python3
import os
import boto3
from botocore.config import Config
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
            result = s3.download_file(bucket_name, object_key, local_file_path)
        except Exception as e:
            print(e)
            
        time.sleep(0.5)

# - 'global_crypto' for global cryptocurrency data
# - 'global_forex' for global forex data
# - 'us_indices' for US indices data
# - 'us_options_opra' for US options (OPRA) data
# - 'us_stocks_sip' for US stocks (SIP) data

download('us_indices/minute_aggs_v1/', year_start = 2024, year_end = 2026)
download('us_indices/day_aggs_v1/', year_start = 2024, year_end = 2026)

download('us_stocks_sip/minute_aggs_v1/', year_start = 2024, year_end = 2026)
download('us_stocks_sip/day_aggs_v1/', year_start = 2024, year_end = 2026)
download('us_stocks_sip/quotes_v1/', year_start = 2023, year_end = 2026)
download('us_stocks_sip/trades_v1/', year_start = 2023, year_end = 2026)

download('us_options_opra/minute_aggs_v1/', year_start = 2024, year_end = 2026)
download('us_options_opra/day_aggs_v1/', year_start = 2024, year_end = 2026)
# download('us_options_opra/quotes_v1/', year_start = 2025, year_end = 2026) # huge 100GB files
download('us_options_opra/trades_v1/', year_start = 2025, year_end = 2026)


