# Financial data downloader for Polygon

Downloads Polygon data and convert it all to parquet-based formats that are more usable for me to do various things

## Daily crypto order books from S3

```bash
./download_crypto_books.py
./download_crypto_books.py --assets BTC ETH
./download_crypto_books.py --date 2026-10-18 --sources coinbase
./download_crypto_books.py --start-date 2026-10-17 --end-date 2026-10-18
./download_crypto_books.py --rebuild-all
```

Outputs one Parquet per UTC day, exchange and underlying asset, for example
`global_crypto/books/coinbase/2026-10-18/2026-10-18-BTC.parquet`.
The default inspects existing daily filenames, rebuilds each asset's latest
local day and newer days, and fills missing daily files. A first run backfills
available S3 days. Older existing files are assumed complete; use `--date`, a
date range or `--rebuild-all` to repair them. No progress database or completion
sidecar is stored. Each selected daily file is rebuilt from committed S3 hours
and atomically replaced; reruns never append or duplicate data.

Uses `api_keys/crypto-data-recorder-aws.json` by default, or
`--default-aws-credentials` for the normal AWS provider chain. Requires `boto3`
and `pyarrow`. `--dry-run` lists planned outputs. This script downloads books;
the separate trades and Massive datasets retain their existing formats.

Rows, UTC-nanosecond timestamps, nested levels and checkpoints are preserved.
Descriptive coverage/gap and S3 provenance metadata is embedded in each Parquet
footer. A current-day file contains only already-published hours; its existence
does not mean the day or feed coverage is complete. See
[dataset details](global_crypto/books/README.md).
