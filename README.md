# Financial data downloader for Polygon

Downloads Polygon data and convert it all to parquet-based formats that are more usable for me to do various things

## Daily crypto order books from S3

```bash
./download_crypto_books.py
./download_crypto_books.py --assets BTC ETH
./download_crypto_books.py --sources binance
./download_crypto_books.py --date 2026-10-18 --sources coinbase
./download_crypto_books.py --start-date 2026-10-17 --end-date 2026-10-18
./download_crypto_books.py --rebuild-all
```

Outputs one Parquet per UTC day, exchange and underlying asset, for example
`global_crypto/books/coinbase/2026-10-18/2026-10-18-BTC.parquet`,
`global_crypto/books/binance/2026-10-20/2026-10-20-BTC.parquet`, or
`perpetuals/kalshi/books/2026-10-18/2026-10-18-BTC.parquet`.
The default discovers all published book sources, including `binance_spot`;
the existing daily updater therefore includes Binance without extra flags.
Local spot books use the exchange directory `binance`. `--sources binance`
selects the S3 source `binance_spot`; the older `--sources binance_spot` also
works. S3 keys, row source values and provenance retain `binance_spot`.
`--output` sets the common data root (default `/data/fin`), containing
`global_crypto/` and `perpetuals/`.
The default inspects existing daily filenames, rebuilds each asset's latest
local day and newer days, and fills missing daily files. A first run backfills
available S3 days. Older existing files are assumed complete; use `--date`, a
date range or `--rebuild-all` to repair them. No progress database or completion
sidecar is stored. Each selected daily file is rebuilt from committed S3 hours
and atomically replaced; reruns never append or duplicate data.

Uses `api_keys/crypto-data-recorder-aws.json` by default, or
`--default-aws-credentials` for the normal AWS provider chain. Requires `boto3`
and `pyarrow`. `--dry-run` lists planned outputs. This script downloads books;
Coinbase trades come from Massive at `global_crypto/trades/<date>.parquet`,
so no Coinbase collector trades are downloaded. Binance collector trades also
remain excluded, even when referenced by the same hourly manifest. Kalshi trades live under
`perpetuals/kalshi/trades/`, updated by `download_perpetuals_kalshi.py`.

Rows, UTC-nanosecond timestamps, nested levels and checkpoints are preserved.
Binance BTCUSDT is named BTC locally, but prices stay **USDT**, the row symbol
stays `BTCUSDT`, and unavailable exchange timestamps stay null.
Descriptive coverage/gap and S3 provenance metadata is embedded in each Parquet
footer. A current-day file contains only already-published hours; its existence
does not mean the day or feed coverage is complete. See
[dataset details](global_crypto/books/README.md).

Both original schema-v2 direct hourly files and newer assembled hours are
supported, with checksums, row counts and metadata validated for each format.
`hours: 24` and `missing_hours: []` mean all hourly files were fetched;
`book_coverage_complete: false` can still reflect recording gaps inside them.
Output includes `book_coverage_gap_count`, `book_uncovered_seconds` (including
unpublished hours), and `source_gap_count` (recorded diagnostic events).
Refetching cannot restore updates that the collector never recorded.
