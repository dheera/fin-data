#!/usr/bin/env bash
set -euo pipefail

SOURCE=/data/fin
LOCAL=/data.local/fin
OUTPUT="$SOURCE/us_stocks_sip/tq_aggs_v2"
CURRENT="$SOURCE/us_stocks_sip/tq_aggs"
LEGACY="$SOURCE/us_stocks_sip/tq_aggs_legacy_per_ticker"
LOG="$SOURCE/logs_stocks_tq_migration.log"

cd "$SOURCE"
exec >>"$LOG" 2>&1
echo "$(date --iso-8601=seconds) waiting for local cache sync"
while pgrep -f '^bash /data.local/fin/sync_fin_cache.sh --apply$' >/dev/null; do
    sleep 60
done

if [[ -e "$OUTPUT" || -e "$LEGACY" ]]; then
    echo "migration staging or legacy path already exists; refusing to continue" >&2
    exit 1
fi

echo "$(date --iso-8601=seconds) generating date-level TQ aggregates"
./gen_stocks_tq_aggs.py --output-dir "$OUTPUT" --workers 4 --force

source_dates=$(find "$SOURCE/us_stocks_sip/quotes" -mindepth 1 -maxdepth 1 -type d -name '20??-??-??' | wc -l)
legacy_dates=$(find "$CURRENT" -mindepth 1 -maxdepth 1 -type d -name '20??-??-??' | wc -l)
output_dates=$(find "$OUTPUT" -mindepth 1 -maxdepth 1 -type f -name '20??-??-??.parquet' | wc -l)
if (( output_dates < legacy_dates || output_dates > source_dates )); then
    echo "validation failed: source_dates=$source_dates legacy_dates=$legacy_dates output_dates=$output_dates" >&2
    exit 1
fi

echo "$(date --iso-8601=seconds) cutting over $output_dates date files"
mv "$CURRENT" "$LEGACY"
mv "$OUTPUT" "$CURRENT"

echo "$(date --iso-8601=seconds) syncing date-level TQ cache locally"
rsync -a --delete --partial --inplace "$CURRENT/" "$LOCAL/us_stocks_sip/tq_aggs/"

echo "$(date --iso-8601=seconds) removing validated legacy TQ tree"
rm -rf -- "$LEGACY"
echo "$(date --iso-8601=seconds) completed"
