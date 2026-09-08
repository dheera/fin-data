#!/usr/bin/env bash
# Populate the SSD cache from the authoritative NAS tree.
#
# Defaults may be overridden with environment variables, for example:
#   OPTIONS_MONTHS=3 FUTURES_YEARS=4 ./sync_fin_cache.sh --apply

set -euo pipefail

SOURCE=/data/fin
DESTINATION=/data.local/fin
APPLY=0
PRUNE=1

# Retention policy.  Aggregate directories are always complete when enabled.
FULL_AGGREGATES="${FULL_AGGREGATES:-1}"
DATA_YEARS="${DATA_YEARS:-2}"
STOCKS_YEARS="${STOCKS_YEARS:-$DATA_YEARS}"
FUTURES_YEARS="${FUTURES_YEARS:-$DATA_YEARS}"
FOREX_YEARS="${FOREX_YEARS:-$DATA_YEARS}"
CRYPTO_YEARS="${CRYPTO_YEARS:-$DATA_YEARS}"
OTHER_YEARS="${OTHER_YEARS:-$DATA_YEARS}"
OPTIONS_MONTHS="${OPTIONS_MONTHS:-2}"

usage() {
    cat <<'EOF'
Usage: sync_fin_cache.sh [options]

Synchronize /data.local/fin from the authoritative /data/fin NAS.
The default is a dry run.  Use --apply to transfer files and prune stale,
managed date partitions from the local cache.

Options:
  --apply                 Perform changes (default: dry run)
  --no-prune              Do not remove expired/missing date partitions
  --source PATH           NAS source (default: /data/fin)
  --destination PATH      Local cache (default: /data.local/fin)
  --data-years N          Default retention for non-options data (default: 2)
  --options-months N      Options raw-data retention (default: 2)
  --help                  Show this help

Environment overrides:
  FULL_AGGREGATES=0       Skip full *aggs* directory mirroring
  STOCKS_YEARS=N          Retention for us_stocks_sip raw data
  FUTURES_YEARS=N         Retention for us_futures_* raw data
  FOREX_YEARS=N           Retention for global_forex raw data
  CRYPTO_YEARS=N          Retention for global_crypto raw data
  OTHER_YEARS=N           Retention for other date-partitioned data
EOF
}

while (($#)); do
    case "$1" in
        --apply) APPLY=1 ;;
        --no-prune) PRUNE=0 ;;
        --source) SOURCE=$2; shift ;;
        --destination) DESTINATION=$2; shift ;;
        --data-years) DATA_YEARS=$2; STOCKS_YEARS=$2; FUTURES_YEARS=$2; FOREX_YEARS=$2; CRYPTO_YEARS=$2; OTHER_YEARS=$2; shift ;;
        --options-months) OPTIONS_MONTHS=$2; shift ;;
        --help) usage; exit 0 ;;
        *) printf 'Unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

[[ -d "$SOURCE" ]] || { printf 'Source does not exist: %s\n' "$SOURCE" >&2; exit 1; }
[[ "$DESTINATION" == /data.local/fin || "$DESTINATION" == /data.local/fin/* ]] || {
    printf 'Destination must be /data.local/fin or a subdirectory: %s\n' "$DESTINATION" >&2
    exit 2
}
mkdir -p "$DESTINATION"

stock_cutoff=$(date -d "$STOCKS_YEARS years ago" +%F)
futures_cutoff=$(date -d "$FUTURES_YEARS years ago" +%F)
forex_cutoff=$(date -d "$FOREX_YEARS years ago" +%F)
crypto_cutoff=$(date -d "$CRYPTO_YEARS years ago" +%F)
other_cutoff=$(date -d "$OTHER_YEARS years ago" +%F)
options_cutoff=$(date -d "$OPTIONS_MONTHS months ago" +%F)

cutoff_for() {
    case "$1" in
        us_options_opra/*) printf '%s\n' "$options_cutoff" ;;
        us_stocks_sip/*) printf '%s\n' "$stock_cutoff" ;;
        us_futures_*/*) printf '%s\n' "$futures_cutoff" ;;
        global_forex/*) printf '%s\n' "$forex_cutoff" ;;
        global_crypto/*) printf '%s\n' "$crypto_cutoff" ;;
        *) printf '%s\n' "$other_cutoff" ;;
    esac
}

rsync_args=(-a --human-readable --info=stats2 --partial --inplace)
if (( ! APPLY )); then
    rsync_args+=(--dry-run)
fi

sync_dir() {
    local rel=$1
    if (( APPLY )); then
        mkdir -p "$DESTINATION/$rel"
    fi
    rsync "${rsync_args[@]}" --delete "$SOURCE/$rel/" "$DESTINATION/$rel/"
}

sync_file() {
    local rel=$1
    if (( APPLY )); then
        mkdir -p "$(dirname "$DESTINATION/$rel")"
    fi
    rsync "${rsync_args[@]}" "$SOURCE/$rel" "$DESTINATION/$rel"
}

printf 'Source: %s\nDestination: %s\nMode: %s\n' "$SOURCE" "$DESTINATION" "$([[ $APPLY -eq 1 ]] && printf apply || printf dry-run)"
printf 'Retention: stocks=%sy futures=%sy forex=%sy crypto=%sy other=%sy options=%sm; full aggregates=%s\n' \
    "$STOCKS_YEARS" "$FUTURES_YEARS" "$FOREX_YEARS" "$CRYPTO_YEARS" "$OTHER_YEARS" "$OPTIONS_MONTHS" "$FULL_AGGREGATES"

if (( PRUNE )); then
    # Reclaim expired raw partitions before copying aggregate files.  This is
    # essential on a nearly-full SSD and cannot affect aggregate directories.
    while IFS= read -r -d '' dated; do
        rel=${dated#"$DESTINATION"/}
        [[ "$rel" == *aggs* ]] && continue
        day=${rel##*/}
        cutoff=$(cutoff_for "$rel")
        if [[ "$day" < "$cutoff" || ! -d "$SOURCE/$rel" ]]; then
            if (( APPLY )); then
                printf 'Pruning %s\n' "$dated"
                rm -rf -- "$dated"
            else
                printf 'Would prune %s\n' "$dated"
            fi
        fi
    done < <(find "$DESTINATION" -mindepth 3 -maxdepth 3 -type d -regextype posix-extended \
        -regex '.*/20[0-9]{2}-[01][0-9]-[0-3][0-9]' -print0 | sort -z)
fi

# Aggregate outputs are compact enough to keep in full.  Limiting the search
# to two levels avoids walking the contents of multi-terabyte data directories.
if [[ "$FULL_AGGREGATES" == 1 ]]; then
    while IFS= read -r -d '' aggregate; do
        sync_dir "${aggregate#"$SOURCE"/}"
    done < <(find "$SOURCE" -mindepth 2 -maxdepth 2 -type d -name '*aggs*' -print0 | sort -z)
fi

# Copy compact, non-date-partitioned parquet snapshots (tickers, dividends,
# corporate events, etc.) in full.
while IFS= read -r -d '' snapshot; do
    sync_file "${snapshot#"$SOURCE"/}"
done < <(find "$SOURCE" -mindepth 1 -maxdepth 2 -type f -name '*.parquet' -print0 | sort -z)

# Raw feeds use YYYY-MM-DD directories.  The directory name, not mtime,
# determines retention so backfills do not accidentally expand the cache.
while IFS= read -r -d '' dated; do
    rel=${dated#"$SOURCE"/}
    [[ "$rel" == *aggs* ]] && continue
    day=${rel##*/}
    [[ "$day" < "$(cutoff_for "$rel")" ]] && continue
    sync_dir "$rel"
done < <(find "$SOURCE" -mindepth 3 -maxdepth 3 -type d -regextype posix-extended \
    -regex '.*/20[0-9]{2}-[01][0-9]-[0-3][0-9]' -print0 | sort -z)

printf 'Finished. Local filesystem:\n'
df -h "$DESTINATION"
