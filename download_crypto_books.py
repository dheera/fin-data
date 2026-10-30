#!/usr/bin/env python3
"""Download hourly S3 order books and assemble one Parquet per asset/UTC day.

Typical usage (run from /data/fin):
    ./download_crypto_books.py                            # All published book sources
    ./download_crypto_books.py --sources binance          # Binance spot books only
    ./download_crypto_books.py --sources binance --assets BTC ETH
    ./download_crypto_books.py --date 2026-10-20 --dry-run # Validate/preview a day

Data flow:
    1. Discover sources/dates under s3://fin.dheera.net/global_crypto/books/.
    2. Read finalized hourly manifests to learn native symbols and base assets.
    3. Choose days from local daily filenames, with no saved progress database.
    4. Download/check one hourly book file at a time for each daily worker.
    5. Stream unchanged Arrow rows into a temporary daily file, then replace
       the destination atomically only after the entire selected day validates.

Local destinations (relative to --output, which defaults to this directory):
    global_crypto/books/coinbase/<date>/<date>-BTC.parquet
    global_crypto/books/binance/<date>/<date>-BTC.parquet
    perpetuals/kalshi/books/<date>/<date>-BTC.parquet

An unrestricted run discovers Coinbase, Kalshi perps, Binance spot, and any
other source publishing this same book format. It never downloads trades,
including trade objects referenced by the same hourly commit manifest.
The local Binance directory is binance because global_crypto contains spot
data. Its S3 source and row/provenance values remain binance_spot; --sources
accepts either name and resolves it to the same S3 inputs.
Binance BTCUSDT is named BTC locally, but its prices remain USDT, its row symbol
remains BTCUSDT, and absent exchange timestamps remain null. No price conversion,
resampling, timestamp substitution or deduplication happens here.

Each asset's latest local UTC day is rebuilt on every default run, along with
newer days and missing daily files. Thus running today can write a partial day;
running tomorrow completes that day from the hours available then. Cold starts
fetch all published days. Existing days older than the latest local day are
assumed complete: --date, a date range or --rebuild-all repairs those explicitly.
Every selected file is rebuilt from S3; existing local rows are never appended.

Requires boto3 and pyarrow. Credentials default to
api_keys/crypto-data-recorder-aws.json; credentials and remote response bodies
must never be printed. This program reads S3 and writes local data only.
"""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

import boto3
from botocore.config import Config
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# All archive windows are UTC and half-open: start <= timestamp < end.
# Integer nanoseconds avoid rounding receipt clocks through floats/datetimes.
HERE = Path(__file__).resolve().parent
HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS
UTC_NS = pa.timestamp('ns', tz='UTC')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\Z')
ASSET = re.compile(r'[A-Z0-9][A-Z0-9_.-]{0,39}\Z')
SOURCE = re.compile(r'[a-z0-9][a-z0-9_]{0,39}\Z')
DAILY = re.compile(r'(\d{4}-\d{2}-\d{2})-([A-Z0-9][A-Z0-9_.-]{0,39})\.parquet\Z')
# Hours may have different connection IDs or operational metadata, but these
# fields define how to interpret values. A daily file must never silently mix
# USD/USDT, different native instruments, scaling conventions or feed cadences.
# Filenames alone (e.g. BTC) cannot tell a researcher which currency prices use.
INTERPRETATION = (
    'source', 'symbol', 'base_asset', 'native_asset', 'quote_currency',
    'price_unit', 'size_unit', 'native_price_unit', 'native_size_unit',
    'price_scale', 'size_scale', 'price_scale_numerator', 'price_scale_denominator',
    'size_scale_numerator', 'size_scale_denominator', 'contract_size',
    'underlying_multiplier', 'source_depth', 'source_depth_kind', 'channel', 'cadence_ms',
)


class FetchError(RuntimeError):
    """A controlled validation error whose message is safe for JSON logs."""
    pass


def require(condition, message):
    """Use explicit checks, not assertions that disappear under python -O."""
    if not condition:
        raise FetchError(message)


def encoded(value):
    """Stable, finite JSON for embedded provenance and identity comparisons."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def parse_day(value):
    """Accept only canonical YYYY-MM-DD dates; reject alternative spellings."""
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError):
        raise FetchError('invalid UTC date: ' + str(value)) from None
    require(result.isoformat() == value, 'dates must use YYYY-MM-DD')
    return result


def day_ns(day):
    """Return midnight UTC as exact integer nanoseconds since the Unix epoch."""
    return (parse_day(day) - date(1970, 1, 1)).days * DAY_NS


def safe_path(root, *parts):
    """Keep archive-supplied path components inside the requested local root.

    Native symbols and dates come from remote metadata. Reject separators,
    traversal and existing symlink components before using them as filenames.
    This is a path guard, not protection against a hostile concurrent local user.
    """
    root = Path(root).resolve()
    path = root
    for part in parts:
        require(part not in ('', '.', '..') and '/' not in part and '\\' not in part,
                'unsafe output path')
        path = path / part
        require(not path.is_symlink(), 'symlink in output path: ' + str(path))
    return path


def check_space(path, minimum, extra=0):
    """Preserve a free-space reserve before a planned allocation/write."""
    require(shutil.disk_usage(path).free >= minimum + extra, 'insufficient free disk space')


def fsync_dir(path):
    """Persist the renamed directory entry after the daily file is fsynced."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def output_lock(root):
    """Allow one writer invocation per data root, including overlapping cron runs.

    The empty lock file contains no progress or completion state. Its descriptor
    owns the advisory lock; closing it releases the lock even after an exception.
    Dry-run callers skip this lock because they do not modify the data root.
    """
    # This is an empty mutual-exclusion lock, not saved download progress.
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(safe_path(root, '.archive.lock'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o660)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise FetchError('another archive download is using this output directory') from None
        yield
    finally:
        os.close(fd)


def make_client(credentials, region):
    """Create a shared read client without logging any credential material.

    An explicit JSON file is the local NAS default. Passing None delegates to
    boto3's usual credential chain. Retries cover S3 request failures; a failed
    streaming-body read still aborts the daily replacement, preserving old data.
    """
    kwargs = {}
    if credentials is not None:
        try:
            value = json.loads(Path(credentials).read_text())
            for key in ('aws_access_key_id', 'aws_secret_access_key'):
                require(isinstance(value.get(key), str) and value[key].strip(), 'invalid credentials')
            kwargs = {key: value[key] for key in
                      ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token') if key in value}
        except (OSError, ValueError, TypeError, AttributeError):
            raise FetchError('cannot read AWS credentials JSON') from None
    return boto3.client('s3', region_name=region,
                        config=Config(retries={'mode': 'standard', 'max_attempts': 8},
                                      max_pool_connections=16), **kwargs)


def list_children(client, bucket, prefix):
    """List immediate S3 prefix children, not every object recursively.

    S3 has keys rather than real directories. Delimiter='/' provides the source
    and date "directory" view, while pagination handles arbitrarily many keys.
    """
    for page in client.get_paginator('list_objects_v2').paginate(
            Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for entry in page.get('CommonPrefixes', []):
            child = entry['Prefix'][len(prefix):].rstrip('/')
            if child and '/' not in child:
                yield child


def books_directory(root, source):
    """Map the archive source to the existing /data/fin directory conventions.

    Kalshi is a perpetuals dataset with its established local path. Binance's
    local venue directory is simply binance: global_crypto already means spot.
    Keep binance_spot in S3 keys and provenance to identify the original inputs.
    This directory mapping does not alter any row values or quote currencies.
    """
    if source == 'kalshi_perp':
        return safe_path(root, 'perpetuals', 'kalshi', 'books')
    if source == 'binance_spot':
        source = 'binance'
    return safe_path(root, 'global_crypto', 'books', source)


def latest_local_days(root, source, assets=None):
    """Infer each asset's newest local date using only canonical daily filenames.

    A BTC-USD-1200.parquet hourly file, a .partial file, a wrong-date filename
    or a symlink is not a completed daily output. Do not open Parquet contents
    here: a corrupt newest file must still be selected and rebuilt from S3.
    Sources have separate roots, so Coinbase BTC never marks Binance BTC done.
    """
    result = {}
    directory = books_directory(root, source)
    if not directory.exists():
        return result
    for folder in directory.iterdir():
        if folder.is_symlink() or not folder.is_dir():
            continue
        try:
            parse_day(folder.name)
        except FetchError:
            continue
        for file in folder.iterdir():
            match = DAILY.fullmatch(file.name)
            if (not match or file.is_symlink() or not file.is_file()
                    or match[1] != folder.name or (assets and match[2] not in assets)):
                continue
            result[match[2]] = max(result.get(match[2], ''), folder.name)
    return result


def read_manifest(client, bucket, key):
    """Read a bounded commit manifest and verify S3's native SHA-256 checksum.

    The manifest is the authoritative list of committed hourly data objects.
    Discovering an arbitrary .parquet key alone is not evidence it is committed.
    ETags are deliberately not used as hashes: multipart/encrypted objects can
    have ETags that are not a content MD5.
    """
    response = client.get_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')
    body = response['Body']
    try:
        require(0 < response['ContentLength'] <= 64 * 1024 * 1024, 'invalid manifest size')
        raw = body.read(64 * 1024 * 1024 + 1)
    finally:
        body.close()
    require(len(raw) == response['ContentLength'], 'truncated manifest: ' + key)
    sha = hashlib.sha256(raw).digest()
    require(response.get('ChecksumSHA256') == base64.b64encode(sha).decode(),
            'manifest S3 checksum mismatch: ' + key)
    try:
        return json.loads(raw), sha.hex()
    except (ValueError, UnicodeError):
        raise FetchError('invalid manifest JSON: ' + key) from None


@dataclass
class Hour:
    """Validated description of one committed book hour; no book rows in RAM.

    entry references only the book object. provenance retains coverage and
    collector/source identities for the eventual daily Parquet footer.
    """
    key: str
    manifest_sha256: str
    source: str
    day: str
    asset: str
    symbol: str
    start_ns: int
    end_ns: int
    entry: dict
    metadata: dict
    analysis_depth: int
    provenance: dict


def legacy_hour_provenance(manifest, start, end):
    """Normalize the original direct-hour format without inventing coverage.

    Before hourly assembly, finalized schema-v2 chunks were published directly
    under the same canonical hourly paths. Only a single canonical first part
    is accepted here; private minute segments and unmerged parts are not hours.
    Transport coverage may start before a reconstructible snapshot exists, so
    keep transport and usable-book coverage separate. Unknown coverage is not
    filled from the first/last row or upgraded to a claim of completeness.
    """
    require(type(manifest.get('part_number')) is int and manifest['part_number'] == 1,
            'legacy canonical hour must be a single first part')
    require(all(isinstance(manifest.get(k), str) and manifest[k]
                for k in ('chunk_id', 'connection_id'))
            and isinstance(manifest.get('collector'), dict), 'missing legacy collector identity')
    a, b = manifest.get('coverage_start_ns'), manifest.get('coverage_end_ns')
    require(type(a) is int and type(b) is int and start <= a <= b <= end,
            'invalid legacy coverage interval')
    book_start = manifest.get('book_reconstructible_start_ns')
    require(book_start is None or (type(book_start) is int and start <= book_start <= end),
            'invalid legacy book coverage start')
    gaps, gap_count = manifest.get('gaps'), manifest.get('gap_count')
    require(isinstance(gaps, list) and type(gap_count) is int and gap_count >= len(gaps),
            'invalid legacy gap diagnostics')
    omitted = manifest.get('gap_summary_omitted', gap_count - len(gaps))
    require(type(omitted) is int and omitted == gap_count - len(gaps),
            'invalid legacy omitted gap count')
    result = dict(legacy_manifest=manifest, gap_count=gap_count,
                  gap_summary_omitted=omitted, gaps=gaps, uncommitted_files=[],
                  publication_residues=[], seal_sha256=None,
                  connection_collectors=[{k: manifest[k] for k in ('connection_id', 'collector')}])
    for field, lower in (('coverage', a),
                         ('book_coverage', max(a, book_start) if book_start is not None else b)):
        intervals = [{'start_ns': lower, 'end_ns': b}] if lower < b else []
        missing = []
        cursor = start
        for interval in intervals:
            if cursor < interval['start_ns']:
                missing.append({'start_ns': cursor, 'end_ns': interval['start_ns']})
            cursor = interval['end_ns']
        if cursor < end:
            missing.append({'start_ns': cursor, 'end_ns': end})
        result[field + '_intervals'] = intervals
        result[field + '_gaps'] = missing
        result[field + '_complete'] = not missing and gap_count == 0
    return result


def load_hour(client, bucket, prefix, key):
    """Validate identity/path/units and select the BOOK object from a manifest.

    This recognizes both original direct-hour manifests and assembled hours.
    A manifest can describe books AND trades; selecting table='books' below is
    intentional. Never infer an asset by stripping a symbol suffix: metadata's
    base_asset maps BTC-USD/BTCUSDT to BTC and KXKSHIBPERP to SHIB correctly.
    """
    relative = key.removeprefix(prefix)
    parts = relative.split('/')
    require(key.startswith(prefix) and len(parts) == 4 and parts[0] == 'books', 'invalid manifest key')
    _, source, day, filename = parts
    require(SOURCE.fullmatch(source), 'invalid source')
    start_day = day_ns(day)
    m, sha = read_manifest(client, bucket, key)
    require(isinstance(m, dict) and m.get('status') == 'finalized'
            and m.get('record_type') in (None, 'hourly_assembly') and m.get('schema_version') == 2,
            'expected committed schema-v2 canonical hour: ' + key)
    symbol = m.get('symbol', '')
    require(isinstance(symbol, str) and NAME.fullmatch(symbol) and m.get('source') == source,
            'manifest identity mismatch')
    start, end = m.get('interval_start_ns'), m.get('interval_end_ns')
    require(type(start) is int and type(end) is int and start % HOUR_NS == 0
            and end - start == HOUR_NS and start_day <= start < end <= start_day + DAY_NS,
            'invalid hourly interval')
    hhmm = f'{(start - start_day) // HOUR_NS:02}00'
    require(filename == f'{symbol}-{hhmm}.manifest.json', 'noncanonical hourly filename')
    metadata = m.get('metadata')
    require(isinstance(metadata, dict), 'missing instrument metadata')
    asset = metadata.get('base_asset', '')
    require(isinstance(asset, str) and ASSET.fullmatch(asset), 'missing or unsafe base_asset')
    require(metadata.get('source') == source and metadata.get('symbol') == symbol,
            'instrument metadata identity mismatch')
    require(type(m.get('analysis_depth')) is int and m['analysis_depth'] > 0, 'invalid analysis depth')
    # Do not enqueue trade objects, even when their checksums/paths appear next
    # to the book reference. The user requested a books-only local mirror.
    entries = [f for f in m.get('files', []) if f.get('table') == 'books']
    require(len(entries) == 1, 'expected exactly one committed book file per hour')
    entry = entries[0]
    require(entry.get('path') == f'books/{source}/{day}/{symbol}-{hhmm}.parquet', 'book path mismatch')
    require(re.fullmatch('[0-9a-f]{64}', str(entry.get('sha256', ''))), 'invalid book checksum')
    require(all(type(entry.get(k)) is int and entry[k] >= 0 for k in ('rows', 'size_bytes')),
            'invalid book size or row count')
    # Keep descriptive provenance in the data footer, never a download ledger.
    fields = ('coverage_intervals', 'book_coverage_intervals', 'coverage_gaps', 'book_coverage_gaps',
              'coverage_complete', 'book_coverage_complete', 'gap_count', 'gap_summary_omitted',
              'gaps', 'uncommitted_files', 'publication_residues', 'seal_sha256')
    if m.get('record_type') is None:
        provenance = legacy_hour_provenance(m, start, end)
    else:
        provenance = {k: m.get(k) for k in fields}
        # Many private minute fragments share one connection/collector. Keep
        # each distinct identity once; keep hourly coverage diagnostics too.
        identities = {}
        for segment in m.get('segments', []):
            original = segment['manifest']
            identity = {k: original.get(k) for k in ('connection_id', 'collector')}
            identities[encoded(identity)] = identity
        provenance['connection_collectors'] = list(identities.values())
    provenance.update(manifest_key=key, manifest_sha256=sha, book_file=entry,
                      interval_start_ns=start, interval_end_ns=end,
                      record_type=m.get('record_type'))
    return Hour(key, sha, source, day, asset, symbol, start, end, entry, metadata,
                m['analysis_depth'], provenance)


def merged_intervals(hours, field, start, end):
    """Union recorded coverage, then return its complement within the UTC day.

    Missing published hours and intra-hour reconnect gaps both remain visible.
    Touching intervals merge; disconnected intervals do not. These calculations
    describe the archived data and never generate rows to fill the gaps.
    """
    intervals = []
    for hour in hours:
        values = hour.provenance.get(field)
        require(isinstance(values, list), 'missing coverage intervals')
        for item in values:
            a, b = item['start_ns'], item['end_ns']
            require(type(a) is int and type(b) is int and hour.start_ns <= a <= b <= hour.end_ns,
                    'invalid coverage interval')
            if a < b:
                intervals.append((a, b))
    union = []
    for a, b in sorted(intervals):
        if union and a <= union[-1]['end_ns']:
            union[-1]['end_ns'] = max(b, union[-1]['end_ns'])
        else:
            union.append({'start_ns': a, 'end_ns': b})
    gaps, cursor = [], start
    for item in union:
        if cursor < item['start_ns']:
            gaps.append({'start_ns': cursor, 'end_ns': item['start_ns']})
        cursor = item['end_ns']
    if cursor < end:
        gaps.append({'start_ns': cursor, 'end_ns': end})
    return union, gaps


def daily_metadata(hours):
    """Build descriptive daily provenance and reject incompatible hourly inputs.

    This metadata is embedded in the output, not read back as resume state.
    Twenty-four hour files can still have gaps; completeness needs both the
    interval union and the original per-hour completeness flags.
    """
    require(bool(hours), 'no committed hours')
    first = hours[0]
    require(len({h.start_ns for h in hours}) == len(hours), 'duplicate hourly input')
    for hour in hours:
        require((hour.source, hour.day, hour.asset, hour.symbol, hour.analysis_depth)
                == (first.source, first.day, first.asset, first.symbol, first.analysis_depth),
                'multiple instruments map to one daily asset or depth changed')
        require(all(hour.metadata.get(k) == first.metadata.get(k) for k in INTERPRETATION),
                'incompatible units or feed semantics across hours')
    start = day_ns(first.day)
    # Keep constant venue metadata once. Fields allowed to vary across hours
    # are not falsely advertised as constant; authoritative manifests remain
    # referenced in hours[]. Units and other interpretation fields were checked
    # above, so Binance USDT cannot be blended with USD in this daily output.
    common = {k: v for k, v in first.metadata.items() if all(h.metadata.get(k) == v for h in hours)}
    info = dict(schema_version=2, record_type='daily_books', source=first.source,
                symbol=first.symbol, base_asset=first.asset, interval_start_ns=start,
                interval_end_ns=start + DAY_NS, analysis_depth=first.analysis_depth,
                metadata=common, bucket=None, hours=[h.provenance for h in hours],
                missing_hours=[f'{h:02}00' for h in range(24)
                               if start + h * HOUR_NS not in {x.start_ns for x in hours}],
                row_order='chronological hourly inputs; original physical rows preserved; no deduplication',
                timestamp_semantics='Original UTC-nanosecond receipt timestamps retained. Checkpoints are carried state.',
                provenance_note='Descriptive source provenance only; never consulted to skip or resume downloads.')
    for field in ('coverage', 'book_coverage'):
        union, gaps = merged_intervals(hours, field + '_intervals', start, start + DAY_NS)
        info[field + '_intervals'], info[field + '_gaps'] = union, gaps
        info[field + '_complete'] = not gaps and all(h.provenance[field + '_complete'] is True for h in hours)
    return info


def download_hour(client, bucket, prefix, hour, path):
    """Stream one book object to disk and verify two independent commitments.

    The byte count/hash must match both the hourly manifest and S3's checksum.
    A truncated response or changed object fails before replacing a daily file.
    Only this one incoming hourly object is retained per worker at a time.
    """
    response = client.get_object(Bucket=bucket, Key=prefix + hour.entry['path'], ChecksumMode='ENABLED')
    body, sha, count = response['Body'], hashlib.sha256(), 0
    try:
        require(response['ContentLength'] == hour.entry['size_bytes'], 'remote book size mismatch')
        with path.open('wb') as output:
            for block in iter(lambda: body.read(1024 * 1024), b''):
                count += len(block)
                require(count <= hour.entry['size_bytes'], 'book object exceeds committed size')
                output.write(block)
                sha.update(block)
    finally:
        body.close()
    require(count == hour.entry['size_bytes'] and sha.hexdigest() == hour.entry['sha256'],
            'downloaded book checksum mismatch')
    require(response.get('ChecksumSHA256') == base64.b64encode(sha.digest()).decode(),
            'book S3 checksum mismatch')


def checked_batches(path, hour, batch_rows, expected_schema):
    """Yield bounded batches only after schema, identity and ordering checks.

    Each hour must agree with its manifest, and all hours in a day must share
    one schema. Receipt clocks must be present, ordered and inside the hour.
    Exchange clocks may be null (notably Binance partial-depth books), and
    checkpoints may refer to an older state_asof_timestamp. Preserve these
    values unchanged; receipt time alone decides which archive hour holds a row.
    """
    with pq.ParquetFile(path) as parquet:
        schema = parquet.schema_arrow.remove_metadata()
        require(expected_schema is None or schema == expected_schema, 'incompatible Parquet schemas')
        require(schema.field('received_timestamp').type == UTC_NS, 'receipt timestamps must be UTC nanoseconds')
        require(parquet.metadata.num_rows == hour.entry['rows'], 'hourly Parquet row count mismatch')
        meta = json.loads(parquet.schema_arrow.metadata[b'btc_book_recorder'])
        require(all(meta.get(k) == getattr(hour, attr) for k, attr in
                    (('source', 'source'), ('symbol', 'symbol'), ('interval_start_ns', 'start_ns'),
                     ('interval_end_ns', 'end_ns'), ('analysis_depth', 'analysis_depth'))),
                'hourly Parquet metadata mismatch')
        require(meta.get('schema_version') == 2
                and meta.get('record_type') == hour.provenance['record_type'],
                'hourly Parquet format mismatch')
        legacy = hour.provenance.get('legacy_manifest')
        if legacy is not None:
            # Direct-hour Parquet footers flatten instrument metadata. Their
            # `status` is the venue's instrument status, not a commit marker.
            require(all(k in meta and meta[k] == v for k, v in hour.metadata.items()),
                    'legacy Parquet instrument metadata mismatch')
            require(all(meta.get(k) == legacy[k] for k in
                        ('chunk_id', 'connection_id', 'part_number', 'collector')),
                    'legacy Parquet collector identity mismatch')
        else:
            require(meta.get('metadata') == hour.metadata,
                    'hourly Parquet instrument metadata mismatch')
        previous = None
        total = 0
        for batch in parquet.iter_batches(batch_size=batch_rows, use_threads=False):
            times = pc.cast(batch.column('received_timestamp'), pa.int64())
            # Canonical assembly sorts a missing source sequence as -1. The
            # temporary comparison key must not replace nulls in the output.
            sequences = pc.fill_null(batch.column('local_sequence'), -1)
            require(times.null_count == 0, 'null receipt timestamp')
            require(pc.min(times).as_py() >= hour.start_ns and pc.max(times).as_py() < hour.end_ns,
                    'row outside committed hour')
            for column, expected in (('source', hour.source), ('symbol', hour.symbol)):
                values = batch.column(column)
                require(values.null_count == 0 and pc.all(pc.equal(values, expected)).as_py(),
                        'row identity mismatch')
            if legacy is not None:
                connections = batch.column('connection_id')
                require(connections.null_count == 0 and
                        pc.all(pc.equal(connections, legacy['connection_id'])).as_py(),
                        'legacy row connection mismatch')
            # Validate both within-batch and cross-batch order. Equal keys are
            # permitted: this downloader never discards ties or duplicate rows.
            first_key, last_key = (times[0].as_py(), sequences[0].as_py()), (times[-1].as_py(), sequences[-1].as_py())
            require(previous is None or previous <= first_key, 'unordered hourly row boundary')
            if len(times) > 1:
                before, after = times.slice(0, len(times)-1), times.slice(1)
                backwards = pc.or_(pc.less(after, before), pc.and_(pc.equal(after, before),
                    pc.less(sequences.slice(1), sequences.slice(0, len(times)-1))))
                require(not pc.any(backwards).as_py(), 'unordered hourly rows')
            previous = last_key
            total += batch.num_rows
            yield batch.replace_schema_metadata(None)
        require(total == hour.entry['rows'], 'decoded hourly row count mismatch')


def build_day(client, bucket, prefix, output_root, hours, *, batch_rows=32768,
              min_free_bytes=1_000_000_000):
    """Rebuild a single source/asset/day and publish it with an atomic rename.

    Existing bytes are never input. The old final file stays intact until all
    selected hours download, validate and flush successfully. At most one hour
    file plus a bounded Arrow batch and the growing output are staged per
    worker. The disk estimate is conservative, not an exact Parquet-size bound;
    repeated free-space checks retain the reserve as compressed output grows.
    """
    hours = sorted(hours, key=lambda hour: hour.start_ns)
    info = daily_metadata(hours)
    info['bucket'] = bucket
    first = hours[0]
    destination = safe_path(books_directory(output_root, first.source), first.day,
                            f'{first.day}-{first.asset}.parquet')
    destination.parent.mkdir(parents=True, exist_ok=True)
    check_space(destination.parent, min_free_bytes,
                sum(h.entry['size_bytes'] for h in hours) + max(h.entry['size_bytes'] for h in hours))
    schema, writer, rows = None, None, 0
    # Staging is on the destination filesystem, so replacing the daily file is atomic.
    with tempfile.TemporaryDirectory(prefix=f'.{first.asset}-download-', dir=destination.parent) as staging:
        stage = Path(staging)
        output, incoming = stage/'day.parquet.partial', stage/'hour.parquet.partial'
        try:
            for hour in hours:
                check_space(destination.parent, min_free_bytes, hour.entry['size_bytes'])
                download_hour(client, bucket, prefix, hour, incoming)
                with pq.ParquetFile(incoming) as parquet:
                    current_schema = parquet.schema_arrow.remove_metadata()
                require(schema is None or current_schema == schema, 'incompatible Parquet schemas')
                if writer is None:
                    # Re-encode the same Arrow values for a daily container.
                    # Parquet 2.6 retains nanosecond timestamps; dictionary and
                    # delta encodings change storage only, not values or units.
                    schema = current_schema
                    output_schema = schema.with_metadata({b'btc_book_recorder': encoded(info)})
                    dictionaries = [c for c in ('source', 'symbol', 'connection_id', 'kind', 'source_ts_kind') if c in schema.names]
                    writer = pq.ParquetWriter(output, output_schema, version='2.6', compression='zstd',
                        compression_level=3, use_dictionary=dictionaries,
                        write_statistics=[c for c in ('received_timestamp', 'participant_timestamp', 'kind') if c in schema.names],
                        column_encoding={'received_timestamp': 'DELTA_BINARY_PACKED'}, write_page_checksum=True)
                for batch in checked_batches(incoming, hour, batch_rows, schema):
                    check_space(destination.parent, min_free_bytes)
                    writer.write_batch(batch, row_group_size=batch_rows)
                    rows += batch.num_rows
                incoming.unlink()
            # Closing writes the Parquet footer. Verify it, fsync the file,
            # rename on the same filesystem, then fsync its directory entry.
            # Any earlier failure exits through TemporaryDirectory cleanup.
            writer.close()
            writer = None
            require(rows == sum(h.entry['rows'] for h in hours), 'daily row count mismatch')
            with pq.ParquetFile(output) as parquet:
                require(parquet.metadata.num_rows == rows and parquet.schema_arrow.remove_metadata() == schema,
                        'daily output footer mismatch')
            with output.open('rb') as file:
                os.fsync(file.fileno())
            output.chmod(0o664)
            size = output.stat().st_size
            os.replace(output, destination)
            fsync_dir(destination.parent)
        finally:
            if writer is not None:
                writer.close()
    return dict(event='written', path=str(destination), source=first.source, asset=first.asset,
                day=first.day, hours=len(hours), missing_hours=info['missing_hours'],
                book_coverage_complete=info['book_coverage_complete'],
                book_coverage_gap_count=len(info['book_coverage_gaps']),
                book_uncovered_seconds=sum(g['end_ns'] - g['start_ns']
                                           for g in info['book_coverage_gaps']) / 1e9,
                source_gap_count=sum(h.provenance.get('gap_count') or 0 for h in hours),
                legacy_direct_hours=sum('legacy_manifest' in h.provenance for h in hours),
                rows=rows, bytes=size)


def parse_args(argv=None):
    """Parse filters/resources without reading credentials or contacting S3."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket', default='fin.dheera.net')
    parser.add_argument('--prefix', default='global_crypto/')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--credentials', type=Path, default=HERE/'api_keys/crypto-data-recorder-aws.json')
    parser.add_argument('--default-aws-credentials', action='store_true', help='Use the normal AWS credential provider chain')
    parser.add_argument('--output', type=Path, default=HERE, help='Data root containing global_crypto/ and perpetuals/ (default: script directory)')
    parser.add_argument('--sources', nargs='+',
                        help='Default: autodiscover all S3 book sources, including coinbase, kalshi_perp and binance_spot; use --sources binance for Binance books only (binance_spot is also accepted)')
    parser.add_argument('--assets', nargs='+', help='Normalized base assets, e.g. BTC ETH SHIB')
    parser.add_argument('--date', help='Rebuild this UTC day, even if older than local data')
    parser.add_argument('--start-date', help='Inclusive UTC start date; rebuild every selected day')
    parser.add_argument('--end-date', help='Inclusive UTC end date')
    parser.add_argument('--rebuild-all', action='store_true', help='Repair all published days regardless of local files')
    parser.add_argument('--workers', type=int, default=2, help='Concurrent daily files (default: 2)')
    parser.add_argument('--batch-rows', type=int, default=32768, help='Bounded Arrow read/write batch size')
    parser.add_argument('--min-free-bytes', type=int, default=1_000_000_000)
    parser.add_argument('--dry-run', action='store_true', help='List planned outputs without downloading book Parquets')
    args = parser.parse_args(argv)
    require(args.workers > 0 and args.batch_rows > 0 and args.min_free_bytes >= 0, 'invalid resource bounds')
    require(not (args.date and (args.start_date or args.end_date or args.rebuild_all)), 'choose date, range, or rebuild-all')
    require(not (args.rebuild_all and (args.start_date or args.end_date)), 'choose range or rebuild-all')
    for day in (args.date, args.start_date, args.end_date):
        if day:
            parse_day(day)
    require(not (args.start_date and args.end_date and args.start_date > args.end_date), 'reversed date range')
    require(all(SOURCE.fullmatch(s) for s in (args.sources or [])), 'invalid source filter')
    if args.sources:
        # Accept the local venue name while continuing to read the original S3
        # prefix. Deduplicate aliases so specifying both cannot rebuild a day
        # twice. Only the CLI filter changes; archived source values stay exact.
        args.sources = list(dict.fromkeys(
            'binance_spot' if s == 'binance' else s for s in args.sources))
    if args.assets:
        args.assets = set(a.upper() for a in args.assets)
        require(all(ASSET.fullmatch(a) for a in args.assets), 'invalid asset filter')
    args.prefix = args.prefix.strip('/') + '/'
    require(args.prefix != '/' and '..' not in args.prefix.split('/') and '\\' not in args.prefix, 'invalid S3 prefix')
    return args


def run(client, args):
    """Discover committed inputs, select daily rebuilds, then execute the plan.

    Discovery is deliberately source-agnostic: adding binance_spot to S3 books/
    automatically includes it in unrestricted runs and the existing daily
    updater. All listing prefixes stay under books/; trades are out of scope.
    Small pools fetch manifests; a separate bounded pool builds daily files.
    """
    sources = args.sources or sorted(s for s in list_children(client, args.bucket, args.prefix+'books/') if SOURCE.fullmatch(s))
    written = failed = skipped = 0
    explicit = bool(args.date or args.start_date or args.end_date or args.rebuild_all)
    for source in sources:
        base = args.prefix + 'books/' + source + '/'
        # Freeze this map BEFORE writing any files. If today's run first builds
        # tomorrow's/newer day, yesterday must still be refreshed later in this
        # same newest-first traversal. Updating latest in the loop would skip it.
        latest = latest_local_days(args.output, source, args.assets)
        symbol_assets = {}  # Ephemeral lookup only; never persisted between runs.
        days = []
        for day in list_children(client, args.bucket, base):
            try:
                parse_day(day)
            except FetchError:
                continue
            if args.date and day != args.date:
                continue
            if args.start_date and day < args.start_date or args.end_date and day > args.end_date:
                continue
            days.append(day)
        # Newest first learns the venue's symbol/asset map early. Older dates
        # are still listed, so a newly requested/missing asset is backfilled.
        for day in sorted(days, reverse=True):
            keys = []
            for page in client.get_paginator('list_objects_v2').paginate(Bucket=args.bucket, Prefix=base+day+'/'):
                keys.extend(item['Key'] for item in page.get('Contents', []) if item['Key'].endswith('.manifest.json'))
            symbol_keys, groups = {}, {}
            for key in sorted(keys):
                match = re.fullmatch(r'(.+)-(\d{2}00)\.manifest\.json', key.rsplit('/', 1)[-1])
                require(match is not None, 'noncanonical hourly manifest: ' + key)
                symbol_keys.setdefault(match[1], []).append(key)
            preloaded = {}
            # One manifest reveals an unfamiliar native symbol's base asset.
            # Cache it only for this invocation; never infer BTC from BTCUSDT
            # string slicing or store a persistent symbol/progress ledger.
            unknown = [items[0] for symbol, items in symbol_keys.items() if symbol not in symbol_assets]
            with ThreadPoolExecutor(max_workers=min(4, args.workers)) as pool:
                for hour in pool.map(lambda key: load_hour(client, args.bucket, args.prefix, key), unknown):
                    symbol_assets[hour.symbol] = hour.asset
                    preloaded[hour.key] = hour
                wanted = []
                for symbol, items in symbol_keys.items():
                    asset = symbol_assets[symbol]
                    if args.assets and asset not in args.assets:
                        continue
                    destination = safe_path(books_directory(args.output, source), day, f'{day}-{asset}.parquet')
                    # Only OLD, EXISTING daily files are skipped by default.
                    # The latest date always rebuilds, and a missing older file
                    # backfills even if this asset has newer local data.
                    if not explicit and day < latest.get(asset, '') and destination.is_file():
                        skipped += 1
                        continue
                    wanted.extend(items)
                for hour in pool.map(lambda key: preloaded[key] if key in preloaded else
                                     load_hour(client, args.bucket, args.prefix, key), wanted):
                    require(hour.asset == symbol_assets[hour.symbol], 'base asset changed within native symbol')
                    groups.setdefault(hour.asset, []).append(hour)
            selected = []
            for asset, hours in sorted(groups.items()):
                # Validate collisions/units even for --dry-run.
                daily_metadata(sorted(hours, key=lambda h: h.start_ns))
                selected.append(hours)
            if args.dry_run:
                # Manifests and daily compatibility are checked, but no book
                # Parquet is fetched and no local output/lock is created.
                for hours in selected:
                    h = hours[0]
                    print(json.dumps(dict(event='planned', source=source, day=day, asset=h.asset,
                        path=str(safe_path(books_directory(args.output, source), day, f'{day}-{h.asset}.parquet')),
                        hours=len(hours), rows=sum(x.entry['rows'] for x in hours),
                        download_bytes=sum(x.entry['size_bytes'] for x in hours))), flush=True)
                continue
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                # Each job writes a different daily path. A failed job preserves
                # that path's previous file; unrelated jobs can still complete.
                jobs = {pool.submit(build_day, client, args.bucket, args.prefix, args.output, hours,
                                    batch_rows=args.batch_rows, min_free_bytes=args.min_free_bytes): hours[0]
                        for hours in selected}
                for job in as_completed(jobs):
                    try:
                        result = job.result()
                    except Exception as exc:
                        failed += 1
                        h = jobs[job]
                        print(json.dumps(dict(event='failed', source=source, day=day, asset=h.asset,
                                              error=str(exc) if isinstance(exc, FetchError) else type(exc).__name__)), flush=True)
                    else:
                        written += 1
                        print(json.dumps(result), flush=True)
    print(json.dumps(dict(event='summary', written=written, failed=failed, older_existing_days_skipped=skipped,
                          dry_run=args.dry_run)), flush=True)
    return int(failed > 0)


def main(argv=None):
    """Run with one output-root lock and return a scheduler-friendly exit code.

    Controlled FetchError messages explain safe validation failures. Unexpected
    exceptions are logged by class only, avoiding SDK response/credential leaks.
    Nonzero exit means at least one daily file failed or discovery aborted.
    """
    try:
        args = parse_args(argv)
        client = make_client(None if args.default_aws_credentials else args.credentials, args.region)
        if args.dry_run:
            return run(client, args)
        with output_lock(args.output):
            return run(client, args)
    except Exception as exc:
        # Never print arbitrary remote response bodies or credential values.
        message = str(exc) if isinstance(exc, FetchError) else type(exc).__name__
        print(json.dumps(dict(event='error', error=message)), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
