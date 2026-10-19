"""Isolated tests for /data/fin/download_crypto_books.py; fake S3, temporary outputs."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


SCRIPT = Path(__file__).resolve().parents[1] / 'download_crypto_books.py'
UTC_NS = pa.timestamp('ns', tz='UTC')
LEVELS = pa.list_(pa.struct([('price', pa.float64()), ('size', pa.float64())]))
SCHEMA = pa.schema([
    ('received_timestamp', UTC_NS), ('monotonic_ns', pa.int64()),
    ('source', pa.string()), ('symbol', pa.string()),
    ('connection_id', pa.string()), ('local_sequence', pa.int64()),
    ('participant_timestamp', UTC_NS), ('source_ts_resolution_ns', pa.int64()),
    ('source_ts_kind', pa.string()), ('source_send_timestamp', UTC_NS),
    ('native_seq', pa.int64()), ('native_seq_first', pa.int64()),
    ('native_seq_prev', pa.int64()), ('native_cross_seq', pa.int64()),
    ('native_sid', pa.string()), ('kind', pa.string()),
    ('bids', LEVELS), ('asks', LEVELS), ('state_asof_timestamp', UTC_NS),
    ('crossed', pa.bool_()),
])
DAY = '2026-10-18'
DAY_NS = int(datetime(2026, 10, 18, tzinfo=timezone.utc).timestamp()) * 1_000_000_000
HOUR_NS = 3_600_000_000_000


@pytest.fixture(scope='module')
def app():
    spec = importlib.util.spec_from_file_location('daily_books_under_test', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sha(value):
    return hashlib.sha256(value).hexdigest()


class FakeS3:
    """Enough of boto3's read interface to exercise exact production parsing."""
    def __init__(self):
        self.objects = {}
        self.calls = []

    def add(self, key, value):
        self.objects[key] = value

    def get_object(self, *, Bucket, Key, **kwargs):
        self.calls.append(('get', Key))
        value = self.objects[Key]
        return {'Body': io.BytesIO(value), 'ContentLength': len(value),
                'ChecksumSHA256': base64.b64encode(hashlib.sha256(value).digest()).decode(),
                'ETag': '"' + hashlib.md5(value).hexdigest() + '"'}

    def head_object(self, *, Bucket, Key, **kwargs):
        self.calls.append(('head', Key))
        value = self.objects[Key]
        return {'ContentLength': len(value),
                'ChecksumSHA256': base64.b64encode(hashlib.sha256(value).digest()).decode()}

    def download_file(self, Bucket, Key, Filename, **kwargs):
        self.calls.append(('download', Key))
        Path(Filename).write_bytes(self.objects[Key])

    def download_fileobj(self, Bucket, Key, Fileobj, **kwargs):
        self.calls.append(('download', Key))
        Fileobj.write(self.objects[Key])

    def list_objects_v2(self, *, Bucket, Prefix='', **kwargs):
        self.calls.append(('list', Prefix))
        entries = [{'Key': key, 'Size': len(value),
                    'ETag': '"' + hashlib.md5(value).hexdigest() + '"'}
                   for key, value in sorted(self.objects.items()) if key.startswith(Prefix)]
        if kwargs.get('Delimiter'):
            delimiter = kwargs['Delimiter']
            common = sorted({Prefix + row['Key'][len(Prefix):].split(delimiter)[0] + delimiter
                             for row in entries if delimiter in row['Key'][len(Prefix):]})
            entries = [row for row in entries if delimiter not in row['Key'][len(Prefix):]]
            return {'Contents': entries, 'CommonPrefixes': [{'Prefix': x} for x in common],
                    'IsTruncated': False}
        return {'Contents': entries, 'IsTruncated': False}

    def get_paginator(self, name):
        assert name == 'list_objects_v2'
        client = self
        class Paginator:
            def paginate(self, **kwargs):
                yield client.list_objects_v2(**kwargs)
        return Paginator()


def make_rows(hour, *, source='coinbase', symbol='BTC-USD', day=DAY):
    day_start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    start = day_start + hour * HOUR_NS
    return [{
        'received_timestamp': start + 123_456_789,
        'monotonic_ns': 99_123_456_789 + hour,
        'source': source, 'symbol': symbol, 'connection_id': 'receiver-a',
        'local_sequence': 11 + hour * 10, 'participant_timestamp': start - 13,
        'source_ts_resolution_ns': 1000, 'source_ts_kind': 'exchange_event',
        'source_send_timestamp': None, 'native_seq': 6001,
        'native_seq_first': None, 'native_seq_prev': None,
        'native_cross_seq': None, 'native_sid': 'sid-7',
        'kind': 'update',
        'bids': [{'price': -0.0, 'size': 0.1}, {'price': 1.0000000000000002, 'size': 9.25}],
        'asks': [{'price': 90_000.125, 'size': 0.000000123456789}],
        'state_asof_timestamp': start + 123_456_789, 'crossed': False,
    }, {
        'received_timestamp': start + 123_456_789,
        'monotonic_ns': 99_123_456_790 + hour,
        'source': source, 'symbol': symbol, 'connection_id': 'receiver-a',
        'local_sequence': 12 + hour * 10, 'participant_timestamp': None,
        'source_ts_resolution_ns': None, 'source_ts_kind': None,
        'source_send_timestamp': start + 3, 'native_seq': None,
        'native_seq_first': None, 'native_seq_prev': None,
        'native_cross_seq': None, 'native_sid': None,
        'kind': 'checkpoint', 'bids': [], 'asks': [],
        'state_asof_timestamp': start - 19, 'crossed': False,
    }]


def add_hour(client, hour, *, source='coinbase', symbol='BTC-USD', asset='BTC', day=DAY,
             rows=None, schema=SCHEMA, metadata_updates=None):
    day_start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000_000
    start = day_start + hour * HOUR_NS
    rows = make_rows(hour, source=source, symbol=symbol, day=day) if rows is None else rows
    metadata = {'source': source, 'symbol': symbol, 'base_asset': asset,
                'native_asset': asset, 'quote_currency': 'USD',
                'price_unit': 'USD per ' + asset, 'size_unit': asset,
                'price_scale': '1', 'size_scale': '1',
                'native_price_unit': 'USD per ' + asset, 'native_size_unit': asset,
                'source_depth': 0, 'source_depth_kind': 'full_aggregated_price_levels',
                'channel': 'level2_batch+matches+heartbeat', 'cadence_ms': 50}
    metadata.update(metadata_updates or {})
    table = pa.Table.from_pylist(rows, schema=schema).replace_schema_metadata({
        b'btc_book_recorder': json.dumps({
            'schema_version': 2, 'record_type': 'hourly_assembly',
            'source': source, 'symbol': symbol, 'metadata': metadata,
            'interval_start_ns': start, 'interval_end_ns': start + HOUR_NS,
            'analysis_depth': 20,
        }, sort_keys=True).encode(),
    })
    stream = io.BytesIO()
    pq.write_table(table, stream, compression='zstd', row_group_size=1)
    blob = stream.getvalue()
    relative = f'books/{source}/{day}/{symbol}-{hour:02d}00.parquet'
    manifest_relative = relative.removesuffix('.parquet') + '.manifest.json'
    manifest = {
        'schema_version': 2, 'status': 'finalized', 'record_type': 'hourly_assembly',
        'source': source, 'symbol': symbol, 'analysis_depth': 20,
        'metadata': metadata, 'interval_start_ns': start,
        'interval_end_ns': start + HOUR_NS, 'finalized_ns': start + HOUR_NS + 1,
        'first_recv_ns': min(row['received_timestamp'] for row in rows),
        'last_recv_ns': max(row['received_timestamp'] for row in rows),
        'coverage_intervals': [{'start_ns': start, 'end_ns': start + HOUR_NS}],
        'book_coverage_intervals': [{'start_ns': start, 'end_ns': start + HOUR_NS}],
        'coverage_gaps': [], 'book_coverage_gaps': [], 'coverage_complete': True,
        'book_coverage_complete': True, 'gap_count': 0, 'gaps': [],
        'uncommitted_files': [], 'segments': [],
        'files': [{'path': relative, 'table': 'books', 'rows': table.num_rows,
                   'size_bytes': len(blob), 'sha256': sha(blob)}],
    }
    client.add('global_crypto/' + relative, blob)
    client.add('global_crypto/' + manifest_relative,
               (json.dumps(manifest, sort_keys=True) + '\n').encode())
    return manifest, table, 'global_crypto/' + manifest_relative


def exact_table(actual, expected):
    assert actual.schema.remove_metadata() == expected.schema.remove_metadata()
    assert actual.num_rows == expected.num_rows
    for field in expected.schema:
        a, b = actual.column(field.name), expected.column(field.name)
        if pa.types.is_timestamp(field.type):
            assert a.cast(pa.int64()).to_pylist() == b.cast(pa.int64()).to_pylist()
        elif field.name in ('bids', 'asks'):
            for left, right in zip(a.to_pylist(), b.to_pylist()):
                assert len(left) == len(right)
                for lvalue, rvalue in zip(left, right):
                    for key in ('price', 'size'):
                        assert struct.pack('>d', lvalue[key]) == struct.pack('>d', rvalue[key])
        else:
            assert a.equals(b), field.name


def loaded(app, client, *keys):
    return [app.load_hour(client, 'test-bucket', 'global_crypto/', key) for key in keys]


def build(app, client, output, hours, **kwargs):
    return app.build_day(client, 'test-bucket', 'global_crypto/', output, hours,
                         batch_rows=1, min_free_bytes=0, **kwargs)


def output_path(root, asset='BTC', source='coinbase', day=DAY):
    return root / 'books' / source / day / f'{day}-{asset}.parquet'


def test_partial_day_refresh_exact_rows_and_no_sidecars(app, tmp_path):
    client = FakeS3()
    _, first, key0 = add_hour(client, 0)
    build(app, client, tmp_path, loaded(app, client, key0))
    destination = output_path(tmp_path)
    exact_table(pq.read_table(destination), first)
    _, second, key1 = add_hour(client, 1)
    # Reverse input intentionally: build_day must concatenate ordered hours.
    result = build(app, client, tmp_path, loaded(app, client, key1, key0))
    exact_table(pq.read_table(destination), pa.concat_tables([first, second]))
    assert result['rows'] == 4 and result['hours'] == 2
    info = json.loads(pq.ParquetFile(destination).schema_arrow.metadata[b'btc_book_recorder'])
    assert info['missing_hours'] == [f'{h:02}00' for h in range(2, 24)]
    assert info['book_coverage_complete'] is False
    assert len(info['hours']) == 2
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize('existing', ['corrupt', 'duplicate', 'extra_hour'])
def test_existing_contents_are_not_resume_state(app, tmp_path, existing):
    client = FakeS3()
    _, expected, key = add_hour(client, 0)
    destination = output_path(tmp_path)
    destination.parent.mkdir(parents=True)
    if existing == 'corrupt':
        destination.write_bytes(b'broken parquet, repair me')
    elif existing == 'duplicate':
        pq.write_table(pa.concat_tables([expected, expected]), destination)
    else:
        _, extra, _ = add_hour(client, 1)
        pq.write_table(pa.concat_tables([expected, extra]), destination)
    build(app, client, tmp_path, loaded(app, client, key))
    exact_table(pq.read_table(destination), expected)


def test_repeat_build_refetches_all_hours(app, tmp_path):
    client = FakeS3()
    _, expected, key = add_hour(client, 0)
    hours = loaded(app, client, key)
    for _ in range(2):
        build(app, client, tmp_path, hours)
    data_key = key.replace('.manifest.json', '.parquet')
    assert client.calls.count(('get', data_key)) == 2
    exact_table(pq.read_table(output_path(tmp_path)), expected)


def test_failed_download_keeps_old_daily_and_removes_tempfiles(app, tmp_path):
    client = FakeS3()
    _, _, key = add_hour(client, 0)
    hours = loaded(app, client, key)
    build(app, client, tmp_path, hours)
    destination = output_path(tmp_path)
    previous = destination.read_bytes()
    data_key = key.replace('.manifest.json', '.parquet')
    original = client.objects[data_key]
    client.objects[data_key] = original[:-1] + bytes([original[-1] ^ 1])
    with pytest.raises(app.FetchError, match='checksum'):
        build(app, client, tmp_path, hours)
    assert destination.read_bytes() == previous
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize('problem', ['schema', 'units', 'collision', 'duplicate_hour',
                                     'identity', 'timestamp', 'sort'])
def test_invalid_inputs_fail_without_replacing_existing(app, tmp_path, problem):
    client = FakeS3()
    _, _, key0 = add_hour(client, 0)
    build(app, client, tmp_path, loaded(app, client, key0))
    destination = output_path(tmp_path)
    old = destination.read_bytes()
    kwargs = {}
    if problem == 'schema':
        kwargs['schema'] = SCHEMA.append(pa.field('unexpected', pa.int64()))
    elif problem == 'units':
        kwargs['metadata_updates'] = {'price_scale': '1000'}
    elif problem == 'collision':
        kwargs['symbol'] = 'BTC-USDC'
    elif problem in ('identity', 'timestamp', 'sort'):
        rows = make_rows(1)
        if problem == 'identity':
            rows[1]['source'] = 'wrong'
        elif problem == 'timestamp':
            rows[1]['received_timestamp'] = DAY_NS + 2 * HOUR_NS
        else:
            rows.reverse()
        kwargs['rows'] = rows
    _, _, key1 = add_hour(client, 1, **kwargs)
    hours = loaded(app, client, key0, key1)
    if problem == 'duplicate_hour':
        hours.append(hours[0])
    with pytest.raises(app.FetchError):
        build(app, client, tmp_path, hours)
    assert destination.read_bytes() == old
    assert list(destination.parent.iterdir()) == [destination]


def test_kshib_uses_base_asset_without_rescaling(app, tmp_path):
    client = FakeS3()
    _, expected, key = add_hour(client, 0, source='kalshi_perp', symbol='KXKSHIBPERP',
                               asset='SHIB', metadata_updates={'native_asset': 'KSHIB',
                               'price_scale': '0.001', 'size_scale': '1000'})
    build(app, client, tmp_path, loaded(app, client, key))
    destination = output_path(tmp_path, 'SHIB', 'kalshi_perp')
    exact_table(pq.read_table(destination), expected)


def test_latest_day_uses_valid_daily_names_per_asset(app, tmp_path):
    files = [('2026-10-17', '2026-10-17-BTC.parquet'),
             ('2026-10-18', '2026-10-18-BTC.parquet'),
             ('2026-10-16', '2026-10-16-ETH.parquet'),
             ('2026-10-18', 'BTC-USD-1800.parquet'),
             ('2026-10-18', '2026-10-19-ETH.parquet'),
             ('2026-10-18', '2026-10-18-ETH.parquet.partial')]
    for day, name in files:
        p = tmp_path / 'books' / 'coinbase' / day / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
    assert app.latest_local_days(tmp_path, 'coinbase') == {'BTC': '2026-10-18', 'ETH': '2026-10-16'}
    assert app.latest_local_days(tmp_path, 'coinbase', {'ETH'}) == {'ETH': '2026-10-16'}


def test_default_backfills_asset_missing_locally(app, tmp_path):
    client = FakeS3()
    add_hour(client, 0, symbol='BTC-USD', asset='BTC', day='2026-10-17')
    add_hour(client, 0, symbol='ETH-USD', asset='ETH', day='2026-10-17')
    add_hour(client, 0, symbol='BTC-USD', asset='BTC')
    add_hour(client, 0, symbol='ETH-USD', asset='ETH')
    destination = output_path(tmp_path)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b'corrupt latest file must be repaired')
    old_btc = output_path(tmp_path, 'BTC', day='2026-10-17')
    old_btc.parent.mkdir(parents=True)
    old_btc.write_bytes(b'older existing file is assumed complete')
    args = app.parse_args(['--output', str(tmp_path), '--workers', '1', '--min-free-bytes', '0'])
    assert app.run(client, args) == 0
    assert output_path(tmp_path, 'ETH', day='2026-10-17').exists()
    assert output_path(tmp_path, 'ETH').exists()
    assert old_btc.read_bytes() == b'older existing file is assumed complete'
    assert pq.ParquetFile(destination).metadata.num_rows == 2


def test_null_sequence_preserved_with_canonical_minus_one_order(app, tmp_path):
    client = FakeS3()
    rows = make_rows(0)
    rows[0]['local_sequence'] = None
    _, expected, key = add_hour(client, 0, rows=rows)
    build(app, client, tmp_path, loaded(app, client, key))
    exact_table(pq.read_table(output_path(tmp_path)), expected)


def test_explicit_older_date_rebuilds_assumed_complete_data(app, tmp_path):
    client = FakeS3()
    _, expected, _ = add_hour(client, 0, day='2026-10-17')
    add_hour(client, 0)
    previous = output_path(tmp_path, day='2026-10-17')
    current = output_path(tmp_path)
    previous.parent.mkdir(parents=True)
    current.parent.mkdir(parents=True)
    previous.write_bytes(b'older corrupt data must heal on explicit date')
    current.write_bytes(b'newer file should not change')
    args = app.parse_args(['--output', str(tmp_path), '--date', '2026-10-17',
                          '--workers', '1', '--min-free-bytes', '0'])
    assert app.run(client, args) == 0
    exact_table(pq.read_table(previous), expected)
    assert current.read_bytes() == b'newer file should not change'


def test_second_hour_connection_error_preserves_old_daily(app, tmp_path):
    client = FakeS3()
    _, _, key0 = add_hour(client, 0)
    _, _, key1 = add_hour(client, 1)
    build(app, client, tmp_path, loaded(app, client, key0))
    destination = output_path(tmp_path)
    previous = destination.read_bytes()
    get = client.get_object
    def fail_second(**kwargs):
        if kwargs['Key'] == key1.replace('.manifest.json', '.parquet'):
            raise ConnectionError('simulated network drop')
        return get(**kwargs)
    client.get_object = fail_second
    with pytest.raises(ConnectionError):
        build(app, client, tmp_path, loaded(app, client, key0, key1))
    assert destination.read_bytes() == previous
    assert list(destination.parent.iterdir()) == [destination]


def save_manifest(client, key, manifest):
    client.add(key, (json.dumps(manifest, sort_keys=True) + '\n').encode())


def rewrite_hour_footer(client, key, footer):
    """Re-sign a fake object after a footer edit, so integrity checks still pass."""
    manifest = json.loads(client.objects[key])
    entry = next(value for value in manifest['files'] if value['table'] == 'books')
    data_key = 'global_crypto/' + entry['path']
    table = pq.read_table(io.BytesIO(client.objects[data_key])).replace_schema_metadata({
        b'btc_book_recorder': json.dumps(footer, sort_keys=True).encode(),
    })
    stream = io.BytesIO()
    pq.write_table(table, stream, compression='zstd', row_group_size=1)
    blob = stream.getvalue()
    entry.update(size_bytes=len(blob), sha256=sha(blob))
    client.add(data_key, blob)
    save_manifest(client, key, manifest)
    return manifest, table


def add_legacy_hour(client, hour, *, partial=False, **kwargs):
    """Original direct-hour schema v2: flattened footer, no record_type."""
    manifest, _, key = add_hour(client, hour, **kwargs)
    start, end = manifest['interval_start_ns'], manifest['interval_end_ns']
    for field in ('record_type', 'coverage_intervals', 'book_coverage_intervals',
                  'coverage_gaps', 'book_coverage_gaps', 'coverage_complete',
                  'book_coverage_complete', 'uncommitted_files', 'segments'):
        manifest.pop(field)
    manifest.update(chunk_id='a' * 32, connection_id='receiver-a', part_number=1,
                    collector={'hostname': 'original-recorder', 'boot_id': 'boot-a',
                               'code_sha256': 'b' * 64, 'package_version': '0.1.0',
                               'started_ns': start - 1000},
                    connection_started_ns=start + 100_000_000 if partial else start - HOUR_NS,
                    coverage_start_ns=start + 100_000_000 if partial else start,
                    coverage_end_ns=end,
                    book_reconstructible_start_ns=manifest['first_recv_ns'] if partial else start,
                    partial_start=partial, partial_end=False,
                    gap_summary_omitted=0, finish_reason='interval_boundary')
    save_manifest(client, key, manifest)
    footer = dict(manifest['metadata'])
    footer.update({field: manifest[field] for field in
                   ('schema_version', 'source', 'symbol', 'analysis_depth',
                    'interval_start_ns', 'interval_end_ns', 'chunk_id',
                    'connection_id', 'part_number', 'collector')})
    footer['time_basis'] = 'local userspace receipt; half-open UTC interval'
    manifest, table = rewrite_hour_footer(client, key, footer)
    return manifest, table, key


def test_legacy_initial_partial_hour_retains_provenance_and_exact_rows(app, tmp_path):
    client = FakeS3()
    manifest, expected, key = add_legacy_hour(client, 7, partial=True)
    hour = loaded(app, client, key)[0]
    assert hour.provenance['legacy_manifest'] == manifest
    assert hour.provenance['connection_collectors'] == [{
        'connection_id': manifest['connection_id'], 'collector': manifest['collector']}]
    assert hour.provenance['coverage_intervals'] == [{
        'start_ns': manifest['coverage_start_ns'], 'end_ns': manifest['coverage_end_ns']}]
    assert hour.provenance['book_coverage_intervals'] == [{
        'start_ns': manifest['book_reconstructible_start_ns'],
        'end_ns': manifest['coverage_end_ns']}]
    assert hour.provenance['coverage_gaps'] == [{
        'start_ns': hour.start_ns, 'end_ns': manifest['coverage_start_ns']}]
    assert hour.provenance['book_coverage_gaps'] == [{
        'start_ns': hour.start_ns, 'end_ns': manifest['book_reconstructible_start_ns']}]
    assert hour.provenance['coverage_complete'] is False
    assert hour.provenance['book_coverage_complete'] is False
    result = build(app, client, tmp_path, [hour])
    exact_table(pq.read_table(output_path(tmp_path)), expected)
    assert result['legacy_direct_hours'] == 1
    assert result['book_coverage_complete'] is False
    saved = json.loads(pq.ParquetFile(output_path(tmp_path)).schema_arrow.metadata[b'btc_book_recorder'])
    assert saved['hours'][0]['legacy_manifest'] == manifest


def test_legacy_full_hour_with_carried_book_is_complete(app, tmp_path):
    client = FakeS3()
    _, expected, key = add_legacy_hour(client, 1)
    hour = loaded(app, client, key)[0]
    assert hour.provenance['coverage_intervals'] == [{'start_ns': hour.start_ns, 'end_ns': hour.end_ns}]
    assert hour.provenance['book_coverage_intervals'] == hour.provenance['coverage_intervals']
    assert hour.provenance['coverage_gaps'] == hour.provenance['book_coverage_gaps'] == []
    assert hour.provenance['coverage_complete'] is True
    assert hour.provenance['book_coverage_complete'] is True
    build(app, client, tmp_path, [hour])
    exact_table(pq.read_table(output_path(tmp_path)), expected)


def test_mixed_legacy_and_assembled_day_preserves_exact_values(app, tmp_path):
    client = FakeS3()
    _, legacy, key0 = add_legacy_hour(client, 0, partial=True)
    _, assembled, key1 = add_hour(client, 1)
    result = build(app, client, tmp_path, loaded(app, client, key1, key0))
    exact_table(pq.read_table(output_path(tmp_path)), pa.concat_tables([legacy, assembled]))
    assert result['rows'] == 4
    assert result['legacy_direct_hours'] == 1
    assert result['hours'] == 2


@pytest.mark.parametrize('changes', [
    {'record_type': 'unknown'}, {'record_type': 'daily_books'},
    {'status': 'open'}, {'schema_version': 1}, {'part_number': 2},
    {'chunk_id': ''}, {'connection_id': ''}, {'collector': None},
    {'coverage_start_ns': None}, {'coverage_end_ns': None},
    {'coverage_start_ns': DAY_NS - 1}, {'coverage_end_ns': DAY_NS + HOUR_NS + 1},
    {'coverage_start_ns': DAY_NS + HOUR_NS, 'coverage_end_ns': DAY_NS},
    {'book_reconstructible_start_ns': DAY_NS - 1},
    {'book_reconstructible_start_ns': DAY_NS + HOUR_NS + 1},
    {'coverage_start_ns': float(DAY_NS)}, {'gap_count': -1},
    {'interval_start_ns': DAY_NS + 1, 'interval_end_ns': DAY_NS + HOUR_NS + 1},
])
def test_invalid_legacy_manifest_rejected(app, changes):
    client = FakeS3()
    manifest, _, key = add_legacy_hour(client, 0)
    manifest.update(changes)
    save_manifest(client, key, manifest)
    with pytest.raises(app.FetchError):
        loaded(app, client, key)


def test_legacy_part_filename_is_not_canonical(app):
    client = FakeS3()
    manifest, _, key = add_legacy_hour(client, 0)
    alternate = key.replace('-0000.manifest.json', '-0000-part02.manifest.json')
    save_manifest(client, alternate, manifest)
    with pytest.raises(app.FetchError):
        loaded(app, client, alternate)


@pytest.mark.parametrize('absent', [True, False])
def test_legacy_without_reconstructible_start_has_unknown_book_coverage(app, absent):
    client = FakeS3()
    manifest, _, key = add_legacy_hour(client, 0)
    if absent:
        manifest.pop('book_reconstructible_start_ns')
    else:
        manifest['book_reconstructible_start_ns'] = None
    save_manifest(client, key, manifest)
    hour = loaded(app, client, key)[0]
    assert hour.provenance['coverage_complete'] is True
    assert hour.provenance['book_coverage_complete'] is False
    assert hour.provenance['book_coverage_intervals'] == []
    assert hour.provenance['book_coverage_gaps'] == [{
        'start_ns': hour.start_ns, 'end_ns': hour.end_ns}]


@pytest.mark.parametrize('field,bad_value', [
    ('chunk_id', 'c' * 32), ('connection_id', 'other-receiver'),
    ('part_number', 2), ('collector', {'hostname': 'other-recorder'}),
    ('schema_version', 1), ('record_type', 'hourly_assembly'),
    ('size_scale', '1000'),
])
def test_tampered_legacy_footer_preserves_existing_daily(app, tmp_path, field, bad_value):
    client = FakeS3()
    _, _, key = add_legacy_hour(client, 0)
    build(app, client, tmp_path, loaded(app, client, key))
    destination = output_path(tmp_path)
    previous = destination.read_bytes()
    data_key = key.replace('.manifest.json', '.parquet')
    footer = json.loads(pq.ParquetFile(io.BytesIO(client.objects[data_key])).schema_arrow.metadata[b'btc_book_recorder'])
    footer[field] = bad_value
    rewrite_hour_footer(client, key, footer)
    with pytest.raises(app.FetchError):
        build(app, client, tmp_path, loaded(app, client, key))
    assert destination.read_bytes() == previous
    assert list(destination.parent.iterdir()) == [destination]


def test_legacy_source_gap_prevents_false_completeness_with_contiguous_coverage(app):
    client = FakeS3()
    manifest, _, key = add_legacy_hour(client, 0)
    manifest.update(gap_count=1, gaps=[{'reason': 'sequence_gap', 'recv_ns': DAY_NS + 10}])
    save_manifest(client, key, manifest)
    hour = loaded(app, client, key)[0]
    assert hour.provenance['coverage_gaps'] == hour.provenance['book_coverage_gaps'] == []
    assert hour.provenance['coverage_complete'] is False
    assert hour.provenance['book_coverage_complete'] is False
    assert hour.provenance['gap_count'] == 1
    assert hour.provenance['gaps'] == manifest['gaps']


def test_all_24_files_with_short_internal_gap_report_gap_duration(app, tmp_path):
    client = FakeS3()
    keys = []
    for hour in range(24):
        manifest, _, key = add_hour(client, hour)
        keys.append(key)
        if hour == 3:
            start, end = manifest['interval_start_ns'], manifest['interval_end_ns']
            gap_start = start + HOUR_NS // 2
            gap_end = gap_start + 2_500_000_000
            intervals = [{'start_ns': start, 'end_ns': gap_start}, {'start_ns': gap_end, 'end_ns': end}]
            gaps = [{'start_ns': gap_start, 'end_ns': gap_end}]
            manifest.update(coverage_intervals=intervals, book_coverage_intervals=intervals,
                            coverage_gaps=gaps, book_coverage_gaps=gaps,
                            coverage_complete=False, book_coverage_complete=False,
                            gap_count=1, gaps=[{'reason': 'transport_closed', 'recv_ns': gap_start}])
            save_manifest(client, key, manifest)
    result = build(app, client, tmp_path, loaded(app, client, *keys))
    assert result['hours'] == 24 and result['missing_hours'] == []
    assert result['book_coverage_complete'] is False
    assert result['book_coverage_gap_count'] == 1
    assert result['book_uncovered_seconds'] == 2.5
    assert result['source_gap_count'] == 1
    assert result['legacy_direct_hours'] == 0
    assert result['rows'] == 48


def test_legacy_book_coverage_cannot_precede_transport_coverage(app):
    client = FakeS3()
    manifest, _, key = add_legacy_hour(client, 0)
    manifest['coverage_start_ns'] = DAY_NS + 100
    # A carried book's state may predate renewed transport coverage. Only the
    # intersection is covered; never extend coverage backward from book state.
    manifest['book_reconstructible_start_ns'] = DAY_NS
    save_manifest(client, key, manifest)
    hour = loaded(app, client, key)[0]
    assert hour.provenance['book_coverage_intervals'] == [{
        'start_ns': DAY_NS + 100, 'end_ns': DAY_NS + HOUR_NS}]
    assert hour.provenance['book_coverage_gaps'] == [{
        'start_ns': DAY_NS, 'end_ns': DAY_NS + 100}]
    assert hour.provenance['book_coverage_complete'] is False


def test_legacy_row_from_other_connection_preserves_previous_daily(app, tmp_path):
    client = FakeS3()
    _, _, key0 = add_legacy_hour(client, 0)
    build(app, client, tmp_path, loaded(app, client, key0))
    destination = output_path(tmp_path)
    previous = destination.read_bytes()
    rows = make_rows(1)
    rows[1]['connection_id'] = 'different-connection'
    _, _, key1 = add_legacy_hour(client, 1, rows=rows)
    with pytest.raises(app.FetchError):
        build(app, client, tmp_path, loaded(app, client, key0, key1))
    assert destination.read_bytes() == previous
    assert list(destination.parent.iterdir()) == [destination]
