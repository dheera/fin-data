#!/usr/bin/env python3
"""Public alternative data collectors; run --help. No credentials required."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import threading
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from fred_definitions import CATALOG

HERE = Path(__file__).resolve().parent
UTC = 'UTC'
SCHEMA = pa.schema([
    ('series_id', pa.string()), ('timestamp', pa.timestamp('ns', tz=UTC)),
    ('value', pa.float64()), ('unit', pa.string()), ('frequency', pa.string()),
    ('source', pa.string()), ('source_url', pa.string()), ('category', pa.string()),
    ('title', pa.string()), ('geography', pa.string()), ('measure_type', pa.string()),
    ('retrieved_at', pa.timestamp('ns', tz=UTC)), ('source_updated_at', pa.timestamp('ns', tz=UTC)),
    ('sample_size', pa.int64()), ('methodology', pa.string()), ('raw_sha256', pa.string()),
], metadata={b'schema_version': b'1', b'timestamp_semantics': b'source observation period/session; NOT availability time',
             b'retrieved_at_semantics': b'first local retrieval of this version; not historical release time'})
DRAM_URL = 'https://www.dramexchange.com/'
PORT_URL = 'https://kentico.portoflosangeles.org/getmedia/f30c3f45-2df9-4830-90ca-2364bfa0484f/operations-report'


def atomic_bytes(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.' + path.name)
    try:
        os.fchmod(fd, 0o664)
        with os.fdopen(fd, 'wb') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)


def write_parquet(path, rows):
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression='zstd')
    atomic_bytes(path, sink.getvalue().to_pybytes())


class Client:
    def __init__(self, root, refresh=False):
        self.root = root
        self.refresh = refresh
        self.cache = {}
        self.lock = threading.Lock()
        self.last_fred_request = 0.0
        for path in (root / 'raw').glob('*.json'):
            try:
                meta = json.loads(path.read_text())
                previous = self.cache.get(meta['url'])
                if previous is None or meta['retrieved_at'] > previous['retrieved_at']:
                    self.cache[meta['url']] = meta
            except (ValueError, KeyError):
                continue

    def get(self, url, params=None):
        prepared_url = requests.Request('GET', url, params=params).prepare().url
        is_fred = urlparse(url).hostname == 'fred.stlouisfed.org'
        cached = self.cache.get(prepared_url)
        if is_fred and cached and not self.refresh:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached['retrieved_at'])).total_seconds()
            path = self.root / 'raw' / (cached['sha256'] + '.bin')
            if 0 <= age < 21600 and path.exists():
                content = path.read_bytes()
                if hashlib.sha256(content).hexdigest() == cached['sha256']:
                    return content, cached['sha256']
        for attempt in range(3):
            try:
                if is_fred:
                    with self.lock:
                        time.sleep(max(0, 1.0 - (time.monotonic() - self.last_fred_request)))
                        self.last_fred_request = time.monotonic()
                r = requests.get(url, params=params, timeout=(10, 45),
                                 headers={'User-Agent': 'fin-alt-data/0.1 (public research data collection)'})
                r.raise_for_status()
                digest = hashlib.sha256(r.content).hexdigest()
                atomic_bytes(self.root / 'raw' / (digest + '.bin'), r.content)
                meta = {'url': r.url, 'retrieved_at': datetime.now(timezone.utc).isoformat(),
                        'content_type': r.headers.get('Content-Type'), 'sha256': digest}
                encoded = json.dumps(meta, indent=2).encode()
                atomic_bytes(self.root / 'raw' / (digest + '.json'), encoded)
                receipt_name = hashlib.sha256(r.url.encode()).hexdigest() + '_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
                atomic_bytes(self.root / 'receipts' / (receipt_name + '.json'), encoded)
                return r.content, digest
            except requests.RequestException as exc:
                if isinstance(exc, requests.HTTPError) and exc.response.status_code not in [429,500,502,503,504]:
                    raise
                if attempt == 2: raise
                time.sleep(2 ** attempt)


def row(series_id, timestamp, value, *, unit, frequency, source, source_url, category,
        title, geography, measure_type='direct', source_updated_at=None, sample_size=None,
        methodology='', raw_sha256='', retrieved_at=None):
    value = float(value)
    if not math.isfinite(value): raise ValueError(f'Nonfinite value: {series_id}')
    ts = pd.Timestamp(timestamp)
    ts = ts.tz_localize(UTC) if ts.tz is None else ts.tz_convert(UTC)
    return dict(series_id=series_id, timestamp=ts, value=value, unit=unit, frequency=frequency,
                source=source, source_url=source_url, category=category, title=title,
                geography=geography, measure_type=measure_type,
                retrieved_at=retrieved_at or pd.Timestamp.now(tz=UTC), source_updated_at=source_updated_at,
                sample_size=sample_size, methodology=methodology, raw_sha256=raw_sha256)


def fetch_fred(client, spec, start):
    content, digest = client.get('https://fred.stlouisfed.org/graph/fredgraph.csv',
                                 {'id': spec['symbol'], 'cosd': start})
    df = pd.read_csv(io.BytesIO(content))
    if list(df.columns) != ['observation_date', spec['symbol']]:
        raise ValueError(f'Unexpected CSV header: {list(df.columns)}')
    html, _ = client.get(spec['url'])
    soup = BeautifulSoup(html, 'html.parser')
    unit_node = soup.select_one('.series-meta-value-units')
    label = soup.find('p', string=re.compile(r'^Frequency:$'))
    if unit_node is None or label is None: raise ValueError('FRED metadata layout changed')
    frequency = label.find_next('span', class_='series-meta-value').get_text(' ', strip=True).split(',')[0].strip().lower()
    if frequency not in {'daily','weekly','monthly','quarterly','annual'}: raise ValueError(f'Unknown frequency: {frequency}')
    title = soup.select_one('h1').get_text(' ', strip=True)
    now = pd.Timestamp.now(tz=UTC)
    values = pd.to_numeric(df[spec['symbol']], errors='coerce')
    # Missing observations are absent, never zeros. Bad nonmissing strings fail closed.
    invalid = values.isna() & ~df[spec['symbol']].astype(str).isin(['.', 'nan', ''])
    if invalid.any(): raise ValueError('Unrecognized FRED numeric value')
    result = [row(spec['series_id'], date, val, unit=unit_node.get_text(' ', strip=True),
                  frequency=frequency, source='fred', source_url=spec['url'], category=spec['category'],
                  title=title, geography='See source series metadata',
                  measure_type='published_index' if 'Index' in title else 'direct',
                  methodology='Native frequency; latest revised history, not historical vintages. Seasonal adjustment and geography are source-specific; see source URL.',
                  raw_sha256=digest, retrieved_at=now)
              for date, val in zip(df['observation_date'], values) if pd.notna(val) and date >= start]
    if not result: raise ValueError('No numeric observations')
    return result


def memory_date(text):
    match = re.search(r'([A-Za-z]{3})\.(\d{1,2})\s+(\d{4})\s+(\d\d:\d\d)', text)
    if not match: raise ValueError(f'Memory update date missing: {text!r}')
    ts = datetime.strptime(' '.join(match.groups()), '%b %d %Y %H:%M')
    return pd.Timestamp(ts).tz_localize('Asia/Taipei').tz_convert(UTC)


def slug(value): return re.sub('[^a-z0-9]+', '_', value.lower()).strip('_')


def parse_memory(content, digest=''):
    soup = BeautifulSoup(content, 'html.parser')
    result = []
    def add(sid, date, value, title, unit, frequency, n=1, methodology='', kind='direct'):
        result.append(row(sid, date, value, unit=unit, frequency=frequency, source='dramexchange',
                          source_url=DRAM_URL, category='memory', title=title, geography='Source quoted market',
                          source_updated_at=date, sample_size=n, methodology=methodology,
                          measure_type=kind, raw_sha256=digest))
    # Duplicate tbody IDs exist on the source; choose by row label as well as ID.
    for body in soup.select('tbody[id]'):
        data_rows = []
        for tr in body.find_all('tr', recursive=False)[1:]:
            cells = [x.get_text(' ', strip=True) for x in tr.find_all('td', recursive=False)]
            if len(cells) >= 7 and re.match(r'^(DDR|GDDR|SLC |MLC |\d+Gb TLC|MicroSD)', cells[0]):
                data_rows.append((cells[0], float(cells[5].replace(',', ''))))
        if not data_rows: continue
        date = memory_date(str(body.find_previous(string=lambda x: x and 'Last Update' in x)))
        weekly = 'Weekly High' in body.get_text(' ', strip=True)
        freq = 'weekly' if weekly else 'session'
        modules = []
        for label, value in data_rows:
            add('memory_' + slug(label), date, value, label + ' session average', 'USD/item', freq,
                methodology='Source session average; chip, module, wafer or card as specified by title.')
            if label.startswith(('DDR4 UDIMM 16GB', 'DDR5 UDIMM 16GB')):
                modules.append((label, value / 16))
        if modules:
            if len(modules) != 2: raise ValueError('Expected both DDR4 and DDR5 16GB UDIMM basket constituents')
            add('ram_udimm_16gb_mean_usd_per_gb', date, sum(v for _,v in modules)/2,
                'Equal-weight DDR4/DDR5 16GB UDIMM spot average per GB', 'USD/GB', 'weekly', 2,
                json.dumps({'aggregation':'arithmetic mean of module average / 16 GB', 'constituents':modules,
                            'limitations':'Wholesale spot quotes; not retail or sales-weighted; excludes server RDIMM.'}), 'basket')
    chip_basket = [r for r in result if r['series_id'] in {
        'memory_ddr5_16gb_2gx8_4800_5600', 'memory_ddr4_16gb_2gx8_3200'}]
    if len(chip_basket) != 2 or len({r['timestamp'] for r in chip_basket}) != 1:
        raise ValueError('DDR4/DDR5 mainstream 16 Gbit chip basket missing or dates disagree')
    add('ram_dram_16gbit_mean_usd_per_gb', chip_basket[0]['timestamp'],
        sum(r['value']/2 for r in chip_basket)/2,
        'Equal-weight DDR4/DDR5 16 Gbit chip spot average per GB', 'USD/GB', 'session', 2,
        json.dumps({'aggregation':'arithmetic mean of two chip session averages divided by 2 GB per 16 Gbit chip',
                    'constituents':[r['series_id'] for r in chip_basket],
                    'limitations':'Component spot costs, excludes module assembly and retail margins; excludes eTT.'}), 'basket')
    header = soup.find('tr', class_='ssditem')
    if header is None: raise ValueError('SSD table missing')
    table = header.find_parent('table')
    date = memory_date(str(table.find_previous(string=lambda x: x and 'LastUpdate' in x)))
    basket = []
    for tr in table.select('tr.ssdpr'):
        cells = [x.get_text(' ', strip=True) for x in tr.find_all('td', recursive=False)]
        if len(cells) != 8: raise ValueError('SSD table layout changed')
        brand, interface, model, capacity, high, low, avg, change = cells
        m = re.fullmatch(r'([\d.]+)\s*(TB|GB)', capacity)
        if not m: raise ValueError('Unknown SSD capacity')
        gb = float(m[1]) * (1000 if m[2] == 'TB' else 1)
        value = float(avg.replace(',', '')) / gb
        label = ' '.join(cells[:4])
        basket.append((label, value))
        add('ssd_' + slug(label), date, value, label + ' average street price per GB', 'USD/GB',
            'weekly', methodology='Source average street price divided by decimal advertised GB; no sales weighting.')
    if len(basket) < 3: raise ValueError('Insufficient SSD basket coverage')
    add('ssd_street_mean_usd_per_gb', date, sum(v for _,v in basket)/len(basket),
        'Equal-weight SSD street-price basket per GB', 'USD/GB', 'weekly', len(basket),
        json.dumps({'aggregation':'arithmetic mean of model average / capacity in decimal GB','constituents':basket,
                    'limitations':'Small changing sample, mixed interfaces/capacities; not market-wide or sales-weighted.'}), 'basket')
    fixed = [(label, value) for label, value in basket if label in {
        'Kimtigo PCIe 4.0 TP5000 1 TB', 'Samsung PCIe 4.0 X4 990 Pro 1 TB',
        'Silicon Power PCIe 4.0 X4 US75 1 TB'}]
    if len(fixed) != 3:
        raise ValueError('Fixed 1TB PCIe4 SSD basket missing a constituent; review basket explicitly')
    add('ssd_pcie4_1tb_fixed_mean_usd_per_gb', date, sum(v for _,v in fixed)/3,
        'Fixed three-model 1TB PCIe4 SSD street-price average per GB', 'USD/GB', 'weekly', 3,
        json.dumps({'aggregation':'equal-weight source average price / 1000 GB','constituents':fixed,
                    'limitations':'Three-model convenience sample; not sales-weighted. Fails if any constituent disappears.'}), 'basket')
    if not any(x['series_id']=='ram_udimm_16gb_mean_usd_per_gb' for x in result):
        raise ValueError('RAM basket missing')
    return result


def parse_port(text, digest=''):
    # pdftotext -layout retains the two reporting columns. Never use report date for both.
    dates = re.findall(r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), ([A-Za-z]+ \d{1,2}, \d{4})', text)
    if len(dates) < 3: raise ValueError('Port date columns missing')
    dates = [pd.Timestamp(datetime.strptime(d, '%B %d, %Y'), tz=UTC) for d in dates[1:3]]
    labels = {
        'Local Cargo Avg Dwell': ('port_la_local_cargo_dwell_days', 'days'),
        'On-Dock Rail Avg Dwell': ('port_la_rail_dwell_days', 'days'),
        'Container Vessels at Berth': ('port_la_vessels_at_berth', 'vessels'),
        'Export Containers On Terminal': ('port_la_export_containers', 'containers'),
        'Empty Containers On Terminal': ('port_la_empty_containers', 'containers'),
    }
    rows = []
    for label, (sid, unit) in labels.items():
        lines = [line for line in text.splitlines() if line.startswith(label)]
        if len(lines) != 1: raise ValueError(f'Port row missing or ambiguous: {label}')
        # Remaining text after POLA contains only the two values for these selected rows.
        tail = re.split(r'POLA\s*\*{0,2}', lines[0])[-1]
        values = re.findall(r'\d[\d,]*(?:\.\d+)?', tail)
        if len(values) != 2: raise ValueError(f'Port numeric columns changed: {lines[0]}')
        for date, val in zip(dates, values):
            rows.append(row(sid, date, float(val.replace(',','')), unit=unit, frequency='daily',
                            source='port_of_los_angeles', source_url=PORT_URL, category='shipping',
                            title=label + ' - Port of Los Angeles', geography='Los Angeles, US',
                            methodology='Port operations report; dwell means time on terminal, not delay against promised arrival. Report may lag calendar date.', raw_sha256=digest))
    return rows


def fetch_port(client):
    content, digest = client.get(PORT_URL)
    with tempfile.TemporaryDirectory() as tmp:
        pdf = Path(tmp) / 'report.pdf'
        pdf.write_bytes(content)
        output = subprocess.run(['pdftotext', '-layout', str(pdf), '-'], check=True, capture_output=True)
    return parse_port(output.stdout.decode(), digest)


def fetch_space_weather(client):
    url = 'https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json'
    content, digest = client.get(url)
    data = json.loads(content)
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        raise ValueError('Unexpected NOAA JSON layout')
    return [row('noaa_planetary_kp', r['time_tag'], r['Kp'], unit='Kp index', frequency='3-hourly',
                source='noaa_swpc', source_url=url, category='space_weather',
                title='Estimated planetary Kp geomagnetic activity', geography='Global',
                raw_sha256=digest, methodology='Rolling NOAA feed; run daily to retain history.') for r in data]


def persist(root, rows, short_history=False):
    grouped = {}
    for r in rows:
        if not re.fullmatch('[a-z0-9_]+', r['series_id']): raise ValueError('Unsafe series ID')
        grouped.setdefault(r['series_id'], []).append(r)
    summaries = []
    for sid, batch in grouped.items():
        path = root / 'archive' / ('legacy_short_history' if short_history else 'legacy_observations') / (sid + '.parquet')
        archive = root / 'archive' / 'revisions' / (sid + '.parquet')
        # Archive is canonical; regenerating latest repairs an interrupted two-file write.
        history = pq.read_table(archive).to_pylist() if archive.exists() else []
        latest = {r['timestamp']:r for r in history}
        seen = set()
        changed = 0
        for r in sorted(batch, key=lambda r:r['timestamp']):
            if r['timestamp'] in seen: raise ValueError(f'Duplicate incoming timestamp: {sid}')
            seen.add(r['timestamp'])
            old = latest.get(r['timestamp'])
            compare = ['value','unit','frequency','methodology','sample_size','source_updated_at','title','measure_type']
            if old is None or any(old[k] != r[k] for k in compare):
                history.append(r)
                latest[r['timestamp']] = r
                changed += 1
        if changed: write_parquet(archive, history)
        current = sorted(latest.values(), key=lambda r:r['timestamp'])
        write_parquet(path, current)
        from library import publish_legacy
        publish_legacy(current, root, short_history=short_history)
        summaries.append({'series_id':sid, 'rows':len(current), 'changed':changed,
                          'first_timestamp':str(current[0]['timestamp']), 'last_timestamp':str(current[-1]['timestamp']),
                          'frequency':current[-1]['frequency'], 'unit':current[-1]['unit']})
    return summaries


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', choices=['all','fred','memradar','gscpi','pmsa','memory','port','space_weather'], default='all')
    p.add_argument('--series', nargs='+', help='FRED symbols, e.g. CUUR0000SETG01 DCOILWTICO')
    p.add_argument('--start', default='2020-01-01', type=lambda x: datetime.strptime(x,'%Y-%m-%d').strftime('%Y-%m-%d'))
    p.add_argument('--output', type=Path, default=HERE, help='Library root, containing data/, archive/ and logs/')
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--list', action='store_true')
    p.add_argument('--include-snapshots', '--include-short-history', action='store_true', help='Also collect short-history DRAMeXchange, LA report and NOAA feeds')
    p.add_argument('--refresh', action='store_true', help='Bypass the six-hour FRED response cache')
    args = p.parse_args()
    if args.list:
        print(json.dumps(CATALOG,indent=2)); return 0
    if not 1 <= args.workers <= 8: p.error('--workers must be 1..8')
    specs = CATALOG
    if args.series:
        if args.source not in ['all','fred']: p.error('--series only applies to FRED')
        unknown = set(args.series) - {s['symbol'] for s in CATALOG}
        if unknown: p.error(f'Unknown symbols: {sorted(unknown)}')
        specs = [s for s in specs if s['symbol'] in args.series]
    args.output.mkdir(parents=True, exist_ok=True)
    lock = open(args.output / '.collect.lock', 'a')
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: p.exit(2, 'Another collector holds this output lock\n')
    client = Client(args.output / 'archive', refresh=args.refresh)
    from history_sources import fetch_memradar, fetch_gscpi, fetch_pmsa
    jobs = []
    if args.source in ['all','fred']:
        jobs += [(s['series_id'], lambda s=s: fetch_fred(client,s,args.start)) for s in specs]
    if not args.series:
        for name, fn in [('memradar',fetch_memradar),('gscpi',fetch_gscpi),('pmsa',fetch_pmsa)]:
            if args.source in ['all',name]: jobs.append((name,lambda fn=fn:fn(client)))
    if not args.series and (args.include_snapshots or args.source != 'all'):
        if args.source in ['all','memory']: jobs.append(('memory',lambda: parse_memory(*client.get(DRAM_URL))))
        if args.source in ['all','port']: jobs.append(('port',lambda: fetch_port(client)))
        if args.source in ['all','space_weather']: jobs.append(('space_weather',lambda: fetch_space_weather(client)))
    report = {'started_at':datetime.now(timezone.utc).isoformat(), 'start':args.start, 'successes':[], 'failures':[]}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(fn): name for name, fn in jobs}
        for future in as_completed(pending):
            name = pending[future]
            try:
                rows = future.result()
                if not rows: raise ValueError('Collector returned no rows')
                summaries = persist(args.output, rows, short_history=name in {'memory','port','space_weather'})
                report['successes'].extend(summaries)
                print(f'OK {name}: {len(rows)} observations, {len(summaries)} series', flush=True)
            except Exception as e:
                report['failures'].append({'collector':name,'error':f'{type(e).__name__}: {e}'})
                print(f'FAILED {name}: {e}', flush=True)
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    encoded = json.dumps(report, indent=2).encode()
    atomic_bytes(args.output / 'logs' / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.json'), encoded)
    atomic_bytes(args.output / 'logs' / 'latest_legacy_refresh.json', encoded)
    from library import build_catalog
    build_catalog(args.output)
    print(f"Completed: {len(report['successes'])} series; {len(report['failures'])} failed collectors")
    return int(bool(report['failures']))

if __name__ == '__main__': raise SystemExit(main())
