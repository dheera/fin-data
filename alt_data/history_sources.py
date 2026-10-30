"""Backfillable memory price and supply-chain sources; no snapshot-only feeds."""
import io
import re
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from collect import row

MEMRADAR_URL = 'https://memradar.com/data/memradar-price-index-monthly.csv'
GSCPI_URL = 'https://www.newyorkfed.org/medialibrary/research/interactives/data/gscpi/gscpi_interactive_data.csv'
PMSA_URL = 'https://www.pmsaship.com/dwell-times-main'


def parse_memradar(content, digest=''):
    text = content.decode('utf-8-sig')
    df = pd.read_csv(io.StringIO(text), comment='#')
    expected = ['segment','month','median_price_usd','median_usd_per_gb','product_count']
    if list(df.columns) != expected: raise ValueError('MemRadar CSV schema changed')
    if set(df.segment) != {'ddr4','ddr5','nvme_ssd','sata_ssd'}: raise ValueError('MemRadar segments changed')
    match = re.search(r'# Generated (\d{4}-\d\d-\d\d)', text)
    if not match: raise ValueError('MemRadar generation date missing')
    updated = pd.Timestamp(match[1], tz='UTC')
    rows=[]
    for r in df.itertuples(index=False):
        if r.product_count < 10: raise ValueError('MemRadar sample below published minimum')
        timestamp = pd.Timestamp(r.month+'-01', tz='UTC')
        if timestamp >= updated.replace(day=1): raise ValueError('Unexpected incomplete month')
        for column,unit in [('median_price_usd','USD/item'),('median_usd_per_gb','USD/GB')]:
            value = getattr(r,column)
            if value <= 0: raise ValueError('Nonpositive memory price')
            rows.append(row(f'memradar_{r.segment}_{column}',timestamp,value,
                unit=unit,frequency='monthly',source='memradar',source_url=MEMRADAR_URL,
                category='memory_history',title=f'MemRadar {r.segment} monthly {column}',
                geography='US retail',measure_type='sample_median',sample_size=int(r.product_count),
                source_updated_at=updated,raw_sha256=digest,
                methodology='Source: MemRadar Memory Price Index, https://memradar.com/price-index/; monthly segment price levels from licensed Amazon history, not a constant-quality return index. Catalog assembled July 2026: survivorship and changing product mix. At least 10 products per month; before July 22 2026 source history records price changes. See https://memradar.com/methodology/ and raw CSV notes. No interpolation.'))
    return rows


def fetch_memradar(client): return parse_memradar(*client.get(MEMRADAR_URL))


def parse_gscpi(content,digest=''):
    df=pd.read_csv(io.BytesIO(content))
    if df.columns[0] != 'Date' or len(df.columns)<2: raise ValueError('GSCPI CSV schema changed')
    # Columns are historical publication vintages, NOT separate indicators.
    releases=pd.to_datetime(df.columns[1:],format='%b-%y')
    column=df.columns[1:][releases.argmax()]
    dates=pd.to_datetime(df.Date,format='%d-%b-%Y',utc=True)
    values=pd.to_numeric(df[column],errors='raise')
    return [row('nyfed_gscpi',date,val,unit='standard deviations',frequency='monthly',source='new_york_fed',
        source_url=GSCPI_URL,category='shipping',title='Global Supply Chain Pressure Index',geography='Global',
        measure_type='proxy',raw_sha256=digest,
        methodology=f'Latest source vintage column {column}; source month-end dates retained. Composite transportation/manufacturing pressure index, not delay in days. Source vintage matrix retained in raw CSV; vintage column identifies release month, not exact availability date.')
        for date,val in zip(dates,values) if pd.notna(val)]


def fetch_gscpi(client): return parse_gscpi(*client.get(GSCPI_URL))


def parse_pmsa(content,digest='',url=''):
    # The source chart publishes a headerless, tab-separated downloadable dataset.
    df=pd.read_csv(io.BytesIO(content),sep='\t',header=None)
    if len(df.columns)!=3: raise ValueError('PMSA chart column count changed')
    dates=pd.to_datetime(df[0],format='%b-%y',utc=True)
    rows=[]
    for col,mode in [(1,'truck'),(2,'rail')]:
        for date,val in zip(dates,pd.to_numeric(df[col],errors='raise')):
            if not 0 <= val <= 100: raise ValueError('Implausible dwell time')
            rows.append(row(f'pmsa_san_pedro_{mode}_dwell_days',date,val,unit='days',frequency='monthly',
                source='pmsa',source_url=url,category='shipping',
                title=f'San Pedro Bay weighted average inbound laden {mode} container dwell time',
                geography='Los Angeles and Long Beach, US',raw_sha256=digest,
                methodology='PMSA published Datawrapper chart dataset. Monthly weighted average dwell at terminals, not delay versus promised delivery. Rolling chart window; retained older observations survive future refreshes. Column order: month, truck, rail.'))
    return rows


def fetch_pmsa(client):
    html,_=client.get(PMSA_URL)
    soup=BeautifulSoup(html,'html.parser')
    urls={x.get('href') or x.get('src') for x in soup.find_all(['a','iframe'])
          if 'datawrapper.dwcdn.net' in str(x) and '8R6Sb' in str(x)}
    if len(urls)!=1: raise ValueError('PMSA chart missing/ambiguous; review source')
    chart=urls.pop()
    url=urljoin(chart,'dataset.csv')
    return parse_pmsa(*client.get(url),url=url)
