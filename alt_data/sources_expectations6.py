#!/usr/bin/env python3
"""Original archived GFS forecast cycles for economic hubs in five undercovered countries.

The canonical date is model initialization, NOT forecast valid time or public availability.
An auxiliary panel retains valid times, lead windows and archived object modification clocks.
"""
from __future__ import annotations
import argparse, hashlib, io, json, re, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from pathlib import Path
import numpy as np
import pandas as pd
import requests
from expansion_common import ROOT, Fetcher, archive_raw_manifest, atomic_bytes
from sources_intent6 import attempt, OMISSIONS
try: import eccodes as ec
except ImportError:
    # Optional binary dependency installed locally without altering the host Python environment.
    sys.path.append(str(ROOT/'archive/dependencies'))
    import eccodes as ec

GROUP='expectations6'
ACCESS_STOP=threading.Event()
LOCATIONS=[
 ('CN','shanghai',31,121,'Shanghai industrial and port region'),('CN','zhengzhou',35,114,'Henan agricultural and logistics region'),('CN','guangzhou',23,113,'Pearl River Delta manufacturing region'),
 ('IN','delhi',29,77,'Delhi electricity demand region'),('IN','mumbai',19,73,'Mumbai industrial and port region'),('IN','ludhiana',31,76,'Punjab agricultural region'),
 ('KR','seoul',38,127,'Seoul electricity demand region'),('KR','ulsan',36,129,'Ulsan industrial and port region'),('KR','jeonju',36,127,'Jeonju agricultural region'),
 ('ID','jakarta',-6,107,'Jakarta industrial and port region'),('ID','medan',4,99,'North Sumatra plantation region'),('ID','surabaya',-7,113,'East Java industrial and agricultural region'),
 ('MX','mexico_city',19,261,'Mexico City electricity demand region'),('MX','monterrey',26,260,'Monterrey industrial region'),('MX','culiacan',25,253,'Sinaloa agricultural region')]
LEADS=[24,72,168]
FIELDS={'temperature_2m':('TMP','2 m above ground','instant','K'),
        'precipitation_6h':('APCP','surface','accum','kg m-2 over preceding 6 forecast hours'),
        'wind_u_10m':('UGRD','10 m above ground','instant','m s-1'),
        'wind_v_10m':('VGRD','10 m above ground','instant','m s-1'),
        'solar_radiation_6h':('DSWRF','surface','avg','W m-2 (preceding 6 forecast hours mean)')}
GRIB_IDENTITIES={
    'temperature_2m':(167,'heightAboveGround',2,'K'),
    'precipitation_6h':(228228,'surface',0,'kg m**-2'),
    'wind_u_10m':(165,'heightAboveGround',10,'m s**-1'),
    'wind_v_10m':(166,'heightAboveGround',10,'m s**-1'),
    'solar_radiation_6h':(260087,'surface',0,'W m**-2')}


def index_records(raw,day,lead):
    rows=[line.split(':') for line in raw.decode().splitlines() if line.strip()]
    if any(int(b[1])<=int(a[1]) for a,b in zip(rows,rows[1:])):raise ValueError('Nonmonotone GRIB offsets')
    result={}
    for field,(var,level,step,unit) in FIELDS.items():
        label=f'{lead} hour fcst' if step=='instant' else f'{lead-6}-{lead} hour {"acc" if step=="accum" else "ave"} fcst'
        # Longer durations may use day units, but six-hour windows stay hours.
        matches=[i for i,r in enumerate(rows) if r[2]=='d='+day+'00' and r[3]==var and r[4]==level and r[5]==label]
        if len(matches)!=1:raise ValueError(f'Expected one {field} {day} {label}, got {len(matches)}')
        i=matches[0]
        if i+1==len(rows):raise ValueError('Missing ending offset')
        result[field]=(int(rows[i][1]),int(rows[i+1][1])-1)
    return result


def range_get(url,start,end,refresh=False,session=None):
    # Range is part of cache identity. The general URL-only Fetcher is deliberately not used here.
    span=f'bytes={start}-{end}';key=hashlib.sha256((url+'|'+span).encode()).hexdigest()
    path=ROOT/'archive/requests'/(key+'.json')
    if not refresh and path.exists():
        meta=json.loads(path.read_text());raw=(ROOT/'archive/raw'/(meta['sha256']+'.bin')).read_bytes()
        if hashlib.sha256(raw).hexdigest()==meta['sha256'] and len(raw)==end-start+1:return raw,meta
    for attempt_no in range(3):
        if ACCESS_STOP.is_set():
            raise RuntimeError('Provider requests stopped after an access/rate-limit response')
        try:
            r=(session or requests).get(url,headers={'Range':span,'User-Agent':'FinAltDataResearch/1.0'},timeout=(15,90));r.raise_for_status()
            if r.status_code!=206 or not r.headers.get('Content-Range','').startswith(f'bytes {start}-{end}/'):raise ValueError('Server did not return the requested range')
            raw=r.content
            if len(raw)!=end-start+1 or not raw.startswith(b'GRIB') or not raw.endswith(b'7777'):raise ValueError('Incomplete GRIB message')
            sha=hashlib.sha256(raw).hexdigest();now=pd.Timestamp.now(tz='UTC')
            meta={'url':r.url,'request_range':span,'content_range':r.headers['Content-Range'],'sha256':sha,'retrieved_at':now.isoformat(),
                  'source_last_modified':r.headers.get('Last-Modified'),'etag':r.headers.get('ETag'),'content_type':r.headers.get('Content-Type')}
            atomic_bytes(ROOT/'archive/raw'/(sha+'.bin'),raw)
            atomic_bytes(ROOT/'archive/receipts'/(key+'_'+now.strftime('%Y%m%dT%H%M%S%fZ')+'.json'),json.dumps(meta,indent=2).encode())
            atomic_bytes(path,json.dumps(meta,indent=2).encode());return raw,meta
        except requests.RequestException as exc:
            status=getattr(getattr(exc,'response',None),'status_code',None)
            if status in [401,403,429]:ACCESS_STOP.set()
            if status not in [None,500,502,503,504] or attempt_no==2:raise
            time.sleep(3**(attempt_no+1))


def decode_points(raw,day,lead,field):
    g=ec.codes_new_from_message(raw)
    try:
        identity=tuple(ec.codes_get(g,k) for k in ['paramId','typeOfLevel','level','units'])
        if identity!=GRIB_IDENTITIES[field] or ec.codes_get(g,'stepType')!=FIELDS[field][2]:
            raise ValueError('Wrong forecast variable, level, units or statistic')
        if ec.codes_get(g,'dataDate')!=int(day) or ec.codes_get(g,'dataTime')!=0:raise ValueError('Wrong forecast initialization')
        start=lead if FIELDS[field][2]=='instant' else lead-6
        if ec.codes_get(g,'startStep')!=start or ec.codes_get(g,'endStep')!=lead:raise ValueError('Wrong forecast accumulation/lead window')
        expected={'Ni':360,'Nj':181,'latitudeOfFirstGridPointInDegrees':90.,'longitudeOfFirstGridPointInDegrees':0.,
                  'latitudeOfLastGridPointInDegrees':-90.,'longitudeOfLastGridPointInDegrees':359.,'iScansNegatively':0,'jScansPositively':0,'jPointsAreConsecutive':0}
        if any(ec.codes_get(g,k)!=v for k,v in expected.items()):raise ValueError('Unexpected grid orientation')
        values=ec.codes_get_values(g)
        if len(values)!=360*181:raise ValueError('Unexpected grid size')
        out=[float(values[(90-lat)*360+lon]) for _,_,lat,lon,_ in LOCATIONS]
        if not np.isfinite(out).all() or any(abs(v)>1e8 for v in out):raise ValueError('Missing/invalid forecast value')
        return out
    finally:ec.codes_release(g)


def collect_one(day,lead,refresh=False):
    cache=ROOT/'archive/expectations6_points'/f'{day}_{lead:03}.json'
    if not refresh and cache.exists():
        obj=json.loads(cache.read_text())
        # Re-extract if code schema changes; raw source is retained independently.
        if obj.get('parser_version')==2:return obj
    fetcher=Fetcher(refresh=refresh)
    if ACCESS_STOP.is_set():raise RuntimeError('Provider requests stopped after an access/rate-limit response')
    url=f'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{day}/00/atmos/gfs.t00z.pgrb2.1p00.f{lead:03}'
    try:
        idx,idxsha=fetcher.get(url+'.idx',max_age_hours=876000)
    except requests.RequestException as exc:
        if getattr(getattr(exc,'response',None),'status_code',None) in [401,403,429]:ACCESS_STOP.set()
        raise
    spans=index_records(idx,day,lead);fields={};hashes=[idxsha];modified=[];receipts=[]
    for field,(start,end) in spans.items():
        raw,receipt=range_get(url,start,end,refresh)
        fields[field]=decode_points(raw,day,lead,field);hashes.append(receipt['sha256']);receipts.append(receipt)
        if not receipt['source_last_modified']:raise ValueError('Missing source Last-Modified')
        modified.append(pd.Timestamp(receipt['source_last_modified']))
    if len(set(modified))!=1:raise ValueError('Source file changed between byte-range requests')
    obj={'parser_version':2,'day':day,'lead':lead,'fields':fields,'raw_sha256':hashes,
         'source_last_modified':modified[0].isoformat(),'receipts':receipts}
    atomic_bytes(cache,json.dumps(obj).encode());return obj


def collect(refresh=False,start='2025-10-01',end=None):
    end=end or (pd.Timestamp.now(tz='UTC').floor('D')-pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    f=Fetcher(refresh=refresh)
    docs=[]
    for url in ['https://registry.opendata.aws/noaa-gfs-bdp-pds/','https://www.nco.ncep.noaa.gov/pmb/products/gfs/']:
        _,h=f.get(url);docs.append(h)
    rows=[];hashes=set(docs);failures=[]
    dates=pd.date_range(start,end,freq='D').strftime('%Y%m%d').tolist();tasks=[(day,lead) for day in dates for lead in LEADS]
    def work(pair):
        try:return collect_one(*pair,refresh=refresh)
        except Exception as exc:return {'failure':{'day':pair[0],'lead':pair[1],'error':str(exc)}}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for n,obj in enumerate(pool.map(work,tasks),1):
            if 'failure' in obj:failures.append(obj['failure']);continue
            init=pd.Timestamp(obj['day'],tz='UTC');lead=obj['lead'];hashes.update(obj['raw_sha256'])
            for i,(country,loc,lat,lon,description) in enumerate(LOCATIONS):
                for field,values in obj['fields'].items():
                    if field.startswith('wind_'):continue
                    rows.append({'location':loc,'country':country,'field':field,'initialization':init,'valid_time':init+pd.Timedelta(hours=lead),
                                 'lead_hours':lead,'window_start_hours':lead if FIELDS[field][2]=='instant' else lead-6,
                                 'value':values[i],'source_object_last_modified':pd.Timestamp(obj['source_last_modified'])})
                rows.append({'location':loc,'country':country,'field':'wind_speed_10m','initialization':init,'valid_time':init+pd.Timedelta(hours=lead),
                             'lead_hours':lead,'window_start_hours':lead,'value':float(np.hypot(obj['fields']['wind_u_10m'][i],obj['fields']['wind_v_10m'][i])),
                             'source_object_last_modified':pd.Timestamp(obj['source_last_modified'])})
            if n%60==0:print('GFS forecast files',n,'/',len(tasks),'failures',len(failures),flush=True)
    if not rows:raise ValueError('No usable forecast files')
    manifest=archive_raw_manifest(hashes,description='NOAA original GFS 1-degree GRIB forecast byte ranges, index files and documentation')
    panel=pd.DataFrame(rows).sort_values(['location','field','lead_hours','initialization'])
    incoming_panel=panel
    panel_path=ROOT/'supplementary/gfs_forecast_issue_target.parquet'
    if panel_path.exists():
        panel=pd.concat([pd.read_parquet(panel_path),panel],ignore_index=True).drop_duplicates(
            ['location','field','lead_hours','initialization'],keep='last').sort_values(
            ['location','field','lead_hours','initialization'])
    buf=io.BytesIO();panel.to_parquet(buf,index=False,compression='zstd');atomic_bytes(ROOT/'supplementary/gfs_forecast_issue_target.parquet',buf.getvalue())
    good=[]
    for (loc,field,lead),part in incoming_panel.groupby(['location','field','lead_hours'],sort=True):
        country,_,lat,lon,description=next(x for x in LOCATIONS if x[1]==loc)
        unit='m s-1' if field=='wind_speed_10m' else FIELDS[field][3]
        sid=f'gfs6_{country.lower()}_{loc}_{field}_lead{lead:03}'
        attempt(good,sid,part.rename(columns={'initialization':'date'})[['date','value']],collector_group=GROUP,
            title=f'GFS {description}: {field.replace("_"," ")}, {lead}-hour lead',provider='NOAA GFS archived operational forecasts',
            category='forecast_weather_expectations',unit=unit,frequency='daily',source_url='https://registry.opendata.aws/noaa-gfs-bdp-pds/',
            geography=description,country_codes=[country],geography_scope='non_us',dataset_identity=f'noaa_gfs|1p00|00UTC|{lat}|{lon}|{field}|{lead}h',
            hypothesis='Forecasts and same-target forecast revisions may precede changes in electricity demand, crop stress, transport disruption and renewable generation expectations.',
            limitations='Archived operational forecasts, not observations or reanalysis. Exact nearest selected 1-degree grid cell, no population/production weighting. Once-daily 00 UTC initialization sampled from four daily cycles. Initialization precedes computation/publication. Object Last-Modified is retained as a source archive clock, not asserted as first public release. Wind speed is derived from native U/V components. Weather forecasts are not market consensus forecasts.',
            native_frequency='6-hourly model cycles; selected 00 UTC cycle daily',finer_frequency_available=True,
            expected_publication_lag='GFS file publication occurs hours after initialization; actual archive object Last-Modified retained per issue/target in supplementary panel. First-public-release times unverified.',
            timestamp_semantics='Model initialization time at 00 UTC; forecast target is date + lead_hours, not an observed outcome date. Not public availability time.',
            raw_sha256=docs,raw_manifest_sha256=[manifest],license='NOAA NODD public data; derived point extraction, attribution NOAA/NCEP.',
            source_metric=field,forecast_lead_hours=int(lead),forecast_model='GFS operational',grid_latitude=float(lat),grid_longitude=float(lon if lon<=180 else lon-360),
            supplemental_panel='supplementary/gfs_forecast_issue_target.parquet',revision_policy='Original operational cycle bytes retained; later model cycles are different forecasts.')
    report={'series':len(good),'observations':sum(m['rows'] for m in good),'forecast_files_requested':len(tasks),'files_failed':failures,'omissions':OMISSIONS,
            'series_ids':[m['series_id'] for m in good],'finished_at':pd.Timestamp.now(tz='UTC').isoformat()}
    atomic_bytes(ROOT/'logs/expectations6_report.json',json.dumps(report,indent=2).encode());return report

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--refresh',action='store_true');p.add_argument('--start',default='2025-10-01');p.add_argument('--end');a=p.parse_args()
    r=collect(a.refresh,a.start,a.end);print(json.dumps({k:v for k,v in r.items() if k!='series_ids'},indent=2));raise SystemExit(bool(r['files_failed']))
if __name__=='__main__':main()
