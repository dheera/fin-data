#!/usr/bin/env python3
"""Four GFS cycles and within-day revisions to identical forecast targets.

Targets are 00 UTC at one, three and seven calendar days after issue day.
Thus forecast lead shortens through the day; this is not a fixed-lead panel.
"""
from __future__ import annotations
import argparse, hashlib, io, json, threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import requests
from expansion_common import ROOT, Fetcher, atomic_bytes, archive_raw_manifest, save_candidate
from sources_expectations6 import LOCATIONS, FIELDS, GRIB_IDENTITIES, ec, range_get, ACCESS_STOP, collect_one as midnight

GROUP='forecast7'
TARGET_DAYS=(1,3,7)
CYCLES=(0,6,12,18)
TRANSPORT=threading.local()

def index_records(raw,day,cycle,lead):
    rows=[line.split(':') for line in raw.decode().splitlines() if line.strip()]
    if any(int(b[1])<=int(a[1]) for a,b in zip(rows,rows[1:])):raise ValueError('Nonmonotone GRIB offsets')
    out={}
    for field,(var,level,step,unit) in FIELDS.items():
        label=f'{lead} hour fcst' if step=='instant' else f'{lead-6}-{lead} hour {"acc" if step=="accum" else "ave"} fcst'
        found=[i for i,r in enumerate(rows) if r[2]==f'd={day}{cycle:02}' and r[3]==var and r[4]==level and r[5]==label]
        # At f006 NOAA supplies both run-total and six-hour precipitation;
        # their windows coincide. Decode both and require identical point values.
        if len(found)!=1 and not (field=='precipitation_6h' and lead==6 and len(found)==2):raise ValueError(f'Expected one {field} {day}/{cycle} {label}; got {len(found)}')
        for j,i in enumerate(found):
            if i+1==len(rows):raise ValueError('Missing ending offset')
            out[field if j==0 else field+'_duplicate']=(int(rows[i][1]),int(rows[i+1][1])-1)
    return out

def decode_points(raw,day,cycle,lead,field):
    g=ec.codes_new_from_message(raw)
    try:
        identity=tuple(ec.codes_get(g,k) for k in ['paramId','typeOfLevel','level','units'])
        if identity!=GRIB_IDENTITIES[field] or ec.codes_get(g,'stepType')!=FIELDS[field][2]:raise ValueError('Wrong forecast variable, level, units or statistic')
        if ec.codes_get(g,'dataDate')!=int(day) or ec.codes_get(g,'dataTime')!=cycle*100:raise ValueError('Wrong forecast initialization')
        start=lead if FIELDS[field][2]=='instant' else lead-6
        if ec.codes_get(g,'startStep')!=start or ec.codes_get(g,'endStep')!=lead:raise ValueError('Wrong forecast accumulation/lead window')
        expected={'Ni':360,'Nj':181,'latitudeOfFirstGridPointInDegrees':90.,'longitudeOfFirstGridPointInDegrees':0.,'latitudeOfLastGridPointInDegrees':-90.,'longitudeOfLastGridPointInDegrees':359.,'iScansNegatively':0,'jScansPositively':0,'jPointsAreConsecutive':0}
        if any(ec.codes_get(g,k)!=v for k,v in expected.items()):raise ValueError('Unexpected grid orientation')
        values=ec.codes_get_values(g)
        if len(values)!=360*181:raise ValueError('Unexpected grid size')
        out=[float(values[(90-lat)*360+lon]) for _,_,lat,lon,_ in LOCATIONS]
        if not np.isfinite(out).all() or any(abs(v)>1e8 for v in out):raise ValueError('Missing/invalid forecast value')
        return out
    finally:ec.codes_release(g)

def collect_one(day,cycle,target_day,refresh=False):
    refresh=refresh and pd.Timestamp(day,tz='UTC')>=pd.Timestamp.now(tz='UTC')-pd.Timedelta(days=14)
    lead=target_day*24-cycle
    cache=ROOT/'archive/forecast7_points'/f'{day}_{cycle:02}_d{target_day}.json'
    if not refresh and cache.exists():
        obj=json.loads(cache.read_text())
        if obj.get('parser_version')==1:return obj
    if cycle==0:
        obj=dict(midnight(day,lead,refresh),parser_version=1,cycle=0,target_day=target_day)
    else:
        if ACCESS_STOP.is_set():raise RuntimeError('Provider stopped after access/rate-limit response')
        url=f'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.{day}/{cycle:02}/atmos/gfs.t{cycle:02}z.pgrb2.1p00.f{lead:03}'
        try:idx,h=Fetcher(refresh=refresh).get(url+'.idx',max_age_hours=876000)
        except requests.RequestException as exc:
            if getattr(getattr(exc,'response',None),'status_code',None) in [401,403,429]:ACCESS_STOP.set()
            raise
        fields={};hashes=[h];modified=[];receipts=[]
        if not hasattr(TRANSPORT,'session'):TRANSPORT.session=requests.Session()
        for field,(a,b) in index_records(idx,day,cycle,lead).items():
            raw,receipt=range_get(url,a,b,refresh,session=TRANSPORT.session)
            native=field.removesuffix('_duplicate')
            vals=decode_points(raw,day,cycle,lead,native)
            if native in fields and vals!=fields[native]:raise ValueError('Conflicting coincident precipitation windows')
            fields[native]=vals;hashes.append(receipt['sha256']);receipts.append(receipt)
            if not receipt['source_last_modified']:raise ValueError('Missing source Last-Modified')
            modified.append(pd.Timestamp(receipt['source_last_modified']))
        if len(set(modified))!=1:raise ValueError('Source changed between range requests')
        obj={'parser_version':1,'day':day,'cycle':cycle,'target_day':target_day,'lead':lead,'fields':fields,'raw_sha256':hashes,'source_last_modified':modified[0].isoformat(),'receipts':receipts}
    atomic_bytes(cache,json.dumps(obj).encode());return obj

def same_target_revisions(part):
    """Only compare adjacent six-hour cycles with exactly identical targets/windows."""
    part=part.sort_values('initialization').copy()
    previous=part.shift()
    mask=(part.initialization-previous.initialization==pd.Timedelta(hours=6))&(part.valid_time==previous.valid_time)
    # Values represent the same variable/target window within a grouped series.
    out=part.loc[mask].copy()
    out['value']=part.value[mask]-previous.value[mask]
    out['previous_initialization']=previous.initialization[mask]
    out['source_object_last_modified']=pd.concat([part.source_object_last_modified,previous.source_object_last_modified],axis=1).max(axis=1)[mask]
    return out

def collect(refresh=False,start='2025-10-01',end=None):
    end=end or (pd.Timestamp.now(tz='UTC').floor('D')-pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    docs=[]
    for url in ['https://registry.opendata.aws/noaa-gfs-bdp-pds/','https://www.nco.ncep.noaa.gov/pmb/products/gfs/']:
        _,h=Fetcher(refresh=refresh).get(url);docs.append(h)
    dates=pd.date_range(start,end,freq='D').strftime('%Y%m%d');tasks=[(d,c,t) for d in dates for c in CYCLES for t in TARGET_DAYS]
    rows=[];hashes=set(docs);failures=[]
    def work(t):
        try:return collect_one(*t,refresh=refresh)
        except Exception as exc:return {'failure':dict(day=t[0],cycle=t[1],target_day=t[2],error=str(exc))}
    with ThreadPoolExecutor(max_workers=6) as pool:
        for n,obj in enumerate(pool.map(work,tasks),1):
            if 'failure' in obj:failures.append(obj['failure'])
            else:
                init=pd.Timestamp(obj['day'],tz='UTC')+pd.Timedelta(hours=obj['cycle']);lead=obj['lead'];hashes.update(obj['raw_sha256'])
                for i,(country,loc,lat,lon,description) in enumerate(LOCATIONS):
                    vals={f:v[i] for f,v in obj['fields'].items() if not f.startswith('wind_')}
                    vals['wind_speed_10m']=float(np.hypot(obj['fields']['wind_u_10m'][i],obj['fields']['wind_v_10m'][i]))
                    for field,value in vals.items():
                        rows.append(dict(location=loc,country=country,field=field,target_day=obj['target_day'],initialization=init,valid_time=init+pd.Timedelta(hours=lead),lead_hours=lead,window_start_hours=lead if field in ['temperature_2m','wind_speed_10m'] else lead-6,value=value,source_object_last_modified=pd.Timestamp(obj['source_last_modified'])))
            if n%120==0:print('GFS four-cycle files',n,'/',len(tasks),'failures',len(failures),flush=True)
    if not rows:raise ValueError('No usable GFS files')
    panel=pd.DataFrame(rows).sort_values(['location','field','target_day','initialization'])
    path=ROOT/'supplementary/gfs_intraday_issue_target.parquet'
    combined=panel
    if path.exists():combined=pd.concat([pd.read_parquet(path),panel]).drop_duplicates(['location','field','target_day','initialization'],keep='last').sort_values(['location','field','target_day','initialization'])
    buf=io.BytesIO();combined.to_parquet(buf,index=False,compression='zstd');atomic_bytes(path,buf.getvalue())
    manifest=archive_raw_manifest(hashes,description='NOAA four operational GFS cycles: 1-degree point forecasts for identical 00UTC targets and their same-target changes')
    existing=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','batch']);pinned=set(existing.loc[existing.batch==9,'series_id']);older=set(existing.loc[existing.batch<9,'series_id'])
    good=[];omissions=[]
    for (loc,field,target),part in panel.groupby(['location','field','target_day']):
        country,_,lat,lon,description=next(x for x in LOCATIONS if x[1]==loc)
        unit='m s-1' if field=='wind_speed_10m' else FIELDS[field][3]
        for kind,data in [('forecast',part),('revision',same_target_revisions(part))]:
            sid=f'gfs7_{country.lower()}_{loc}_{field}_targetd{target}_{kind}'
            if sid in older:raise ValueError('Existing series collision '+sid)
            if pinned and sid not in pinned:continue
            if data.value.nunique()<2:omissions.append(dict(series_id=sid,reason='constant'));continue
            try:
                meta=save_candidate(GROUP,sid,data.rename(columns={'initialization':'date'})[['date','value']],title=f'GFS {description}: {field.replace("_"," ")}, day-{target} 00 UTC target {kind}',provider='NOAA GFS archived operational forecasts',category='forecast_weather_revisions' if kind=='revision' else 'forecast_weather_expectations',unit=unit,frequency='irregular_intraday' if kind=='revision' else '6-hourly',source_url='https://registry.opendata.aws/noaa-gfs-bdp-pds/',geography=description,country_codes=[country],country_code_standard='ISO 3166-1 alpha-2',geography_scope='non_us',dataset_identity=f'noaa_gfs|1p00|allcycles|{lat}|{lon}|{field}|day{target}_00UTC|{kind}',hypothesis='Changes to weather expectations may precede electricity, crop, transport and renewable-generation repricing; economic value is a research hypothesis, not validated alpha.',limitations='Operational forecasts, not observations or market consensus. Exact selected one-degree grid cells, no production/population weighting. Target is 00 UTC at day offset from initialization calendar date; lead shortens through each day. Revisions compare adjacent cycles only within an identical target and six-hour accumulation window; omit midnight target rollover and missing predecessor cycles. Initialization precedes publication; Last-Modified is an archive clock, not verified first release. Derived revisions and old once-daily samples are not independent sources.',native_frequency='Four model cycles daily at 00,06,12,18 UTC; revisions at 06,12,18 UTC when both cycles exist',finer_frequency_available=False,expected_publication_lag='Hours after initialization; per-file archive Last-Modified retained in companion panel, not verified first-public-release timestamp.',timestamp_semantics='Model initialization UTC; revision timestamp is later component initialization. Target time and archive modification clock in companion. Not historical tradable availability.',raw_sha256=docs,raw_manifest_sha256=[manifest],license='NOAA NODD public data; attribution NOAA/NCEP.',point_in_time=False,source_metric=field,forecast_target_offset_days=int(target),forecast_lead_hours_by_cycle={str(c):int(target*24-c) for c in CYCLES},forecast_model='GFS operational',grid_latitude=float(lat),grid_longitude=float(lon if lon<=180 else lon-360),derived=kind=='revision',supplemental_panel='supplementary/gfs_intraday_issue_target.parquet',related_series_ids=[x for x in [f'gfs6_{country.lower()}_{loc}_{field}_lead{target*24:03}'] if x in older],revision_policy='Original operational cycle bytes retained; each cycle is a distinct issue. Same-target changes are deterministic differences, no cross-midnight rolling target comparison.')
                good.append(meta)
            except ValueError as exc:
                if 'insufficient genuine' not in str(exc):raise
                omissions.append(dict(series_id=sid,reason=str(exc)))
    report=dict(series=len(good),observations=sum(m['rows'] for m in good),forecast_files_requested=len(tasks),files_failed=failures,omissions=omissions,series_ids=[m['series_id'] for m in good],finished_at=pd.Timestamp.now(tz='UTC').isoformat())
    atomic_bytes(ROOT/'logs/forecast7_report.json',json.dumps(report,indent=2).encode());return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--refresh',action='store_true');p.add_argument('--start',default='2025-10-01');p.add_argument('--end');a=p.parse_args()
    r=collect(a.refresh,a.start,a.end);print(json.dumps({k:v for k,v in r.items() if k!='series_ids'},indent=2));raise SystemExit(bool(r['files_failed']))
