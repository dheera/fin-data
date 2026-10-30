#!/usr/bin/env python3
"""OpenAQ fixed-site measured pollution histories with original source attribution.

API credentials are used only for API discovery and are never logged or in URLs.
Historical bytes come from the public S3 archive; no recurring task is installed.
"""
from __future__ import annotations
import argparse, gzip, hashlib, io, json, math, re, time, threading
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import requests
from expansion_common import ROOT, atomic_bytes, archive_raw_manifest, normalize_frame, coverage, qualifies_research, save_candidate

GROUP='openaq10'
API='https://api.openaq.org/v3/'
S3='https://openaq-data-archive.s3.amazonaws.com/'
COUNTRIES=['ZA','GH','UG','KE','RW','ET','NG','ID','PH','VN','TH','MY','MN','NP','BD','PK','LK','KZ','UZ','CL','CO','BR','PE','EC','AR','BO','MX']
POLLUTANTS={'pm25','pm10','no2','so2','co','o3'}

class SafeFetch:
    def __init__(self, root=ROOT):
        self.root=Path(root);self.api_last=0.;self.api_count=0;self._local=threading.local()
        self.key=json.loads(Path('/data/fin/api_keys/openaq.json').read_text())['api_key']
    def request(self,*args,**kwargs):
        if not hasattr(self._local,'session'):self._local.session=requests.Session()
        return self._local.session.get(*args,**kwargs)
    def get(self,url,params=None,*,max_age_hours=24*365,refresh=False,source_last_modified=None):
        parsed=urlparse(url);auth=parsed.hostname=='api.openaq.org'
        if parsed.scheme!='https' or parsed.hostname not in {'api.openaq.org','openaq-data-archive.s3.amazonaws.com','docs.openaq.org'}:
            raise ValueError('Unapproved OpenAQ download host')
        prepared=requests.Request('GET',url,params=params).prepare().url
        if self.key in prepared:raise ValueError('Credential must not appear in URL')
        index=self.root/'archive/requests'/(hashlib.sha256(prepared.encode()).hexdigest()+'.json')
        if index.exists() and not refresh:
            meta=json.loads(index.read_text());p=self.root/'archive/raw'/(meta['sha256']+'.bin')
            age=(pd.Timestamp.now(tz='UTC')-pd.Timestamp(meta['retrieved_at'])).total_seconds()/3600
            unchanged = source_last_modified is None or (meta.get('source_last_modified') is not None and pd.Timestamp(meta['source_last_modified']) == pd.Timestamp(source_last_modified))
            if p.exists() and 0<=age<max_age_hours and unchanged:
                b=p.read_bytes()
                if hashlib.sha256(b).hexdigest()==meta['sha256']:return b,meta['sha256']
        headers={'User-Agent':'FinAltDataResearch/1.0 (public time-series research)'}
        if auth:
            if self.api_count>=1900:raise RuntimeError('OpenAQ per-run API request cap reached')
            wait=max(0.,1.9-(time.monotonic()-self.api_last))
            if wait:time.sleep(wait)
            headers['X-API-Key']=self.key;self.api_last=time.monotonic();self.api_count+=1
        try:r=self.request(url,params=params,headers=headers,timeout=(15,120),allow_redirects=False)
        except requests.RequestException:raise RuntimeError('OpenAQ network request failed; details withheld') from None
        if r.status_code!=200:raise RuntimeError('OpenAQ request HTTP '+str(r.status_code))
        b=r.content
        if self.key.encode() in b:raise RuntimeError('Refused response containing credential')
        h=hashlib.sha256(b).hexdigest()
        meta={'url':prepared,'sha256':h,'retrieved_at':pd.Timestamp.now(tz='UTC').isoformat(),'content_type':r.headers.get('Content-Type'),'source_last_modified':r.headers.get('Last-Modified'),'collector_group':GROUP}
        atomic_bytes(self.root/'archive/raw'/(h+'.bin'),b)
        receipt=index.stem+'_'+pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')+'.json'
        atomic_bytes(self.root/'archive/receipts'/receipt,json.dumps(meta,indent=2).encode())
        atomic_bytes(index,json.dumps(meta,indent=2).encode());return b,h

def discover(fetch):
    discovered={};hashes={}
    for code in COUNTRIES:
        raw,h=fetch.get(API+'locations',{'iso':code,'monitor':'true','mobile':'false','limit':1000,'page':1},max_age_hours=24)
        doc=json.loads(raw);discovered[code]=doc;hashes[code]=h
        doc['discovery_scope']={'limit':1000,'page':1,'truncated':isinstance(doc['meta'].get('found'),int) and doc['meta']['found']>1000,'description':'Bounded first-page reference-monitor discovery, not a census of national stations.'}
        rows=doc['results'];summ=[]
        for x in rows:
            ps=sorted({s['parameter']['name'] for s in x['sensors']} & POLLUTANTS)
            if not ps:continue
            summ.append({'id':x['id'],'name':x['name'],'pollutants':ps,'first':(x.get('datetimeFirst') or {}).get('utc'),'last':(x.get('datetimeLast') or {}).get('utc'),'provider':x.get('provider'),'licenses':x.get('licenses')})
        print(code,'found',doc['meta'].get('found'),'eligible',len(summ),json.dumps(sorted(summ,key=lambda x:(len(x['pollutants']),x['last'] or ''),reverse=True)[:3],ensure_ascii=False),flush=True)
    atomic_bytes(ROOT/'logs/openaq10_discovery.json',json.dumps({'countries':discovered,'raw_sha256':hashes},indent=2).encode())
    raw,h=fetch.get(API+'licenses',{'limit':1000});atomic_bytes(ROOT/'logs/openaq10_licenses.json',json.dumps({'response':json.loads(raw),'sha256':h},indent=2).encode())


def list_location(fetch, location, years, *, refresh=False):
    parts=[];hashes=[];ns={'s':'http://s3.amazonaws.com/doc/2006-03-01/'}
    for year in years:
        raw,h=fetch.get(S3,{'list-type':2,'prefix':f'records/csv.gz/locationid={location}/year={year}/','max-keys':1000},max_age_hours=24,refresh=refresh)
        tree=ET.fromstring(raw);hashes.append(h)
        if tree.findtext('s:IsTruncated',namespaces=ns)!='false':raise ValueError('Unexpected truncated annual S3 listing')
        for item in tree.findall('s:Contents',ns):
            key=item.findtext('s:Key',namespaces=ns)
            m=re.fullmatch(r'records/csv.gz/locationid='+str(location)+r'/year=\d{4}/month=\d{2}/location-'+str(location)+r'-(\d{8})\.csv\.gz',key)
            if not m:continue
            parts.append({'key':key,'day':pd.Timestamp(m[1]).date().isoformat(),'source_last_modified':item.findtext('s:LastModified',namespaces=ns),'bytes':int(item.findtext('s:Size',namespaces=ns))})
    return sorted(parts,key=lambda x:x['day']),hashes

def select_window(parts):
    """Latest reasonably continuous historical segment, at most two years.

    A source shutdown is a gap, never padded; a short restart does not displace a
    useful older history. Last source date and omitted recent segment are explicit.
    """
    groups=[]
    for p in parts:
        if not groups or (pd.Timestamp(p['day'])-pd.Timestamp(groups[-1][-1]['day'])).days>120:groups.append([])
        groups[-1].append(p)
    for group in reversed(groups):
        cutoff=pd.Timestamp(group[-1]['day'])-pd.Timedelta(days=730)
        window=[p for p in group if pd.Timestamp(p['day'])>=cutoff]
        if len(window)>=120:return window
    return []

def choose_locations(fetch):
    discovery=json.loads((ROOT/'logs/openaq10_discovery.json').read_text());licenses=json.loads((ROOT/'logs/openaq10_licenses.json').read_text())
    licensemap={x['id']:x for x in licenses['response']['results']}
    wanted=['ZA','GH','UG','RW','ET','NG','ID','PH','VN','TH','MN','NP','BD','PK','LK','KZ','UZ','CL','BR','PE','AR','MX']
    locations=[];excluded=[]
    for code in wanted:
        candidates=[]
        for x in discovery['countries'][code]['results']:
            ps={s['parameter']['name'] for s in x['sensors']} & POLLUTANTS
            if not ps or not x.get('datetimeLast') or not x.get('datetimeFirst'):continue
            if x['isMobile'] or not x['isMonitor']:continue
            if any(licensemap.get(y['id'],{}).get('commercialUseAllowed') is False for y in x.get('licenses') or []):continue
            candidates.append(x)
        candidates.sort(key=lambda x:(x['datetimeLast']['utc']>='2025-01-01',len({s['parameter']['name'] for s in x['sensors']} & POLLUTANTS),x['datetimeLast']['utc']),reverse=True)
        if not candidates:excluded.append({'country':code,'reason':'No eligible reference fixed monitor'});continue
        x=candidates[0];parts,hashes=list_location(fetch,x['id'],[2023,2024,2025,2026]);window=select_window(parts)
        if not window:excluded.append({'country':code,'location_id':x['id'],'reason':'Fewer than 120 archive daily files in any recent continuous segment'});continue
        locations.append({'location':x,'partitions':window,'listing_raw_sha256':hashes,'discovery_raw_sha256':discovery['raw_sha256'][code],'available_archive_start':parts[0]['day'],'available_archive_end':parts[-1]['day'],'selected_archive_start':window[0]['day'],'selected_archive_end':window[-1]['day'],'license_details':[licensemap.get(y['id'],y) for y in x.get('licenses') or []]})
        print('SELECT',code,x['id'],x['name'],len(window),window[0]['day'],window[-1]['day'],flush=True)
    doc={'locations':locations,'exclusions':excluded,'license_raw_sha256':licenses['sha256'],'method':'One fixed reference-monitor site per candidate country; rank recent activity then pollutant diversity. Latest >=120-file segment separated by no gap >120 days, capped at 731 local dates.'}
    atomic_bytes(ROOT/'logs/openaq10_selection.json',json.dumps(doc,indent=2).encode());return doc

def fetch_flags(fetch,resource,identifier,*,refresh=False):
    results=[];hashes=[];page=1;seen=set()
    while True:
        raw,h=fetch.get(API+f'{resource}/{identifier}/flags',{'limit':1000,'page':page},max_age_hours=24,refresh=refresh);obj=json.loads(raw);hashes.append(h)
        new=0
        for row in obj['results']:
            marker=json.dumps(row,sort_keys=True)
            if marker not in seen:seen.add(marker);results.append(row);new+=1
        found=obj.get('meta',{}).get('found')
        # Some current API flags responses return the complete set despite a
        # smaller requested limit. Count unique source records against found.
        if isinstance(found,int) and len(results)>=found:break
        if not isinstance(found,int) and len(obj['results'])<1000:break
        if isinstance(found,int) and not obj['results'] and len(results)<found:raise ValueError('Incomplete declared flag count')
        if new==0:raise ValueError('Flag pagination repeated without complete declared count')
        if page>=20:raise ValueError('Flag pagination exceeds bounded cap')
        page+=1
    return results,hashes

def location_flags(fetch,location,*,refresh=False):
    return fetch_flags(fetch,'locations',location,refresh=refresh)

def sensor_flags(fetch,sensor,*,refresh=False):
    return fetch_flags(fetch,'sensors',sensor,refresh=refresh)

def refreshed_partitions(chosen,parts):
    # Include newly backfilled dates inside old gaps, plus new dates, while
    # preserving the selected start and leaving genuinely missing dates absent.
    return [p for p in parts if p['day']>=chosen['selected_archive_start']]

def error_mask(dates,sensor_id,flags):
    invalid=pd.Series(False,index=dates.index)
    for flag in flags:
        if flag['flagType'].get('level')!='ERROR':continue
        ids=flag.get('sensorIds') or []
        if ids and sensor_id not in ids:continue
        invalid|=dates.between(pd.Timestamp(flag['datetimeFrom']['utc']),pd.Timestamp(flag['datetimeTo']['utc']),inclusive='both')
    return invalid

def retracted_dates(previous,sensor,flags):
    # Missing archive rows are not a withdrawal. Only explicit source ERROR
    # intervals from this sensor's endpoint retract already published dates.
    return [d.isoformat() for d in previous.loc[error_mask(previous.date,sensor['id'],flags),'date']]

def parse_archive(raw, location_id):
    frame=pd.read_csv(io.BytesIO(gzip.decompress(raw)),float_precision='round_trip')
    if frame.empty:return frame
    required={'location_id','sensors_id','datetime','parameter','units','value'}
    if not required.issubset(frame.columns):raise ValueError('OpenAQ archive schema mismatch')
    if not frame.location_id.eq(location_id).all():raise ValueError('Archive location mismatch')
    if not frame['datetime'].str.contains(r'(?:Z|[+-]\d{2}:\d{2})$',regex=True).all():raise ValueError('Measurement timestamp has no explicit timezone offset')
    frame['date']=pd.to_datetime(frame['datetime'],utc=True,format='ISO8601',errors='raise')
    frame['value']=pd.to_numeric(frame.value,errors='coerce').astype(float)
    return frame

def clean_sensor(frame,sensor,flags):
    """Keep explicit finite nonnegative native measurements; never impute or average."""
    s=frame[frame.sensors_id.eq(sensor['id'])].copy();stats={'source_rows':len(s)}
    if s.empty:return pd.DataFrame(columns=['date','value']),stats
    param=sensor['parameter']
    if set(s.parameter)!={param['name']} or set(s.units)!={param['units']}:raise ValueError('Sensor parameter/unit changed')
    bad=~np.isfinite(s.value)|s.value.lt(0);stats['nonfinite_or_negative_omitted']=int(bad.sum());s=s[~bad]
    # The caller supplies only this sensor endpoint's full flag history.
    invalid=error_mask(s.date,sensor['id'],flags)
    stats['error_flag_interval_omitted']=int(invalid.sum());s=s[~invalid]
    conflicts=s.groupby('date').value.nunique();conflict_dates=conflicts[conflicts.gt(1)].index
    stats['conflicting_timestamp_omitted']=len(conflict_dates);s=s[~s.date.isin(conflict_dates)]
    stats['identical_duplicates_removed']=int(s.duplicated(['date','value']).sum())
    s=s.drop_duplicates('date')[['date','value']]
    if s.empty:return s,stats
    return normalize_frame(s),stats

def infer_frequency(frame):
    seconds=float(frame.date.diff().dt.total_seconds().median())
    for n,name in [(60,'1min'),(300,'5min'),(600,'10min'),(900,'15min'),(1800,'30min'),(3600,'hourly')]:
        if abs(seconds-n)<1:return name
    if 0<seconds<3600:return 'subhourly'
    if 3600<seconds<86400:return 'irregular_intraday'
    return 'daily' if seconds==86400 else 'ineligible'

def validate_quality_revision(previous,sensor,flags,incoming):
    withdrawn=retracted_dates(previous,sensor,flags)
    eligible=(not incoming.empty and incoming.value.nunique()>1 and
              qualifies_research({'frequency':infer_frequency(incoming),**coverage(incoming)}))
    if withdrawn and not eligible:
        raise ValueError('Explicit source ERROR withdrawal leaves insufficient publishable history; refresh failed and requires series retirement support')
    return withdrawn

def collect(fetch,workers=6,refresh=False):
    path=ROOT/'logs/openaq10_selection.json'
    selection=json.loads(path.read_text()) if path.exists() else choose_locations(fetch)
    published=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','dataset_identity','content_sha256','batch','path'])
    own=published[published.series_id.str.startswith('openaq10_')];pinned=set(own.series_id)
    baseline=published[~published.series_id.isin(pinned)];seen={};metas=[];excluded=list(selection['exclusions']);failures=[]
    docs=[]
    for url in ['https://docs.openaq.org/aws/about','https://docs.openaq.org/resources/measurements','https://docs.openaq.org/using-the-api/rate-limits','https://docs.openaq.org/about/terms','https://docs.openaq.org/api/operations/sensor_flags_get_v3_sensors__sensor_id__flags_get']:
        _,h=fetch.get(url);docs.append(h)
    for chosen in selection['locations']:
        loc=chosen['location'];lid=loc['id'];code=loc['country']['code']
        if pinned and not any(s.startswith(f'openaq10_{code.lower()}_{lid}_') for s in pinned):continue
        if pinned or refresh:
            parts,hashes=list_location(fetch,lid,range(2023,pd.Timestamp.now(tz='UTC').year+1),refresh=refresh)
            # Re-list ordinary published refreshes; append new source dates and
            # retain the initial history start, rather than silently truncate it.
            chosen['partitions']=refreshed_partitions(chosen,parts)
            chosen['listing_raw_sha256']=hashes
            if parts:chosen['available_archive_end']=parts[-1]['day']
        flags_by_sensor={};flaghashes=[]
        for sensor in loc['sensors']:
            if sensor['parameter']['name'] not in POLLUTANTS:continue
            series_id=f'openaq10_{code.lower()}_{lid}_{sensor["id"]}_{sensor["parameter"]["name"]}'
            if pinned and series_id not in pinned:continue
            try:
                flags,hashes=sensor_flags(fetch,sensor['id'],refresh=refresh)
                if any(row['locationId']!=lid for row in flags):raise ValueError('Sensor flags location mismatch')
            except (ValueError,RuntimeError) as exc:
                if str(exc) in {'OpenAQ request HTTP 429','OpenAQ request HTTP 401','OpenAQ request HTTP 403'}:raise
                excluded.append({'series_id':series_id,'reason':'Complete sensor quality flags unavailable: '+str(exc)});print('OMIT',series_id,'quality flags unavailable',flush=True);continue
            flags_by_sensor[str(sensor['id'])]={'flags':flags,'raw_sha256':hashes};flaghashes.extend(hashes)
        chosen['sensor_flags']=flags_by_sensor
        def one(part):
            raw,h=fetch.get(S3+part['key'],refresh=refresh,source_last_modified=part['source_last_modified']);p=dict(part,sha256=h)
            try:frame=parse_archive(raw,lid)
            except ValueError as exc:
                if str(exc)!='Archive location mismatch':raise
                p['excluded_reason']='Archive body location_id differs from partition location; whole file omitted'
                frame=pd.DataFrame()
            return p,frame
        parts=[];frames=[]
        with ThreadPoolExecutor(workers) as pool:
            for i,(part,f) in enumerate(pool.map(one,chosen['partitions'])):
                parts.append(part)
                if part.get('excluded_reason'):excluded.append({'location_id':lid,'source_key':part['key'],'reason':part['excluded_reason']})
                if not f.empty:frames.append(f)
                if i%150==0:print('DOWNLOAD',code,lid,i,len(chosen['partitions']),flush=True)
        chosen['partitions']=parts
        atomic_bytes(ROOT/f'logs/openaq10_location_{lid}.json',json.dumps(chosen,indent=2).encode())
        if not frames:excluded.append({'location_id':lid,'reason':'Empty archived measurements'});continue
        frame=pd.concat(frames,ignore_index=True)
        rawhashes=[p['sha256'] for p in parts]+chosen['listing_raw_sha256']+flaghashes+[chosen['discovery_raw_sha256'],selection['license_raw_sha256']]+docs
        manifest=archive_raw_manifest(rawhashes,description=f'OpenAQ original CSV-gzip measurements, archive listings, location metadata, error flags, licenses and documentation for location {lid}')
        for sensor in loc['sensors']:
            if sensor['parameter']['name'] not in POLLUTANTS:continue
            sid=f'openaq10_{code.lower()}_{lid}_{sensor["id"]}_{sensor["parameter"]["name"]}'
            if pinned and sid not in pinned:continue
            if str(sensor['id']) not in flags_by_sensor:continue
            flags=flags_by_sensor[str(sensor['id'])]['flags']
            try:f,stats=clean_sensor(frame,sensor,flags)
            except ValueError as exc:excluded.append({'series_id':sid,'reason':str(exc)});continue
            withdrawn=[]
            if sid in pinned:
                oldpath=ROOT/own.set_index('series_id').loc[sid,'path'];withdrawn=validate_quality_revision(pd.read_parquet(oldpath,columns=['date']),sensor,flags,f)
            if f.empty:excluded.append({'series_id':sid,'reason':'No finite nonnegative unflagged measurements'});continue
            frequency=infer_frequency(f);cov=coverage(f)
            if f.value.nunique()<2 or not qualifies_research({'frequency':frequency,**cov}):excluded.append({'series_id':sid,'reason':'Insufficient nonconstant native daily-or-finer history','coverage':cov,'frequency':frequency,'quality_filter_counts':stats});continue
            identity=f'openaq:v3:location:{lid}:sensor:{sensor["id"]}:native_measurements'
            h=hashlib.sha256(f.date.astype('int64').to_numpy().tobytes()+f.value.to_numpy().tobytes()).hexdigest()
            if sid in set(baseline.series_id) or identity in set(baseline.dataset_identity.dropna()) or h in set(baseline.content_sha256.dropna()) or h in seen:excluded.append({'series_id':sid,'reason':'Previously published or duplicate numeric identity'});continue
            seen[h]=sid
            m=save_candidate(GROUP,sid,f,title=f'{loc["country"]["name"]}: {loc["name"]} measured {sensor["parameter"]["name"].upper()}',provider='OpenAQ public measurement archive',category='international_measured_air_pollution',unit=sensor['parameter']['units'],frequency=frequency,source_url=f'https://explore.openaq.org/locations/{lid}',geography=f'{loc["name"]}, {loc.get("locality") or ""}, {loc["country"]["name"]}'.replace(', ,',','),hypothesis='Pollution deviations can proxy local combustion, industry, traffic, smoke, dust and weather-related disruption; condition on weather, station operations, regulation and seasonal patterns before testing market effects.',limitations='Single fixed reference-monitor site, not a national exposure or production index. Original reported measurements, not necessarily validated final regulatory data. No imputation; negative/nonfinite values and sensor-specific ERROR-flag intervals omitted. Source unit retained without mass/molar conversion. Upstream and archive gaps, relocations, outages and revisions can confound activity signals. Native observation timestamps are not historical public availability. Some series ended before the current date; inspect end and max_gap_days.',raw_sha256=[manifest],raw_manifest_sha256=[manifest],country_codes=[code],country_code_standard='ISO 3166-1 alpha-2',geography_scope='non_us',native_frequency=frequency,native_frequency_basis='Median retained original measurement interval; no resampling or aggregation.',expected_publication_lag='S3 daily files written 72 hours after local day end per OpenAQ; observation-to-archive delay normally 72–96 hours plus upstream delay. Historical files can be patched later; exact first-publication history unknown. API recent measurements can arrive sooner but are not the source of this backfill.',publication_lag_days_estimate=4.0,publication_lag_basis='OpenAQ documented 72 hours after local day end; conservative 4-day estimate, not verified per-observation publication time.',timestamp_semantics='Original provider measurement timestamp with explicit local offset converted to UTC. Measurement/averaging-period label; whether start/end is provider-specific and is not assumed. Not publication or collection time.',source_timezone=loc['timezone'],dataset_identity=identity,point_in_time=False,revision_policy='OpenAQ archive may retroactively patch missing data or historical scrapes. Original fetched bytes and local collection times retained; no original-release reconstruction.',finer_frequency_available=frequency != 'daily',source_location=loc,source_sensor=sensor,source_coordinates=loc['coordinates'],source_provider=loc['provider'],source_owner=loc['owner'],source_instruments=loc['instruments'],license=chosen['license_details'] or 'License not specified in OpenAQ location metadata; original provider attribution retained; no claim of unrestricted redistribution.',source_attribution=f'OpenAQ; {loc["provider"]["name"]}; {loc["owner"]["name"]}',quality_filter_counts=stats,quality_flags_scope='Complete sensor-specific endpoint flags; ERROR ranges applied to that sensor only, inclusive source range endpoints. Warning flags retained in source log but not removed. Historical flags are current retrospective QA, not as-of release flags.',quality_control_point_in_time=False,retracted_dates=withdrawn,retraction_policy='On refresh, only previously published dates inside current explicit sensor-specific ERROR intervals are retracted. Ordinary absent source rows or missing files do not retract earlier values.',source_partitions_file=f'logs/openaq10_location_{lid}.json',selected_archive_start=chosen['partitions'][0]['day'],selected_archive_end=chosen['partitions'][-1]['day'],available_archive_end=chosen['available_archive_end'])
            metas.append(m);print('OK',sid,len(f),frequency,cov['start'],cov['end'],flush=True)
    files=[]
    for m in metas:
        p=ROOT/'archive/staging'/GROUP/(m['series_id']+'.parquet');j=p.with_suffix('.json');files.append({'series_id':m['series_id'],'path':str(p.relative_to(ROOT)),'parquet_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'metadata_sha256':hashlib.sha256(j.read_bytes()).hexdigest()})
    inventory={'group':GROUP,'created_at':pd.Timestamp.now(tz='UTC').isoformat(),'series_count':len(metas),'observation_count':sum(m['rows'] for m in metas),'country_counts':pd.Series([m['country_codes'][0] for m in metas]).value_counts().to_dict(),'frequency_counts':pd.Series([m['frequency'] for m in metas]).value_counts().to_dict(),'files':files,'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'exclusions':excluded,'errors':failures,'api_requests_this_run':fetch.api_count,'discovery_scope':'Bounded first 1000 reference fixed sites per country, not a national census','discovery_truncated_countries':[code for code,doc in json.loads((ROOT/'logs/openaq10_discovery.json').read_text())['countries'].items() if isinstance(doc['meta'].get('found'),int) and doc['meta']['found']>1000]}
    atomic_bytes(ROOT/'logs/openaq10_inventory.json',json.dumps(inventory,indent=2).encode());print('COMPLETE',len(metas),inventory['observation_count'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--discover',action='store_true');p.add_argument('--select',action='store_true');p.add_argument('--refresh',action='store_true');p.add_argument('--workers',type=int,default=6);args=p.parse_args();fetch=SafeFetch()
    if args.discover:discover(fetch)
    elif args.select:choose_locations(fetch)
    else:collect(fetch,args.workers,args.refresh)
