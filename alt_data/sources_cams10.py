#!/usr/bin/env python3
"""Archived operational CAMS forecasts, with initialization and target clocks kept separate.

Credentials: /data/fin/api_keys/copernicus_ads.json (url,key). The user must manually
accept the dataset licence. This collector never accepts licences or schedules jobs.
"""
from __future__ import annotations
import argparse, hashlib, io, json, re, time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import numpy as np
import pandas as pd
import requests
from expansion_common import ROOT, Fetcher, atomic_bytes, archive_raw_manifest, save_candidate, normalize_frame
from sources_expectations6 import ec

GROUP = 'cams10'
API = 'https://ads.atmosphere.copernicus.eu/api'
DATASET = 'cams-global-atmospheric-composition-forecasts'
SOURCE = 'https://ads.atmosphere.copernicus.eu/datasets/' + DATASET
DOC = 'https://confluence.ecmwf.int/spaces/CKB/pages/212454117/CAMS+Global+atmospheric+composition+forecast+data+documentation'
LEADS = (0, 24, 72)
DEFAULT_START = '2025-10-01'
REFRESH_OVERLAP_DAYS = 7
FIELDS = {
    'pm25': ('particulate_matter_2.5um', 210073, 'kg m**-3', 'kg m-3'),
    'dust_aod550': ('dust_aerosol_optical_depth_550nm', 210209, '~', 'dimensionless'),
    'total_column_co': ('total_column_carbon_monoxide', 210127, 'kg m**-2', 'kg m-2'),
    'total_column_no2': ('total_column_nitrogen_dioxide', 210125, 'kg m**-2', 'kg m-2'),
}
# Exact 0.4-degree ADS grid points close to these economic regions. These are
# unweighted individual cells, not city averages or measured industrial emissions.
LOCATIONS = [
    ('CN','shanghai',31.2,121.6,'Shanghai industrial and port region'),
    ('IN','ahmedabad',23.2,72.4,'Ahmedabad manufacturing region'),
    ('ID','jakarta',-6.0,106.8,'Jakarta industrial and port region'),
    ('VN','haiphong',20.8,106.8,'Haiphong industrial and port region'),
    ('BR','sao_paulo',-23.6,-46.8,'Sao Paulo manufacturing region'),
    ('ZA','richards_bay',-28.8,32.0,'Richards Bay coal export and industrial region'),
    ('TR','istanbul',41.2,28.8,'Istanbul manufacturing and shipping region'),
    ('SA','jubail',27.2,49.6,'Jubail petrochemical and industrial region'),
]

def digest(raw): return hashlib.sha256(raw).hexdigest()

def series_id(country, location, field, lead):
    return f'cams10_{country.lower()}_{location}_{field}_lead{int(lead):03}'

def refresh_selection(catalog, location_ids=None):
    """Freeze a published group's membership, while permitting its own values to update."""
    owned=catalog.series_id.str.startswith('cams10_')
    pinned=set(catalog.loc[owned,'series_id'])
    baseline=catalog.loc[~owned].copy()
    all_locations={x[1] for x in LOCATIONS}
    allowed={loc for country,loc,*_ in LOCATIONS
             if not pinned or any(series_id(country,loc,f,h) in pinned for f in FIELDS for h in LEADS)}
    requested=set(location_ids) if location_ids is not None else allowed
    if requested-all_locations:raise ValueError('Unknown CAMS location selection')
    if requested-allowed:raise ValueError('Location is outside the published CAMS selection')
    locations=[x for x in LOCATIONS if x[1] in requested]
    if not locations:raise ValueError('No selected locations')
    return pinned,baseline,locations

def merge_companions(previous, current):
    """Replace returned keys, preserving every unaffected location and historical key."""
    keys=['country','location','field','lead_hours','initialization']
    for frame in (previous,current):
        if not frame.empty and frame.duplicated(keys).any():raise ValueError('Duplicate CAMS companion key')
    frames=[f.copy() for f in (previous,current) if not f.empty]
    if not frames:raise ValueError('No CAMS companion observations')
    for frame in frames:
        for column in ('initialization','valid_time','retrieved_at'):
            frame[column]=pd.to_datetime(frame[column],utc=True).astype('datetime64[ns, UTC]')
    return pd.concat(frames,ignore_index=True).sort_values('retrieved_at',kind='stable').drop_duplicates(keys,keep='last').sort_values(keys).reset_index(drop=True)

def window_start(location, pinned, previous, end, explicit_start=None):
    """Refresh a short overlapping window once all selected histories have a base."""
    if explicit_start is not None:return explicit_start
    if previous.empty:return DEFAULT_START
    country,loc,*_=location
    wanted={(f,h) for f in FIELDS for h in LEADS
            if not pinned or series_id(country,loc,f,h) in pinned}
    history=previous[previous.location.eq(loc)]
    latest=history.groupby(['field','lead_hours']).initialization.max().to_dict()
    if wanted-set(latest):return DEFAULT_START
    anchor=min(pd.Timestamp(end,tz='UTC'),min(pd.Timestamp(latest[k]) for k in wanted).floor('D'))
    return max(pd.Timestamp(DEFAULT_START,tz='UTC'),anchor-pd.Timedelta(days=REFRESH_OVERLAP_DAYS)).strftime('%Y-%m-%d')

def sanitized(obj, secret=''):
    """Keep request evidence without credentials or signed download URLs."""
    if isinstance(obj, dict):
        return {k: ('[redacted]' if re.search('token|password|authorization|secret', k, re.I)
                    else sanitized(v, secret)) for k,v in obj.items()}
    if isinstance(obj, list): return [sanitized(v, secret) for v in obj]
    if isinstance(obj, str):
        if secret: obj = obj.replace(secret, '[redacted]')
        return re.sub(r'https?://[^\s<>\"]+', lambda m: urlunsplit((*urlsplit(m[0])[:3], '', '')), obj)
    return obj

def request_for(location, start, end):
    _,_,lat,lon,_ = location
    return dict(date=f'{start}/{end}', time=['00:00','12:00'], leadtime_hour=[str(h) for h in LEADS],
                type=['forecast'], variable=[f[0] for f in FIELDS.values()], data_format='grib',
                area=[round(lat+.4,1),round(lon-.4,1),round(lat-.4,1),round(lon+.4,1)])

def selected_requests(location, start, end, pinned):
    request=request_for(location,start,end)
    if not pinned:return [request]
    grouped={}
    for field in FIELDS:
        leads=tuple(h for h in LEADS if series_id(location[0],location[1],field,h) in pinned)
        if leads:grouped.setdefault(leads,[]).append(FIELDS[field][0])
    return [{**request,'variable':variables,'leadtime_hour':[str(h) for h in leads]}
            for leads,variables in grouped.items()]

def begin_refresh_intent(tasks):
    """A retry resumes completed regions from the same explicit refresh batch."""
    selection=[{'location':loc[1],'request':req} for loc,req in tasks]
    h=digest(json.dumps(selection,sort_keys=True).encode())
    path=ROOT/'archive/cams10_refresh_intents'/(h+'.json')
    state=json.loads(path.read_text()) if path.exists() else {}
    if state.get('status')!='running':
        state={'status':'running','selection':selection,'started_at':pd.Timestamp.now(tz='UTC').isoformat(),'completed':{}}
        atomic_bytes(path,json.dumps(state,indent=2).encode())
    return path,state

def retrieve_with_intent(client,request,max_wait_seconds,intent=None):
    if intent is None:return client.retrieve(request,max_wait_seconds,refresh=False)
    path,state=intent;h=digest(json.dumps(request,sort_keys=True).encode())
    if h in state['completed']:
        receipt=state['completed'][h];raw=(ROOT/'archive/raw'/(receipt['raw_sha256']+'.bin')).read_bytes()
        if digest(raw)!=receipt['raw_sha256']:raise ValueError('Refresh intent source checksum differs')
        return raw,receipt
    raw,receipt=client.retrieve(request,max_wait_seconds,refresh=True)
    state['completed'][h]=receipt;atomic_bytes(path,json.dumps(state,indent=2).encode())
    return raw,receipt

class ADSAccessError(RuntimeError): pass

class ADS:
    def __init__(self, credential_path='/data/fin/api_keys/copernicus_ads.json'):
        cfg=json.loads(Path(credential_path).read_text())
        if cfg['url'].rstrip('/') != API: raise ValueError('Unexpected ADS credential endpoint')
        self.key=cfg['key']; self.session=requests.Session()

    def call(self, method, path, body=None):
        if not path.startswith('/') or path.startswith('//'): raise ValueError('Invalid API path')
        # Never allow authenticated redirects; never attach credentials to asset downloads.
        try:
            r=self.session.request(method,API+path,json=body,headers={'PRIVATE-TOKEN':self.key},
                                   allow_redirects=False,timeout=(15,90))
        except requests.RequestException:
            raise ADSAccessError('ADS transport failed; response not received') from None
        try: obj=r.json()
        except ValueError: obj={'status':r.status_code,'detail':'Non-JSON API response'}
        safe_response=sanitized(obj,self.key)
        if path.endswith('/results') and isinstance(safe_response.get('asset'),dict):
            value=safe_response['asset'].get('value',{})
            if isinstance(value,dict) and 'href' in value:
                value['href']='[asset download URL omitted]'
        receipt=dict(method=method,endpoint=API+path,request=body,status_code=r.status_code,
                     response=safe_response,retrieved_at=pd.Timestamp.now(tz='UTC').isoformat())
        stamp=pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')
        atomic_bytes(ROOT/'archive/cams10_receipts'/f'{stamp}.json',json.dumps(receipt,indent=2).encode())
        if r.status_code not in (200,201,202):
            if r.status_code==403 and 'licenc' in json.dumps(obj).lower():
                raise ADSAccessError('Dataset licence must be accepted manually at '+SOURCE+'?tab=download#manage-licences')
            raise ADSAccessError(f'ADS HTTP {r.status_code}: '+str(sanitized(obj,self.key).get('title','request failed')))
        return obj

    def retrieve(self, request, max_wait_seconds=1200, refresh=False):
        identity=digest(json.dumps(request,sort_keys=True).encode())
        path=ROOT/'archive/cams10_jobs'/(identity+'.json')
        obj=json.loads(path.read_text()) if path.exists() else dict(request=request)
        if obj.get('raw_sha256') and not refresh:
            raw=(ROOT/'archive/raw'/(obj['raw_sha256']+'.bin')).read_bytes()
            if digest(raw)!=obj['raw_sha256']: raise ValueError('Cached forecast checksum differs')
            return raw,obj
        if obj.get('raw_sha256') and refresh:
            # Preserve prior job evidence and source bytes. Only the completed job
            # is replaced; an already pending refresh job is resumed on retry.
            archive=path.parent/'history'/(identity+'_'+str(obj['job_id'])+'.json')
            atomic_bytes(archive,json.dumps(obj,indent=2).encode())
            obj=dict(request=request,previous_job_id=obj['job_id'])
        if not obj.get('job_id'):
            response=self.call('POST',f'/retrieve/v1/processes/{DATASET}/execution',{'inputs':request})
            job_id=response.get('jobID')
            if not job_id:
                links=[x['href'] for x in response.get('links',[]) if x.get('rel')=='monitor']
                if len(links)!=1: raise ValueError('Missing unique ADS job ID')
                job_id=urlsplit(links[0]).path.rstrip('/').split('/')[-1]
            if not re.fullmatch(r'[a-zA-Z0-9-]+',job_id): raise ValueError('Invalid ADS job ID')
            obj.update(job_id=job_id,submitted_at=pd.Timestamp.now(tz='UTC').isoformat())
            atomic_bytes(path,json.dumps(obj,indent=2).encode())
        deadline=time.monotonic()+max_wait_seconds
        while True:
            status=self.call('GET','/retrieve/v1/jobs/'+obj['job_id'])
            obj.update(status=status['status'],status_checked_at=pd.Timestamp.now(tz='UTC').isoformat())
            atomic_bytes(path,json.dumps(obj,indent=2).encode())
            if status['status']=='successful': break
            if status['status'] not in ('accepted','running','queued'): raise ADSAccessError('ADS job '+status['status'])
            if time.monotonic()>=deadline: raise ADSAccessError('ADS job remains '+status['status']+'; rerun resumes saved job')
            print('CAMS job',obj['job_id'],status['status'],flush=True);time.sleep(20)
        result=self.call('GET','/retrieve/v1/jobs/'+obj['job_id']+'/results')['asset']['value']
        url=result['href'];parsed=urlsplit(url)
        if parsed.scheme!='https' or not any(parsed.hostname==d or parsed.hostname.endswith('.'+d)
            for d in ('ecmwf.int','copernicus.eu','copernicus-climate.eu')): raise ValueError('Unexpected ADS download host')
        try:
            r=requests.get(url,allow_redirects=False,timeout=(15,180))
        except requests.RequestException: raise ADSAccessError('ADS asset download failed') from None
        if r.status_code!=200: raise ADSAccessError(f'ADS asset HTTP {r.status_code}')
        raw=r.content
        if len(raw)!=int(result['file:size']) or not raw.startswith(b'GRIB') or not raw.endswith(b'7777'):
            raise ValueError('Incomplete or non-GRIB ADS asset')
        h=digest(raw);atomic_bytes(ROOT/'archive/raw'/(h+'.bin'),raw)
        obj.update(raw_sha256=h,bytes=len(raw),download_host=parsed.hostname,
                   retrieved_at=pd.Timestamp.now(tz='UTC').isoformat(),source_last_modified=r.headers.get('Last-Modified'))
        atomic_bytes(path,json.dumps(obj,indent=2).encode());return raw,obj

def decode(raw, location, request):
    country,loc,lat,lon,_=location;rows=[]
    start,end=request['date'].split('/')
    expected_dates=pd.date_range(start,end,freq='D',tz='UTC')
    expected={(d+pd.Timedelta(hours=int(t[:2])),int(lead),field) for d in expected_dates
              for t in request['time'] for lead in request['leadtime_hour'] for field in FIELDS
              if FIELDS[field][0] in request['variable']}
    seen=set()
    with io.BytesIO(raw) as stream:
        # ecCodes' file API requires a real fileno. Message offsets are framed by
        # the GRIB indicator and edition-dependent total-length section instead.
        offset=0
        while offset<len(raw):
            if raw[offset:offset+4]!=b'GRIB': raise ValueError('Invalid GRIB framing')
            edition=raw[offset+7];length=int.from_bytes(raw[offset+4:offset+7],'big') if edition==1 else int.from_bytes(raw[offset+8:offset+16],'big')
            if length<16 or raw[offset+length-4:offset+length]!=b'7777': raise ValueError('Incomplete GRIB message')
            g=ec.codes_new_from_message(raw[offset:offset+length]);offset+=length
            try:
                param=int(ec.codes_get(g,'paramId'));field=next((k for k,v in FIELDS.items() if v[1]==param),None)
                if field is None: raise ValueError('Unexpected CAMS parameter')
                identity=tuple(ec.codes_get(g,k) for k in ('typeOfLevel','level','units','stepType'))
                if identity!=('surface',0,FIELDS[field][2],'instant'): raise ValueError('Unexpected CAMS level/units/statistic')
                if ec.codes_get(g,'dataType')!='fc': raise ValueError('Expected operational forecast, not analysis/reanalysis')
                if int(ec.codes_get(g,'stepUnits'))!=1: raise ValueError('Forecast step is not in hours')
                init=pd.to_datetime(str(ec.codes_get(g,'dataDate'))+f'{int(ec.codes_get(g,"dataTime")):04d}',format='%Y%m%d%H%M',utc=True)
                lead=int(ec.codes_get(g,'endStep'));key=(init,lead,field)
                if key not in expected or key in seen: raise ValueError('Unexpected or duplicate forecast initialization/lead/field')
                if int(ec.codes_get(g,'startStep'))!=lead: raise ValueError('Expected instantaneous target value')
                if ec.codes_get(g,'gridType')!='regular_ll' or not np.isclose(ec.codes_get(g,'iDirectionIncrementInDegrees'),.4) or not np.isclose(ec.codes_get(g,'jDirectionIncrementInDegrees'),.4): raise ValueError('Unexpected ADS grid')
                lats=ec.codes_get_array(g,'latitudes');lons=ec.codes_get_array(g,'longitudes');vals=ec.codes_get_values(g)
                lon_delta=(lons-lon+180)%360-180
                indices=np.where(np.isclose(lats,lat,atol=1e-5)&np.isclose(lon_delta,0,atol=1e-5))[0]
                if len(indices)!=1: raise ValueError('Requested exact ADS grid point absent or repeated')
                if ec.codes_get(g,'bitmapPresent') and not int(ec.codes_get_array(g,'bitmap')[indices[0]]):
                    raise ValueError('Selected CAMS grid point is missing in GRIB bitmap')
                value=float(vals[indices[0]])
                if not np.isfinite(value) or value<0 or value>=1e5: raise ValueError('Invalid concentration/AOD')
                valid=pd.to_datetime(str(ec.codes_get(g,'validityDate'))+f'{int(ec.codes_get(g,"validityTime")):04d}',format='%Y%m%d%H%M',utc=True)
                if valid!=init+pd.Timedelta(hours=lead): raise ValueError('Inconsistent forecast valid time')
                seen.add(key);rows.append(dict(country=country,location=loc,field=field,initialization=init,
                    lead_hours=lead,valid_time=valid,value=value,grid_latitude=lat,grid_longitude=lon))
            finally:ec.codes_release(g)
    if seen!=expected: raise ValueError(f'Missing {len(expected-seen)} requested forecast fields')
    return pd.DataFrame(rows)

def collect(refresh=False,start=None,end=None,location_ids=None,max_wait_seconds=1200):
    end=end or (pd.Timestamp.now(tz='UTC').floor('D')-pd.Timedelta(days=2)).strftime('%Y-%m-%d')
    if start is not None and pd.Timestamp(start)>pd.Timestamp(end):raise ValueError('CAMS start is after end')
    published=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','dataset_identity','content_sha256'])
    pinned,baseline,locations=refresh_selection(published,location_ids)
    companion=ROOT/'supplementary/cams10_issue_target.parquet'
    if pinned and not companion.exists():raise ValueError('Published CAMS companion is missing; restore it before refresh')
    previous=pd.read_parquet(companion) if companion.exists() else pd.DataFrame()
    client=ADS();panels=[];receipts=[];tasks=[]
    for loc in locations:
        request_start=window_start(loc,pinned,previous,end,start)
        tasks.extend((loc,req) for req in selected_requests(loc,request_start,end,pinned))
    intent=begin_refresh_intent(tasks) if refresh else None
    for loc,request in tasks:
        raw,receipt=retrieve_with_intent(client,request,max_wait_seconds,intent)
        part=decode(raw,loc,request);part['raw_sha256']=receipt['raw_sha256'];part['retrieved_at']=pd.Timestamp(receipt['retrieved_at'])
        panels.append(part);receipts.append(dict(receipt,location=loc[1],country=loc[0]));print('CAMS decoded',loc[1],len(part),flush=True)
    panel=merge_companions(previous,pd.concat(panels,ignore_index=True))
    buf=io.BytesIO();panel.to_parquet(buf,index=False,compression='zstd');content=buf.getvalue();companion_hash=digest(content)
    # Immutable copies prevent a partial refresh from invalidating companion
    # hashes embedded in already-published histories that were not promoted.
    versioned=ROOT/'supplementary/cams10'/('issue_target_'+companion_hash+'.parquet')
    atomic_bytes(versioned,content)
    companion_info={'path':str(versioned.relative_to(ROOT)),'sha256':companion_hash}
    docs=[]
    for url in (DOC,SOURCE,'https://ads.atmosphere.copernicus.eu/how-to-api'):
        _,h=Fetcher().get(url,max_age_hours=876000);docs.append(h)
    collection_latest=ROOT/'archive/cams10_collections/latest.json'
    prior_sources=[]
    if not previous.empty:
        if not collection_latest.exists():raise ValueError('Previous CAMS source collection manifest is missing')
        prior=json.loads(collection_latest.read_text())
        if prior.get('format')!='cams10_collection_v1' or digest(companion.read_bytes())!=prior['companion']['sha256']:
            raise ValueError('Previous CAMS companion/source manifest disagree')
        prior_sources=prior['sources']
    older=set(baseline.series_id);identities=set(baseline.dataset_identity.dropna());numbers=set(baseline.content_sha256.dropna())
    good=[];omissions=[];plans=[]
    for (country,loc,field,lead),part in panel.groupby(['country','location','field','lead_hours']):
        sid=series_id(country,loc,field,lead);identity=f'cams_operational|0.4degree|{loc}|{field}|lead{lead}|00_12UTC'
        if pinned and sid not in pinned:continue
        frame=normalize_frame(part.rename(columns={'initialization':'date'})[['date','value']]);h=digest(frame.date.astype('int64').to_numpy().tobytes()+frame.value.to_numpy().tobytes())
        if sid in older or identity in identities or h in numbers or frame.value.nunique()<2:omissions.append({'series_id':sid,'reason':'existing/duplicate/constant'});continue
        plans.append((country,loc,field,lead,part,sid,identity,frame));numbers.add(h)
    source_keys=('location','country','raw_sha256','job_id','request','retrieved_at','bytes')
    adopted={(r['location'],r['job_id']):{k:r[k] for k in source_keys} for r in prior_sources+receipts}
    sources=sorted(adopted.values(),key=lambda r:(r['retrieved_at'],r['location'],r['job_id']))
    if set(panel.raw_sha256)-{s['raw_sha256'] for s in sources}:raise ValueError('Companion source absent from adopted collection')
    collection=dict(format='cams10_collection_v1',sources=sources,companion=companion_info,
                    published_selection=sorted(pinned),candidate_series_ids=sorted(p[5] for p in plans),omissions=omissions)
    collection_bytes=json.dumps(collection,sort_keys=True,indent=2).encode();collection_hash=digest(collection_bytes)
    collection_path=ROOT/'archive/cams10_collections'/(collection_hash+'.json')
    atomic_bytes(collection_path,collection_bytes);atomic_bytes(ROOT/'archive/raw'/(collection_hash+'.bin'),collection_bytes)
    collection_info={'path':str(collection_path.relative_to(ROOT)),'sha256':collection_hash}
    manifest=archive_raw_manifest(docs+[s['raw_sha256'] for s in sources]+[collection_hash],description='Original ADS operational CAMS GRIB responses, adopted request/response collection manifest, and provider documentation')
    for country,loc,field,lead,part,sid,identity,frame in plans:
        label=next(x[4] for x in LOCATIONS if x[1]==loc)
        meta=save_candidate(GROUP,sid,frame,title=f'CAMS {label}: {field.replace("_"," ")}, {lead}-hour forecast',
            provider='Copernicus CAMS operational atmospheric composition forecasts',category='atmospheric_composition_expectations',
            unit=FIELDS[field][3],frequency='12-hourly',source_url=SOURCE,geography=label,country_codes=[country],
            country_code_standard='ISO 3166-1 alpha-2',geography_scope='non_us',dataset_identity=identity,
            hypothesis='Pollution, transported dust and combustion forecasts may help study industrial activity, solar production, transport disruptions and outdoor-demand risks; predictive trading value has not been validated.',
            limitations='Operational model forecasts, not directly measured emissions or city averages. Dust/AOD and CO/NO2 columns respond to transported pollution and meteorology, so are not pure local economic-activity proxies. One 0.4-degree grid point per hub. PM2.5 is near-surface model mass concentration; CO/NO2 are full atmospheric columns, not surface concentrations. Model cycle upgrade 49r1 to 50r1 on 2026-05-12 can create breaks. Forecast issue time precedes publication. No reconstructed historical availability. Lead 0 is model initial forecast state, not a physical ground measurement.',
            native_frequency='Forecast initialization at 00 and 12 UTC daily; source surface valid-time fields available hourly through 120 hours. Selected fixed leads 0,24,72 hours.',
            finer_frequency_available=True,expected_publication_lag='Provider documentation: 00UTC cycle guaranteed by 10UTC and 12UTC by 22UTC (10 hours), sometimes earlier. ADS delivery may be delayed; this is an approximate schedule, not verified historical first release.',
            timestamp_semantics='Model initialization UTC. Forecast valid time = initialization + fixed lead, retained in companion; date is neither valid time nor first public availability.',
            raw_sha256=docs,raw_manifest_sha256=[manifest],license='CC-BY licence; Copernicus Atmosphere Monitoring Service / ECMWF attribution.',
            point_in_time=False,forecast_model='CAMS operational IFS atmospheric composition',forecast_lead_hours=int(lead),
            source_metric=FIELDS[field][0],grid_latitude=float(part.grid_latitude.iloc[0]),grid_longitude=float(part.grid_longitude.iloc[0]),
            supplementary_files=[companion_info],supplemental_panel=companion_info['path'],
            source_collection_manifest=collection_info,
            refresh_policy='Initial backfill starts 2025-10-01. Later default refreshes request a seven-day overlap from the least-current selected history, preserving all earlier observations. --start selects a deeper recheck. --refresh forces a new provider job for the selected window; it does not establish historical release times.',
            revision_policy='Distinct archived operational cycles retained; historical archive corrections may exist. This is not EAC4 reanalysis and no claim of original first-publication bytes is made.')
        good.append(meta)
    files=[]
    for m in good:
        p=ROOT/'archive/staging'/GROUP/(m['series_id']+'.parquet')
        files.append(dict(series_id=m['series_id'],path=str(p.relative_to(ROOT)),parquet_sha256=digest(p.read_bytes()),metadata_sha256=digest(p.with_suffix('.json').read_bytes())))
    report=dict(series_count=len(good),observations=sum(m['rows'] for m in good),files=files,source_sha256=digest(Path(__file__).read_bytes()),
                supplementary_files=[companion_info],collection_manifest=collection_info,omissions=omissions,errors=[],finished_at=pd.Timestamp.now(tz='UTC').isoformat())
    atomic_bytes(companion,content);atomic_bytes(collection_latest,collection_bytes)
    atomic_bytes(ROOT/'logs/cams10_inventory.json',json.dumps(report,indent=2).encode())
    if intent:
        intent[1].update(status='complete',finished_at=pd.Timestamp.now(tz='UTC').isoformat())
        atomic_bytes(intent[0],json.dumps(intent[1],indent=2).encode())
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--refresh',action='store_true');p.add_argument('--start',help='Explicit backfill/recheck start; default is initial history or a seven-day refresh overlap');p.add_argument('--end');p.add_argument('--locations',nargs='+');p.add_argument('--max-wait-seconds',type=int,default=1200)
    a=p.parse_args()
    try:r=collect(a.refresh,a.start,a.end,a.locations,a.max_wait_seconds);print(json.dumps({k:v for k,v in r.items() if k!='files'},indent=2))
    except ADSAccessError as e:print(str(e),flush=True);raise SystemExit(2)
