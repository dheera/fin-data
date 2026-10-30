#!/usr/bin/env python3
"""GB operational electricity, forecast releases and REMIT announcements (batch 9).

Original publication-filtered API responses are retained. Observation clocks, forecast
targets and REMIT event times are deliberately separate from source publication clocks.
"""
from __future__ import annotations
import argparse, hashlib, io, json, re, threading, time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import pyarrow.parquet as pq
from expansion_common import ROOT, atomic_bytes, archive_raw_manifest, save_candidate, normalize_frame

GROUP='grid7'
BASE='https://data.elexon.co.uk/bmrs/api/v1'
SPEC='https://data.elexon.co.uk/swagger/v1/swagger.json'
STOP=threading.Event()
DATASETS={
 'FUELINST':('generation',7,'5min','five-minute average generation by fuel type'),
 'FUELHH':('generation',7,'30min','half-hour average generation by fuel type'),
 'INDO':('demand',7,'30min','initial national demand outturn'),
 'ITSDO':('demand',7,'30min','initial transmission system demand outturn'),
 'NDF':('demand',1,'irregular_intraday','national demand forecast'),
 'IMBALNGC':('imbalance',1,'irregular_intraday','indicated generation minus transmission demand forecast'),
 'MELNGC':('margin',1,'irregular_intraday','indicated export-limit margin above national demand forecast'),
 'WINDFOR':('generation',7,'irregular_intraday','wind generation forecast')}
FORECASTS={'NDF','IMBALNGC','MELNGC','WINDFOR'}
CORE_GENERATION={'BIOMASS','CCGT','NUCLEAR','WIND','NPSHYD'}


def get(url,params=None,refresh=False):
    """URL cache and immutable receipts; stop immediately on an access/rate barrier."""
    prepared=requests.Request('GET',url,params=params).prepare().url
    key=hashlib.sha256(prepared.encode()).hexdigest();path=ROOT/'archive/requests'/(key+'.json')
    if path.exists() and not refresh:
        receipt=json.loads(path.read_text());raw=(ROOT/'archive/raw'/(receipt['sha256']+'.bin')).read_bytes()
        if hashlib.sha256(raw).hexdigest()==receipt['sha256']:return raw,receipt['sha256']
    for attempt in range(3):
        if STOP.is_set():raise RuntimeError('Elexon requests stopped following access/rate barrier')
        try:
            time.sleep(.2)
            r=requests.get(url,params=params,headers={'User-Agent':'FinAltDataResearch/1.0 (public historical research)'},timeout=(15,120))
            if r.status_code in [401,403,429]:STOP.set()
            r.raise_for_status();raw=r.content;sha=hashlib.sha256(raw).hexdigest();now=pd.Timestamp.now(tz='UTC')
            receipt={'url':r.url,'sha256':sha,'retrieved_at':now.isoformat(),'content_type':r.headers.get('Content-Type'),
                     'source_last_modified':r.headers.get('Last-Modified'),'etag':r.headers.get('ETag')}
            atomic_bytes(ROOT/'archive/raw'/(sha+'.bin'),raw)
            atomic_bytes(ROOT/'archive/receipts'/(key+'_'+now.strftime('%Y%m%dT%H%M%S%fZ')+'.json'),json.dumps(receipt,indent=2).encode())
            atomic_bytes(path,json.dumps(receipt,indent=2).encode());return raw,sha
        except requests.RequestException as exc:
            status=getattr(getattr(exc,'response',None),'status_code',None)
            if status not in [None,500,502,503,504] or attempt==2:raise
            time.sleep(3**(attempt+1))


def windows(start,end,days):
    a=pd.Timestamp(start,tz='UTC');end=pd.Timestamp(end,tz='UTC')
    while a<end:
        b=min(a+pd.Timedelta(days=days),end);yield a,b;a=b


def fetch_partition(dataset,a,b,refresh=False):
    params={'publishDateTimeFrom':a.isoformat(),'publishDateTimeTo':b.isoformat(),'format':'json'}
    if dataset in {'NDF','IMBALNGC','MELNGC'}:params['boundary']='N'
    # Routine refresh checks recent publications; immutable older response bytes remain cached.
    recent=b>pd.Timestamp.now(tz='UTC')-pd.Timedelta(days=14)
    raw,h=get(BASE+'/datasets/'+dataset,params,refresh and recent);obj=json.loads(raw)
    if not isinstance(obj,dict) or not isinstance(obj.get('data'),list):raise ValueError('Malformed API response')
    records=obj['data']
    if not records:raise ValueError('Empty publication partition '+dataset+' '+str(a))
    for r in records:
        if r.get('dataset')!=dataset:raise ValueError('Wrong dataset')
        t=pd.Timestamp(r['publishTime'])
        if not a<=t<=b:raise ValueError('API did not obey publication bounds')
        if dataset in {'NDF','IMBALNGC','MELNGC'} and r.get('boundary')!='N':raise ValueError('Wrong national boundary')
    return {'dataset':dataset,'start':a.isoformat(),'end':b.isoformat(),'sha256':h,'rows':len(records)}


def valid_operational_rows(df,dataset):
    valid=pd.Series(True,index=df.index)
    if dataset.startswith('FUEL'):
        groups=['publishTime','startTime']
        valid=df.assign(magnitude=df.value.abs()).groupby(groups).magnitude.transform('max').ne(0)
        core=df[df.dimension.isin(CORE_GENERATION)].copy()
        stats=core.assign(magnitude=core.value.abs()).groupby(groups).agg(kinds=('dimension','nunique'),maximum=('magnitude','max'))
        bad=set(stats.index[(stats.kinds.eq(len(CORE_GENERATION)))&stats.maximum.eq(0)])
        # Native generation and interconnector telemetry can fail separately.
        partial=pd.Series([(p,t) in bad for p,t in zip(df.publishTime,df.startTime)],index=df.index)
        valid &= ~(partial & ~df.dimension.str.startswith('INT'))
    elif dataset in {'INDO','ITSDO','NDF'}:valid=df.value.gt(0)
    return valid


def parse_dataset(records,dataset):
    """Preserve all source issue/target pairs, remove only exact overlap duplicates."""
    field=DATASETS[dataset][0];df=pd.DataFrame(records)
    for col in ['publishTime','startTime']:df[col]=pd.to_datetime(df[col],utc=True,errors='raise')
    df['value']=pd.to_numeric(df[field],errors='raise')
    if dataset.startswith('FUEL'):df['dimension']=df['fuelType']
    else:df['dimension']='N'
    df=df[df.value.notna()].copy()
    if not np.isfinite(df.value).all():raise ValueError('Nonfinite source values')
    key=['dimension','publishTime','startTime']
    conflicts=df.groupby(key,dropna=False).value.nunique()
    if (conflicts>1).any():raise ValueError('Conflicting values at identical source issue/target')
    df=df.drop_duplicates(key).sort_values(key).reset_index(drop=True)
    df=df[valid_operational_rows(df,dataset)].copy()
    return df


def outturn_frames(df):
    # Prefer earliest source publication returned for each observed interval; never average revisions.
    return {dim:g.sort_values(['startTime','publishTime']).drop_duplicates('startTime',keep='first')
            .rename(columns={'startTime':'date'})[['date','value','publishTime']].reset_index(drop=True)
            for dim,g in df.groupby('dimension')}


def forecast_frames(df,dataset):
    """At each release select first target at/after fixed lead; never use a later release."""
    out={};step=3600 if dataset=='WINDFOR' else 1800
    for lead in ([6,24,48] if dataset=='WINDFOR' else [6,24]):
        g=df.copy();g['lead_seconds']=(g.startTime-g.publishTime).dt.total_seconds()
        g=g[(g.lead_seconds>=lead*3600)&(g.lead_seconds<lead*3600+step)]
        g=g.sort_values(['publishTime','startTime']).drop_duplicates('publishTime',keep='first')
        out[lead]=g.rename(columns={'publishTime':'date'})[['date','value','startTime','lead_seconds']].reset_index(drop=True)
    return out


def slug(x):return re.sub(r'[^a-z0-9]+','_',str(x).lower()).strip('_')


def parquet_panel(path,frame,keys,dataset=None):
    # A bounded refresh must not erase clocks and revisions needed by older canonical values.
    if path.exists():frame=pd.concat([pd.read_parquet(path),frame],ignore_index=True)
    frame=frame.drop_duplicates(keys,keep='last').sort_values(keys).reset_index(drop=True)
    if dataset:
        frame['canonical_valid']=valid_operational_rows(frame,dataset)
    b=io.BytesIO();frame.to_parquet(b,index=False,compression='zstd');atomic_bytes(path,b.getvalue())


@lru_cache(maxsize=1)
def publication_guard():
    cat=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','batch','frequency','dataset_identity','content_sha256'])
    old=cat[cat.batch.lt(9)]
    return {'ids':set(old.series_id),'identities':set(old.dataset_identity.dropna()),'hashes':set(old.content_sha256),
            'pinned':dict(zip(cat.loc[cat.batch.eq(9),'series_id'],cat.loc[cat.batch.eq(9),'frequency']))}


def write_candidate(sid,frame,metas,omissions,**kwargs):
    if frame.empty or frame.value.nunique()<2:
        omissions.append({'series_id':sid,'reason':'empty or constant'});return
    if kwargs.get('source_dataset','').startswith('FUEL') and frame.loc[frame.value.ne(0),'date'].dt.floor('D').nunique()<90:
        omissions.append({'series_id':sid,'reason':'fewer than 90 days with nonzero generation/flow; near-constant source history'});return
    guard=publication_guard()
    if sid in guard['ids'] or kwargs['dataset_identity'] in guard['identities']:
        omissions.append({'series_id':sid,'reason':'existing library identity'});return
    if guard['pinned'] and sid not in guard['pinned']:
        omissions.append({'series_id':sid,'reason':'outside pinned batch9 selection'});return
    if sid in guard['pinned'] and guard['pinned'][sid]!=kwargs['frequency']:raise ValueError('Published cadence changed')
    canonical=normalize_frame(frame[['date','value']])
    numeric=hashlib.sha256(canonical.date.astype('int64').to_numpy().tobytes()+canonical.value.to_numpy().tobytes()).hexdigest()
    if numeric in guard['hashes'] or any(m['content_sha256']==numeric for m in metas):
        omissions.append({'series_id':sid,'reason':'exact numeric duplicate'});return
    try:
        meta=save_candidate(GROUP,sid,frame[['date','value']],country_codes=['GB'],geography_scope='non_us',
             country_code_standard='ISO 3166-1 alpha-2',
             geography='Great Britain electricity system (excludes Northern Ireland)',
             point_in_time=False,license='Elexon public Insights Solution data; retain Elexon/NESO attribution and applicable data terms.',
             revision_policy='Original response bytes retained; source publication labels are not independently verified first availability.',
             **kwargs)
        metas.append(meta);print('OK',sid,meta['rows'],flush=True)
    except ValueError as exc:
        if 'insufficient genuine' not in str(exc):raise
        omissions.append({'series_id':sid,'reason':str(exc)})


def collect_operations(start,end,refresh=False):
    _,specsha=get(SPEC,refresh=refresh)
    partitions=[];errors=[];tasks=[(d,a,b) for d,v in DATASETS.items() for a,b in windows(start,end,v[1])]
    def work(t):
        try:return fetch_partition(*t,refresh=refresh)
        except Exception as exc:return {'error':{'dataset':t[0],'start':t[1].isoformat(),'end':t[2].isoformat(),'error':str(exc)}}
    with ThreadPoolExecutor(max_workers=3) as pool:
        for n,result in enumerate(pool.map(work,tasks),1):
            if 'error' in result:errors.append(result['error'])
            else:partitions.append(result)
            if n%25==0:print('Operations partitions',n,'/',len(tasks),'errors',len(errors),flush=True)
    atomic_bytes(ROOT/'logs/grid7_operations_requests.json',json.dumps({'start':start,'end_exclusive':end,'partitions':partitions,'errors':errors},indent=2).encode())
    if errors:raise ValueError(f'{len(errors)} incomplete operation partitions; refuse publication')
    metas=[];omissions=[];panel_paths=[]
    for dataset in DATASETS:
        parts=[p for p in partitions if p['dataset']==dataset];records=[]
        for p in parts:records.extend(json.loads((ROOT/'archive/raw'/(p['sha256']+'.bin')).read_bytes())['data'])
        df=parse_dataset(records,dataset);startts=pd.Timestamp(start,tz='UTC');endts=pd.Timestamp(end,tz='UTC')
        df=df[(df.publishTime>=startts)&(df.publishTime<endts)]
        panel_path=ROOT/'supplementary/grid7'/f'{dataset.lower()}_publication_target.parquet'
        # Each retained row contains the actual source release and target clocks; no release interpolation.
        parquet_panel(panel_path,df,['dimension','publishTime','startTime'],dataset);panel_paths.append(str(panel_path.relative_to(ROOT)))
        manifest=archive_raw_manifest([specsha]+[p['sha256'] for p in parts],description='Elexon '+dataset+' complete publication partitions and OpenAPI schema')
        common=dict(provider='Elexon Insights operational electricity',category='intraday_electricity_operations',unit='MW',
             source_url=BASE+'/datasets/'+dataset,raw_sha256=[specsha],raw_manifest_sha256=[manifest],source_dataset=dataset,
             supplemental_panel=str(panel_path.relative_to(ROOT)),source_documentation_url=SPEC,
             hypothesis='Power mix, demand, reserve margins and forecast changes may precede electricity, gas, carbon and energy-company price changes.')
        if dataset in FORECASTS:
            for lead,frame in forecast_frames(df,dataset).items():
                write_candidate('gb_elexon7_'+dataset.lower()+f'_forecast_lead{lead:02}h',frame,metas,omissions,
                    title=f'Great Britain {DATASETS[dataset][3]}, first target at least {lead} hours after release',
                    frequency='irregular_intraday',native_frequency='Source publication events; multiple releases per day',finer_frequency_available=False,
                    dataset_identity=f'elexon|{dataset}|boundaryN|published_release|first_target_ge_{lead}h',
                    expected_publication_lag='Canonical date is the source publishTime, not forecast target. Publication feed/network latency unverified.',
                    timestamp_semantics='Source publishTime in UTC. Value predicts first target at or after stated lead, within one target interval. Exact targets retained in supplementary panel.',
                    limitations='Publication-time forecast sequence, not observed power. National boundary N only. Each value uses the same release; no latest forecast joined backwards. Targets are rounded up to the next source interval, not interpolated. Source publishTime may describe provider publication rather than first website accessibility. Historical revision completeness is not independently established.',
                    forecast_min_lead_hours=lead,**common)
        else:
            for dimension,frame in outturn_frames(df).items():
                lag=(frame.publishTime-frame.date).dt.total_seconds()
                if (lag<0).any():raise ValueError('Outturn precedes publication')
                median=float(lag.median());p95=float(lag.quantile(.95))
                write_candidate('gb_elexon7_'+dataset.lower()+'_'+slug(dimension),frame,metas,omissions,
                    title=f'Great Britain {DATASETS[dataset][3]}: {dimension}',frequency=DATASETS[dataset][2],
                    native_frequency=DATASETS[dataset][2],finer_frequency_available=False,
                    dataset_identity=f'elexon|{dataset}|{dimension}|interval_start|earliest_returned_publication',
                    expected_publication_lag=f'Observed retained source publishTime minus interval start: median {median:g} seconds; p95 {p95:g} seconds. Exact source clocks in supplementary panel; network latency not measured.',
                    timestamp_semantics='UTC observation interval start from source startTime, not public availability. Matching source publishTime is retained in supplementary panel.',
                    publication_lag_median_seconds=median,publication_lag_p95_seconds=p95,
                    limitations='Operational aggregates, distinct from delayed settlement-unit B1610 metering. Earliest returned publication retained per interval; complete historical revisions not independently established. MW is average power, not MWh energy. Negative interconnector values retain source sign. Initial demand may differ from final settlement demand. Wholly zero fuel snapshots, simultaneously zero core native-generation snapshots, and nonpositive national/transmission demand are treated as invalid feeds. Working interconnector records survive native-generation feed gaps. Companion canonical_valid flags this rule; individual zero fuel observations otherwise retained. No filling.',**common)
    accepted={m['series_id'] for m in metas}
    for path in (ROOT/'archive/staging/grid7').glob('*.json'):
        old=json.loads(path.read_text())
        if old.get('source_dataset') in DATASETS and old['series_id'] not in accepted:
            folder=ROOT/'archive/work/grid7_rejected';folder.mkdir(parents=True,exist_ok=True)
            for suffix in ['.json','.parquet']:
                src=path.with_suffix(suffix)
                if src.exists():src.replace(folder/(path.stem+'_'+old['content_sha256'][:12]+suffix))
    report={'series':len(metas),'observations':sum(m['rows'] for m in metas),'series_ids':[m['series_id'] for m in metas],
            'partitions':len(partitions),'errors':errors,'omissions':omissions,'supplementary_panels':panel_paths}
    atomic_bytes(ROOT/'logs/grid7_operations_report.json',json.dumps(report,indent=2).encode());return report


def remit_counts(ids,details,start,end):
    """Hourly bins close at date; zeros only inside exhaustively downloaded windows."""
    ids=ids.drop_duplicates('id').copy();ids['publishTime']=pd.to_datetime(ids.publishTime,utc=True)
    start=pd.Timestamp(start,tz='UTC');end=pd.Timestamp(end,tz='UTC')
    ids=ids[(ids.publishTime>=start)&(ids.publishTime<end)]
    if ids.id.duplicated().any():raise ValueError('Duplicate message IDs')
    if not details.empty:
        details=details.drop_duplicates('id')
        if set(details.id)!=set(ids.id):raise ValueError('Incomplete REMIT details')
        for field in ['mrid','revisionNumber','publishTime']:
            left=ids.set_index('id')[field].sort_index();right=details.set_index('id')[field].sort_index()
            if field=='publishTime':right=pd.to_datetime(right,utc=True)
            if not left.equals(right):raise ValueError('REMIT identifier/detail clock mismatch: '+field)
        # Outage profiles are intentionally not interpreted as historical knowledge.
        ids=ids.merge(details[['id','fuelType','unavailabilityType','eventStatus']],on='id',validate='one_to_one')
    masks={'all_messages':pd.Series(True,index=ids.index),'initial_revision':ids.revisionNumber.eq(1),
           'updated_revision':ids.revisionNumber.gt(1)}
    if not details.empty:
        for field in ['fuelType','unavailabilityType','eventStatus']:
            for value in sorted(ids[field].dropna().unique()):masks[slug(field)+'_'+slug(value)]=ids[field].eq(value)
    index=pd.date_range(start+pd.Timedelta(hours=1),end,freq='h');out={}
    for name,mask in masks.items():
        selected=ids[mask]
        # Require events on at least 90 different days before admitting a category count.
        if selected.publishTime.dt.floor('D').nunique()<90:continue
        count=(selected.publishTime.dt.floor('h')+pd.Timedelta(hours=1)).value_counts().reindex(index,fill_value=0)
        out[name]=pd.DataFrame({'date':index,'value':count.astype(float).to_numpy()})
    return out


def collect_remit(start,end,refresh=False):
    _,specsha=get(SPEC,refresh=refresh);parts=[];allids=[]
    for n,(a,b) in enumerate(windows(start,end,7),1):
        recent=b>pd.Timestamp.now(tz='UTC')-pd.Timedelta(days=14)
        raw,h=get(BASE+'/remit/list/by-publish',{'from':a.isoformat(),'to':b.isoformat(),
                     'latestRevisionOnly':'false','profileOnly':'false','format':'json'},refresh and recent)
        obj=json.loads(raw);rows=obj['data']
        for row in rows:
            if not a<=pd.Timestamp(row['publishTime'])<=b:raise ValueError('REMIT list ignored publication bounds')
        parts.append({'start':a.isoformat(),'end':b.isoformat(),'sha256':h,'rows':len(rows)});allids.extend(rows)
        if n%10==0:print('REMIT list partitions',n,'messages',len(allids),flush=True)
    ids=pd.DataFrame(allids).drop_duplicates('id').sort_values('id');ids['publishTime']=pd.to_datetime(ids.publishTime,utc=True)
    ids=ids[(ids.publishTime>=pd.Timestamp(start,tz='UTC'))&(ids.publishTime<pd.Timestamp(end,tz='UTC'))]
    identifiers=ids.id.tolist();batches=[identifiers[i:i+400] for i in range(0,len(identifiers),400)]
    def work(batch):
        # Exact revision IDs are cached. New revisions get new IDs and are downloaded separately.
        raw,h=get(BASE+'/remit',{'messageId':batch,'format':'json'},False);rows=json.loads(raw)['data']
        if {x['id'] for x in rows}!=set(batch) or len(rows)!=len(batch):raise ValueError('Incomplete REMIT bulk response')
        return {'ids':batch,'sha256':h,'rows':len(rows)},rows
    detailparts=[];details=[]
    with ThreadPoolExecutor(max_workers=3) as pool:
        for n,(part,rows) in enumerate(pool.map(work,batches),1):
            detailparts.append(part);details.extend(rows)
            if n%20==0:print('REMIT detail batches',n,'/',len(batches),flush=True)
    detailframe=pd.DataFrame(details)
    for col in ['publishTime','createdTime','eventStartTime','eventEndTime']:
        if col in detailframe:detailframe[col]=pd.to_datetime(detailframe[col],utc=True)
    # Store nested profiles as literal JSON: they may contain intervals added after this message's publishTime.
    if 'outageProfile' in detailframe:detailframe['outageProfile_json']=detailframe.pop('outageProfile').map(lambda x:json.dumps(x,sort_keys=True))
    parquet_panel(ROOT/'supplementary/grid7/remit_identifiers.parquet',ids,['id'])
    parquet_panel(ROOT/'supplementary/grid7/remit_message_details.parquet',detailframe,['id'])
    manifest=archive_raw_manifest([specsha]+[p['sha256'] for p in parts+detailparts],description='Complete Elexon REMIT all-revision publication lists, exact-ID bulk details and API specification')
    atomic_bytes(ROOT/'logs/grid7_remit_requests.json',json.dumps({'start':start,'end_exclusive':end,'partitions':parts,'detail_batches':detailparts,'errors':[]},indent=2).encode())
    metas=[];omissions=[]
    for name,frame in remit_counts(ids,detailframe,start,end).items():
        write_candidate('gb_elexon7_remit_hourly_'+name,frame,metas,omissions,
            title='Great Britain REMIT announcements per closed UTC hour: '+name.replace('_',' '),
            provider='Elexon REMIT publication archive',category='electricity_outage_announcements',unit='published message revisions per hour',frequency='hourly',
            source_url=BASE+'/remit/list/by-publish',dataset_identity='elexon|REMIT|all_revisions|publish_hour_end|'+name,
            raw_sha256=[specsha],raw_manifest_sha256=[manifest],native_frequency='Event-level publication timestamps (seconds)',finer_frequency_available=True,
            expected_publication_lag='Canonical date is UTC hour end, after all included source publishTime values; exact event publication seconds retained. Distribution latency unverified.',
            timestamp_semantics='UTC closing boundary of [date-1hour, date) source publication bin, not outage start/end time.',
            hypothesis='Publication bursts and changes in announced outages may precede power, gas and carbon market repricing.',
            limitations='Counts of distinct message revision IDs, not distinct outages, unavailable MW, or active capacity. Zero means no returned publication in a successfully downloaded interval. All revisions explicitly requested; initial means source revisionNumber=1. Type/fuel/status categories use archived detail fields; immutable historical classifications are not independently verified. Outage profiles can contain intervals later than the selected revision and must not be treated as then-known information. No capacity reconstructed from those profiles.',
            supplemental_panel='supplementary/grid7/remit_message_details.parquet',source_documentation_url=SPEC,announcement_count_filter=name)
    report={'series':len(metas),'observations':sum(m['rows'] for m in metas),'series_ids':[m['series_id'] for m in metas],
            'messages':len(ids),'partitions':len(parts),'detail_batches':len(detailparts),'errors':[],'omissions':omissions}
    atomic_bytes(ROOT/'logs/grid7_remit_report.json',json.dumps(report,indent=2).encode());return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--refresh',action='store_true');p.add_argument('--source',choices=['operations','remit','all'],default='all')
    p.add_argument('--start',default='2025-10-01');p.add_argument('--end');a=p.parse_args()
    a.end=a.end or (pd.Timestamp.now(tz='UTC').floor('D')-pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    reports={}
    if a.source in ['operations','all']:reports['operations']=collect_operations(a.start,a.end,a.refresh)
    if a.source in ['remit','all']:reports['remit']=collect_remit(a.start,a.end,a.refresh)
    print(json.dumps(reports,indent=2))
if __name__=='__main__':main()
