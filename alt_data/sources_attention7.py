#!/usr/bin/env python3
"""Native intraday news-event discovery and global language-project readership."""
from __future__ import annotations
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import hashlib, io, json, math, re, threading, time, zipfile
from pathlib import Path
import pandas as pd
import requests
from expansion_common import (ROOT, atomic_bytes, normalize_frame, coverage, qualifies_research,
                              save_candidate, archive_raw_manifest)
from country_codes import normalize_country_codes

GROUP = 'attention7'
PARSER_VERSION = 1
FOCUS = ['CN','IN','KR','ID','MX','JP','TW','AU','NZ','SG','MY','TH','VN','PH','PK','BD',
         'BR','AR','CL','CO','PE','GB','DE','FR','ES','IT','PL','TR','ZA','NG','EG','SA','AE','IL','UA','US']
LANGUAGES = {'en':'English','es':'Spanish','pt':'Portuguese','fr':'French','de':'German','ru':'Russian',
 'uk':'Ukrainian','zh':'Chinese','ko':'Korean','ja':'Japanese','hi':'Hindi','bn':'Bengali','ta':'Tamil',
 'te':'Telugu','mr':'Marathi','id':'Indonesian','ms':'Malay','vi':'Vietnamese','th':'Thai','ar':'Arabic',
 'fa':'Persian','tr':'Turkish','pl':'Polish','nl':'Dutch','sv':'Swedish','fi':'Finnish','no':'Norwegian',
 'da':'Danish','he':'Hebrew','sw':'Swahili'}
METRICS = ['events','protest_events','material_conflict_events','business_labor_events','mean_tone']
METRIC_LABELS = {'events':'newly discovered coded events','protest_events':'newly discovered protest events',
 'material_conflict_events':'newly discovered material-conflict events',
 'business_labor_events':'newly discovered business/labor-actor events','mean_tone':'event-associated mean news tone'}
OMISSIONS = []
BARRIERS = []
SEEN = {}

class Omission(ValueError): pass
class AccessDenied(RuntimeError): pass
class CacheMiss(RuntimeError): pass

class PublicFetcher:
    """Archive originals and receipts, reuse immutable history, stop on denials/rate limits."""
    def __init__(self, refresh=False, cache_only=False, request_pause_seconds=.15):
        self.refresh=refresh; self.cache_only=cache_only; self.request_pause_seconds=request_pause_seconds
        self.local=threading.local(); self.denied=threading.Event()
    def get(self,url,*,timeout=90,immutable=True):
        if self.denied.is_set():raise AccessDenied('Provider access stopped after denial/rate limit')
        key=hashlib.sha256(url.encode()).hexdigest();p=ROOT/'archive/requests'/f'{key}.json'
        if p.exists() and (self.cache_only or immutable or not self.refresh):
            info=json.loads(p.read_text());rawpath=ROOT/'archive/raw'/f"{info['sha256']}.bin"
            if rawpath.exists():
                raw=rawpath.read_bytes()
                if hashlib.sha256(raw).hexdigest()==info['sha256']:return raw,info['sha256']
        if self.cache_only:raise CacheMiss('No archived original response: '+url)
        if not hasattr(self.local,'session'):
            self.local.session=requests.Session()
            self.local.session.headers['User-Agent']='FinAltDataResearch/1.0 (public historical time-series research)'
        for attempt in range(3):
            if self.denied.is_set():raise AccessDenied('Provider access stopped after denial/rate limit')
            try:
                response=self.local.session.get(url,timeout=(15,timeout))
                if response.status_code in (401,403,429):
                    self.denied.set();raise AccessDenied(f'HTTP {response.status_code}: {url}')
                response.raise_for_status();break
            except requests.RequestException as exc:
                status=getattr(getattr(exc,'response',None),'status_code',None)
                if status not in (None,500,502,503,504) or attempt==2:raise
                time.sleep(2**attempt)
        raw=response.content;digest=hashlib.sha256(raw).hexdigest()
        now=pd.Timestamp.now(tz='UTC')
        info={'url':response.url,'sha256':digest,'retrieved_at':now.isoformat(),
              'content_type':response.headers.get('Content-Type'),'last_modified':response.headers.get('Last-Modified'),
              'etag':response.headers.get('ETag'),'status':response.status_code}
        atomic_bytes(ROOT/'archive/raw'/f'{digest}.bin',raw)
        atomic_bytes(ROOT/'archive/receipts'/f'{key}_{now:%Y%m%dT%H%M%S%fZ}.json',json.dumps(info,indent=2).encode())
        atomic_bytes(p,json.dumps(info,indent=2).encode())
        time.sleep(self.request_pause_seconds)
        return raw,digest

@lru_cache(maxsize=1)
def baseline():
    records=json.loads((ROOT/'logs/expansion_batch9_baseline.json').read_text())['series']
    result={field:{r.get(field) for r in records} for field in ['series_id','dataset_identity','content_sha256']}
    cat=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','batch','frequency'])
    result['pinned']=dict(cat.loc[cat.batch.eq(9),['series_id','frequency']].itertuples(index=False,name=None))
    return result

def save(sid,frame,**meta):
    old=baseline()
    if old['pinned'] and sid not in old['pinned']:raise Omission('Outside published batch9 selection')
    if sid in old['series_id'] or meta['dataset_identity'] in old['dataset_identity']:raise Omission('Already published identity')
    frame=normalize_frame(frame)
    if frame.value.nunique()<2 or not qualifies_research({'frequency':meta['frequency'],**coverage(frame)}):
        raise Omission('Constant or insufficient actual history')
    digest=hashlib.sha256(frame.date.astype('int64').to_numpy().tobytes()+frame.value.to_numpy().tobytes()).hexdigest()
    if digest in old['content_sha256'] or (digest in SEEN and SEEN[digest]!=sid):raise Omission('Duplicate numeric history')
    if sid in old['pinned'] and old['pinned'][sid]!=meta['frequency']:raise ValueError('Published cadence changed')
    SEEN[digest]=sid
    meta['country_codes']=normalize_country_codes(meta.get('country_codes',[]))
    meta['country_code_standard']='ISO 3166-1 alpha-2'
    meta.setdefault('native_frequency',meta['frequency']);meta.setdefault('finer_frequency_available',False)
    meta.setdefault('point_in_time',False)
    return save_candidate(GROUP,sid,frame,**meta)

def attempt(good,sid,frame,**meta):
    try:good.append(save(sid,frame,**meta))
    except Omission as exc:OMISSIONS.append({'series_id':sid,'reason':str(exc)})

def parse_gdelt(raw,stamp,fips_to_iso):
    """One complete 61-column Event export. Never treat Mention records as new events."""
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members=[n for n in archive.namelist() if not n.endswith('/')]
        if members!=[stamp+'.export.CSV']:raise ValueError('Unexpected GDELT archive member')
        lines=archive.read(members[0]).decode('utf-8').splitlines()
    # A valid zero-byte member confirms an empty processing batch; a missing ZIP never does.
    result={iso:{m:0 for m in METRICS if m!='mean_tone'} for iso in fips_to_iso.values()}
    tones=defaultdict(list);ids=set()
    for line in lines:
        row=line.split('\t')
        if len(row)!=61:raise ValueError('Unexpected GDELT 2.0 column count')
        event_id=int(row[0])
        if event_id in ids:raise ValueError('Duplicate GLOBALEVENTID in processing batch')
        ids.add(event_id)
        if row[59]!=stamp:raise ValueError('DATEADDED differs from processing archive timestamp')
        tone=float(row[34])
        if not math.isfinite(tone) or abs(tone)>100:raise ValueError('Invalid GDELT AvgTone')
        if row[29] not in ('1','2','3','4'):raise ValueError('Unknown GDELT QuadClass')
        iso=fips_to_iso.get(row[53])
        if not iso:continue
        values=result[iso];values['events']+=1
        values['protest_events']+=int(row[28]=='14')
        values['material_conflict_events']+=int(row[29]=='4')
        values['business_labor_events']+=int(bool({'BUS','LAB'} & set(row[12:15]+row[22:25])))
        tones[iso].append(tone)
    for iso,values in result.items():
        values['mean_tone']=sum(tones[iso])/len(tones[iso]) if tones[iso] else None
    return {'metrics':result,'global_rows':len(ids),'id_min':min(ids) if ids else None,'id_max':max(ids) if ids else None}

def gdelt(fetch,start,end):
    docs=[]
    for url in ['https://gdeltproject.org/data.html','https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf']:
        _,sha=fetch.get(url);docs.append(sha)
    raw,cross_sha=fetch.get('https://download.geonames.org/export/dump/countryInfo.txt')
    docs.append(cross_sha)
    rows=[r.split('\t') for r in raw.decode().splitlines() if r and not r.startswith('#')]
    mapping={r[3]:r[0] for r in rows if r[0] in FOCUS and r[3]}
    names={r[0]:r[4] for r in rows if r[0] in FOCUS}
    if set(mapping.values())!=set(FOCUS):raise ValueError('Incomplete FIPS/ISO mapping')
    signature=hashlib.sha256(json.dumps({'version':PARSER_VERSION,'mapping':mapping},sort_keys=True).encode()).hexdigest()
    dates=pd.date_range(start,end+pd.Timedelta(days=1)-pd.Timedelta(minutes=15),freq='15min')
    folder=ROOT/'archive/work/attention7_gdelt';folder.mkdir(exist_ok=True,parents=True)
    def one(date):
        stamp=date.strftime('%Y%m%d%H%M%S');url=f'https://data.gdeltproject.org/gdeltv2/{stamp}.export.CSV.zip'
        raw,sha=fetch.get(url,timeout=90)
        cached=folder/f'{stamp}.json'
        if cached.exists():
            obj=json.loads(cached.read_text())
            if obj.get('raw_sha256')==sha and obj.get('parser_signature')==signature:return obj
        obj={'stamp':stamp,'raw_sha256':sha,'url':url,'parser_signature':signature,**parse_gdelt(raw,stamp,mapping)}
        atomic_bytes(cached,json.dumps(obj,separators=(',',':')).encode());return obj
    complete=[];failures=[]
    # Each worker takes sequential tasks. It consults a shared stop flag before every HTTP request.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(one,date):date for date in dates}
        for future in as_completed(futures):
            date=futures[future]
            try:complete.append(future.result())
            except AccessDenied:
                for pending in futures:pending.cancel()
                raise
            except Exception as exc:
                failures.append({'date':date.isoformat(),'error':str(exc)})
                print('GDELT MISSING',date,str(exc),flush=True)
            if (len(complete)+len(failures))%250==0:print('GDELT',len(complete),'/',len(dates),'missing',len(failures),flush=True)
    complete.sort(key=lambda x:x['stamp'])
    # IDs are allocated sequentially. Reject overlapping ranges rather than risk counting one event twice.
    previous=None
    for obj in complete:
        if obj['id_min'] is not None:
            if previous is not None and obj['id_min']<=previous:raise ValueError('GDELT global event-ID ranges overlap across batches')
            previous=obj['id_max']
    manifest=archive_raw_manifest([obj['raw_sha256'] for obj in complete],description='Original GDELT 2.0 15-minute new-event exports for attention7')
    atomic_bytes(ROOT/'logs/attention7_gdelt_intervals.json',json.dumps({'start':str(start),'end':str(end),
        'expected_intervals':len(dates),'complete_intervals':len(complete),'failures':failures,'raw_manifest_sha256':manifest,
        'global_new_events':sum(o['global_rows'] for o in complete),'country_fips_to_iso':mapping,
        'intervals':[{k:o[k] for k in ['stamp','raw_sha256','global_rows','id_min','id_max']} for o in complete]},indent=2).encode())
    for obj,date in zip(complete,pd.to_datetime([o['stamp'] for o in complete],format='%Y%m%d%H%M%S',utc=True)):
        obj['date']=date
    good=[]
    for iso in FOCUS:
        for metric in METRICS:
            data=[{'date':o['date'],'value':o['metrics'][iso][metric]}
                  for o in complete if o['metrics'][iso][metric] is not None]
            frame=pd.DataFrame(data)
            if metric!='mean_tone' and frame.loc[frame.value.gt(0),'date'].dt.floor('D').nunique()<90:
                OMISSIONS.append({'series_id':f'gdelt7_{iso.lower()}_{metric}','reason':'Fewer than 90 active days'});continue
            attempt(good,f'gdelt7_{iso.lower()}_{metric}',frame,title=f'GDELT 2.0 {names[iso]}: {METRIC_LABELS[metric]}',
                provider='GDELT 2.0 worldwide news events',category='intraday_news_tone' if metric=='mean_tone' else 'intraday_news_event_attention',
                unit='event-associated text tone score' if metric=='mean_tone' else 'newly discovered coded events per 15-minute processing batch',
                frequency='15min',source_url='https://data.gdeltproject.org/gdeltv2/',geography=names[iso],country_codes=[iso],
                geography_scope='us' if iso=='US' else 'non_us',dataset_identity=f'GDELT2:15min:discovery:action_geo:{iso}:{metric}',
                hypothesis='Changes in media attention, conflict, labor/business actor coverage and tone may precede changes in perceived country risk and supply disruptions.',
                limitations='Algorithmically coded multilingual media events, not verified incidents, unique articles, transactions or casualties. ActionGeo is reported event location, not source language, publisher country or actor nationality. Only newly discovered Event rows count; Mention rows are not downloaded or counted as additional events. Separate media events can refer to the same real-world incident. Business/labor requires BUS or LAB in either actor type fields; this is not a broad economic-news classifier. Tone is an unweighted average of event-associated AvgTone and is neither market sentiment nor article-unique. Missing/failed archive intervals are omitted; count zeros require a successfully parsed complete interval. Tone is absent when no located event exists. Historical records/coverage may change.',
                expected_publication_lag='GDELT processes and releases 15-minute batches. DATEADDED is database addition time, not original article release time; archive first-publication delay is unverified.',
                timestamp_semantics='UTC DATEADDED database-discovery processing-batch timestamp. It is not the claimed event date, article publication timestamp, or verified first public availability.',
                raw_sha256=docs,raw_manifest_sha256=[manifest],source_metric=metric,source_country_fips=next(k for k,v in mapping.items() if v==iso),
                source_refresh_cadence='15 minutes',documentation_url='https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf',
                revision_policy='Completed event exports are retained as acquisition snapshots and reused; refresh adds new archive intervals. A later source correction is not automatically substituted into previously archived intervals.',
                aggregation='Count distinct source GLOBALEVENTID in complete processing archive; or mean AvgTone over matched events.',
                minimum_active_days=90,parser_version=PARSER_VERSION)
    return good

def parse_wikimedia(raw,project,access,start,end):
    obj=json.loads(raw)
    if not isinstance(obj.get('items'),list) or not obj['items']:raise ValueError('Missing Wikimedia items')
    rows=[]
    for item in obj['items']:
        if (item.get('project')!=project.removesuffix('.org') or item.get('access')!=access or
            item.get('agent')!='user' or item.get('granularity')!='hourly'):raise ValueError('Wrong Wikimedia dimensions')
        stamp=str(item['timestamp'])
        if not re.fullmatch(r'\d{10}',stamp):raise ValueError('Wikimedia hour timestamp is malformed')
        value=item['views']
        if isinstance(value,bool) or not isinstance(value,int) or value<0:raise ValueError('Invalid Wikimedia view count')
        rows.append({'date':stamp,'value':value})
    frame=pd.DataFrame(rows);frame.date=pd.to_datetime(frame.date,format='%Y%m%d%H',utc=True)
    if not frame.date.between(start,end).all():raise ValueError('Wikimedia response outside requested window')
    return normalize_frame(frame)

def wikimedia(fetch,start,end):
    # Primary documentation was reviewed through search-indexed official pages.
    # Its separate web host returned 502/403 on direct archival fetch. Do not retry/bypass it;
    # the public data API is available and each original JSON response is archived below.
    docs=[]
    # Admission filtering after download is too late to protect provider quotas.
    # Once batch9 exists, only refresh its actually published Wiki channels.
    pinned=baseline()['pinned']
    jobs=[(language,access) for language in LANGUAGES for access in ['desktop','mobile-web','mobile-app']
          if not pinned or f'wikimedia7_{language}_{access.replace("-","_")}_hourly_views' in pinned]
    def one(language,access):
        project=language+'.wikipedia.org';frames=[];hashes=[];urls=[];partial=False
        for year in range(start.year,end.year+1):
            lo=max(start,pd.Timestamp(f'{year}-01-01',tz='UTC'))
            hi=min(end+pd.Timedelta(hours=23),pd.Timestamp(f'{year}-12-31 23:00',tz='UTC'))
            url=f'https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate/{project}/{access}/user/hourly/{lo:%Y%m%d%H}/{hi:%Y%m%d%H}'
            try:raw,sha=fetch.get(url,immutable=False)
            except (AccessDenied,CacheMiss) as exc:
                partial=True
                BARRIERS.append({'source':'wikimedia','project':project,'access':access,'url':url,'error':str(exc)})
                break
            frames.append(parse_wikimedia(raw,project,access,lo,hi));hashes.append(sha);urls.append(url)
        if not frames:return language,access,None,None
        frame=normalize_frame(pd.concat(frames,ignore_index=True))
        meta=dict(title=f'Wikipedia {LANGUAGES[language]} edition: hourly human-classified {access} page views',
            provider='Wikimedia hourly project pageviews',category='intraday_internet_readership',unit='human-classified page views per hour',
            frequency='hourly',source_url=urls[-1],geography=f'Global readers of the {LANGUAGES[language]} Wikipedia edition',
            country_codes=[],geography_scope='global',dataset_identity=f'Wikimedia:aggregate:{project}:{access}:user:hourly',
            hypothesis='Hourly language-project readership may capture attention shocks, disruptions and changes in internet-use rhythms; relevance to markets requires testing.',
            limitations='Project-wide page views, not topic/article search interest, unique users, purchasing intent or reader-country traffic. Language editions have global audiences and cannot be assigned to a country. User traffic excludes identified spiders and automated agents but bot detection is imperfect and methodology may change. Access channels are separate and must not be interpreted as independent audiences. The API omits zeros and cannot distinguish zero from unloaded data, so absent hours are omitted, never filled or reconstructed from daily totals. Broad intraday patterns are strongly seasonal.',
            expected_publication_lag='Wikimedia documents loading after the observation hour ends, usually within a few hours; processing problems can delay data by 24 hours or more. Exact historical first-release times are unavailable.',
            timestamp_semantics='UTC start of source observation hour. It is not API release or local acquisition time.',
            raw_sha256=docs+hashes,source_urls=urls,source_project=project,source_access=access,source_agent='user',source_language=language,
            collection_window_partial=partial,
            collection_access_note='API returned HTTP 429 during initial acquisition; requests stopped. This run only reuses archived successful responses.' if fetch.cache_only else 'Public API; requests stop on HTTP 401, 403 or 429.',
            source_refresh_cadence='Hourly underlying data; typically published within a few hours after period end',
            revision_policy='The API can backfill or revise history. Explicit refresh fetches mutable API response URLs again; prior local originals and numeric versions remain archived.',
            documentation_url='https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html',
            publication_lag_documentation_url='https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/documentation/troubleshooting.html',
            documentation_snapshot_status='Primary official documentation reviewed via search index; direct documentation host 502/403, not retried. Original data API JSON archived.',
            license='Wikimedia Analytics API data, CC0 1.0',parser_version=PARSER_VERSION)
        return language,access,frame,meta
    good=[]
    # Wikimedia recommends waiting for each API request before issuing the next.
    with ThreadPoolExecutor(max_workers=1) as pool:
        futures=[pool.submit(one,language,access) for language,access in jobs]
        for future in as_completed(futures):
            language,access,frame,meta=future.result()
            if frame is None:continue
            attempt(good,f'wikimedia7_{language}_{access.replace("-","_")}_hourly_views',frame,**meta)
            print('WIKIMEDIA',language,access,len(frame),'series',len(good),flush=True)
    return good

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',choices=['all','gdelt','wikimedia'],default='all')
    p.add_argument('--refresh',action='store_true');p.add_argument('--cached-only',action='store_true');p.add_argument('--end',default=str((pd.Timestamp.now(tz='UTC').normalize()-pd.Timedelta(days=2)).date()))
    p.add_argument('--gdelt-start',default='2026-07-01');p.add_argument('--wiki-start',default='2024-01-01');args=p.parse_args()
    end=pd.Timestamp(args.end,tz='UTC');result={'started_at':pd.Timestamp.now(tz='UTC').isoformat(),'sources':{},'failures':[]}
    for name,start,fn in [('gdelt',args.gdelt_start,gdelt),('wikimedia',args.wiki_start,wikimedia)]:
        if args.source not in ['all',name]:continue
        try:
            good=fn(PublicFetcher(args.refresh,args.cached_only,request_pause_seconds=5 if name=='wikimedia' else .15),pd.Timestamp(start,tz='UTC'),end)
            result['sources'][name]={'series':len(good),'observations':sum(m['rows'] for m in good),'series_ids':[m['series_id'] for m in good]}
        except Exception as exc:
            import traceback;traceback.print_exc();result['failures'].append({'source':name,'error':str(exc)})
    result['omissions']=OMISSIONS;result['barriers']=BARRIERS;result['finished_at']=pd.Timestamp.now(tz='UTC').isoformat()
    if args.source=='wikimedia' and args.cached_only:
        result['acquisition_note']='Initial live API acquisition returned HTTP 429 on Spanish desktop 2025 after ten successful annual responses. Requests stopped immediately; no API retries were made. This reconstruction uses only archived successful originals. A future explicitly scheduled refresh may resume with smaller windows and a conservative request rate after source limits permit it.'
    atomic_bytes(ROOT/'logs'/f'attention7_{args.source}_report.json',json.dumps(result,indent=2).encode())
    print(json.dumps(result,indent=2));raise SystemExit(bool(result['failures']))
if __name__=='__main__':main()
