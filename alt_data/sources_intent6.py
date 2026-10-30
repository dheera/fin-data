#!/usr/bin/env python3
"""Daily hiring demand and observed trending-search attention, with original source snapshots."""
from __future__ import annotations
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import csv, hashlib, io, json, re, zipfile
import pandas as pd
from country_codes import COUNTRY_ALPHA2, normalize_country_codes
from expansion_common import ROOT, Fetcher, atomic_bytes, normalize_frame, coverage, qualifies_research, save_candidate

GROUP = 'intent6'
OMISSIONS = []
SEEN = {}
FOCUS = {'CN','IN','KR','ID','MX'}
US_CODES = {'US','AS','GU','MP','PR','UM','VI'}
# Transparent multilingual keyword rules, applied to the displayed trend cluster and its related queries.
# These are attention topics, NOT a model of sentiment or purchase intent.
TOPICS = {
 'markets': r'\b(bitcoin|ethereum|crypto|forex|stock market|stock price|exchange rate|gold price|bourse|bolsa|dolar|dólar|rupiah|saham|nifty|sensex|kospi|kosdaq)\b|比特币|股市|黄金价格|환율|주가|비트코인|금값|सोने का भाव|शेयर बाजार',
 'labor': r'\b(layoffs?|unemployment|job vacancies|job openings|hiring|redundancies|despidos?|desempleo|lowongan|pengangguran|phk)\b|招聘|裁员|失业|채용|실업|해고|नौकरी|बेरोजगार',
 'shopping': r'\b(black friday|cyber monday|prime day|coupon|discount|shopping|temu|shein|shopee|lazada|tokopedia|tiktok shop|buen fin|hot sale|ofertas|descuentos|belanja|diskon)\b|购物|打折|쿠팡|할인|세일|छूट',
 'travel': r'\b(flights?|airline|airport|airfare|booking|hotels?|vacation|vuelos?|aeropuerto|aerolinea|aerolínea|viajes?|pesawat|bandara|tiket|penerbangan|liburan)\b|航班|机票|旅游|항공|공항|항공권|여행|हवाई|उड़ान',
 'energy': r'\b(blackout|power outage|electricity|gas prices?|oil prices?|petrol|diesel|gasolina|apagón|apagon|listrik|bensin|pemadaman|pertamina)\b|停电|电价|油价|정전|전기요금|유가|बिजली|पेट्रोल',
 'weather_disruption': r'\b(hurricane|typhoon|cyclone|floods?|flooding|heatwave|drought|earthquake|wildfire|tsunami|huracán|huracan|inundaciones|terremoto|sequía|sequia|banjir|gempa|kekeringan|badai)\b|台风|洪水|地震|干旱|태풍|홍수|지진|폭염|बाढ़|भूकंप|चक्रवात',
 'credit_stress': r'\b(bankruptcy|bank run|bank collapse|recession|debt default|mortgage rate|credit crunch|quiebra|recesión|recesion|bancarrota|bangkrut|gagal bayar|resesi)\b|破产|衰退|违约|파산|경기침체|연체|दिवालिया|मंदी',
 'technology_products': r'\b(iphone|samsung galaxy|nvidia|chatgpt|claude|gemini ai|deepseek|playstation|nintendo|xbox|macbook|tesla|byd)\b|华为|小米|比亚迪|아이폰|갤럭시|삼성전자',
}

class Omission(ValueError): pass

def slug(s):return re.sub('[^a-z0-9]+','_',str(s).lower()).strip('_')
def token(s):return slug(s)[:65]+'_'+hashlib.sha256(str(s).encode()).hexdigest()[:10]

@lru_cache(maxsize=1)
def baseline():
    p=ROOT/'archive/intent6_selection/baseline.json'
    if not p.exists():
        cat=pd.read_parquet(ROOT/'catalog.parquet');cat=cat[cat.batch.lt(8)]
        atomic_bytes(p,json.dumps({'series_ids':cat.series_id.tolist(),'identities':cat.dataset_identity.dropna().tolist(),'hashes':cat.content_sha256.tolist()}).encode())
    obj={k:set(v) for k,v in json.loads(p.read_text()).items()}
    cat=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','batch','frequency'])
    obj['pinned'] = dict(zip(cat.loc[cat.batch.eq(8),'series_id'],cat.loc[cat.batch.eq(8),'frequency']))
    return obj

def save(sid,frame,*,collector_group=GROUP,**meta):
    old=baseline()
    if old['pinned'] and sid not in old['pinned']:raise Omission('Outside published batch8 selection')
    if sid in old['series_ids'] or meta['dataset_identity'] in old['identities']:raise Omission('Already published identity')
    frame=normalize_frame(frame)
    if frame.value.nunique()<2 or not qualifies_research({'frequency':meta['frequency'],**coverage(frame)}):raise Omission('Constant or inadequate actual history')
    digest=hashlib.sha256(frame.date.astype('int64').to_numpy().tobytes()+frame.value.to_numpy().tobytes()).hexdigest()
    if digest in old['hashes'] or (digest in SEEN and SEEN[digest]!=sid):raise Omission('Duplicate numeric history')
    if sid in old['pinned'] and old['pinned'][sid]!=meta['frequency']:raise ValueError('Published cadence changed')
    SEEN[digest]=sid
    meta['country_codes']=normalize_country_codes(meta.get('country_codes',[]))
    meta['country_code_standard']='ISO 3166-1 alpha-2'
    meta.setdefault('native_frequency',meta['frequency']);meta.setdefault('finer_frequency_available',False)
    meta.setdefault('timestamp_semantics','UTC date label of source observation; not historical availability.')
    meta.setdefault('point_in_time',False)
    return save_candidate(collector_group,sid,frame,**meta)

def attempt(good,sid,frame,**meta):
    try:good.append(save(sid,frame,**meta))
    except Omission as exc:OMISSIONS.append({'series_id':sid,'reason':str(exc)})

def clean(dates,values):
    f=pd.DataFrame({'date':pd.to_datetime(dates,utc=True),'value':pd.to_numeric(values,errors='raise')}).dropna()
    if f.groupby('date').value.nunique().gt(1).any():raise ValueError('Conflicting duplicate date')
    return normalize_frame(f.drop_duplicates('date'))

def parse_indeed(raw):
    df=pd.read_csv(io.BytesIO(raw),dtype={'cbsa_code':str})
    values=[c for c in df if c.startswith('indeed_job_postings_index')]
    if not values or 'date' not in df:raise ValueError('Unknown Indeed schema')
    dimensions=[c for c in df if c not in ['date',*values]]
    for key,part in df.groupby(dimensions,dropna=False,sort=True):
        key=key if isinstance(key,tuple) else (key,)
        dims=dict(zip(dimensions,map(str,key)))
        for col in values:
            yield dims,col,clean(part.date,part[col])

def indeed(fetcher):
    tree_raw,tree_sha=fetcher.get('https://api.github.com/repos/hiring-lab/job_postings_tracker/git/trees/master?recursive=1')
    tree=json.loads(tree_raw);rev=tree['sha']
    if tree.get('truncated'):raise ValueError('Incomplete repository tree')
    prefix=f'https://raw.githubusercontent.com/hiring-lab/job_postings_tracker/{rev}/'
    docs,doc_sha=fetcher.get(prefix+'README.md',max_age_hours=876000)
    _,lic_sha=fetcher.get(prefix+'LICENSE',max_age_hours=876000)
    # Retain US national/sector data, but skip the large already overrepresented metro/state panels.
    files=[x['path'] for x in tree['tree'] if x['path'].endswith('.csv') and '/' in x['path']
           and not x['path'].startswith(('US/metro_','US/state_'))]
    p=ROOT/'archive/intent6_selection/indeed_files.json'
    if p.exists():files=json.loads(p.read_text())['files']
    else:atomic_bytes(p,json.dumps({'files':files},indent=2).encode())
    def download(path):return path,fetcher.get(prefix+path,max_age_hours=876000)
    good=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for path,(raw,sha) in pool.map(download,files):
            code=path.split('/')[0]
            # EA is the provider's euro-area aggregate, whose composition is not encoded in the CSV.
            codes=[code] if code!='EA' else []
            for dims,col,frame in parse_indeed(raw):
                dimstring=json.dumps(dims,sort_keys=True,ensure_ascii=False)
                identity=f'indeed_job_postings|{path}|{dimstring}|{col}'
                sid='indeed6_'+slug(code)+'_'+token(path+'|'+dimstring+'|'+col)
                category='labor_job_postings_sector' if 'sector' in path else ('labor_job_postings_national' if 'aggregate' in path else 'labor_job_postings_regional')
                attempt(good,sid,frame,title=f"Indeed {code} job postings: {', '.join(dims.values())} ({'NSA' if col.endswith('_NSA') else 'SA'})",
                    provider='Indeed Hiring Lab',category=category,unit='index (2020-02-01 = 100)',frequency='daily',
                    source_url=prefix+path,geography=code+': '+', '.join(dims.values()),country_codes=codes,
                    geography_scope='us' if code=='US' else 'non_us',dataset_identity=identity,
                    hypothesis='Hiring demand by occupation and region may lead employment, wages and cyclical activity.',
                    limitations='Seven-day trailing average; latest revised history. Seasonal methodology changed in November 2024. Index is not a vacancy count and is not an Indeed/Recruit revenue indicator. Daily labels are released weekly, not same-day observations available to traders.',
                    expected_publication_lag='Refreshed weekly; exact historical release timestamps not supplied in CSV.',
                    raw_sha256=[sha,tree_sha,doc_sha,lic_sha],license='CC BY 4.0; attribution: Indeed Hiring Lab',
                    source_commit=rev,source_file=path,source_dimensions=dims,source_value_column=col,
                    seasonal_adjustment='not seasonally adjusted' if col.endswith('_NSA') else 'seasonally adjusted',
                    source_refresh_cadence='weekly',revision_policy='Source revises historical indices and seasonal factors.')
            print('INDEED',path,len(good),flush=True)
    return good


def parse_trends(raw):
    rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig'))))
    if not rows:raise Omission('Empty export; absence is not a verified zero')
    if not {'Trends','Search volume','Started','Trend breakdown'}.issubset(rows[0]):raise ValueError('Unknown trends CSV schema')
    # Exact duplicate source rows are one displayed cluster. Same title with different start is distinct.
    unique={tuple(r.get(k,'') for k in ['Trends','Started']):r for r in rows}
    rows=list(unique.values())
    result={'listed_clusters':len(rows)}
    texts=[(r['Trends']+' '+r.get('Trend breakdown','')).casefold() for r in rows]
    for name,pat in TOPICS.items():result[name]=sum(bool(re.search(pat,t,re.I)) for t in texts)
    return result


def trends(fetcher):
    info_raw,info_sha=fetcher.get('https://huggingface.co/api/datasets/aurman/GoogleTrendArchive')
    rev=json.loads(info_raw)['sha'];base=f'https://huggingface.co/datasets/aurman/GoogleTrendArchive/resolve/{rev}/'
    _,doc_sha=fetcher.get(base+'README.md',max_age_hours=876000)
    loc_raw,loc_sha=fetcher.get(base+'Trends_LocationList.csv',max_age_hours=876000)
    _,scraper_sha=fetcher.get(base+'trendsscraper_public.py',max_age_hours=876000)
    raw,sha=fetcher.get(base+'daily_compressed.zip',timeout=600,max_age_hours=876000)
    locations=dict(pd.read_csv(io.BytesIO(loc_raw),dtype=str)[['tag','location']].itertuples(index=False,name=None))
    chosen={k for k in locations if (k in COUNTRY_ALPHA2 or k.split('-')[0] in FOCUS) and k.split('-')[0] in COUNTRY_ALPHA2}
    p=ROOT/'archive/intent6_selection/trends_locations.json'
    if p.exists():chosen=set(json.loads(p.read_text())['locations'])
    else:atomic_bytes(p,json.dumps({'locations':sorted(chosen)},indent=2).encode())
    z=zipfile.ZipFile(io.BytesIO(raw)); members={}; dates=defaultdict(list)
    for name in z.namelist():
        m=re.fullmatch(r'([^/]+)/trending_[^/]+_1d_(\d{8})-(\d{4})\.csv',name)
        if m and m[1] in chosen:
            # For repeated same-day captures use the last capture, never sum them.
            key=(m[1],m[2]); previous=members.get(key)
            if previous is None or m[3]>previous[0]:members[key]=(m[3],name)
    for (loc,day),(clock,name) in sorted(members.items()):
        try:metrics=parse_trends(z.read(name))
        except Omission:continue
        dates[loc].append({'date':pd.Timestamp(day,tz='UTC'),**metrics})
    good=[]
    rules_sha=hashlib.sha256(json.dumps(TOPICS,sort_keys=True).encode()).hexdigest()
    for loc,rows in sorted(dates.items()):
        data=pd.DataFrame(rows);code=loc.split('-')[0]
        for metric in ['listed_clusters',*TOPICS]:
            # Exclude rare-topic zero-filled panels; require real matching attention on 90 days.
            if metric!='listed_clusters' and data[metric].gt(0).sum()<90:continue
            sid=f'trends6_{slug(loc)}_{metric}'
            attempt(good,sid,clean(data.date,data[metric]),title=f'Google Trending Now {locations[loc]}: {metric.replace("_"," ")}',
                provider='GoogleTrendArchive (Urman et al.)',category='search_trending_'+metric,
                unit='displayed trending clusters per daily collection',frequency='daily',source_url=base+'daily_compressed.zip',
                geography=locations[loc],country_codes=[code],geography_scope='us' if code in US_CODES else 'non_us',
                dataset_identity=f'googletrendarchive|daily_exports|{loc}|{metric}|rules_v1',
                hypothesis='Changes in country-level attention to markets, shopping, travel, employment and disruptions may motivate tests of near-term activity and risk.',
                limitations='Author-collected Google Trending Now exports, not a representative sample of all searches, search intensity, sentiment or conversion. Keyword topic rules have multilingual recall/ambiguity limits. Counts refer to clusters displayed in the latest daily capture; clusters can persist across days and topics can overlap. Missing/empty exports are omitted. No regional counts are summed to a country. This raw ZIP is a finite historical archive and may end before the separately processed author dataset.',
                expected_publication_lag='Collection-date label from filename; filename clock timezone and historical public archive release times are unverified. Not point-in-time safe.',
                timestamp_semantics='UTC-normalized collector calendar date, not query onset, source publication time or known UTC capture time.',
                raw_sha256=[sha,info_sha,doc_sha,loc_sha,scraper_sha],license='CC BY 4.0; attribution: GoogleTrendArchive / Aleksandra Urman and coauthors',
                source_commit=rev,source_location_code=loc,source_metric=metric,topic_rules=TOPICS if metric!='listed_clusters' else {},
                topic_rules_sha256=rules_sha,source_refresh_cadence='Finite author dataset; check source revision on refresh',
                revision_policy='Archive replacement may revise historical captures; latest archived author version only.')
    print('TRENDS',len(good),'locations',len(dates),flush=True)
    return good

COLLECTORS={'indeed':indeed,'trends':trends}
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',choices=['all',*COLLECTORS],default='all');p.add_argument('--refresh',action='store_true');args=p.parse_args()
    result={'started_at':pd.Timestamp.now(tz='UTC').isoformat(),'sources':{},'failures':[]}
    for name,fn in COLLECTORS.items():
        if args.source not in ['all',name]:continue
        try:
            good=fn(Fetcher(refresh=args.refresh));result['sources'][name]={'series':len(good),'observations':sum(m['rows'] for m in good),'series_ids':[m['series_id'] for m in good]}
        except Exception as exc:
            import traceback;traceback.print_exc();result['failures'].append({'source':name,'error':str(exc)})
    result['omissions']=OMISSIONS;result['finished_at']=pd.Timestamp.now(tz='UTC').isoformat()
    atomic_bytes(ROOT/'logs'/f'intent6_{args.source}_report.json',json.dumps(result,indent=2).encode())
    print(json.dumps({k:v for k,v in result.items() if k not in ['sources','omissions']},indent=2));raise SystemExit(bool(result['failures']))
if __name__=='__main__':main()
