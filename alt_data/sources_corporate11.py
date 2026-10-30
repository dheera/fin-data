#!/usr/bin/env python3
"""Japan weekly financing/flows and Korean corporate disclosure content.

Sparse corporate events and current Japanese issue snapshots remain supplementary;
only genuinely weekly numerical histories enter the canonical staging directory.
OpenDART requires a query credential: request receipts deliberately redact it.
"""
from __future__ import annotations
import argparse, csv, hashlib, io, json, re, threading, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from expansion_common import ROOT, atomic_bytes, save_candidate, normalize_frame

GROUP = 'corporate11'
MOF_URL = 'https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv'
MOF_PAGE = 'https://www.mof.go.jp/english/policy/international_policy/reference/itn_transactions_in_securities/index.htm'
JPX_BASE = 'https://www.jpx.co.jp/english/markets/statistics-equities/margin/'
DART = 'https://opendart.fss.or.kr/api/'
# Deliberately diverse liquid listed-company panel, not a survivorship-free census.
STOCKS = {
 '005930':'semiconductors','000660':'semiconductors','009150':'electronic_components',
 '006400':'batteries','373220':'batteries','051910':'chemicals','005490':'steel',
 '005380':'automobiles','000270':'automobiles','012330':'auto_components',
 '009540':'shipbuilding','010140':'shipbuilding','042660':'shipbuilding',
 '012450':'defense','047810':'aerospace','034020':'power_equipment',
 '010950':'refining','096770':'energy','011170':'chemicals','066570':'electronics',
 '035420':'internet','035720':'internet','036570':'gaming','259960':'gaming',
 '352820':'entertainment','041510':'entertainment','207940':'biopharma',
 '068270':'biopharma','128940':'pharmaceuticals','069620':'pharmaceuticals',
 '105560':'banking','055550':'banking','086790':'banking','032830':'insurance',
 '003490':'airlines','011200':'shipping','000120':'logistics','015760':'electricity',
 '036460':'gas','033780':'consumer_staples','097950':'food','004370':'food',
 '090430':'cosmetics','139480':'retail','008770':'travel_retail','028260':'construction',
 '030200':'telecom','017670':'telecom',
}

class SafeFetch:
    def __init__(self, root=ROOT, refresh=False):
        self.root=Path(root); self.refresh=refresh; self.local=threading.local()
        self.lock=threading.Lock(); self.last=0.; self.calls=0
        self.key=json.loads(Path('/data/fin/api_keys/opendart.json').read_text())['api_key']
    def get(self, url, params=None, *, auth=False):
        host=urlparse(url).hostname
        if urlparse(url).scheme!='https' or host not in {'opendart.fss.or.kr','www.mof.go.jp','www.jpx.co.jp'}:
            raise ValueError('Unapproved corporate11 download host')
        if auth and host!='opendart.fss.or.kr':raise ValueError('Credential host mismatch')
        params=dict(params or {})
        if 'crtfc_key' in params or self.key in url:raise ValueError('Pass credentials only through auth argument')
        public_params={**params,**({'crtfc_key':'REDACTED'} if auth else {})}
        safe_url=requests.Request('GET',url,params=public_params).prepare().url
        key=hashlib.sha256(safe_url.encode()).hexdigest()
        index=self.root/'archive/requests'/(key+'.json')
        if index.exists() and not self.refresh:
            meta=json.loads(index.read_text()); p=self.root/'archive/raw'/(meta['sha256']+'.bin')
            age=(pd.Timestamp.now(tz='UTC')-pd.Timestamp(meta['retrieved_at'])).total_seconds()
            if p.exists() and 0<=age<24*3600:
                b=p.read_bytes()
                if hashlib.sha256(b).hexdigest()==meta['sha256']:return b,meta['sha256'],meta['retrieved_at']
        if auth:params['crtfc_key']=self.key
        if not hasattr(self.local,'session'):self.local.session=requests.Session()
        for attempt in range(3):
            if auth:
                with self.lock:
                    if self.calls>=7000:raise RuntimeError('OpenDART conservative run request cap reached')
                    delay=max(0.,.16-(time.monotonic()-self.last))
                    if delay:time.sleep(delay)
                    self.last=time.monotonic(); self.calls+=1
            try:r=self.local.session.get(url,params=params,timeout=(15,90),allow_redirects=False,headers={'User-Agent':'FinAltDataResearch/1.0'})
            except requests.RequestException:
                if attempt==2:raise RuntimeError('Corporate11 network request failed; URL details withheld') from None
                time.sleep(2**attempt);continue
            if r.status_code==200:break
            if r.status_code in {429,500,502,503,504} and attempt<2:
                retry=r.headers.get('Retry-After','')
                delay=max(2**attempt,float(retry)) if retry.isdigit() else 2**attempt
                if delay>60:raise RuntimeError('Provider requested extended retry delay')
                time.sleep(delay);continue
            raise RuntimeError('Corporate11 request HTTP '+str(r.status_code))
        b=r.content
        if self.key.encode() in b:raise RuntimeError('Refused response containing credential')
        digest=hashlib.sha256(b).hexdigest(); now=pd.Timestamp.now(tz='UTC').isoformat()
        meta={'url':safe_url,'sha256':digest,'retrieved_at':now,'collector_group':GROUP,
              'content_type':r.headers.get('Content-Type'),'source_last_modified':r.headers.get('Last-Modified'),
              'authentication':'query credential redacted' if auth else 'public'}
        atomic_bytes(self.root/'archive/raw'/(digest+'.bin'),b)
        atomic_bytes(index,json.dumps(meta,indent=2).encode())
        atomic_bytes(self.root/'archive/receipts'/(key+'_'+pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')+'.json'),json.dumps(meta,indent=2).encode())
        return b,digest,now
    def dart(self, endpoint, params=None):
        b,h,t=self.get(DART+endpoint+'.json',params,auth=True)
        try:j=json.loads(b)
        except ValueError:raise ValueError('OpenDART invalid JSON response') from None
        if j.get('status') not in {'000','013'}:raise RuntimeError('OpenDART status '+str(j.get('status')))
        return j,h,t

def supplementary(root, name, frame, meta):
    root=Path(root); path=root/'supplementary'/GROUP/(name+'.parquet')
    table=pa.Table.from_pandas(frame,preserve_index=False)
    table=table.replace_schema_metadata({b'alt_data_supplementary':json.dumps(meta,sort_keys=True).encode()})
    sink=pa.BufferOutputStream();pq.write_table(table,sink,compression='zstd')
    atomic_bytes(path,sink.getvalue().to_pybytes())
    summary={**meta,'path':str(path.relative_to(root)),'rows':len(frame),'parquet_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    atomic_bytes(path.with_suffix('.json'),json.dumps(summary,indent=2).encode());return summary

def parse_mof(raw):
    text=raw.decode('cp932')
    if 'net acquisition with a plus sign' not in text:raise ValueError('MOF current-file net sign convention missing')
    rows=[]
    for r in csv.reader(io.StringIO(text)):
        if not r:continue
        label=r[0].strip()
        m=re.fullmatch(r'(\d{4})．\s*(\d{1,2})．\s*(\d{1,2})[～〜]\s*(?:(\d{4})．\s*)?(\d{1,2})．\s*(\d{1,2})',label)
        if not m:
            if re.match(r'\d{4}',label):raise ValueError('Unparsed MOF date-looking source row')
            continue
        y,sm,sd=map(int,m.groups()[:3]); ey,em,ed=m.groups()[3:];em,ed=int(em),int(ed)
        end_year=int(ey) if ey else y+(em<sm)
        start=pd.Timestamp(year=y,month=sm,day=sd,tz='UTC')
        end=pd.Timestamp(year=end_year,month=em,day=ed,tz='UTC')
        if (end-start).days!=6:raise ValueError('Unexpected MOF weekly period')
        if len(r)!=23:raise ValueError('MOF column count changed')
        vals=[float(x.strip().replace(',','')) for x in r[1:]]
        # Current CSV backcasts positive-acquisition semantics even before 2014.
        # Its source note is authoritative over the legacy landing-page warning.
        for o in [0,3,7,11,14,18]:
            if abs(vals[o]-vals[o+1]-vals[o+2])>1.01:raise ValueError('MOF acquisition/disposition/net inconsistency')
        rows.append({'date':end,'period_start':start,'period_label_original':r[0],**{f'c{i+1}':x for i,x in enumerate(vals)}})
    frame=pd.DataFrame(rows)
    if frame.empty or frame.date.duplicated().any():raise ValueError('MOF empty/duplicate periods')
    return frame.sort_values('date').reset_index(drop=True)

def collect_mof(fetch):
    raw,h,t=fetch.get(MOF_URL); frame=parse_mof(raw); meta=[]
    assets=['equity_acquisition','equity_disposition','equity_net','long_debt_acquisition','long_debt_disposition','long_debt_net','equity_long_debt_net','short_debt_acquisition','short_debt_disposition','short_debt_net','total_net']
    for i,(direction,measure) in enumerate((d,m) for d in ['outward','inward'] for m in assets):
        meta.append(save_candidate(GROUP,f'corporate11_mof_japan_{direction}_{measure}',frame.rename(columns={f'c{i+1}':'value'}),
            title=f'Japan {direction} securities investment {measure.replace("_"," ")}',provider='Japan Ministry of Finance',category='international_portfolio_flows',unit='100 million JPY',frequency='weekly',source_url=MOF_URL,geography='Japan',
            hypothesis='Cross-border equity/debt demand may contain information about yen, bond and equity allocation pressure.',
            limitations='Designated major investors, not all transactions. Current revised history, not historical first releases. Investment-fund classification changes in January 2014. Rounding can make net differ by 1 source unit from acquisition minus disposition. Current CSV explicitly backcasts net acquisition positive; no automatic pre-2014 sign reversal.',
            raw_sha256=h,root=fetch.root,native_frequency='weekly',timestamp_semantics='UTC midnight label of Saturday period end; not release time. Source week runs Sunday through Saturday.',
            publication_delay_days_estimate=5,publication_delay_basis='Normally released the following Thursday; holidays can change the schedule. Approximation only.',
            expected_publication_lag='Approximately five days after Saturday week-end, normally the following Thursday; holidays can change the schedule.',
            country_codes=['JP'],geography_scope='non_us',historical_release_times_available=False,locally_available_at=t,dataset_identity=f'mof_japan_international_transactions_weekly:{direction}:{measure}',source_column=i+1))
    sup=supplementary(fetch.root,'mof_weekly_periods',frame,{'provider':'Japan Ministry of Finance','raw_sha256':[h],'locally_available_at':t,'timestamp_semantics':'Period dates, not publication timestamps. c1..c22 map to canonical metadata source_column.'})
    return meta,[sup]

def workbook_links(fetch,page):
    b,h,t=fetch.get(page)
    return [(urljoin(page,a['href']),h) for a in BeautifulSoup(b,'html.parser').find_all('a',href=True) if re.search(r'\.xlsx?$',a['href'])]

def date_rows(raw):
    f=pd.read_excel(io.BytesIO(raw),header=None)
    mask=f.iloc[:,0].map(lambda x:isinstance(x,(datetime,pd.Timestamp)))
    # Header last-update date is in the final column, not the first.
    d=f.loc[mask].copy()
    if d.empty:raise ValueError('No JPX date-labelled observation rows')
    d[0]=pd.to_datetime(d[0],utc=True)
    for c in d.columns[1:]:d[c]=pd.to_numeric(d[c],errors='coerce')
    # The official accounts workbook repeats 2010-01-29 identically. Only
    # identical complete rows may be removed; conflicting periods fail closed.
    d=d.drop_duplicates()
    if d[0].duplicated().any():raise ValueError('JPX conflicting duplicate period dates')
    return d.sort_values(0).reset_index(drop=True),f

def parse_jpx_premiums(raw):
    f=pd.read_excel(io.BytesIO(raw),header=None,dtype=str)
    if 'per share per day' not in ' '.join(f.fillna('').astype(str).values.ravel()):raise ValueError('JPX premium units changed')
    d=f[f[0].str.fullmatch(r'\d{8}',na=False)].copy()
    d.columns=['trade_date_original','stock_code','issue_name','exchange','over_lent_shares_original','maximum_premium_jpy_original','premium_jpy_original']
    d['trade_date']=pd.to_datetime(d.trade_date_original,format='%Y%m%d',utc=True)
    for orig,new in [('over_lent_shares_original','over_lent_shares'),('maximum_premium_jpy_original','maximum_premium_jpy_per_share_day'),('premium_jpy_original','premium_jpy_per_share_day')]:
        d[new]=pd.to_numeric(d[orig],errors='coerce')
    d['premium_missing_marker']=[original if pd.isna(value) else None for original,value in zip(d.premium_jpy_original,d.premium_jpy_per_share_day)]
    if d.empty or d.duplicated(['trade_date','stock_code']).any():raise ValueError('JPX premium empty/duplicate')
    return d.reset_index(drop=True)

def parse_jpx_designated(raw):
    f=pd.read_excel(io.BytesIO(raw),header=None,dtype=str).fillna('')
    if f.shape[1]!=23:raise ValueError('JPX designated issue column count changed')
    date_rows_=f[f[1].str.fullmatch(r'\d{4}/\d{2}/\d{2}',na=False)]
    if len(date_rows_)!=1:raise ValueError('JPX designated application/publication dates ambiguous')
    application=pd.Timestamp(date_rows_.iloc[0,1]).date().isoformat()
    publication=pd.Timestamp(date_rows_.iloc[0,21]).date().isoformat()
    d=f[f[7].str.fullmatch(r'JP[A-Z0-9]{10}',na=False)].copy()
    names=['lot_code_original','designation_original','lending_alert_original','issue_name','market_section','loan_margin_type_original','issue_code_original','isin',
           'short_shares','short_change_shares','short_percent_of_listed','long_shares','long_change_shares','long_percent_of_listed','short_long_percent',
           'negotiable_short_shares','negotiable_short_change_shares','standardized_short_shares','standardized_short_change_shares',
           'negotiable_long_shares','negotiable_long_change_shares','standardized_long_shares','standardized_long_change_shares']
    d.columns=names
    d['source_cells_json']=[json.dumps(row,ensure_ascii=False,sort_keys=True) for row in d.to_dict('records')]
    for name in names[8:]:
        values=d[name].str.replace(',','',regex=False)
        missing=values.isin(['','*','-','*****'])
        d[name]=pd.to_numeric(values.mask(missing),errors='raise')
    d['application_date']=application;d['publication_date_source']=publication
    d['restricted_issue']=d.designation_original.str.contains('規')
    d['daily_publication_issue']=d.designation_original.str.contains('日')
    d['lending_restriction_flag']=d.lending_alert_original.str.contains('株')
    d['lending_caution_flag']=d.lending_alert_original.str.contains('喚')
    if d.empty or d['isin'].duplicated().any():raise ValueError('JPX designated issue empty or duplicated')
    return d.reset_index(drop=True)

def collect_jpx(fetch):
    sources={};meta=[];sups=[]
    links=workbook_links(fetch,JPX_BASE+'05.html')
    for u,pagehash in links:
        b,h,t=fetch.get(u);d,whole=date_rows(b)
        if whole.shape[1]==13:kind='negotiable_standardized'
        elif whole.shape[1]==22:kind='accounts'
        elif whole.shape[1]==26:kind='new_accounts'
        else:raise ValueError('JPX historical workbook schema changed')
        sources[kind]=(d,h,t,u)
    if not {'negotiable_standardized','accounts'}.issubset(sources):raise ValueError('JPX required historical sources missing')
    # No silent splice across the September 2026 reporting-schema change.
    specs={
      'negotiable_standardized':[(1,'total_short_shares'),(2,'total_short_value'),(3,'total_long_shares'),(4,'total_long_value'),(5,'negotiable_short_shares'),(6,'negotiable_short_value'),(7,'standardized_short_shares'),(8,'standardized_short_value'),(9,'negotiable_long_shares'),(10,'negotiable_long_value'),(11,'standardized_long_shares'),(12,'standardized_long_value')],
      'accounts':[(1,'customer_short_shares'),(2,'customer_short_value'),(3,'customer_long_shares'),(4,'customer_long_value'),(5,'member_short_shares'),(6,'member_short_value'),(7,'member_long_shares'),(8,'member_long_value'),(13,'within_member_matched_shares'),(14,'finance_company_shares_lent_shares'),(15,'finance_company_shares_lent_value'),(16,'finance_company_funds_lent_shares'),(17,'finance_company_funds_lent_value'),(18,'member_shares_lent_shares'),(19,'member_shares_lent_value'),(20,'member_funds_lent_shares'),(21,'member_funds_lent_value')],
    }
    for kind,items in specs.items():
        d,h,t,u=sources[kind]
        for c,name in items:
            frame=d[[0,c]].dropna().rename(columns={0:'date',c:'value'})
            meta.append(save_candidate(GROUP,'corporate11_jpx_japan_margin_'+name,frame,
                title='Japan outstanding margin transactions '+name.replace('_',' '),provider='Japan Exchange Group',category='equity_financing_pressure',unit='million JPY' if name.endswith('_value') else 'thousand shares',frequency='weekly',source_url=u,geography='Japan',
                hypothesis='Leverage, short positions and securities-finance reliance may reveal crowding and forced-flow exposure.',
                limitations='Market aggregates, not individual-stock borrow rates. Tokyo/Osaka/Nagoya through 2013-07-12; Tokyo/Nagoya from 2013-07-19. Current revised history. Genuine missing values retained as absent. Stops at pre-2026-09-25 schema boundary; newer workbook retained separately. Contract-price values for customer/member balances; closing-price values for lending balances.',
                raw_sha256=h,root=fetch.root,native_frequency='weekly',timestamp_semantics='UTC midnight label of source application date; not publication time.',historical_release_times_available=False,
                country_codes=['JP'],geography_scope='non_us',expected_publication_lag='Approximately four days after Friday application date, typically the following Tuesday; estimate, not per-row release time.',
                publication_delay_days_estimate=4,publication_delay_basis='Typically published the following Tuesday; estimate, not per-row release time.',locally_available_at=t,dataset_identity='jpx_margin_weekly_pre20260925:'+name,source_column=c,source_workbook_kind=kind))
    if 'new_accounts' in sources:
        d,h,t,u=sources['new_accounts'];d.columns=['application_date']+[f'source_column_{c}' for c in d.columns[1:]]
        sups.append(supplementary(fetch.root,'jpx_margin_new_schema',d,{'provider':'Japan Exchange Group','source_url':u,'raw_sha256':[h],'locally_available_at':t,'limitation':'New schema from September 25, 2026; too short for canonical history. Original workbook contains precise column labels.'}))
    for u,_ in workbook_links(fetch,JPX_BASE+'02.html'):
        b,h,t=fetch.get(u);d=parse_jpx_premiums(b)
        sups.append(supplementary(fetch.root,'jpx_current_premiums',d,{'provider':'Japan Exchange Group','source_url':u,'raw_sha256':[h],'locally_available_at':t,'frequency':'business_daily_snapshot','limitation':'Current issue snapshot only. ***** is missing/not specified, never zero. Specific Japanese margin-system premiums, not comprehensive securities lending fees. No recurring collector installed.'}))
    for u,_ in workbook_links(fetch,JPX_BASE+'index.html'):
        b,h,t=fetch.get(u);f=pd.read_excel(io.BytesIO(b),header=None,dtype=str).fillna('')
        # Preserve bilingual headers and every original cell for the complicated issue table.
        records=[{'row':int(r),'column':int(c),'cell_original':str(v)} for r,row in f.iterrows() for c,v in row.items() if v!='']
        sups.append(supplementary(fetch.root,'jpx_current_daily_designated_cells',pd.DataFrame(records),{'provider':'Japan Exchange Group','source_url':u,'raw_sha256':[h],'locally_available_at':t,'frequency':'business_daily_snapshot','limitation':'Current designated/restricted issues only. Lossless cell table, including headers and notes; not a full-market historical daily panel.'}))
        sups.append(supplementary(fetch.root,'jpx_current_daily_designated',parse_jpx_designated(b),{'provider':'Japan Exchange Group','source_url':u,'raw_sha256':[h],'locally_available_at':t,'frequency':'business_daily_snapshot','limitation':'Current designated/restricted issues only, not full-market history. Source five-character issue codes preserved alongside ISIN. Amounts in shares/units, ratios in percent; missing ratios retained as null. Application date and source publication calendar date separate; exact publication timestamp not supplied.'}))
    return meta,sups

def discover_dart_endpoints(fetch):
    b,h,t=fetch.get('https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005')
    links=sorted({urljoin('https://opendart.fss.or.kr',a['href']) for a in BeautifulSoup(b,'html.parser').find_all('a',href=True) if 'detail.do?apiGrpCd=DS005' in a['href']})
    out=[]
    for u in links:
        raw,dh,_=fetch.get(u);s=BeautifulSoup(raw,'html.parser');txt=s.get_text(' ',strip=True)
        matches=re.findall(r'https://opendart\.fss\.or\.kr/api/([A-Za-z0-9]+)\.json',txt)
        if len(set(matches))!=1:raise ValueError('Cannot identify OpenDART documented endpoint')
        fields=[];response=False
        for table in s.find_all('table'):
            cells=[[x.get_text(' ',strip=True) for x in tr.find_all(['th','td'])] for tr in table.find_all('tr')]
            if cells and any('응답키' in ' '.join(row) for row in cells):
                for row in cells:
                    if len(row)>=3 and re.fullmatch(r'[A-Za-z0-9_]+',row[0]):fields.append({'field':row[0],'label_ko':row[1],'description_ko':row[2]})
        out.append({'endpoint':matches[0],'guide_url':u,'guide_sha256':dh,'fields':fields})
    if len(out)!=36:raise ValueError('OpenDART major-event endpoint count changed')
    return out

def parse_corporations(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names=[n for n in z.namelist() if n.upper().endswith('.XML')]
        if len(names)!=1:raise ValueError('Unexpected corporation-code archive')
        tree=ET.fromstring(z.read(names[0]))
    return pd.DataFrame([{x.tag:(x.text or '').strip() for x in row} for row in tree.findall('list')])

def normalize_disclosure(row, stock, sector, raw_hash, collected_at):
    if not re.fullmatch(r'\d{14}',row.get('rcept_no','')):raise ValueError('Invalid DART receipt number')
    original=dict(row)
    r={**row,'stock_code':row.get('stock_code') or stock,'sector_panel':sector,'raw_sha256':raw_hash,'locally_available_at':collected_at}
    # Filing date is a local calendar date, deliberately no fabricated UTC timestamp.
    r['filing_date']=pd.Timestamp(row['rcept_dt']).date().isoformat()
    r['historical_release_time_utc']=None
    r['is_amendment']=bool(re.search('정정|첨부추가|발행조건확정|기재정정',row.get('report_nm','')))
    r['later_correction_flag']='U' in row.get('rm','')
    r['withdrawn_flag']='W' in row.get('rm','')
    r['source_record_json']=json.dumps(original,ensure_ascii=False,sort_keys=True)
    return r

def parse_numeric_original(value):
    if not isinstance(value,str):return None
    s=value.strip().replace(',','')
    # Accept only plain source numeric strings. Units/percent symbols and dates
    # are never guessed or stripped into unrelated magnitudes.
    if not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)',s):return None
    v=float(s)
    return v if np.isfinite(v) else None

def event_rows(endpoint, source_rows, stock, sector, raw_hash, collected_at, disclosure_map):
    result=[]
    for i,row in enumerate(source_rows):
        receipt=str(row.get('rcept_no',''))
        if not re.fullmatch(r'\d{14}',receipt):raise ValueError('DART structured event missing receipt')
        disclosure=disclosure_map.get(receipt,{})
        result.append({'endpoint':endpoint,'row_in_response':i,'corp_code':row.get('corp_code'),
            'corp_name':row.get('corp_name'),'stock_code':stock,'sector_panel':sector,'rcept_no':receipt,
            'filing_date':disclosure.get('filing_date'), 'receipt_date_component':pd.Timestamp(receipt[:8]).date().isoformat(),
            'report_nm':disclosure.get('report_nm'),'is_amendment':disclosure.get('is_amendment'),
            'later_correction_flag':disclosure.get('later_correction_flag'),'withdrawn_flag':disclosure.get('withdrawn_flag'),
            'raw_sha256':raw_hash,'locally_available_at':collected_at,
            'source_record_json':json.dumps(row,ensure_ascii=False,sort_keys=True)})
    return result

def collect_dart(fetch,workers=4):
    raw,h,t=fetch.get(DART+'corpCode.xml',auth=True);corporations=parse_corporations(raw)
    selected=corporations[corporations.stock_code.isin(STOCKS)].copy()
    if selected.stock_code.duplicated().any():raise ValueError('Ambiguous selected stock mapping')
    selected['sector_panel']=selected.stock_code.map(STOCKS)
    supmeta=[supplementary(fetch.root,'dart_company_panel',selected,{'provider':'Korea FSS OpenDART','raw_sha256':[h],'locally_available_at':t,'selection':'48 diverse selected Korean listed companies; selection based on current listings, contains survivorship/selection bias. Not an index or census.'})]
    endpoints=discover_dart_endpoints(fetch)
    atomic_bytes(fetch.root/'supplementary'/GROUP/'dart_event_dictionary.json',json.dumps(endpoints,ensure_ascii=False,indent=2).encode())
    end=pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y%m%d');start='20150101'
    all_disclosures=[];all_events=[];failures=[];company_summaries=[]
    def company(record):
        stock=record['stock_code'];corp=record['corp_code'];sector=record['sector_panel']
        disclosures=[];events=[];issues=[];page=1;total=None
        while True:
            j,rh,rt=fetch.dart('list',{'corp_code':corp,'bgn_de':start,'end_de':end,'last_reprt_at':'N','sort':'date','sort_mth':'asc','page_count':100,'page_no':page})
            if j['status']=='013':break
            if total is None:total=int(j['total_count'])
            elif total!=int(j['total_count']):raise ValueError('DART list changed during pagination')
            if any(x.get('corp_code')!=corp for x in j['list']):raise ValueError('DART disclosure corporation mismatch')
            disclosures.extend(normalize_disclosure(x,stock,sector,rh,rt) for x in j['list'])
            if page>=int(j['total_page']):break
            if page>=200:raise ValueError('DART company pagination cap exceeded')
            page+=1
        if total is not None and len(disclosures)!=total:raise ValueError('Incomplete DART list pagination')
        if len({x['rcept_no'] for x in disclosures})!=len(disclosures):raise ValueError('Duplicate DART receipt in company list')
        by_receipt={x['rcept_no']:x for x in disclosures}
        for ep in endpoints:
            try:
                j,rh,rt=fetch.dart(ep['endpoint'],{'corp_code':corp,'bgn_de':start,'end_de':end})
                if any(x.get('corp_code')!=corp for x in j.get('list',[])):raise ValueError('DART structured corporation mismatch')
                events.extend(event_rows(ep['endpoint'],j.get('list',[]),stock,sector,rh,rt,by_receipt))
            except (RuntimeError,ValueError) as exc:
                # All our request errors are sanitized and contain no URLs/secrets.
                issues.append({'stock_code':stock,'endpoint':ep['endpoint'],'reason':str(exc)})
        folder=fetch.root/'supplementary'/GROUP/'company_parts';folder.mkdir(parents=True,exist_ok=True)
        # Checkpoints allow the long acquisition to be inspected and reused.
        atomic_bytes(folder/(stock+'.json'),json.dumps({'disclosures':disclosures,'events':events,'failures':issues},ensure_ascii=False).encode())
        return stock,disclosures,events,issues
    with ThreadPoolExecutor(max_workers=workers) as pool:
        jobs={pool.submit(company,row):row['stock_code'] for row in selected.to_dict('records')}
        for future in as_completed(jobs):
            stock=jobs[future]
            try:
                stock,ds,es,fs=future.result();all_disclosures.extend(ds);all_events.extend(es);failures.extend(fs)
                company_summaries.append({'stock_code':stock,'disclosures':len(ds),'structured_events':len(es),'failures':len(fs)})
                print('DART company',stock,'disclosures',len(ds),'structured_events',len(es),'failures',len(fs),flush=True)
            except (RuntimeError,ValueError) as exc:failures.append({'stock_code':stock,'reason':str(exc)})
            atomic_bytes(fetch.root/'logs/corporate11_dart_progress.json',json.dumps({'completed_companies':len(company_summaries),'selected_companies':len(selected),'disclosures':len(all_disclosures),'structured_events':len(all_events),'failures':failures,'updated_at':pd.Timestamp.now(tz='UTC').isoformat()},indent=2).encode())
    common={'provider':'Korea FSS OpenDART','geography':'South Korea','frequency':'irregular_event',
        'source_url':'https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS005','country_codes':['KR'],'geography_scope':'non_us',
        'requested_start':start,'requested_end':end,'selected_companies':len(selected),
        'timestamp_semantics':'filing_date is a Korea local calendar date from disclosure index. Exact historical release timestamp unknown. receipt_date_component is separately named and never treated as an exact publication time.',
        'limitations':'Currently listed selected-company panel, not survivorship-free. All available index revisions retained. Structured endpoints are current extracted report content; historical as-known amounts are not guaranteed. Do not forward-fill these records into daily histories or sum amendments as independent economic events.'}
    ds=pd.DataFrame(all_disclosures).sort_values(['filing_date','rcept_no']).reset_index(drop=True)
    es=pd.DataFrame(all_events).sort_values(['receipt_date_component','rcept_no','endpoint','row_in_response']).reset_index(drop=True)
    supmeta.append(supplementary(fetch.root,'dart_disclosures',ds,{**common,'raw_sha256':sorted(set(ds.raw_sha256)),'company_results':company_summaries,'failures':failures}))
    supmeta.append(supplementary(fetch.root,'dart_structured_events',es,{**common,'raw_sha256':sorted(set(es.raw_sha256)),'event_dictionary':'dart_event_dictionary.json'}))
    # Tidy content values retain source strings, field labels, numeric parse only
    # when unambiguous, and original currency fields in the parent record.
    dictionary={x['endpoint']:{f['field']:f for f in x['fields']} for x in endpoints}
    fields=[]
    for idx,event in es.iterrows():
        for field,original in json.loads(event.source_record_json).items():
            if field in {'rcept_no','corp_code','corp_name','corp_cls'}:continue
            label=dictionary[event.endpoint].get(field,{}).get('label_ko')
            unit='KRW' if label and re.search(r'\(원\)',label) else ('percent' if label and '%' in label else ('shares' if label and '(주)' in label else None))
            fields.append({'event_row':idx,'endpoint':event.endpoint,'rcept_no':event.rcept_no,'stock_code':event.stock_code,'field':field,'field_label_ko':label,'value_original':str(original),'numeric_value':parse_numeric_original(original),'unit_from_documented_label':unit})
    supmeta.append(supplementary(fetch.root,'dart_event_fields',pd.DataFrame(fields),{**common,'raw_sha256':sorted(set(es.raw_sha256)),'parent_table':'dart_structured_events.parquet','units':'Only explicit documented (원), %, (주) labels mapped; other units/currency retained in original fields. Numeric parsing does not establish that a field is a magnitude.'}))
    # Retrieve original complete filings for recent contractual/business-result
    # and capital-action content, including corrections. Bounded 8 per company.
    selected_docs=[]
    for stock,group in ds.groupby('stock_code'):
        candidates=group[group.report_nm.str.contains('공급계약|잠정|영업실적|자기주식|유상증자|전환사채|합병',regex=True)]
        selected_docs.extend(candidates.sort_values('filing_date',ascending=False).head(8).to_dict('records'))
    docs=[]
    def document(row):
        b,rh,rt=fetch.get(DART+'document.xml',{'rcept_no':row['rcept_no']},auth=True)
        try:
            with zipfile.ZipFile(io.BytesIO(b)) as z:
                members=[]
                for name in z.namelist():
                    blob=z.read(name)
                    text=None
                    for encoding in ['utf-8','cp949']:
                        try:text=blob.decode(encoding);break
                        except UnicodeDecodeError:pass
                    if text is not None:members.append({'member_name':name,'document_text':text})
        except zipfile.BadZipFile:
            try:status=ET.fromstring(b).findtext('status')
            except ET.ParseError:status='unknown'
            if status=='014':raise ValueError('OpenDART status 014: provider reports original filing file does not exist') from None
            raise ValueError('OpenDART original filing unavailable as ZIP; provider status '+str(status)) from None
        return [{'rcept_no':row['rcept_no'],'stock_code':row['stock_code'],'filing_date':row['filing_date'],'report_nm':row['report_nm'],'raw_sha256':rh,'locally_available_at':rt,**member} for member in members]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        jobs={pool.submit(document,row):row['rcept_no'] for row in selected_docs}
        for future in as_completed(jobs):
            try:docs.extend(future.result())
            except (RuntimeError,ValueError) as exc:failures.append({'rcept_no':jobs[future],'stage':'original_document','reason':str(exc)})
    if docs:
        df=pd.DataFrame(docs).sort_values(['rcept_no','member_name']).reset_index(drop=True)
        supmeta.append(supplementary(fetch.root,'dart_original_documents',df,{**common,'raw_sha256':sorted(set(df.raw_sha256)),
            'requested_filings':len(selected_docs),'unavailable_requested_filings':[x for x in failures if x.get('stage')=='original_document'],
            'selection':'Latest eight matching contract/business-result/buyback/issuance/merger filings per company; bounded document-content sample, not exhaustive history. Source unavailable files explicitly recorded.'}))
    return [],supmeta,failures

def collect_ownership(fetch,workers=4):
    """Public insider/large-holder disclosure content, kept as irregular events."""
    panel=pd.read_parquet(fetch.root/'supplementary'/GROUP/'dart_company_panel.parquet')
    guide_hashes=[]
    for identifier in ['2019021','2019022']:
        _,h,_=fetch.get('https://opendart.fss.or.kr/guide/detail.do',{'apiGrpCd':'DS004','apiId':identifier});guide_hashes.append(h)
    rows=[];failures=[]
    def work(record,endpoint):
        j,h,t=fetch.dart(endpoint,{'corp_code':record['corp_code']});out=[]
        for source in j.get('list',[]):
            if source.get('corp_code')!=record['corp_code']:raise ValueError('DART ownership corporation mismatch')
            receipt=str(source.get('rcept_no',''))
            if not re.fullmatch(r'\d{14}',receipt):raise ValueError('Invalid ownership receipt')
            row={**{k:str(v) for k,v in source.items()},'endpoint':endpoint,'stock_code':record['stock_code'],
                 'sector_panel':record['sector_panel'],'filing_date':pd.Timestamp(source['rcept_dt']).date().isoformat(),
                 'raw_sha256':h,'locally_available_at':t,'source_record_json':json.dumps(source,ensure_ascii=False,sort_keys=True)}
            for field in ['stkqy','stkqy_irds','stkrt','stkrt_irds','ctr_stkqy','ctr_stkrt','sp_stock_lmp_cnt','sp_stock_lmp_irds_cnt','sp_stock_lmp_rate','sp_stock_lmp_irds_rate']:
                if field in source:row[field+'_numeric']=parse_numeric_original(source[field])
            out.append(row)
        return out
    with ThreadPoolExecutor(max_workers=workers) as pool:
        jobs={pool.submit(work,r,e):(r['stock_code'],e) for r in panel.to_dict('records') for e in ['majorstock','elestock']}
        for future in as_completed(jobs):
            try:rows.extend(future.result())
            except (ValueError,RuntimeError) as exc:failures.append({'stock_code':jobs[future][0],'endpoint':jobs[future][1],'reason':str(exc)})
    frame=pd.DataFrame(rows).sort_values(['filing_date','rcept_no','endpoint']).reset_index(drop=True)
    meta={'provider':'Korea FSS OpenDART','frequency':'irregular_event','raw_sha256':sorted(set(frame.raw_sha256)),
          'observed_start':frame.filing_date.min(),'observed_end':frame.filing_date.max(),
          'documentation_raw_sha256':guide_hashes,'selected_companies':len(panel),'failures':failures,
          'timestamp_semantics':'Source filing_date is a Korean calendar date, not an exact release timestamp. Ownership counts/changes are reported events, not daily positions or executed trades.',
          'limitations':'Current structured extraction of historical filings for a selected currently listed-company panel. API accepts company only; coverage is the returned source window, not an unlimited historical archive. Reporting changes may include non-market transfers, exercises, corrections and other events. Do not interpret all changes as insider purchases/sales. Missing fields not zero. Full original field strings retained beside approximate float numeric parses; exact amounts are in original strings.'}
    return [],[supplementary(fetch.root,'dart_ownership_events',frame,meta)],failures

def audit(root=ROOT):
    root=Path(root);checks=[];raw_checked=set()
    def read(h):
        b=(root/'archive/raw'/(h+'.bin')).read_bytes()
        if hashlib.sha256(b).hexdigest()!=h:raise ValueError('Raw hash mismatch')
        raw_checked.add(h);return b
    for path in sorted((root/'archive/staging'/GROUP).glob('*.parquet')):
        m=json.loads(path.with_suffix('.json').read_text());actual=pd.read_parquet(path)
        raw=read(m['raw_sha256'][0])
        if 'source_workbook_kind' in m:
            f,_=date_rows(raw);expected=normalize_frame(f[[0,m['source_column']]].dropna().rename(columns={0:'date',m['source_column']:'value'}))
        else:
            f=parse_mof(raw);expected=normalize_frame(f.rename(columns={f'c{m["source_column"]}':'value'}))
        pd.testing.assert_frame_equal(actual,expected)
        checks.append({'series_id':m['series_id'],'rows':len(actual),'raw_reconstruction_equal':True,'parquet_sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    supplementary_checks=[]
    folder=root/'supplementary'/GROUP
    for path in sorted(folder.glob('*.parquet')):
        meta=json.loads(path.with_suffix('.json').read_text());df=pd.read_parquet(path)
        if len(df)!=meta['rows'] or hashlib.sha256(path.read_bytes()).hexdigest()!=meta['parquet_sha256']:raise ValueError('Supplementary file mismatch')
        for h in meta.get('raw_sha256',[]):read(h)
        supplementary_checks.append({'path':str(path.relative_to(root)),'rows':len(df),'raw_hashes_verified':len(meta.get('raw_sha256',[]))})
    for name in ['dart_disclosures','dart_structured_events','dart_ownership_events']:
        path=folder/(name+'.parquet')
        if not path.exists():continue
        frame=pd.read_parquet(path)
        panel=pd.read_parquet(folder/'dart_company_panel.parquet').set_index('corp_code').stock_code.to_dict()
        cache={}
        for row in frame.itertuples(index=False):
            if row.raw_sha256 not in cache:
                obj=json.loads(read(row.raw_sha256));cache[row.raw_sha256]={json.dumps(x,ensure_ascii=False,sort_keys=True) for x in obj.get('list',[])}
            if row.source_record_json not in cache[row.raw_sha256]:raise ValueError('DART event not reconstructible from response')
            source=json.loads(row.source_record_json)
            if panel.get(source.get('corp_code'))!=row.stock_code:raise ValueError('DART stock/corporation mapping mismatch')
            if source.get('rcept_dt') and row.filing_date!=pd.Timestamp(source['rcept_dt']).date().isoformat():raise ValueError('DART source filing date mismatch')
        if name=='dart_disclosures' and frame.rcept_no.duplicated().any():raise ValueError('Duplicate disclosure receipt')
    # Reconstruct supplementary observation tables, not merely their checksums.
    for name in ['mof_weekly_periods','jpx_current_premiums','dart_company_panel','jpx_margin_new_schema','jpx_current_daily_designated_cells','jpx_current_daily_designated']:
        path=folder/(name+'.parquet')
        if not path.exists():continue
        meta=json.loads(path.with_suffix('.json').read_text());raw=read(meta['raw_sha256'][0]);actual=pd.read_parquet(path)
        if name=='mof_weekly_periods':expected=parse_mof(raw)
        elif name=='jpx_current_premiums':expected=parse_jpx_premiums(raw)
        elif name=='jpx_current_daily_designated':expected=parse_jpx_designated(raw)
        elif name=='dart_company_panel':
            expected=parse_corporations(raw);expected=expected[expected.stock_code.isin(STOCKS)].copy();expected['sector_panel']=expected.stock_code.map(STOCKS);expected=expected.reset_index(drop=True)
        elif name=='jpx_margin_new_schema':
            expected,_=date_rows(raw);expected.columns=['application_date']+[f'source_column_{c}' for c in expected.columns[1:]]
        else:
            f=pd.read_excel(io.BytesIO(raw),header=None,dtype=str).fillna('')
            expected=pd.DataFrame([{'row':int(r),'column':int(c),'cell_original':str(v)} for r,row in f.iterrows() for c,v in row.items() if v!=''])
        pd.testing.assert_frame_equal(actual.reset_index(drop=True),expected.reset_index(drop=True))
    path=folder/'dart_original_documents.parquet'
    if path.exists():
        docs=pd.read_parquet(path)
        for h,group in docs.groupby('raw_sha256'):
            with zipfile.ZipFile(io.BytesIO(read(h))) as z:
                for r in group.itertuples(index=False):
                    blob=z.read(r.member_name)
                    decoded=None
                    for encoding in ['utf-8','cp949']:
                        try:decoded=blob.decode(encoding);break
                        except UnicodeDecodeError:pass
                    if decoded!=r.document_text:raise ValueError('Original DART document mismatch')
    path=folder/'dart_event_fields.parquet'
    if path.exists():
        events=pd.read_parquet(folder/'dart_structured_events.parquet')
        source_rows=[json.loads(v) for v in events.source_record_json]
        fields=pd.read_parquet(path)
        for row in fields.itertuples(index=False):
            original=source_rows[row.event_row][row.field]
            if str(original)!=row.value_original:raise ValueError('DART field original differs from raw event')
            value=parse_numeric_original(original)
            if value is None:
                if not pd.isna(row.numeric_value):raise ValueError('DART missing/text field acquired numeric value')
            elif row.numeric_value!=value:raise ValueError('DART field numeric value differs from raw event')
    source_hash=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    outcomes=acquisition_outcomes(root)
    if outcomes['errors']:raise ValueError('Unresolved corporate11 acquisition failures; see part summaries')
    report={'status':'passed','audited_at':pd.Timestamp.now(tz='UTC').isoformat(),'source_sha256':source_hash,
            'canonical_series':len(checks),'canonical_observations':sum(x['rows'] for x in checks),'raw_objects_verified':len(raw_checked),
            'canonical_checks':checks,'series_checks':checks,'supplementary_checks':supplementary_checks,**outcomes}
    atomic_bytes(root/'logs/corporate11_source_audit.json',json.dumps(report,indent=2).encode());return report

def acquisition_outcomes(root):
    errors=[];coverage_gaps=[]
    for part in ['japan','dart','ownership']:
        p=Path(root)/'logs'/('corporate11_'+part+'_summary.json')
        if not p.exists():continue
        for issue in json.loads(p.read_text()).get('failures',[]):
            item={'part':part,**issue}
            if issue.get('stage')=='original_document' and 'status 014' in issue.get('reason',''):coverage_gaps.append(item)
            else:errors.append(item)
    return {'errors':errors,'coverage_gaps':coverage_gaps}

def inventory(root=ROOT):
    root=Path(root);files=[];supplementary_files=[]
    for p in sorted((root/'archive/staging'/GROUP).glob('*.parquet')):
        m=json.loads(p.with_suffix('.json').read_text())
        files.append({'series_id':m['series_id'],'path':str(p.relative_to(root)),
                      'parquet_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
                      'metadata_sha256':hashlib.sha256(p.with_suffix('.json').read_bytes()).hexdigest()})
    for p in sorted((root/'supplementary'/GROUP).rglob('*')):
        if p.is_file():supplementary_files.append({'path':str(p.relative_to(root)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    report={'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'source_path':'sources_corporate11.py',
            'created_at':pd.Timestamp.now(tz='UTC').isoformat(),'files':files,'supplementary_files':supplementary_files,**acquisition_outcomes(root)}
    atomic_bytes(root/'logs/corporate11_inventory.json',json.dumps(report,indent=2).encode());return report

def main():
    p=argparse.ArgumentParser();p.add_argument('--part',choices=['all','japan','dart','ownership','audit'],default='all');p.add_argument('--workers',type=int,default=4);p.add_argument('--refresh',action='store_true');args=p.parse_args()
    if args.part=='audit':
        report=audit();inventory();print(json.dumps({k:v for k,v in report.items() if k not in {'canonical_checks','series_checks','supplementary_checks'}},indent=2));return
    fetch=SafeFetch(refresh=args.refresh);metas=[];sups=[];failures=[]
    if args.part in {'all','japan'}:
        for collect in [collect_mof,collect_jpx]:
            m,s=collect(fetch);metas.extend(m);sups.extend(s);print(collect.__name__,len(m),'series',flush=True)
    if args.part in {'all','dart'}:
        m,s,f=collect_dart(fetch,workers=args.workers);metas.extend(m);sups.extend(s);failures.extend(f)
    if args.part in {'all','ownership'}:
        m,s,f=collect_ownership(fetch,workers=args.workers);metas.extend(m);sups.extend(s);failures.extend(f)
    result={'completed_at':pd.Timestamp.now(tz='UTC').isoformat(),'part':args.part,'staged_series':len(metas),'observations':sum(x['rows'] for x in metas),'supplementary':sups,'failures':failures,'api_requests':fetch.calls}
    atomic_bytes(fetch.root/'logs'/('corporate11_'+args.part+'_summary.json'),json.dumps(result,ensure_ascii=False,indent=2).encode())
    print('COMPLETE',args.part,'series',len(metas),'supplementary tables',len(sups),'failures',len(failures),flush=True)
    print('AUDIT',audit()['status'],flush=True)

if __name__=='__main__':main()
