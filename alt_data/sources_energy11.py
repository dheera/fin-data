#!/usr/bin/env python3
"""ENTSO-E power and GIE gas/LNG histories, with credential-safe provenance.

Only requested historical delivery intervals are staged. Forecasts are latest
retrospective estimates, not an archive of originally published forecast vintages.
ENTSO-E A03 variable blocks are decoded per the official code list; no missing
A01 intervals are filled. GIE no-data reports and missing numeric fields are omitted.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
from itertools import zip_longest
import json
import math
from pathlib import Path
import re
import threading
import time
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import zipfile

import numpy as np
import pandas as pd
import requests

from expansion_common import ROOT, atomic_bytes, archive_raw_manifest, coverage, normalize_frame, qualifies_research, save_candidate

GROUP = 'energy11'
ENTSOE = 'https://web-api.tp.entsoe.eu/api'
GIE_MANUAL = 'https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf'
ENTSOE_CODES = 'https://www.entsoe.eu/Documents/EDI/Library/Core/entso-e-code-list-v36r0.pdf'
SUPPLEMENT = ROOT / 'supplementary' / GROUP
START = '2024-01-01'
AREAS = {
    'DE_LU': ('10Y1001A1001A82H', 'Germany–Luxembourg', ['DE', 'LU']),
    'FR': ('10YFR-RTE------C', 'France', ['FR']),
    'ES': ('10YES-REE------0', 'Spain', ['ES']),
    'PT': ('10YPT-REN------W', 'Portugal', ['PT']),
    'BE': ('10YBE----------2', 'Belgium', ['BE']),
    'NL': ('10YNL----------L', 'Netherlands', ['NL']),
    'AT': ('10YAT-APG------L', 'Austria', ['AT']),
    'CH': ('10YCH-SWISSGRIDZ', 'Switzerland', ['CH']),
    'PL': ('10YPL-AREA-----S', 'Poland', ['PL']),
    'CZ': ('10YCZ-CEPS-----N', 'Czechia', ['CZ']),
    'SK': ('10YSK-SEPS-----K', 'Slovakia', ['SK']),
    'HU': ('10YHU-MAVIR----U', 'Hungary', ['HU']),
    'RO': ('10YRO-TEL------P', 'Romania', ['RO']),
    'BG': ('10YCA-BULGARIA-R', 'Bulgaria', ['BG']),
    'GR': ('10YGR-HTSO-----Y', 'Greece', ['GR']),
    'HR': ('10YHR-HEP------M', 'Croatia', ['HR']),
    'SI': ('10YSI-ELES-----O', 'Slovenia', ['SI']),
    'FI': ('10YFI-1--------U', 'Finland', ['FI']),
    'EE': ('10Y1001A1001A39I', 'Estonia', ['EE']),
    'LV': ('10YLV-1001A00074', 'Latvia', ['LV']),
    'LT': ('10YLT-1001A0008Q', 'Lithuania', ['LT']),
    'DK1': ('10YDK-1--------W', 'Denmark west', ['DK']),
    'DK2': ('10YDK-2--------M', 'Denmark east', ['DK']),
    'SE1': ('10Y1001A1001A44P', 'Sweden SE1', ['SE']),
    'SE2': ('10Y1001A1001A45N', 'Sweden SE2', ['SE']),
    'SE3': ('10Y1001A1001A46L', 'Sweden SE3', ['SE']),
    'SE4': ('10Y1001A1001A47J', 'Sweden SE4', ['SE']),
    'NO1': ('10YNO-1--------2', 'Norway NO1', ['NO']),
    'NO2': ('10YNO-2--------T', 'Norway NO2', ['NO']),
    'NO3': ('10YNO-3--------J', 'Norway NO3', ['NO']),
    'NO4': ('10YNO-4--------9', 'Norway NO4', ['NO']),
    'NO5': ('10Y1001A1001A48H', 'Norway NO5', ['NO']),
    'IT_NORD': ('10Y1001A1001A73I', 'Italy north', ['IT']),
    'IT_SUD': ('10Y1001A1001A788', 'Italy south', ['IT']),
    'IE': ('10YIE-1001A00010', 'Ireland', ['IE']),
    'GB': ('10YGB----------A', 'Great Britain', ['GB']),
}
PSR = dict(zip([f'B{i:02}' for i in range(1,21)], [
    'biomass','lignite','coal_derived_gas','fossil_gas','hard_coal','fossil_oil',
    'oil_shale','peat','geothermal','hydro_pumped_storage','hydro_run_of_river',
    'hydro_reservoir','marine','nuclear','other_renewable','solar','waste',
    'wind_offshore','wind_onshore','other']))
KINDS = {
    'actual_load': {'documentType':'A65','processType':'A16'},
    'forecast_load_day_ahead': {'documentType':'A65','processType':'A01'},
    'actual_generation': {'documentType':'A75','processType':'A16'},
    'forecast_wind_solar_day_ahead': {'documentType':'A69','processType':'A01'},
    'day_ahead_price': {'documentType':'A44'},
}
GIE_METRICS = {
    'agsi': {'gasInStorage':('gas_in_storage','TWh'), 'full':('storage_full','percent'),
             'injection':('injection','GWh/day'),'withdrawal':('withdrawal','GWh/day'),
             'netWithdrawal':('net_withdrawal','GWh/day'),'workingGasVolume':('working_gas_volume','TWh'),
             'injectionCapacity':('injection_capacity','GWh/day'),'withdrawalCapacity':('withdrawal_capacity','GWh/day'),
             'contractedCapacity':('contracted_capacity','TWh'),'availableCapacity':('available_capacity','TWh')},
    'alsi': {'inventory.lng':('lng_inventory','1000 m3 LNG'),'inventory.gwh':('gas_inventory','GWh'),
             'sendOut':('send_out','GWh/day'),'dtmi.lng':('maximum_lng_inventory','1000 m3 LNG'),
             'dtmi.gwh':('maximum_gas_inventory','GWh'),'dtrs':('reference_send_out_capacity','GWh/day')},
}


class AccessError(RuntimeError):
    pass


class NoData(RuntimeError):
    pass


def slug(value):
    return re.sub(r'[^a-z0-9]+','_',str(value).lower()).strip('_')


def gie_country_code(country):
    # GIE uses GB* for the post-Brexit reporting group, not a new country.
    return 'GB' if country in {'GB*','GB','UK'} else country


class SafeFetch:
    """Never persist auth parameters, exception URLs, headers, or reflected keys."""
    def __init__(self, root=ROOT, refresh=False, keys=None):
        self.root=Path(root); self.refresh=refresh; self.lock=threading.Lock(); self.local=threading.local()
        self.keys=keys or {p:json.loads((Path('/data/fin/api_keys')/(p+'.json')).read_text())['api_key'] for p in ['entsoe','gie']}
        self.requests=0; self.last=0.;self.blocked=set()

    def get(self,url,params=None,*,provider=None,max_age_hours=24*365):
        host=urlparse(url).hostname
        if urlparse(url).scheme!='https' or host not in {'web-api.tp.entsoe.eu','agsi.gie.eu','alsi.gie.eu','www.gie.eu','www.entsoe.eu'}:
            raise ValueError('Unapproved energy download host')
        params=dict(params or {})
        if provider in self.blocked:raise AccessError('Provider disabled after an authentication rejection')
        if any(k.lower() in {'securitytoken','apikey','api_key','x-key'} for k in params):
            raise ValueError('Authentication belongs to the private transport only')
        safeurl=requests.Request('GET',url,params=params).prepare().url
        if any(k in safeurl for k in self.keys.values()):raise ValueError('Credential detected in public URL')
        ident=hashlib.sha256(safeurl.encode()).hexdigest()
        index=self.root/'archive/requests'/(ident+'.json')
        if index.exists() and not self.refresh:
            m=json.loads(index.read_text());p=self.root/'archive/raw'/(m['sha256']+'.bin')
            age=(pd.Timestamp.now(tz='UTC')-pd.Timestamp(m['retrieved_at'])).total_seconds()/3600
            if p.exists() and 0<=age<max_age_hours:
                b=p.read_bytes()
                if hashlib.sha256(b).hexdigest()==m['sha256']:return b,m['sha256']
        headers={'User-Agent':'FinAltDataResearch/1.0'}
        if provider=='entsoe':params['securityToken']=self.keys['entsoe']
        elif provider=='gie':headers['x-key']=self.keys['gie']
        if not hasattr(self.local,'session'):self.local.session=requests.Session()
        for attempt in range(3):
            with self.lock:
                if self.requests>=2400:raise RuntimeError('Per-run bounded request budget reached')
                delay=max(0,.24-(time.monotonic()-self.last))
                if delay:time.sleep(delay)
                self.last=time.monotonic();self.requests+=1
            try:r=self.local.session.get(url,params=params,headers=headers,timeout=(15,120),allow_redirects=provider is None)
            except requests.RequestException:
                if attempt<2:time.sleep(2**attempt);continue
                raise RuntimeError('Energy network request failed; private transport details withheld') from None
            if r.status_code in {401,403}:
                self.blocked.add(provider)
                raise AccessError('Provider access rejected HTTP '+str(r.status_code))
            if r.status_code in {429,500,502,503,504} and attempt<2:
                retry=r.headers.get('Retry-After','')
                delay=float(retry) if retry.isdigit() else 2**(attempt+1)
                if delay>60:raise RuntimeError('Provider requested a long retry delay')
                time.sleep(delay);continue
            b=r.content
            if any(k.encode() in b for k in self.keys.values()):raise AccessError('Response reflected a credential; not archived')
            if provider=='gie' and b.lstrip().startswith(b'{'):
                o=json.loads(b)
                if o.get('error') or 'Invalid or missing API key' in str(o.get('message','')):
                    self.blocked.add(provider)
                    raise AccessError('GIE endpoint access denied')
            # ENTSO-E empty-result acknowledgements are HTTP 400 but contain
            # no secret; preserve their original bytes as omission evidence.
            empty=(provider=='entsoe' and r.status_code==400 and b'No matching data found' in b)
            if r.status_code!=200 and not empty:raise RuntimeError('Energy request HTTP '+str(r.status_code))
            digest=hashlib.sha256(b).hexdigest();now=pd.Timestamp.now(tz='UTC')
            m={'url':safeurl,'sha256':digest,'retrieved_at':now.isoformat(),'content_type':r.headers.get('Content-Type'),
               'http_status':r.status_code,'collector_group':GROUP,'authentication_redacted':provider is not None}
            atomic_bytes(self.root/'archive/raw'/(digest+'.bin'),b)
            atomic_bytes(self.root/'archive/receipts'/(ident+'_'+now.strftime('%Y%m%dT%H%M%S%fZ')+'.json'),json.dumps(m,indent=2).encode())
            atomic_bytes(index,json.dumps(m,indent=2).encode())
            return b,digest
        raise RuntimeError('Energy request retries exhausted')


def xml_root(raw):
    tree=ET.fromstring(raw)
    for el in tree.iter():el.tag=el.tag.split('}')[-1]
    return tree


def xml_documents(raw):
    if raw.startswith(b'PK'):
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            return [xml_root(z.read(n)) for n in z.namelist() if n.lower().endswith('.xml')]
    return [xml_root(raw)]


def series_dimensions(ts):
    dims={'psr':ts.findtext('MktPSRType/psrType') or '',
            'direction':'consumption' if ts.findtext('outBiddingZone_Domain.mRID') and ts.findtext('MktPSRType/psrType') else 'production',
            'unit':ts.findtext('quantity_Measure_Unit.name') or ts.findtext('price_Measure_Unit.name') or '',
            'currency':ts.findtext('currency_Unit.name') or '',
            'business_type':ts.findtext('businessType') or ''}
    # Prices for different auctions/products may occupy identical delivery
    # timestamps. Never merge classification sequences or resolution products.
    for field,tag in [('auction_type','auction.type'),('contract_type','contract_MarketAgreement.type'),
                      ('classification_sequence','classificationSequence_AttributeInstanceComponent.position')]:
        value=ts.findtext(tag)
        if value is not None:dims[field]=value
    return dims


def parse_entsoe(raw,start,end):
    """Decode source-defined blocks; retain missing periods as absent."""
    result=defaultdict(list);info=[];resolutions=defaultdict(set);start=pd.Timestamp(start);end=pd.Timestamp(end)
    for doc in xml_documents(raw):
        if doc.tag=='Acknowledgement_MarketDocument':
            reason=doc.findtext('Reason/text') or ''
            if 'No matching data found' in reason:raise NoData('ENTSO-E no matching observations')
            raise ValueError('ENTSO-E rejected request; inspect archived acknowledgement')
        info.append({'document_id':doc.findtext('mRID'),'revision':doc.findtext('revisionNumber'),
                     'response_created_time':doc.findtext('createdDateTime'),
                     'created_time_semantics':'Response creation time; NOT assumed to be original historical release time'})
        for ts in doc.findall('TimeSeries'):
            dimensions=series_dimensions(ts);key=json.dumps(dimensions,sort_keys=True)
            curve=ts.findtext('curveType') or 'A01'
            if curve not in {'A01','A03'}:raise ValueError('Unsupported ENTSO-E curve type')
            for period in ts.findall('Period'):
                pstart=pd.Timestamp(period.findtext('timeInterval/start'));pend=pd.Timestamp(period.findtext('timeInterval/end'))
                resolution=pd.Timedelta(period.findtext('resolution'));seconds=resolution.total_seconds()
                if seconds<=0 or seconds>86400:raise ValueError('Unexpected ENTSO-E time resolution')
                slots=int((pend-pstart)/resolution)
                if pstart+slots*resolution!=pend:raise ValueError('Nonintegral ENTSO-E period duration')
                points=[]
                for point in period.findall('Point'):
                    position=int(point.findtext('position'))
                    value=point.findtext('quantity')
                    if value is None:value=point.findtext('price.amount')
                    # Empty quantity remains a source gap, including its A03 block.
                    value=float(value) if value is not None else np.nan
                    if not 1<=position<=slots:raise ValueError('ENTSO-E point out of period bounds')
                    points.append((position,value))
                points.sort()
                if len({p for p,v in points})!=len(points):raise ValueError('Duplicate ENTSO-E point position')
                for i,(position,value) in enumerate(points):
                    stop=(points[i+1][0] if i+1<len(points) else slots+1) if curve=='A03' else position+1
                    if not math.isfinite(value):continue
                    resolutions[key].add(seconds)
                    for j in range(position,stop):
                        date=pstart+(j-1)*resolution
                        if start<=date<end:result[key].append((date,value))
    return result,info,resolutions


def deduplicate(frame):
    if frame.empty:return frame,0
    f=frame.drop_duplicates(['date','value'])
    conflicts=f.groupby('date').value.nunique();bad=conflicts[conflicts>1].index
    f=f[~f.date.isin(bad)].drop_duplicates('date')
    return normalize_frame(f) if len(f) else f,len(bad)


def frequency(frame):
    n=float(frame.date.diff().dt.total_seconds().median())
    return {60:'1min',300:'5min',600:'10min',900:'15min',1800:'30min',3600:'hourly',86400:'daily'}.get(n,'subhourly' if n<3600 else 'irregular_intraday')


def nested_number(row,field):
    item=row
    for part in field.split('.'):
        if not isinstance(item,dict):return None
        item=item.get(part)
    if item is None or str(item).strip() in {'','-','--','N/A','NA','null'}:return None
    value=float(item)
    return value if math.isfinite(value) else None


def parse_gie(rows,field,start,end):
    values=[];stats={'no_data_reports_omitted':0,'missing_values_omitted':0}
    start=pd.Timestamp(start);end=pd.Timestamp(end)
    for row in rows:
        if row.get('status')=='N':stats['no_data_reports_omitted']+=1;continue
        d=pd.Timestamp(row['gasDayStart'],tz='UTC')
        if not start<=d<end:continue
        value=nested_number(row,field)
        if value is None:stats['missing_values_omitted']+=1;continue
        values.append((d,value))
    frame,conflicts=deduplicate(pd.DataFrame(values,columns=['date','value']))
    stats['conflicting_dates_omitted']=conflicts
    return frame,stats


def atomic_parquet(path,frame):
    b=io.BytesIO();frame.to_parquet(b,index=False,compression='zstd');atomic_bytes(path,b.getvalue())


class Collector:
    def __init__(self,fetch,start,end,inventory_filename='energy11_inventory.json'):
        self.fetch=fetch;self.start=pd.Timestamp(start,tz='UTC');self.end=pd.Timestamp(end,tz='UTC')
        self.inventory_filename=inventory_filename
        self.metas=[];self.omissions=[];self.errors=[];self.supplements=[];self.source_records=[];self.file_records={};self.gie_recoveries=[]
        self.source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.lock=threading.Lock();self.hashes=set();self.ids=set()
        c=pd.read_parquet(ROOT/'catalog.parquet',columns=['series_id','dataset_identity','content_sha256'])
        c=c[~c.series_id.str.startswith(('entsoe11_','gie11_'))]
        self.baseline_identities=set(c.dataset_identity.dropna());self.baseline_hashes=set(c.content_sha256.dropna())

    def save(self,sid,frame,**meta):
        if frame.empty:
            self.omissions.append({'series_id':sid,'reason':'No usable observations'});return
        freq=meta.pop('frequency',frequency(frame));cov=coverage(frame)
        digest=hashlib.sha256(frame.date.astype('int64').to_numpy().tobytes()+frame.value.to_numpy().tobytes()).hexdigest()
        if frame.value.nunique()<2:
            self.omissions.append({'series_id':sid,'reason':'Constant numeric history','coverage':cov});return
        if not qualifies_research({'frequency':freq,**cov}):
            path=SUPPLEMENT/(sid+'.parquet');atomic_parquet(path,frame)
            sm={**meta,**cov,'frequency':freq,'series_id':sid,'main_catalog_eligible':False,'reason':'Insufficient genuine daily/weekly coverage'}
            atomic_bytes(path.with_suffix('.json'),json.dumps(sm,indent=2).encode())
            self.supplements.append({'path':str(path.relative_to(ROOT)),**cov,'series_id':sid});return
        with self.lock:
            if meta['dataset_identity'] in self.baseline_identities or digest in self.baseline_hashes or digest in self.hashes:
                self.omissions.append({'series_id':sid,'reason':'Duplicate identity or identical numeric history'});return
            if sid in self.ids:raise ValueError('Energy series ID collision')
            self.ids.add(sid);self.hashes.add(digest)
        now=pd.Timestamp.now(tz='UTC').isoformat()
        m=save_candidate(GROUP,sid,frame,frequency=freq,**meta,geography_scope='non_us',country_code_standard='ISO 3166-1 alpha-2',
                         native_frequency=freq,finer_frequency_available=freq!='daily',point_in_time=False,
                         locally_available_at=now,revision_policy='Latest retrospective provider history. Original fetched bytes retained; no claim that current values were known at observation time.')
        path=ROOT/'archive/staging'/GROUP/(sid+'.parquet')
        file_record={'series_id':sid,'path':str(path.relative_to(ROOT)),
                     'parquet_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                     'metadata_sha256':hashlib.sha256(path.with_suffix('.json').read_bytes()).hexdigest()}
        with self.lock:self.metas.append(m);self.file_records[sid]=file_record
        print('OK',sid,len(frame),freq,flush=True)

    def windows(self,params):
        a=self.start
        while a<self.end:
            b=min(a+pd.DateOffset(years=1),self.end)
            mid=min(a+pd.DateOffset(months=6),b)
            query={**params,'periodStart':a.strftime('%Y%m%d%H%M'),'periodEnd':mid.strftime('%Y%m%d%H%M')}
            url=requests.Request('GET',ENTSOE,params=query).prepare().url
            cached=ROOT/'archive/requests'/(hashlib.sha256(url.encode()).hexdigest()+'.json')
            # Reuse pilot half-year archives, otherwise request the documented
            # maximum one calendar year to reduce unnecessary API calls.
            if mid<b and cached.exists():yield a,mid;yield mid,b
            else:yield a,b
            a=b

    def entsoe_task(self,area,kind,destination=None):
        eic,name,countries=AREAS[area]
        params=dict(KINDS[kind]) if kind in KINDS else {'documentType':'A11'}
        if kind in {'actual_load','forecast_load_day_ahead'}:params['outBiddingZone_Domain']=eic
        elif kind=='day_ahead_price':params.update({'in_Domain':eic,'out_Domain':eic})
        elif kind=='physical_flow':
            params.update({'out_Domain':eic,'in_Domain':AREAS[destination][0]})
            countries=sorted(set(countries+AREAS[destination][2]));name+=' → '+AREAS[destination][1]
        else:params['in_Domain']=eic
        parts=defaultdict(list);hashes=[];allres=defaultdict(set);records=[]
        for a,b in self.windows(params):
            q={**params,'periodStart':a.strftime('%Y%m%d%H%M'),'periodEnd':b.strftime('%Y%m%d%H%M')}
            raw,h=self.fetch.get(ENTSOE,q,provider='entsoe');hashes.append(h)
            try:parsed,info,res=parse_entsoe(raw,a,b)
            except NoData:
                self.omissions.append({'source':f'entsoe:{area}:{kind}:{destination or ""}','window_start':a.isoformat(),'window_end':b.isoformat(),'reason':'Provider reports no matching data','raw_sha256':h});continue
            for key,rows in parsed.items():parts[key].extend(rows);allres[key].update(res[key])
            records.append({'raw_sha256':h,'parameters':q,'documents':info})
        public_kind='energy_price' if kind=='day_ahead_price' else kind
        label=slug(area+'_'+public_kind+('_'+destination if destination else ''))
        recordpath=SUPPLEMENT/('entsoe_'+label+'_documents.json')
        atomic_bytes(recordpath,json.dumps(records,indent=2).encode())
        for key,rows in parts.items():
            dims=json.loads(key);frame,conflicts=deduplicate(pd.DataFrame(rows,columns=['date','value']))
            tag=('_'+PSR.get(dims['psr'],slug(dims['psr']))+'_'+dims['direction']) if dims['psr'] else ''
            if kind=='day_ahead_price':
                tag='_auction_'+slug(dims.get('auction_type','unspecified'))+'_contract_'+slug(dims.get('contract_type','unspecified'))+'_seq_'+slug(dims.get('classification_sequence','unspecified'))
            unit=(dims['currency']+'/' if dims['currency'] else '')+{'MAW':'MW','MWH':'MWh'}.get(dims['unit'],dims['unit'])
            sid='entsoe11_'+label+tag
            if dims['currency']:sid+='_'+slug(dims['currency'])
            displaytag=tag.replace('_b25_','_energy_storage_').replace('_b26_','_demand_side_response_')
            self.save(sid,frame,title=f'ENTSO-E {name}: {public_kind.replace("_"," ")}{displaytag.replace("_"," ")}',
                      provider='ENTSO-E Transparency Platform',category='european_power_system',unit=unit,
                      source_url='https://transparency.entsoe.eu/',geography=name,country_codes=countries,
                      hypothesis='Electricity demand, fuel-specific generation, renewable forecasts, cross-border flows and wholesale prices may reveal industrial activity, weather sensitivity, fuel substitution and regional capacity stress; test with correct information timing.',
                      limitations='Latest retrospective history, including revisions. Date is delivery interval start, not publication time. Forecast issue-time histories are not provided by this extraction; response createdDateTime is query creation time. A03 source-defined constant blocks decoded on their explicit resolution, never across period boundaries; A01 gaps not filled. Conflicting duplicate timestamps omitted. Changing native resolution retained.',
                      raw_sha256=hashes,dataset_identity='ENTSOE:'+area+':'+public_kind+':'+str(destination)+':'+key,
                      source_dimensions=dims,source_area_eic=eic,source_resolution_seconds=sorted(allres[key]),
                      source_resource_type_description={'B25':'Energy storage','B26':'Demand side response'}.get(dims['psr'],PSR.get(dims['psr'])),
                      price_product_semantics='Distinct source auction.type, contract_MarketAgreement.type and classification sequence; neutral source codes retained, no inferred venue or named auction mapping.' if kind=='day_ahead_price' else None,
                      mixed_native_resolution=len(allres[key])>1,
                      forecast_horizon_class='day_ahead' if kind.startswith('forecast_') else None,
                      historical_forecast_issue_time_available=False if kind.startswith('forecast_') else None,
                      source_native_frequency=';'.join(str(int(x))+' seconds' for x in sorted(allres[key])),
                      native_frequency_basis='Median stored interval; source A03 blocks expanded only within their explicitly defined validity. Inspect source_resolution_seconds for resolution changes.',
                      timestamp_semantics='UTC start of delivery interval; for forecasts this is the forecast target time, never forecast issue time.',
                      expected_publication_lag='Actual load/generation/flows commonly near-real-time to next day, varies by provider and revisions; day-ahead forecasts normally precede delivery. Energy-price timing depends on source auction/contract/sequence (A44 includes daily and intraday contracts). Exact original-release history unknown; no uniform lag asserted.',
                      publication_lag_days_estimate=None,publication_lag_basis='No validated uniform per-series lag; query response timestamp is not original publication.',
                      source_attribution='ENTSO-E Transparency Platform and contributing TSOs',license='ENTSO-E Transparency Platform data reuse terms; contributor attribution retained.',
                      document_companion=str(recordpath.relative_to(ROOT)),quality_filter_counts={'conflicting_dates_omitted':conflicts})

    def gie_pages(self,platform,params,path='/api'):
        try:return self._gie_pages_once(platform,params,path)
        except (AccessError,ValueError) as exc:
            message=str(exc)
            if message not in {'GIE empty page before declared end','Response reflected a credential; not archived'}:raise
            # One complete retry, rather than mixing opposite page directions.
            # reverse is a documented ordinary pagination option, not auth.
            retry={**params,'reverse':'true'}
            result=self._gie_pages_once(platform,retry,path)
            self.gie_recoveries.append({'platform':platform,'path':path,'parameters':params,'initial_error':message,
                                        'resolution':'Complete bounded re-download using documented reverse pagination','raw_sha256':result[1]})
            return result

    def _gie_pages_once(self,platform,params,path='/api'):
        rows=[];hashes=[];page=1;seen=set()
        while True:
            raw,h=self.fetch.get('https://'+platform+'.gie.eu'+path,{**params,'page':page,'size':300},provider='gie')
            obj=json.loads(raw);hashes.append(h)
            if not isinstance(obj,dict) or not isinstance(obj.get('data'),list):raise ValueError('GIE response schema mismatch')
            last=int(obj.get('last_page',1))
            if last>30:raise ValueError('GIE pagination exceeds bounded page cap')
            if h in seen:raise ValueError('GIE repeated a pagination page')
            seen.add(h);rows.extend(obj['data'])
            if not obj['data'] and last>1:raise ValueError('GIE empty page before declared end')
            if page>=last:break
            page+=1
        return rows,hashes

    def gie_task(self,platform,country,company=None,facility=None,name=None):
        params={'country':country,'from':self.start.date().isoformat(),'to':(self.end-pd.Timedelta(days=1)).date().isoformat()}
        if company:params['company']=company
        if facility:params['facility']=facility
        rows,hashes=self.gie_pages(platform,params)
        if not rows:
            self.omissions.append({'source':platform+':'+str(params),'reason':'No GIE daily reports'});return
        expected=facility or company or country
        if any(str(x.get('code','')).upper()!=expected.upper() for x in rows):raise ValueError('GIE ignored dataset identity filter')
        label=slug('_'.join(x for x in [platform,country,company,facility] if x))
        # Preserve original statuses, gas-day labels, update timestamps and notices.
        companion=SUPPLEMENT/('gie_'+label+'_reports.json')
        atomic_bytes(companion,json.dumps({'parameters':params,'raw_sha256':hashes,'reports':rows},indent=2).encode())
        name=name or rows[0]['name']
        for field,(metric,unit) in GIE_METRICS[platform].items():
            frame,stats=parse_gie(rows,field,self.start,self.end)
            self.save('gie11_'+label+'_'+metric,frame,title=f'GIE {platform.upper()} {name}: {metric.replace("_"," ")}',
                      provider='GIE '+platform.upper(),category='european_gas_storage_lng',unit=unit,frequency='daily',
                      source_url='https://'+platform+'.gie.eu/',geography=name,country_codes=[gie_country_code(country)],
                      hypothesis='Storage injection and withdrawal, LNG inventories and send-out, and capacity constraints may anticipate regional gas supply stress, fuel substitution, power prices and energy-intensive industry margins.',
                      limitations='Latest retrospectively corrected daily reports. Gas day label is not a midnight instantaneous measurement or public availability time. Confirmed and estimated reports retained; status N and missing numbers omitted, never zero-filled. Facility ownership, group membership and aggregate reporting coverage can change. Source updatedAt is retained as an unzoned string, not assumed to be original publication.',
                      raw_sha256=hashes,dataset_identity='GIE:'+platform+':'+country+':'+str(company)+':'+str(facility)+':'+field,
                      source_field=field,source_company_eic=company,source_facility_eic=facility,
                      timestamp_semantics='UTC midnight is a date label for source gasDayStart. Inventory describes end of that gas day; injection/withdrawal/send-out cover that gas day. Not publication time.',
                      native_frequency_basis='Original daily gas-day reports without resampling.',
                      expected_publication_lag='GIE manual: prior gas-day reports updated at 19:30 CET and 23:00; some contributors arrive in second update. Gas-day-start label to release roughly 1–2 calendar days; historical revisions can arrive much later.',
                      publication_lag_days_estimate=2.0,publication_lag_basis='Conservative approximate gasDayStart-label to scheduled daily publication per GIE API manual; not a verified per-row first-release timestamp.',
                      source_attribution='GIE '+platform.upper()+'; original storage/LNG operators',license='Public free API data; attribution to GIE/AGSI/ALSI mandatory: https://iip.gie.eu/data-usage',
                      source_status_companion=str(companion.relative_to(ROOT)),quality_filter_counts=stats)

    def gie_discovery(self):
        raw,h=self.fetch.get('https://agsi.gie.eu/api/about',{'show':'listing'},provider='gie',max_age_hours=24)
        listings=json.loads(raw)
        if not isinstance(listings,list):raise ValueError('GIE storage listing schema mismatch')
        atomic_bytes(SUPPLEMENT/'gie_agsi_facility_listing.json',json.dumps({'raw_sha256':h,'listing':listings},indent=2).encode())
        countries=sorted({x['country'] for x in listings})
        # Two active groups/facilities per country, ordered by identifier, plus all
        # national aggregates. This is a bounded representative panel, not a census.
        facilities=defaultdict(list)
        for company in listings:
            for f in company.get('facilities',[]):
                if (not f.get('operational_end_date') or f['operational_end_date']>=self.start.date().isoformat()) and (not f.get('operational_start_date') or f['operational_start_date']<=(self.end-pd.Timedelta(days=180)).date().isoformat()):
                    facilities[f['country']].append(f)
        tasks=[('agsi',c,None,None,None) for c in countries]
        for country,fs in sorted(facilities.items()):
            for f in sorted(fs,key=lambda x:(bool(x.get('operational_end_date')),x['eic']))[:2]:
                tasks.append(('agsi',country,f['company'],f['eic'],f['name']))
        # The ALSI listing endpoint currently denies this key although daily
        # country data are authorized. Use documented country filters, no bypass.
        tasks.extend(('alsi',c,None,None,None) for c in ['BE','DE','ES','FR','GR','HR','IT','LT','NL','PL','PT','GB*','FI','SE'])
        atomic_bytes(SUPPLEMENT/'gie_selection.json',json.dumps({'tasks':tasks,'storage_selection':'All country totals and at most two active operator/facility EIC datasets per country; current listing, not historical membership.','raw_listing_sha256':h},indent=2).encode())
        return tasks

    def gie_outages(self):
        query={'start':self.start.date().isoformat(),'end':self.end.date().isoformat()}
        rows,hashes=self.gie_pages('agsi',query,'/api/unavailability')
        # Source filters have been observed not to limit returned event dates;
        # archive the full returned event table, do not claim an as-known panel.
        frame=pd.json_normalize(rows)
        path=SUPPLEMENT/'gie_agsi_unavailability_events.parquet';atomic_parquet(path,frame)
        meta={'rows':len(frame),'raw_sha256':hashes,'source_url':'https://agsi.gie.eu/api/unavailability',
              'requested_source_filters':query,'source_date_filter_verified':False,
              'timestamp_semantics':'published, start, end are original unzoned source timestamp strings; timezone not verified.',
              'point_in_time':False,'limitations':'Current event records include retrospective updates. No stable version identifier or complete revision chain is exposed here. No unavailable-capacity daily panel is fabricated. Reported unavailable volume/injection/withdrawal remain event attributes; overlapping and revised events must be resolved before aggregation.',
              'units':{'volume':'TWh','injection':'GWh/day','withdrawal':'GWh/day'},'source_attribution':'GIE AGSI and contributing storage operators'}
        atomic_bytes(path.with_suffix('.json'),json.dumps(meta,indent=2).encode())
        self.supplements.append({'path':str(path.relative_to(ROOT)),'rows':len(frame),'type':'gas_unavailability_events'})

    def run_tasks(self,tasks,workers):
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures={pool.submit(fn,*args):label for label,fn,args in tasks}
            for future in as_completed(futures):
                label=futures[future]
                try:future.result()
                except AccessError as exc:
                    self.errors.append({'source':label,'error':str(exc),'type':'access'});print('ACCESS',label,str(exc),flush=True)
                except Exception as exc:
                    # Exception text can contain a transport URL; only known
                    # collector-generated messages are safe to surface.
                    error=str(exc) if isinstance(exc,(ValueError,RuntimeError)) and not any(k in str(exc) for k in self.fetch.keys.values()) else type(exc).__name__
                    self.errors.append({'source':label,'error':error,'type':type(exc).__name__});print('ERROR',label,error,flush=True)
                self.inventory(final=False)

    def inventory(self,final=False):
        with self.lock:files=[dict(self.file_records[k]) for k in sorted(self.file_records)]
        inv={'group':GROUP,'status':'complete' if final else 'running','created_at':pd.Timestamp.now(tz='UTC').isoformat(),
             'series_count':len(self.metas),'observation_count':sum(m['rows'] for m in self.metas),
             'provider_counts':pd.Series([m['provider'] for m in self.metas],dtype='object').value_counts().to_dict(),
             'frequency_counts':pd.Series([m['frequency'] for m in self.metas],dtype='object').value_counts().to_dict(),
             'country_codes':sorted({c for m in self.metas for c in m['country_codes']}),'files':files,
             'source_sha256':self.source_sha256,'errors':self.errors,
             'omissions':self.omissions,'supplementary':self.supplements,'api_requests_this_run':self.fetch.requests,
             'source_recoveries':self.gie_recoveries,
             'requested_start':self.start.isoformat(),'requested_end_exclusive':self.end.isoformat(),
             'refresh_command':'python3 alt_data/sources_energy11.py --refresh',
             'access_limitations':['ALSI /api/about listing denied this key during authenticated probe, although ALSI /api country-history access succeeded. Country totals collected via authorized endpoint; no LNG facility census claimed.'],
             'publication_status':'staged_only; root integration required'}
        atomic_bytes(ROOT/'logs'/self.inventory_filename,json.dumps(inv,indent=2).encode())
        return inv


def main():
    p=argparse.ArgumentParser();p.add_argument('--refresh',action='store_true');p.add_argument('--start',default=START)
    p.add_argument('--end',default=pd.Timestamp.now(tz='UTC').normalize().date().isoformat())
    p.add_argument('--workers',type=int,default=4);p.add_argument('--only',choices=['entsoe','gie']);p.add_argument('--areas',nargs='*')
    args=p.parse_args();fetch=SafeFetch(refresh=args.refresh);c=Collector(fetch,args.start,args.end)
    for doc in [GIE_MANUAL,ENTSOE_CODES]:fetch.get(doc)
    tasks=[]
    if args.only!='gie':
        for area in args.areas or AREAS:
            for kind in KINDS:tasks.append((f'entsoe:{area}:{kind}',c.entsoe_task,(area,kind)))
        if not args.areas:
            for a,b in [('FR','DE_LU'),('FR','ES'),('FR','BE'),('FR','CH'),('FR','GB'),('DE_LU','NL'),('DE_LU','PL'),('DE_LU','CZ'),('DE_LU','AT'),('NO2','DE_LU'),('NO2','GB'),('SE4','PL'),('FI','EE'),('LT','PL'),('RO','HU'),('BG','GR'),('ES','PT'),('IT_NORD','CH')]:
                for source,destination in [(a,b),(b,a)]:tasks.append((f'entsoe:flow:{source}:{destination}',c.entsoe_task,(source,'physical_flow',destination)))
    if args.only!='entsoe':
        for task in c.gie_discovery():tasks.append(('gie:'+':'.join(str(x) for x in task[:4]),c.gie_task,task))
        tasks.append(('gie:unavailability',c.gie_outages,()))
    power=[t for t in tasks if t[0].startswith('entsoe:')];gas=[t for t in tasks if t[0].startswith('gie:')]
    tasks=[t for pair in zip_longest(power,gas) for t in pair if t is not None]
    c.run_tasks(tasks,args.workers);inv=c.inventory(final=True)
    finalize_inventory()
    print('COMPLETE',inv['series_count'],inv['observation_count'],'errors',len(inv['errors']),flush=True)


def finalize_inventory():
    """Pin exactly the successful source files and every supplementary artifact."""
    path=ROOT/'logs/energy11_inventory.json';inv=json.loads(path.read_text())
    if inv['status']!='complete':raise ValueError('Cannot finalize an unfinished energy collection')
    wanted={x['series_id'] for x in inv['files']};staging=ROOT/'archive/staging'/GROUP
    moved=[];stamp=pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%S%fZ')
    for p in staging.glob('*.parquet'):
        if p.stem in wanted:continue
        # Keep failed/prior-attempt artifacts for diagnostics, outside the
        # group-scoped publication folder. Never remove a canonical series.
        dest=ROOT/'archive/attempts'/GROUP/stamp/p.name;dest.parent.mkdir(parents=True,exist_ok=True)
        p.rename(dest)
        if p.with_suffix('.json').exists():p.with_suffix('.json').rename(dest.with_suffix('.json'))
        moved.append(str(dest.relative_to(ROOT)))
    inv['quarantined_prior_attempt_files']=moved
    inv['supplementary_files']=[{'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
                                for p in sorted(SUPPLEMENT.rglob('*')) if p.is_file()]
    inv['execution_source_sha256']=inv['source_sha256']
    inv['source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    inv['finalized_at']=pd.Timestamp.now(tz='UTC').isoformat()
    atomic_bytes(path,json.dumps(inv,indent=2).encode())
    return inv


if __name__=='__main__':main()
