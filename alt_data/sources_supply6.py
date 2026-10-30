#!/usr/bin/env python3
"""Physical fuel inventories, supply and operational constraints (batch 8)."""
from __future__ import annotations
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import time
import threading
import unicodedata
from urllib.parse import urljoin
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes, coverage,
                              normalize_frame, qualifies_research, save_candidate)
from country_codes import normalize_country_codes

GROUP = 'supply6'
FAILURES = []
OMISSIONS = Counter()
BASELINE = None
SEEN = set()
SID_IDENTITIES = {}

def slug(s):
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode()
    return re.sub('[^a-z0-9]+', '_', s.lower()).strip('_')

def baseline():
    global BASELINE
    if BASELINE is None:
        c = pd.read_parquet(ROOT / 'catalog.parquet')
        old = c[c.batch < 8]
        own = c[(c.batch == 8) & c.series_id.str.startswith(('eia_pet_', 'eia_ngstorage_', 'india_coal_', 'enagas_', 'cenagas_'))]
        BASELINE = {k: set(old[k].dropna()) for k in ['series_id', 'dataset_identity', 'content_sha256']}
        BASELINE['pinned_ids'] = set(own.series_id)
        BASELINE['has_batch8'] = bool((c.batch == 8).any())
    return BASELINE

def save(sid, frame, **meta):
    old = baseline()
    if sid in SID_IDENTITIES and SID_IDENTITIES[sid] != meta['dataset_identity']:
        raise ValueError('Series slug collision: '+sid)
    SID_IDENTITIES[sid] = meta['dataset_identity']
    if old['has_batch8'] and sid not in old['pinned_ids']:
        OMISSIONS['not_published_batch8'] += 1
        return
    if frame.empty or frame.value.nunique() < 2:
        OMISSIONS['constant_or_empty'] += 1
        return
    if (frame.value!=0).sum() < (26 if meta['frequency']=='weekly' else 90):
        OMISSIONS['insufficient_active_observations'] += 1
        return
    frame = normalize_frame(frame)
    if not qualifies_research(dict(coverage(frame), frequency=meta['frequency'])):
        OMISSIONS['insufficient_history'] += 1
        return
    digest = hashlib.sha256(frame.date.astype('int64').to_numpy().tobytes() + frame.value.to_numpy().tobytes()).hexdigest()
    if sid in old['series_id'] or meta['dataset_identity'] in old['dataset_identity'] or digest in old['content_sha256'] or digest in SEEN:
        OMISSIONS['existing_or_identical'] += 1
        return
    SEEN.add(digest)
    meta['country_codes'] = normalize_country_codes(meta['country_codes'])
    meta['country_code_standard'] = 'ISO 3166-1 alpha-2'
    meta.setdefault('native_frequency', meta['frequency'])
    meta.setdefault('finer_frequency_available', False)
    meta.setdefault('point_in_time', False)
    result = save_candidate(GROUP, sid, frame, **meta)
    print('OK', sid, result['rows'], flush=True)
    return result

class Access:
    """Provider-scoped stop after explicit access denial, bounded polite calls."""
    def __init__(self, refresh=False, interval=.35):
        self.fetcher = Fetcher(refresh=refresh)
        self.historical_fetcher = Fetcher(refresh=False)
        self.blocked = None
        self.lock = threading.Lock()
        self.next_at = 0.
        self.interval = interval
    def get(self, url, *, historical=False, **kwargs):
        fetcher = self.historical_fetcher if historical else self.fetcher
        if self.blocked: raise RuntimeError('Provider requests stopped: ' + self.blocked)
        # Cache hits consume no provider request and need no HTTP pacing slot.
        # Verify bytes before returning, matching Fetcher cache semantics.
        if not fetcher.refresh:
            prepared = requests.Request('GET',url,params=kwargs.get('params')).prepare().url
            path = ROOT/'archive/requests'/(hashlib.sha256(prepared.encode()).hexdigest()+'.json')
            if path.exists():
                receipt = json.loads(path.read_text())
                age = (pd.Timestamp.now(tz='UTC')-pd.Timestamp(receipt['retrieved_at'])).total_seconds()
                raw_path=ROOT/'archive/raw'/(receipt['sha256']+'.bin')
                if 0<=age<kwargs.get('max_age_hours',24)*3600 and raw_path.exists():
                    raw=raw_path.read_bytes()
                    if hashlib.sha256(raw).hexdigest()==receipt['sha256']: return raw,receipt['sha256']
        with self.lock:
            if self.blocked:
                raise RuntimeError('Provider access stopped: ' + self.blocked)
            delay = max(0, self.next_at - time.monotonic())
            self.next_at = max(self.next_at, time.monotonic()) + self.interval
        if delay: time.sleep(delay)
        if self.blocked: raise RuntimeError('Provider access stopped: ' + self.blocked)
        try:
            return (self.historical_fetcher if historical else self.fetcher).get(url, **kwargs)
        except requests.HTTPError as exc:
            if exc.response.status_code in {401, 403, 429}:
                self.blocked = str(exc)
            raise

def parse_eia_petroleum(raw):
    """Native weekly columns only; reject four-week rolling aliases from Contents."""
    book = pd.ExcelFile(io.BytesIO(raw))
    contents = book.parse('Contents', header=None)
    sheets = []
    for row in contents.itertuples(index=False, name=None):
        vals = [str(v).strip() for v in row]
        for sn in book.sheet_names:
            if sn.startswith('Data ') and sn in vals and 'Weekly' in vals:
                sheets.append(sn)
    if not sheets: raise ValueError('No native weekly sheets in EIA workbook')
    result = []
    for sn in sheets:
        data = book.parse(sn, header=None)
        if str(data.iloc[1,0]).strip() != 'Sourcekey' or str(data.iloc[2,0]).strip() != 'Date':
            raise ValueError('EIA source key/date header changed')
        dates = pd.to_datetime(data.iloc[3:,0], utc=True, errors='coerce')
        for j in range(1, data.shape[1]):
            key, title = str(data.iloc[1,j]).strip(), str(data.iloc[2,j]).strip()
            if key == 'nan' or title == 'nan': continue
            value = pd.to_numeric(data.iloc[3:,j], errors='coerce')
            frame = pd.DataFrame({'date':dates,'value':value}).dropna().reset_index(drop=True)
            # Publication includes balance changes and rolling ratios; retain native
            # levels/flows, not mechanically differenced stock histories.
            if re.search(r'4-week|stock change|change in stocks|days of supply', title, re.I): continue
            unit = re.search(r'\(([^()]*)\)\s*$', title)
            if not unit: raise ValueError('Missing EIA unit: ' + title)
            result.append((key, title, unit.group(1), sn, frame))
    return result

def eia_category(title):
    t = title.lower()
    if 'stock' in t: return 'petroleum_inventories'
    if 'utilization' in t: return 'refinery_utilization'
    if 'input' in t: return 'refinery_feedstock_inputs'
    if 'import' in t or 'export' in t: return 'petroleum_trade_flows'
    if 'supplied' in t: return 'petroleum_apparent_demand'
    return 'petroleum_production_supply'

def collect_eia(refresh=False):
    access = Access(refresh)
    landing = 'https://www.eia.gov/petroleum/supply/weekly/'
    page, ph = access.get(landing)
    links = [urljoin(landing,a['href']) for a in BeautifulSoup(page,'html.parser').find_all('a',href=True)
             if re.search(r'/psw0[1-9](?:a)?\.xls$',a['href'])]
    links = list(dict.fromkeys(links))
    if len(links) < 9: raise ValueError('EIA weekly workbook links missing')
    rows = {}
    for url in links:
        raw, digest = access.get(url)
        for key,title,unit,sheet,frame in parse_eia_petroleum(raw):
            if key in rows:
                prior = rows[key]
                overlap = frame.merge(prior['frame'], on='date', suffixes=('_new','_old'))
                if not np.array_equal(overlap.value_new.to_numpy(), overlap.value_old.to_numpy()):
                    raise ValueError('Conflicting EIA workbook histories for ' + key)
                # Union genuine native source dates; duplicate table representations
                # are one identity, not separate histories.
                prior['frame'] = pd.concat([prior['frame'],frame]).drop_duplicates('date').sort_values('date')
                prior['raw'].append(digest)
                prior['locations'].append({'url':url,'sheet':sheet})
            else:
                rows[key] = dict(frame=frame,title=title,unit=unit,raw=[digest],url=url,sheet=sheet,locations=[{'url':url,'sheet':sheet}])
    for key,r in rows.items():
        save('eia_pet_' + slug(key),r['frame'],title=r['title'],provider='us_energy_information_administration',
             category=eia_category(r['title']),unit=r['unit'],frequency='weekly',source_url=r['url'],
             geography='United States; native region/product/origin defined by EIA series title',country_codes=['US'],geography_scope='us',
             hypothesis='Inventories, refinery throughput, product demand and cross-border supply can affect petroleum balances and refining margins on weekly horizons.',
             limitations='Latest revised survey/estimate history, not release-time vintages. Flows are average thousand barrels per day over the reporting week; inventories are week-end levels. Native regional/national totals overlap. Production and some minor product components are estimated; methodological and rounding breaks remain. Missing/suppressed cells omitted, no interpolation.',
             raw_sha256=r['raw']+[ph],dataset_identity='eia:petroleum:weekly:'+key,
             source_series_id=key,source_worksheets=r['locations'],
             expected_publication_lag='Usually following Wednesday 10:30 America/New_York (5 days after Friday period end); holidays vary. No per-observation historical release timestamp verified.',
             timestamp_semantics='Friday week-ending date as UTC period label; not availability time.',
             aggregation='Native EIA weekly level or weekly average flow; no resampling; excludes four-week rolling tables, stock-change aliases and days-of-supply ratios.',
             refresh_cadence='weekly after EIA WPSR release')
    url = 'https://ir.eia.gov/ngs/ngshistory.xls'
    raw,digest = access.get(url)
    for region,frame in parse_eia_gas(raw):
        save('eia_ngstorage_'+slug(region),frame,title='US working natural gas in underground storage: '+region,
             provider='us_energy_information_administration',category='natural_gas_storage_inventories',unit='billion cubic feet',frequency='weekly',
             source_url=url,geography='United States: '+region,country_codes=['US'],geography_scope='us',
             hypothesis='Working gas inventory levels constrain seasonal supply and can affect natural gas prices and calendar spreads.',
             limitations='Latest revised EIA-912 survey estimates. Pre-June-19-2015 backcast regional estimates excluded. Five-region structure officially introduced November 2015. National/regional totals overlap; no stock-change duplicate series.',
             raw_sha256=[digest],dataset_identity='eia:weekly_working_natural_gas_storage:'+region,
             source_sheet='html_report_history',source_column=region,
             expected_publication_lag='Usually following Thursday 10:30 America/New_York; holidays vary. Historical first release clocks not verified.',
             timestamp_semantics='Friday week-ending inventory date, UTC period label, not release time.',aggregation='Native weekly EIA-912 working-gas stock only; no differencing or fill.',refresh_cadence='weekly after WNGSR release')

def parse_eia_gas(raw):
    d = pd.read_excel(io.BytesIO(raw),sheet_name='html_report_history',header=None)
    headers = d.iloc[6].tolist()
    if str(headers[0]).strip() != 'Week ending' or str(headers[1]).strip() != 'Source': raise ValueError('EIA natural gas header changed')
    dates = pd.to_datetime(d.iloc[7:,0],utc=True,errors='coerce')
    surveyed = d.iloc[7:,1].astype(str).str.strip().eq('EIA-912')
    result=[]
    for j in range(2,len(headers)):
        frame=pd.DataFrame({'date':dates,'value':pd.to_numeric(d.iloc[7:,j],errors='coerce')})
        frame=frame[surveyed].dropna().reset_index(drop=True)
        result.append((str(headers[j]).strip(),frame))
    return result


INDIA_LANDING = 'https://npp.gov.in/publishedReports'
INDIA_START = '2025-01-01'
ENAGAS_LANDING = 'https://www.enagas.es/es/gestion-tecnica-sistema/energy-data/parametros-fisicos/capacidades-tecnicas-flujos-fisicos/seguimiento-diario-sistema/'

def parse_india_coal(raw, expected_date):
    d = pd.read_excel(io.BytesIO(raw), sheet_name='dailyCoalReport', header=None)
    strings = [str(x).strip() for x in d.iloc[:4].to_numpy().ravel() if pd.notna(x)]
    reported = [re.search(r'AS ON (\d{2}-\d{2}-\d{4})',x) for x in strings]
    reported = [pd.Timestamp(m.group(1)[6:]+'-'+m.group(1)[3:5]+'-'+m.group(1)[:2],tz='UTC') for m in reported if m]
    date = pd.Timestamp(expected_date)
    if date.tzinfo is None: date = date.tz_localize('UTC')
    if reported != [date]: raise ValueError('Coal report observation date mismatch')
    def col(fragment, row=3):
        ids = [j for j,x in enumerate(d.iloc[row]) if fragment.lower() in str(x).lower()]
        if len(ids)!=1: raise ValueError('Coal report column ambiguous: '+fragment)
        return ids[0]
    name_col=col('Name of Thermal Power')
    capacity_col=col('Capacity (MW)')
    metrics={'stock_total':col('Total',4),'receipts':col('Receipt of the day'),'consumption':col('Consumption of the day')}
    state_cols=[j for j,x in enumerate(d.iloc[3]) if str(x).strip()=='State']
    genco_cols=[j for j,x in enumerate(d.iloc[3]) if str(x).strip()=='GENCO']
    rows=[]; seen=set()
    for _,r in d.iloc[6:].iterrows():
        if not str(r.iloc[0]).strip().isdigit(): continue
        plant=r.iloc[name_col]
        cap=pd.to_numeric(r.iloc[capacity_col],errors='coerce')
        # A missing installed-capacity cell does not invalidate reported stock/flow.
        if pd.isna(plant): continue
        plant=re.sub(r'\s+',' ',str(plant)).strip()
        plant=re.sub(r'\s*-\s*','-',plant)
        if plant in seen: raise ValueError('Ambiguous duplicate coal plant: '+plant)
        seen.add(plant)
        extra={'state':str(r.iloc[state_cols[0]]).strip() if state_cols and pd.notna(r.iloc[state_cols[0]]) else None,
               'genco':str(r.iloc[genco_cols[0]]).strip() if genco_cols and pd.notna(r.iloc[genco_cols[0]]) else None}
        for metric,j in metrics.items():
            value=pd.to_numeric(r.iloc[j],errors='coerce')
            if pd.isna(value): continue
            if value<0: raise ValueError('Negative reported coal stock/flow')
            rows.append({'date':date,'plant':plant,'metric':metric,'value':float(value),**extra})
    if len(seen)<100: raise ValueError('Coal report unexpectedly small')
    return pd.DataFrame(rows)

def collect_india_coal(refresh=False):
    access=Access(refresh, interval=.6)
    page,ph=access.get(INDIA_LANDING)
    soup=BeautifulSoup(page,'html.parser')
    urls=[urljoin(INDIA_LANDING,a['href']) for a in soup.find_all('a',href=True) if re.search(r'/dailyCoal1-\d{4}-\d{2}-\d{2}\.xls$',a['href'])]
    if len(set(urls))!=1: raise ValueError('Current India coal XLS link missing')
    latest_url=urls[0]
    latest_date=re.search(r'dailyCoal1-(\d{4}-\d{2}-\d{2})',latest_url).group(1)
    end=min(pd.Timestamp(latest_date),pd.Timestamp.now().normalize()-pd.Timedelta(days=1))
    dates=pd.date_range(INDIA_START,end)
    prefix=latest_url.split('/daily/fuel/')[0]+'/daily/fuel/'
    missing=[]; pieces=[]; hashes=[]; receipts=[]
    def fetch(day):
        url=prefix+day.strftime('%d-%m-%Y')+'/dailyCoal1-'+day.strftime('%Y-%m-%d')+'.xls'
        try:
            raw,h=access.get(url,historical=day<end-pd.Timedelta(days=30),max_age_hours=24 if day>=end-pd.Timedelta(days=30) else 24*365)
            return day,h,parse_india_coal(raw,day),url
        except requests.HTTPError as exc:
            if exc.response.status_code==404: return day,None,None,url
            raise
        except Exception as exc:
            access.blocked='Source parser failure: '+str(exc)
            raise
    with ThreadPoolExecutor(max_workers=3) as pool:
        for day,h,frame,url in pool.map(fetch,dates):
            if h is None:
                missing.append(day.isoformat());continue
            hashes.append(h);pieces.append(frame);receipts.append({'date':day.strftime('%Y-%m-%d'),'raw_sha256':h,'url':url})
            if len(hashes)%50==0: print('INDIA_REPORTS',len(hashes),flush=True)
    if not pieces: raise ValueError('No India coal reports acquired')
    manifest=archive_raw_manifest(hashes,description='Original India NPP daily plant coal XLS reports; absent report days never filled')
    atomic_bytes(ROOT/'logs/supply6_india_reports.json',json.dumps({'reports':receipts,'missing_404':missing},indent=2).encode())
    data=pd.concat(pieces,ignore_index=True)
    for (plant,metric),r in data.groupby(['plant','metric'],sort=True):
        if (r.value>0).sum()<90:
            OMISSIONS['india_fewer_than_90_active_days']+=1;continue
        source_meta=r.dropna(subset=['state']).sort_values('date')
        state=source_meta.iloc[-1].state if len(source_meta) else None
        latest_genco=r.dropna(subset=['genco']).sort_values('date')
        operator=latest_genco.iloc[-1].genco if len(latest_genco) else None
        save('india_coal_'+slug(plant)+'_'+metric,r[['date','value']],
             title='India plant coal '+{'stock_total':'total stock','receipts':'daily receipts','consumption':'daily consumption'}[metric]+': '+plant,
             provider='india_central_electricity_authority',category='coal_inventories' if metric=='stock_total' else 'coal_supply_flows',
             unit='thousand tonnes',frequency='daily',source_url=INDIA_LANDING,geography='India'+(': '+state if state else '')+': '+plant,
             country_codes=['IN'],geography_scope='non_us',hypothesis='Plant fuel stock constraints and receipt/consumption imbalances can lead power scarcity, imported coal demand and utility margins.',
             limitations='Latest downloadable report editions; first publication and later revision histories unavailable. Plant identity follows report name after whitespace normalization and removing spaces immediately around hyphens; substantive renames remain separate. No zero-filled missing reports or blank values. Daily reported zeros retained. No operator/state classification is backdated into historical observations; latest labels are descriptive only.',
             raw_sha256=[ph],raw_manifest_sha256=[manifest],dataset_identity='india:cea:daily_coal:'+plant+':'+metric,
             source_entity_id=plant,source_metric=metric,source_name_normalization='Collapse whitespace and remove spaces immediately around hyphens; notably METTUR TPS - II and METTUR TPS-II form one station history.',latest_reported_state=state,latest_reported_genco=operator,
             expected_publication_lag='Daily reports, generally next-day or later; historical first release timestamps not established. Current report link may lag several days.',
             timestamp_semantics='AS ON Indian local reporting calendar date, encoded as UTC period label; not midnight availability.',
             aggregation='Native reported plant stock or same-day receipts/consumption; no resampling, interpolation or plant aggregation.',refresh_cadence='daily',refresh_revision_window_days=30,older_archive_cache_hours=8760)

# Select one inventory unit and one end-of-day value: omit duplicated beginning
# stocks, alternate-unit conversions and derived fill percentages. Regasification
# send-out, underground gas flows and national gas-field inflows are already
# measured by the existing ENTSOG family; provider/energy-basis differences do
# not establish distinct signals. See logs/expansion_batch8_semantic_review.json.
ENAGAS_FIELDS={
    'tankFinalStocksM3':('lng_terminal_inventories','cubic metres LNG','end-of-day LNG stock'),
    'loadsGNL':('lng_terminal_loadings','cubic metres LNG per gas day','LNG vessel loadings'),
    'downloadsGNL':('lng_terminal_unloadings','cubic metres LNG per gas day','LNG vessel unloadings'),
    'cisternLoad':('lng_truck_loadings','GWh per gas day','LNG tanker-truck loadings'),
}

def parse_enagas(raw, day):
    obj=json.loads(raw)
    if 'gasSystemPPDDataReportPojo' not in obj: raise ValueError('Enagas report schema missing')
    gas=obj['gasSystemPPDDataReportPojo'];rows=[]
    date=pd.Timestamp(day)
    if date.tzinfo is None: date=date.tz_localize('UTC')
    def add(group,entity,value,category,unit,title):
        if value is None or str(value).strip() in ('','-','--','null'): return
        val=float(value)
        if not np.isfinite(val): return
        rows.append(dict(date=date,group=group,entity=entity,value=val,category=category,unit=unit,title=title))
    for field,(category,unit,title) in ENAGAS_FIELDS.items():
        if field not in gas: raise ValueError('Enagas field missing: '+field)
        for entity,value in gas[field].items(): add(field,entity,value,category,unit,title)
    for field,entity,category,unit,title in [
      ('aass','Exist. Comerciales Finales','natural_gas_storage_inventories','GWh','underground commercial working-gas inventory'),
      ('gasPipelines','Final','gas_pipeline_linepack','GWh','end-of-day pipeline linepack'),
      ('transferNetInflows','BiometanoDistribucion','biomethane_supply','GWh per gas day','distribution biomethane inflows'),
      ('transferNetInflows','BiometanoTransporte','biomethane_supply','GWh per gas day','transmission biomethane inflows')]:
        if entity in gas.get(field,{}): add(field,entity,gas[field][entity],category,unit,title)
    # Commodity demand comes directly from the operator. Border pipeline flows
    # are excluded because the existing ENTSOG family already covers them.
    for field in ['nationalDemand','conventionalDemand','electricitySector']:
        if field in obj.get('otherRequestedPPDDataReportPojo',{}):
            add('demand',field,obj['otherRequestedPPDDataReportPojo'][field],'natural_gas_demand','GWh per gas day',field)
    return pd.DataFrame(rows),obj.get('lastDate')

def collect_enagas(refresh=False):
    access=Access(refresh,interval=.6)
    page,ph=access.get(ENAGAS_LANDING)
    soup=BeautifulSoup(page,'html.parser')
    component=soup.select_one('.daily-system-tranking-section')
    endpoint='https://www.enagas.es'+component['data-currentpage']+'.dailysystemtrackingdto.json'
    # The public page JS defines the GET endpoint and date parameter.
    scripts=[urljoin(ENAGAS_LANDING,s['src']) for s in soup.find_all('script',src=True) if '/dailysystemtracking.' in s['src']]
    if len(scripts)!=1: raise ValueError('Enagas endpoint documentation script missing')
    _,jh=access.get(scripts[0])
    dates=pd.date_range('2025-01-01',pd.Timestamp.now().normalize()-pd.Timedelta(days=1))
    pieces=[];hashes=[];receipts=[]
    def fetch(day):
        raw,h=access.get(endpoint,params={'date':day.strftime('%d/%m/%Y')},historical=day<dates[-1]-pd.Timedelta(days=30),max_age_hours=24 if day>=dates[-1]-pd.Timedelta(days=30) else 24*365)
        try:
            frame,last=parse_enagas(raw,day)
        except Exception as exc:
            access.blocked='Source parser failure: '+str(exc)
            raise
        return day,h,frame,last
    with ThreadPoolExecutor(max_workers=3) as pool:
        for day,h,frame,last in pool.map(fetch,dates):
            pieces.append(frame);hashes.append(h)
            receipts.append({'date':day.strftime('%Y-%m-%d'),'raw_sha256':h,'source_lastDate_raw':last})
            if len(hashes)%50==0: print('ENAGAS_REPORTS',len(hashes),flush=True)
    manifest=archive_raw_manifest(hashes,description='Original Enagas daily gas-system JSON reports; requested gas date retained in acquisition index')
    atomic_bytes(ROOT/'logs/supply6_enagas_reports.json',json.dumps({'endpoint':endpoint,'reports':receipts},indent=2).encode())
    data=pd.concat(pieces,ignore_index=True)
    for (group,entity),r in data.groupby(['group','entity'],sort=True):
        if (r.value!=0).sum()<90:
            OMISSIONS['enagas_fewer_than_90_active_days']+=1;continue
        first=r.iloc[0]
        save('enagas_'+slug(group)+'_'+slug(entity),r[['date','value']],
             title='Spain '+first.title+': '+entity,provider='enagas_gts',category=first.category,unit=first.unit,frequency='daily',
             source_url=ENAGAS_LANDING,geography='Spain: '+entity,country_codes=['ES'],geography_scope='non_us',
             hypothesis='LNG inventories, cargo arrivals, truck loadings, underground stocks and gas demand reveal supply tightness and substitution into power generation.',
             limitations='Latest available operator reports, not historical first releases. Source lastDate strings retained in acquisition index but their timezone and first-release status are not independently verified. Native total-plant rows overlap individual terminals. Stocks use physical quantity/energy level units even though page general heading says GWh/day. Missing fields omitted; reported zeros retained; no fill. Regasification send-out, storage injections/withdrawals and national gas-field inflows are excluded as alternate reports of measurements already covered by ENTSOG.',
             raw_sha256=[ph,jh],raw_manifest_sha256=[manifest],dataset_identity='enagas:daily_system_tracking:'+group+':'+entity,
             source_group=group,source_entity_id=entity,expected_publication_lag='Generally next gas day; source lastDate is an update label, not independently verified first availability.',
             timestamp_semantics='Requested Spanish gas-day calendar label encoded UTC; not release timestamp. No assumed historical availability.',
             aggregation='Native daily field; only end-of-day inventory and one unit retained. No differencing or resampling.',refresh_cadence='daily',refresh_revision_window_days=30,older_archive_cache_hours=8760)


CENAGAS_BASE='https://boletin-gestor.cenagas.gob.mx'
CENAGAS_LANDING=CENAGAS_BASE+'/Multimedia/InyeccionesExtracciones'

def parse_cenagas(raw, expected_node):
    obj=json.loads(raw)
    if obj.get('nodoComercial')!=expected_node: raise ValueError('CENAGAS node response mismatch')
    dates,values=obj.get('fechas'),obj.get('valores')
    if not isinstance(dates,list) or not isinstance(values,list) or len(dates)!=len(values):
        raise ValueError('CENAGAS dates/values length mismatch')
    frame=pd.DataFrame({'date':pd.to_datetime(dates,utc=True,errors='raise'),
                        'value':pd.to_numeric(pd.Series(values),errors='coerce')}).dropna()
    # Dashboard excludes zero records from its statistics; whether each zero
    # means inactivity or an unavailable report is not independently documented.
    # Conservatively retain only positive measured source values, never fill.
    if (frame.value<0).any(): raise ValueError('Negative CENAGAS directional flow')
    zeros=int(frame.value.eq(0).sum())
    frame=frame[frame.value>0].reset_index(drop=True)
    if frame.date.duplicated().any(): raise ValueError('CENAGAS duplicate source dates')
    return frame,obj,zeros

def parse_cenagas_excel(raw, sheet):
    data=pd.read_excel(io.BytesIO(raw),sheet_name=sheet)
    if list(data.columns) not in (['Fecha','Nodo Comercial','Zona','Cantidad'],['Fecha','Nodo Comercial','Zona','Origen','Cantidad']):
        raise ValueError('CENAGAS bulk export schema changed')
    data=data.rename(columns={'Fecha':'date','Nodo Comercial':'node','Zona':'zone','Origen':'origin','Cantidad':'value'})
    data['date']=pd.to_datetime(data.date,utc=True,errors='raise')
    data['value']=pd.to_numeric(data.value,errors='coerce')
    data['node']=data.node.astype(str).str.strip()
    if data.duplicated(['date','node']).any(): raise ValueError('CENAGAS bulk duplicate node/date')
    if (data.value<0).any(): raise ValueError('CENAGAS bulk negative directional quantity')
    return data

def collect_cenagas(refresh=False):
    access=Access(refresh,interval=.6)
    page,ph=access.get(CENAGAS_LANDING)
    soup=BeautifulSoup(page,'html.parser')
    forms=[f for f in soup.find_all('form',action=True) if 'DescargarExcel' in f['action']]
    if len(forms)!=1: raise ValueError('CENAGAS public Excel export form missing')
    url=urljoin(CENAGAS_LANDING,forms[0]['action'])
    receipts=[]
    for direction,sheet in [('Inyeccion','Inyecciones'),('Extraccion','Extracciones')]:
        meta_url=CENAGAS_BASE+'/Multimedia/ObtenerMetadata'+direction
        raw,mh=access.get(meta_url);meta=json.loads(raw)
        params={'tipo':sheet,'fechaInicio':str(meta['fechaMin'])[:10] if direction=='Inyeccion' else '2025-01-01','fechaFin':str(meta['fechaMax'])[:10]}
        raw,h=access.get(url,params=params,timeout=90)
        data=parse_cenagas_excel(raw,sheet)
        receipts.append({'direction':direction,'sheet':sheet,'url':url,'params':params,'raw_sha256':h,'rows':len(data),'nodes':data.node.nunique()})
        for name,r in data.groupby('node',sort=True):
            sid='cenagas_'+slug(direction)+'_'+slug(name)
            zeros=int(r.value.eq(0).sum());missing=int(r.value.isna().sum())
            frame=r[r.value>0][['date','value']]
            last=r.sort_values('date').iloc[-1]
            save(sid,frame,title='Mexico SISTRANGAS '+('gas injection' if direction=='Inyeccion' else 'gas withdrawal')+': '+name,
                 provider='mexico_cenagas',category='natural_gas_pipeline_injections' if direction=='Inyeccion' else 'natural_gas_pipeline_withdrawals',
                 unit='GJ per day',frequency='daily',source_url=url,geography='Mexico: '+str(last.zone)+': '+name,
                 country_codes=['MX'],geography_scope='non_us',hypothesis='Commercial-node gas inflows and industrial/power withdrawals reveal Mexican fuel supply, LNG/US import dependence and industrial operating demand.',
                 limitations='Latest historical operator Excel export, not historical first releases. Daily quantities currently end at prior month-end; publication lag not established. Injection archive acquired from October2018; withdrawal export acquired from January2025 because the full eight-year withdrawal export was stopped after a bounded acquisition attempt; the recent export completed successfully. Source-zero records omitted conservatively because missing versus no-flow semantics are not independently documented; dashboard also excludes zeros from its descriptive statistics. No interpolation or daily fill. Tariff-zone label is the latest descriptive row; historical zone records remain in original workbook.',
                 raw_sha256=[ph,mh,h],dataset_identity='cenagas:sistrangas:'+direction+':'+name,
                 source_entity_id=name,source_direction=direction,source_tariff_zone=str(last.zone).strip(),source_gas_origin=str(last.get('origin')).strip() if pd.notna(last.get('origin')) else None,source_sheet=sheet,source_export_sha256=h,source_export_params=params,
                 source_zero_records_omitted=zeros,source_missing_records_omitted=missing,source_period_end=meta['fechaMax'],
                 expected_publication_lag='Not independently established; source end '+str(meta['fechaMax'])[:10]+' retrieved '+pd.Timestamp.now(tz='UTC').strftime('%Y-%m-%d')+'. Treat as delayed historical data until future first-seen monitoring measures lag.',
                 timestamp_semantics='Native Mexican daily gas reporting date as UTC period label, not UTC measurement instant or release time.',
                 aggregation='One native daily energy quantity per commercial node and direction; positive source observations only.',refresh_cadence='weekly check; source currently updates through month-end')
    atomic_bytes(ROOT/'logs/supply6_cenagas_exports.json',json.dumps({'exports':receipts},indent=2).encode())

PROVIDERS = {'eia':collect_eia,'india_coal':collect_india_coal,'enagas':collect_enagas,'cenagas':collect_cenagas}



def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--provider',choices=['all']+list(PROVIDERS),default='all')
    p.add_argument('--refresh',action='store_true')
    args=p.parse_args()
    for name,fn in PROVIDERS.items():
        if args.provider not in ('all',name): continue
        try: fn(args.refresh)
        except Exception as exc:
            FAILURES.append({'provider':name,'error':str(exc),'type':type(exc).__name__})
            print('ERROR',name,type(exc).__name__,str(exc),flush=True)
    report={'failures':FAILURES,'omissions':dict(OMISSIONS),'staged_candidates':len(list((ROOT/'archive/staging'/GROUP).glob('*.json')))}
    atomic_bytes(ROOT/('logs/supply6_run_'+args.provider+'.json'),json.dumps(report,indent=2).encode())
    print(json.dumps(report),flush=True)
    return int(bool(FAILURES))
if __name__ == '__main__': raise SystemExit(main())
