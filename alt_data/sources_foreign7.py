#!/usr/bin/env python3
"""Foreign intraday operational electricity and exchange-market histories."""
from __future__ import annotations
import argparse, csv, hashlib, io, json, re, zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from sources_asia6 import AsiaFetcher
from expansion_common import ROOT, atomic_bytes, archive_raw_manifest, normalize_frame, coverage, qualifies_research, save_candidate
GROUP='foreign7'
OMISSIONS=[]
SEEN={}
KPX='https://kpx.or.kr/powerinfoGraphSearch.es'
KPX_SOURCE='https://kpx.or.kr/powerSource.es'
IEX='https://www.iexindia.com/market-data/{product}/market-snapshot'
CENACE='https://www.cenace.gob.mx/Paginas/SIM/Reportes/EstimacionDemandaReal.aspx'

@lru_cache(maxsize=1)
def baseline():
 c=pd.read_parquet(ROOT/'catalog.parquet');old=c[c.batch.lt(9)];new=c[c.batch.eq(9)]
 return set(old.series_id),set(old.dataset_identity.dropna()),set(old.content_sha256),dict(zip(new.series_id,new.frequency))
def clean(dates,values):
 f=pd.DataFrame({'date':pd.to_datetime(dates,utc=True),'value':pd.to_numeric(values,errors='raise')}).dropna()
 if f.groupby('date').value.nunique().gt(1).any():raise ValueError('Conflicting observations at same timestamp')
 return normalize_frame(f.drop_duplicates('date'))
def save(sid,f,**meta):
 ids,identities,hashes,pinned=baseline();f=normalize_frame(f)
 h=hashlib.sha256(f.date.astype('int64').to_numpy().tobytes()+f.value.to_numpy().tobytes()).hexdigest()
 reason=None
 if sid in ids or meta['dataset_identity'] in identities:reason='Previously published identity'
 if pinned and pinned.get(sid)!=meta['frequency']:reason='Outside frozen batch9 selection'
 if f.value.nunique()<2 or not qualifies_research({'frequency':meta['frequency'],**coverage(f)}):reason='Constant or insufficient genuine history'
 if h in hashes or (h in SEEN and SEEN[h]!=sid):reason='Duplicate numeric history'
 if reason:OMISSIONS.append({'series_id':sid,'reason':reason});return None
 SEEN[h]=sid
 meta.update(country_code_standard='ISO 3166-1 alpha-2',geography_scope='non_us',point_in_time=False,finer_frequency_available=False)
 meta.setdefault('native_frequency',meta['frequency']);meta.setdefault('revision_policy','Provider historical files may revise; archived response bytes retained, historical first release not established.')
 m=save_candidate(GROUP,sid,f,**meta);print('OK',sid,len(f),flush=True);return m

def month_ranges(start,end):
 d=pd.Timestamp(start);end=pd.Timestamp(end)
 while d<=end:
  stop=min(d+pd.offsets.MonthEnd(0),end)
  yield d.strftime('%Y-%m-%d'),stop.strftime('%Y-%m-%d')
  d=stop+pd.Timedelta(days=1)

def js_array(text,name):
 m=re.search(r'\bvar\s+'+re.escape(name)+r'\s*=\s*(\[.*?\]);',text,re.S)
 if not m:raise ValueError('Missing JS array '+name)
 return json.loads(m[1])

def parse_kpx(raw,days):
 text=raw.decode('utf-8');out=defaultdict(list)
 for day,clock,market,total in zip(days,['y_time','t_time'],['y','x'],['w','v']):
  times=js_array(text,clock);a=js_array(text,market);b=js_array(text,total)
  if not times:continue
  if len(times)!=len(a):raise ValueError('KPX market observation clock and value arrays do not align')
  dt=pd.to_datetime(pd.Series(times).astype(str),format='%Y%m%d%H%M%S')
  if set(dt.dt.strftime('%Y-%m-%d'))!={day}:raise ValueError('KPX ignored query dates')
  if ((dt.dt.minute%5)!=0).any() or dt.dt.second.ne(0).any() or dt.duplicated().any():raise ValueError('Invalid KPX clock')
  for metric,vals in [('market_demand',a),('estimated_total_demand',b)]:
   if metric.startswith('estimated') and day<'2024-11-23':continue
   if len(vals)!=len(times):
    OMISSIONS.append({'candidate':day+':'+metric,'reason':'Source value and timestamp arrays do not align; entire metric-day omitted'});continue
   for t,v in zip(dt,vals):
    if v is None or v<=0:continue
    out[metric].append((t.tz_localize('Asia/Seoul').tz_convert('UTC'),float(v)))
 return {k:clean(*zip(*v)) for k,v in out.items()}

KPX_FIELDS={'raisingWater':('pumped_storage','pumped-storage hydro generation'), 'waterPower':('hydro','hydroelectric generation'), 'gas':('gas','gas-fired generation'), 'oil':('oil','oil-fired generation'), 'coal':('coal','bituminous coal generation'), 'localCoal':('anthracite','domestic anthracite generation'), 'nuclearPower':('nuclear','nuclear generation'), 'sunlight':('market_solar','electricity-market solar generation'), 'ppa':('estimated_ppa_solar','estimated PPA solar generation'), 'btm':('estimated_btm_solar','estimated behind-the-meter solar generation'), 'windPower':('wind','wind generation')}
def parse_kpx_generation(raw,day):
 text=raw.decode('utf-8');records=js_array(text,'ictArr')
 if not records:return {}
 df=pd.DataFrame(records);dt=pd.to_datetime(df.once,format='%Y%m%d%H%M%S')
 if set(dt.dt.strftime('%Y-%m-%d'))!={day}:raise ValueError('KPX generation ignored requested date')
 if ((dt.dt.minute%5)!=0).any() or dt.dt.second.ne(0).any() or dt.duplicated().any():raise ValueError('Invalid KPX generation clock')
 dt=dt.dt.tz_localize('Asia/Seoul').dt.tz_convert('UTC')
 out={}
 for field in KPX_FIELDS:
  if field not in df:continue
  vals=pd.to_numeric(df[field],errors='raise')
  if field in ['ppa','btm'] and day<'2024-11-23':continue
  out[field]=clean(dt,vals)
 return out

def kpx(fetch,start,end):
 # Demand: native endpoint compares two individual days, not the whole intervening range.
 days=pd.date_range(start,end);jobs=[(d.strftime('%Y-%m-%d'),min(d+pd.Timedelta(days=1),days[-1]).strftime('%Y-%m-%d')) for d in days[::2]]
 frames=defaultdict(list);parts=[]
 def one(pair):
  raw,h=fetch.get(KPX,{'mid':'a10404030000','device':'chart','view_sdate':pair[0],'view_edate':pair[1]},max_age_hours=720)
  return pair,h,parse_kpx(raw,pair)
 with ThreadPoolExecutor(3) as pool:
  for i,(pair,h,data) in enumerate(pool.map(one,jobs)):
   parts.append({'days':list(pair),'sha256':h})
   for k,v in data.items():frames[k].append(v)
   if i%20==0:print('KPX demand',i,len(jobs),flush=True)
 manifest=archive_raw_manifest([p['sha256'] for p in parts],description='KPX original historical five-minute demand comparison HTML pages')
 atomic_bytes(ROOT/'logs/foreign7_kpx_demand_partitions.json',json.dumps(parts,indent=2).encode())
 good=[]
 for metric,fs in frames.items():
  f=pd.concat(fs);m=save('kr_kpx7_'+metric+'_5min',clean(f.date,f.value),title='South Korea: five-minute '+metric.replace('_',' '),provider='Korea Power Exchange operational telemetry',category='korean_intraday_electricity_demand',unit='MW',frequency='5min',source_url=KPX,country_codes=['KR'],geography='South Korea national electricity system',dataset_identity='kpx:powerinfoGraphSearch:5min:'+metric,source_metric=metric,raw_sha256=[manifest],raw_manifest_sha256=[manifest],source_partitions=parts,source_timezone='Asia/Seoul',timestamp_semantics='Native five-minute instantaneous observation time in Asia/Seoul, converted to UTC; not publication or historical availability.',expected_publication_lag='Live dashboard refreshed every five minutes; exact observation-to-publication delay and original historical release timestamps are not supplied.',hypothesis='Intraday demand deviations may reveal industrial utilization, weather load and electricity-market stress.',limitations='Instantaneous operational MW, not five-minute energy. Total demand includes estimated non-market solar; PPA and behind-the-meter values are model estimates, not official metering. Total demand only available from 2024-11-23. Zero/nonpositive national-demand placeholders omitted; total-demand metric-days with unaligned source value and timestamp arrays omitted in full; no filling. Historical pages can revise.')
  if m:good.append(m)
 return good

def kpx_generation(fetch,start,end):
 frames=defaultdict(list);parts=[]
 def one(day):
  d=day.strftime('%Y-%m-%d');raw,h=fetch.get(KPX_SOURCE,{'device':'chart','view_sdate':d,'view_edate':d},max_age_hours=720)
  return d,h,parse_kpx_generation(raw,d)
 with ThreadPoolExecutor(3) as pool:
  for i,(day,h,data) in enumerate(pool.map(one,pd.date_range(start,end))):
   parts.append({'day':day,'sha256':h})
   for k,v in data.items():frames[k].append(v)
   if i%30==0:print('KPX generation',day,flush=True)
 atomic_bytes(ROOT/'logs/foreign7_kpx_generation_partitions.json',json.dumps(parts,indent=2).encode())
 manifest=archive_raw_manifest([p['sha256'] for p in parts],description='KPX five-minute generation source historical pages')
 good=[]
 for field,fs in frames.items():
  metric,label=KPX_FIELDS[field];f=pd.concat(fs);m=save('kr_kpx7_'+metric+'_generation_5min',clean(f.date,f.value),title='South Korea: five-minute '+label,provider='Korea Power Exchange operational telemetry',category='korean_intraday_generation_mix',unit='MW',frequency='5min',source_url=KPX_SOURCE,country_codes=['KR'],geography='South Korea national electricity system',dataset_identity='kpx:powerSource:5min:'+field,source_metric=field,raw_sha256=[manifest],raw_manifest_sha256=[manifest],source_partitions=parts,source_timezone='Asia/Seoul',timestamp_semantics='Native five-minute operational observation time in Asia/Seoul, converted to UTC; not release time.',expected_publication_lag='Five-minute operational dashboard. Exact historical first-publication lag and revision clocks unavailable.',hypothesis='Generation mix changes can indicate fuel substitution, outages and power-system supply stress.',limitations='Operational instantaneous MW, not interval energy. Pumped-storage values are signed: negative values indicate pumping consumption. PPA and behind-the-meter solar are estimates, not official metered generation. Source renewable classification changes on 2026-07-01; ambiguous combined renewable fields and duplicate totals excluded. Individual native fuel fields retained without summing an unofficial national total. Zero values can be legitimate, especially solar at night. Source missing records omitted, no filling.')
  if m:good.append(m)
 return good

def parse_iex(raw,start,end):
 soup=BeautifulSoup(raw,'html.parser');payload=''
 for node in soup.find_all('script'):
  s=node.string or node.get_text()
  for m in re.finditer(r'self\.__next_f\.push\((\[.*?\])\)',s,re.S):
   obj=json.loads(m[1]);
   if len(obj)>1 and isinstance(obj[1],str):payload+=obj[1]
 m=re.search(r'"reportData":',payload)
 if not m:raise ValueError('No IEX reportData payload')
 obj,_=json.JSONDecoder().raw_decode(payload[m.end():].lstrip());rows=obj['records']
 if not rows:raise ValueError('No IEX interval records')
 df=pd.DataFrame(rows);days=pd.to_datetime(df.date,format='%d-%m-%Y')
 if days.min()<pd.Timestamp(start) or days.max()>pd.Timestamp(end):raise ValueError('IEX ignored requested dates')
 df['period']=df.period.str.replace(r'\s+','',regex=True)
 if not df.period.str.fullmatch(r'\d\d:\d\d-\d\d:\d\d').all():raise ValueError('IEX payload not quarter-hourly')
 mins=df.period.str[:5].map(lambda s:int(s[:2])*60+int(s[3:]));ends=df.period.str[-5:].map(lambda s:int(s[:2])*60+int(s[3:]));
 if not ((ends-mins).eq(15)&mins.between(0,1425)&ends.between(15,1440)&mins.mod(15).eq(0)&ends.mod(15).eq(0)).all():raise ValueError('Invalid quarter-hour window')
 df['date']=(days+pd.to_timedelta(mins,unit='m')).dt.tz_localize('Asia/Kolkata').dt.tz_convert('UTC')
 if df.date.duplicated().any():raise ValueError('Duplicate IEX interval')
 return df

def iex(fetch,start,end):
 good=[]
 for product,short in [('real-time-market','rtm'),('day-ahead-market','dam')]:
  parts=[];frames=[]
  for a,b in month_ranges(start,end):
   params={'interval':'ONE_FOURTH_HOUR','dp':'SELECT_RANGE','fromDate':pd.Timestamp(a).strftime('%d-%m-%Y'),'toDate':pd.Timestamp(b).strftime('%d-%m-%Y'),'showGraph':'false'}
   raw,h=fetch.get(IEX.format(product=product),params,max_age_hours=720,timeout=120);df=parse_iex(raw,a,b);frames.append(df);parts.append({'start':a,'end':b,'sha256':h});print('IEX',short,a,len(df),flush=True)
  atomic_bytes(ROOT/f'logs/foreign7_iex_{short}_partitions.json',json.dumps(parts,indent=2).encode());df=pd.concat(frames)
  manifest=archive_raw_manifest([x['sha256'] for x in parts],description='IEX native fifteen-minute monthly market-snapshot pages')
  for col,label in [('purchase_bid','purchase bids'),('sell_bid','sell bids'),('mcv','unconstrained market-cleared volume'),('final_scheduled_volume','final scheduled volume'),('mcp','unconstrained market clearing price')]:
   if col not in df:continue
   m=save('in_iex7_'+short+'_'+col+'_15min',clean(df.date,pd.to_numeric(df[col].replace('-',np.nan),errors='raise')),title='India IEX '+short.upper()+': '+label,provider='Indian Energy Exchange',category='indian_intraday_power_market',unit='INR/MWh' if col=='mcp' else 'MW',frequency='15min',source_url=IEX.format(product=product),country_codes=['IN'],geography='India IEX '+short.upper()+' auction market',dataset_identity='iex:market_snapshot:15min:'+short+':'+col,source_metric=col,source_product=product,source_partitions=parts,raw_sha256=[manifest],raw_manifest_sha256=[manifest],source_timezone='Asia/Kolkata',timestamp_semantics='Start of native 15-minute delivery block in Asia/Kolkata, converted to UTC. This is delivery time, not auction-clearing or publication time.',expected_publication_lag='Auction results describe future delivery blocks; historical first-publication times not supplied. Final scheduled volumes can incorporate real-time curtailment uploaded on trading day T+2, per source notes.',hypothesis='Bid imbalances, clearing prices and scheduled electricity volumes may reveal industrial activity and short-term grid scarcity.',limitations='IEX exchange market only, not nationwide actual consumption or generation. Bids are submitted demand/supply, not executed transactions; MCV is unconstrained clearing and final schedules can differ following congestion or curtailment. Null interval fields are omitted without filling. Latest historical report may include revisions; do not treat final schedules as known before delivery.',license='IEX public report; attribution retained. Source states all rights reserved and correct quotation with attribution permitted.')
   if m:good.append(m)
 return good

CENACE_COLUMNS={'Generacion (MWh)':('generation','generation'), 'Importacion Total (MWh)':('imports','total electricity imports'), 'Exportacion Total (MWh)':('exports','total electricity exports'), 'Intercambio neto entre Gerencias (MWh)':('net_interchange','net interchange between control regions'), 'Estimacion de Demanda por Balance (MWh)':('balance_demand','estimated demand by balance')}

def cenace_payload(page,start,end,method):
 soup=BeautifulSoup(page,'html.parser');payload={x['name']:x.get('value','') for x in soup.find_all('input',{'name':True}) if x.get('type') not in ['submit','image']}
 for prefix,day in [('FI',start),('FF',end)]:
  name='RadDatePicker'+prefix+'VisualizarPor'+method;k='ctl00_ContentPlaceHolder1_'+name
  day=pd.Timestamp(day);iso=day.strftime('%Y-%m-%d');shown=day.strftime('%d/%m/%Y');clock=iso+'-00-00-00'
  payload['ctl00$ContentPlaceHolder1$'+name]=iso
  payload['ctl00$ContentPlaceHolder1$'+name+'$dateInput']=shown
  maximum=re.search(r'\"maxDate\":\"(\d{4}-\d{2}-\d{2}-00-00-00)\"',page.decode())[1]
  minimum='2016-01-27-00-00-00'
  payload[k+'_dateInput_ClientState']=json.dumps({'enabled':True,'emptyMessage':'','validationText':clock,'valueAsString':clock,'lastSetTextBoxValue':shown,'minDateStr':minimum,'maxDateStr':maximum})
  payload[k+'_ClientState']=json.dumps({'minDateStr':minimum,'maxDateStr':maximum})
 payload['ctl00$ContentPlaceHolder1$DescargarArchivosCsv_Por'+method]='Descargar en archivo .zip'
 return payload

def parse_cenace(raw,start,end):
 z=zipfile.ZipFile(io.BytesIO(raw));chosen={};out=[];refs=[]
 for member in z.namelist():
  m=re.search(r'_(\d+)_v\d+ Dia Operacion (\d{4}-\d{2}-\d{2}) v(\d{4} \d{2} \d{2}_\d{2} \d{2} \d{2})\.csv$',member)
  if not m:raise ValueError('Unknown CENACE ZIP report name '+member)
  liquidation,day,publication=int(m[1]),m[2],m[3]
  if not start<=day<=end:raise ValueError('CENACE ignored requested date interval')
  # Original liquidation 0, earliest still downloadable file for this report date.
  # Retain all later reports in the original ZIP, but do not overwrite with later settlement.
  if liquidation!=0:continue
  if day not in chosen or publication<chosen[day][0]:chosen[day]=(publication,member)
 for day,(publication,member) in sorted(chosen.items()):
  b=z.read(member);text=b.decode('utf-8-sig');rows=list(csv.reader(io.StringIO(text)))
  header=next((i for i,r in enumerate(rows) if len(r)>2 and r[0].strip()=='Sistema'),None)
  if header is None:raise ValueError('CENACE report header missing')
  if not any('LIQUIDACION 0' in ''.join(r) and pd.Timestamp(day).strftime('%d/%m/%Y') in ''.join(r) for r in rows[:header]):raise ValueError('CENACE filename and report operation disagree')
  df=pd.DataFrame(rows[header+1:],columns=[x.strip() for x in rows[header]])
  df['hour']=pd.to_numeric(df['Hora'],errors='raise').astype(int)
  for (system,area),part in df.groupby(['Sistema','Area']):
   # The raw reports do not provide UTC offsets. DST transition days with hours
   # outside 1..24 cannot be encoded safely using simple interval labels.
   if not part.hour.between(1,24).all() or part.hour.duplicated().any():
    OMISSIONS.append({'candidate':day+':'+system+':'+area,'reason':'Ambiguous/non-24-hour source clock day omitted'});continue
   for column,(metric,label) in CENACE_COLUMNS.items():
    if column not in part:continue
    for row in part.to_dict('records'):
     value=row[column].strip()
     if not value or value in {'---','-','NA','N/A'}:continue
     out.append({'date':pd.Timestamp(day,tz='UTC')+pd.Timedelta(hours=int(row['hour'])-1),'value':float(value),'system':system,'area':area,'metric':metric})
  refs.append({'operation_date':day,'source_publication_local_label':pd.to_datetime(publication,format='%Y %m %d_%H %M %S').isoformat(),'member':member,'liquidation':0})
 if not out:raise ValueError('No CENACE initial-settlement observations')
 return pd.DataFrame(out),refs

def cenace(fetch,start,end):
 # Official site calendar ends fourteen days before now for this settlement report.
 end=min(pd.Timestamp(end),pd.Timestamp.now(tz='UTC').tz_localize(None).normalize()-pd.Timedelta(days=15)).strftime('%Y-%m-%d')
 page,pageh=fetch.get(CENACE);frames=[];parts=[]
 for a,b in month_ranges(start,end):
  payload=cenace_payload(page,a,b,'Balance');raw,h=fetch.post(CENACE,payload,form=True,timeout=120,max_age_hours=720)
  df,refs=parse_cenace(raw,a,b);frames.append(df);parts.append({'start':a,'end':b,'sha256':h,'reports':refs});print('CENACE',a,len(df),flush=True)
 atomic_bytes(ROOT/'logs/foreign7_cenace_partitions.json',json.dumps(parts,indent=2).encode())
 manifest=archive_raw_manifest([p['sha256'] for p in parts],description='CENACE original monthly ZIPs: hourly estimated demand balance, all settlement versions retained')
 df=pd.concat(frames);good=[]
 labels={m:l for m,l in CENACE_COLUMNS.values()}
 for (system,area,metric),part in df.groupby(['system','area','metric']):
  m=save('mx_cenace7_'+system.lower()+'_'+area.lower()+'_'+metric+'_hourly',clean(part.date,part.value),title=f'Mexico CENACE {system}/{area}: hourly {labels[metric]}',provider='Mexico CENACE initial settlement demand balance',category='mexican_hourly_power_balance',unit='MWh per reporting hour',frequency='hourly',source_url=CENACE,country_codes=['MX'],geography=f'Mexico electricity system {system}, control area {area}',dataset_identity=f'cenace:demanda_balance:liquidacion0:{system}:{area}:{metric}',source_system=system,source_area=area,source_metric=metric,source_settlement=0,raw_sha256=[pageh,manifest],raw_manifest_sha256=[manifest],source_partitions=parts,source_timezone='Provider market-hour label; UTC offset unverified',timestamp_semantics='CENACE operating-day/hour period label represented on a UTC calendar: source hour 1 becomes 00:00, hour 24 becomes 23:00. NOT a verified UTC physical observation instant. Days with ambiguous duplicate or out-of-range hours omitted.',expected_publication_lag='Initial liquidation-0 daily report normally published about 14 calendar days after operation. Exact source publication labels retained per original report; timezone unverified, so they are not asserted UTC availability instants.',source_refresh_cadence='daily initial settlement, followed by later re-settlements',revision_policy='Retains earliest currently downloadable liquidation-0 report per operation day. Original ZIPs also retain later settlement revisions. Historical first publication immutability is not established.',hypothesis='Regional load, generation and cross-border energy imbalances can identify slower industrial and supply shifts; fourteen-day release delay limits immediate-trading applications.',limitations='Hourly energy (MWh), not instantaneous MW. Balance demand includes technical and nontechnical losses. Use system plus area jointly: source BCS rows label their area BCA. Original liquidation-0 retained rather than later settlement revisions; still not proven historical first-release values. Source market-hour UTC offset is unverified, so timestamps are explicitly period labels unsuitable for exact cross-market intraday alignment without additional clock mapping. Missing fields and ambiguous clock days omitted, no filling.')
  if m:good.append(m)
 return good

PROVIDERS={'cenace':cenace,'kpx':kpx,'kpx_generation':kpx_generation,'iex':iex}
def run(provider=None,refresh=False,start=None,end=None):
 end=end or (pd.Timestamp.now(tz='UTC')-pd.Timedelta(days=2)).strftime('%Y-%m-%d');good=[];errors={};fetch=AsiaFetcher(refresh=refresh)
 for name in [provider] if provider else PROVIDERS:
  try:
   out=PROVIDERS[name](fetch,start or {'iex':'2022-04-01','cenace':'2024-01-01'}.get(name,'2025-01-01'),end);good+=out;report={'provider':name,'series':len(out),'observations':sum(x['rows'] for x in out),'errors':[],'omissions':OMISSIONS}
  except Exception as exc:
   import traceback;traceback.print_exc();errors[name]=str(exc);report={'provider':name,'errors':[repr(exc)]}
  atomic_bytes(ROOT/f'logs/foreign7_{name}.json',json.dumps(report,indent=2).encode())
 return good,errors
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--provider',choices=PROVIDERS);p.add_argument('--refresh',action='store_true');p.add_argument('--start');p.add_argument('--end');a=p.parse_args();_,errors=run(a.provider,a.refresh,a.start,a.end);raise SystemExit(bool(errors))
