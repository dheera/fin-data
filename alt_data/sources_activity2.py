#!/usr/bin/env python3
"""Observed physical-economy expansion: grids, pipelines, ports and passenger activity.

Only source observations are retained. No interpolation, synthetic zero days,
unit-converted duplicates or renamed existing series are staged.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO, StringIO
import json
import hashlib
import bz2
import re
import time
from urllib.parse import urlencode
import pandas as pd
from expansion_common import Fetcher, ROOT, atomic_bytes, save_candidate

START = '2024-01-01'
END = pd.Timestamp.now(tz='UTC').normalize().date().isoformat()
FAILURES = []
OMISSIONS = []

def slug(x):
    return re.sub('[^a-z0-9]+','_',str(x).lower()).strip('_')

def fail(name, exc):
    message=str(exc)
    expected=any(term in message for term in ['Constant or empty series','insufficient genuine daily history','Port metadata identity has no observations'])
    target=OMISSIONS if expected else FAILURES
    target.append({'provider':name,'error':message})
    print('OMIT' if expected else 'FAIL',name,message,flush=True)

def clean(dates, values):
    x=pd.DataFrame({'date':pd.to_datetime(dates,utc=True),'value':pd.to_numeric(values,errors='raise')}).dropna()
    # Exact repeated records may occur in overlapping source files. Contradictory
    # timestamp values must never silently pick an arbitrary revision.
    n=x.groupby('date').value.nunique()
    if (n>1).any():
        raise ValueError('Conflicting duplicate source timestamps')
    return x.drop_duplicates('date').sort_values('date').reset_index(drop=True)

def save(sid, frame, **meta):
    if frame.value.nunique()<2:
        raise ValueError('Constant or empty series')
    result=save_candidate('activity2',sid,frame,**meta)
    print('OK',sid,result['rows'],result['start'][:10],result['end'][:10],flush=True)
    return result

EIA_MAJOR = 'CISO ERCO MISO PJM SWPP NYIS ISNE BPAT TVA SOCO FPL DUK AZPS PACE PACW PSCO NEVP SRP NWMT PSEI'.split()
EIA_MEASURES = {
    'Demand (MW)': ('demand','Electricity demand'),
    'Net Generation (MW)': ('generation','Net electricity generation'),
    'Total Interchange (MW)': ('interchange','Total net electricity interchange'),
}
EIA_FUELS = {
    'Net Generation (MW) from Coal':('coal','Coal generation'),
    'Net Generation (MW) from Natural Gas':('gas','Natural gas generation'),
    'Net Generation (MW) from Nuclear':('nuclear','Nuclear generation'),
    'Net Generation (MW) from Hydropower Excluding Pumped Storage':('hydro','Conventional hydropower generation'),
    'Net Generation (MW) from Solar without Integrated Battery Storage':('solar','Solar generation without integrated batteries'),
    'Net Generation (MW) from Wind without Integrated Battery Storage':('wind','Wind generation without integrated batteries'),
    'Net Generation (MW) from Battery Storage':('battery','Battery net generation'),
    'Net Generation (MW) from Geothermal':('geothermal','Geothermal generation'),
}

def parse_eia(raw):
    wanted={'Balancing Authority','UTC Time at End of Hour','Region'} | EIA_MEASURES.keys() | EIA_FUELS.keys()
    x=pd.read_csv(BytesIO(raw),usecols=lambda c:c in wanted)
    x['date']=pd.to_datetime(x['UTC Time at End of Hour'],format='%m/%d/%Y %I:%M:%S %p',utc=True)
    return x

def collect_eia(fetch):
    chunks=[]; hashes=[]; urls=[]
    now=pd.Timestamp(END)
    for year in range(2024,now.year+1):
        for half in ('Jan_Jun','Jul_Dec'):
            if year==now.year and half=='Jul_Dec' and now.month<7: continue
            url=f'https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_BALANCE_{year}_{half}.csv'
            raw,digest=fetch.get(url,timeout=240)
            chunks.append(parse_eia(raw));hashes.append(digest);urls.append(url)
    data=pd.concat(chunks,ignore_index=True)
    data=data[data.date<pd.Timestamp(END,tz='UTC')]
    output=[]; seen_content=set()
    for ba,group in data.groupby('Balancing Authority'):
        metrics=dict(EIA_MEASURES)
        if ba in EIA_MAJOR: metrics.update(EIA_FUELS)
        for col,(key,title) in metrics.items():
            if col not in group or group[col].count()<2160:continue
            try:
                frame=clean(group.date,group[col])
                fingerprint=hashlib.sha256(pd.util.hash_pandas_object(frame,index=False).values.tobytes()).hexdigest()
                if fingerprint in seen_content: continue
                seen_content.add(fingerprint)
                output.append(save(f'eia930_{slug(ba)}_{key}',frame,
                    title=f'United States {ba}: {title}',provider='us_energy_information_administration',
                    category='electricity',unit='MW',frequency='hourly',geography=f'United States / {ba} balancing authority / {group.Region.iloc[-1]}',
                    source_url='https://www.eia.gov/electricity/gridmonitor/',source_urls=urls,
                    hypothesis=('Regional power demand measures industrial, commercial and household activity and weather sensitivity; generation and interchange identify fuel burn, congestion and utility exposure.' if key in ['demand','generation','interchange'] else f'{title} changes regional marginal fuel demand, electricity scarcity and generation margins; unexpected outages and renewable swings can affect gas, coal and power markets.'),
                    limitations='EIA Form 930 reported observations. Imputed and adjusted columns are deliberately excluded. MW is interval power, not MWh. End-of-hour UTC timestamps retained. Operational submissions may have outliers and revisions; gaps remain missing. BA ownership and boundaries can change. Historical downloads do not establish original release time.',
                    raw_sha256=hashes,dataset_identity=f'EIA930/BALANCE/{ba}/{col}',native_frequency='hourly',
                    timestamp_semantics='UTC end of operating hour',documentation_url='https://www.eia.gov/electricity/930-content/about.html',
                    source_column=col,license='US government public data'))
            except Exception as exc:fail(f'eia/{ba}/{key}',exc)
    return output

PORT_ROOT='https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services'
# Stable curated panel discovered from actual daily histories, one major port
# per economy. Metadata lists additional ports without downloadable history.
PORT_PANEL='port111 port1114 port1160 port1188 port1265 port23 port31 port387 port408 port435 port493 port504 port514 port518 port57 port694 port744 port786 port833 port908 port955'.split()

def arcgis_query(fetch,url,params):
    records=[];hashes=[];offset=0
    while True:
        q={**params,'f':'json','returnGeometry':'false','resultRecordCount':1000,'resultOffset':offset}
        raw,digest=fetch.get(url+'/query',q,timeout=120)
        payload=json.loads(raw)
        if 'error' in payload:raise ValueError(payload['error'])
        rows=[f['attributes'] for f in payload['features']]
        records.extend(rows);hashes.append(digest)
        if not payload.get('exceededTransferLimit'):break
        if not rows:raise ValueError('ArcGIS truncated response with no rows')
        offset+=len(rows)
    return pd.DataFrame(records),hashes

def collect_ports(fetch):
    meta_url=PORT_ROOT+'/PortWatch_ports_database/FeatureServer/0'
    ports,meta_hash=arcgis_query(fetch,meta_url,{'where':'1=1','outFields':'*','orderByFields':'ObjectId'})
    ports=ports[ports.portid.isin(PORT_PANEL)]
    if len(ports)!=len(PORT_PANEL): raise ValueError('Curated port identity missing from provider metadata')
    url=PORT_ROOT+'/Daily_Ports_Data/FeatureServer/0'
    output=[]
    for _,port in ports.iterrows():
        try:
            p={'where':f"portid='{port.portid}' AND date >= DATE '2024-01-01'",'outFields':'date,portid,import,export','orderByFields':'date'}
            data,hashes=arcgis_query(fetch,url,p)
            if data.empty: raise ValueError('Port metadata identity has no observations in current Daily_Ports_Data service')
            for field in ['import','export']:
                frame=clean(data.date,data[field])
                output.append(save(f'portwatch_{slug(port.ISO3)}_{slug(port.portid)}_{field}',frame,
                    title=f'{port.country}: {port.portname} estimated {field} tonnage',provider='IMF PortWatch',category='shipping_trade',
                    unit='metric tonnes',frequency='daily',geography=f'{port.country} / {port.portname}',
                    source_url=url+'/query?'+urlencode(p),
                    hypothesis=f'Maritime {field} tonnage at this economy\'s busiest vessel-call port can lead regional trade and industrial demand. Port specializations include {port.industry_top1}, {port.industry_top2} and {port.industry_top3}; disruptions may affect freight, exposed exporters and commodity supply.',
                    limitations='Experimental IMF/Oxford estimates from AIS vessel positions and load factors, not customs declarations or port authority weighings. Estimates can be revised and have coverage/model errors. Geographic panel selects each economy\'s busiest port by supplied historical vessel count; this is a present-day selection, not a point-in-time investment universe. Zero values are retained only when explicitly reported. History does not establish release-time availability.',
                    raw_sha256=hashes+meta_hash,dataset_identity=f'imf:portwatch:{port.portid}:{field}',native_frequency='daily',
                    documentation_url='https://portwatch.imf.org/',latitude=port.lat,longitude=port.lon,port_industries=[port.industry_top1,port.industry_top2,port.industry_top3],selection_rule='Fixed panel of the busiest vessel-call port in each of 21 economies with verified daily data; selected October 2026'))
        except Exception as exc:fail(f'ports/{port.portid}',exc)
        time.sleep(.12)
    return output

CTA_SOURCES={
 'rail':('5neh-572f','station_id','stationname',60),
 'bus':('jyb9-n7fm','route','route',50),
}

def collect_cta(fetch):
    output=[]
    for mode,(rid,key,label,limit) in CTA_SOURCES.items():
        url=f'https://data.cityofchicago.org/resource/{rid}.json'
        # Preserve individual station/route measurements. Rank on a fixed 2023
        # pre-sample period to avoid selecting on post-start ridership outcomes.
        params={'$select':f'{key},sum(rides) as volume','$where':"date >= '2023-01-01' AND date < '2024-01-01'",'$group':key,'$order':'volume DESC','$limit':limit}
        raw,rank_hash=fetch.get(url,params,timeout=120)
        ids=[r[key] for r in json.loads(raw)]
        selected=','.join("'"+x.replace("'","''")+"'" for x in ids)
        params={'$select':','.join(dict.fromkeys([key,label,'date','rides'])),'$where':f"date >= '{START}' AND {key} in ({selected})",'$order':f'date,{key}','$limit':50000}
        chunks=[];hashes=[rank_hash]
        for offset in range(0,2000000,50000):
            raw,h=fetch.get(url,{**params,'$offset':offset},timeout=120);rows=json.loads(raw);hashes.append(h);chunks.extend(rows)
            if len(rows)<50000:break
        else:raise ValueError('CTA pagination limit reached')
        data=pd.DataFrame(chunks)
        for ident,group in data.groupby(key):
            try:
                name=group[label].iloc[-1]
                output.append(save(f'cta_{mode}_{slug(ident)}_rides',clean(group.date,group.rides),
                    title=f'Chicago {mode}: {name} daily rides',provider='chicago_transit_authority',category='urban_mobility',unit='rides',frequency='daily',geography=f'United States / Chicago / {name}',
                    source_url=url+'?'+urlencode(params),hypothesis='Passenger activity along this specific travel corridor tracks return to offices, tourism, neighborhood footfall and consumer mobility, potentially informing local retail, property and transport revenue.',
                    limitations='Official daily CTA counts; rail station entries and bus boardings are not unique people. Service changes, construction, special events and fares can change counts. Selected using fixed 2023 ridership rank. Local service date stored as UTC midnight period label; publication and revision times are not observation dates. Missing days are omitted.',
                    raw_sha256=hashes,dataset_identity=f'cta:{rid}:{key}:{ident}:rides',native_frequency='daily',documentation_url=f'https://data.cityofchicago.org/d/{rid}',selection_rule=f'Top {limit} {mode} corridors by full-year 2023 source ridership'))
            except Exception as exc:fail(f'cta/{mode}/{ident}',exc)
    return output

# Explicit metrics avoid currency/unit-converted duplicates and forecasts.
ENERGI_CONFIG={
 'Gasflow':('GasDay',[],['KWhFromBiogas','KWhToDenmark','KWhFromNorthSea','KWhToOrFromStorage','KWhToOrFromGermany','KWhToSweden','kWhFromTyra','KWhToPoland'],'natural_gas'),
 'StorageUtilization':('GasDay',[],['StoredTotal','StoredInt','InjectedTotal','WithdrawedTotal','WithdrawedInt'],'natural_gas_storage'),
 'GasDailyBalancingPrice':('GasDay',[],['EEXSpotIndexEUR_MWh','EEXWithinDayEUR_MWh','SalesPriceDKK_kWh','PurchasePriceDKK_kWh','THEPriceDKK_kWh'],'natural_gas_prices'),
 'EntryExitGasquality':('HourUTC',['EntryExitPoint'],['StationFlow','GrossCalorificValueMJ','WobbeIndexMJ'],'gas_pipeline_flows'),
 'ConsumptionDK3619IndustryHour':('TimeUTC',['DK36Code'],['Consumption_MWh'],'industrial_electricity'),
 'ConsumptionConsumerCategoryHour':('TimeUTC',['RegionName','ConsumerCategory3'],['ConsumptionkWh'],'sector_electricity'),
 'CO2Emis':('Minutes5UTC',['PriceArea'],['CO2Emission'],'electricity_carbon'),
 'ImbalancePrice':('TimeUTC',['PriceArea'],['SatisfiedDemand','ImbalancePriceEUR','aFRRUpMW','aFRRDownMW'],'electricity_balancing'),
}

def energi_dataset(fetch,ds):
    datecol,groups,metrics,category=ENERGI_CONFIG[ds]
    base='https://api.energidataservice.dk'
    raw,meta_hash=fetch.get(base+'/meta/dataset/'+ds);meta=json.loads(raw)
    columnmeta={c['dbColumn']:c for c in meta['columns']}
    params={'start':START,'end':END,'limit':0}
    # Keep 5-minute observations; payloads are substantial but single requests
    # per dataset follow the provider's download-once/reuse guidance.
    url=base+'/dataset/'+ds
    raw,digest=fetch.get(url,params,timeout=240)
    payload=json.loads(raw);data=pd.DataFrame(payload['records'])
    if payload.get('total',len(data))>len(data):raise ValueError('Energinet truncated result')
    output=[]
    grouped=data.groupby(groups,dropna=False) if groups else [((),data)]
    for keys,frame in grouped:
        if not isinstance(keys,tuple):keys=(keys,)
        keyparts=[str(x) for x in keys]
        if ds=='ConsumptionDK3619IndustryHour':
            title_suffix=frame['DK36Title'].iloc[-1]
            # Classification changed in January 2025. Values at the same DK36
            # code/time across DK19 parents are non-overlapping constituent use.
            frame=frame.groupby(datecol,as_index=False).agg(Consumption_MWh=('Consumption_MWh',lambda x:x.sum(min_count=1)))
        else:title_suffix=' / '.join(keyparts) or 'Denmark'
        for metric in metrics:
            if metric not in frame:continue
            c=columnmeta[metric]
            if not c.get('unit'): raise ValueError('Explicit physical unit missing: '+ds+'/'+metric)
            try:
                out=clean(frame[datecol],frame[metric]);interval=out.date.diff().dt.total_seconds().median()
                freq={300:'5min',900:'15min',3600:'hourly',86400:'daily'}.get(interval,'irregular_intraday')
                if interval>=86400:freq='daily'
                identity=ds+'/'+('/'.join(keyparts)+'/' if keyparts else '')+metric
                output.append(save('energinet_'+slug(identity),out,title=f'Denmark {title_suffix}: {c["displayName"]}',provider='energinet',category=category,
                    unit=c['unit'],frequency=freq,geography='Denmark / '+title_suffix,
                    source_url=url+'?'+urlencode(params),hypothesis=('Sector electricity consumption is a direct operational proxy for production and business activity; changes can inform Danish industrial demand and exposed equities.' if 'Consumption' in ds else 'Gas flows, storage, fuel quality, carbon intensity and balancing stress affect Nordic energy scarcity, marginal fuel costs, cross-border supply and utility margins.'),
                    limitations=meta['description']+' '+str(meta.get('caution',''))+' '+c.get('description','')+' '+c.get('comment','')+' Original UTC interval labels retained for subdaily data; gas-day daily labels are period labels, not UTC instant of gas-day commencement. Missing source values omitted. Data may be revised and have publication delays. Backfilled history is not point-in-time availability. '+('Industry classification changed January 2025; constituent DK19 rows within a DK36/time are summed when necessary.' if ds=='ConsumptionDK3619IndustryHour' else ''),
                    raw_sha256=[meta_hash,digest],dataset_identity='energinet:'+identity,native_frequency=freq,documentation_url='https://api.energidataservice.dk/meta/dataset/'+ds,
                    source_metadata_url=base+'/meta/dataset/'+ds,source_resolution=meta.get('resolution'),expected_publication_lag='9–15 days or more' if ds.startswith('Consumption') else 'Dataset-specific operational publication; see metadata'))
            except Exception as exc:fail(f'energinet/{identity if "identity" in locals() else ds+metric}',exc)
    return output

def collect_energinet(fetch):
    output=[]
    for ds in ENERGI_CONFIG:
        try:output.extend(energi_dataset(fetch,ds))
        except Exception as exc:fail('energinet/'+ds,exc)
    return output


AEMO_REGIONS={'NSW1':'New South Wales','QLD1':'Queensland','VIC1':'Victoria','SA1':'South Australia','TAS1':'Tasmania'}

def parse_aemo(raw):
    data=pd.read_csv(BytesIO(raw))
    data=data[data.PERIODTYPE=='TRADE'].copy()
    # All NEM trading timestamps use fixed AEST, including DST dates.
    data['date']=pd.to_datetime(data.SETTLEMENTDATE,format='%Y/%m/%d %H:%M:%S').dt.tz_localize('Etc/GMT-10').dt.tz_convert('UTC')
    return data

def collect_aemo(fetch):
    out=[]
    periods=pd.period_range(START,pd.Timestamp(END),freq='M')
    for region,name in AEMO_REGIONS.items():
        chunks=[];hashes=[];urls=[]
        for month in periods:
            url=f'https://aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{month.year}{month.month:02d}_{region}.csv'
            raw,digest=fetch.get(url,timeout=120)
            chunks.append(parse_aemo(raw));hashes.append(digest);urls.append(url)
        data=pd.concat(chunks,ignore_index=True)
        data=data[data.date<pd.Timestamp(END,tz='UTC')]
        for col,title,unit in [('TOTALDEMAND','Electricity total demand','MW'),('RRP','Electricity regional reference price','AUD/MWh')]:
            out.append(save(f'aemo_{slug(region)}_{slug(col)}',clean(data.date,data[col]),title=f'Australia {name}: {title}',
                provider='australian_energy_market_operator',category='electricity_prices' if col=='RRP' else 'electricity',unit=unit,frequency='5min',
                geography=f'Australia / {name}',source_url='https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data',source_urls=urls,
                hypothesis='Australian state electricity demand tracks weather and industrial activity; five-minute wholesale prices expose renewable variability, fuel scarcity, congestion and utility margin shocks.',
                limitations='Official AEMO monthly price/demand downloads. Only TRADE observations; forecast PERIODTYPE rows excluded. Market timestamps use fixed Australian Eastern Standard Time (UTC+10), converted to UTC. Regional reference prices can be negative or extremely high; these are retained. Data revisions and publication lags require point-in-time care.',
                raw_sha256=hashes,dataset_identity=f'AEMO:PRICE_AND_DEMAND:{region}:{col}',native_frequency='5min',
                timestamp_semantics='Five-minute settlement interval ending timestamp, fixed AEST converted to UTC',documentation_url='https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data'))
    return out


def parse_euro_csv(raw,compressed=False):
    csvraw=bz2.decompress(raw) if compressed else raw
    # Some official exports mix UTF-8 airport names with Latin-1 country names.
    # Preserve valid UTF-8; decode only isolated invalid bytes as Latin-1.
    text=csvraw.decode('utf-8','surrogateescape')
    text=''.join(chr(ord(c)-0xDC00) if 0xDC80 <= ord(c) <= 0xDCFF else c for c in text)
    return pd.read_csv(StringIO(text))

def euro_files(fetch,stem,extension='csv',years=None):
    frames=[];hashes=[];urls=[]
    for year in years or range(2024,pd.Timestamp(END).year+1):
        url=f'https://www.eurocontrol.int/performance/data/download/csv/{stem}_{year}.{extension}'
        raw,h=fetch.get(url,timeout=120)
        frames.append(parse_euro_csv(raw,extension.endswith('bz2')))
        hashes.append(h);urls.append(url)
    return pd.concat(frames,ignore_index=True),hashes,urls

def collect_eurocontrol(fetch):
    out=[]
    ranking,rank_hash,_=euro_files(fetch,'airport_traffic',years=[2023])
    ranking=ranking.groupby('APT_ICAO').FLT_TOT_1.sum().sort_values(ascending=False)
    # Fixed pre-sample ranking of large and regional hubs gives interpretable
    # exposure to airline networks, tourism, jet fuel and air cargo.
    panel=list(ranking.head(80).index)
    traffic,hashes,urls=euro_files(fetch,'airport_traffic')
    for airport,frame in traffic[traffic.APT_ICAO.isin(panel)].groupby('APT_ICAO'):
        name=frame.APT_NAME.iloc[-1];country=frame.STATE_NAME.iloc[-1]
        out.append(save(f'eurocontrol_{slug(airport)}_ifr_movements',clean(frame.FLT_DATE,frame.FLT_TOT_1),
            title=f'{country} {name}: daily IFR aircraft movements',provider='eurocontrol',category='aviation_activity',unit='aircraft movements',frequency='daily',geography=f'{country} / {name}',
            source_url='https://ansperformance.eu/csv/#aptflt-csv',source_urls=urls,
            hypothesis='Airport aircraft movements track tourism, airline capacity and air-cargo connectivity, with potential effects on airlines, airport operators, travel companies and jet-fuel demand.',
            limitations='EUROCONTROL Network Manager IFR movements: arrivals plus departures, not passengers or distinct aircraft. Airport operator alternative counts excluded. Each flight may appear at both endpoints. Fixed top-80 ranking uses 2023 traffic, before the collected sample. Daily series are released with monthly publication lag and may be revised; not same-day availability.',
            raw_sha256=hashes+rank_hash,dataset_identity=f'EUROCONTROL:airport_traffic:{airport}:FLT_TOT_1',native_frequency='daily',documentation_url='https://ansperformance.eu/reference/dataset/airport-traffic/',expected_publication_lag='Monthly release of daily observations'))
    delays,hashes,urls=euro_files(fetch,'apt_dly','csv.bz2')
    for airport,frame in delays[delays.APT_ICAO.isin(panel[:40])].groupby('APT_ICAO'):
        name=frame.APT_NAME.iloc[-1];country=frame.STATE_NAME.iloc[-1]
        try:
            out.append(save(f'eurocontrol_{slug(airport)}_arrival_atfm_delay',clean(frame.FLT_DATE,frame.DLY_APT_ARR_1),
                title=f'{country} {name}: airport arrival ATFM delay',provider='eurocontrol',category='aviation_disruption',unit='minutes',frequency='daily',geography=f'{country} / {name}',
                source_url='https://ansperformance.eu/csv/#aptdly-csv',source_urls=urls,
                hypothesis='Capacity, weather and staffing delays at major hubs cause airline disruption costs, lost aircraft utilization and network spillovers; these can inform exposed airline and tourism earnings.',
                limitations='Airport-attributable air traffic flow management delay minutes, imposed at departure to regulate arrivals. This is not observed arrival lateness or total passenger delay. Official explicit zeros retained; no synthetic zero days. Without post-operations adjustments. Daily observations released monthly and revised; dates are not publication times.',
                raw_sha256=hashes+rank_hash,dataset_identity=f'EUROCONTROL:apt_dly:{airport}:DLY_APT_ARR_1',native_frequency='daily',documentation_url='https://ansperformance.eu/reference/dataset/airport-arrival-atfm-delay/',expected_publication_lag='Monthly release of daily observations'))
        except Exception as exc:fail('eurocontrol/'+airport,exc)
    return out

COLLECTORS={'eurocontrol':collect_eurocontrol,'aemo':collect_aemo,'eia':collect_eia,'ports':collect_ports,'cta':collect_cta,'energinet':collect_energinet}
def main():
    FAILURES.clear(); OMISSIONS.clear()
    p=argparse.ArgumentParser();p.add_argument('--refresh',action='store_true');p.add_argument('--provider',choices=COLLECTORS,action='append');args=p.parse_args()
    fetch=Fetcher(refresh=args.refresh);out=[]
    for name in args.provider or COLLECTORS:
        try:out.extend(COLLECTORS[name](fetch))
        except Exception as exc:fail(name,exc)
    report={'series':len(out),'providers':args.provider or list(COLLECTORS),'failures':FAILURES,'omissions':OMISSIONS,'completed_at':pd.Timestamp.now(tz='UTC').isoformat()}
    suffix='_'.join(args.provider or ['all'])
    atomic_bytes(ROOT/f'archive/staging/activity2/collection_report_{suffix}.json',json.dumps(report,indent=2).encode())
    print(json.dumps(report,indent=2))
    return int(bool(FAILURES))
if __name__=='__main__':raise SystemExit(main())
