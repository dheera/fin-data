#!/usr/bin/env python3
"""Daily and intraday transport, power and local economic activity, expansion 3.

One immutable selection file per feed keeps subsequent refreshes on the same
entities and measurements. Source observations and explicit source zeroes are
retained; gaps are never interpolated or manufactured.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import hashlib
import json
import re
from pathlib import Path
import time
from urllib.parse import urlencode
import pandas as pd
import pyarrow.parquet as pq
from expansion_common import ROOT, Fetcher, atomic_bytes, save_candidate, normalize_frame, coverage, qualifies_daily
from sources_activity2 import clean, slug, euro_files, parse_eia, EIA_FUELS, PORT_ROOT, arcgis_query

START='2024-01-01'
END=pd.Timestamp.now(tz='UTC').normalize().date().isoformat()
FOLDER=ROOT/'archive/staging/activity3'
FAILURES=[]
OMISSIONS=[]
BASELINE=None
SEEN=set()
# These single-fuel BA measurements have shorter reporting histories but equal
# the already-published total generation series on every candidate timestamp.
# Evidence was checked on 2026-10-20, including 100% of candidate rows; the
# preserved candidates and comparisons live in archive/rejected/activity3.
# Keep the canonical baseline history rather than count a shorter window as a
# new indicator. Explicit aliases also prevent a later refresh recreating them.
EIA_BASELINE_ALIASES = {
    **{f'eia930_{ba}_hydro':f'eia930_{ba}_generation'
       for ba in ['dopd','gcpd','scl','sepa','spa','tpwr','wauw','yad']},
    **{f'eia930_{ba}_wind':f'eia930_{ba}_generation'
       for ba in ['gwa','wwa']},
}

def baseline():
    global BASELINE
    if BASELINE is None:
        p=FOLDER/'baseline.json'
        if p.exists(): BASELINE=json.loads(p.read_text())
        else:
            c=pd.read_parquet(ROOT/'catalog.parquet')
            BASELINE={'ids':c.series_id.tolist(),'identities':c.dataset_identity.dropna().tolist(),'hashes':c.content_sha256.dropna().tolist()}
            atomic_bytes(p,json.dumps(BASELINE).encode())
    return BASELINE

def selection(name, candidates):
    p=FOLDER/('selection_'+name+'.json')
    if p.exists(): return json.loads(p.read_text())
    items=sorted(set(candidates))
    atomic_bytes(p,json.dumps(items,indent=2).encode())
    return items

def omit(name, reason):
    OMISSIONS.append({'source':name,'reason':str(reason)})

def failure(name, exc):
    FAILURES.append({'source':name,'error':str(exc)})
    print('FAIL',name,str(exc),flush=True)

def save(sid,frame,**meta):
    b=baseline()
    if sid in b['ids'] or meta['dataset_identity'] in b['identities']:
        omit(sid,'Already in the original library');return None
    canonical=EIA_BASELINE_ALIASES.get(sid)
    if canonical in b['ids']:
        omit(sid,'Shorter-window baseline alias of '+canonical);return None
    if frame.empty or frame.value.nunique()<2:
        omit(sid,'Empty or constant measurements');return None
    frame=normalize_frame(frame)
    frame['date']=frame['date'].astype('datetime64[ns, UTC]')
    cov=coverage(frame)
    if not qualifies_daily({**cov,'frequency':meta['frequency']}):
        omit(sid,'Insufficient genuine daily coverage: '+str(cov));return None
    digest=hashlib.sha256(frame.date.astype('int64').to_numpy().tobytes()+frame.value.to_numpy().tobytes()).hexdigest()
    if digest in b['hashes'] or digest in SEEN:
        omit(sid,'Identical full numeric history');return None
    SEEN.add(digest)
    meta.setdefault('native_frequency',meta['frequency'])
    meta.setdefault('finer_frequency_available',False)
    out=save_candidate('activity3',sid,frame,**meta)
    print('OK',sid,out['rows'],out['observed_days'],flush=True)
    return out

def collect_eurocontrol(fetch):
    out=[]
    configs={
      'airport_traffic':('csv',{'FLT_TOT_1':('ifr_movements','Aircraft movements','aircraft movements','aviation_activity')}),
      'apt_dly':('csv.bz2',{
       'DLY_APT_ARR_1':('arrival_atfm_delay','Airport arrival ATFM delay','minutes','aviation_disruption'),
       'DLY_APT_ARR_C_1':('arrival_atc_capacity_delay','Airport ATC capacity delay','minutes','aviation_capacity'),
       'DLY_APT_ARR_G_1':('arrival_aerodrome_capacity_delay','Aerodrome capacity delay','minutes','aviation_capacity'),
       'DLY_APT_ARR_S_1':('arrival_atc_staffing_delay','Airport ATC staffing delay','minutes','aviation_staffing'),
       'DLY_APT_ARR_W_1':('arrival_weather_delay','Airport weather delay','minutes','aviation_weather'),
       'DLY_APT_ARR_D_1':('arrival_deicing_delay','Airport de-icing delay','minutes','aviation_weather')}),
      'all_pre_departure_delays':('csv',{'DLY_ALL_PRE_2':('all_predeparture_delay','All-cause pre-departure delay','minutes','aviation_disruption')}),
      'atfm_slot_adherence':('csv',{'FLT_DEP_OUT_LATE_1':('departures_late_atfm_slot','Departures outside late ATFM slot tolerance','flights','aviation_disruption')}),
    }
    for ds,(ext,metrics) in configs.items():
        data,hashes,urls=euro_files(fetch,ds,ext)
        allowed=selection('euro_'+ds,[str(x) for x in data.APT_ICAO.dropna().unique()])
        for airport,x in data[data.APT_ICAO.isin(allowed)].groupby('APT_ICAO'):
            name=x.APT_NAME.iloc[-1];country=x.STATE_NAME.iloc[-1]
            for field,(key,title,unit,category) in metrics.items():
                try:
                    frame=clean(x.FLT_DATE,x[field])
                    if field not in ['FLT_TOT_1','DLY_APT_ARR_1'] and (frame.value>0).sum()<30:
                        omit(f'euro/{airport}/{field}','Fewer than 30 positive operational days');continue
                    item=save(f'eurocontrol_{slug(airport)}_{key}',frame,title=f'{country} {name}: {title}',provider='eurocontrol',category=category,unit=unit,frequency='daily',geography=f'{country} / {name}',
                      source_url='https://ansperformance.eu/csv/',source_urls=urls,
                      hypothesis='Airport activity and operational disruption can lead airline capacity, tourism and cargo demand, jet-fuel burn, staffing pressure and airline or airport operating margins. Cause-specific delays separate weather, labour availability and capacity bottlenecks.',
                      limitations='Official EUROCONTROL daily observations; no synthetic zero days. Network Manager IFR traffic is not passenger counts. ATFM delay is ground holding imposed to regulate downstream airport arrivals, not actual arrival lateness. Pre-departure delay is a different airport-reported measure. ATFM late-slot counts describe regulated flights outside tolerance. Daily histories are released monthly and revised, so observation date is not availability date. Source panels and reporting coverage can change. Only metrics with sufficient nonmissing history retained.',
                      raw_sha256=hashes,dataset_identity=f'EUROCONTROL:{ds}:{airport}:{field}',source_column=field,
                      documentation_url='https://ansperformance.eu/reference/dataset/airport-arrival-atfm-delay/' if ds=='apt_dly' else 'https://ansperformance.eu/data/',expected_publication_lag='Monthly publication of daily observations',selection_rule='Fixed October 2026 observed airport panel; new identities only')
                    if item:out.append(item)
                except Exception as exc: failure(f'eurocontrol/{ds}/{airport}/{field}',exc)
    # ANSP area measures describe network congestion outside individual airports.
    ds='ert_dly_ansp';data,hashes,urls=euro_files(fetch,ds,'csv.bz2')
    allowed=selection('euro_enroute',data.ENTITY_NAME.dropna().tolist())
    metrics={'FLT_ERT_1':('enroute_flights','En-route IFR flights','flights'),'DLY_ERT_1':('enroute_delay','En-route ATFM delay','minutes'),'DLY_ERT_C_1':('capacity_delay','En-route ATC capacity delay','minutes'),'DLY_ERT_S_1':('staffing_delay','En-route ATC staffing delay','minutes'),'DLY_ERT_W_1':('weather_delay','En-route weather delay','minutes')}
    for entity,x in data[data.ENTITY_NAME.isin(allowed)].groupby('ENTITY_NAME'):
        for field,(key,title,unit) in metrics.items():
            frame=clean(x.FLT_DATE,x[field])
            if (frame.value>0).sum()<30:continue
            try:
                item=save(f'eurocontrol_ansp_{slug(entity)}_{key}',frame,title=f'{entity}: {title}',provider='eurocontrol',category='airspace_operations',unit=unit,frequency='daily',geography='Europe / '+str(entity),source_url='https://ansperformance.eu/csv/#ert-dly-csv',source_urls=urls,
                 hypothesis='En-route airspace activity and congestion reveal airline network load, labour or weather bottlenecks, rerouting pressure and fuel burn beyond individual airport conditions.',limitations='EUROCONTROL Network Manager ANSP-area daily counts or ATFM delay minutes. Areas overlap flight trajectories; do not sum flights across providers as unique flights. Explicit published zeros retained; missing values omitted. Monthly publication and revision lag; history is not point-in-time availability.',raw_sha256=hashes,dataset_identity=f'EUROCONTROL:{ds}:{entity}:{field}',documentation_url='https://ansperformance.eu/reference/dataset/en-route-atfm-delay-aua/',expected_publication_lag='Monthly publication of daily observations')
                if item:out.append(item)
            except Exception as exc:failure(f'eurocontrol/{entity}/{field}',exc)
    return out

def collect_eia(fetch):
    frames=[];hashes=[];urls=[]
    for year in range(2024,pd.Timestamp(END).year+1):
        for half in ['Jan_Jun','Jul_Dec']:
            if year==pd.Timestamp(END).year and half=='Jul_Dec' and pd.Timestamp(END).month<7:continue
            url=f'https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_BALANCE_{year}_{half}.csv'
            raw,h=fetch.get(url,timeout=240);frames.append(parse_eia(raw));hashes.append(h);urls.append(url)
    data=pd.concat(frames,ignore_index=True);data=data[data.date<pd.Timestamp(END,tz='UTC')]
    allowed=selection('eia_fuel',data['Balancing Authority'].dropna().tolist());out=[]
    for ba,x in data[data['Balancing Authority'].isin(allowed)].groupby('Balancing Authority'):
        for field,(key,title) in EIA_FUELS.items():
            try:
                item=save(f'eia930_{slug(ba)}_{key}',clean(x.date,x[field]),title=f'United States {ba}: {title}',provider='us_energy_information_administration',category='electricity',unit='MW',frequency='hourly',geography=f'United States / {ba} balancing authority / {x.Region.iloc[-1]}',source_url='https://www.eia.gov/electricity/gridmonitor/',source_urls=urls,
                 hypothesis='Local fuel-specific power output identifies renewable intermittency, marginal fossil fuel demand, storage dispatch and generation outages affecting utilities and physical energy markets.',limitations='EIA Form 930 reported generation; excludes imputed and adjusted columns. UTC end-of-hour observations retained. MW represents interval power. Missing observations omitted, operational outliers and revisions retained. BA boundaries and assets can change.',raw_sha256=hashes,dataset_identity=f'EIA930/BALANCE/{ba}/{field}',documentation_url='https://www.eia.gov/electricity/930-content/about.html',expected_publication_lag='Operational reported data, generally next-day or later; subject to revision',timestamp_semantics='UTC operating hour ending')
                if item:out.append(item)
            except Exception as exc:failure(f'eia/{ba}/{field}',exc)
    return out

def collect_cta(fetch):
    out=[]
    for mode,rid,key,label in [('rail','5neh-572f','station_id','stationname'),('bus','jyb9-n7fm','route','route')]:
        url=f'https://data.cityofchicago.org/resource/{rid}.json'
        params={'$select':','.join(dict.fromkeys([key,label,'date','rides'])),'$where':f"date >= '{START}'",'$order':f'date,{key}','$limit':50000}
        rows=[];hashes=[]
        for offset in range(0,2000000,50000):
            raw,h=fetch.get(url,{**params,'$offset':offset},timeout=120);part=json.loads(raw);hashes.append(h);rows.extend(part)
            if len(part)<50000:break
        else:raise ValueError('CTA pagination limit')
        data=pd.DataFrame(rows);allowed=selection('cta_'+mode,data[key].tolist())
        for ident,x in data[data[key].isin(allowed)].groupby(key):
            try:
                name=x[label].iloc[-1]
                item=save(f'cta_{mode}_{slug(ident)}_rides',clean(x.date,x.rides),title=f'Chicago {mode}: {name} daily rides',provider='chicago_transit_authority',category='urban_mobility',unit='rides',frequency='daily',geography='United States / Chicago / '+name,source_url=url+'?'+urlencode(params),
                 hypothesis='Corridor passenger activity tracks neighborhood retail and office footfall, commuting, visitor flows and consumer mobility; disruptions may affect local business revenues.',limitations='CTA daily rail entries or bus boardings are not unique people. Source service-day labels stored at UTC midnight. Fare policy, construction and service changes may move counts. Explicit source zeroes retained; absent dates not created. Publication and revisions can lag service dates.',raw_sha256=hashes,dataset_identity=f'cta:{rid}:{key}:{ident}:rides',documentation_url=f'https://data.cityofchicago.org/d/{rid}',expected_publication_lag='Source publishes operational daily counts with a lag')
                if item:out.append(item)
            except Exception as exc:failure(f'cta/{mode}/{ident}',exc)
    return out

PORT_TYPES={'container':'Container vessels','dry_bulk':'Dry-bulk vessels','general_cargo':'General cargo vessels','roro':'Roll-on/roll-off vessels','tanker':'Tankers'}
def collect_ports(fetch):
    meta_url=PORT_ROOT+'/PortWatch_ports_database/FeatureServer/0'
    ports,meta_hash=arcgis_query(fetch,meta_url,{'where':'1=1','outFields':'*','orderByFields':'ObjectId'})
    # Round-robin through economies, each sorted by reported vessel calls, then
    # add globally busiest ports. This spreads exposures beyond giant hubs.
    ranked=ports.sort_values('vessel_count_total',ascending=False)
    groups=[x for _,x in ranked.groupby('ISO3',sort=False)]
    panel=[]
    for position in range(3):
        for group in groups:
            if len(group)>position:panel.append(group.iloc[position].portid)
    panel=selection('ports',panel[:250])
    selected=ports[ports.portid.isin(panel)]
    url=PORT_ROOT+'/Daily_Ports_Data/FeatureServer/0';out=[]
    metrics={f'{measure}_{kind}':(measure,label) for kind,label in PORT_TYPES.items() for measure in ['portcalls','import','export']}
    for _,port in selected.iterrows():
        try:
            params={'where':f"portid='{port.portid}' AND date >= DATE '2024-01-01'",'outFields':'date,portid,'+','.join(metrics),'orderByFields':'date'}
            data,hashes=arcgis_query(fetch,url,params)
            if data.empty:omit(port.portid,'Metadata port has no rows in the current daily feed');continue
            for field,(measure,label) in metrics.items():
                frame=clean(data.date,data[field])
                if (frame.value>0).sum()<45:
                    omit(f'{port.portid}/{field}','Fewer than 45 positive measured days');continue
                title=label+(': vessel calls' if measure=='portcalls' else f': estimated {measure} tonnage')
                item=save(f'portwatch_{slug(port.ISO3)}_{slug(port.portid)}_{field}',frame,title=f'{port.country} {port.portname} — {title}',provider='IMF PortWatch',category={'container':'container_shipping','dry_bulk':'dry_bulk_shipping','general_cargo':'general_cargo_shipping','roro':'vehicle_shipping','tanker':'tanker_shipping'}[field.split('_',1)[1]],unit='vessel calls' if measure=='portcalls' else 'metric tonnes',frequency='daily',geography=f'{port.country} / {port.portname}',source_url=url+'/query?'+urlencode(params),
                  hypothesis=f'{label} activity at this port tracks a distinct maritime channel: containers carry manufactures and retail goods, dry bulk carries ores and grain, tankers carry liquid energy or chemicals, and roll-on/roll-off vessels carry vehicles. Port-specific flows can lead exposed commodity demand, inventory tightness, freight rates and exporters. Local industrial exposure: {port.industry_top1}; {port.industry_top2}; {port.industry_top3}.',
                  limitations='Experimental IMF/Oxford estimates from AIS vessel positions and inferred load factors. Vessel counts are calls, not unique vessels or tonnage. Import/export amounts are modeled metric tonnes rather than customs declarations. Port and vessel-type classification, AIS coverage and load estimation can change and be revised. Explicit source zeroes retained; absent dates omitted. Backfilled history does not establish when each estimate was first published. Panel fixed on October 2026 port metadata and current geography; not a historical investable universe.',raw_sha256=hashes+meta_hash,dataset_identity=f'imf:portwatch:{port.portid}:{field}',documentation_url='https://portwatch.imf.org/',expected_publication_lag='Daily updates with AIS processing delay and retrospective revisions',latitude=port.lat,longitude=port.lon,source_column=field,port_industries=[port.industry_top1,port.industry_top2,port.industry_top3],selection_rule='Fixed country-diverse panel ranked by supplied historical vessel activity')
                if item:out.append(item)
        except Exception as exc:
            failure('ports/'+port.portid,exc)
            if getattr(getattr(exc,'response',None),'status_code',None) in {401,403,429}:
                break  # Respect access blocks or server-directed rate limits.
        time.sleep(.15)
    return out

def aggregate_tlc(raw,month):
    """Aggregate submitted trips to six-hour New York wall-clock periods.

    Only pickup timestamps inside the nominal monthly submission are retained.
    Midnight, 06:00, 12:00 and 18:00 are never ambiguous DST timestamps. Adjacent
    UTC bins can differ by 5/7 hours at seasonal clock changes.
    """
    columns=['tpep_pickup_datetime','PULocationID','DOLocationID','fare_amount']
    data=pq.read_table(BytesIO(raw),columns=columns).to_pandas()
    stamp=pd.to_datetime(data.tpep_pickup_datetime,errors='coerce')
    period=pd.Period(month,freq='M')
    valid=(stamp>=period.start_time)&(stamp<(period+1).start_time)
    data=data.loc[valid].copy();stamp=stamp.loc[valid]
    data['date']=stamp.dt.floor('6h').dt.tz_localize('America/New_York').dt.tz_convert('UTC')
    pickups=data.groupby(['PULocationID','date']).agg(pickups=('PULocationID','size'),fare_amount=('fare_amount',lambda x:x.sum(min_count=1))).reset_index().rename(columns={'PULocationID':'zone'})
    destinations=data.groupby(['DOLocationID','date']).size().rename('destination_trips').reset_index().rename(columns={'DOLocationID':'zone'})
    return pickups,destinations

def collect_tlc(fetch):
    page='https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    raw,page_hash=fetch.get(page);text=raw.decode()
    urls=sorted(set(re.findall(r'https://[^"\s<>]+yellow_tripdata_[^"\s<>]+parquet',text)))
    urls=[u for u in urls if 'yellow_tripdata_202' in u and u.rsplit('_',1)[1][:7]>=START[:7]]
    if not urls:raise ValueError('No official TLC monthly parquet links discovered')
    lookup='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    raw,zone_hash=fetch.get(lookup);zones=pd.read_csv(BytesIO(raw));zones=zones[zones.LocationID.between(1,263)]
    allowed=selection('tlc_zones',[str(x) for x in zones.LocationID])
    zones=zones[zones.LocationID.astype(str).isin(allowed)]
    pickups=[];destinations=[];hashes=[page_hash,zone_hash]
    for url in urls:
        raw,h=fetch.get(url,timeout=240);month=url.rsplit('_',1)[1][:7]
        pick,dest=aggregate_tlc(raw,month);pickups.append(pick);destinations.append(dest);hashes.append(h)
        print('TLC MONTH',month,len(pick),len(dest),flush=True)
    pickup=pd.concat(pickups,ignore_index=True);destination=pd.concat(destinations,ignore_index=True)
    out=[]
    for _,zone in zones.iterrows():
        for metric,title,unit,table in [('pickups','Yellow taxi pickups','trips',pickup),('fare_amount','Yellow taxi metered fares from pickups','USD',pickup),('destination_trips','Yellow taxi destination-bound trips by pickup period','trips',destination)]:
            x=table[table.zone==zone.LocationID]
            try:
                frame=clean(x.date,x[metric])
                item=save(f'nyc_tlc_yellow_zone_{int(zone.LocationID)}_{metric}',frame,title=f'New York metro {zone.Zone}: {title}',provider='new_york_city_taxi_and_limousine_commission',category='taxi_activity' if metric!='fare_amount' else 'local_transport_revenue',unit=unit,frequency='6-hourly',geography=f'United States / New York metropolitan area / {zone.Borough} / {zone.Zone}',source_url=page,source_urls=urls,
                  hypothesis='Neighborhood taxi activity measures business travel, tourism, airport connectivity and retail or nightlife footfall. Metered fare totals proxy local paid transport spending. Six-hour intervals preserve within-day business versus evening patterns and event disruptions.',
                  limitations='Research aggregation of official TLC yellow taxi trip submissions. Pickup timestamps are local New York wall-clock readings; six-hour local intervals are converted to UTC. DST boundary intervals can be five or seven actual hours. Destination-bound counts use the pickup period and destination zone, not arrival time. Fares are reported signed meter amounts before extras, tolls and tips; negative corrections and source outliers are retained. Zero-event zone intervals are omitted, not synthesized. Rows with pickup dates outside their nominal source month or unknown zones are excluded. Monthly releases typically lag about two months and may be revised. Aggregation exposes no individual trip records.',
                  raw_sha256=hashes,dataset_identity=f'NYC_TLC:yellow_tripdata:zone:{int(zone.LocationID)}:6h:{metric}',native_frequency='trip events',finer_frequency_available=True,available_frequency='Trip-level pickup timestamps in raw monthly source Parquets',documentation_url='https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf',expected_publication_lag='Typically two months; monthly trip record releases',timestamp_semantics='Start of six-hour New York local pickup period converted to UTC',aggregation='Count of reported trips or sum of reported metered fares; observed bins only',taxi_zone=int(zone.LocationID))
                if item:out.append(item)
            except Exception as exc:failure(f'tlc/{zone.LocationID}/{metric}',exc)
    return out

RTE_METRICS={'consommation':'Electricity consumption','thermique':'Thermal generation','nucleaire':'Nuclear generation','solaire':'Solar generation','hydraulique':'Hydropower generation','pompage':'Pumped-storage consumption','bioenergies':'Bioenergy generation','ech_physiques':'Net physical interchange','stockage_batterie':'Battery charging','destockage_batterie':'Battery discharging','eolien_terrestre':'Onshore wind generation','eolien_offshore':'Offshore wind generation'}
def clean_rte(dates,values):
    # The provider repeats a few UTC timestamps around DST changes, sometimes
    # with contradictory values. Omit all such ambiguous observations rather
    # than inventing a timestamp or arbitrarily choosing one revision.
    frame=pd.DataFrame({'date':pd.to_datetime(dates,utc=True),'value':pd.to_numeric(values,errors='raise')}).dropna()
    counts=frame.groupby('date').value.nunique();bad=counts[counts>1].index
    frame=frame[~frame.date.isin(bad)]
    return clean(frame.date,frame.value),len(bad)

def collect_rte(fetch):
    base='https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def'
    raw,mh=fetch.get(base);metadata=json.loads(raw)
    select='code_insee_region,libelle_region,date_heure,'+','.join(RTE_METRICS)
    params={'where':"date_heure >= '2024-01-01'",'select':select,'order_by':'date_heure','delimiter':';','use_labels':'false'}
    raw,h=fetch.get(base+'/exports/csv',params,timeout=240)
    data=pd.read_csv(BytesIO(raw),sep=';',dtype={'code_insee_region':str})
    # The export endpoint is unpaginated. Compare its record count with the same
    # query on /records so a truncated download cannot silently qualify.
    count_raw,ch=fetch.get(base+'/records',{'where':params['where'],'limit':0})
    expected=json.loads(count_raw)['total_count']
    if len(data)!=expected:raise ValueError(f'RTE export count mismatch: {len(data)} != {expected}')
    allowed=selection('rte_regions',data.code_insee_region.tolist());out=[]
    for code,x in data[data.code_insee_region.isin(allowed)].groupby('code_insee_region'):
        name=x.libelle_region.iloc[-1]
        for field,title in RTE_METRICS.items():
            try:
                # Published ND means unavailable rather than zero.
                values=x[field].replace({'ND':None,'N/D':None,'-':None})
                frame,conflicts=clean_rte(x.date_heure,values)
                if conflicts:omit(f'rte/{code}/{field}',f'Omitted {conflicts} conflicting source DST timestamps')
                item=save(f'rte_fr_region_{slug(code)}_{field}',frame,title=f'France {name}: {title}',provider='rte_france_odre',category='regional_electricity',unit='MW',frequency='30min',geography='France / '+name,source_url=base+'/exports/csv?'+urlencode(params),
                  hypothesis='Regional electricity demand and generation expose French industrial activity, local renewable supply, nuclear or hydro availability and regional import pressure, informing European power and fuel markets and utilities.',
                  limitations='RTE regional eCO2mix consolidated/final half-hourly series assembled from metering plus source estimation. UTC offsets supplied by the source are honored. A few source UTC timestamps conflict around DST changes; all values at such ambiguous timestamps are omitted instead of picking or retiming one. Missing source values omitted; no interpolation. Power is MW, not energy MWh. Consolidated readings typically arrive in the middle of the following month and final values in the following year; source gaps and revisions remain. Interchange is signed per provider convention, not gross bilateral trade. Capacity ratios and rate-converted copies are excluded.',raw_sha256=[mh,h,ch],dataset_identity=f'RTE:eco2mix-regional-cons-def:{code}:{field}',source_column=field,omitted_conflicting_timestamps=conflicts,documentation_url='https://odre.opendatasoft.com/explore/dataset/eco2mix-regional-cons-def/information/',expected_publication_lag='Consolidated data around mid-M+1; final values in Q2 of the following year',source_revision_policy='Consolidated values superseded by final metered estimates')
                if item:out.append(item)
            except Exception as exc:failure(f'rte/{code}/{field}',exc)
    return out

COLLECTORS={'rte':collect_rte,'tlc':collect_tlc,'ports':collect_ports,'eurocontrol':collect_eurocontrol,'eia':collect_eia,'cta':collect_cta}
def main():
    FAILURES.clear();OMISSIONS.clear();SEEN.clear()
    p=argparse.ArgumentParser();p.add_argument('--refresh',action='store_true');p.add_argument('--provider',choices=COLLECTORS,action='append');args=p.parse_args()
    baseline();out=[];fetch=Fetcher(refresh=args.refresh)
    for name in args.provider or COLLECTORS:
        try:out.extend(COLLECTORS[name](fetch))
        except Exception as exc:failure(name,exc)
    report={'series':len(out),'providers':args.provider or list(COLLECTORS),'failures':FAILURES,'omissions':OMISSIONS,'completed_at':pd.Timestamp.now(tz='UTC').isoformat()}
    atomic_bytes(FOLDER/('collection_report_'+'_'.join(args.provider or ['all'])+'.json'),json.dumps(report,indent=2).encode())
    print(json.dumps({k:v for k,v in report.items() if k!='omissions'},indent=2));print('OMISSIONS',len(OMISSIONS))
    return int(bool(FAILURES))
if __name__=='__main__':raise SystemExit(main())
