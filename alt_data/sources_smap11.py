#!/usr/bin/env python3
"""Measured SMAP L3 V009 regional soil moisture; authenticated public NASA data.

Requires h5py, listed in requirements.txt. Credentials are read
only from api_keys/earthdata.json and never written into URLs or receipts.
Run: python sources_smap11.py [--start 2024-01-01] [--end YYYY-MM-DD] [--audit]
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import hashlib
import io
import json
import math
from multiprocessing import get_context
from pathlib import Path
import re
import threading
import time
from urllib.parse import urlparse

import h5py
import numpy as np
import pandas as pd
import requests

from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes,
    coverage, normalize_frame, parquet_bytes, qualifies_research, save_candidate)

GROUP = 'smap11'
COLLECTION = 'C2938664585-NSIDC_CPRD'
CMR = 'https://cmr.earthdata.nasa.gov/search/granules.json'
GUIDE = 'https://nsidc.org/sites/default/files/documents/user-guide/spl3smp-v009-userguide.pdf'
LATENCY = 'https://nsidc.org/data/user-resources/help-center/what-are-latencies-smap-radiometer-data-sets'
DATA_URL = 'https://nsidc.org/data/spl3smp/versions/9'
REGIONS = [
    dict(id='mato_grosso', name='Mato Grosso box', country='BR', countries=['BR'], bbox=[-58,-17,-52,-10], exposure='soybeans and maize'),
    dict(id='parana', name='Parana box (includes Argentina fringe)', country='BR', countries=['AR','BR'], bbox=[-54,-26,-49,-22], exposure='soybeans and maize'),
    dict(id='buenos_aires_santa_fe', name='Buenos Aires–Santa Fe crop-belt box (includes Uruguay fringe)', country='AR', countries=['AR','UY'], bbox=[-64,-38,-58,-30], exposure='soybeans maize and wheat'),
    dict(id='punjab_haryana', name='Punjab–Haryana box (India and Pakistan)', country='IN', countries=['IN','PK'], bbox=[73,28,78,33], exposure='wheat and rice'),
    dict(id='madhya_pradesh', name='Madhya Pradesh box', country='IN', countries=['IN'], bbox=[74,21,82,26], exposure='soybeans and wheat'),
    dict(id='central_thailand', name='Central Thailand box (includes Laos fringe)', country='TH', countries=['LA','TH'], bbox=[99,14,102,18], exposure='rice'),
    dict(id='mekong_delta', name='Lower Mekong box (Vietnam and Cambodia)', country='VN', countries=['VN','KH'], bbox=[104,9,107,12], exposure='rice'),
    dict(id='sumatra', name='Sumatra and southern Malay Peninsula box', country='ID', countries=['ID','MY','SG'], bbox=[99,-5,105,2], exposure='palm oil; dense vegetation can preclude recommended retrievals'),
]
FIELDS = ['soil_moisture','soil_moisture_error','retrieval_qual_flag',
          'surface_flag','latitude','longitude','tb_time_seconds','tb_time_utc']
# Inclusive native EASE-grid indices. Derived from the original full-file
# latitude/longitude coordinates; checked again against those bytes in audit.
ROW_SLICE = (92,328)
COL_SLICE = (310,768)
MIN_PIXELS = 3
MIN_COVERAGE = 0.10
SAMPLE_TITLE = 'SMAP_L3_SM_P_20250101_R19240_001.h5'
SAMPLE_URL = 'https://data.nsidc.earthdatacloud.nasa.gov/nsidc-cumulus-prod-protected/SMAP/SPL3SMP/009/2025/01/' + SAMPLE_TITLE
SUPP = ROOT / 'supplementary' / GROUP
LOG = ROOT / 'logs'


def digest_bytes(b): return hashlib.sha256(b).hexdigest()
def source_hash(): return digest_bytes(Path(__file__).read_bytes())
def write_json(path, data): atomic_bytes(path, json.dumps(data, indent=2, allow_nan=False).encode())
def source_string(x): return x.decode() if isinstance(x, bytes) else str(x)


def geography_provenance():
    """Checked country intersections describe boxes; they never mask values."""
    source=LOG/'smap11_geography_review.json'
    if not source.exists():raise ValueError('Independent bounding-box geography report missing')
    review=json.loads(source.read_text())
    for region in REGIONS:
        row=next(x for x in review['regions'] if x['region_id']==region['id'])
        if row['bbox']!=region['bbox'] or sorted(row['country_polygon_bbox_intersections'])!=sorted(region['countries']):
            raise ValueError('Region countries disagree with checked box intersections')
    raw_hash=review['source']['raw_sha256']
    if digest_bytes((ROOT/'archive/raw'/(raw_hash+'.bin')).read_bytes())!=raw_hash:raise ValueError('Country boundary provenance hash mismatch')
    atomic_bytes(SUPP/'geography_review.json',source.read_bytes())
    return raw_hash


class PrivateFetch:
    """Archive only source bytes and unauthenticated URLs, with bounded retries."""
    def __init__(self, root=ROOT, refresh=False, token=None):
        self.root=Path(root); self.refresh=refresh; self.local=threading.local()
        self.token=token or json.loads(Path('/data/fin/api_keys/earthdata.json').read_text())['token']
        if not self.token: raise ValueError('Earthdata token is missing')

    def get(self,url,params=None):
        p=urlparse(url)
        if p.scheme!='https' or p.hostname not in {'opendap.earthdata.nasa.gov','data.nsidc.earthdatacloud.nasa.gov'}:
            raise ValueError('Unapproved NASA data host')
        public_url=requests.Request('GET',url,params=params).prepare().url
        if self.token in public_url: raise ValueError('Credential in public URL')
        key=digest_bytes(public_url.encode()); index=self.root/'archive/requests'/(key+'.json')
        if index.exists() and not self.refresh:
            m=json.loads(index.read_text()); path=self.root/'archive/raw'/(m['sha256']+'.bin')
            if path.exists():
                b=path.read_bytes()
                if digest_bytes(b)==m['sha256']: return b,m['sha256']
        if not hasattr(self.local,'session'):
            self.local.session=requests.Session()
            self.local.session.headers.update({'Authorization':'Bearer '+self.token,'User-Agent':'FinAltDataResearch/1.0'})
        for attempt in range(3):
            try:r=self.local.session.get(url,params=params,timeout=(15,90),allow_redirects=False)
            except requests.RequestException:
                if attempt<2:time.sleep(2**attempt);continue
                raise RuntimeError('NASA network request failed; private transport omitted') from None
            if r.status_code in {302,303,307,308}:
                # Protected NSIDC downloads return a short-lived CloudFront URL.
                # Never archive that signed URL or forward the bearer header.
                redirect=r.headers.get('Location','');target=urlparse(redirect)
                if p.hostname!='data.nsidc.earthdatacloud.nasa.gov' or target.scheme!='https' or not (target.hostname or '').endswith('.cloudfront.net'):
                    raise RuntimeError('Unexpected NASA authentication redirect')
                try:r=requests.get(redirect,timeout=(15,90),allow_redirects=False)
                except requests.RequestException:
                    raise RuntimeError('NASA distribution request failed; signed transport omitted') from None
            if r.status_code in {429,500,502,503,504} and attempt<2:
                retry=r.headers.get('Retry-After','');delay=float(retry) if retry.isdigit() else 2**(attempt+1)
                if delay>60:raise RuntimeError('NASA requested a longer retry delay')
                time.sleep(delay);continue
            if r.status_code!=200:raise RuntimeError('NASA data HTTP '+str(r.status_code))
            b=r.content
            if self.token.encode() in b: raise RuntimeError('Response reflected a credential; not archived')
            if b[:8]!=b'\x89HDF\r\n\x1a\n':raise ValueError('NASA response is not HDF5/NetCDF4')
            h=digest_bytes(b); now=pd.Timestamp.now(tz='UTC')
            m={'url':public_url,'sha256':h,'retrieved_at':now.isoformat(),'content_type':r.headers.get('Content-Type'),
               'http_status':200,'collector_group':GROUP,'authentication_redacted':True}
            atomic_bytes(self.root/'archive/raw'/(h+'.bin'),b)
            write_json(self.root/'archive/receipts'/(key+'_'+now.strftime('%Y%m%dT%H%M%S%fZ')+'.json'),m)
            write_json(index,m)
            return b,h
        raise RuntimeError('NASA retries exhausted')

    def cached(self,url,params=None):
        public_url=requests.Request('GET',url,params=params).prepare().url
        index=self.root/'archive/requests'/(digest_bytes(public_url.encode())+'.json')
        if not index.exists():return False
        m=json.loads(index.read_text())
        return (self.root/'archive/raw'/(m['sha256']+'.bin')).exists()


def decode_utc(array):
    """Native HDF5 S24 and OPeNDAP NetCDF S1 character arrays, no epoch guess."""
    a=np.asarray(array)
    if a.dtype.kind!='S':raise ValueError('Expected source ASCII UTC array')
    if a.ndim==3 and a.dtype.itemsize==1:
        a=np.ascontiguousarray(a).view('S'+str(a.shape[-1])).reshape(a.shape[:-1])
    if a.ndim!=2:raise ValueError('Unexpected UTC time dimensions')
    return np.char.strip(np.char.rstrip(a.astype('U'), '\x00'))


def valid_retrievals(values,flags,time_strings):
    # The guide explicitly recommends flags 0 and 8. Do not use the stale
    # nominal HDF5 valid_max=0.5: the guide allows moisture up to soil porosity,
    # and genuine recommended retrievals in the original sample exceed 0.5.
    has_time=np.char.startswith(time_strings,'20') & np.char.endswith(time_strings,'Z') & (np.char.find(time_strings,'*')<0)
    return np.isfinite(values)&(values>=0)&(values<=1)&np.isin(flags,[0,8])&has_time


def spatial_mask(lat,lon,bbox):
    west,south,east,north=bbox
    return (lat>=south)&(lat<=north)&(lon>=west)&(lon<=east)


def static_grid(raw, sliced=True):
    """Recover the fixed equal-area lattice from native center coordinates.

    Daily lat/lon arrays are missing outside that day's observed swaths. The
    full validation granule nevertheless observes at least one cell of every
    row and column. Exact row/column constancy and AM/PM agreement are required.
    """
    grids=[]
    with h5py.File(io.BytesIO(raw)) as f:
        for pas in ['AM','PM']:
            g=f['Soil_Moisture_Retrieval_Data_'+pas];suffix='_pm' if pas=='PM' else ''
            lat=g['latitude'+suffix][:];lon=g['longitude'+suffix][:]
            if lat.shape!=(406,964) or lon.shape!=lat.shape:raise ValueError('Unexpected native EASE grid dimensions')
            rows=[];columns=[]
            for values in lat:
                unique=np.unique(values[np.isfinite(values)&(values>=-90)&(values<=90)])
                if len(unique)!=1:raise ValueError('Cannot establish fixed latitude row')
                rows.append(unique[0])
            for values in lon.T:
                unique=np.unique(values[np.isfinite(values)&(values>=-180)&(values<=180)])
                if len(unique)!=1:raise ValueError('Cannot establish fixed longitude column')
                columns.append(unique[0])
            grids.append((np.broadcast_to(np.asarray(rows)[:,None],lat.shape),np.broadcast_to(np.asarray(columns)[None,:],lat.shape)))
    if not all(np.array_equal(a,b) for a,b in zip(grids[0],grids[1])):raise ValueError('AM/PM fixed grids disagree')
    lat,lon=grids[0]
    if sliced:return lat[ROW_SLICE[0]:ROW_SLICE[1]+1,COL_SLICE[0]:COL_SLICE[1]+1],lon[ROW_SLICE[0]:ROW_SLICE[1]+1,COL_SLICE[0]:COL_SLICE[1]+1]
    return lat,lon


def flag_hist(values):
    keys,counts=np.unique(values,return_counts=True)
    return {str(int(k)):int(n) for k,n in zip(keys,counts)}


def parse_subset(raw,granule,grid,regions=REGIONS):
    rows=[]
    with h5py.File(io.BytesIO(raw),'r') as f:
        for pas in ['AM','PM']:
            g=f['Soil_Moisture_Retrieval_Data_'+pas];suffix='_pm' if pas=='PM' else ''
            # Both transports contain original unaveraged native arrays.
            # Read only the target rectangle from original global HDF5 files.
            native_global=g['soil_moisture'+suffix].shape==(406,964)
            selection=(slice(ROW_SLICE[0],ROW_SLICE[1]+1),slice(COL_SLICE[0],COL_SLICE[1]+1)) if native_global else (...,)
            d={name:g[name+suffix][selection] for name in FIELDS}
            d['tb_time_utc']=decode_utc(d['tb_time_utc'])
            if len({a.shape for a in d.values()})!=1:raise ValueError('Source array dimensions disagree')
            if d['latitude'].shape!=grid[0].shape:raise ValueError('Subset and fixed grid shapes disagree')
            for coordinate,bounds,expected in [('latitude',(-90,90),grid[0]),('longitude',(-180,180),grid[1])]:
                present=np.isfinite(d[coordinate])&(d[coordinate]>=bounds[0])&(d[coordinate]<=bounds[1])
                if not np.array_equal(d[coordinate][present],expected[present]):raise ValueError('Source geolocation differs from fixed EASE grid')
            unit=source_string(g['soil_moisture'+suffix].attrs['units'])
            if unit not in {'cm**3/cm**3','m3/m3'}:raise ValueError('Unexpected soil moisture units')
            valid=valid_retrievals(d['soil_moisture'],d['retrieval_qual_flag'],d['tb_time_utc'])
            time_ns=np.full(valid.shape,np.iinfo('int64').min,dtype='int64')
            parsed=pd.to_datetime(d['tb_time_utc'][valid],utc=True,format='ISO8601',errors='coerce')
            time_ns[valid]=parsed.asi8
            valid &= time_ns!=np.iinfo('int64').min
            numeric_recommended=np.isfinite(d['soil_moisture'])&(d['soil_moisture']>=0)&(d['soil_moisture']<=1)&np.isin(d['retrieval_qual_flag'],[0,8])
            for region in regions:
                inside=spatial_mask(grid[0],grid[1],region['bbox']); n=int(inside.sum())
                if not n:raise ValueError('Subset misses requested region')
                good=inside&valid;count=int(good.sum());fraction=count/n
                record={'region_id':region['id'],'pass':pas,'product_date':pd.Timestamp(granule['time_start']).normalize().isoformat(),
                    'granule_id':granule['id'],'granule_title':granule['title'],
                    'region_grid_cells':n,'valid_pixels':count,'valid_fraction':fraction,
                    'quality_recommended_pixels':int((inside&np.isin(d['retrieval_qual_flag'],[0,8])).sum()),
                    'malformed_or_missing_utc_pixels':int((inside&numeric_recommended&~valid).sum()),
                    'retrieval_quality_flag_counts':json.dumps(flag_hist(d['retrieval_qual_flag'][inside]),sort_keys=True),
                    'surface_flag_counts':json.dumps(flag_hist(d['surface_flag'][inside]),sort_keys=True),
                    'quality_rule':'retrieval_qual_flag 0 or 8; numeric soil moisture in [0,1]; native UTC available',
                    'eligible_region_observation':count>=MIN_PIXELS and fraction>=MIN_COVERAGE,
                    'value':None,'observation_time_min':None,'observation_time_median':None,'observation_time_max':None,
                    'mean_source_retrieval_error':None,'retrieval_error_pixels':0,'above_nominal_attribute_max_pixels':0}
                if count:
                    v=d['soil_moisture'][good].astype('float64'); times=pd.to_datetime(time_ns[good],utc=True)
                    day=pd.Timestamp(granule['time_start']).normalize()
                    if (times<day-pd.Timedelta(days=1)).any() or (times>=day+pd.Timedelta(days=2)).any():
                        raise ValueError('Acquisition UTC outside daily granule vicinity')
                    errors=d['soil_moisture_error'][good].astype('float64');ok=np.isfinite(errors)&(errors>=0)&(errors<=1)
                    record.update(value=float(v.mean()),observation_time_min=times.min().isoformat(),
                        observation_time_median=pd.Timestamp(int(np.median(times.asi8)),tz='UTC').isoformat(),
                        observation_time_max=times.max().isoformat(),
                        mean_source_retrieval_error=float(errors[ok].mean()) if ok.any() else None,
                        retrieval_error_pixels=int(ok.sum()),above_nominal_attribute_max_pixels=int((v>0.5).sum()))
                rows.append(record)
    return rows


def subset_url(granule):
    urls=[x['href'] for x in granule['links'] if urlparse(x.get('href','')).hostname=='opendap.earthdata.nasa.gov' and not x.get('inherited')]
    if len(urls)!=1:raise ValueError('No unique source-listed OPeNDAP URL')
    expressions=[]
    for pas in ['AM','PM']:
        for field in FIELDS:
            expressions.append('/Soil_Moisture_Retrieval_Data_'+pas+'/'+field+('_pm' if pas=='PM' else '')+
                f'[{ROW_SLICE[0]}:{ROW_SLICE[1]}][{COL_SLICE[0]}:{COL_SLICE[1]}]')
    return urls[0]+'.dap.nc4',{'dap4.ce':';'.join(expressions)}


def search_granules(fetch,start,end):
    entries=[];hashes=[]
    for page in range(1,9):
        raw,h=fetch.get(CMR,{'collection_concept_id':COLLECTION,'temporal':start+','+end,
            'page_size':2000,'page_num':page,'sort_key[]':'start_date'},max_age_hours=0)
        hashes.append(h); part=json.loads(raw)['feed']['entry']; entries.extend(part)
        if len(part)<2000:break
    else:raise ValueError('CMR bounded page limit exceeded')
    if len({x['id'] for x in entries})!=len(entries):raise ValueError('CMR duplicate granule IDs')
    # Keep one latest CMR revision per daily product; do not average overlapping versions.
    selected={}
    for item in entries:
        if not pd.Timestamp(start)<=pd.Timestamp(item['time_start'])<pd.Timestamp(end):continue
        key=item['time_start']
        if key not in selected or (item['updated'],item['id'])>(selected[key]['updated'],selected[key]['id']):selected[key]=item
    return sorted(selected.values(),key=lambda x:x['time_start']),hashes


def save_series(records,manifest,docs):
    allrows=pd.DataFrame(records);allrows['product_date']=pd.to_datetime(allrows['product_date'],utc=True)
    SUPP.mkdir(parents=True,exist_ok=True)
    quality=SUPP/'regional_observation_quality.parquet'
    atomic_bytes(quality,allrows.to_parquet(index=False,compression='zstd'))
    files=[];omissions=[];supp=[]
    for region in REGIONS:
        for pas in ['AM','PM']:
            subset=allrows[(allrows.region_id==region['id'])&(allrows['pass']==pas)]
            good=subset[subset.eligible_region_observation]
            sid='smap11_'+region['country'].lower()+'_'+region['id']+'_'+pas.lower()+'_soil_moisture'
            if good.empty:
                omissions.append({'series_id':sid,'reason':'No region observations meet recommended quality and spatial coverage'});continue
            frame=normalize_frame(pd.DataFrame({'date':good.product_date,'value':good.value}))
            meta=dict(title=f'SMAP measured surface soil moisture: {region["name"]} ({pas})',
                provider='NASA NSIDC SMAP',category='measured_crop_region_soil_moisture',unit='m3/m3',frequency='daily',
                source_url=DATA_URL,geography=region['name'],
                hypothesis=f'Measured topsoil wetness in a region exposed to {region["exposure"]} may signal crop stress, harvest interruption or planting conditions. Test against crop calendars and actual information availability.',
                limitations='Radiometer retrieval, not modeled rootzone moisture. Rectangular region mean of observed recommended-quality equal-area cells; not a crop mask or crop-weighted estimate. Changing spatial samples and sparse coverage can bias changes. At least 3 valid cells and 10% of all box grid cells required. AM/PM separate; no interpolation. Latest retrospective V009 files, not historical as-issued vintages. Daily product label is not acquisition/publication time; exact source UTC acquisition ranges are in companion.',
                raw_sha256=[manifest],raw_manifest_sha256=[manifest],batch=11,collector_group=GROUP,
                country_codes=region['countries'],geography_scope='non_us',region_bbox=region['bbox'],
                geography_boundary_method='Rectangular grid-center selection, not administrative borders. Country codes include intersected border/peninsula fringes, not weighted national contributions.',
                geography_companion='supplementary/smap11/geography_review.json',
                dataset_identity=f'SMAP:SPL3SMP:009:{region["id"]}:{pas}:recommended_DCA_mean',
                native_frequency='daily product with AM and PM local-solar overpasses; cell revisits typically every 2–3 days',
                source_native_frequency='Daily L3 composites; no guarantee each region/cell is observed daily',
                timestamp_semantics='UTC midnight label of source daily L3 granule. Native per-pixel acquisition UTC is preserved in raw subset and region min/median/max in quality companion. This date is not publication time.',
                acquisition_time_source='Native valid tb_time_utc ASCII strings; malformed timestamps (including literal *** fractional seconds) omitted at pixel level and counted. No conversion from ambiguous tb_time_seconds epoch.',
                expected_publication_lag='Provider normal-operation mean acquisition-to-generation latency 1 day 05:33:38; requirement 2 days 02:00:00. Not a guarantee of historical first availability; revisions may be later.',
                publication_lag_days_estimate=None,
                publication_lag_basis='Unknown from the daily product-midnight label; provider latency is measured from native acquisition time. See '+LATENCY,
                publication_lag_kind='historical_first_publication_unknown',
                source_generation_latency_days_mean=1+(5*3600+33*60+38)/86400,
                source_generation_latency_days_requirement=2+2/24,
                source_generation_latency_origin='Native data acquisition time, not daily product label or first historical release',
                historical_first_availability_known=False,point_in_time=False,
                soil_depth='approximately top 5 cm',spatial_resolution='36 km EASE-Grid 2.0 equal-area',
                source_algorithm='Dual Channel Algorithm (DCA), V009 baseline',
                quality_filter='retrieval_qual_flag in {0,8}; source value in [0,1]; source UTC present; region>=3 pixels and>=10% all-box cell coverage',
                nominal_valid_max_caveat='HDF5 attribute 0.5 is not enforced; guide allows up to soil porosity and source recommended-quality values can exceed 0.5. Physical [0,1] bound used.',
                source_unit='cm**3/cm**3; ratio numerically identical to m3/m3',
                quality_companion=str(quality.relative_to(ROOT)),granule_companion='supplementary/smap11/granules.json',
                valid_fraction_min=float(good.valid_fraction.min()),valid_fraction_median=float(good.valid_fraction.median()),
                malformed_utc_pixels_excluded=int(subset.malformed_or_missing_utc_pixels.sum()),
                recommended_quality_pixels_total=int(subset.quality_recommended_pixels.sum()),
                malformed_utc_exclusion_fraction=float(subset.malformed_or_missing_utc_pixels.sum()/max(1,subset.quality_recommended_pixels.sum())),
                excluded_product_days=int((~subset.eligible_region_observation).sum()),
                source_attribution="O'Neill et al. (2023), SMAP L3 Radiometer Global Daily 36 km EASE-Grid Soil Moisture Version 9, NASA NSIDC DAAC, doi:10.5067/4XXOGX0OOW1S. Spatial subset and quality rules documented here.",
                license='NASA Earth science open data; cite dataset and subset per NSIDC citation policy',documentation_sha256=docs)
            cv=coverage(frame)
            if qualifies_research(dict(cv,frequency='daily')):
                m=save_candidate(GROUP,sid,frame,**meta);path=ROOT/'archive/staging'/GROUP/(sid+'.parquet')
                files.append({'series_id':sid,'path':str(path.relative_to(ROOT)),'parquet_sha256':digest_bytes(path.read_bytes()),
                    'metadata_sha256':digest_bytes(path.with_suffix('.json').read_bytes()),'rows':len(frame)})
            else:
                m=dict(meta,series_id=sid,**cv,research_eligible=False,daily_eligible=False,status='sparse_observed_companion')
                path=SUPP/(sid+'.parquet');atomic_bytes(path,parquet_bytes(frame,m));write_json(path.with_suffix('.json'),m)
                supp.append({'path':str(path.relative_to(ROOT)),'sha256':digest_bytes(path.read_bytes())})
                omissions.append({'series_id':sid,'reason':'Observed region history is too sparse for research eligibility','coverage':cv})
    return files,omissions,supp


def run(start,end,workers=4,refresh=False):
    public=Fetcher();private=PrivateFetch(refresh=refresh);docs=[]
    for url in [GUIDE,LATENCY,DATA_URL]:
        _,h=public.get(url,max_age_hours=24*365);docs.append(h)
    docs.append(geography_provenance())
    entries,cmr_hashes=search_granules(public,start,end)
    print(json.dumps({'event':'discovered','granules':len(entries),'start':start,'end_exclusive':end}),flush=True)
    # Original full granule is retained as a cross-check of server-side extraction.
    sample_raw,sample_hash=private.get(SAMPLE_URL);grid=static_grid(sample_raw)
    sample_entry={'id':'G3356712686-NSIDC_CPRD','title':SAMPLE_TITLE,'time_start':'2025-01-01T00:00:00Z',
        'links':[{'href':'https://opendap.earthdata.nasa.gov/collections/'+COLLECTION+'/granules/'+SAMPLE_TITLE}]}
    sample_subset_url,sample_subset_params=subset_url(sample_entry)
    _,sample_subset_hash=private.get(sample_subset_url,sample_subset_params)
    records=[];granules=[];failures=[];nbytes=0
    def acquire(entry):
        subset,params=subset_url(entry)
        # Reuse already acquired efficient subsets. Original global files are
        # substantially faster than server-side transforms on uncached days.
        if private.cached(subset,params) and not refresh:
            url=subset;raw,h=private.get(url,params);source_format='opendap_netcdf4_native_slice'
        else:
            urls=[x['href'] for x in entry['links'] if not x.get('inherited') and urlparse(x.get('href','')).hostname=='data.nsidc.earthdatacloud.nasa.gov' and x['href'].endswith('.h5')]
            if len(urls)!=1:raise ValueError('No unique source-listed original HDF5 URL')
            url=urls[0];raw,h=private.get(url);source_format='original_global_hdf5'
        rows=parse_subset(raw,entry,grid)
        return rows,dict(granule_id=entry['id'],title=entry['title'],time_start=entry['time_start'],time_end=entry['time_end'],
            cmr_updated=entry['updated'],cmr_update_semantics='metadata revision time, not first publication',
            raw_sha256=h,bytes=len(raw),source_url=url,source_format=source_format,
            subset_parameters=params if source_format=='opendap_netcdf4_native_slice' else None),len(raw)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks={pool.submit(acquire,e):e for e in entries}
        for count,task in enumerate(as_completed(tasks),1):
            entry=tasks[task]
            try:
                rr,gg,nb=task.result();records.extend(rr);granules.append(gg);nbytes+=nb
            except Exception as exc:
                # Messages from our parsers/transports never contain private URLs or credentials.
                failures.append({'granule_id':entry['id'],'title':entry['title'],'error_type':type(exc).__name__,'error':str(exc)[:180]})
            if count%25==0 or count==len(entries):
                state={'event':'progress','complete':count,'total':len(entries),'failed':len(failures),'bytes':nbytes}
                print(json.dumps(state),flush=True);write_json(LOG/'smap11_progress.json',state)
    granules.sort(key=lambda x:x['time_start']);records.sort(key=lambda x:(x['product_date'],x['region_id'],x['pass']))
    selected=dict(collection=COLLECTION,version='009',regions=REGIONS,row_slice_inclusive=ROW_SLICE,col_slice_inclusive=COL_SLICE,
        source_fields=FIELDS,cmr_raw_sha256=cmr_hashes,documentation_sha256=docs,original_sample_sha256=sample_hash,
        original_sample_subset_sha256=sample_subset_hash,
        raw_granules=granules,failures=failures,requested_start=start,requested_end_exclusive=end,
        retrieval_timestamp=pd.Timestamp.now(tz='UTC').isoformat(),selection_policy='Latest CMR metadata revision per product date; no source gaps filled',
        extraction='Cached NASA OPeNDAP native variable/array subsets plus original global HDF5 files; identical native grid-cell selection. No reprojection, interpolation, or averaging at provider. Format recorded per granule.')
    selected['static_grid_basis']='Native full validation granule row/column center coordinates; each row and column constant where observed; AM/PM identical; all later nonfill coordinates checked against this fixed lattice. Daily swath coordinate availability is not the coverage denominator.'
    write_json(SUPP/'granules.json',selected)
    if not records:raise RuntimeError('No usable SMAP granules')
    manifest=archive_raw_manifest([g['raw_sha256'] for g in granules]+cmr_hashes+docs+[sample_hash,sample_subset_hash],description='SMAP11 original provider OPeNDAP subsets, original full sample, CMR search and official documentation')
    files,omissions,supp=save_series(records,manifest,docs)
    supplementary=[{'path':str(p.relative_to(ROOT)),'sha256':digest_bytes(p.read_bytes())} for p in sorted(SUPP.glob('*')) if p.is_file()]
    inventory=dict(group=GROUP,status='downloaded_pending_audit',source_sha256=source_hash(),files=files,supplementary_files=supplementary,
        errors=failures,omissions=omissions,series_count=len(files),observation_count=sum(f['rows'] for f in files),
        granule_count=len(granules),raw_granule_bytes=nbytes,source_format_counts=dict(Counter(g['source_format'] for g in granules)),requested_start=start,requested_end_exclusive=end,
        frequency_counts={'daily':len(files)},country_codes=sorted({c for r in REGIONS for c in r['countries']}),
        refresh_command='python3 alt_data/sources_smap11.py --refresh')
    write_json(LOG/'smap11_inventory.json',inventory)
    audit()


def rebuild():
    """Regenerate final metadata/inventory from collected native observations."""
    selected=json.loads((SUPP/'granules.json').read_text())
    old=json.loads((LOG/'smap11_inventory.json').read_text())
    geographic_hash=geography_provenance()
    selected['regions']=REGIONS
    selected['documentation_sha256']=sorted(set(selected['documentation_sha256']+[geographic_hash]))
    write_json(SUPP/'granules.json',selected)
    quality=pd.read_parquet(SUPP/'regional_observation_quality.parquet')
    hashes=[x['raw_sha256'] for x in selected['raw_granules']]+selected['cmr_raw_sha256']+selected['documentation_sha256']+[selected['original_sample_sha256'],selected['original_sample_subset_sha256']]
    manifest=archive_raw_manifest(hashes,description='SMAP11 original provider OPeNDAP subsets, original full sample, CMR search and official documentation')
    files,omissions,_=save_series(quality.to_dict('records'),manifest,selected['documentation_sha256'])
    old.update(source_sha256=source_hash(),files=files,omissions=omissions,series_count=len(files),observation_count=sum(x['rows'] for x in files),
        country_codes=sorted({c for r in REGIONS for c in r['countries']}),frequency_counts={'daily':len(files)},
        supplementary_files=[{'path':str(p.relative_to(ROOT)),'sha256':digest_bytes(p.read_bytes())} for p in sorted(SUPP.glob('*')) if p.is_file()])
    write_json(LOG/'smap11_inventory.json',old)
    audit()


_AUDIT_GRID=None


def _initialize_audit(grid):
    global _AUDIT_GRID
    _AUDIT_GRID=grid


def _replay_granule(g):
    try:
        raw=(ROOT/'archive/raw'/(g['raw_sha256']+'.bin')).read_bytes()
        if digest_bytes(raw)!=g['raw_sha256']:return [],'Raw hash mismatch '+g['granule_id']
        entry={'id':g['granule_id'],'title':g['title'],'time_start':g['time_start']}
        return parse_subset(raw,entry,_AUDIT_GRID),None
    except Exception as exc:
        return [],'Replay failed '+g['granule_id']+' '+type(exc).__name__


def audit():
    """Replay all region observations from archived arrays and verify original bytes."""
    inventory=json.loads((LOG/'smap11_inventory.json').read_text());selection=json.loads((SUPP/'granules.json').read_text())
    errors=[];checks=[];rows=[]
    original_raw=(ROOT/'archive/raw'/(selection['original_sample_sha256']+'.bin')).read_bytes()
    if digest_bytes(original_raw)!=selection['original_sample_sha256']:raise ValueError('Original sample hash mismatch')
    grid=static_grid(original_raw)
    # Independent processes avoid HDF5's interpreter-wide serialization while
    # replaying original bytes. No network or credential access occurs here.
    with ProcessPoolExecutor(max_workers=4,mp_context=get_context('spawn'),initializer=_initialize_audit,initargs=(grid,)) as pool:
        for replay_rows,error in pool.map(_replay_granule,selection['raw_granules'],chunksize=8):
            rows.extend(replay_rows)
            if error:errors.append(error)
    replay=pd.DataFrame(rows)
    if not replay.empty:
        replay['product_date']=pd.to_datetime(replay.product_date,utc=True)
        saved_quality=pd.read_parquet(SUPP/'regional_observation_quality.parquet')
        columns=list(saved_quality.columns);sort=['product_date','region_id','pass']
        try:pd.testing.assert_frame_equal(replay[columns].sort_values(sort).reset_index(drop=True),saved_quality.sort_values(sort).reset_index(drop=True),check_dtype=False)
        except AssertionError:errors.append('Quality companion does not replay')
        for item in inventory['files']:
            path=ROOT/item['path'];frame=pd.read_parquet(path);meta=json.loads(path.with_suffix('.json').read_text())
            region=next(r for r in REGIONS if meta['region_bbox']==r['bbox']);pas='AM' if '_am_' in item['series_id'] else 'PM'
            r=replay[(replay.region_id==region['id'])&(replay['pass']==pas)&replay.eligible_region_observation]
            expected=normalize_frame(pd.DataFrame({'date':r.product_date,'value':r.value}))
            try:pd.testing.assert_frame_equal(frame,expected)
            except AssertionError:errors.append('Canonical values do not replay '+item['series_id'])
            h=digest_bytes(path.read_bytes())
            if h!=item['parquet_sha256']:errors.append('Canonical hash changed '+item['series_id'])
            if digest_bytes(path.with_suffix('.json').read_bytes())!=item['metadata_sha256']:errors.append('Metadata hash changed '+item['series_id'])
            checks.append({'series_id':item['series_id'],'parquet_sha256':h,'rows':len(frame),'raw_replayed':True})
    sample_hash=selection.get('original_sample_subset_sha256')
    comparison=[]
    if sample_hash:
        with h5py.File(ROOT/'archive/raw'/(selection['original_sample_sha256']+'.bin')) as full, h5py.File(ROOT/'archive/raw'/(sample_hash+'.bin')) as subset:
            for pas in ['AM','PM']:
                gn='Soil_Moisture_Retrieval_Data_'+pas;suffix='_pm' if pas=='PM' else ''
                for field in FIELDS:
                    name=field+suffix;a=full[gn][name][ROW_SLICE[0]:ROW_SLICE[1]+1,COL_SLICE[0]:COL_SLICE[1]+1];b=subset[gn][name][:]
                    if field=='tb_time_utc':a=decode_utc(a);b=decode_utc(b)
                    equal=bool(np.array_equal(a,b));comparison.append({'pass':pas,'field':field,'equal':equal})
                    if not equal:errors.append('Original/subset mismatch '+pas+' '+field)
                lat,lon=static_grid(original_raw,sliced=False)
                for region in REGIONS:
                    rr,cc=np.where(spatial_mask(lat,lon,region['bbox']))
                    if len(rr) and (rr.min()<ROW_SLICE[0] or rr.max()>ROW_SLICE[1] or cc.min()<COL_SLICE[0] or cc.max()>COL_SLICE[1]):errors.append('Subset clipping region '+region['id'])
    else:errors.append('Independent original/subset sample pair is missing')
    for p in inventory['supplementary_files']:
        if digest_bytes((ROOT/p['path']).read_bytes())!=p['sha256']:errors.append('Supplementary hash changed '+p['path'])
    result=dict(source_sha256=source_hash(),series_checks=checks,errors=errors,source_granules_replayed=len(selection['raw_granules']),
        original_vs_subset_fields=comparison,checked_at=pd.Timestamp.now(tz='UTC').isoformat())
    write_json(LOG/'smap11_source_audit.json',result)
    inventory['source_sha256']=source_hash();inventory['status']='verified' if not errors else 'audit_failed'
    write_json(LOG/'smap11_inventory.json',inventory)
    print(json.dumps({'event':'audit','series':len(checks),'granules':len(selection['raw_granules']),'errors':errors}),flush=True)
    if errors:raise RuntimeError('SMAP source audit failed')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--start',default='2024-01-01');p.add_argument('--end',default=pd.Timestamp.now(tz='UTC').date().isoformat())
    p.add_argument('--workers',type=int,default=4);p.add_argument('--refresh',action='store_true');p.add_argument('--audit',action='store_true');p.add_argument('--rebuild',action='store_true');args=p.parse_args()
    if args.rebuild:rebuild()
    elif args.audit:audit()
    else:
        start=pd.Timestamp(args.start,tz='UTC');end=pd.Timestamp(args.end,tz='UTC')
        if not start<end:raise ValueError('Start must precede end')
        if not 1<=args.workers<=6:raise ValueError('Use 1–6 concurrent requests')
        run(start.isoformat(),end.isoformat(),args.workers,args.refresh)
