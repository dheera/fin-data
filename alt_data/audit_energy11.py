#!/usr/bin/env python3
"""Independent raw-source spot audit for energy11; does not call its parsers."""
import hashlib
import io
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from expansion_common import ROOT, atomic_bytes, coverage, qualifies_research


def root_xml(raw):
    t=ET.fromstring(raw)
    for e in t.iter():e.tag=e.tag.split('}')[-1]
    return t


def audit():
    inv=json.loads((ROOT/'logs/energy11_inventory.json').read_text())
    if inv['status']!='complete':raise RuntimeError('Collection must complete before final audit')
    checks=defaultdict(list);found=defaultdict(set);raw_verified=set();file_checks=[]
    observations=0
    for item in inv['files']:
        path=ROOT/item['path'];rawparquet=path.read_bytes();rawmeta=path.with_suffix('.json').read_bytes()
        assert hashlib.sha256(rawparquet).hexdigest()==item['parquet_sha256'],item['series_id']
        assert hashlib.sha256(rawmeta).hexdigest()==item['metadata_sha256'],item['series_id']
        meta=json.loads(rawmeta);frame=pd.read_parquet(path)
        assert frame.columns.tolist()==['date','value']
        assert str(frame.date.dtype)=='datetime64[ns, UTC]'
        assert frame.date.is_monotonic_increasing and frame.date.is_unique and np.isfinite(frame.value).all()
        assert (frame.date<pd.Timestamp(inv['requested_end_exclusive'])).all()
        assert (frame.date>=pd.Timestamp(inv['requested_start'])).all()
        assert qualifies_research({'frequency':meta['frequency'],**coverage(frame)})
        footer=json.loads(pq.read_metadata(path).metadata[b'alt_data'])
        assert meta==footer
        observations+=len(frame)
        samples=frame.iloc[np.unique(np.linspace(0,len(frame)-1,7,dtype=int))]
        documents={}
        if meta.get('document_companion'):
            documents={x['raw_sha256']:x for x in json.loads((ROOT/meta['document_companion']).read_text())}
        for row in samples.itertuples(index=False):
            check={'series_id':meta['series_id'],'date':row.date,'value':row.value,'dimensions':meta.get('source_dimensions'),
                   'field':meta.get('source_field'),'identity':meta['dataset_identity']}
            for digest in meta['raw_sha256']:
                if documents and digest not in documents:continue
                if documents:
                    q=documents[digest]['parameters']
                    a=pd.to_datetime(q['periodStart'],format='%Y%m%d%H%M',utc=True)
                    b=pd.to_datetime(q['periodEnd'],format='%Y%m%d%H%M',utc=True)
                    if not a<=row.date<b:continue
                checks[digest].append(check)
        file_checks.append({'series_id':meta['series_id'],'rows':len(frame),'source_samples':len(samples)})
    for digest,group in checks.items():
        raw=(ROOT/'archive/raw'/(digest+'.bin')).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==digest
        raw_verified.add(digest)
        if raw.lstrip().startswith(b'{'):
            data=json.loads(raw)['data']
            for check in group:
                for r in data:
                    if r.get('status')=='N' or r['gasDayStart']!=check['date'].date().isoformat():continue
                    v=r
                    for field in check['field'].split('.'):
                        v=v.get(field) if isinstance(v,dict) else None
                    if v in (None,'','-','--','NA','N/A','null'):continue
                    v=float(v)
                    if np.isfinite(v):found[(check['series_id'],check['date'].isoformat())].add(v)
        else:
            if raw.startswith(b'PK'):
                with zipfile.ZipFile(io.BytesIO(raw)) as z:docs=[root_xml(z.read(n)) for n in z.namelist() if n.lower().endswith('.xml')]
            else:docs=[root_xml(raw)]
            by_dimensions=defaultdict(list)
            for check in group:by_dimensions[json.dumps(check['dimensions'],sort_keys=True)].append(check)
            for doc in docs:
                for ts in doc.findall('TimeSeries'):
                    d={'psr':ts.findtext('MktPSRType/psrType') or '',
                       'direction':'consumption' if ts.findtext('outBiddingZone_Domain.mRID') and ts.findtext('MktPSRType/psrType') else 'production',
                       'unit':ts.findtext('quantity_Measure_Unit.name') or ts.findtext('price_Measure_Unit.name') or '',
                       'currency':ts.findtext('currency_Unit.name') or '', 'business_type':ts.findtext('businessType') or ''}
                    for field,tag in [('auction_type','auction.type'),('contract_type','contract_MarketAgreement.type'),
                                      ('classification_sequence','classificationSequence_AttributeInstanceComponent.position')]:
                        value=ts.findtext(tag)
                        if value is not None:d[field]=value
                    selected=by_dimensions.get(json.dumps(d,sort_keys=True),[])
                    if not selected:continue
                    curve=ts.findtext('curveType') or 'A01'
                    for period in ts.findall('Period'):
                        start=pd.Timestamp(period.findtext('timeInterval/start'));end=pd.Timestamp(period.findtext('timeInterval/end'))
                        resolution=pd.Timedelta(period.findtext('resolution'))
                        wanted=[c for c in selected if start<=c['date']<end]
                        if not wanted:continue
                        points={int(x.findtext('position')):x for x in period.findall('Point')}
                        for check in wanted:
                            position=int((check['date']-start)/resolution)+1
                            if curve=='A03':
                                prior=[p for p in points if p<=position]
                                if not prior:continue
                                position=max(prior)
                            if position not in points:continue
                            p=points[position];value=p.findtext('quantity')
                            if value is None:value=p.findtext('price.amount')
                            if value is not None and np.isfinite(float(value)):
                                found[(check['series_id'],check['date'].isoformat())].add(float(value))
        if len(raw_verified)%30==0:print('AUDIT_RAW',len(raw_verified),flush=True)
    unique_checks={(c['series_id'],c['date'].isoformat()):c for rows in checks.values() for c in rows}
    failures=[]
    for key,c in unique_checks.items():
        values=found[key]
        if values!={c['value']}:failures.append({'series_id':c['series_id'],'date':c['date'].isoformat(),'stored':c['value'],'source_values':sorted(values)})
    report={'status':'passed' if not failures else 'failed','series_count':len(file_checks),'observations':observations,
            'raw_sources_hash_verified':len(raw_verified),'independent_raw_source_samples':len(unique_checks),'failures':failures,
            'file_checks':file_checks,'audit_method':'Every inventory file: Parquet/JSON hashes, exact footer equality, canonical schema, monotonic unique finite historical observations, eligibility. Seven evenly spaced observations per series independently looked up in original JSON or XML blocks without collector parsers. This samples source values; it is not an exhaustive reconstruction of every observation.',
            'collector_source_sha256':hashlib.sha256((ROOT/'sources_energy11.py').read_bytes()).hexdigest(),
            'audit_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'created_at':pd.Timestamp.now(tz='UTC').isoformat()}
    atomic_bytes(ROOT/'logs/energy11_audit.json',json.dumps(report,indent=2).encode())
    print(json.dumps({k:v for k,v in report.items() if k not in {'file_checks','failures'}},indent=2))
    if failures:raise SystemExit(1)


if __name__=='__main__':audit()
