import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
import pandas as pd
import numpy as np
from sources_openaq10 import SafeFetch, parse_archive, clean_sensor, infer_frequency, select_window, list_location, location_flags, sensor_flags, refreshed_partitions, retracted_dates, validate_quality_revision, API

SENSOR={'id':9,'parameter':{'name':'pm25','units':'µg/m³'}}
def raw(rows):
    header='location_id,sensors_id,datetime,parameter,units,value\n'
    return gzip.compress((header+'\n'.join(rows)).encode())
def frame(values):
    return pd.DataFrame({'sensors_id':9,'parameter':'pm25','units':'µg/m³','date':pd.date_range('2024-01-01',periods=len(values),freq='h',tz='UTC'),'value':values})

class OpenAQ10Tests(unittest.TestCase):
    def test_archive_offset_clock(self):
        f=parse_archive(raw(['2,9,2024-01-01T01:00:00+05:30,pm25,µg/m³,12.25']),2)
        self.assertEqual(f.date.iloc[0],pd.Timestamp('2023-12-31T19:30Z'));self.assertEqual(f.value.iloc[0],12.25)
    def test_archive_reject_wrong_site(self):
        with self.assertRaisesRegex(ValueError,'location mismatch'):parse_archive(raw(['3,9,2024-01-01T01:00:00Z,pm25,µg/m³,12']),2)
    def test_negative_missing_zero(self):
        f,s=clean_sensor(frame([0,1,-999,np.nan,np.inf]),SENSOR,[])
        self.assertEqual(list(f.value),[0,1]);self.assertEqual(s['nonfinite_or_negative_omitted'],3)
    def test_flags_sensor_scope_and_boundaries(self):
        fs=[{'flagType':{'level':'ERROR'},'sensorIds':[9],'datetimeFrom':{'utc':'2024-01-01T01:00:00Z'},'datetimeTo':{'utc':'2024-01-01T02:00:00Z'}},{'flagType':{'level':'ERROR'},'sensorIds':[10],'datetimeFrom':{'utc':'2024-01-01T03:00:00Z'},'datetimeTo':{'utc':'2024-01-01T03:00:00Z'}}]
        f,s=clean_sensor(frame([1,2,3,4]),SENSOR,fs);self.assertEqual(list(f.value),[1,4]);self.assertEqual(s['error_flag_interval_omitted'],2)
    def test_location_wide_error_and_warning(self):
        flags=[{'flagType':{'level':level},'sensorIds':[],'datetimeFrom':{'utc':t},'datetimeTo':{'utc':t}} for level,t in [('ERROR','2024-01-01T01:00:00Z'),('WARNING','2024-01-01T02:00:00Z')]]
        f,_=clean_sensor(frame([1,2,3]),SENSOR,flags);self.assertEqual(list(f.value),[1,3])
    def test_conflicts_omitted_duplicates_collapse(self):
        f=frame([1,2,3]);f=pd.concat([f,f.iloc[[0]],f.iloc[[1]].assign(value=99)],ignore_index=True)
        result,stats=clean_sensor(f,SENSOR,[]);self.assertEqual(list(result.value),[1,3]);self.assertEqual(stats['conflicting_timestamp_omitted'],1);self.assertEqual(stats['identical_duplicates_removed'],1)
    def test_unit_change_rejected(self):
        f=frame([1,2]);f.loc[1,'units']='ppm'
        with self.assertRaisesRegex(ValueError,'unit changed'):clean_sensor(f,SENSOR,[])
    def test_native_frequency(self):
        f=frame([1,2,3]);self.assertEqual(infer_frequency(f),'hourly');f.date=pd.date_range('2024-01-01',periods=3,freq='5min',tz='UTC');self.assertEqual(infer_frequency(f),'5min')
    def test_window_does_not_replace_dense_history_with_short_restart(self):
        dates=list(pd.date_range('2023-01-01',periods=200))+list(pd.date_range('2026-08-01',periods=50))
        p=[{'day':d.date().isoformat()} for d in dates];r=select_window(p);self.assertEqual(len(r),200);self.assertEqual(r[-1]['day'],'2023-07-19')
    def test_window_caps_history(self):
        p=[{'day':d.date().isoformat()} for d in pd.date_range('2023-01-01',periods=1000)];self.assertEqual(len(select_window(p)),731)
    def fetcher(self,root):
        with patch.object(Path,'read_text',return_value=json.dumps({'api_key':'unit-test-secret'})):return SafeFetch(root)
    def test_auth_only_approved_api_and_no_redirect(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);response=Mock(status_code=200,content=b'{"results":[]}',headers={})
            with patch('sources_openaq10.SafeFetch.request',return_value=response) as get:
                f.get(API+'locations',{'iso':'ZA'});kwargs=get.call_args.kwargs
                self.assertFalse(kwargs['allow_redirects']);self.assertEqual(kwargs['headers']['X-API-Key'],'unit-test-secret')
            for p in Path(d).rglob('*'):
                if p.is_file():self.assertNotIn(b'unit-test-secret',p.read_bytes())
    def test_no_secret_to_public_archive_or_external_host(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);response=Mock(status_code=200,content=b'data',headers={})
            with patch('sources_openaq10.SafeFetch.request',return_value=response) as get:
                f.get('https://openaq-data-archive.s3.amazonaws.com/example');self.assertNotIn('X-API-Key',get.call_args.kwargs['headers'])
                with self.assertRaises(ValueError):f.get('https://example.org')
                with self.assertRaises(ValueError):f.get(API+'locations',{'key':'unit-test-secret'})
                self.assertEqual(get.call_count,1)
    def test_error_body_and_redirect_not_recorded(self):
        for status in [302,401,429,500]:
            with self.subTest(status=status),tempfile.TemporaryDirectory() as d:
                f=self.fetcher(d);response=Mock(status_code=status,content=b'unit-test-secret',headers={})
                with patch('sources_openaq10.SafeFetch.request',return_value=response),self.assertRaisesRegex(RuntimeError,'HTTP '+str(status)):f.get(API+'locations')
                self.assertEqual(list(Path(d).rglob('*')),[])
    def test_success_response_with_key_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);response=Mock(status_code=200,content=b'unit-test-secret',headers={})
            with patch('sources_openaq10.SafeFetch.request',return_value=response),self.assertRaisesRegex(RuntimeError,'containing credential'):f.get(API+'locations')
            self.assertEqual(list(Path(d).rglob('*')),[])

    def test_unchanged_archive_clock_uses_cache(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);response=Mock(status_code=200,content=b'data',headers={'Last-Modified':'Tue, 01 Oct 2024 00:00:00 GMT'})
            with patch('sources_openaq10.SafeFetch.request',return_value=response) as request:
                url='https://openaq-data-archive.s3.amazonaws.com/example'
                f.get(url);f.get(url,source_last_modified='2024-10-01T00:00:00Z');self.assertEqual(request.call_count,1)
    def test_changed_archive_clock_refetches(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);responses=[Mock(status_code=200,content=b'old',headers={'Last-Modified':'Tue, 01 Oct 2024 00:00:00 GMT'}),Mock(status_code=200,content=b'new',headers={'Last-Modified':'Wed, 02 Oct 2024 00:00:00 GMT'})]
            with patch('sources_openaq10.SafeFetch.request',side_effect=responses) as request:
                url='https://openaq-data-archive.s3.amazonaws.com/example'
                f.get(url);data,_=f.get(url,source_last_modified='2024-10-02T00:00:00Z');self.assertEqual(data,b'new');self.assertEqual(request.call_count,2)
    def test_forced_refresh_bypasses_cache(self):
        with tempfile.TemporaryDirectory() as d:
            f=self.fetcher(d);response=Mock(status_code=200,content=b'data',headers={})
            with patch('sources_openaq10.SafeFetch.request',return_value=response) as request:
                url='https://openaq-data-archive.s3.amazonaws.com/example'
                f.get(url);f.get(url,refresh=True);self.assertEqual(request.call_count,2)
    def test_listing_refresh_forwarded_and_truncation_rejected(self):
        f=Mock();f.get.return_value=(b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated></ListBucketResult>','h')
        parts,hashes=list_location(f,2,[2024],refresh=True);self.assertTrue(f.get.call_args.kwargs['refresh']);self.assertEqual(parts,[])
        f.get.return_value=(b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated></ListBucketResult>','h')
        with self.assertRaisesRegex(ValueError,'truncated'):list_location(f,2,[2024])
    def test_flags_refresh_forwarded(self):
        f=Mock();f.get.return_value=(b'{"results":[]}', 'h');flags,hashes=location_flags(f,2,refresh=True)
        self.assertTrue(f.get.call_args.kwargs['refresh']);self.assertEqual(flags,[]);self.assertEqual(hashes,['h'])

    def test_complete_flags_response_larger_than_requested_page(self):
        f=Mock();rows=[{'test_flag':i} for i in range(1002)];f.get.return_value=(json.dumps({'meta':{'found':1002},'results':rows}).encode(),'h')
        got,hashes=location_flags(f,2);self.assertEqual(got,rows);self.assertEqual(f.get.call_count,1)
    def test_flags_repeated_incomplete_page_rejected(self):
        f=Mock();rows=[{'test_flag':i} for i in range(1000)];f.get.return_value=(json.dumps({'meta':{'found':1002},'results':rows}).encode(),'h')
        with self.assertRaisesRegex(ValueError,'repeated'):location_flags(f,2)
        self.assertEqual(f.get.call_count,2)

    def test_naive_timestamp_rejected(self):
        with self.assertRaisesRegex(ValueError,'timezone offset'):parse_archive(raw(['2,9,2024-01-01T01:00:00,pm25,µg/m³,12']),2)
    def test_short_flag_page_with_declared_total_continues(self):
        f=Mock();f.get.side_effect=[(json.dumps({'meta':{'found':2},'results':[{'test_flag':1}]}).encode(),'h1'),(json.dumps({'meta':{'found':2},'results':[{'test_flag':2}]}).encode(),'h2')]
        got,hashes=location_flags(f,2);self.assertEqual(len(got),2);self.assertEqual(f.get.call_count,2)
    def test_sensor_flags_use_sensor_endpoint(self):
        f=Mock();f.get.return_value=(b'{"meta":{"found":0},"results":[]}', 'h');sensor_flags(f,9)
        self.assertEqual(f.get.call_args.args[0],API+'sensors/9/flags')
    def test_refresh_repairs_interior_gaps_and_preserves_start(self):
        chosen={'selected_archive_start':'2024-01-02','selected_archive_end':'2024-01-04','partitions':[{'day':'2024-01-02'},{'day':'2024-01-04'}]}
        parts=[{'day':f'2024-01-0{i}'} for i in range(1,6)]
        self.assertEqual([p['day'] for p in refreshed_partitions(chosen,parts)],['2024-01-02','2024-01-03','2024-01-04','2024-01-05'])
    def test_new_error_retracts_old_value_but_ordinary_gap_preserves_it(self):
        import hashlib
        from expansion_common import save_candidate
        from library import publish_staged,build_catalog,load
        from refresh import promote_existing
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);rawbytes=b'openaq quality revision fixture';digest=hashlib.sha256(rawbytes).hexdigest();p=root/'archive/raw'/(digest+'.bin');p.parent.mkdir(parents=True);p.write_bytes(rawbytes)
            f=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=100,tz='UTC'),'value':np.arange(100,dtype=float)})
            kwargs=dict(title='fixture',provider='fixture',category='fixture',unit='µg/m³',frequency='daily',source_url='https://example.com',geography='fixture',hypothesis='fixture',limitations='fixture',raw_sha256=digest,root=root,dataset_identity='fixture')
            save_candidate('openaq10','quality_fixture',f,**kwargs);publish_staged(1,1,root)
            when=f.date.iloc[5].isoformat();flags=[{'flagType':{'level':'ERROR'},'sensorIds':[],'datetimeFrom':{'utc':when},'datetimeTo':{'utc':when}}]
            withdrawn=retracted_dates(f,SENSOR,flags);self.assertEqual(withdrawn,[when])
            save_candidate('openaq10','quality_fixture',f.drop(index=[5,6]),retracted_dates=withdrawn,**kwargs)
            promote_existing(root,groups={'openaq10'});build_catalog(root);result=load('quality_fixture',root)
            self.assertEqual(len(result),99);self.assertNotIn(f.date.iloc[5],result.date.tolist());self.assertIn(f.date.iloc[6],result.date.tolist())

    def test_full_error_withdrawal_cannot_silently_succeed(self):
        old=frame([1,2,3]);flags=[{'flagType':{'level':'ERROR'},'sensorIds':[],'datetimeFrom':{'utc':old.date.iloc[0].isoformat()},'datetimeTo':{'utc':old.date.iloc[-1].isoformat()}}]
        with self.assertRaisesRegex(ValueError,'requires series retirement'):validate_quality_revision(old,SENSOR,flags,old.iloc[:0][['date','value']])
    def test_partial_error_withdrawal_below_eligibility_fails(self):
        old=frame([1,2,3]);when=old.date.iloc[0].isoformat();flags=[{'flagType':{'level':'ERROR'},'sensorIds':[],'datetimeFrom':{'utc':when},'datetimeTo':{'utc':when}}]
        with self.assertRaisesRegex(ValueError,'insufficient'):validate_quality_revision(old,SENSOR,flags,old.iloc[1:][['date','value']])
    def test_empty_refresh_without_explicit_withdrawal_is_not_retraction(self):
        old=frame([1,2,3]);self.assertEqual(validate_quality_revision(old,SENSOR,[],old.iloc[:0][['date','value']]),[])

if __name__=='__main__':unittest.main()
