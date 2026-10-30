import fcntl, importlib.util, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
import requests
from sources_cams10 import ADS, API, ADSAccessError, FIELDS, LOCATIONS, begin_refresh_intent, collect, decode, digest, ec, merge_companions, refresh_selection, request_for, retrieve_with_intent, sanitized, selected_requests, series_id, window_start

def message(pid=210209, lead=24, value=.25, missing_target=False, **changes):
    g=ec.codes_grib_new_from_samples('regular_ll_sfc_grib1')
    try:
        vals={'paramId':pid,'Ni':3,'Nj':3,'latitudeOfFirstGridPointInDegrees':23.6,
              'longitudeOfFirstGridPointInDegrees':72.,'latitudeOfLastGridPointInDegrees':22.8,
              'longitudeOfLastGridPointInDegrees':72.8,'iDirectionIncrementInDegrees':.4,
              'jDirectionIncrementInDegrees':.4,'dataDate':20260901,'dataTime':0,'dataType':'fc','step':lead}
        vals.update(changes)
        for k,v in vals.items(): ec.codes_set(g,k,v)
        values=np.arange(9,dtype=float)/1000+value
        if missing_target:
            ec.codes_set(g,'missingValue',9999.0);ec.codes_set(g,'bitmapPresent',1);values[4]=9999.0
        ec.codes_set_values(g,values)
        return ec.codes_get_message(g)
    finally: ec.codes_release(g)

class CAMS(unittest.TestCase):
    def setUp(self):
        self.loc=LOCATIONS[1]
        self.req=request_for(self.loc,'2026-09-01','2026-09-01')
        self.req.update(time=['00:00'],leadtime_hour=['24'],variable=['dust_aerosol_optical_depth_550nm'])

    def supervisor(self):
        path=Path(__file__).resolve().parents[1]/'archive/work/run_cams10_backfill.py'
        spec=importlib.util.spec_from_file_location('cams10_supervisor_test',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module

    def test_supervisor_duplicate_invocation_leaves_running_status_unchanged(self):
        supervisor=self.supervisor()
        with tempfile.TemporaryDirectory() as tmp,patch.object(supervisor,'ROOT',Path(tmp)),patch.object(supervisor,'run') as run:
            logs=Path(tmp)/'logs';logs.mkdir()
            status=logs/'cams10_backfill_job.json';status.write_text('{"status":"running","pid":123}')
            before=status.read_bytes()
            with (logs/'cams10_backfill.lock').open('a') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                self.assertEqual(supervisor.main(),0)
            run.assert_not_called();self.assertEqual(status.read_bytes(),before)

    def test_supervisor_failure_stops_following_provider_poll(self):
        supervisor=self.supervisor();client=supervisor.MonitoredADS.__new__(supervisor.MonitoredADS)
        supervisor.STOP.set()
        with patch.object(ADS,'call') as call:
            with self.assertRaisesRegex(RuntimeError,'Local monitoring stopped'):
                client.call('GET','/retrieve/v1/jobs/previously-accepted-job')
            call.assert_not_called()

    def test_supervisor_download_integrity_error_sets_failure_without_waiting_other_future(self):
        supervisor=self.supervisor()
        with tempfile.TemporaryDirectory() as tmp,patch.object(supervisor,'ROOT',Path(tmp)),patch.object(supervisor,'STATUS',Path(tmp)/'status.json'),patch.object(supervisor,'MonitoredADS') as client:
            request=supervisor.request_for(self.loc,supervisor.START,supervisor.END)
            path=Path(tmp)/'archive/cams10_jobs'/(digest(__import__('json').dumps(request,sort_keys=True).encode())+'.json')
            path.parent.mkdir(parents=True);path.write_text('{"job_id":"saved-existing-job"}')
            client.return_value.retrieve.side_effect=ValueError('Cached forecast checksum differs')
            with self.assertRaisesRegex(ValueError,'checksum'):
                supervisor.fetch(self.loc)
            report=__import__('json').loads((Path(tmp)/'status.json').read_text())
            self.assertEqual(report['status'],'failed');self.assertEqual(report['error_type'],'ValueError')
            self.assertTrue(supervisor.STOP.is_set())

    def test_supervisor_bounded_transport_retry_keeps_existing_job_and_rejects_terminal_failure(self):
        supervisor=self.supervisor()
        with tempfile.TemporaryDirectory() as tmp,patch.object(supervisor,'ROOT',Path(tmp)),patch.object(supervisor,'STATUS',Path(tmp)/'status.json'),patch.object(supervisor,'MonitoredADS') as client,patch.object(supervisor,'decode',return_value=[1]) as decode,patch.object(supervisor.STOP,'wait',return_value=False) as wait:
            request=supervisor.request_for(self.loc,supervisor.START,supervisor.END)
            path=Path(tmp)/'archive/cams10_jobs'/(digest(__import__('json').dumps(request,sort_keys=True).encode())+'.json')
            path.parent.mkdir(parents=True);path.write_text('{"job_id":"saved-existing-job"}')
            client.return_value.retrieve.side_effect=[ADSAccessError('ADS transport failed; response not received')]*5+[(b'GRIB',{'job_id':'saved-existing-job'})]
            report=supervisor.fetch(self.loc)
            self.assertEqual(report['receipt']['job_id'],'saved-existing-job')
            self.assertEqual([c.args[0] for c in wait.call_args_list],[30,60,60,60,60]);self.assertEqual(client.return_value.retrieve.call_count,6)
            client.return_value.retrieve.reset_mock();wait.reset_mock()
            client.return_value.retrieve.side_effect=ADSAccessError('ADS transport failed; response not received')
            with self.assertRaisesRegex(ADSAccessError,'ADS transport failed'):supervisor.fetch(self.loc)
            self.assertEqual(client.return_value.retrieve.call_count,6);self.assertEqual(wait.call_count,5)
            self.assertTrue(supervisor.STOP.is_set());supervisor.STOP.clear()
            client.return_value.retrieve.reset_mock();wait.reset_mock()
            client.return_value.retrieve.side_effect=ADSAccessError('ADS job failed')
            with self.assertRaisesRegex(ADSAccessError,'ADS job failed'):supervisor.fetch(self.loc)
            self.assertEqual(client.return_value.retrieve.call_count,1);wait.assert_not_called()

    def test_exact_cell_forecast_clock(self):
        out=decode(message(),self.loc,self.req)
        self.assertEqual(len(out),1);self.assertAlmostEqual(out.value.iloc[0],.254,places=7)
        self.assertEqual(str(out.initialization.iloc[0]),'2026-09-01 00:00:00+00:00')
        self.assertEqual(str(out.valid_time.iloc[0]),'2026-09-02 00:00:00+00:00')
        self.assertEqual(out.lead_hours.iloc[0],24)

    def test_missing_and_duplicate_messages(self):
        req={**self.req,'leadtime_hour':['24','72']}
        with self.assertRaisesRegex(ValueError,'Missing'):decode(message(),self.loc,req)
        with self.assertRaisesRegex(ValueError,'duplicate'):decode(message()*2,self.loc,self.req)

    def test_analysis_wrong_parameter_wrong_grid_rejected(self):
        for raw in [message(dataType='an'),message(pid=210127),message(iDirectionIncrementInDegrees=1.)]:
            with self.assertRaises(ValueError):decode(raw,self.loc,self.req)

    def test_missing_and_negative_values_rejected(self):
        with self.assertRaises(ValueError):decode(message(value=-1.),self.loc,self.req)
        with self.assertRaises(ValueError):decode(message()[:-1],self.loc,self.req)

    def test_bitmap_missing_selected_cell_is_not_a_numeric_observation(self):
        raw=message(missing_target=True)
        g=ec.codes_new_from_message(raw)
        try:
            self.assertEqual(ec.codes_get_values(g)[4],9999.)
            self.assertEqual(ec.codes_get_array(g,'bitmap')[4],0)
        finally:ec.codes_release(g)
        with self.assertRaisesRegex(ValueError,'missing in GRIB bitmap'):decode(raw,self.loc,self.req)

    def test_small_plan_and_surface_identity(self):
        self.assertEqual(len(LOCATIONS)*len(FIELDS)*3,96)
        self.assertEqual(self.req['area'],[23.6,72.,22.8,72.8])
        self.assertEqual(self.req['data_format'],'grib')
        for field,(_,pid,_,_) in FIELDS.items():
            req={**self.req,'variable':[FIELDS[field][0]]}
            self.assertEqual(decode(message(pid=pid),self.loc,req).field.iloc[0],field)

    def test_redacts_secrets_and_signed_urls(self):
        s=sanitized({'PRIVATE-TOKEN':'abc','x':'abc https://x.example/file?signature=q&key=s#hash'},'abc')
        self.assertEqual(s,{'PRIVATE-TOKEN':'[redacted]','x':'[redacted] https://x.example/file'})

    def test_auth_never_redirects_and_license_not_accepted(self):
        client=ADS.__new__(ADS);client.key='dummy-secret';client.session=unittest.mock.Mock()
        r=requests.Response();r.status_code=403;r._content=b'{"title":"required licences not accepted"}'
        client.session.request.return_value=r
        with tempfile.TemporaryDirectory() as tmp,patch('sources_cams10.ROOT',Path(tmp)):
            with self.assertRaisesRegex(ADSAccessError,'accepted manually'):client.call('POST','/retrieve/v1/processes/test/execution',{'inputs':self.req})
            text=''.join(p.read_text() for p in Path(tmp).rglob('*.json'))
            self.assertNotIn('dummy-secret',text)
        args,kwargs=client.session.request.call_args
        self.assertEqual(args[1],API+'/retrieve/v1/processes/test/execution')
        self.assertFalse(kwargs['allow_redirects'])
        self.assertEqual(client.session.request.call_count,1)

    def test_refresh_pins_membership_and_excludes_own_baseline(self):
        own=series_id('IN','ahmedabad','dust_aod550',24)
        cat=pd.DataFrame({'series_id':[own,'old_other_feed'],'dataset_identity':['own','old'],'content_sha256':['same-own-numbers','old-hash']})
        pinned,baseline,locations=refresh_selection(cat)
        self.assertEqual(pinned,{own});self.assertEqual(baseline.series_id.tolist(),['old_other_feed'])
        self.assertEqual([loc[1] for loc in locations],['ahmedabad'])
        with self.assertRaisesRegex(ValueError,'outside'):refresh_selection(cat,['shanghai'])

    def test_refresh_requests_only_published_field_lead_pairs(self):
        pinned={series_id('IN','ahmedabad','pm25',0),series_id('IN','ahmedabad','dust_aod550',72)}
        requests=selected_requests(self.loc,'2026-09-01','2026-09-01',pinned)
        actual={(v,h) for req in requests for v in req['variable'] for h in req['leadtime_hour']}
        self.assertEqual(actual,{('particulate_matter_2.5um','0'),('dust_aerosol_optical_depth_550nm','72')})

    def test_incremental_window_uses_least_current_history_and_explicit_override(self):
        pinned={series_id('IN','ahmedabad','pm25',0),series_id('IN','ahmedabad','dust_aod550',72)}
        previous=pd.DataFrame({'location':['ahmedabad','ahmedabad'],'field':['pm25','dust_aod550'],
                               'lead_hours':[0,72],'initialization':pd.to_datetime(['2026-10-19T12:00Z','2026-10-15T12:00Z'])})
        self.assertEqual(window_start(self.loc,pinned,previous,'2026-10-20'),'2026-10-08')
        self.assertEqual(window_start(self.loc,pinned,previous,'2026-10-20','2026-01-01'),'2026-01-01')
        self.assertEqual(window_start(self.loc,pinned,previous.iloc[:1],'2026-10-20'),'2025-10-01')
        self.assertEqual(window_start(self.loc,pinned,pd.DataFrame(),'2026-10-20'),'2025-10-01')

    def test_incremental_merge_retains_old_dates_and_replaces_overlap(self):
        old=decode(message(),self.loc,self.req);old['raw_sha256']='a'*64;old['retrieved_at']=pd.Timestamp('2026-10-01',tz='UTC')
        current=old.copy();current['value']=.5;current['raw_sha256']='b'*64
        extra=current.copy();extra['initialization']+=pd.Timedelta(hours=12);extra['valid_time']+=pd.Timedelta(hours=12)
        combined=merge_companions(old,pd.concat([current,extra]))
        self.assertEqual(len(combined),2);self.assertEqual(combined.value.tolist(),[.5,.5])
        earlier=old.copy();earlier['initialization']-=pd.Timedelta(days=100);earlier['valid_time']-=pd.Timedelta(days=100)
        merged=merge_companions(pd.concat([earlier,old]),current)
        self.assertEqual(merged.value.tolist(),[old.value.iloc[0],.5])
        stale=current.copy();stale['retrieved_at']=pd.Timestamp('2026-09-01',tz='UTC')
        protected=merge_companions(old,stale)
        self.assertEqual(protected.value.tolist(),old.value.tolist())

    def test_partial_refresh_preserves_other_companions_and_old_version(self):
        old=decode(message(),self.loc,self.req);old['raw_sha256']='a'*64;old['retrieved_at']=pd.Timestamp('2026-10-01',tz='UTC')
        other=old.copy();other['country']='CN';other['location']='shanghai';other['grid_latitude']=31.2;other['grid_longitude']=121.6
        previous=pd.concat([old,other],ignore_index=True)
        raw=message(value=.5)+message(value=.6,dataTime=1200)
        own=[series_id('IN','ahmedabad','dust_aod550',24),series_id('CN','shanghai','dust_aod550',24)]
        cat=pd.DataFrame({'series_id':own,'dataset_identity':['own1','own2'],'content_sha256':['a','b']})
        with tempfile.TemporaryDirectory() as tmp,patch('sources_cams10.ROOT',Path(tmp)),patch('sources_cams10.ADS') as client,patch('sources_cams10.Fetcher') as fetcher:
            cat.to_parquet(Path(tmp)/'catalog.parquet',index=False)
            companion=Path(tmp)/'supplementary/cams10_issue_target.parquet';companion.parent.mkdir();previous.to_parquet(companion,index=False)
            old_version=Path(tmp)/'supplementary/old_published_companion.parquet';old_version.write_bytes(companion.read_bytes());old_bytes=old_version.read_bytes()
            client.return_value.retrieve.return_value=(raw,{'raw_sha256':digest(raw),'retrieved_at':'2026-10-21T00:00:00Z'})
            # Stop before candidate creation: this test targets retrieval selection
            # and companion writes, with no real network or main-library writes.
            fetcher.return_value.get.side_effect=RuntimeError('stop_after_companion')
            with self.assertRaisesRegex(RuntimeError,'stop_after_companion'):
                collect(refresh=True,start='2026-09-01',end='2026-09-01',location_ids=['ahmedabad'])
            call=client.return_value.retrieve.call_args
            self.assertTrue(call.kwargs['refresh']);self.assertEqual(client.return_value.retrieve.call_count,1)
            self.assertEqual(call.args[0]['variable'],['dust_aerosol_optical_depth_550nm'])
            self.assertEqual(call.args[0]['leadtime_hour'],['24'])
            versions=list((Path(tmp)/'supplementary/cams10').glob('issue_target_*.parquet'))
            updated=pd.read_parquet(versions[0])
            self.assertEqual(len(updated),3)
            for column in ('initialization','valid_time','retrieved_at'):
                other[column]=other[column].astype('datetime64[ns, UTC]')
            pd.testing.assert_frame_equal(updated[updated.location.eq('shanghai')].reset_index(drop=True),other.reset_index(drop=True))
            self.assertEqual(old_version.read_bytes(),old_bytes)
            self.assertEqual(companion.read_bytes(),old_bytes)
            self.assertEqual(len(versions),1)

    def test_explicit_refresh_resubmits_completed_job_without_losing_old_cache(self):
        req={'safe':'request'};identity=digest(__import__('json').dumps(req,sort_keys=True).encode())
        raw=b'GRIBfake7777';h=digest(raw)
        with tempfile.TemporaryDirectory() as tmp,patch('sources_cams10.ROOT',Path(tmp)):
            root=Path(tmp);(root/'archive/raw').mkdir(parents=True);(root/'archive/raw'/(h+'.bin')).write_bytes(raw)
            jobs=root/'archive/cams10_jobs';jobs.mkdir();obj={'request':req,'job_id':'old-job','raw_sha256':h}
            path=jobs/(identity+'.json');path.write_text(__import__('json').dumps(obj))
            client=ADS.__new__(ADS);client.call=unittest.mock.Mock(side_effect=ADSAccessError('still blocked'))
            cached,_=client.retrieve(req);self.assertEqual(cached,raw);client.call.assert_not_called()
            with self.assertRaisesRegex(ADSAccessError,'still blocked'):client.retrieve(req,refresh=True)
            self.assertEqual(client.call.call_args.args[0],'POST')
            self.assertEqual((root/'archive/raw'/(h+'.bin')).read_bytes(),raw)
            self.assertTrue((jobs/'history'/(identity+'_old-job.json')).exists())
            self.assertEqual(__import__('json').loads(path.read_text())['raw_sha256'],h)

    def test_forced_refresh_retry_reuses_completed_region_intent(self):
        req_a=self.req;req_b={**self.req,'date':'2026-09-02/2026-09-02'}
        tasks=[(self.loc,req_a),(self.loc,req_b)]
        raw=message();h=digest(raw);receipt={'raw_sha256':h,'job_id':'fresh-job-a','request':req_a}
        with tempfile.TemporaryDirectory() as tmp,patch('sources_cams10.ROOT',Path(tmp)):
            folder=Path(tmp)/'archive/raw';folder.mkdir(parents=True);(folder/(h+'.bin')).write_bytes(raw)
            client=unittest.mock.Mock();client.retrieve.side_effect=[(raw,receipt),ADSAccessError('ADS job remains running')]
            intent=begin_refresh_intent(tasks)
            retrieve_with_intent(client,req_a,0,intent)
            with self.assertRaises(ADSAccessError):retrieve_with_intent(client,req_b,0,intent)
            restarted=begin_refresh_intent(tasks);client.retrieve.reset_mock()
            got,_=retrieve_with_intent(client,req_a,0,restarted)
            self.assertEqual(got,raw);client.retrieve.assert_not_called()

if __name__=='__main__':unittest.main()
