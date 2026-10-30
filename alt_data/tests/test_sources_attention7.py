import io,json,sys,unittest,zipfile,tempfile
from unittest.mock import Mock,patch
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sources_attention7 import parse_gdelt,parse_wikimedia,wikimedia,LANGUAGES,PublicFetcher,CacheMiss,AccessDenied

STAMP='20260701001500'
def event(eid=1,geo='CH',root='14',quad='3',tone='-4.5',actor='LAB',stamp=STAMP):
    row=['']*61
    for index,value in {0:str(eid),1:'20160101',12:actor,28:root,29:quad,34:tone,53:geo,59:stamp}.items():row[index]=value
    return row
def archive(rows,member=STAMP+'.export.CSV'):
    sink=io.BytesIO()
    with zipfile.ZipFile(sink,'w') as z:z.writestr(member,'\n'.join('\t'.join(row) for row in rows))
    return sink.getvalue()
def wikidata(hours=('2026010100','2026010102'),values=(5,7),**overrides):
    items=[]
    for hour,value in zip(hours,values):
        items.append(dict(project='hi.wikipedia',access='desktop',agent='user',granularity='hourly',timestamp=hour,views=value,**overrides))
    return json.dumps({'items':items}).encode()

class AttentionParsers(unittest.TestCase):
    def wiki_collector(self,pinned):
        fetch=Mock(cache_only=False)
        def response(url,**kwargs):
            project,access=url.split('/aggregate/',1)[1].split('/')[:2]
            items=[dict(project=project.removesuffix('.org'),access=access,agent='user',granularity='hourly',
                        timestamp=stamp,views=value) for stamp,value in [('2026010100',5),('2026010101',7)]]
            return json.dumps({'items':items}).encode(),'0'*64
        fetch.get.side_effect=response
        with patch('sources_attention7.baseline',return_value={'pinned':pinned}),patch('sources_attention7.attempt') as attempt,patch('builtins.print'):
            wikimedia(fetch,pd.Timestamp('2026-01-01',tz='UTC'),pd.Timestamp('2026-01-01',tz='UTC'))
        return fetch,attempt
    def wiki(self,raw):
        return parse_wikimedia(raw,'hi.wikipedia.org','desktop',pd.Timestamp('2026-01-01',tz='UTC'),pd.Timestamp('2026-01-02',tz='UTC'))
    def test_gdelt_discovery_counts_not_claimed_event_date_or_mentions(self):
        rows=[event(),event(2,quad='4',tone='2.5',actor='BUS'),event(3,geo='IN',root='01',quad='1',actor='GOV')]
        rows[0][31]='900'  # NumMentions must never multiply a new event.
        r=parse_gdelt(archive(rows),STAMP,{'CH':'CN','IN':'IN'})
        self.assertEqual(r['global_rows'],3)
        self.assertEqual(r['metrics']['CN'],{'events':2,'protest_events':2,'material_conflict_events':1,'business_labor_events':2,'mean_tone':-1.0})
        self.assertEqual(r['metrics']['IN']['events'],1)
    def test_event_location_is_not_actor_nationality(self):
        row=event(geo='IN');row[7]='CHN'
        r=parse_gdelt(archive([row]),STAMP,{'CH':'CN','IN':'IN'})
        self.assertEqual(r['metrics']['CN']['events'],0);self.assertIsNone(r['metrics']['CN']['mean_tone'])
        self.assertEqual(r['metrics']['IN']['events'],1)
    def test_actor_type2_and_actor2_are_retained(self):
        row=event(actor='GOV');row[24]='BUS'
        r=parse_gdelt(archive([row]),STAMP,{'CH':'CN'})
        self.assertEqual(r['metrics']['CN']['business_labor_events'],1)
    def test_unclassified_root_is_not_invented_as_protest(self):
        r=parse_gdelt(archive([event(root='--',quad='4')]),STAMP,{'CH':'CN'})
        self.assertEqual(r['metrics']['CN']['events'],1)
        self.assertEqual(r['metrics']['CN']['protest_events'],0)
        self.assertEqual(r['metrics']['CN']['material_conflict_events'],1)
    def test_complete_empty_batch_has_zero_counts_and_missing_tone(self):
        r=parse_gdelt(archive([]),STAMP,{'CH':'CN'})
        self.assertEqual(r['metrics']['CN']['events'],0);self.assertIsNone(r['metrics']['CN']['mean_tone'])
    def test_missing_or_wrong_archive_is_not_zero(self):
        with self.assertRaises(zipfile.BadZipFile):parse_gdelt(b'',STAMP,{'CH':'CN'})
        with self.assertRaisesRegex(ValueError,'archive member'):parse_gdelt(archive([],member='mentions.CSV'),STAMP,{'CH':'CN'})
    def test_gdelt_rejects_conflicting_clock_and_duplicate_event(self):
        with self.assertRaisesRegex(ValueError,'DATEADDED'):parse_gdelt(archive([event(stamp='20260701000000')]),STAMP,{'CH':'CN'})
        with self.assertRaisesRegex(ValueError,'Duplicate'):parse_gdelt(archive([event(),event()]),STAMP,{'CH':'CN'})
    def test_gdelt_rejects_schema_drift_invalid_tone_and_unknown_quad(self):
        for row in [event()+['extra'],event(tone='nan'),event(tone='101'),event(quad='0')]:
            with self.assertRaises(ValueError):parse_gdelt(archive([row]),STAMP,{'CH':'CN'})
    def test_wikimedia_hours_remain_hours_and_missing_stays_missing(self):
        f=self.wiki(wikidata())
        self.assertEqual(f.date.dt.hour.tolist(),[0,2]);self.assertEqual(f.value.tolist(),[5.,7.])
        self.assertEqual(str(f.date.dtype),'datetime64[ns, UTC]')
    def test_wikimedia_wrong_agent_or_dimensions_fail(self):
        for key,value in [('agent','all-agents'),('project','en.wikipedia'),('access','mobile-web'),('granularity','daily')]:
            obj=json.loads(wikidata());obj['items'][0][key]=value
            with self.assertRaisesRegex(ValueError,'dimensions'):self.wiki(json.dumps(obj).encode())
    def test_wikimedia_invalid_counts_fail(self):
        for value in [True,-1,1.5,'3']:
            with self.assertRaisesRegex(ValueError,'view count'):self.wiki(wikidata(values=(value,3)))
    def test_wikimedia_duplicates_and_outside_window_fail(self):
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.wiki(wikidata(hours=('2026010100','2026010100')))
        with self.assertRaisesRegex(ValueError,'outside'):self.wiki(wikidata(hours=('2025123123','2026010100')))
    def test_wikimedia_no_items_is_missing_not_zero(self):
        with self.assertRaisesRegex(ValueError,'items'):self.wiki(b'{"items":[]}')
    def test_published_wikimedia_refresh_only_requests_selected_channels(self):
        sid='wikimedia7_es_desktop_hourly_views'
        fetch,attempt=self.wiki_collector({sid:'hourly','gdelt7_cn_events':'15min'})
        fetch.get.assert_called_once_with('https://wikimedia.org/api/rest_v1/metrics/pageviews/aggregate/es.wikipedia.org/desktop/user/hourly/2026010100/2026010123',immutable=False)
        self.assertEqual(attempt.call_count,1);self.assertEqual(attempt.call_args.args[1],sid)
        fetch,attempt=self.wiki_collector({'gdelt7_cn_events':'15min'})
        fetch.get.assert_not_called();attempt.assert_not_called()
    def test_unpublished_wikimedia_collection_retains_all_planned_channels(self):
        fetch,attempt=self.wiki_collector({})
        self.assertEqual(fetch.get.call_count,len(LANGUAGES)*3)
        requested={call.args[0].split('/aggregate/',1)[1].split('/user/',1)[0] for call in fetch.get.call_args_list}
        self.assertEqual(requested,{language+'.wikipedia.org/'+access for language in LANGUAGES for access in ['desktop','mobile-web','mobile-app']})
        self.assertEqual(attempt.call_count,len(LANGUAGES)*3)
    def test_cache_only_never_requests_missing_source(self):
        with tempfile.TemporaryDirectory() as directory, patch('sources_attention7.ROOT',Path(directory)),patch('sources_attention7.requests.Session') as session:
            with self.assertRaises(CacheMiss):PublicFetcher(cache_only=True).get('https://example.test/missing')
            session.assert_not_called()
    def test_rate_limit_stops_subsequent_requests(self):
        with tempfile.TemporaryDirectory() as directory,patch('sources_attention7.ROOT',Path(directory)),patch('sources_attention7.requests.Session') as session:
            session.return_value.get.return_value.status_code=429
            fetch=PublicFetcher()
            with self.assertRaises(AccessDenied):fetch.get('https://example.test/first')
            with self.assertRaises(AccessDenied):fetch.get('https://example.test/second')
            self.assertEqual(session.return_value.get.call_count,1)
    def test_mutable_history_refresh_replaces_cache_but_cache_only_never_fetches(self):
        with tempfile.TemporaryDirectory() as directory,patch('sources_attention7.ROOT',Path(directory)),patch('sources_attention7.requests.Session') as session:
            url='https://example.test/year'
            first=Mock(status_code=200,url=url,content=b'old',headers={})
            second=Mock(status_code=200,url=url,content=b'revised',headers={})
            session.return_value.get.side_effect=[first,second]
            self.assertEqual(PublicFetcher(request_pause_seconds=0).get(url,immutable=False)[0],b'old')
            self.assertEqual(PublicFetcher(refresh=True,request_pause_seconds=0).get(url,immutable=False)[0],b'revised')
            self.assertEqual(PublicFetcher(refresh=True,cache_only=True).get(url,immutable=False)[0],b'revised')
            self.assertEqual(session.return_value.get.call_count,2)

if __name__=='__main__':unittest.main()
