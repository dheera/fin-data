import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import pandas as pd

from sources_energy11 import AccessError, Collector, SafeFetch, deduplicate, gie_country_code, parse_entsoe, parse_gie


def xml(points, curve='A03', resolution='PT15M', start='2025-01-01T00:00Z', end='2025-01-01T01:00Z', price=False):
    field='price.amount' if price else 'quantity'
    rows=''.join(f'<Point><position>{p}</position>'+('' if v is None else f'<{field}>{v}</{field}>')+'</Point>' for p,v in points)
    return (f'<GL_MarketDocument xmlns="urn:test"><createdDateTime>2026-01-01T12:00Z</createdDateTime><TimeSeries>'
            f'<quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>{curve}</curveType>'
            f'<Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval>'
            f'<resolution>{resolution}</resolution>{rows}</Period></TimeSeries></GL_MarketDocument>').encode()


class Energy11Parsers(unittest.TestCase):
    def test_variable_blocks_expand_only_until_next_point_or_period_end(self):
        data,info,res=parse_entsoe(xml([(1,4),(3,7)]),pd.Timestamp('2025-01-01T00:00Z'),pd.Timestamp('2025-01-01T02:00Z'))
        rows=next(iter(data.values()))
        self.assertEqual([v for d,v in rows],[4,4,7,7])
        self.assertEqual(rows[-1][0],pd.Timestamp('2025-01-01T00:45Z'))
        self.assertIn('NOT',info[0]['created_time_semantics'])

    def test_fixed_block_gaps_are_not_filled(self):
        data,_,_=parse_entsoe(xml([(1,4),(3,7)],curve='A01'),'2025-01-01T00:00Z','2025-01-02T00:00Z')
        self.assertEqual([d.minute for d,v in next(iter(data.values()))],[0,30])

    def test_missing_variable_block_is_not_backfilled_from_prior_value(self):
        data,_,_=parse_entsoe(xml([(1,4),(2,None),(4,9)]),'2025-01-01T00:00Z','2025-01-02T00:00Z')
        self.assertEqual([v for d,v in next(iter(data.values()))],[4,9])

    def test_clip_window_and_preserve_negative_power_price(self):
        data,_,_=parse_entsoe(xml([(1,-5),(2,3)],price=True),'2025-01-01T00:00Z','2025-01-01T00:15Z')
        self.assertEqual([v for d,v in next(iter(data.values()))],[-5])

    def test_distinct_price_products_at_same_timestamp_are_never_merged(self):
        one=xml([(1,50)],price=True).decode().split('<TimeSeries>')[1].split('</TimeSeries>')[0]
        one='<auction.type>A01</auction.type><contract_MarketAgreement.type>A01</contract_MarketAgreement.type>'+one
        two=one.replace('<price.amount>50</price.amount>','<price.amount>80</price.amount>')
        raw=('<GL_MarketDocument><TimeSeries><classificationSequence_AttributeInstanceComponent.position>1</classificationSequence_AttributeInstanceComponent.position>'+one+'</TimeSeries><TimeSeries><classificationSequence_AttributeInstanceComponent.position>2</classificationSequence_AttributeInstanceComponent.position>'+two+'</TimeSeries></GL_MarketDocument>').encode()
        data,_,_=parse_entsoe(raw,'2025-01-01T00:00Z','2025-01-01T00:15Z')
        self.assertEqual(len(data),2)
        self.assertEqual({json.loads(key)['classification_sequence'] for key in data},{'1','2'})
        self.assertEqual(sorted(rows[0][1] for rows in data.values()),[50,80])

    def test_reject_duplicate_positions(self):
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            parse_entsoe(xml([(1,4),(1,7)]),'2025-01-01T00:00Z','2025-01-02T00:00Z')

    def test_source_timezone_offsets_are_honored(self):
        data,_,_=parse_entsoe(xml([(1,4)],start='2025-10-26T02:00+02:00',end='2025-10-26T02:00+01:00'),'2025-10-26T00:00Z','2025-10-26T02:00Z')
        rows=next(iter(data.values()))
        self.assertEqual(len(rows),4)
        self.assertEqual(rows[0][0],pd.Timestamp('2025-10-26T00:00Z'))

    def test_conflicting_source_measurements_are_omitted_not_averaged(self):
        frame=pd.DataFrame({'date':pd.to_datetime(['2025-01-01','2025-01-01','2025-01-02'],utc=True),'value':[1,2,3]})
        clean,n=deduplicate(frame)
        self.assertEqual(n,1);self.assertEqual(clean.value.tolist(),[3])

    def test_gie_no_data_and_null_are_not_zeros_estimates_are_retained(self):
        rows=[{'gasDayStart':f'2025-01-0{i}','status':s,'sendOut':v} for i,s,v in [(1,'C','0'),(2,'N','0'),(3,'E','2.5'),(4,'C','-')]]
        frame,stats=parse_gie(rows,'sendOut','2025-01-01T00:00Z','2025-01-05T00:00Z')
        self.assertEqual(frame.value.tolist(),[0,2.5]);self.assertEqual(stats['no_data_reports_omitted'],1)
        self.assertEqual(stats['missing_values_omitted'],1)

    def test_gie_nested_lng_units_not_confused(self):
        rows=[{'gasDayStart':'2025-01-01','status':'C','inventory':{'lng':'12','gwh':'81'}}]
        self.assertEqual(parse_gie(rows,'inventory.lng','2025-01-01T00:00Z','2025-01-02T00:00Z')[0].value.iloc[0],12)
        self.assertEqual(parse_gie(rows,'inventory.gwh','2025-01-01T00:00Z','2025-01-02T00:00Z')[0].value.iloc[0],81)

    def test_post_brexit_reporting_group_is_not_a_new_country(self):
        self.assertEqual(gie_country_code('GB*'),'GB')
        self.assertEqual(gie_country_code('UK'),'GB')
        self.assertEqual(gie_country_code('UA'),'UA')

    def test_empty_gie_page_retries_entire_scope_in_reverse_not_partial_merge(self):
        collector=object.__new__(Collector);collector.gie_recoveries=[]
        collector._gie_pages_once=Mock(side_effect=[ValueError('GIE empty page before declared end'),([{'value':5}],['digest'])])
        self.assertEqual(collector.gie_pages('agsi',{'country':'DE'})[0],[{'value':5}])
        self.assertEqual(collector._gie_pages_once.call_args.args[1],{'country':'DE','reverse':'true'})
        self.assertEqual(len(collector.gie_recoveries),1)

    def test_empty_last_page_is_not_silently_accepted_as_complete(self):
        collector=object.__new__(Collector);collector.fetch=Mock()
        collector.fetch.get.side_effect=[(json.dumps({'last_page':2,'data':[{'value':5}]}).encode(),'one'),(json.dumps({'last_page':2,'data':[]}).encode(),'two')]
        with self.assertRaisesRegex(ValueError,'empty page'):
            collector._gie_pages_once('agsi',{})

    def test_credentials_excluded_from_receipts_and_url_cache(self):
        with TemporaryDirectory() as folder:
            fetch=SafeFetch(root=folder,keys={'entsoe':'secret-test-entsoe','gie':'secret-test-gie'})
            response=Mock(status_code=200,content=xml([(1,5)]),headers={'Content-Type':'text/xml'})
            session=Mock();session.get.return_value=response;fetch.local.session=session
            fetch.get('https://web-api.tp.entsoe.eu/api',{'documentType':'A65'},provider='entsoe')
            self.assertEqual(session.get.call_args.kwargs['params']['securityToken'],'secret-test-entsoe')
            for path in Path(folder).rglob('*'):
                if path.is_file():self.assertNotIn(b'secret-test',path.read_bytes())

    def test_reflected_credential_response_not_archived(self):
        with TemporaryDirectory() as folder:
            fetch=SafeFetch(root=folder,keys={'entsoe':'secret-test-entsoe','gie':'secret-test-gie'})
            response=Mock(status_code=200,content=b'bad secret-test-entsoe',headers={})
            session=Mock();session.get.return_value=response;fetch.local.session=session
            with self.assertRaises(AccessError):fetch.get('https://web-api.tp.entsoe.eu/api',provider='entsoe')
            self.assertEqual(list(Path(folder).rglob('*.bin')),[])

    def test_access_denied_json_is_not_treated_as_data(self):
        with TemporaryDirectory() as folder:
            fetch=SafeFetch(root=folder,keys={'entsoe':'secret-test-entsoe','gie':'secret-test-gie'})
            response=Mock(status_code=200,content=json.dumps({'error':'access denied','message':'Invalid or missing API key'}).encode(),headers={})
            session=Mock();session.get.return_value=response;fetch.local.session=session
            with self.assertRaises(AccessError):fetch.get('https://agsi.gie.eu/api',provider='gie')


if __name__=='__main__':unittest.main()
