import csv
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import sources_corporate11 as s


def workbook(rows):
    b=io.BytesIO()
    pd.DataFrame(rows).to_excel(b,index=False,header=False)
    return b.getvalue()


class MofTests(unittest.TestCase):
    def fixture(self, period='2013．12．29～1．4', net=3):
        values=[10,7,net,20,5,15,18,4,3,1,19]*2
        b=io.StringIO();w=csv.writer(b)
        w.writerow(['International Transactions in Securities show net acquisition with a plus sign'])
        w.writerow([period]+values)
        return b.getvalue().encode('cp932')

    def test_cross_year_period_end_and_backcast_sign(self):
        f=s.parse_mof(self.fixture())
        self.assertEqual(f.date.iloc[0],pd.Timestamp('2014-01-04',tz='UTC'))
        self.assertEqual(f.period_start.iloc[0],pd.Timestamp('2013-12-29',tz='UTC'))
        self.assertEqual(f.c3.iloc[0],3)
        pre=s.parse_mof(self.fixture('2005．1．2～1．8'))
        self.assertEqual(pre.c3.iloc[0],3)  # no erroneous old-page sign flip

    def test_net_sign_or_period_change_fails(self):
        with self.assertRaisesRegex(ValueError,'inconsistency'):s.parse_mof(self.fixture(net=-3))
        with self.assertRaisesRegex(ValueError,'weekly period'):s.parse_mof(self.fixture('2025．1．1～1．8'))

    def test_explicit_cross_year_and_all_date_looking_rows_accounted_for(self):
        first=self.fixture('2006．12．31～ 2007．1．6')
        second=self.fixture('2007．1．7～1．13').decode('cp932').splitlines()[-1]
        combined=first+second.encode('cp932')+b'\n'
        frame=s.parse_mof(combined)
        self.assertEqual(len(frame),2)
        self.assertEqual(frame.date.tolist(),[pd.Timestamp('2007-01-06',tz='UTC'),pd.Timestamp('2007-01-13',tz='UTC')])
        with self.assertRaisesRegex(ValueError,'Unparsed MOF date-looking'):
            s.parse_mof(combined+b'2007 UNKNOWN SOURCE DATE,1,2,3\n')


class JpxTests(unittest.TestCase):
    def test_identical_repeat_removed_missing_not_zero(self):
        day=pd.Timestamp('2010-01-29')
        f,_=s.date_rows(workbook([['header','shares','value'],[day,10,'-'],[day,10,'-']]))
        self.assertEqual(len(f),1)
        self.assertTrue(pd.isna(f.iloc[0,2]))

    def test_conflicting_period_rejected(self):
        day=pd.Timestamp('2010-01-29')
        with self.assertRaisesRegex(ValueError,'conflicting duplicate'):
            s.date_rows(workbook([[day,10],[day,11]]))

    def test_missing_premium_is_not_zero_and_alphanumeric_code_preserved(self):
        f=s.parse_jpx_premiums(workbook([
            ['charge per share per day',None,None,None,None,None,None],
            ['20261020','141A','name','TSE','0','7.4','*****'],
            ['20261020','0050','fund','TSE','100','6','0']]))
        self.assertEqual(f.stock_code.tolist(),['141A','0050'])
        self.assertTrue(pd.isna(f.premium_jpy_per_share_day.iloc[0]))
        self.assertEqual(f.premium_jpy_per_share_day.iloc[1],0)

    def test_designated_issue_dates_codes_missing_ratio_and_flags(self):
        date_row=['']*23;date_row[1]='2026/10/20';date_row[21]='2026/10/21'
        row=['B','規','株','Example','Growth','貸','153A0','JP3205750007',100,-5,'*',200,10,2,50,0,0,100,-5,100,0,100,10]
        f=s.parse_jpx_designated(workbook([date_row,row]))
        self.assertEqual(f.application_date.iloc[0],'2026-10-20')
        self.assertEqual(f.publication_date_source.iloc[0],'2026-10-21')
        self.assertEqual(f.issue_code_original.iloc[0],'153A0')
        self.assertTrue(f.restricted_issue.iloc[0]);self.assertTrue(f.lending_restriction_flag.iloc[0])
        self.assertTrue(pd.isna(f.short_percent_of_listed.iloc[0]))
        self.assertEqual(f.short_change_shares.iloc[0],-5)


class DartTests(unittest.TestCase):
    def test_local_filing_date_not_fabricated_release_time_and_revision_flags(self):
        row={'rcept_no':'20250103000001','rcept_dt':'20250103','report_nm':'[기재정정]주요사항보고서','rm':'UW'}
        out=s.normalize_disclosure(row,'005930','electronics','a'*64,'2026-01-01T00:00:00Z')
        self.assertEqual(out['filing_date'],'2025-01-03')
        self.assertIsNone(out['historical_release_time_utc'])
        self.assertTrue(out['is_amendment'])
        self.assertTrue(out['later_correction_flag'])
        self.assertTrue(out['withdrawn_flag'])
        self.assertEqual(json.loads(out['source_record_json']),row)

    def test_sparse_event_and_exact_amount_currency_preserved(self):
        source={'rcept_no':'20250103000001','corp_code':'00126380','bd_fta':'1,234,567','ovis_fta_crn':'USD','bd_mtd':'2028년 01월 03일'}
        rows=s.event_rows('cvbdIsDecsn',[source],'005930','electronics','a'*64,'2026-01-01T00:00:00Z',{})
        self.assertEqual(len(rows),1)
        self.assertIsNone(rows[0]['filing_date'])
        self.assertEqual(json.loads(rows[0]['source_record_json']),source)

    def test_numeric_values_do_not_guess_date_currency_or_missing_marker(self):
        self.assertEqual(s.parse_numeric_original('1,234.50'),1234.5)
        self.assertEqual(s.parse_numeric_original('-5'),-5)
        for value in ['-','*****','2025-01-03','100 USD','5%','1억원','NaN']:
            self.assertIsNone(s.parse_numeric_original(value))

    def test_auth_query_is_never_archived_and_raw_echo_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            fetch=s.SafeFetch.__new__(s.SafeFetch)
            fetch.root=Path(directory);fetch.refresh=False;fetch.key='private-sentinel-token'
            fetch.local=threading.local();fetch.lock=threading.Lock();fetch.last=0.;fetch.calls=0
            session=Mock();fetch.local.session=session
            response=Mock(status_code=200,content=b'{"status":"013"}',headers={})
            session.get.return_value=response
            _,_,_=fetch.get(s.DART+'list.json',{'corp_code':'00126380'},auth=True)
            self.assertEqual(session.get.call_args.kwargs['params']['crtfc_key'],fetch.key)
            for p in Path(directory).rglob('*'):
                if p.is_file():self.assertNotIn(fetch.key.encode(),p.read_bytes())
            self.assertIn('REDACTED',next((Path(directory)/'archive/requests').glob('*.json')).read_text())
            response.content=fetch.key.encode()
            with self.assertRaisesRegex(RuntimeError,'containing credential'):
                fetch.get(s.DART+'list.json',{'corp_code':'00000001'},auth=True)


if __name__=='__main__':unittest.main()
