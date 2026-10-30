"""Boundary checks for physical-economy source parsing and paging."""
from pathlib import Path
import bz2
import json
import sys
import unittest
from unittest.mock import patch
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sources_activity2 as activity
from sources_activity2 import clean, parse_eia, parse_aemo, parse_euro_csv, arcgis_query


class ActivityExpansionTests(unittest.TestCase):
    def test_eia_keeps_reported_and_utc_end_hour_not_imputed(self):
        raw=b'Balancing Authority,UTC Time at End of Hour,Demand (MW),Demand (MW) (Imputed),Region\nCISO,11/03/2024 8:00:00 AM,100,,CAL\nCISO,11/03/2024 9:00:00 AM,,200,CAL\n'
        frame=parse_eia(raw)
        self.assertNotIn('Demand (MW) (Imputed)',frame)
        out=clean(frame.date,frame['Demand (MW)'])
        self.assertEqual(out.value.tolist(),[100])
        self.assertEqual(out.date.iloc[0],pd.Timestamp('2024-11-03T08:00:00Z'))

    def test_market_clock_is_fixed_aest_and_forecasts_excluded(self):
        raw=b'REGION,SETTLEMENTDATE,TOTALDEMAND,RRP,PERIODTYPE\nNSW1,2025/01/01 00:05:00,20,-5,TRADE\nNSW1,2025/01/01 00:10:00,25,100,FORECAST\n'
        frame=parse_aemo(raw)
        self.assertEqual(len(frame),1)
        self.assertEqual(frame.date.iloc[0],pd.Timestamp('2024-12-31T14:05:00Z'))
        self.assertEqual(frame.RRP.iloc[0],-5)

    def test_source_null_and_duplicate_semantics(self):
        out=clean(['2024-01-01','2024-01-02','2024-01-03','2024-01-03'],[0,None,-3,-3])
        self.assertEqual(out.value.tolist(),[0,-3])
        self.assertEqual(out.date.dt.day.tolist(),[1,3])
        with self.assertRaisesRegex(ValueError,'Conflicting duplicate'):
            clean(['2024-01-01','2024-01-01'],[3,4])

    def test_mixed_source_encoding_preserves_names_and_numbers(self):
        # Official EUROCONTROL 2025 export has UTF-8 Esenboğa and Latin-1 Türkiye.
        raw=b'APT_NAME,STATE_NAME,DLY_APT_ARR_1\nAnkara/ Esenbo\xc4\x9fa,T\xfcrkiye,92\n'
        frame=parse_euro_csv(bz2.compress(raw),True)
        self.assertEqual(frame.APT_NAME.iloc[0],'Ankara/ Esenboğa')
        self.assertEqual(frame.STATE_NAME.iloc[0],'Türkiye')
        self.assertEqual(frame.DLY_APT_ARR_1.iloc[0],92)

    def test_arcgis_obeys_transfer_limit_without_filling_days(self):
        class Client:
            def __init__(self):self.offsets=[]
            def get(self,url,params,**kwargs):
                self.offsets.append(params['resultOffset'])
                if len(self.offsets)==1:
                    p={'features':[{'attributes':{'date':'2024-01-01','import':0}}],'exceededTransferLimit':True}
                else:
                    p={'features':[{'attributes':{'date':'2024-01-03','import':5}}],'exceededTransferLimit':False}
                return json.dumps(p).encode(),'0'*64
        c=Client();frame,hashes=arcgis_query(c,'https://example.invalid',{'where':'1=1'})
        self.assertEqual(c.offsets,[0,1])
        self.assertEqual(frame.date.tolist(),['2024-01-01','2024-01-03'])
        self.assertEqual(frame['import'].tolist(),[0,5])
        self.assertEqual(len(hashes),2)

    def test_unexpected_provider_failure_returns_nonzero(self):
        def broken(fetch): raise RuntimeError('upstream unavailable')
        with patch.dict(activity.COLLECTORS,{'eia':broken},clear=True), \
             patch.object(sys,'argv',['sources_activity2.py','--provider','eia']), \
             patch.object(activity,'atomic_bytes') as write, patch('builtins.print'):
            self.assertEqual(activity.main(),1)
            report=json.loads(write.call_args.args[1])
            self.assertEqual(report['failures'][0]['error'],'upstream unavailable')
        activity.FAILURES.clear()

    def test_arcgis_zero_row_truncation_is_error(self):
        class Client:
            def get(self,*args,**kwargs):
                return b'{"features":[],"exceededTransferLimit":true}','0'*64
        with self.assertRaisesRegex(ValueError,'truncated'):
            arcgis_query(Client(),'https://example.invalid',{})

if __name__=='__main__':unittest.main()
