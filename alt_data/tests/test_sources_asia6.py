import json
import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
import sources_asia6 as s


class AsiaParserTests(unittest.TestCase):
    def row(self, **overrides):
        return dict(ROWSTATUS='0',VARNAME='铜$$Copper',VARID='cu_f',WHTYPE='1',
                    REGNAME='上海$$Shanghai',WHABBRNAME='测试仓库$$Test',WGHTUNIT='2',
                    WRTWGHTS=25,SPOTWGHTS=50,WHSTOCKS=75,**overrides)

    def report(self, rows, date='20240102'):
        return json.dumps(dict(o_tradingday=date,o_cursor=rows,update_date='20240724 17:00:00')).encode()

    def test_shfe_daily_keeps_zero_and_chinese_identity(self):
        row=self.row();row['WRTWGHTS']=0
        frame,times=s.parse_shfe_json(self.report([row]),'2024-01-02')
        self.assertEqual(frame.iloc[0].value,0)
        self.assertEqual(frame.iloc[0]['key'],'shfe:inventory:铜:1:上海:测试仓库:on_warrant')
        self.assertEqual(frame.iloc[0].unit,'tonnes')
        self.assertEqual(times['update_date'],'20240724 17:00:00')

    def test_shfe_date_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError,'date mismatch'):
            s.parse_shfe_json(self.report([self.row()]),'2024-01-03')

    def test_shfe_omits_totals_foreign_regions_and_missing(self):
        a=self.row();a['ROWSTATUS']='2'
        b=self.row();b['REGNAME']='新加坡$$Singapore'
        c=self.row();c['WRTWGHTS']='--'
        self.assertTrue(s.parse_shfe_json(self.report([a,b,c]),'2024-01-02')[0].empty)

    def test_shfe_weekly_filters_factory_and_impossible_spot_placeholder(self):
        a=self.row();a['SPOTWGHTS']=0
        b=self.row();b['WHTYPE']='2'
        f,_=s.parse_shfe_json(self.report([a,b]),'2024-01-02',weekly=True)
        self.assertEqual(f.metric.tolist(),['available_warrant_capacity'])
        self.assertEqual(f.value.tolist(),[75])

    def test_shfe_unknown_unit_and_negative_rejected(self):
        a=self.row();a['WGHTUNIT']='99'
        with self.assertRaises(KeyError):s.parse_shfe_json(self.report([a]),'2024-01-02')
        a=self.row();a['WRTWGHTS']=-2
        with self.assertRaises(ValueError):s.parse_shfe_json(self.report([a]),'2024-01-02')

    def test_shfe_html_rowspan_current_week_and_units(self):
        raw="""<table class="content_intro"><tr><td>2026-10-16</td></tr></table>
        <table class="el-table_table"><thead><tr><th>地区</th><th>仓库</th><th>前库存</th><th>前仓单</th><th>本库存</th><th>本仓单</th><th>变化</th><th>变化</th><th>前可用</th><th>本可用</th><th>变化</th></tr></thead>
        <tbody><tr class="special_row_type"><td colspan="10">白银</td><td>单位：千克</td></tr>
        <tr><td rowspan="2">上海</td><td>仓库一</td><td>80</td><td>30</td><td>100</td><td>40</td><td>20</td><td>10</td><td>900</td><td>890</td><td>-10</td></tr>
        <tr><td>仓库二</td><td>10</td><td>10</td><td>20</td><td>20</td><td>10</td><td>10</td><td>100</td><td>90</td><td>-10</td></tr></tbody></table>""".encode()
        frame,_=s.parse_shfe_html(raw,'2026-10-16',weekly=True)
        self.assertEqual(set(frame.unit),{'kg'})
        self.assertEqual(set(frame.region),{'上海'})
        self.assertEqual(frame.loc[frame.metric.eq('deliverable_inventory'),'value'].tolist(),[100,20])
        self.assertEqual(frame.loc[frame.metric.eq('available_warrant_capacity'),'value'].tolist(),[890,90])

    def test_shfe_eight_column_warrant_only_warehouse_has_capacity_not_spot(self):
        raw="""<table class="content_intro"><tr><td>2026-10-16</td></tr></table>
        <table class="el-table_table"><thead><tr><th>地区</th><th>仓库</th><th>上周期货</th><th>本周期货</th><th>增减</th><th>上周可用</th><th>本周可用</th><th>增减</th></tr></thead>
        <tbody><tr class="special_row_type"><td colspan="7">不锈钢(仓库)</td><td>单位：吨</td></tr>
        <tr><td>江苏</td><td>仓库一</td><td>100</td><td>80</td><td>-20</td><td>900</td><td>920</td><td>20</td></tr></tbody></table>""".encode()
        f,_=s.parse_shfe_html(raw,'2026-10-16',weekly=True)
        self.assertEqual(f.metric.tolist(),['available_warrant_capacity'])
        self.assertEqual(f.value.tolist(),[920])

    def test_shared_capacity_alias_requires_same_facility_full_dates_and_conversion(self):
        rows=[]
        for i,day in enumerate(pd.date_range('2024-01-05',periods=30,freq='W-FRI',tz='UTC')):
            for c,value in [('铜',300+i),('铝',round((300+i)/3)),('锌',500+i)]:
                rows.append(dict(key=c,date=day,value=value,metric='available_warrant_capacity',unit='tonnes',region='上海',warehouse='仓库',warehouse_type='1',commodity=c))
        data=pd.DataFrame(rows);aliases=s.shared_capacity_aliases(data)
        self.assertEqual([a['excluded_identity'] for a in aliases],['铝'])
        changed=data.copy();changed.loc[changed.key.eq('铝').idxmax(),'value']+=5
        self.assertEqual(s.shared_capacity_aliases(changed),[])
        self.assertEqual(s.shared_capacity_aliases(data.iloc[1:]),[])
        constant=data.copy();constant['value']=0
        self.assertEqual(s.shared_capacity_aliases(constant),[])

    def test_kpx_missing_minimum_is_not_zero_demand(self):
        raw=b'chartData.push({"Date":"2024/01/01","Value":100,"Value2":80,"Value3":70,"Value5":0});'
        frame=s.parse_kpx(raw,'2024-01-01')
        self.assertTrue(pd.isna(frame.iloc[0].Value5))
        self.assertEqual(frame.iloc[0].Value3,70)
        with self.assertRaises(ValueError):s.parse_kpx(raw.replace(b'"Value3":70',b'"Value3":0'),'2024-01-01')

    def test_kofia_native_null_and_zero_distinguished(self):
        raw=json.dumps({'ds1':[{'TMPV1':'20240102','TMPV2':0,'TMPV3':None}]}).encode()
        frame=s.parse_kofia(raw,[2,3],'2024-01-01','2024-12-31')
        self.assertEqual(frame.iloc[0].TMPV2,0)
        self.assertTrue(pd.isna(frame.iloc[0].TMPV3))

    def test_kofia_cross_sections_dates_negative_counts_rejected(self):
        base={'TMPV1':'20240102','TMPV2':3}
        cases=[[base,base],[dict(base,TMPV1='202401')],[dict(base,TMPV1='20250101')],[dict(base,TMPV2=-1)],[dict(base,TMPV2=.2)]]
        for rows in cases:
            with self.subTest(rows=rows),self.assertRaises(ValueError):
                s.parse_kofia(json.dumps({'ds1':rows}).encode(),[2],'2024-01-01','2024-12-31',counts=True)

    def test_signed_reported_flow_adjustments_preserved(self):
        raw=json.dumps({'ds1':[{'TMPV1':'20240102','TMPV5':-7950}]}).encode()
        frame=s.parse_kofia(raw,[5],'2024-01-01','2024-12-31',signed_components=True)
        self.assertEqual(frame.iloc[0].TMPV5,-7950)

    def test_access_denial_stops_host_without_retry(self):
        import requests,tempfile
        response=requests.Response();response.status_code=429
        error=requests.HTTPError(response=response)
        with tempfile.TemporaryDirectory() as root,patch.object(s.requests,'get',side_effect=error) as get:
            f=s.AsiaFetcher(root=root)
            with self.assertRaises(requests.HTTPError):f.get('https://example.org/query')
            with self.assertRaises(s.Omission):f.get('https://example.org/other')
            self.assertEqual(get.call_count,1)

    def test_kofia_field_mapping_uses_primary_header_ids(self):
        raw=json.dumps({'dsGrid':[{'HEADER_ID':'G001','HEADER_NM':'융자'},
             {'HEADER_ID':'H003','HEADER_NM':'유가증권','PARENT_HDRID':'G001','HEADER_TYP':'N','HIDE_YN':'N'}],
             'dsGridInfo':[{'SERVICE_NM':'credit'}]}).encode()
        labels,_=s.kofia_headers(raw,[3])
        self.assertEqual(labels,{3:'융자: 유가증권'})
        with self.assertRaises(KeyError):s.kofia_headers(raw,[4])

    def test_conflicting_duplicates_not_silently_last_wins(self):
        with self.assertRaisesRegex(ValueError,'Conflicting'):
            s.clean(['2024-01-01','2024-01-01'],[1,2])

    def test_refresh_pins_published_ids_and_cadence(self):
        old={'ids':set(),'identities':set(),'hashes':set(),'pinned':{'known':'weekly'}}
        frame=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=200,tz='UTC'),'value':np.arange(200,dtype=float)})
        with patch.object(s,'baseline',return_value=old):
            with self.assertRaises(s.Omission):s.save('new',frame,dataset_identity='x',frequency='daily')
            with self.assertRaises(s.Omission):s.save('known',frame,dataset_identity='x',frequency='daily')


if __name__=='__main__':unittest.main()
