import unittest
from tempfile import TemporaryDirectory
from pathlib import Path
import pandas as pd
from sources_grid7 import parse_dataset, outturn_frames, forecast_frames, remit_counts, windows, parquet_panel


class Grid7ParserTests(unittest.TestCase):
    def test_publication_windows_have_no_gap(self):
        w=list(windows('2025-10-01','2025-10-17',7))
        self.assertEqual(len(w),3)
        self.assertEqual(w[0][1],w[1][0]);self.assertEqual(w[1][1],w[2][0])
        self.assertEqual(w[-1][1],pd.Timestamp('2025-10-17',tz='UTC'))

    def test_outturn_uses_earliest_returned_release_not_mean(self):
        rows=[{'publishTime':'2025-10-01T00:30Z','startTime':'2025-10-01T00:00Z','demand':12},
              {'publishTime':'2025-10-01T00:45Z','startTime':'2025-10-01T00:00Z','demand':99}]
        frame=outturn_frames(parse_dataset(rows,'INDO'))['N']
        self.assertEqual(frame.value.tolist(),[12])
        self.assertEqual(frame.publishTime.iloc[0],pd.Timestamp('2025-10-01T00:30Z'))

    def test_conflicting_exact_release_fails(self):
        rows=[{'publishTime':'2025-10-01T00:30Z','startTime':'2025-10-01T00:00Z','demand':12},
              {'publishTime':'2025-10-01T00:30Z','startTime':'2025-10-01T00:00Z','demand':99}]
        with self.assertRaisesRegex(ValueError,'Conflicting'):parse_dataset(rows,'INDO')

    def test_exact_partition_overlap_deduplicated(self):
        row={'publishTime':'2025-10-01T00:30Z','startTime':'2025-10-01T00:00Z','demand':12}
        self.assertEqual(len(parse_dataset([row,row],'INDO')),1)

    def test_all_fuel_zero_feed_omitted_but_individual_zero_kept(self):
        rows=[{'publishTime':'2025-10-01T00:05Z','startTime':'2025-10-01T00:00Z','fuelType':fuel,'generation':0}
              for fuel in ['CCGT','NUCLEAR']]
        rows.extend([{'publishTime':'2025-10-01T00:10Z','startTime':'2025-10-01T00:05Z','fuelType':'CCGT','generation':0},
                     {'publishTime':'2025-10-01T00:10Z','startTime':'2025-10-01T00:05Z','fuelType':'NUCLEAR','generation':1200}])
        f=parse_dataset(rows,'FUELINST')
        self.assertEqual(len(f),2);self.assertEqual(set(f.value),{0,1200})

    def test_zero_demand_omitted_but_zero_margin_retained(self):
        base={'publishTime':'2025-10-01T00:30Z','startTime':'2025-10-01T00:00Z'}
        self.assertTrue(parse_dataset([dict(base,demand=0)],'INDO').empty)
        self.assertEqual(len(parse_dataset([dict(base,margin=0)],'MELNGC')),1)

    def test_partial_generation_feed_gap_preserves_interconnector(self):
        base={'publishTime':'2025-10-01T00:05Z','startTime':'2025-10-01T00:00Z'}
        rows=[dict(base,fuelType=fuel,generation=0) for fuel in ['BIOMASS','CCGT','NUCLEAR','WIND','NPSHYD']]
        rows.append(dict(base,fuelType='INTFR',generation=900))
        f=parse_dataset(rows,'FUELINST')
        self.assertEqual(f.dimension.tolist(),['INTFR']);self.assertEqual(f.value.tolist(),[900])

    def test_forecast_stays_at_own_release_and_next_target(self):
        rows=[{'publishTime':'2025-10-01T00:17Z','startTime':'2025-10-01T06:00Z','demand':10},
              {'publishTime':'2025-10-01T00:17Z','startTime':'2025-10-01T06:30Z','demand':20},
              {'publishTime':'2025-10-01T01:17Z','startTime':'2025-10-01T07:30Z','demand':30}]
        f=forecast_frames(parse_dataset(rows,'NDF'),'NDF')[6]
        self.assertEqual(f.value.tolist(),[20,30]);self.assertEqual(f.lead_seconds.tolist(),[22380,22380])
        self.assertEqual(f.date.iloc[0],pd.Timestamp('2025-10-01T00:17Z'))

    def test_forecast_no_target_outside_tolerance(self):
        rows=[{'publishTime':'2025-10-01T00:17Z','startTime':'2025-10-01T07:00Z','demand':10}]
        self.assertTrue(forecast_frames(parse_dataset(rows,'NDF'),'NDF')[6].empty)

    def test_remit_hour_closes_after_publication_not_event_start(self):
        days=pd.date_range('2025-01-01',periods=91,tz='UTC')
        ids=pd.DataFrame({'id':range(91),'mrid':[str(x) for x in range(91)],'revisionNumber':1,
                          'publishTime':days+pd.Timedelta(minutes=20)})
        f=remit_counts(ids,pd.DataFrame(),'2025-01-01','2025-04-02')['all_messages']
        self.assertEqual(f.iloc[0].date,pd.Timestamp('2025-01-01T01:00Z'));self.assertEqual(f.iloc[0].value,1)
        self.assertEqual(f.value.sum(),91);self.assertEqual(f.iloc[1].value,0)

    def test_remit_boundary_enters_following_hour(self):
        days=pd.date_range('2025-01-01',periods=91,tz='UTC')
        ids=pd.DataFrame({'id':range(91),'mrid':[str(x) for x in range(91)],'revisionNumber':1,
                          'publishTime':days+pd.Timedelta(hours=1)})
        f=remit_counts(ids,pd.DataFrame(),'2025-01-01','2025-04-02')['all_messages']
        self.assertEqual(f.iloc[0].value,0);self.assertEqual(f.iloc[1].value,1)

    def test_remit_missing_details_fails(self):
        ids=pd.DataFrame({'id':[1,2],'mrid':['a','b'],'revisionNumber':[1,2],
                          'publishTime':['2025-01-01T00:20Z','2025-01-02T00:20Z']})
        details=pd.DataFrame({'id':[1],'mrid':['a'],'revisionNumber':[1],'publishTime':['2025-01-01T00:20Z']})
        with self.assertRaisesRegex(ValueError,'Incomplete'):remit_counts(ids,details,'2025-01-01','2025-04-02')

    def test_bounded_refresh_preserves_old_companion_rows(self):
        with TemporaryDirectory() as folder:
            p=Path(folder)/'panel.parquet'
            parquet_panel(p,pd.DataFrame({'id':[1,2],'value':[10,20]}),['id'])
            parquet_panel(p,pd.DataFrame({'id':[2,3],'value':[21,30]}),['id'])
            self.assertEqual(pd.read_parquet(p).to_dict('list'),{'id':[1,2,3],'value':[10,21,30]})


if __name__=='__main__':unittest.main()
