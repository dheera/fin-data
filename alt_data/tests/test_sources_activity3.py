"""Meaningful boundaries for trip aggregation and expansion identity selection."""
from io import BytesIO
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import sources_activity3 as activity


def trip_bytes(rows):
    x=pd.DataFrame(rows,columns=['tpep_pickup_datetime','PULocationID','DOLocationID','fare_amount'])
    x['tpep_pickup_datetime']=pd.to_datetime(x.tpep_pickup_datetime)
    sink=BytesIO();pq.write_table(pa.Table.from_pandas(x,preserve_index=False),sink)
    return sink.getvalue()

class ActivityThirdExpansionTests(unittest.TestCase):
    def test_six_hour_bins_handle_spring_dst_without_inventing_hours(self):
        raw=trip_bytes([
          ('2024-03-10 01:10',4,5,12.),('2024-03-10 03:10',4,5,-2.),
          ('2024-03-10 07:10',4,6,9.),('2024-02-29 12:00',4,5,1000.)])
        p,d=activity.aggregate_tlc(raw,'2024-03')
        self.assertEqual(p.pickups.tolist(),[2,1])
        self.assertEqual(p.fare_amount.tolist(),[10.,9.])
        self.assertEqual(p.date.tolist(),[pd.Timestamp('2024-03-10 05:00Z'),pd.Timestamp('2024-03-10 10:00Z')])
        self.assertEqual(d.destination_trips.sum(),3)
        self.assertEqual(d.zone.tolist(),[5,6])

    def test_autumn_duplicate_wall_clock_trips_are_events_not_duplicate_observations(self):
        raw=trip_bytes([('2024-11-03 01:30',1,2,5.),('2024-11-03 01:30',1,2,6.),('2024-11-03 06:00',1,2,7.)])
        p,d=activity.aggregate_tlc(raw,'2024-11')
        self.assertEqual(p.pickups.tolist(),[2,1])
        self.assertEqual(p.date.diff().dropna().dt.total_seconds().iloc[0],7*3600)
        self.assertEqual(p.fare_amount.sum(),18.)

    def test_null_fares_stay_missing_and_absent_bins_are_not_filled(self):
        raw=trip_bytes([('2024-01-01 02:00',1,2,None),('2024-01-03 02:00',1,2,4.)])
        p,_=activity.aggregate_tlc(raw,'2024-01')
        self.assertTrue(pd.isna(p.fare_amount.iloc[0]))
        self.assertEqual(len(p),2)
        self.assertEqual(p.pickups.tolist(),[1,1])

    def test_rte_conflicting_dst_timestamps_are_omitted_not_averaged(self):
        dates=['2024-10-27T01:00Z','2024-10-27T01:00Z','2024-10-27T02:00Z','2024-10-27T02:00Z','2024-10-27T02:30Z']
        out,conflicts=activity.clean_rte(dates,[3,4,-5,-5,None])
        self.assertEqual(conflicts,1)
        self.assertEqual(out.value.tolist(),[-5])
        self.assertEqual(out.date.iloc[0],pd.Timestamp('2024-10-27T02:00Z'))

    def test_fixed_selection_does_not_replace_universe_on_refresh(self):
        with tempfile.TemporaryDirectory() as folder,patch.object(activity,'FOLDER',Path(folder)):
            self.assertEqual(activity.selection('x',['b','a','b']),['a','b'])
            self.assertEqual(activity.selection('x',['c','b']),['a','b'])

    def test_existing_identity_rejected_even_when_new_name_and_window(self):
        with patch.object(activity,'BASELINE',{'ids':[],'identities':['existing'],'hashes':[]}),patch.object(activity,'save_candidate') as stage:
            self.assertIsNone(activity.save('renamed',pd.DataFrame({'date':[],'value':[]}),dataset_identity='existing'))
            stage.assert_not_called()

    def test_shorter_single_fuel_history_is_not_counted_again(self):
        old=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=240,tz='UTC'),'value':pd.Series(range(240),dtype=float)})
        short=old.iloc[40:].reset_index(drop=True)
        full_hash=hashlib.sha256(old.date.astype('int64').to_numpy().tobytes()+old.value.to_numpy().tobytes()).hexdigest()
        short_hash=hashlib.sha256(short.date.astype('int64').to_numpy().tobytes()+short.value.to_numpy().tobytes()).hexdigest()
        self.assertNotEqual(full_hash,short_hash)
        with patch.object(activity,'BASELINE',{'ids':['eia930_dopd_generation'],'identities':[],'hashes':[full_hash]}),patch.object(activity,'save_candidate') as stage:
            result=activity.save('eia930_dopd_hydro',short,dataset_identity='EIA930/BALANCE/DOPD/Net Generation (MW) from Hydropower Excluding Pumped Storage',frequency='daily')
            self.assertIsNone(result)
            stage.assert_not_called()
            self.assertIn('eia930_dopd_generation',activity.OMISSIONS[-1]['reason'])

    def test_alias_exclusion_does_not_reject_other_fuels_for_the_same_area(self):
        frame=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=181,tz='UTC'),'value':range(181)})
        with patch.object(activity,'BASELINE',{'ids':['eia930_dopd_generation'],'identities':[],'hashes':[]}),patch.object(activity,'save_candidate',return_value={'rows':181,'observed_days':181}) as stage,patch('builtins.print'):
            activity.save('eia930_dopd_solar',frame,dataset_identity='DOPD distinct solar measurement',frequency='daily')
            stage.assert_called_once()
        activity.SEEN.clear()

    def test_source_parquet_microseconds_are_normalized_before_hashing(self):
        x=pd.DataFrame({'date':pd.date_range('2024-01-01',periods=181,tz='UTC').as_unit('us'),'value':range(181)})
        with patch.object(activity,'BASELINE',{'ids':[],'identities':[],'hashes':[]}),patch.object(activity,'save_candidate',return_value={'rows':181,'observed_days':181}) as stage,patch('builtins.print'):
            activity.save('example',x,dataset_identity='example',frequency='daily')
            self.assertEqual(str(stage.call_args.args[2].date.dtype),'datetime64[ns, UTC]')
        activity.SEEN.clear()

    def test_unexpected_errors_produce_nonzero_status(self):
        def broken(fetch):raise RuntimeError('upstream unavailable')
        with patch.dict(activity.COLLECTORS,{'tlc':broken},clear=True),patch.object(activity,'baseline'),patch.object(activity,'atomic_bytes') as write,patch.object(sys,'argv',['sources_activity3.py','--provider','tlc']),patch('builtins.print'):
            self.assertEqual(activity.main(),1)
            report=json.loads(write.call_args.args[1]);self.assertEqual(report['failures'][0]['error'],'upstream unavailable')
        activity.FAILURES.clear();activity.OMISSIONS.clear()

if __name__=='__main__':unittest.main()
