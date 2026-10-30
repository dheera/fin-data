import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
from sources_forecast7 import index_records,decode_points,same_target_revisions,ec,LOCATIONS

class IntradayForecast(unittest.TestCase):
    def test_first_six_hour_duplicate_and_cycle_guard(self):
        raw=b'1:0:d=2025100118:TMP:2 m above ground:6 hour fcst:\n2:100:d=2025100118:APCP:surface:0-6 hour acc fcst:\n3:200:d=2025100118:APCP:surface:0-6 hour acc fcst:\n4:300:d=2025100118:UGRD:10 m above ground:6 hour fcst:\n5:400:d=2025100118:VGRD:10 m above ground:6 hour fcst:\n6:500:d=2025100118:DSWRF:surface:0-6 hour ave fcst:\n7:600:d=2025100118:FOO:surface:6 hour fcst:\n'
        out=index_records(raw,'20251001',18,6)
        self.assertEqual(out['precipitation_6h_duplicate'],(200,299))
        with self.assertRaises(ValueError):index_records(raw,'20251001',12,6)

    def test_decode_nonmidnight_and_wrong_cycle(self):
        g=ec.codes_grib_new_from_samples('regular_ll_sfc_grib2')
        try:
            for k,v in {'paramId':167,'Ni':360,'Nj':181,'latitudeOfFirstGridPointInDegrees':90.,'longitudeOfFirstGridPointInDegrees':0.,'latitudeOfLastGridPointInDegrees':-90.,'longitudeOfLastGridPointInDegrees':359.,'iDirectionIncrementInDegrees':1.,'jDirectionIncrementInDegrees':1.,'dataDate':20251001,'dataTime':1800,'forecastTime':6}.items():ec.codes_set(g,k,v)
            ec.codes_set_values(g,np.arange(360*181,dtype=float));raw=ec.codes_get_message(g)
        finally:ec.codes_release(g)
        self.assertEqual(decode_points(raw,'20251001',18,6,'temperature_2m'),[float((90-lat)*360+lon) for _,_,lat,lon,_ in LOCATIONS])
        with self.assertRaisesRegex(ValueError,'initialization'):decode_points(raw,'20251001',12,6,'temperature_2m')

    def test_no_target_rollover_or_missing_predecessor_revision(self):
        init=pd.to_datetime(['2025-10-01T00:00Z','2025-10-01T06:00Z','2025-10-01T18:00Z','2025-10-02T00:00Z','2025-10-02T06:00Z'],utc=True)
        targets=pd.to_datetime(['2025-10-02']*3+['2025-10-03']*2,utc=True)
        p=pd.DataFrame(dict(initialization=init,valid_time=targets,value=[10.,14.,21.,100.,97.],source_object_last_modified=init+pd.Timedelta(hours=4)))
        r=same_target_revisions(p)
        self.assertEqual(r.value.tolist(),[4.,-3.])
        self.assertEqual(r.initialization.tolist(),[init[1],init[4]])
        self.assertEqual(r.previous_initialization.tolist(),[init[0],init[3]])
        self.assertEqual(r.source_object_last_modified.tolist(),[init[1]+pd.Timedelta(hours=4),init[4]+pd.Timedelta(hours=4)])

if __name__=='__main__':unittest.main()
