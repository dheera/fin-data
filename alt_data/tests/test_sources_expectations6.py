import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sources_expectations6 import index_records,LOCATIONS,decode_points,ec
import numpy as np

class ForecastParsers(unittest.TestCase):
    def test_select_six_hour_precipitation_not_total_accumulation(self):
        raw=b'1:0:d=2025100100:TMP:2 m above ground:24 hour fcst:\n2:100:d=2025100100:APCP:surface:0-1 day acc fcst:\n3:200:d=2025100100:APCP:surface:18-24 hour acc fcst:\n4:300:d=2025100100:UGRD:10 m above ground:24 hour fcst:\n5:400:d=2025100100:VGRD:10 m above ground:24 hour fcst:\n6:500:d=2025100100:DSWRF:surface:18-24 hour ave fcst:\n7:600:d=2025100100:FOO:surface:24 hour fcst:\n'
        r=index_records(raw,'20251001',24);self.assertEqual(r['precipitation_6h'],(200,299));self.assertEqual(r['solar_radiation_6h'],(500,599))
        with self.assertRaises(ValueError):index_records(raw,'20251002',24)
    def test_location_cells_are_unique_and_all_target_countries_present(self):
        self.assertEqual({x[0] for x in LOCATIONS},{'CN','IN','KR','ID','MX'})
        self.assertEqual(len({(x[2],x[3]) for x in LOCATIONS}),len(LOCATIONS))
    def test_grib_grid_decoding_and_initialization_guard(self):
        g=ec.codes_grib_new_from_samples('regular_ll_sfc_grib2')
        try:
            for k,v in {'paramId':167,'Ni':360,'Nj':181,'latitudeOfFirstGridPointInDegrees':90.,'longitudeOfFirstGridPointInDegrees':0.,'latitudeOfLastGridPointInDegrees':-90.,'longitudeOfLastGridPointInDegrees':359.,'iDirectionIncrementInDegrees':1.,'jDirectionIncrementInDegrees':1.,'dataDate':20251001,'dataTime':0,'forecastTime':24}.items():ec.codes_set(g,k,v)
            ec.codes_set_values(g,np.arange(360*181,dtype=float));raw=ec.codes_get_message(g)
        finally:ec.codes_release(g)
        expected=[float((90-lat)*360+lon) for _,_,lat,lon,_ in LOCATIONS]
        self.assertEqual(decode_points(raw,'20251001',24,'temperature_2m'),expected)
        with self.assertRaisesRegex(ValueError,'initialization'):decode_points(raw,'20251002',24,'temperature_2m')
        with self.assertRaisesRegex(ValueError,'window'):decode_points(raw,'20251001',72,'temperature_2m')
        with self.assertRaisesRegex(ValueError,'variable'):decode_points(raw,'20251001',24,'wind_u_10m')

if __name__=='__main__':unittest.main()
