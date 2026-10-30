import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import h5py
import numpy as np

from sources_smap11 import (PrivateFetch, decode_utc, parse_subset, static_grid,
    valid_retrievals, subset_url)


def example_swath(*, flags=None, values=None, no_observations=False, bad_units=False, malformed_time=False):
    out=io.BytesIO()
    with h5py.File(out,'w') as f:
        for pas in ['AM','PM']:
            g=f.create_group('Soil_Moisture_Retrieval_Data_'+pas);suffix='_pm' if pas=='PM' else ''
            lat=np.array([[2,2],[1,1]],dtype='float32');lon=np.array([[1,2],[1,2]],dtype='float32')
            fields={'latitude':lat,'longitude':lon,
                'soil_moisture':np.array(values if values is not None else [[.2,.3],[.61,.9]],dtype='float32'),
                'soil_moisture_error':np.full((2,2),.01,dtype='float32'),
                'retrieval_qual_flag':np.array(flags if flags is not None else [[0,8],[0,1]],dtype='uint16'),
                'surface_flag':np.zeros((2,2),dtype='uint16'),
                'tb_time_seconds':np.ones((2,2)),
                'tb_time_utc':np.array([['2025-01-01T06:01:00.000Z','2025-01-01T06:02:00.000Z'],
                    ['2025-01-01T06:03:00.000Z','2025-01-01T06:04:00.000Z']],dtype='S24')}
            if no_observations:
                fields['latitude'].fill(-9999);fields['longitude'].fill(-9999)
                fields['soil_moisture'].fill(-9999);fields['retrieval_qual_flag'].fill(65534)
                fields['tb_time_utc'][:]=b'N/A'
            if malformed_time:fields['tb_time_utc'][0,0]=b'2025-01-01T06:01:00.***Z'
            for name,data in fields.items():g.create_dataset(name+suffix,data=data)
            g['soil_moisture'+suffix].attrs['units']='percent' if bad_units else 'cm**3/cm**3'
    return out.getvalue()


REGION=[{'id':'test','bbox':[0,0,3,3]}]
GRANULE={'id':'test-granule','title':'test','time_start':'2025-01-01T00:00:00Z'}
GRID=(np.array([[2,2],[1,1]],dtype='float32'),np.array([[1,2],[1,2]],dtype='float32'))


class Smap11(unittest.TestCase):
    def test_qa_recommended_flags_and_high_porosity_values(self):
        rows=parse_subset(example_swath(),GRANULE,GRID,REGION)
        self.assertEqual(len(rows),2)
        r=rows[0]
        self.assertEqual(r['valid_pixels'],3)
        self.assertAlmostEqual(r['value'],(.2+.3+.61)/3,places=7)
        self.assertEqual(r['above_nominal_attribute_max_pixels'],1)
        self.assertEqual(r['valid_fraction'],.75)
        self.assertTrue(r['eligible_region_observation'])

    def test_daily_product_label_is_separate_from_real_pixel_acquisition(self):
        r=parse_subset(example_swath(),GRANULE,GRID,REGION)[0]
        self.assertEqual(r['product_date'],'2025-01-01T00:00:00+00:00')
        self.assertEqual(r['observation_time_min'],'2025-01-01T06:01:00+00:00')
        self.assertEqual(r['observation_time_max'],'2025-01-01T06:03:00+00:00')

    def test_unobserved_region_keeps_fixed_denominator_and_no_fake_zero(self):
        r=parse_subset(example_swath(no_observations=True),GRANULE,GRID,REGION)[0]
        self.assertEqual(r['region_grid_cells'],4)
        self.assertEqual(r['valid_pixels'],0)
        self.assertEqual(r['valid_fraction'],0)
        self.assertIsNone(r['value'])
        self.assertFalse(r['eligible_region_observation'])

    def test_too_few_good_cells_remain_quality_only(self):
        r=parse_subset(example_swath(flags=[[0,8],[1,65534]]),GRANULE,GRID,REGION)[0]
        self.assertEqual(r['valid_pixels'],2)
        self.assertFalse(r['eligible_region_observation'])

    def test_unsupported_units_and_shifted_geolocation_fail(self):
        with self.assertRaisesRegex(ValueError,'units'):
            parse_subset(example_swath(bad_units=True),GRANULE,GRID,REGION)
        with self.assertRaisesRegex(ValueError,'geolocation'):
            parse_subset(example_swath(),GRANULE,(GRID[0]+1,GRID[1]),REGION)

    def test_netcdf_character_array_and_native_hdf_strings_agree(self):
        a=np.array([['2025-01-01T06:00:00.000Z','N/A                    ']],dtype='S25')
        chars=np.ascontiguousarray(a).view('S1').reshape(1,2,25)
        np.testing.assert_array_equal(decode_utc(a),decode_utc(chars))

    def test_fill_values_and_bad_time_are_not_valid_retrievals(self):
        v=np.array([[-9999,.4,.2,1.1]])
        q=np.array([[0,0,0,0]],dtype='uint16')
        t=np.array([['2025-01-01T00:00:00Z','N/A','2025-01-01T00:00:00Z','2025-01-01T00:00:00Z']])
        self.assertEqual(valid_retrievals(v,q,t).tolist(),[[False,False,True,False]])

    def test_literal_missing_fraction_omits_only_that_pixel(self):
        row=parse_subset(example_swath(flags=[[0,8],[0,0]],malformed_time=True),GRANULE,GRID,REGION)[0]
        self.assertEqual(row['malformed_or_missing_utc_pixels'],1)
        self.assertEqual(row['valid_pixels'],3)
        self.assertTrue(row['eligible_region_observation'])
        self.assertAlmostEqual(row['value'],(.3+.61+.9)/3,places=7)

    def test_subset_uses_source_url_and_keeps_native_utc_variable(self):
        u,p=subset_url({'links':[{'href':'https://opendap.earthdata.nasa.gov/collections/X/granules/file.h5'}]})
        self.assertTrue(u.endswith('file.h5.dap.nc4'))
        self.assertIn('/Soil_Moisture_Retrieval_Data_PM/tb_time_utc_pm[92:328][310:768]',p['dap4.ce'])

    def test_private_auth_not_persisted_or_forwarded_to_distribution(self):
        with TemporaryDirectory() as folder:
            fetch=PrivateFetch(root=folder,token='secret-test-token');session=Mock();fetch.local.session=session
            session.get.return_value=Mock(status_code=303,headers={'Location':'https://example.cloudfront.net/data?Signature=secret-signature'})
            response=Mock(status_code=200,headers={'Content-Type':'application/hdf5'},content=example_swath())
            with patch('sources_smap11.requests.get',return_value=response) as download:
                fetch.get('https://data.nsidc.earthdatacloud.nasa.gov/protected/file.h5')
                self.assertNotIn('headers',download.call_args.kwargs)
            for path in Path(folder).rglob('*'):
                if path.is_file():
                    self.assertNotIn(b'secret-test-token',path.read_bytes())
                    self.assertNotIn(b'secret-signature',path.read_bytes())

    def test_reflected_token_is_rejected_before_archiving(self):
        with TemporaryDirectory() as folder:
            fetch=PrivateFetch(root=folder,token='secret-test-token');session=Mock();fetch.local.session=session
            session.get.return_value=Mock(status_code=200,content=b'secret-test-token',headers={})
            with self.assertRaisesRegex(RuntimeError,'reflected'):
                fetch.get('https://opendap.earthdata.nasa.gov/file')
            self.assertEqual(list(Path(folder).rglob('*.bin')),[])


if __name__=='__main__':unittest.main()
