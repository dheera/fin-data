import copy
from pathlib import Path
import tempfile
import unittest
import pandas as pd
import pyarrow.parquet as pq
from collect import SCHEMA, memory_date, parse_memory, parse_port, persist, row

FIXTURES = Path(__file__).parent / 'fixtures'

class ParserTests(unittest.TestCase):
    def test_memory_units_and_baskets(self):
        rows = parse_memory((FIXTURES/'memory.html').read_bytes())
        indexed = {r['series_id']:r for r in rows}
        self.assertEqual(len(rows), 32)
        self.assertAlmostEqual(indexed['ram_udimm_16gb_mean_usd_per_gb']['value'], (239+165.9)/32)
        self.assertAlmostEqual(indexed['ram_dram_16gbit_mean_usd_per_gb']['value'], (60.633+83.414)/4)
        self.assertAlmostEqual(indexed['ssd_pcie4_1tb_fixed_mean_usd_per_gb']['value'], (133+249.99+186.97)/3000)
        self.assertEqual(indexed['ssd_street_mean_usd_per_gb']['sample_size'],5)
        self.assertEqual(indexed['ram_udimm_16gb_mean_usd_per_gb']['frequency'],'weekly')
        self.assertEqual(indexed['ssd_street_mean_usd_per_gb']['timestamp'], pd.Timestamp('2026-10-16T02:30:00Z'))

    def test_memory_missing_constituent_rejected(self):
        content=(FIXTURES/'memory.html').read_text().replace('990 Pro','Unexpected model')
        with self.assertRaisesRegex(ValueError,'Fixed 1TB'): parse_memory(content)

    def test_memory_date_missing_rejected(self):
        with self.assertRaises(ValueError): memory_date('LastUpdate: unknown')

    def test_port_column_dates_and_values(self):
        rows=parse_port((FIXTURES/'port.txt').read_text())
        rail=[r for r in rows if r['series_id']=='port_la_rail_dwell_days']
        self.assertEqual([r['value'] for r in rail],[5.1,5.2])
        self.assertEqual([r['timestamp'].strftime('%Y-%m-%d') for r in rail],['2026-10-14','2026-10-15'])
        self.assertEqual(len(rows),10)

    def test_port_layout_change_rejected(self):
        text=(FIXTURES/'port.txt').read_text().replace('5.2 days','N/A')
        with self.assertRaisesRegex(ValueError,'columns changed'):parse_port(text)

class StorageTests(unittest.TestCase):
    def sample(self):
        return row('test','2026-01-01',2,unit='USD',frequency='daily',source='test',source_url='https://example.com',
                   category='test',title='Test',geography='US',retrieved_at=pd.Timestamp('2026-02-01T00:00:00Z'))

    def test_reruns_and_revisions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            r=self.sample()
            persist(root,[r])
            r2=copy.deepcopy(r);r2['retrieved_at']=pd.Timestamp('2026-02-02T00:00:00Z');r2['raw_sha256']='new-download'
            self.assertEqual(persist(root,[r2])[0]['changed'],0)
            r3=copy.deepcopy(r2);r3['value']=3
            self.assertEqual(persist(root,[r3])[0]['changed'],1)
            latest=pq.read_table(root/'archive/legacy_observations/test.parquet')
            history=pq.read_table(root/'archive/revisions/test.parquet')
            self.assertEqual(latest.schema,SCHEMA)
            self.assertEqual(latest.num_rows,1)
            self.assertEqual(history.num_rows,2)
            self.assertEqual(latest['value'].to_pylist(),[3])
            self.assertEqual(history['retrieved_at'].to_pylist()[0],r['retrieved_at'])
            # Latest view can be reconstructed after an interrupted write.
            (root/'archive/legacy_observations/test.parquet').unlink()
            persist(root,[r3])
            self.assertTrue((root/'archive/legacy_observations/test.parquet').exists())

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            row('test','2020-01-01',float('nan'),unit='x',frequency='daily',source='x',source_url='x',category='x',title='x',geography='x')

    def test_duplicate_timestamp_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,'Duplicate'):persist(Path(tmp),[self.sample(),self.sample()])

if __name__=='__main__':unittest.main()
