import unittest
from pathlib import Path
from history_sources import parse_memradar, parse_pmsa, parse_gscpi

FIXTURES=Path(__file__).parent/'fixtures'

class HistoryTests(unittest.TestCase):
    def test_segment_history_and_units(self):
        rows=parse_memradar((FIXTURES/'memradar.csv').read_bytes())
        self.assertEqual(len({r['series_id'] for r in rows}),8)
        ddr4=[r for r in rows if r['series_id']=='memradar_ddr4_median_usd_per_gb']
        self.assertEqual(len(ddr4),83)
        self.assertEqual(str(ddr4[0]['timestamp'].date()),'2019-11-01')
        self.assertEqual(ddr4[0]['unit'],'USD/GB')
        self.assertGreaterEqual(min(r['sample_size'] for r in rows),10)

    def test_pmsa_headerless_first_row_is_data(self):
        rows=parse_pmsa((FIXTURES/'pmsa.tsv').read_bytes())
        self.assertEqual(len(rows),30)
        truck=[r for r in rows if '_truck_' in r['series_id']]
        rail=[r for r in rows if '_rail_' in r['series_id']]
        self.assertEqual(truck[0]['value'],2.87)
        self.assertEqual(rail[0]['value'],5.18)
        self.assertEqual(str(truck[0]['timestamp'].date()),'2025-07-01')

    def test_gscpi_chooses_latest_vintage_not_average(self):
        rows=parse_gscpi(b'Date,Feb-24,Jan-24\n31-Dec-2023,1.2,3.4\n31-Jan-2024,2.3,\n')
        self.assertEqual([r['value'] for r in rows],[1.2,2.3])
        self.assertTrue(all(r['measure_type']=='proxy' for r in rows))
        self.assertIn('Feb-24',rows[0]['methodology'])

if __name__=='__main__':unittest.main()
