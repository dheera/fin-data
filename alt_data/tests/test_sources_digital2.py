import json
from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sources_digital2 import parse_pypi, parse_ripe, parse_fda, NPM, CRAN, PYPI, WIKIPEDIA
from sources_digital import NPM_PACKAGES


class AdditionalDigitalSourceTests(unittest.TestCase):
    def test_pypi_does_not_double_count_mirrors(self):
        raw = json.dumps({'data': [
            {'category': 'with_mirrors', 'date': '2026-01-01', 'downloads': 70},
            {'category': 'without_mirrors', 'date': '2026-01-01', 'downloads': 20},
            {'category': 'without_mirrors', 'date': '2026-01-03', 'downloads': 0},
        ]})
        frame = parse_pypi(raw)
        self.assertEqual(frame.to_dict('records'), [
            {'date': '2026-01-01', 'value': 20}, {'date': '2026-01-03', 'value': 0}])

    def test_ripe_rejects_automatic_weekly_resolution(self):
        with self.assertRaisesRegex(ValueError, 'non-daily'):
            parse_ripe(json.dumps({'status': 'ok', 'data': {'resolution': '1w', 'stats': []}}), 'v4_prefixes_ris')

    def test_ripe_preserves_fractional_aggregates_and_omits_sentinel(self):
        raw = json.dumps({'status': 'ok', 'data': {'resolution': '1d', 'stats': [
            {'v4_prefixes_ris': 200.5, 'timeline': [{'starttime': '2026-01-01', 'endtime': '2026-01-01'}]},
            {'v4_prefixes_ris': -1, 'timeline': [{'starttime': '2026-01-02', 'endtime': '2026-01-02'}]},
            {'v4_prefixes_ris': 0, 'timeline': [{'starttime': '2026-01-03', 'endtime': '2026-01-03'}]},
        ]}})
        frame = parse_ripe(raw, 'v4_prefixes_ris')
        self.assertEqual(frame.value.tolist(), [200.5, 0])
        self.assertEqual(frame.date.tolist(), ['2026-01-01', '2026-01-03'])

    def test_ripe_error_is_not_interpreted_as_empty_success(self):
        with self.assertRaisesRegex(ValueError, 'invalid resource'):
            parse_ripe(json.dumps({'status': 'error', 'messages': ['invalid resource']}), 'asns_ris')

    def test_ripe_retains_only_explicit_interval_endpoints(self):
        raw = json.dumps({'status': 'ok', 'data': {'resolution': '1d', 'stats': [
            {'asns_ris': 2, 'timeline': [{'starttime': '2026-01-01', 'endtime': '2026-01-06'}]},
        ]}})
        frame = parse_ripe(raw, 'asns_ris')
        self.assertEqual(frame.date.tolist(), ['2026-01-01', '2026-01-06'])
        self.assertEqual(frame.value.tolist(), [2, 2])
        self.assertEqual(len(frame), 2)  # Intermediate days are not synthesized.

    def test_fda_uses_received_day_and_reported_bins_only(self):
        frame = parse_fda(json.dumps({'results': [
            {'time': '20240101', 'count': 4}, {'time': '20240103', 'count': 0},
        ]}))
        self.assertEqual(frame.value.tolist(), [4, 0])
        self.assertEqual(frame.date.tolist(), [pd.Timestamp('2024-01-01', tz='UTC'), pd.Timestamp('2024-01-03', tz='UTC')])
        self.assertEqual(len(frame), 2)

    def test_fixed_universes_are_unique_and_npm_is_new(self):
        for specs in (NPM, CRAN, PYPI):
            names = [x[0] for x in specs]
            self.assertEqual(len(names), len(set(names)))
        self.assertFalse(set(x[0] for x in NPM) & set(x[0] for x in NPM_PACKAGES))
        topics = [(lang, article) for lang, article, _ in WIKIPEDIA]
        self.assertEqual(len(topics), len(set(topics)))


if __name__ == '__main__':
    unittest.main()
