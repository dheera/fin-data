import json
from pathlib import Path
import sys
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sources_digital import parse_csv, parse_grouped_counts, parse_npm, parse_wikimedia, year_windows


class DigitalSourceParsingTests(unittest.TestCase):
    def test_wikimedia_preserves_days_and_reported_zero(self):
        raw = json.dumps({"items": [
            {"timestamp": "2024010100", "views": 0},
            {"timestamp": "2024010300", "views": 51},
        ]})
        frame = parse_wikimedia(raw)
        self.assertEqual(frame.value.tolist(), [0, 51])
        self.assertEqual(frame.date.iloc[1], pd.Timestamp("2024-01-03", tz="UTC"))
        self.assertEqual(len(frame), 2)  # January 2 is not fabricated.

    def test_npm_does_not_fill_missing_date_or_reinterpret_downloads(self):
        raw = json.dumps({"downloads": [
            {"day": "2024-01-01", "downloads": 12},
            {"day": "2024-01-03", "downloads": 0},
        ]})
        frame = parse_npm(raw)
        self.assertEqual(frame.to_dict("records"), [
            {"date": "2024-01-01", "value": 12},
            {"date": "2024-01-03", "value": 0},
        ])

    def test_tor_filters_dimension_and_omits_unreported_values(self):
        raw = b"# Tor\nusers,date,country\n12,2024-01-01,ir\n88,2024-01-01,ru\n,2024-01-02,ir\n0,2024-01-03,ir\n"
        frame = parse_csv(raw, "users", {"country": "ir"})
        self.assertEqual(frame.value.tolist(), [12, 0])
        self.assertEqual(frame.date.tolist(), ["2024-01-01", "2024-01-03"])

    def test_city_group_counts_are_not_merged_across_categories(self):
        raw = json.dumps([
            {"date": "2024-01-01", "sr_type": "water", "value": "4"},
            {"date": "2024-01-01", "sr_type": "noise", "value": "90"},
            {"date": "2024-01-03", "sr_type": "water", "value": "2"},
        ])
        frame = parse_grouped_counts(raw, "sr_type", "water")
        self.assertEqual(frame.value.tolist(), ["4", "2"])
        self.assertEqual(len(frame), 2)

    def test_npm_request_windows_have_no_overlap_or_gaps(self):
        windows = list(year_windows("2024-01-01", "2026-10-18"))
        days = []
        for begin, stop in windows:
            interval = pd.date_range(begin, stop)
            self.assertLessEqual(len(interval), 365)
            days.extend(interval)
        self.assertEqual(len(days), len(set(days)))
        self.assertEqual(days, list(pd.date_range("2024-01-01", "2026-10-18")))


if __name__ == "__main__":
    unittest.main()
