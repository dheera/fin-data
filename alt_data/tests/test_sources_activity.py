import sys
from pathlib import Path
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sources_activity import clean_frame, parse_energy, remove_ambiguous_duplicates


class EnergyParsingTests(unittest.TestCase):
    def test_nulls_remain_gaps_and_negative_power_is_preserved(self):
        result = parse_energy({
            "unix_seconds": [1704067200, 1704068100, 1704069000, 1704069900],
            "production_types": [{"name": "Load", "data": [100, None, -20, 105]}],
        }, "load")
        frame, frequency, intervals = result["load"]
        self.assertEqual(frame.value.tolist(), [100, -20, 105])
        self.assertEqual(len(frame), 3)
        self.assertEqual(intervals, [900, 1800])
        self.assertEqual(str(frame.date.dt.tz), "UTC")

    def test_preserves_both_utc_instants_in_daylight_saving_overlap(self):
        dates = pd.to_datetime(["2025-10-26T00:30Z", "2025-10-26T01:30Z"])
        result = parse_energy({
            "unix_seconds": [int(x.timestamp()) for x in dates],
            "production_types": [{"name": "Load", "data": [50, 70]}],
        }, "load")
        frame, frequency, _ = result["load"]
        self.assertEqual(frequency, "hourly")
        self.assertFalse(frame.date.duplicated().any())
        self.assertEqual(frame.value.tolist(), [50, 70])

    def test_rejects_timestamp_value_misalignment(self):
        with self.assertRaises(ValueError):
            parse_energy({
                "unix_seconds": [1704067200, 1704068100],
                "production_types": [{"name": "Load", "data": [50]}],
            }, "load")

    def test_bad_numeric_source_value_is_an_error(self):
        with self.assertRaises(ValueError):
            clean_frame(["2024-01-01"], ["not a number"])

    def test_conflicting_counter_time_labels_are_omitted_not_averaged(self):
        frame = pd.DataFrame({"date": [1, 1, 2, 2, 3], "value": [10, 10, 20, 30, 40]})
        parsed, conflicts = remove_ambiguous_duplicates(frame)
        self.assertEqual(conflicts, 1)
        self.assertEqual(parsed.date.tolist(), [1, 3])
        self.assertEqual(parsed.value.tolist(), [10, 40])


if __name__ == "__main__":
    unittest.main()
