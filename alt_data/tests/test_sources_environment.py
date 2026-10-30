import gzip
import unittest

import pandas as pd

from sources_environment import frame_from_values, parse_ndbc, parse_usgs


NDBC_SAMPLE = b"""#YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE
#yr mo dy hr mn degT m/s m/s m sec sec degT hPa degC degC degC nmi hPa ft
2025 01 01 00 10 120 3.2 4.0 1.2 8.0 5.0 120 1012.3 18.4 19.5 14.0 99.0 99.0 99.0
2025 01 01 00 20 120 99.0 99.0 99.0 99.0 99.0 999 9999.0 999.0 999.0 999.0 99.0 99.0 99.0
2025 01 01 00 30 120 3.4 4.2 1.4 8.0 5.0 120 1012.5 18.4 19.6 14.0 99.0 99.0 99.0
"""


class EnvironmentParserTests(unittest.TestCase):
    def test_native_ndbc_minutes_and_measurement_missing_values(self):
        raw = parse_ndbc(gzip.compress(NDBC_SAMPLE))
        self.assertEqual(raw.date.dt.minute.tolist(), [10, 20, 30])
        self.assertEqual(str(raw.date.dt.tz), "UTC")
        wind = frame_from_values(raw.date, raw.WSPD, sentinel=99.0)
        water = frame_from_values(raw.date, raw.WTMP, sentinel=999.0)
        pressure = frame_from_values(raw.date, raw.PRES, sentinel=9999.0)
        self.assertEqual(wind.value.tolist(), [3.2, 3.4])
        self.assertEqual(water.value.tolist(), [19.5, 19.6])
        self.assertEqual(pressure.value.tolist(), [1012.3, 1012.5])
        # Source gaps stay gaps; no ten-minute interpolation is introduced.
        self.assertEqual(wind.date.diff().iloc[-1], pd.Timedelta(minutes=20))

    def test_ndbc_conflicting_duplicate_rejected(self):
        last = NDBC_SAMPLE.splitlines()[-1].replace(b"00 30", b"00 10")
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            parse_ndbc(NDBC_SAMPLE + last + b"\n")

    def test_usgs_day_label_sentinel_and_qualifiers(self):
        payload = {"value": {"timeSeries": [{
            "sourceInfo": {"siteCode": [{"value": "01463500"}]},
            "variable": {"variableCode": [{"value": "00060"}], "noDataValue": -999999},
            "values": [{"value": [
                {"dateTime": "2025-01-01T00:00:00.000-05:00", "value": "1250", "qualifiers": ["A"]},
                {"dateTime": "2025-01-02T00:00:00.000-05:00", "value": "-999999", "qualifiers": []},
                {"dateTime": "2025-01-03T00:00:00.000-05:00", "value": "1130", "qualifiers": ["P", "e"]},
            ]}],
        }]}}
        result = parse_usgs(payload)
        site, code, frame, source, qualifiers = result[0]
        self.assertEqual((site, code), ("01463500", "00060"))
        self.assertEqual(frame.date.tolist(), ["2025-01-01", "2025-01-03"])
        self.assertEqual(frame.value.tolist(), [1250.0, 1130.0])
        self.assertEqual(qualifiers, ["A", "P", "e"])


if __name__ == "__main__":
    unittest.main()
