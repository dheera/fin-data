import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd

from sources_physical2 import (
    clean_frame, main, omission, parse_coops, parse_dwd, parse_epa_county_zip, parse_ghcn, parse_hydat,
)


class Physical2ParserTests(unittest.TestCase):
    def test_ghcn_scaling_trace_quality_and_presumed_zeros(self):
        # January: measured rain, trace, failed QC, presumed zero, absent, negative temperature.
        records = [(123, " ", " ", "S"), (0, "T", " ", "S"),
                   (200, " ", "X", "S"), (0, "P", " ", "S"), (0, "I", " ", "S")]
        records += [(-9999, " ", " ", " ")] * (31 - len(records))
        line = "USW00023183202501PRCP" + "".join(f"{v:5d}{m}{q}{s}" for v, m, q, s in records)
        temp = "USW00023183202501TMIN" + f"{-125:5d}  S" + f"{-9999:5d}   " * 30
        values, flags = parse_ghcn((line + "\n" + temp + "\n").encode())
        self.assertEqual([row["value"] for row in values["PRCP"]], [12.3, 0.0])
        self.assertEqual(values["TMIN"][0]["value"], -12.5)
        self.assertEqual(flags["PRCP"], ["S"])

    def test_ghcn_leap_day_is_a_real_calendar_date(self):
        line = "USW00023183202402TMAX" + f"{100:5d}  S" * 31
        values, _ = parse_ghcn(line.encode())
        self.assertEqual(len(values["TMAX"]), 29)
        self.assertEqual(values["TMAX"][-1]["date"], "2024-02-29")

    def test_dwd_product_member_and_source_day(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("Metadaten.txt", "Ignore this metadata")
            archive.writestr("produkt_klima_tag_20250101_20250103_00001.txt",
                             "STATIONS_ID;MESS_DATUM; FX; SDK;eor\n"
                             "1;20250101; 12.5; 1.2;eor\n"
                             "1;20250103; -999; 2.5;eor\n")
        frame = parse_dwd(buffer.getvalue())
        wind = clean_frame(frame.date, frame.FX.mask(frame.FX == -999))
        self.assertEqual(wind.value.tolist(), [12.5])
        self.assertEqual(wind.date.iloc[0], pd.Timestamp("2025-01-01", tz="UTC"))

    def test_coops_preserves_native_hour_and_removes_flagged_values(self):
        payload = {"data": [
            {"t": "2025-01-01 00:00", "s": "4.5", "g": "8.0", "f": "0,0"},
            {"t": "2025-01-01 01:00", "s": "99", "g": "99", "f": "1,0"},
            {"t": "2025-01-01 02:00", "s": "", "g": "", "f": "0,0"},
            {"t": "2025-01-01 03:00", "s": "3.5", "g": "6.0", "f": "0,0"},
        ]}
        frame = parse_coops(payload, "wind")
        self.assertEqual(frame.value.tolist(), [4.5, 3.5])
        self.assertEqual(frame.date.dt.hour.tolist(), [0, 3])

    def test_hydat_rejects_truncated_page(self):
        with self.assertRaisesRegex(ValueError, "truncated"):
            parse_hydat({"features": [], "numberMatched": 100})

    def test_conflicting_dates_raise_without_arbitrary_resolution(self):
        with self.assertRaisesRegex(ValueError, "Conflicting values"):
            clean_frame(["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-04"],
                        [1, 2, 3, 4])

    def test_epa_preserves_zero_padded_county_identity(self):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr(
                "daily_aqi_by_county_2025.csv",
                '"State Name","county Name","State Code","County Code","Date","AQI"\n'
                '"Alabama","Baldwin","01","003","2025-01-01",42\n'
            )
        frame = parse_epa_county_zip(buffer.getvalue())
        self.assertEqual(frame.loc[0, "State Code"] + frame.loc[0, "County Code"], "01003")
        self.assertEqual(frame.loc[0, "AQI"], 42)

    def test_cli_surfaces_failed_collection_but_not_eligibility_omissions(self):
        cases = [
            ("acquisition", lambda _: ([], [{"item": "station", "error": "HTTP 403 Forbidden"}]), 1, 1, 0),
            ("omission", lambda _: ([], [omission("station", "insufficient observed days")]), 0, 0, 1),
        ]
        def broken_parser(_):
            raise ValueError("Unexpected source columns")
        cases.append(("parser", broken_parser, 1, 1, 0))
        for label, collector, code, failures, omissions in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                with patch("sources_physical2.ROOT", root), \
                     patch.dict("sources_physical2.PROVIDERS", {"coops": collector}), \
                     patch("builtins.print"):
                    self.assertEqual(main(["--provider", "coops"]), code)
                report = json.loads((root / "logs/physical2_coops.json").read_text())
                self.assertEqual(len(report["errors"]), failures)
                self.assertEqual(len(report["omissions"]), omissions)


if __name__ == "__main__":
    unittest.main()
