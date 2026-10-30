import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from sources_energy4 import clean, energinet_geography, entsog_geography, nz_trading_periods, ons_local_time, parse_entsog, parse_elexon_sample, parse_japan_grid, resource_date, select_elexon_units


class Energy4Parsers(unittest.TestCase):
    def test_entsog_preserves_dst_and_omits_long_capacity_validity(self):
        rows = [
            {"periodFrom": "2025-03-29T06:00:00+01:00",
             "periodTo": "2025-03-30T06:00:00+02:00", "value": 4, "isNA": False},
            {"periodFrom": "2025-10-25T06:00:00+02:00",
             "periodTo": "2025-10-26T06:00:00+01:00", "value": 5, "isNA": False},
            {"periodFrom": "2025-01-01T06:00:00+01:00",
             "periodTo": "2026-01-01T06:00:00+01:00", "value": 600, "isNA": False},
            {"periodFrom": "2025-01-01T06:00:00+01:00",
             "periodTo": "2025-01-02T06:00:00+01:00", "value": "", "isNA": True},
        ]
        frame = parse_entsog(json.dumps({"operationaldata": rows}).encode())
        self.assertEqual(frame.value.tolist(), [4, 5])
        self.assertEqual(frame.date.iloc[0], pd.Timestamp("2025-03-29T05:00Z"))

    def test_clean_does_not_average_conflicting_reports(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            clean(["2025-01-01", "2025-01-01"], [3, 4])
        frame = clean(["2025-01-01", "2025-01-01", "2025-01-02"], [3, 3, None])
        self.assertEqual(len(frame), 1)
        self.assertEqual(str(frame.date.dtype), "datetime64[ns, UTC]")

    def test_ons_operational_clock(self):
        frame = ons_local_time(pd.Series(["2025-01-01 00:00:00", "2025-07-01 00:00:00"]))
        self.assertEqual(frame.iloc[0], pd.Timestamp("2025-01-01T03:00Z"))
        self.assertEqual(frame.iloc[1], pd.Timestamp("2025-07-01T03:00Z"))

    def test_nz_46_and_50_period_dst_days(self):
        rows = []
        for day, count in [("2025-04-06", 50), ("2025-09-28", 46), ("2025-09-29", 48)]:
            rows.append({"asset": "X", "Trading_Date": day,
                         **{f"TP{i}": i if i <= count else None for i in range(1, 51)}})
        long = nz_trading_periods(pd.DataFrame(rows), ["asset"])
        self.assertEqual(len(long), 144)
        self.assertFalse(long.date.duplicated().any())
        spring = long[(long.date >= pd.Timestamp("2025-09-27T12:00Z")) &
                      (long.date < pd.Timestamp("2025-09-28T11:00Z"))]
        self.assertEqual(len(spring), 46)
        self.assertEqual(spring.date.max(), pd.Timestamp("2025-09-28T10:30Z"))

    def test_nz_rejects_manufactured_dst_extra_periods(self):
        data = pd.DataFrame([{"asset": "X", "Trading_Date": "2025-09-28", "TP47": 5}])
        with self.assertRaisesRegex(ValueError, "DST"):
            nz_trading_periods(data, ["asset"])

    def test_ons_resource_dates(self):
        self.assertEqual(resource_date({"url": "https://example/DATA_2025_08.parquet"}),
                         pd.Timestamp("2025-08-01"))
        self.assertEqual(resource_date({"url": "https://example/DATA_2025.parquet"}),
                         pd.Timestamp("2025-01-01"))
        self.assertIsNone(resource_date({"url": "https://example/dictionary.json"}))

    def test_elexon_sample_settlement_revisions_and_dst(self):
        rows = []
        # Period number 25 stays fixed; clock hour differs on transition days.
        for day, end in [("2025-03-30", "2025-03-30T12:30:00"),
                         ("2025-03-31", "2025-03-31T11:30:00"),
                         ("2025-10-26", "2025-10-26T11:30:00"),
                         ("2025-10-27", "2025-10-27T12:30:00")]:
            for run, value in [("II", 1), ("RF", 2), ("SF", 1.5)]:
                rows.append({"settlementDate": day, "settlementPeriod": 25,
                             "halfHourEndTime": end, "settlementRunType": run,
                             "quantity": value, "bmUnit": "T_EXAMPLE"})
        parsed = parse_elexon_sample(json.dumps(rows).encode())
        self.assertEqual(len(parsed), 4)
        self.assertTrue(parsed.value.eq(2).all())
        self.assertEqual(parsed.date.iloc[0], pd.Timestamp("2025-03-30T12:30Z"))

    def test_elexon_rejects_ignored_sampling_filter(self):
        rows = [{"settlementPeriod": 24}]
        with self.assertRaisesRegex(ValueError, "filter"):
            parse_elexon_sample(json.dumps(rows).encode())

    def test_japan_grid_units_timezone_signed_power_and_missing_values(self):
        raw = ("単位[MW平均],,,供給力\nDATE,TIME,エリア需要,揚水,太陽光発電実績\n"
               "2025/01/01,00:00,100,-12,0\n"
               "2025/01/01,00:30,105,5,\n").encode("cp932")
        frame = parse_japan_grid(raw)
        self.assertEqual(frame.date.iloc[0], pd.Timestamp("2024-12-31T15:00Z"))
        self.assertEqual(frame["揚水"].tolist(), [-12, 5])
        self.assertTrue(pd.isna(frame["太陽光発電実績"].iloc[1]))
        self.assertEqual(parse_japan_grid(raw.decode("cp932").encode("utf-8")).date.iloc[0],
                         pd.Timestamp("2024-12-31T15:00Z"))
        with self.assertRaisesRegex(ValueError, "units"):
            parse_japan_grid(raw.replace(b"MW", b"kWh"))

    def test_nordic_geography_uses_market_exposure_not_provider_headquarters(self):
        self.assertEqual(energinet_geography("AfrrReservesNordic", ["PriceArea"], ["NO3"], "UpDemandMW"),
                         ("Norway", ["NO"]))
        self.assertEqual(energinet_geography("InertiaNordicSyncharea", [], [], "InertiaFIGWs"),
                         ("Finland", ["FI"]))
        self.assertEqual(energinet_geography("FcrNdDK2", ["PriceArea"], ["DK2"], "PriceTotalEUR")[1],
                         ["DK", "SE"])
        self.assertEqual(energinet_geography("FcrNdDK2", ["PriceArea"], ["SE2"], "PurchasedVolumeLocal"),
                         ("Sweden", ["SE"]))

    def test_gas_geography_does_not_assign_tap_terminal_to_swiss_registration(self):
        self.assertEqual(entsog_geography(dict(operatorKey="AL-TSO-0001", pointKey="ITP-00008",
                                               tSOCountry="CH", adjacentCountry="IT"))[0], ["IT"])
        self.assertEqual(entsog_geography(dict(operatorKey="UK-TSO-0004", pointKey="ITP-00207",
                                               tSOCountry="NL", adjacentCountry="UK"))[0], ["GB"])
        self.assertEqual(entsog_geography(dict(operatorKey="FR-TSO-0001", pointKey="example",
                                               tSOCountry="FR", adjacentCountry="DE"))[0], ["DE", "FR"])

    def test_elexon_balanced_selection_is_stable_and_keeps_whole_histories(self):
        frames, reference = [], {}
        for bm, kind, sign in [("A1", "T", 1), ("A2", "T", 1), ("B1", "S", -1)]:
            frames.append(pd.DataFrame({"bmUnit": bm, "date": pd.date_range("2025-01-01", periods=200, tz="UTC"),
                                        "value": [sign * (i + 1) for i in range(200)]}))
            reference[bm] = {"bmUnitType": kind, "gspGroupId": "_A"}
        data = pd.concat(frames, ignore_index=True)
        with TemporaryDirectory() as tmp, patch("sources_energy4.ROOT", Path(tmp)):
            chosen = select_elexon_units(data, reference, limit=2)
            self.assertEqual(len(chosen), 2)
            self.assertIn("B1", [bm for bm, frame in chosen])
            self.assertTrue(all(len(frame) == 200 for bm, frame in chosen))
            self.assertEqual([bm for bm, frame in chosen],
                             [bm for bm, frame in select_elexon_units(data, reference, limit=3)])


if __name__ == "__main__":
    unittest.main()
