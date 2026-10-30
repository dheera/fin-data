import json
import unittest

import pandas as pd

from sources_economy4 import clean, moa_unit, observed_weekly, parse_eu_price, parse_moa, parse_taxud, roc_date, taxud_week_date, trade_geography


class EconomySourceTests(unittest.TestCase):
    def test_distinct_taiwan_date_formats_agree(self):
        self.assertEqual(roc_date("113.02.29"), pd.Timestamp("2024-02-29", tz="UTC"))
        self.assertEqual(roc_date("1130229"), roc_date("113.02.29"))

    def test_taiwan_error_payload_is_not_empty_success(self):
        with self.assertRaisesRegex(ValueError, "MOA error"):
            parse_moa(json.dumps([{"errMsg": "too many rows"}]).encode(), "raw")

    def test_fish_source_code_and_name_are_preserved(self):
        raw = [{"交易日期": "1130102", "品種代碼": 1011, "魚貨名稱": "吳郭魚",
                "市場名稱": "台北", "平均價": 44.2, "交易量": 93}]
        data = parse_moa(json.dumps(raw).encode(), "raw", aquatic=True)
        self.assertEqual(data.iloc[0]["作物代號"], "1011")
        self.assertEqual(data.iloc[0]["市場名稱"], "台北")
        self.assertEqual(data.iloc[0]["種類代碼"], "aquatic")

    def test_flower_auction_units_are_bunches_not_kilograms(self):
        self.assertEqual(moa_unit("N06", "traded_volume"), "bunches")
        self.assertEqual(moa_unit("N06", "average_price"), "TWD per bunch")
        self.assertEqual(moa_unit("N04", "traded_volume"), "kilograms")
        self.assertEqual(moa_unit("aquatic", "average_price"), "TWD per kilogram")
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            moa_unit("None", "average_price")

    def test_price_nulls_and_monthly_rows_are_omitted(self):
        base = {"beginDate": "01/01/2024", "endDate": "07/01/2024",
                "memberStateCode": "DE", "unit": "€/100Kg", "periodType": "Week"}
        raw = [dict(base, price="€1,234.50"), dict(base, price=None),
               dict(base, price="€3", periodType="Month", endDate="31/01/2024")]
        data = parse_eu_price(json.dumps(raw).encode())
        self.assertEqual(data.value.tolist(), [1234.5])

    def test_weekly_price_endpoint_rejects_long_period(self):
        raw = [{"beginDate": "01/01/2024", "endDate": "31/01/2024",
                "memberStateCode": "DE", "unit": "EUR", "price": "€9"}]
        with self.assertRaisesRegex(ValueError, "Non-weekly"):
            parse_eu_price(json.dumps(raw).encode())

    def test_conflicting_duplicate_dates_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            clean(["2024-01-01", "2024-01-01"], [1, 2])

    def test_event_week_aggregation_keeps_gaps_and_drops_partial_boundaries(self):
        data = clean(["2024-01-01", "2024-01-03", "2024-01-17", "2024-01-29"],
                     [2, 3, 7, 11])
        result = observed_weekly(data, pd.Timestamp("2024-01-01", tz="UTC"),
                                 pd.Timestamp("2024-01-29", tz="UTC"))
        self.assertEqual(result.value.tolist(), [5, 7])
        self.assertEqual(result.date.dt.strftime("%Y-%m-%d").tolist(),
                         ["2024-01-07", "2024-01-21"])

    def test_taxud_calendar_example_and_boundary_omission(self):
        self.assertEqual(taxud_week_date("2025", 9), pd.Timestamp("2025-03-02", tz="UTC"))
        for year, week in [("2024", 53), ("2025", 1), ("2024/2025", 9), ("2027", 9)]:
            self.assertTrue(pd.isna(taxud_week_date(year, week)))

    def test_taxud_duplicate_source_cells_conflict(self):
        base = dict(sector="Fish", marketingYear="2024", week=23, memberStateCode="DE",
                    partnerCode="NO", product="Salmon", cn8ProductCode="001",
                    taric10ProductCode="00100", kg=5, euroValue=20)
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            parse_taxud(json.dumps([base, dict(base, kg=6)]).encode())
        flagged = parse_taxud(json.dumps([base, dict(base, kg=6)]).encode(), flag_ambiguous=True)
        self.assertTrue(flagged["_ambiguous_source_cell"].all())
        data = parse_taxud(json.dumps([base, base]).encode())
        self.assertEqual(len(data), 1)

    def test_customs_pseudo_country_codes_are_not_iso_geographies(self):
        self.assertEqual(trade_geography("EL", "UK"), ("non_us", ["GB", "GR"]))
        self.assertEqual(trade_geography("DE", "US"), ("mixed", ["DE", "US"]))
        self.assertEqual(trade_geography("DE", "PR"), ("mixed", ["DE", "PR"]))
        self.assertEqual(trade_geography("DE", "QP"), ("mixed", ["DE"]))


if __name__ == "__main__":
    unittest.main()
