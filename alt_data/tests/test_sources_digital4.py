import json
import unittest

import pandas as pd

from sources_digital4 import binance_symbol_id, clean, parse_amsix_table, parse_binance_klines, parse_ioda, parse_llama_breakdown, parse_tranco


class DigitalFourParsingTests(unittest.TestCase):
    def test_llama_absent_protocol_is_missing_and_reported_zero_is_kept(self):
        obj = {"totalDataChartBreakdown": [
            [1704067200, {"A": 0, "B": 12}],
            [1704153600, {"A": 3}],
            [1704240000, {"A": None, "B": -2}],
        ]}
        result = parse_llama_breakdown(obj, "2024-01-01", "2024-01-04")
        self.assertEqual([v for _, v in result["A"]], [0, 3])
        self.assertEqual([v for _, v in result["B"]], [12, -2])
        self.assertEqual([d.day for d, _ in result["B"]], [1, 3])

    def test_conflicting_observations_are_not_silently_overwritten(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            clean(["2024-01-01", "2024-01-01"], [1, 2])
        result = clean(["2024-01-01", "2024-01-01"], [1, 1])
        self.assertEqual(len(result), 1)
        self.assertEqual(str(result.date.dtype), "datetime64[ns, UTC]")

    def test_binance_retains_quote_units_and_excludes_open_bar(self):
        def row(day, total="100.2", count=17, buys="45.7"):
            stamp = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
            return [stamp, "1", "1", "1", "1", "99", stamp + 86400000 - 1, total, count, "40", buys, "0"]
        result = parse_binance_klines([row("2024-01-01"), row("2024-01-02")], "2024-01-01", "2024-01-02")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0][7], 100.2)
        self.assertEqual(result.iloc[0][8], 17)
        self.assertEqual(result.iloc[0][10], 45.7)
        with self.assertRaisesRegex(ValueError, "exceeds"):
            parse_binance_klines([row("2024-01-01", buys="101")], "2024-01-01", "2024-01-02")

    def test_non_ascii_exchange_symbols_have_distinct_stable_file_ids(self):
        self.assertNotEqual(binance_symbol_id("币安人生USDT"), binance_symbol_id("牛来USDT"))
        self.assertEqual(binance_symbol_id("BTCUSDT"), "btcusdt")

    def test_tranco_is_ordinal_observation_not_zero_filled_rank(self):
        result = parse_tranco(b"1,example.de\r\n56,example.jp\r\n")
        self.assertEqual(result["rank"].tolist(), [1, 56])
        with self.assertRaisesRegex(ValueError, "Malformed"):
            parse_tranco(b"1,example.de\n2,example.de\n")

    def test_ioda_nulls_keep_original_hour_positions_and_skip_forecasts(self):
        source = {"datasource": "gtr", "subtype": "WEB_SEARCH", "from": 1704067200,
                  "until": 1704078000, "step": 3600, "nativeStep": 1800, "values": [2, None, 4]}
        forecast = {**source, "datasource": "gtr-sarima"}
        result = parse_ioda(json.dumps({"error": None, "data": [[source], [forecast]]}))
        self.assertEqual(len(result), 1)
        meta, frame = result[0]
        self.assertEqual(frame.value.tolist(), [2, 4])
        self.assertEqual(frame.date.dt.hour.tolist(), [0, 2])
        self.assertEqual(meta["nativeStep"], 1800)

    def test_ioda_refuses_unexpected_temporal_resolution(self):
        source = {"datasource": "bgp", "step": 7200, "values": [2]}
        with self.assertRaisesRegex(ValueError, "hourly step"):
            parse_ioda(json.dumps({"error": None, "data": [[source]]}))

    def test_ioda_country_batch_keeps_country_and_product_binding(self):
        common = {"datasource": "gtr", "subtype": "WEB_SEARCH", "entityType": "country",
                  "from": 1704067200, "step": 3600, "nativeStep": 1800}
        payload = {"error": None, "data": [
            [{**common, "entityCode": "DE", "values": [1, None, 3]},
             {**common, "entityCode": "JP", "values": [9, 8, None]}],
            [{**common, "entityCode": "JP", "subtype": "MAPS", "values": [4, 5, 6]}],
        ]}
        result = {(m["entityCode"], m["subtype"]): f.value.tolist()
                  for m, f in parse_ioda(json.dumps(payload))}
        self.assertEqual(result, {("DE", "WEB_SEARCH"): [1, 3],
                                  ("JP", "WEB_SEARCH"): [9, 8],
                                  ("JP", "MAPS"): [4, 5, 6]})

    def test_amsix_daily_cells_cross_year_without_publishing_weekly_average(self):
        table = pd.DataFrame([
            ["Start date", "2025-12-29"],
            ["End date", "2026-01-05"],
            ["Unit", "PByte"],
            ["Week", "Average", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            ["29/12-04/01", 999, 1, 2, None, 4, 5, 6, 7],
        ])
        frame, unit = parse_amsix_table(table)
        self.assertEqual(unit, "PByte")
        self.assertEqual(frame.value.tolist(), [1, 2, 4, 5, 6, 7])
        self.assertEqual(frame.date.iloc[-1], pd.Timestamp("2026-01-04", tz="UTC"))
        self.assertNotIn(999, frame.value.tolist())

    def test_amsix_dst_range_endpoint_does_not_create_an_eighth_day(self):
        table = pd.DataFrame([
            ["Start date", "2026-03-23"], ["End date", "2026-03-30"], ["Unit", "PByte"],
            ["Week", "Average", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            ["23/03-30/03", 99, 1, 2, 3, 4, 5, 6, 7],
        ])
        frame, _ = parse_amsix_table(table)
        self.assertEqual(len(frame), 7)
        self.assertEqual(frame.date.iloc[-1], pd.Timestamp("2026-03-29", tz="UTC"))


if __name__ == "__main__":
    unittest.main()
