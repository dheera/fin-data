import json
import unittest
from unittest.mock import patch

import pandas as pd

import sources_emerging6 as source


class Emerging6Tests(unittest.TestCase):
    def test_pihps_uses_published_dates_and_omits_missing_prices(self):
        raw = json.dumps({"data": [
            {"date": "02 Jan 24", "name": "Beras (kg)", "price": 100},
            {"date": "04 Jan 24", "name": "Beras (kg)", "price": None},
            {"date": "05 Jan 24", "name": "Beras (kg)", "price": 0},
            {"date": "08 Jan 24", "name": "Beras (kg)", "price": 110},
        ]})
        frame = source.parse_pihps(raw, {"Beras": "com_1"})
        self.assertEqual(frame.value.tolist(), [100, 110])
        self.assertEqual(frame.date.dt.day.tolist(), [2, 8])
        self.assertEqual(frame.commodity_id.tolist(), ["com_1", "com_1"])

    def test_pihps_rejects_unrequested_commodity(self):
        with self.assertRaisesRegex(ValueError, "unrequested"):
            source.parse_pihps(json.dumps({"data": [{"date": "02 Jan 24", "name": "Oil (kg)", "price": 3}]}), {"Rice": "com_1"})

    def test_conflicting_date_values_are_not_arbitrarily_resolved(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            source.reported_frame(["2024-01-02", "2024-01-02"], [1, 2])

    def test_financial_number_formats_and_missing(self):
        self.assertEqual(source.number("(1,234.50)"), -1234.5)
        self.assertTrue(pd.isna(source.number("-")))
        self.assertEqual(source.number("0.00"), 0)

    def test_cdsl_expands_date_headers_excludes_totals_and_keeps_zero(self):
        cash = """<table><tr><td>02-01-2025</td><td>Equity</td><td>Stock Exchange</td><td>20</td><td>30</td><td>-10</td><td>-1</td><td>80</td></tr>
        <tr><td></td><td>Debt</td><td>Stock Exchange</td><td>0</td><td>0</td><td>0</td><td>0</td><td>80</td></tr>
        <tr><td></td><td>Equity</td><td>Sub-total</td><td>20</td><td>30</td><td>-10</td><td>-1</td><td>80</td></tr></table>"""
        deriv = """<table><tr><td>02-JAN-2025</td><td>INDEX_FUTURES</td><td>10</td><td>100</td><td>11</td><td>110</td><td>500</td><td>1000</td></tr>
        <tr><td>Total for January</td><td>INDEX_FUTURES</td><td>10</td><td>100</td><td>11</td><td>110</td><td>500</td><td>1000</td></tr></table>"""
        frame = source.parse_cdsl((cash + deriv).encode())
        self.assertEqual(len(frame), 5)
        self.assertEqual(frame[frame.family == "cash"].value.tolist(), [-10, 0])
        self.assertEqual(frame[frame.family == "derivatives"].value.tolist(), [100, 110, 500])
        self.assertEqual(frame.date.nunique(), 1)

    def test_sniim_parses_modal_column_and_pagination(self):
        raw = """<span id='lblPaginacion'>Página 2 de 3</span><table id='tblResultados'>
        <tr><td colspan='9'>Frutas</td></tr>
        <tr><td>02/01/2025</td><td>Aguacate Hass</td><td>Primera</td><td>Caja 10 kg</td><td>Michoacán</td><td>40</td><td>60</td><td>50</td><td>Local</td></tr></table>""".encode()
        data, page, pages = source.parse_sniim(raw)
        self.assertEqual((page, pages), (2, 3))
        self.assertEqual(data.value.tolist(), [50])
        self.assertEqual(data.origin.iloc[0], "Michoacán")

    def test_sniim_rejects_unpaginated_response(self):
        with self.assertRaises(ValueError):
            source.parse_sniim(b"<html>Temporary error</html>")
        with self.assertRaisesRegex(ValueError, "per-kilogram"):
            source.parse_sniim(b"Commercial-package prices", expected_per_kg=True)

    def test_sniim_single_date_layout_uses_explicit_report_date(self):
        raw = """Registros del 02/01/2025 al 02/01/2025
        <span id='lblPaginacion'>Página 1 de 1</span><table id='tblResultados'>
        <tr><td>Aguacate Hass</td><td>Primera</td><td>Caja de 10 kg.</td><td>Michoacán</td><td>50</td><td>50</td><td>50</td><td></td></tr></table>""".encode()
        data, _, _ = source.parse_sniim(raw)
        self.assertEqual(data.date.iloc[0], pd.Timestamp("2025-01-02", tz="UTC"))
        self.assertEqual(data.value.iloc[0], 50)

    def quotes(self):
        dates = pd.date_range("2024-01-01", periods=210, tz="UTC")
        return pd.DataFrame({"date": dates, "value": range(1, 211), "presentation": "kg"})

    def test_published_weekly_cadence_cannot_switch_to_daily(self):
        quotes = self.quotes()
        self.assertEqual(source.quote_frame(quotes)[1], "business_daily")
        frame, frequency = source.quote_frame(quotes, "weekly")
        self.assertEqual(frequency, "weekly")
        self.assertEqual(len(frame), 30)
        self.assertTrue(frame.date.dt.weekday.eq(0).all())

    def test_published_daily_does_not_switch_to_weekly_if_sparse(self):
        self.assertEqual(source.quote_frame(self.quotes().iloc[::7], "business_daily"), (None, None))

    def test_weekly_median_uses_raw_quotes_and_does_not_fill_empty_weeks(self):
        rows = []
        for week in range(31):
            if week == 10:
                continue
            monday = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(weeks=week)
            rows.extend([{"date": monday, "value": 1 + week, "presentation": "a"},
                         {"date": monday, "value": 3 + week, "presentation": "b"},
                         {"date": monday + pd.Timedelta(days=1), "value": 101 + week, "presentation": "a"}])
        frame, frequency = source.quote_frame(pd.DataFrame(rows), "weekly")
        self.assertEqual(frequency, "weekly")
        self.assertEqual(frame.value.iloc[0], 3)
        self.assertEqual(len(frame), 30)
        self.assertNotIn(pd.Timestamp("2024-03-11", tz="UTC"), set(frame.date))

    def test_stage_pins_batch_membership_and_cadence(self):
        with patch.object(source, "PINNED", {"selected": "weekly"}), patch.object(source, "IDS", set()):
            self.assertIsNone(source.stage("not_selected", self.quotes(), frequency="weekly"))
            self.assertIsNone(source.stage("selected", self.quotes(), frequency="business_daily"))

    def test_sniim_splits_truncated_month_into_complete_date_partitions(self):
        class FakeFetcher:
            def get(self, url, params, **kwargs):
                first = pd.to_datetime(params["fechaInicio"], format="%d/%m/%Y", utc=True)
                last = pd.to_datetime(params["fechaFinal"], format="%d/%m/%Y", utc=True)
                pages = 2 if (last - first).days > 7 else 1
                html = (f"Pesos ($) por kilogramo calculado <span id='lblPaginacion'>Página 1 de {pages}</span><table id='tblResultados'>"
                        f"<tr><td>{first.strftime('%d/%m/%Y')}</td><td>Rice</td><td>First</td><td>kg</td>"
                        "<td>Local</td><td>1</td><td>3</td><td>2</td><td></td></tr></table>").encode()
                return html, source.hashlib.sha256(html).hexdigest()
        with patch.object(source.time, "sleep"):
            result = source.sniim_market(FakeFetcher(), 33, "Market", pd.Timestamp("2025-01-31", tz="UTC"))
        partitions = result[-1]
        self.assertEqual(len(partitions), 4)
        self.assertEqual(partitions[0]["start"], "2025-01-01")
        self.assertEqual(partitions[-1]["end"], "2025-01-31")
        self.assertTrue(all(x["pages"] == 1 for x in partitions))
        for a, b in zip(partitions, partitions[1:]):
            self.assertEqual(pd.Timestamp(b["start"]) - pd.Timestamp(a["end"]), pd.Timedelta(days=1))

    def test_weekly_quotes_exclude_the_unfinished_current_week(self):
        current = pd.Timestamp.now(tz="UTC").normalize()
        monday = current - pd.Timedelta(days=current.weekday())
        quotes = self.quotes()
        quotes = pd.concat([quotes, pd.DataFrame({"date": [monday], "value": [10000], "presentation": ["kg"]})])
        frame, frequency = source.quote_frame(quotes, "weekly")
        self.assertEqual(frequency, "weekly")
        self.assertNotIn(monday, set(frame.date))


if __name__ == "__main__":
    unittest.main()
