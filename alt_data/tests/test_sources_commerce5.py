import io
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd

from sources_commerce5 import (
    baseline, clean_secop_records, complete_event_panel, french_department_country,
    observed_quotes, parse_french_fuel, parse_ppd, property_medians,
    published_frequency, save, weekly_labels,
)


class Commerce5Tests(unittest.TestCase):
    def test_actual_quote_selection_never_carries_a_price_into_an_empty_week(self):
        dates = pd.date_range("2024-01-01", periods=35, freq="7D", tz="UTC").delete(10)
        quotes = pd.DataFrame({"date": dates, "value": range(1, len(dates) + 1)})
        last = quotes.iloc[-1].copy()
        last["date"] += pd.Timedelta(hours=8)
        last["value"] = 100
        frame, frequency, _ = observed_quotes(pd.concat([quotes, last.to_frame().T], ignore_index=True))
        self.assertEqual(frequency, "weekly")
        self.assertEqual(frame.iloc[-1].value, 100)
        self.assertNotIn(pd.Timestamp("2024-03-11", tz="UTC"), set(frame.date))
        self.assertEqual(len(frame), 34)

    def test_dense_actual_quotes_select_one_daily_value_per_reported_day(self):
        dates = pd.date_range("2024-01-01", periods=101, freq="D", tz="UTC").delete(5)
        quotes = pd.DataFrame({"date": dates, "value": range(1, 101)})
        frame, frequency, _ = observed_quotes(quotes)
        self.assertEqual(frequency, "daily")
        self.assertEqual(len(frame), 100)
        self.assertNotIn(pd.Timestamp("2024-01-06", tz="UTC"), set(frame.date))

    def test_published_weekly_quotes_remain_weekly_when_daily_becomes_eligible(self):
        quotes = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=210, tz="UTC"),
                               "value": range(1, 211)})
        self.assertEqual(observed_quotes(quotes)[1], "daily")
        frame, frequency, _ = observed_quotes(quotes, required_frequency="weekly")
        self.assertEqual(frequency, "weekly")
        self.assertEqual(len(frame), 30)
        self.assertEqual(frame.value.tolist(), list(range(7, 211, 7)))
        self.assertTrue(frame.date.dt.weekday.eq(0).all())

    def test_pinned_quote_or_price_cadence_that_cannot_qualify_is_skipped(self):
        sparse = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=35, freq="7D", tz="UTC"),
                               "value": range(1, 36)})
        short = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=100, tz="UTC"),
                              "value": range(1, 101)})
        for required, part in [("daily", sparse), ("weekly", short)]:
            for function, field in [(observed_quotes, "value"), (property_medians, "price")]:
                with self.subTest(frequency=required, aggregation=function.__name__):
                    result = function(part.rename(columns={"value": field}), required_frequency=required)
                    self.assertTrue(result[0].empty)
                    self.assertEqual(result[1], required)

    def test_published_frequency_lookup_uses_only_batch_seven(self):
        catalog = pd.DataFrame({"series_id": ["old", "weekly", "daily"], "batch": [6, 7, 7],
                                "frequency": ["daily", "weekly", "daily"],
                                "content_sha256": ["a", "b", "c"],
                                "dataset_identity": ["old", "weekly", "daily"]})
        baseline.cache_clear()
        try:
            with patch("sources_commerce5.pd.read_parquet", return_value=catalog):
                self.assertEqual(published_frequency("weekly"), "weekly")
                self.assertEqual(published_frequency("daily"), "daily")
                self.assertIsNone(published_frequency("old"))
                self.assertIsNone(published_frequency("unpublished"))
        finally:
            baseline.cache_clear()

    def test_save_refuses_changed_published_cadence(self):
        frame = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=210, tz="UTC"),
                              "value": range(1, 211)})
        old = dict(ids=set(), identities=set(), hashes=set(), pinned={"published"},
                   frequencies={"published": "weekly"})
        with patch("sources_commerce5.baseline", return_value=old), \
                patch("sources_commerce5.save_candidate") as writer:
            self.assertIsNone(save("published", frame, frequency="daily", dataset_identity="published"))
            writer.assert_not_called()

    def test_conflicting_same_time_price_invalidates_whole_instrument(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            observed_quotes(pd.DataFrame({"date": ["2024-01-01"] * 2, "value": [1.2, 1.3]}))

    def test_official_empty_price_node_is_not_a_zero_price(self):
        raw = io.BytesIO()
        with zipfile.ZipFile(raw, "w") as archive:
            archive.writestr("annual.xml", """
                <pdv_liste><pdv id="1" latitude="4800000" longitude="200000" cp="75001" pop="R">
                <ville>Paris</ville><prix/><prix id="1" nom="Gazole" maj="2024-01-02T03:04:05" valeur="1.729"/>
                <rupture id="1" debut="2024-01-03T12:00:00" type="temporaire" fin=""/>
                </pdv></pdv_liste>""")
        frame, profiles, outages, _ = parse_french_fuel(raw.getvalue())
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0].value, 1.729)
        self.assertEqual(profiles["1"]["latitude"], 48)
        self.assertEqual(len(outages), 1)

    def test_event_week_totals_exclude_incomplete_boundary_weeks(self):
        start = pd.Timestamp("2024-01-03", tz="UTC")
        end = pd.Timestamp("2024-09-05", tz="UTC")
        dates = pd.date_range(start, end, freq="7D")
        frame, frequency, _ = complete_event_panel(
            pd.DataFrame({"date": dates, "value": range(1, len(dates) + 1)}), start, end)
        self.assertEqual(frequency, "weekly")
        self.assertEqual(frame.date.min(), pd.Timestamp("2024-01-08", tz="UTC"))
        self.assertEqual(frame.date.max(), pd.Timestamp("2024-08-26", tz="UTC"))
        self.assertTrue(frame.date.dt.weekday.eq(0).all())

    def test_single_event_does_not_become_a_qualified_zero_filled_history(self):
        frame, _, _ = complete_event_panel(
            pd.DataFrame({"date": ["2024-01-01"], "value": [1]}),
            pd.Timestamp("2024-01-01", tz="UTC"), pd.Timestamp("2026-01-01", tz="UTC"))
        self.assertTrue(frame.empty)

    def test_published_weekly_events_remain_complete_weekly_totals(self):
        start = pd.Timestamp("2024-01-01", tz="UTC")
        end = start + pd.Timedelta(days=210)
        events = pd.DataFrame({"date": pd.date_range(start, periods=210), "value": range(1, 211)})
        self.assertEqual(complete_event_panel(events, start, end)[1], "daily")
        frame, frequency, active_days = complete_event_panel(events, start, end, required_frequency="weekly")
        self.assertEqual((frequency, len(frame), active_days), ("weekly", 30, 210))
        self.assertEqual(frame.value.sum(), events.value.sum())
        self.assertEqual(frame.iloc[0].value, 28)
        sparse = events.iloc[::7]
        skipped, frequency, _ = complete_event_panel(sparse, start, end, required_frequency="daily")
        self.assertTrue(skipped.empty)
        self.assertEqual(frequency, "daily")

    def test_overseas_department_codes_are_iso_territories(self):
        self.assertEqual(french_department_country("974"), "RE")
        self.assertEqual(french_department_country("2A"), "FR")
        with self.assertRaises(ValueError):
            french_department_country("99")

    def test_property_parser_uses_deed_date_and_rejects_unapplied_monthly_deletes(self):
        row = '"id","123456","2024-01-02 00:00","SW1A 1AA","F","N","L","1","","ROAD","","LONDON","WESTMINSTER","GREATER LONDON","A","A"\n'
        parsed = parse_ppd(row.encode())
        self.assertEqual(parsed.iloc[0].postal_district, "SW1A")
        self.assertEqual(parsed.iloc[0].date, pd.Timestamp("2024-01-02", tz="UTC"))
        self.assertEqual(parsed.iloc[0].price, 123456)
        with self.assertRaises(ValueError):
            parse_ppd(row.replace(',"A"\n', ',"D"\n').encode())
        with self.assertRaises(ValueError):
            parse_ppd((row + row).encode())

    def test_property_weekly_median_is_computed_from_transactions_not_daily_medians(self):
        part = pd.DataFrame({"date": pd.to_datetime(["2024-01-01"] * 3 + ["2024-01-02"], utc=True),
                             "price": [10, 20, 30, 100]})
        frame, frequency = property_medians(part)
        self.assertEqual(frequency, "weekly")
        self.assertEqual(frame.iloc[0].value, 25)

    def test_published_weekly_property_medians_use_transactions_after_daily_eligibility(self):
        rows = []
        for week, monday in enumerate(pd.date_range("2024-01-01", periods=30, freq="7D", tz="UTC")):
            rows.extend((monday, value + week) for value in range(10, 71, 10))
            rows.extend((monday + pd.Timedelta(days=day), 100 + week) for day in range(1, 7))
        part = pd.DataFrame(rows, columns=["date", "price"])
        self.assertEqual(property_medians(part)[1], "daily")
        frame, frequency = property_medians(part, required_frequency="weekly")
        self.assertEqual(frequency, "weekly")
        self.assertEqual(len(frame), 30)
        # Median of each week's 13 actual transactions is 70 + week;
        # median of the seven daily medians would incorrectly be 100 + week.
        self.assertEqual(frame.value.tolist(), list(range(70, 100)))
        self.assertTrue(frame.date.dt.weekday.eq(0).all())

    def test_secop_identical_duplicates_collapse_conflicts_are_omitted(self):
        row = dict(id_contrato="A", fecha_de_firma="2024-01-01", departamento="Bogotá",
                   sector="Health", codigo_de_categoria_principal="V1.42101801",
                   tipo_de_contrato="Supply", valor_del_contrato="100")
        data = pd.DataFrame([row, dict(row), dict(row, id_contrato="B"),
                             dict(row, id_contrato="B", valor_del_contrato="101")])
        cleaned, bad = clean_secop_records(data)
        self.assertEqual(cleaned.id_contrato.tolist(), ["A"])
        self.assertEqual(cleaned.family.tolist(), ["4210"])
        self.assertEqual(bad, ["B"])


if __name__ == "__main__":
    unittest.main()
