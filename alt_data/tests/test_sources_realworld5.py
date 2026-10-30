import io
import unittest
from unittest.mock import patch

import pandas as pd

import sources_realworld5 as sources
from sources_realworld5 import (complete_fire_weeks, eea_parse, inpe_aggregate, lice_parse,
                                phytoplankton_parse, PHYTO_METRICS)


class PhysicalSourceTests(unittest.TestCase):
    def test_refresh_pins_native_ids_cadence_and_serialized_country_codes(self):
        catalog = pd.DataFrame([
            dict(series_id="old", batch=6, dataset_identity="old_native", content_sha256="old_hash",
                 frequency="daily", country_codes='["US"]'),
            dict(series_id="eea_selected", batch=7, dataset_identity="chemistry", content_sha256="chem_hash",
                 frequency="hourly", country_codes='["GB"]'),
            dict(series_id="inpe_selected", batch=7, dataset_identity="fire", content_sha256="fire_hash",
                 frequency="weekly", country_codes='["BR"]')])
        prior = sources.BASELINE
        try:
            sources.BASELINE = None
            with patch.object(sources.pd, "read_parquet", return_value=catalog):
                selection = sources.baseline()
            self.assertEqual(selection["series_id"], {"old"})
            self.assertEqual(selection["pinned_eea_countries"], ["GB"])
            self.assertEqual(selection["pinned_frequency"]["inpe_selected"], "weekly")
            self.assertEqual(selection["pinned_ids"], {"eea_selected", "inpe_selected"})
        finally:
            sources.BASELINE = prior

    def eea_rows(self):
        return pd.DataFrame([
            dict(Samplingpoint="IS/test", Pollutant=11, Start="2025-06-01 01:00:00",
                 End="2025-06-01 02:00:00", Value="3.2", Unit="ug.m-3", AggType="hour",
                 Validity=1, Verification=3, ResultTime="2025-06-01 03:00:00"),
            dict(Samplingpoint="IS/test", Pollutant=11, Start="2025-06-01 02:00:00",
                 End="2025-06-01 03:00:00", Value="0", Unit="ug.m-3", AggType="hour",
                 Validity=-99, Verification=3, ResultTime="2025-06-01 03:00:00"),
        ])

    def test_eea_fixed_timezone_and_invalid_maintenance(self):
        rows = eea_parse(self.eea_rows())
        self.assertEqual(len(rows), 1)
        data = rows[0][1]
        self.assertEqual(len(data), 1)
        self.assertEqual(str(data.date.iloc[0]), "2025-06-01 00:00:00+00:00")
        self.assertEqual(data.value.iloc[0], 3.2)

    def test_eea_latest_revision_and_conflicting_tie(self):
        old = self.eea_rows().iloc[[0]]
        new = old.copy()
        new["Value"] = "7"
        new["ResultTime"] = "2025-06-02 12:00:00"
        self.assertEqual(eea_parse(pd.concat([old, new]))[0][1].value.iloc[0], 7)
        new["ResultTime"] = old.ResultTime.iloc[0]
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            eea_parse(pd.concat([old, new]))

    def test_eea_below_detection_values_preserved_not_zeroed(self):
        rows = self.eea_rows().iloc[[0]].copy()
        rows["Validity"] = 3
        rows["Value"] = "0.075"
        self.assertEqual(eea_parse(rows)[0][1].value.iloc[0], .075)

    def fire_rows(self):
        return pd.DataFrame([
            dict(id="a", data_hora_gmt="2025-01-01 10:00:00", satelite="NOAA-20",
                 pais="Brasil", estado="BAHIA", municipio="A", pais_id="33",
                 estado_id="29", municipio_id="1", frp=5.5),
            dict(id="b", data_hora_gmt="2025-01-01 11:00:00", satelite="NOAA-20",
                 pais="Brasil", estado="BAHIA", municipio="A", pais_id="33",
                 estado_id="29", municipio_id="1", frp=None),
            dict(id="c", data_hora_gmt="2025-01-02 12:00:00", satelite="NOAA-21",
                 pais="Brasil", estado="BAHIA", municipio="A", pais_id="33",
                 estado_id="29", municipio_id="1", frp=8.5),
        ])

    def test_fire_platforms_and_missing_power(self):
        raw = self.fire_rows().to_csv(index=False).encode()
        table = inpe_aggregate(raw, "202501")
        native = table[table.level == "municipality"].set_index("satelite")
        self.assertEqual(native.loc["NOAA-20", "detections"], 2)
        self.assertTrue(pd.isna(native.loc["NOAA-20", "frp_sum"]))
        self.assertEqual(native.loc["NOAA-21", "frp_sum"], 8.5)

    def test_fire_duplicate_id_is_not_extra_detection(self):
        frame = self.fire_rows().iloc[[0]]
        table = inpe_aggregate(pd.concat([frame, frame]).to_csv(index=False).encode(), "202501")
        self.assertTrue(table.detections.eq(1).all())
        conflicting = frame.copy()
        conflicting["frp"] = 123
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            inpe_aggregate(pd.concat([frame, conflicting]).to_csv(index=False).encode(), "202501")

    def test_fire_month_must_match_native_dates(self):
        with self.assertRaisesRegex(ValueError, "outside"):
            inpe_aggregate(self.fire_rows().to_csv(index=False).encode(), "202502")

    def test_fire_week_sum_preserves_zeros_and_rejects_partial_outage_or_missing_frp(self):
        frame = pd.DataFrame({"date": pd.date_range("2025-01-06", periods=23, tz="UTC"),
                              "value": [0., 0., 0., 2., 4., 0., 0.] * 3 + [99., 99.]})
        # An absent satellite reporting day invalidates its week.
        frame = frame.drop(index=9)
        # Missing FRP invalidates its week as well, instead of becoming zero.
        frame.loc[17, "value"] = float("nan")
        weekly = complete_fire_weeks(frame)
        self.assertEqual(len(weekly), 1)
        self.assertEqual(weekly.date.iloc[0], pd.Timestamp("2025-01-06", tz="UTC"))
        self.assertEqual(weekly.value.iloc[0], 6.)

    def test_lice_no_count_zero_is_not_an_observation(self):
        base = {"Site No": "FS1", "Site Name": "Test", "Business Name": "Business",
                "Year": "2025", "Week No": "1", "Week Beginning": "2024-12-30",
                "Weekly Average AF": "0", "Reason For No Count": ""}
        no_count = dict(base, **{"Week No": "2", "Week Beginning": "2025-01-06",
                                 "Reason For No Count": "Fallow"})
        mismatch = dict(base, **{"Week No": "3", "Week Beginning": "2025-01-27"})
        panels = lice_parse([pd.DataFrame([base, no_count, mismatch])])
        self.assertEqual(len(panels[0][1]), 1)
        self.assertEqual(panels[0][1].value.iloc[0], 0)

    def plankton_rows(self):
        row = dict(Id="1", CollectedTimestamp="01/04/2025 09:00:00", Sin="A-01",
                   SiteName="Site", AreaName="Area", LocalAuthorityName="Authority",
                   LingulodiniumPolyedrumResultValue="40")
        row.update({field: " 0" for field in PHYTO_METRICS})
        row["LingulaulaxPolyedraResultValue"] = " "
        row["PseudoNitzschiaResultValue"] = "120"
        return pd.DataFrame([row])

    def test_plankton_synonyms_one_series_and_flags_missing(self):
        rows = self.plankton_rows()
        rows["AlexandriumResultValue"] = "<20"
        panels = phytoplankton_parse(rows)
        mapping = {p[1]: p[3] for p in panels}
        self.assertNotIn("AlexandriumResultValue", mapping)
        self.assertNotIn("LingulodiniumPolyedrumResultValue", mapping)
        self.assertEqual(mapping["LingulaulaxPolyedraResultValue"].value.iloc[0], 40)

    def test_plankton_distinct_same_day_samples_mean_and_duplicate_ids_rejected(self):
        first = self.plankton_rows()
        second = first.copy()
        second["Id"] = "2"
        second["PseudoNitzschiaResultValue"] = "240"
        panels = phytoplankton_parse(pd.concat([first, second]))
        sample = next(p for p in panels if p[1] == "PseudoNitzschiaResultValue")
        self.assertEqual(sample[3].value.iloc[0], 180)
        self.assertEqual(sample[-1], 1)
        with self.assertRaisesRegex(ValueError, "Repeated"):
            phytoplankton_parse(pd.concat([first, first]))


if __name__ == "__main__":
    unittest.main()
