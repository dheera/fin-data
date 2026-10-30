import io
import unittest
import zipfile

import pandas as pd
from country_codes import normalize_country_codes

from sources_research4 import (
    ISO3_COUNTRIES, cftc_fields, cftc_numeric, clean, gdelt_aggregate, gdelt_panel, nzta_frame,
)


class Research4Tests(unittest.TestCase):
    def test_country_codes_are_consistent_and_do_not_invent_regions(self):
        self.assertEqual(normalize_country_codes(["NZL", "NZ", "USA", "XKX"]),
                         ["NZ", "US", "XK"])
        with self.assertRaises(ValueError):
            normalize_country_codes(["EU"])
        with self.assertRaises(ValueError):
            normalize_country_codes("US")

    def gdelt_zip(self, records):
        columns = ["GLOBALEVENTID", "SQLDATE", "Actor1CountryCode", "Actor2CountryCode",
                   "Actor1Type1Code", "EventBaseCode", "QuadClass", "AvgTone",
                   "ActionGeo_CountryCode", "DATEADDED"]
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as z:
            z.writestr("20261019.export.CSV",
                       "\n".join("\t".join(str(r.get(k, "")) for k in columns) for r in records))
        return output.getvalue(), columns

    def record(self, **changes):
        row = dict(GLOBALEVENTID="1", SQLDATE="20251019", Actor1CountryCode="FRA",
                   Actor2CountryCode="DEU", Actor1Type1Code="BUS", EventBaseCode="42",
                   QuadClass="1", AvgTone="-2.5", ActionGeo_CountryCode="FR",
                   DATEADDED="20261019")
        row.update(changes)
        return row

    def test_gdelt_uses_discovery_date_and_unique_event_count(self):
        raw, columns = self.gdelt_zip([self.record(), self.record(GLOBALEVENTID="2", AvgTone="1.5")])
        result = gdelt_aggregate(raw, columns, "2026-10-19", {"FR": "France"}, ISO3_COUNTRIES)
        event = result[result.kind.eq("location_event")].iloc[0]
        self.assertEqual(event.date, "2026-10-19")
        self.assertEqual(event.detail, "042")
        self.assertEqual(event["count"], 2)
        self.assertEqual(event.tone, -.5)

    def test_gdelt_excludes_cameo_region_from_country_panels(self):
        raw, columns = self.gdelt_zip([self.record(Actor1CountryCode="AFR")])
        result = gdelt_aggregate(raw, columns, "2026-10-19", {"FR": "France"}, ISO3_COUNTRIES)
        self.assertEqual(set(result.kind), {"location_event"})

    def test_gdelt_rejects_wrong_archive_date_and_repeated_event_ids(self):
        for records in [[self.record(DATEADDED="20261018")], [self.record(), self.record()]]:
            raw, columns = self.gdelt_zip(records)
            with self.assertRaises(ValueError):
                gdelt_aggregate(raw, columns, "2026-10-19", {"FR": "France"}, ISO3_COUNTRIES)

    def test_gdelt_zeros_only_completed_archives_and_does_not_fill_missing_tone(self):
        rows = pd.DataFrame({"date": ["2024-01-01", "2024-01-04"],
                             "count": [3, 1], "tone": [-2., 1.]})
        completed = ["2024-01-01", "2024-01-03", "2024-01-04"]
        counts = gdelt_panel(rows, "count", completed)
        self.assertEqual(counts.value.tolist(), [3, 0, 1])
        self.assertNotIn(pd.Timestamp("2024-01-02", tz="UTC"), set(counts.date))
        self.assertEqual(len(gdelt_panel(rows, "tone", completed)), 2)

    def test_gdelt_restricting_geographic_keys_preserves_retained_panel_statistics(self):
        raw, columns = self.gdelt_zip([self.record(), self.record(GLOBALEVENTID="2", ActionGeo_CountryCode="OS")])
        before = gdelt_aggregate(raw, columns, "2026-10-19", {"FR": "France", "OS": "Oceans"}, ISO3_COUNTRIES)
        after = gdelt_aggregate(raw, columns, "2026-10-19", {"FR": "France"}, ISO3_COUNTRIES)
        retained = before[~(before.kind.eq("location_event") & before.country.eq("OS"))]
        pd.testing.assert_frame_equal(retained.reset_index(drop=True), after.reset_index(drop=True))

    def test_cftc_uses_all_contract_levels_not_changes_percentages_or_crop_aliases(self):
        names = ["Open_Interest_All", "Swap__Positions_Short_All", "M_Money_Positions_Long_Old",
                 "Change_in_M_Money_Long_All", "Pct_of_OI_M_Money_Long_All",
                 "Traders_Tot_All", "Traders_Tot_Rept_Long_All", "Tot_Rept_Positions_Long_All",
                 "Conc_Gross_LE_4_TDR_Long_All", "Conc_Net_LE_8_TDR_Long_All"]
        fields = cftc_fields([{"name": name, "fieldName": name.lower()} for name in names])
        self.assertEqual(set(fields), {"open_interest_all", "swap__positions_short_all",
                                      "traders_tot_all", "conc_gross_le_4_tdr_long_all"})

    def test_cftc_suppression_is_missing_and_zero_is_real(self):
        result = cftc_numeric(pd.Series(["0", ".", "11", None]))
        self.assertEqual(result.iloc[0], 0)
        self.assertTrue(pd.isna(result.iloc[1]))
        self.assertEqual(result.iloc[2], 11)
        self.assertTrue(pd.isna(result.iloc[3]))
        with self.assertRaises(ValueError):
            cftc_numeric(pd.Series(["unrecognized marker"]))

    def test_nzta_keeps_native_epoch_fractional_class_counts_and_zero(self):
        rows = [dict(OBJECTID=i, startDate=1704067200000 + i * 86400000, siteID=1,
                     regionName="region", SiteRef="001", classWeight="Heavy",
                     siteDescription="test", laneNumber=1, flowDirection=2, trafficCount=count)
                for i, count in enumerate([0, 58.5])]
        result = nzta_frame(rows)
        self.assertEqual(result.trafficCount.tolist(), [0, 58.5])
        self.assertEqual(result.date.iloc[0], pd.Timestamp("2024-01-01", tz="UTC"))
        with self.assertRaises(ValueError):
            nzta_frame(rows + [rows[0]])
        rows[1]["trafficCount"] = -1
        with self.assertRaises(ValueError):
            nzta_frame(rows)

    def test_clean_rejects_conflicting_measurements_without_filling_gaps(self):
        dates = ["2024-01-01", "2024-01-01", "2024-01-03"]
        self.assertEqual(len(clean(dates, [1, 1, 0])), 2)
        with self.assertRaises(ValueError):
            clean(dates, [1, 2, 0])


if __name__ == "__main__":
    unittest.main()
