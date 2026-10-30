import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd

from sources_physical3 import (
    EPA_COLUMNS, balanced, clean_frame, main, parse_epa, parse_hydat, parse_usgs,
    require_daily, EligibilityOmission, usgs_primary, old_identities, save, usgs,
)


class Physical3Tests(unittest.TestCase):
    def epa_zip(self, records):
        frame = pd.DataFrame(records).reindex(columns=EPA_COLUMNS)
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w") as archive:
            archive.writestr("daily.csv", frame.to_csv(index=False))
        return out.getvalue()

    def test_epa_retains_ambient_event_and_rejects_policy_duplicate(self):
        base = {"State Code": 1, "County Code": 3, "Site Num": 12, "POC": 1,
                "Parameter Code": 42602, "Sample Duration": "1 HOUR",
                "Pollutant Standard": "NO2 1-hour 2010", "Date Local": "2024-02-29",
                "Observation Count": 24, "Arithmetic Mean": 17.0}
        rows = [dict(base, **{"Event Type": "Included"}),
                dict(base, **{"Event Type": "Excluded", "Arithmetic Mean": 9.0}),
                dict(base, **{"Event Type": "Included", "Sample Duration": "8 HOUR"}),
                dict(base, **{"Event Type": "Included", "Observation Count": 0})]
        frame = parse_epa(self.epa_zip(rows), "42602")
        self.assertEqual(frame["Arithmetic Mean"].tolist(), [17.0])
        self.assertEqual(frame.site.tolist(), ["010030012"])
        self.assertEqual(frame.monitor.tolist(), ["010030012_1"])

    def test_epa_pm_keeps_raw_hourly_summary_not_block_duplicate(self):
        base = {"State Code": 6, "County Code": 37, "Site Num": 1, "POC": 3,
                "Parameter Code": 88101, "Sample Duration": "1 HOUR",
                "Pollutant Standard": None, "Date Local": "2024-01-01",
                "Observation Count": 24, "Arithmetic Mean": -0.5}
        rows = [base, dict(base, **{"Sample Duration": "24-HR BLK AVG",
                                   "Pollutant Standard": "PM25 24-hour 2012"})]
        frame = parse_epa(self.epa_zip(rows), "88101")
        self.assertEqual(frame["Arithmetic Mean"].tolist(), [-0.5])

    def usgs_payload(self):
        return {"value": {"timeSeries": [{
            "sourceInfo": {"siteCode": [{"value": "01100000"}], "siteName": "River station",
                           "geoLocation": {"geogLocation": {"latitude": 42.0, "longitude": -71.0}}},
            "variable": {"variableCode": [{"value": "00060"}], "noDataValue": -999999.0,
                         "options": {"option": [{"name": "Statistic", "optionCode": "00003"}]},
                         "variableName": "Discharge", "unit": {"unitCode": "ft3/s"}},
            "values": [{"method": [{"methodID": 1}], "value": [
                {"dateTime": "2024-01-01T00:00:00.000", "value": "0", "qualifiers": ["A"]},
                {"dateTime": "2024-01-02T00:00:00.000", "value": "-999999", "qualifiers": []},
                {"dateTime": "2024-01-03T00:00:00.000", "value": "17", "qualifiers": ["P"]},
            ]}],
        }]}}

    def test_usgs_omits_no_data_preserves_real_zero_and_qualifiers(self):
        result = parse_usgs(self.usgs_payload())
        self.assertEqual(result[0]["frame"].value.tolist(), [0.0, 17.0])
        self.assertEqual(result[0]["qualifiers"], ["A", "P"])
        self.assertEqual(result[0]["frame"].date.dt.day.tolist(), [1, 3])

    def test_usgs_conflicting_instruments_require_primary_series(self):
        payload = self.usgs_payload()
        second = copy.deepcopy(payload["value"]["timeSeries"][0]["values"][0])
        second["method"] = [{"methodID": 2}]
        second["value"][0]["value"] = "99"
        payload["value"]["timeSeries"][0]["values"].append(second)
        result = parse_usgs(payload)
        self.assertTrue(result[0]["requires_primary_query"])

    def test_usgs_censored_measurements_are_not_exact_values(self):
        payload = self.usgs_payload()
        payload["value"]["timeSeries"][0]["values"][0]["value"][2]["qualifiers"] = ["A", "<"]
        result = parse_usgs(payload)
        self.assertEqual(result[0]["frame"].value.tolist(), [0.0])

    def test_usgs_wrong_statistic_is_rejected(self):
        payload = self.usgs_payload()
        payload["value"]["timeSeries"][0]["variable"]["options"]["option"][0]["optionCode"] = "00001"
        with self.assertRaisesRegex(ValueError, "statistic"):
            parse_usgs(payload)

    def test_usgs_primary_checks_identity_and_pagination(self):
        meta = {"time_series_id": "primary-id", "parameter": "00060", "site": "01100000"}
        class FakeFetcher:
            def get(self, *args, **kwargs):
                return json.dumps({"features": [], "links": [{"rel": "next", "href": "next"}]}).encode(), "digest"
        with self.assertRaisesRegex(ValueError, "pagination"):
            usgs_primary(FakeFetcher(), meta)

    def test_hydat_requires_real_schema_and_no_padding(self):
        csv = b"STATION_NUMBER,DATE,DISCHARGE,LEVEL\n01AA001,2024-01-01,12,\n01AA001,2024-01-04,14,4.5\n"
        frame = parse_hydat(csv)
        level = clean_frame(frame.DATE, frame.LEVEL)
        self.assertEqual(level.value.tolist(), [4.5])
        self.assertEqual(level.date.dt.day.tolist(), [4])
        with self.assertRaisesRegex(ValueError, "schema"):
            parse_hydat(b"date,value\n2024-01-01,1\n")

    def test_sparse_or_conflicting_data_cannot_pass(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            clean_frame(["2024-01-01", "2024-01-01"], [1, 2])
        sparse = clean_frame(pd.date_range("2024-01-01", periods=100, freq="7D"), range(100))
        with self.assertRaises(EligibilityOmission):
            require_daily(sparse)

    def test_selection_balances_geography_and_is_deterministic(self):
        items = [{"state": "A", "rank": x} for x in range(5)] + [{"state": "B", "rank": 5}]
        result = balanced(items, lambda x: x["state"], 3, lambda x: x["rank"])
        self.assertEqual([x["state"] for x in result], ["A", "B", "A"])

    def test_own_publication_remains_refreshable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pd.DataFrame({"batch": [4, 5], "dataset_identity": ["old", "own_new"]}).to_parquet(root / "catalog.parquet")
            with patch("sources_physical3.ROOT", root):
                self.assertEqual(old_identities(), {"old"})

    def test_newly_ineligible_candidate_is_preserved_outside_publication_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            staged = root / "archive/staging/physical3/test_series.parquet"
            staged.parent.mkdir(parents=True)
            staged.write_bytes(b"previous staged artifact")
            published = root / "data/river/test_series.parquet"
            published.parent.mkdir(parents=True)
            published.write_bytes(b"published data")
            constant = clean_frame(pd.date_range("2024-01-01", periods=100), [1] * 100)
            with patch("sources_physical3.ROOT", root), self.assertRaises(EligibilityOmission):
                save("test_series", constant)
            self.assertFalse(staged.exists())
            archived = list((root / "archive/rejected/physical3").glob("*/test_series.parquet"))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_bytes(), b"previous staged artifact")
            self.assertEqual(published.read_bytes(), b"published data")

    def test_usgs_shorter_height_alias_stays_excluded_even_in_pinned_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sid = "usgs_12453690_gauge_height"
            staged = root / f"archive/staging/physical3/{sid}.parquet"
            staged.parent.mkdir(parents=True)
            staged.write_bytes(b"older duplicate candidate")
            common_meta = {"site": "12453690", "time_series_id": "id", "metadata_raw_sha256": ["metadata"]}
            selected = [dict(common_meta, parameter=p) for p in ["00065", "62614"]]
            payload = self.usgs_payload()
            original = payload["value"]["timeSeries"][0]
            original["sourceInfo"]["siteCode"][0]["value"] = "12453690"
            original["variable"]["unit"]["unitCode"] = "ft"
            payload["value"]["timeSeries"] = []
            for parameter in ["00065", "62614"]:
                item = copy.deepcopy(original)
                item["variable"]["variableCode"][0]["value"] = parameter
                payload["value"]["timeSeries"].append(item)
            class FakeFetcher:
                def get(self, *args, **kwargs):
                    return json.dumps(payload).encode(), "daily"
            def fake_pin(name, build):
                return selected if name == "usgs" else []
            def fake_save(series_id, frame, **meta):
                return {"series_id": series_id}
            with patch("sources_physical3.ROOT", root), patch("sources_physical3.pin", fake_pin), \
                 patch("sources_physical3.save", fake_save), patch("builtins.print"):
                rows, omissions = usgs(FakeFetcher())
            self.assertEqual([x["series_id"] for x in rows], ["usgs_12453690_lake_elevation"])
            self.assertTrue(any(sid in x["item"] and "Semantic alias" in x["error"] for x in omissions))
            self.assertFalse(staged.exists())
            self.assertEqual(len(list((root / "archive/rejected/physical3").glob(f"*/{sid}.parquet"))), 1)

    def test_cli_distinguishes_source_failures_from_eligibility(self):
        for errors, expected in [
            ([{"kind": "expected_omission", "error": "insufficient data"}], 0),
            ([{"kind": "unexpected_error", "error": "HTTP error"}], 1),
        ]:
            with tempfile.TemporaryDirectory() as tmp:
                with patch("sources_physical3.ROOT", Path(tmp)), \
                     patch("sources_physical3.hydat", lambda _: ([], errors)), \
                     patch("builtins.print"):
                    self.assertEqual(main(["--provider", "hydat"]), expected)


if __name__ == "__main__":
    unittest.main()
