import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import pandas as pd

from sources_research3 import (
    baseline_identities, clean, github_activity, marine, sec_panels,
    sec_profile_zip, sec_sic_names, sql_literal,
)


def zipped(filename, text):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(filename, text)
    return buffer.getvalue()


class Research3ParserTests(unittest.TestCase):
    def test_clean_preserves_hour_and_real_zero_without_filling_missing_observations(self):
        frame = clean(["2024-02-29T03:00", "2024-02-29T01:00", "2024-02-29T02:00"],
                      [0, 1.5, None])
        self.assertEqual(frame.date.dt.hour.tolist(), [1, 3])
        self.assertEqual(frame.value.tolist(), [1.5, 0.0])
        self.assertEqual(str(frame.date.dtype), "datetime64[ns, UTC]")

    def test_conflicting_source_dates_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            clean(["2024-01-01", "2024-01-01"], [1, 2])

    def test_sec_profiles_use_latest_presample_filing_per_cik(self):
        raw = zipped("sub.txt",
                     "cik\tsic\tcountryba\tstprba\tfiled\taccepted\tname\n"
                     "123\t100\tUS\tCA\t20231001\t2023-10-01 12:00:00\tEarlier\n"
                     "123\t7372\tUS\tNY\t20231201\t2023-12-01 12:00:00\tLatest\n"
                     "123\t9999\tUS\tTX\t20240102\t2024-01-02 12:00:00\tFuture\n"
                     "456\t20\tCA\t\t20231101\t2023-11-01 12:00:00\tCanadian\n")
        frame = sec_profile_zip(raw).set_index("cik")
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.loc["123", "sic"], "7372")
        self.assertEqual(frame.loc["123", "stprba"], "NY")
        self.assertEqual(frame.loc["456", "countryba"], "CA")

    def test_sec_profiles_reject_missing_classification_columns(self):
        with self.assertRaisesRegex(ValueError, "columns"):
            sec_profile_zip(zipped("sub.txt", "cik\tname\n123\tCompany\n"))

    def test_sec_sic_html_first_row_is_header_and_codes_are_zero_padded(self):
        raw = (b"<table><tr><td>SIC Code</td><td>Industry Title</td></tr>"
               b"<tr><td>100</td><td>Agricultural production</td></tr>"
               b"<tr><td>7372</td><td>Software</td></tr></table>")
        self.assertEqual(sec_sic_names(raw), {"0100": "Agricultural production", "7372": "Software"})
        with self.assertRaisesRegex(ValueError, "columns"):
            sec_sic_names(b"<table><tr><td>Code</td></tr><tr><td>1</td></tr></table>")

    def test_sec_panel_joins_cik_and_counts_each_accession_once(self):
        profiles = "cik\tsic\tcountryba\tstprba\tfiled\taccepted\tname\n"
        for cik in range(1, 6):
            profiles += f"{cik}\t7372\tUS\tCA\t20231201\t2023-12-01 12:00:00\tCompany {cik}\n"
        records = []
        for i, day in enumerate(pd.date_range("2024-01-01", periods=125)):
            date = day.strftime("%Y-%m-%d")
            # An unmatched filer is present every day, including a day when
            # the cohort has no filings. That day's legitimate panel count is 0.
            records.append(f"999|Unmatched|8-K|{date}|edgar/data/999/{i}.txt")
            if i == 60:
                continue
            for cik in range(1, 6):
                records.append(f"{cik}|Company {cik}|8-K|{date}|edgar/data/{cik}/{i}.txt")
            if i % 2:
                # One accession indexed under two forms remains one ALL filing.
                records.extend([
                    f"1|Company 1|SC 13G|{date}|edgar/data/1/extra-{i}.txt",
                    f"1|Company 1|SCHEDULE 13G|{date}|edgar/data/1/extra-{i}.txt",
                ])
        index = zipped("master.idx", "header\nCIK|Company Name|Form Type|Date Filed|Filename\n-----\n"
                       + "\n".join(records))
        profile = zipped("sub.txt", profiles)
        sic = b"<table><tr><td>SIC Code</td><td>Industry Title</td></tr><tr><td>7372</td><td>Software</td></tr></table>"
        captured = {}

        class FakeFetcher:
            def get(self, url, *args, **kwargs):
                if url.endswith("2023q4.zip"):
                    return profile, "profile"
                if "classification" in url:
                    return sic, "sic"
                return index, "index"

        def fake_save(sid, frame, **meta):
            captured[sid] = (frame, meta)
            return {"series_id": sid}

        with patch("sources_research3.save", fake_save), patch("sources_research3.time.sleep"):
            sec_panels(FakeFetcher())
        frame, meta = captured["sec_panel_sic_7372_all"]
        self.assertEqual(frame.value.iloc[0], 5)
        self.assertEqual(frame.value.iloc[1], 6)
        self.assertEqual(frame.value.iloc[60], 0)
        self.assertEqual(len(frame), 125)
        self.assertEqual(meta["cohort_companies"], 5)

    def test_github_counts_use_utc_and_openings_without_inventing_zero_days(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "archive/research3_selection"
            folder.mkdir(parents=True)
            selection = {"raw_sha256": "selection", "repos": [
                {"full_name": "owner/repo", "selected_topic": "database", "updated_at": "2022-01-01"}]}
            (folder / "github.json").write_text(json.dumps(selection))
            captured, queries = {}, []

            class FakeFetcher:
                def get(self, url, params, **kwargs):
                    queries.append(params["query"])
                    data = [
                        {"repo_name": "owner/repo", "event_type": "PullRequestEvent", "date": "2024-01-01", "value": "3"},
                        {"repo_name": "owner/repo", "event_type": "PullRequestEvent", "date": "2024-01-03", "value": "5"},
                        {"repo_name": "owner/repo", "event_type": "PushEvent", "date": "2024-01-02", "value": "7"},
                    ]
                    return json.dumps({"data": data, "rows": len(data)}).encode(), "events"

            def fake_save(sid, frame, **meta):
                captured[sid] = (frame, meta)
                return {"series_id": sid}

            with patch("sources_research3.ROOT", root), patch("sources_research3.save", fake_save), \
                 patch("sources_research3.time.sleep"):
                github_activity(FakeFetcher())
            query = queries[0]
            self.assertIn("toDate(created_at,'UTC')", query)
            self.assertIn("count() AS value", query)
            self.assertIn("event_type NOT IN ('PullRequestEvent','IssuesEvent') OR action='opened'", query)
            frame, meta = captured["github_owner_repo_opened_pull_requests"]
            self.assertEqual(frame.date.dt.day.tolist(), [1, 3])
            self.assertEqual(frame.value.tolist(), [3, 5])
            self.assertEqual(meta["dataset_identity"], "GHArchive:owner/repo:PullRequestEvent:opened")
            self.assertEqual(captured["github_owner_repo_push_events"][0].value.tolist(), [7])

    def test_github_truncated_aggregate_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / "archive/research3_selection"
            folder.mkdir(parents=True)
            (folder / "github.json").write_text(json.dumps({"raw_sha256": "selection", "repos": [
                {"full_name": "owner/repo", "selected_topic": "database", "updated_at": "2022-01-01"}]}))
            class FakeFetcher:
                def get(self, *args, **kwargs):
                    return json.dumps({"rows_before_limit_at_least": 4, "rows": 1, "data": []}).encode(), "events"
            with patch("sources_research3.ROOT", root), self.assertRaisesRegex(ValueError, "truncated"):
                github_activity(FakeFetcher())

    def test_sql_literals_escape_quotes_and_backslashes(self):
        self.assertEqual(sql_literal("a'b\\c"), "'a\\'b\\\\c'")

    def marine_selection(self, root, count=2):
        folder = root / "archive/research3_selection"
        folder.mkdir(parents=True)
        ports = [{"portid": f"port{i}", "portname": f"Port {i}", "country": "Country",
                  "lat": 1.25 + i * .01, "lon": -2.25 + i * .01} for i in range(count)]
        (folder / "marine.json").write_text(json.dumps({"ports": ports, "raw_sha256": ["ports"]}))

    def marine_payload(self):
        return {"utc_offset_seconds": 0, "latitude": 1.5, "longitude": -2.5,
                "hourly_units": {"wave_height": "m", "wave_direction": "°", "wave_period": "s"},
                "hourly": {"time": ["2024-01-01T00:00", "2024-01-01T01:00", "2024-01-01T02:00"],
                           "wave_height": [1.0, None, 2.0], "wave_direction": [359.0, 0.0, 1.0],
                           "wave_period": [4.0, 5.0, 6.0]}}

    def test_marine_uses_returned_ocean_cell_and_preserves_hourly_direction(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.marine_selection(root)
            payload = self.marine_payload()
            captured, requested = {}, []
            class FakeFetcher:
                def get(self, url, params, **kwargs):
                    requested.append(params)
                    return json.dumps(payload).encode(), "ocean"
            def fake_save(sid, frame, **meta):
                captured[sid] = (frame, meta)
                return {"series_id": sid}
            with patch("sources_research3.ROOT", root), patch("sources_research3.save", fake_save), \
                 patch("sources_research3.time.sleep"):
                marine(FakeFetcher())
            self.assertEqual(len(captured), 3)  # Two ports resolve to one cell.
            self.assertTrue(all(x["models"] == "era5_ocean" and x["timezone"] == "GMT" for x in requested))
            wave, meta = captured["era5_ocean_1_5000_minus2_5000_wave_height"]
            self.assertEqual(wave.date.dt.hour.tolist(), [0, 2])
            self.assertEqual(meta["latitude"], 1.5)
            self.assertEqual(meta["requested_latitude"], 1.25)
            direction = captured["era5_ocean_1_5000_minus2_5000_wave_direction"][0]
            self.assertEqual(direction.value.tolist(), [359.0, 0.0, 1.0])
            self.assertEqual(meta["dataset_identity"], "ERA5-Ocean:1.5000,-2.5000:wave_height")

    def test_marine_rejects_non_utc_and_changed_units(self):
        for change, message in [({"utc_offset_seconds": 3600}, "UTC"), ({"unit": "ft"}, "units")]:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.marine_selection(root, 1)
                payload = self.marine_payload()
                if "unit" in change:
                    payload["hourly_units"]["wave_height"] = change["unit"]
                else:
                    payload.update(change)
                class FakeFetcher:
                    def get(self, *args, **kwargs):
                        return json.dumps(payload).encode(), "ocean"
                with patch("sources_research3.ROOT", root), self.assertRaisesRegex(ValueError, message):
                    marine(FakeFetcher())

    def test_refresh_baseline_exclusion_does_not_exclude_own_new_series(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pd.DataFrame({"batch": [4, 5], "dataset_identity": ["old", "own"]}).to_parquet(root / "catalog.parquet")
            with patch("sources_research3.ROOT", root):
                self.assertEqual(baseline_identities(), {"old"})


if __name__ == "__main__":
    unittest.main()
