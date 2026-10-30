import io
import unittest
import zipfile

import pandas as pd

from sources_macro2 import (
    canonical_sec_records, latest_repo_points, parse_cp_zip, parse_sec_index, sec_category,
)


def zipped(name, text):
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w") as archive:
        archive.writestr(name, text)
    return result.getvalue()


class Macro2Tests(unittest.TestCase):
    def test_cp_omits_weekly_and_suppressed_observations(self):
        xml = """<root xmlns:v="https://example.org/CP_VOL">
        <v:Series FREQ="9" SERIES_NAME="daily"><v:Obs OBS_STATUS="A" TIME_PERIOD="2024-01-02" OBS_VALUE="15"/>
        <v:Obs OBS_STATUS="ND" TIME_PERIOD="2024-01-03" OBS_VALUE="-9999"/></v:Series>
        <v:Series FREQ="5" SERIES_NAME="weekly"><v:Obs OBS_STATUS="A" TIME_PERIOD="2024-01-02" OBS_VALUE="99"/></v:Series></root>"""
        rows = list(parse_cp_zip(zipped("CP_data.xml", xml)))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3], [("2024-01-02", 15.0)])

    def test_final_repo_null_does_not_reveal_withheld_preliminary_value(self):
        result = latest_repo_points(
            [("2024-01-01", 12), ("2024-01-02", 10), ("2024-01-03", 14)],
            [("2024-01-01", 13), ("2024-01-02", None)],
        )
        self.assertEqual(result, [("2024-01-01", 13), ("2024-01-02", None), ("2024-01-03", 14)])

    def test_sec_multiform_submission_and_form_rename(self):
        records = pd.DataFrame([
            ("SC 13G", "2024-01-02", "a"),
            ("SCHEDULE 13G", "2024-01-02", "a"),
            ("SC TO-T", "2024-01-02", "a"),
        ], columns=["form", "date", "filename"])
        result = canonical_sec_records(records)
        self.assertEqual(len(result), 2)
        self.assertEqual(set(result.form), {"SC 13G", "SC TO-T"})
        conflict = pd.concat([records, pd.DataFrame(
            [("SC 13G", "2024-01-03", "a")], columns=records.columns)])
        with self.assertRaisesRegex(ValueError, "conflicting"):
            canonical_sec_records(conflict)

    def test_sec_parse_and_categories(self):
        content = "Header\nCIK|Company Name|Form Type|Date Filed|Filename\n-----\n1|Example|4|2024-01-02|file.txt\n"
        self.assertEqual(parse_sec_index(zipped("master.idx", content)).iloc[0]["form"], "4")
        self.assertEqual(sec_category("DEF 14A"), "corporate_disclosures")
        self.assertEqual(sec_category("485BPOS"), "fund_disclosures")
        self.assertEqual(sec_category("4"), "ownership_disclosures")
        self.assertEqual(sec_category("424B2"), "capital_formation")


if __name__ == "__main__":
    unittest.main()
