import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sources_digital3 import (
    Omission, coin_selection, parse_coin, parse_cran_packages, parse_wiki_titles, parse_fda_classifications,
    tor_identity, trim_download_history, coin_quality_metadata,
)


class DigitalThirdExpansionTests(unittest.TestCase):
    def test_cran_task_view_uses_package_links_not_arbitrary_names(self):
        raw = b'''<a href="../packages/Foo.bar/index.html">package</a>
                  <a href="https://CRAN.R-project.org/package=Other">other</a>
                  <a href="https://CRAN.R-project.org/package=Foo.bar">duplicate</a>
                  <a href="https://example.com/notapackage">no</a>'''
        self.assertEqual(parse_cran_packages(raw), ["Foo.bar", "Other"])

    def test_download_history_omits_pre_activity_zero_padding(self):
        frame = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=140, tz="UTC"),
                              "value": [0] * 30 + [2] * 110})
        out = trim_download_history(frame)
        self.assertEqual(len(out), 110)
        self.assertEqual(out.date.iloc[0], pd.Timestamp("2025-01-31", tz="UTC"))

    def test_short_lived_package_does_not_pass_with_years_of_leading_zeros(self):
        frame = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=1000, tz="UTC"),
                              "value": [0] * 940 + [5] * 60})
        with self.assertRaisesRegex(Omission, "90 active"):
            trim_download_history(frame)

    def test_download_history_preserves_true_later_zero_and_missing_day(self):
        dates = pd.date_range("2024-01-01", periods=120, tz="UTC").delete(40)
        values = [3] * len(dates)
        values[50] = 0
        out = trim_download_history(pd.DataFrame({"date": dates, "value": values}))
        self.assertEqual(len(out), 119)
        self.assertEqual(int(out.value.eq(0).sum()), 1)
        self.assertNotIn(pd.Timestamp("2024-02-10", tz="UTC"), out.date.tolist())

    def test_coin_blank_is_absent_and_explicit_zero_is_retained(self):
        raw = b"time,TxCnt,SplyCur\n2024-01-01,0,1\n2024-01-02,,2\n2024-01-03,5,3\n"
        out = parse_coin(raw, "TxCnt")
        self.assertEqual(out.date.tolist(), ["2024-01-01", "2024-01-03"])
        self.assertEqual(out.value.tolist(), [0, 5])

    def test_coin_incomplete_discovery_cannot_silently_pass(self):
        with self.assertRaisesRegex(ValueError, "pagination"):
            coin_selection({"data": [], "next_page_url": "https://example.com/next"}, "2024-01-01", "2026-10-18")

    def test_coin_selection_requires_daily_community_and_long_coverage(self):
        spec = {"asset": "testasset", "metrics": [
            {"metric": "AdrActCnt", "frequencies": [{"frequency": "1d", "community": True,
                "min_time": "2020-01-01", "max_time": "2026-10-18"}]},
            {"metric": "TxCnt", "frequencies": [{"frequency": "1d", "community": False,
                "min_time": "2020-01-01", "max_time": "2026-10-18"}]},
            {"metric": "FeeTotNtv", "frequencies": [{"frequency": "1w", "community": True,
                "min_time": "2020-01-01", "max_time": "2026-10-18"}]},
            {"metric": "SplyCur", "frequencies": [{"frequency": "1d", "community": True,
                "min_time": "2026-10-01", "max_time": "2026-10-18"}]},
        ]}
        with patch("sources_digital3.new_identity", return_value=True):
            self.assertEqual(coin_selection({"data": [spec]}, "2024-01-01", "2026-10-18"),
                             [["testasset", "AdrActCnt"]])

    def test_tor_identity_matches_both_prior_collection_formats(self):
        self.assertEqual(tor_identity("bridge", "us"), "Tor/userstats-bridge-country/us/users")
        self.assertEqual(tor_identity("relay", "in"), "Tor/userstats-relay-country/{'country': 'in'}/users/{}")

    def test_wiki_selection_canonicalizes_main_namespace_and_omits_missing(self):
        raw = json.dumps({"query": {"pages": {
            "10": {"title": "Canonical page", "ns": 0},
            "-1": {"title": "Missing", "ns": 0, "missing": ""},
            "11": {"title": "Category:Thing", "ns": 14},
        }}})
        self.assertEqual(parse_wiki_titles(raw), ["Canonical_page"])

    def test_device_code_alias_of_existing_harmonized_name_is_excluded(self):
        raw = json.dumps({"meta": {"results": {"total": 2}}, "results": [
            {"product_code": "ABC", "device_name": "Pump, Infusion"},
            {"product_code": "DEF", "device_name": "Orthopedic Instrument"},
        ]})
        old = ["openFDA/device/event.json/device.openfda.device_name.exact=Pump, Infusion/count=date_received"]
        names, excluded = parse_fda_classifications(raw, old)
        self.assertEqual(excluded, ["ABC"])
        self.assertEqual(names["DEF"], "Orthopedic Instrument")

    def test_device_crosswalk_cannot_silently_use_truncated_page(self):
        raw = json.dumps({"meta": {"results": {"total": 2000}}, "results": [
            {"product_code": "ABC", "device_name": "Pump, Infusion"},
        ]})
        with self.assertRaisesRegex(ValueError, "truncated"):
            parse_fda_classifications(raw, [])

    def test_flow_experimental_notice_applies_to_aggregate_and_components(self):
        for asset in ["flow", "flow_native", "flow_evm"]:
            meta = coin_quality_metadata(asset, "FeeTotNtv")
            self.assertTrue(meta["experimental"])
            self.assertEqual(meta["provider_quality_notice_date"], "2026-01-12")
            self.assertIn("must not be summed", meta["aggregation_dependency"])
        self.assertEqual(coin_quality_metadata("btc", "FeeTotNtv"), {})


if __name__ == "__main__":
    unittest.main()
