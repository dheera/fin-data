import gzip
import json
import unittest

import pandas as pd
import requests

from sources_knowledge5 import (Omission, ProviderAccess, app_category, app_metadata, clean,
                                nvd_metadata, parse_epss, parse_flathub_day,
                                parse_osrs_day, parse_sans, require_activity)


class KnowledgeSourceTests(unittest.TestCase):
    def test_provider_block_stops_queued_network_calls(self):
        class Blocked:
            refresh = False
            calls = 0
            def get(self, *args, **kwargs):
                self.calls += 1
                response = requests.Response()
                response.status_code = 429
                raise requests.HTTPError("rate limited", response=response)
        source = Blocked()
        access = ProviderAccess(source)
        with self.assertRaises(requests.HTTPError):
            access.get("https://example.com/")
        with self.assertRaises(Omission):
            access.get("https://example.com/next")
        self.assertEqual(source.calls, 1)

    def test_flathub_native_tuple_and_missing(self):
        obj = {"date": "2025/01/01", "refs": {"a": {"x86_64": [9, 4], "aarch64": [3, 1]}},
               "ref_by_country": {"a": {"DE": [5, 0], "UNKNOWN": [7, 5]}}}
        selected = [
            {"app": "a", "country": "", "metric": "downloads", "key": "a|downloads"},
            {"app": "a", "country": "", "metric": "updates", "key": "a|updates"},
            {"app": "a", "country": "DE", "metric": "downloads", "key": "a|DE"},
            {"app": "a", "country": "FR", "metric": "downloads", "key": "a|FR"},
        ]
        result = parse_flathub_day(obj, "2025-01-01", selected).set_index("key").value.to_dict()
        self.assertEqual(result, {"a|downloads": 12, "a|updates": 5, "a|DE": 5})

    def test_flathub_partial_present_country_counts_are_quarantined_per_app(self):
        obj = {"date": "2025/01/01",
               "refs": {"a": {"x86_64": [26719, 2000]}, "b": {"x86_64": [100, 20]}},
               "ref_by_country": {"a": {"BR": [1, 0]}, "b": {"BR": [96, 20], "UNKNOWN": [4, 0]}}}
        selected = [
            {"app": "a", "country": "", "metric": "downloads", "key": "a|downloads"},
            {"app": "a", "country": "BR", "metric": "downloads", "key": "a|BR"},
            {"app": "b", "country": "BR", "metric": "downloads", "key": "b|BR"},
        ]
        result = parse_flathub_day(obj, "2025-01-01", selected).set_index("key").value.to_dict()
        self.assertEqual(result, {"a|downloads": 26719, "b|BR": 96})
        obj["ref_by_country"]["b"]["BR"][0] = 95
        result = parse_flathub_day(obj, "2025-01-01", selected).set_index("key").value.to_dict()
        self.assertEqual(result, {"a|downloads": 26719})

    def test_flathub_rejects_wrong_day(self):
        with self.assertRaises(ValueError):
            parse_flathub_day({"date": "2025/01/02"}, "2025-01-01", [])

    def test_appstream_native_categories(self):
        raw = gzip.compress(b'<components><component type="desktop-application"><id>a</id><name>Example</name><categories><category>Security</category><category>Network</category></categories></component></components>')
        apps = app_metadata(raw)
        self.assertEqual(apps["a"]["name"], "Example")
        self.assertEqual(app_category("a", apps["a"]), "desktop_security_privacy_demand")

    def test_osrs_side_semantics_null_and_zero(self):
        stamp = int(pd.Timestamp("2025-01-01", tz="UTC").timestamp())
        result = parse_osrs_day({"timestamp": stamp, "data": {
            "2": {"highPriceVolume": 0, "lowPriceVolume": 8, "avgHighPrice": None},
            "3": {"highPriceVolume": 5},
        }}, "2025-01-01", {"2": {}})
        self.assertEqual(result.field.tolist(), ["highPriceVolume", "lowPriceVolume"])
        self.assertEqual(result.value.tolist(), [0, 8])
        self.assertTrue((result.date == pd.Timestamp("2025-01-01", tz="UTC")).all())

    def test_osrs_rejects_wrong_period(self):
        with self.assertRaises(ValueError):
            parse_osrs_day({"timestamp": 1, "data": {}}, "2025-01-01", {})

    def test_sans_retains_native_counts_and_gaps(self):
        result = parse_sans({
            "0": {"date": "2025-01-01", "records": 100, "sources": 7, "targets": 5},
            "1": {"date": "2025-01-03", "records": 80, "sources": 6, "targets": 4},
            "startdate": "2025-01-01", "enddate": "2025-01-03", "port": 443,
        }, "2025-01-01", "2025-01-04")
        self.assertEqual(len(result), 2)
        self.assertEqual(result.records.tolist(), [100, 80])
        with self.assertRaises(ValueError):
            parse_sans({"0": {"date": "2025-01-01", "records": -1, "sources": 7, "targets": 5}},
                       "2025-01-01", "2025-01-04")

    def test_nvd_excludes_environment_prerequisites(self):
        obj = {"vulnerabilities": [{"cve": {
            "id": "CVE-2025-1", "vulnStatus": "Analyzed",
            "published": "2025-01-01T00:00:00",
            "descriptions": [{"lang": "en", "value": "test"}],
            "configurations": [{"nodes": [{"cpeMatch": [
                {"vulnerable": False, "criteria": "cpe:2.3:o:environment:os:*:*:*:*:*:*:*:*"},
                {"vulnerable": True, "criteria": "cpe:2.3:a:vendor:product:*:*:*:*:*:*:*:*"}]}]}],
            "metrics": {"cvssMetricV31": [{"cvssData": {"attackVector": "NETWORK"}}]},
        }}]}
        info = nvd_metadata(obj)["CVE-2025-1"]
        self.assertEqual(info["products"], [["a", "vendor", "product"]])
        self.assertEqual(info["attack_vector"], "NETWORK")

    def test_epss_header_date_and_model(self):
        raw = gzip.compress(b"#model_version:v2025.03.14,score_date:2025-04-01T12:03:10Z\ncve,epss,percentile\nCVE-2024-1,0.12345,0.9\nCVE-2024-2,0.23456,0.95\n")
        frame, header = parse_epss(raw, "2025-04-01", {"CVE-2024-1"})
        self.assertEqual(frame.value.tolist(), [0.12345])
        self.assertNotIn("percentile", frame.columns)
        self.assertEqual(header["model_version"], "v2025.03.14")
        with self.assertRaises(ValueError):
            parse_epss(raw, "2025-04-02", {"CVE-2024-1"})

    def test_epss_rejects_invalid_probability(self):
        raw = gzip.compress(b"#model_version:v2025.03.14,score_date:2025-04-01T12:03:10Z\ncve,epss,percentile\nCVE-2024-1,1.1,0.9\n")
        with self.assertRaises(ValueError):
            parse_epss(raw, "2025-04-01", {"CVE-2024-1"})

    def test_sparse_activity_does_not_qualify_on_zeros(self):
        frame = pd.DataFrame({"date": pd.date_range("2025-01-01", periods=200, tz="UTC"),
                              "value": [0.0] * 199 + [1.0]})
        with self.assertRaises(Omission):
            require_activity(frame)

    def test_conflicting_duplicate_dates_rejected(self):
        with self.assertRaises(ValueError):
            clean(["2025-01-01", "2025-01-01"], [1, 2])
        self.assertEqual(len(clean(["2025-01-01", "2025-01-01"], [1, 1])), 1)


if __name__ == "__main__":
    unittest.main()
