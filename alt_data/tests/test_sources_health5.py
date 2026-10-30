import json

import pandas as pd
import unittest

import sources_health5 as health




def study(locations, date="2025-01-08", kind="ACTUAL"):
    return {
        "protocolSection": {
            "statusModule": {"studyFirstPostDateStruct": {"date": date, "type": kind}},
            "contactsLocationsModule": {"locations": [{"country": c} for c in locations]},
            "designModule": {"studyType": "INTERVENTIONAL", "phases": ["PHASE2"]},
            "armsInterventionsModule": {"interventions": [{"type": "DRUG"}, {"type": "DRUG"}]},
        },
        "derivedSection": {"conditionBrowseModule": {"meshes": [
            {"id": "D1", "term": "Condition A"}, {"id": "D1", "term": "Condition A"}
        ]}},
    }


class HealthSourceTests(unittest.TestCase):
    def test_source_nulls_are_not_zeros_and_conflicting_dates_fail(self):
        part = pd.DataFrame({"day": ["2025-01-06", "2025-01-13", "2025-01-20"],
                             "count": [0, None, 3]})
        frame = health.reported_frame(part, "day", "count")
        assert frame.value.tolist() == [0, 3]
        part = pd.concat([part, pd.DataFrame({"day": ["2025-01-20"], "count": [4]})])
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            health.reported_frame(part, "day", "count")


    def test_trials_deduplicate_locations_and_facets_without_allocating_patients(self):
        events, names, labels, missing = health.trial_events(
            [study(["France", "France", "United States", "Unknown"])],
            {"france": "FR", "united states": "US"})
        for code in ["GLOBAL", "FR", "US"]:
            assert events[code, "condition", "D1"] == {"2025-01-06": 1}
            assert events[code, "intervention_type", "DRUG"] == {"2025-01-06": 1}
            assert events[code, "all", "all"] == {"2025-01-06": 1}
        assert missing == {"Unknown": 1}
        assert names["FR"] == "France"
        assert labels["D1"] == "Condition A"


    def test_trials_reject_estimated_or_month_only_publication_dates(self):
        events, *_ = health.trial_events(
            [study(["France"], "2025-01"), study(["France"], kind="ESTIMATED")], {"france": "FR"})
        assert not events


    def test_country_mapping_uses_named_columns_and_keeps_namibia(self):
        class FakeFetcher:
            def get(self, *args, **kwargs):
                return b"COUNTRY_AREA_TERRITORY,ISO2\nFrance,FR\nNamibia,NA\n", "hash"
        mapping = health.country_names(FakeFetcher())
        assert mapping["france"] == "FR"
        assert mapping["namibia"] == "NA"


    def test_incomplete_paginated_trial_window_is_rejected(self):
        class FakeFetcher:
            def get(self, *args, **kwargs):
                return json.dumps({"totalCount": 2, "studies": [{
                    "protocolSection": {"identificationModule": {"nctId": "NCT1"}}
                }]}).encode(), "hash"
        with self.assertRaisesRegex(ValueError, "differs"):
            health.trial_page(FakeFetcher(), 2025, "2025-12-31")


    def test_duplicate_paginated_trial_ids_are_rejected(self):
        class FakeFetcher:
            calls = 0
            def get(self, *args, **kwargs):
                self.calls += 1
                obj = {"totalCount": 2, "studies": [{
                    "protocolSection": {"identificationModule": {"nctId": "NCT1"}}
                }]}
                if self.calls == 1:
                    obj["nextPageToken"] = "next"
                return json.dumps(obj).encode(), "hash"
        with self.assertRaisesRegex(ValueError, "Duplicate trial"):
            health.trial_page(FakeFetcher(), 2025, "2025-12-31")


    def test_ukhsa_does_not_turn_monthly_or_derived_baselines_into_extra_histories(self):
        assert health.ukhsa_metric_unit("e-coli_testing_bacteraemiaNumberTestedRollingMonth") is None
        assert health.ukhsa_metric_unit("x_syndromic_GPInHours_baselineRateByDay") is None
        assert health.ukhsa_metric_unit("measles_cases_casesByOnsetWeekExcludingReportingLag") is None
        assert health.ukhsa_metric_unit("RSV_testing_positivityByWeek") == "percent of tested specimens"

