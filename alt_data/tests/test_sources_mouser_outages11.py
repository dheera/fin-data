import unittest
import pandas as pd
from sources_mouser11 import parse_numbers, listing_eligibility, replace_processed_scopes
from sources_outages11 import normalize_messages


class ComponentTests(unittest.TestCase):
    def test_stock_missing_is_not_zero(self):
        self.assertEqual(parse_numbers({"Availability": "On Order", "LeadTime": "Contact us"}), [])

    def test_explicit_stock_lead_and_price_basis(self):
        rows = parse_numbers({"AvailabilityInStock": "1,234", "LeadTime": "8 Weeks",
                              "PriceBreaks": [{"Price": "$1.25", "Quantity": 100, "Currency": "USD"}]})
        self.assertIn(("stock", 1234., "pieces", None), rows)
        self.assertIn(("lead_time", 56., "days", None), rows)
        self.assertIn(("unit_price", 1.25, "USD/piece", 100), rows)

    def test_ambiguous_price_or_currency_is_not_inferred(self):
        rows = parse_numbers({"PriceBreaks": [{"Price": "$1.25", "Quantity": 1},
                                              {"Price": "1,25", "Quantity": 1, "Currency": "EUR"}]})
        self.assertEqual(rows, [])

    def test_numeric_zero_stock_retained(self):
        self.assertEqual(parse_numbers({"AvailabilityInStock": 0}), [("stock", 0., "pieces", None)])

    def test_negative_or_text_prefixed_price_rejected(self):
        for text in ["-$1.25", "Call for price 1.25"]:
            self.assertEqual(parse_numbers({"PriceBreaks": [{"Price": text, "Quantity": 1, "Currency": "USD"}]}), [])

    def test_region_restricted_and_placeholder_listing_not_numeric_eligible(self):
        part={"MouserPartNumber":"N/A","ManufacturerPartNumber":"BSS138",
              "LeadTime":"0 Days","RestrictionMessage":"Mouser does not presently sell this product in your region."}
        flags=listing_eligibility(part)
        self.assertFalse(flags['numeric_observations_eligible'])
        self.assertTrue(flags['restricted_listing']);self.assertTrue(flags['invalid_distributor_id'])
        self.assertEqual(part['LeadTime'],'0 Days')  # raw record remains intact
        self.assertFalse(listing_eligibility({'MouserPartNumber':'N/A'})['numeric_observations_eligible'])
        self.assertFalse(listing_eligibility({'MouserPartNumber':'123-valid','RestrictionMessage':'Restricted'})['numeric_observations_eligible'])
        self.assertTrue(listing_eligibility({'MouserPartNumber':'123-valid','AvailabilityInStock':0})['numeric_observations_eligible'])

    def test_reprocess_retracts_rejected_rows_preserving_prior_dates_and_other_requests(self):
        today=pd.Timestamp('2026-10-21T07:00:00Z');yesterday=today-pd.Timedelta(days=1)
        rows=[{'collected_at':t,'requested_part':part,'raw_sha256':'same-bytes',
               'mouser_part_number':'N/A','measure':'lead_time','value':0.}
              for t,part in [(today,'BSS138'),(yesterday,'BSS138'),(today,'OTHER')]]
        frame=replace_processed_scopes(pd.DataFrame(rows),pd.DataFrame(),[(today,'BSS138','same-bytes')],
                                      ['collected_at','requested_part','mouser_part_number','measure'])
        self.assertEqual(len(frame),2)
        self.assertEqual(set(zip(frame.collected_at,frame.requested_part)),{(yesterday,'BSS138'),(today,'OTHER')})

    def test_reprocess_replaces_old_numeric_value_instead_of_appending(self):
        t=pd.Timestamp('2026-10-21T07:00:00Z')
        row={'collected_at':t,'requested_part':'P','raw_sha256':'hash','mouser_part_number':'123-P','measure':'stock','value':4.}
        new={**row,'value':0.}
        frame=replace_processed_scopes(pd.DataFrame([row]),pd.DataFrame([new]),[(t,'P','hash')],
                                      ['collected_at','requested_part','mouser_part_number','measure'])
        self.assertEqual(frame.value.tolist(),[0.])


class OutageRevisionTests(unittest.TestCase):
    def row(self, identifier, revision, unavailable=50., status="Active"):
        return {"id": identifier, "mrid": "outage-a", "revisionNumber": revision,
                "publishTime": f"2026-01-01T0{identifier}:00:00Z", "createdTime": "2026-01-01T00:00:00Z",
                "eventStartTime": "2026-01-02T00:00:00Z", "eventEndTime": "2026-01-03T00:00:00Z",
                "assetId": "plant-a", "unavailableCapacity": unavailable,
                "eventStatus": status, "outageProfile": []}

    def test_only_consecutive_revisions_get_capacity_difference(self):
        result, _ = normalize_messages([self.row(1, 1, 50.), self.row(2, 2, 70.), self.row(3, 4, 30.)])
        self.assertTrue(pd.isna(result.loc[0, "reported_unavailable_capacity_change_mw"]))
        self.assertEqual(result.loc[1, "reported_unavailable_capacity_change_mw"], 20.)
        self.assertTrue(pd.isna(result.loc[2, "reported_unavailable_capacity_change_mw"]))
        self.assertFalse(result.point_in_time_verified.any())

    def test_dismissal_does_not_fabricate_zero_capacity(self):
        result, _ = normalize_messages([self.row(1, 1), self.row(2, 2, status="Dismissed")])
        self.assertEqual(result.loc[1, "unavailableCapacity"], 50.)
        self.assertEqual(result.loc[1, "reported_unavailable_capacity_change_mw"], 0.)

    def test_zero_duration_profile_retained_and_flagged(self):
        row = self.row(1, 1)
        row["outageProfile"] = [{"startTime": "2026-01-02T00:00:00Z", "endTime": "2026-01-02T00:00:00Z", "capacity": 10.}]
        _, profiles = normalize_messages([row])
        self.assertTrue(profiles.loc[0, "nonpositive_duration"])
        self.assertEqual(profiles.loc[0, "source_capacity"], 10.)

    def test_missing_profile_is_json_null(self):
        row = self.row(1, 1)
        del row["outageProfile"]
        result, _ = normalize_messages([row])
        self.assertEqual(result.loc[0, "outageProfile_json"], "null")


if __name__ == "__main__":
    unittest.main()
