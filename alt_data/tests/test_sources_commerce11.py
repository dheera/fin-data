import json
import unittest

from sources_commerce11 import parse_weekly, query_body


class CardHistoryTests(unittest.TestCase):
    def fixture(self, values=("100.00", None, "0")):
        dates = ["05-01-2024", "12-01-2024", "19-01-2024"]
        items = [{"Tarih": d, "YEARWEEK": "2024-" + str(i + 1), "TP_TEST": v}
                 for i, (d, v) in enumerate(zip(dates, values))]
        return {"totalCount": len(items), "items": items, "seriesNames": {"TP_TEST": "test"}}

    def test_no_missing_week_fill_and_real_zero_retained(self):
        out = parse_weekly(json.dumps(self.fixture()).encode(), "TP.TEST")
        self.assertEqual(out.value.tolist(), [100., 0.])
        self.assertEqual(out.date.dt.strftime("%Y-%m-%d").tolist(), ["2024-01-05", "2024-01-19"])

    def test_monthly_conversion_rejected(self):
        obj = self.fixture()
        obj["items"][0]["Tarih"] = "2024-01"
        with self.assertRaisesRegex(ValueError, "native weekly"):
            parse_weekly(json.dumps(obj).encode(), "TP.TEST")

    def test_partial_response_rejected(self):
        obj = self.fixture()
        obj["totalCount"] += 1
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            parse_weekly(json.dumps(obj).encode(), "TP.TEST")

    def test_duplicate_week_rejected(self):
        obj = self.fixture(("1", "2", "3"))
        obj["items"][1]["Tarih"] = obj["items"][0]["Tarih"]
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            parse_weekly(json.dumps(obj).encode(), "TP.TEST")

    def test_native_query_does_not_apply_transform(self):
        body = query_body(["A", "B"], "2014-03-01", "2026-10-20")
        self.assertEqual(body["frequency"], "0")
        self.assertEqual(body["formulas"], "0-0")
        self.assertEqual(body["startDate"], "01-03-2014")
        self.assertFalse(body["groupSeperator"])

    def test_invalid_negative_rejected(self):
        with self.assertRaisesRegex(ValueError, "Negative"):
            parse_weekly(json.dumps(self.fixture(("-1", None, "0"))).encode(), "TP.TEST")


if __name__ == "__main__":
    unittest.main()
