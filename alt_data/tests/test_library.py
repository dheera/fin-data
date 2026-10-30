import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd
import pyarrow.parquet as pq

from expansion_common import DATA_SCHEMA, normalize_frame, save_candidate, coverage, qualifies_weekly
from library import build_catalog, load, publish, publish_staged, read_metadata, validate
from refresh import promote_existing


class LibraryTests(unittest.TestCase):
    def stage(self, root, sid="sample", offset=0, days=100):
        raw = b"test fixture raw data" + str(offset).encode()
        digest = hashlib.sha256(raw).hexdigest()
        path = root / "archive/raw" / (digest + ".bin")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return save_candidate(
            "test", sid, pd.DataFrame({"date": pd.date_range("2024-01-01", periods=days, tz="UTC"),
                                      "value": [float(x + offset) for x in range(days)]}),
            title=sid, provider="test", category="test", unit="count", frequency="daily",
            source_url="https://example.com/test", geography="test", hypothesis="test",
            limitations="test fixture", raw_sha256=digest, root=root, dataset_identity=sid)

    def test_short_history_and_duplicates_cannot_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "insufficient"):
                self.stage(Path(tmp), days=90)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            normalize_frame([{"date": "2024-01-01", "value": 1}, {"date": "2024-01-01", "value": 2}])

    def test_catalog_handles_named_and_numbered_provider_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for sid, source_column, numeric_extra, offset in [("named", "amount", 2, 0), ("numbered", 1, 2.5, 1)]:
                meta = self.stage(root, sid, offset=offset)
                meta.update(source_column=source_column, numeric_extra=numeric_extra, batch=1, status="ready")
                data = pd.read_parquet(root / "archive/staging/test" / (sid + ".parquet"))
                publish(data, meta, root)
            result = build_catalog(root).set_index("series_id")
            self.assertEqual(result.loc["named", "source_column"], "amount")
            self.assertEqual(result.loc["numbered", "source_column"], "1")
            self.assertEqual(result.loc["numbered", "numeric_extra"], 2.5)
            self.assertTrue(pd.api.types.is_numeric_dtype(result.numeric_extra))
            self.assertTrue(pd.api.types.is_integer_dtype(result.rows))
            self.assertTrue(pd.api.types.is_bool_dtype(result.daily_eligible))
            self.assertIsInstance(read_metadata(root / "data/test/numbered.parquet")["source_column"], int)
            stored = pd.read_parquet(root / "catalog.parquet").set_index("series_id")
            self.assertEqual(stored.loc["numbered", "source_column"], "1")
            self.assertEqual(len(load("numbered", root)), 100)

    def test_input_microseconds_are_canonical_before_hashing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.stage(root)
            frame = pd.read_parquet(root / "archive/staging/test/sample.parquet")
            frame["date"] = frame["date"].astype("datetime64[us, UTC]")
            meta = save_candidate(
                "microseconds", "microsecond_source", frame, title="microsecond source",
                provider="test", category="test", unit="count", frequency="daily",
                source_url="https://example.com/test", geography="test", hypothesis="test",
                limitations="fixture", raw_sha256=first["raw_sha256"], root=root,
                dataset_identity="microsecond_source")
            self.assertEqual(meta["content_sha256"], first["content_sha256"])
            publish_staged(1, 5, root, groups={"microseconds"})
            self.assertEqual(validate(root)["failures"], [])

    def test_native_weekly_history_is_published_without_upsampling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = self.stage(root)
            frame = pd.DataFrame({"date": pd.date_range("2023-01-02", periods=104, freq="7D", tz="UTC"),
                                  "value": range(104)})
            meta = save_candidate(
                "weekly", "foreign_weekly", frame, title="Weekly foreign trade",
                provider="test", category="test", unit="tonnes", frequency="weekly",
                source_url="https://example.com/weekly", geography="test", hypothesis="test",
                limitations="fixture", raw_sha256=base["raw_sha256"], root=root,
                dataset_identity="foreign_weekly")
            self.assertFalse(meta["daily_eligible"])
            self.assertTrue(meta["research_eligible"])
            publish_staged(1, 6, root, groups={"weekly"})
            self.assertEqual(len(load("foreign_weekly", root)), 104)
            self.assertEqual(validate(root)["failures"], [])

    def test_weekly_gate_rejects_monthly_or_short_or_sparse_observations(self):
        for periods, freq in [(12, "7D"), (36, "MS"), (30, "14D")]:
            frame = normalize_frame(pd.DataFrame({
                "date": pd.date_range("2020-01-01", periods=periods, freq=freq, tz="UTC"),
                "value": range(periods)}))
            self.assertFalse(qualifies_weekly(dict(coverage(frame), frequency="weekly")))

    def test_twice_daily_forecast_cycles_keep_both_native_observations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = self.stage(root)
            frame = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=200, freq="12h", tz="UTC"),
                                  "value": range(200)})
            meta = save_candidate(
                "twice_daily", "forecast_cycles", frame, title="Twice-daily forecast cycles",
                provider="test", category="test", unit="kg m-2", frequency="12-hourly",
                source_url="https://example.com/forecast", geography="test", hypothesis="test",
                limitations="fixture", raw_sha256=base["raw_sha256"], root=root,
                dataset_identity="forecast_cycles")
            self.assertTrue(meta["daily_eligible"])
            self.assertEqual(meta["median_interval_seconds"], 43200)
            publish_staged(1, 10, root, groups={"twice_daily"})
            pd.testing.assert_frame_equal(load("forecast_cycles", root), normalize_frame(frame))
            self.assertEqual(validate(root)["failures"], [])

    def test_manifest_members_are_verified_and_survive_refresh(self):
        from expansion_common import archive_raw_manifest, atomic_bytes, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = self.stage(root)
            member = b"additional raw payload"
            digest = hashlib.sha256(member).hexdigest()
            raw_path = root / "archive/raw" / (digest + ".bin")
            raw_path.write_bytes(member)
            manifest = archive_raw_manifest([digest], description="Original daily files", root=root)
            meta["raw_manifest_sha256"] = [manifest]
            staged = root / "archive/staging/test/sample.parquet"
            atomic_bytes(staged, parquet_bytes(pd.read_parquet(staged), meta))
            publish_staged(1, 6, root)
            self.assertEqual(validate(root)["raw_responses_verified"], 3)
            self.stage(root, offset=1000)
            promote_existing(root)
            build_catalog(root)
            self.assertEqual(read_metadata(root / "data/test/sample.parquet")["raw_manifest_sha256"], [manifest])
            raw_path.write_bytes(b"corrupt")
            self.assertTrue(any(x.get("raw_sha256") == digest for x in validate(root)["failures"]))

    def test_batch_count_checked_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            with self.assertRaisesRegex(ValueError, "need 2"):
                publish_staged(2, 1, root)
            self.assertFalse((root / "data").exists())

    def test_audited_selection_publishes_only_pinned_numeric_histories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "a")
            selected = self.stage(root, "b", offset=100)
            second = self.stage(root, "c", offset=200)
            result = publish_staged(2, 6, root,
                                    selection={"b": selected["content_sha256"], "c": second["content_sha256"]},
                                    selection_policy="Fixture pinned selection", workers=2)
            self.assertEqual([m["series_id"] for m in result["series"]], ["b", "c"])
            self.assertEqual(result["selection_policy"], "Fixture pinned selection")
            self.assertEqual(validate(root)["failures"], [])

    def test_changed_or_incomplete_selection_fails_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "a")
            with self.assertRaisesRegex(ValueError, "changed after selection"):
                publish_staged(1, 6, root, selection={"a": "0" * 64})
            with self.assertRaisesRegex(ValueError, "exactly"):
                publish_staged(2, 6, root, selection={"a": "0" * 64})
            self.assertFalse((root / "data").exists())

    def test_pinned_publication_detects_changed_values_with_unchanged_footer(self):
        from expansion_common import atomic_bytes, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            meta = self.stage(root)
            path = root / "archive/staging/test/sample.parquet"
            changed = pd.read_parquet(path)
            changed.loc[0, "value"] = -100
            atomic_bytes(path, parquet_bytes(changed, meta))
            with self.assertRaisesRegex(ValueError, "Numeric history changed"):
                publish_staged(1, 6, root, selection={"sample": meta["content_sha256"]})
            self.assertFalse((root / "data").exists())

    def test_duplicate_numeric_history_cannot_pad_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "a")
            self.stage(root, "b")
            with self.assertRaisesRegex(ValueError, "only 1"):
                publish_staged(2, 1, root)

    def test_duplicate_ids_across_staging_groups_cannot_pad_count(self):
        from expansion_common import atomic_bytes, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "same")
            other = self.stage(root, "different", offset=1)
            path = root / "archive/staging/test/different.parquet"
            data = pd.read_parquet(path)
            other["series_id"] = "same"
            atomic_bytes(root / "archive/staging/another/same.parquet", parquet_bytes(data, other))
            path.unlink()
            with self.assertRaisesRegex(ValueError, "only 1"):
                publish_staged(2, 4, root)
            self.assertFalse((root / "data").exists())

    def test_batch_number_cannot_be_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "a")
            publish_staged(1, 1, root)
            self.stage(root, "b", offset=1)
            with self.assertRaisesRegex(ValueError, "already exists"):
                publish_staged(1, 1, root)

    def test_unselected_staging_group_cannot_pad_expansion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            with self.assertRaisesRegex(ValueError, "only 0"):
                publish_staged(1, 4, root, groups={"another"})
            self.assertFalse((root / "data").exists())
            publish_staged(1, 4, root, groups={"test"})
            self.assertEqual(validate(root, expected_new=1)["failures"], [])

    def test_batch_receipt_membership_is_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 4, root)
            receipt = root / "logs/batch_4.json"
            info = json.loads(receipt.read_text())
            info["series"][0]["series_id"] = "incorrect"
            receipt.write_text(json.dumps(info))
            self.assertTrue(any("receipt" in x["error"] for x in validate(root)["failures"]))

    def test_current_history_preferred_across_provider_categories(self):
        from expansion_common import atomic_bytes, coverage, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "old")
            meta = self.stage(root, "current", offset=100)
            path = root / "archive/staging/test/current.parquet"
            data = pd.read_parquet(path)
            data["date"] = pd.date_range(
                end=pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1),
                periods=len(data), tz="UTC")
            meta.update(coverage(data))
            meta["provider"] = "zz_current_provider"
            meta["content_sha256"] = hashlib.sha256(
                data.date.astype("int64").to_numpy().tobytes() + data.value.to_numpy().tobytes()
            ).hexdigest()
            atomic_bytes(path, parquet_bytes(data, meta))
            result = publish_staged(1, 4, root)
            self.assertEqual(result["series"][0]["series_id"], "current")

    def test_refresh_does_not_publish_failed_or_unrequested_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 1, root)
            self.stage(root, offset=1000)
            self.assertEqual(promote_existing(root, groups={"unrequested"}), [])
            self.assertEqual(promote_existing(root, groups={"test"}, eligible_ids=set()), [])
            self.assertEqual(load("sample", root).iloc[0]["value"], 0)

    def test_explicit_source_withdrawals_remove_old_values(self):
        from expansion_common import atomic_bytes, coverage, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 4, root)
            path = root / "archive/staging/test/sample.parquet"
            data = pd.read_parquet(path)
            withdrawn = data.iloc[0]["date"].isoformat()
            metadata = read_metadata(path)
            metadata["retracted_dates"] = [withdrawn]
            # An inconsistent withdrawal must fail before changing public data.
            atomic_bytes(path, parquet_bytes(data, metadata))
            with self.assertRaisesRegex(ValueError, "retraction"):
                promote_existing(root)
            self.assertEqual(len(load("sample", root)), 100)
            data = data.iloc[1:].reset_index(drop=True)
            metadata.update(coverage(data))
            metadata["content_sha256"] = hashlib.sha256(
                data.date.astype("int64").to_numpy().tobytes() + data.value.to_numpy().tobytes()
            ).hexdigest()
            atomic_bytes(path, parquet_bytes(data, metadata))
            promote_existing(root)
            build_catalog(root)
            self.assertEqual(len(load("sample", root)), 99)
            self.assertNotIn(pd.Timestamp(withdrawn), load("sample", root).date.tolist())
            self.assertEqual(validate(root)["failures"], [])

    def test_changed_units_or_category_require_explicit_migration(self):
        from expansion_common import atomic_bytes, parquet_bytes
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 1, root)
            path = root / "archive/staging/test/sample.parquet"
            metadata = read_metadata(path)
            metadata["category"] = "changed"
            atomic_bytes(path, parquet_bytes(pd.read_parquet(path), metadata))
            with self.assertRaisesRegex(ValueError, "category changed"):
                promote_existing(root)
            self.assertFalse((root / "data/changed").exists())

    def test_cadence_change_rejects_the_refresh_before_any_series_is_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root, "a")
            previous_b = self.stage(root, "b", offset=200)
            publish_staged(2, 7, root)
            original_a = load("a", root)
            original_b = load("b", root)
            self.stage(root, "a", offset=1000)
            save_candidate(
                "test", "b",
                pd.DataFrame({"date": pd.date_range("2024-01-01", periods=104, freq="7D", tz="UTC"),
                              "value": [float(x + 200) for x in range(104)]}),
                title="b", provider="test", category="test", unit="count", frequency="weekly",
                source_url="https://example.com/test", geography="test", hypothesis="test",
                limitations="test fixture", raw_sha256=previous_b["raw_sha256"],
                root=root, dataset_identity="b")
            with self.assertRaisesRegex(ValueError, "frequency changed"):
                promote_existing(root)
            pd.testing.assert_frame_equal(load("a", root), original_a)
            pd.testing.assert_frame_equal(load("b", root), original_b)
            self.assertEqual(read_metadata(root / "data/test/b.parquet")["frequency"], "daily")

    def test_two_revisions_with_same_retrieval_timestamp_are_both_kept(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 1, root)
            path = root / "data/test/sample.parquet"
            first = read_metadata(path)
            for offset in (1000, 2000):
                meta = self.stage(root, offset=offset)
                meta.update(batch=1, status="ready", retrieved_at=first["retrieved_at"])
                data = pd.read_parquet(root / "archive/staging/test/sample.parquet")
                publish(data, meta, root)
            revisions = list((root / "archive/revisions/sample").glob("*.parquet"))
            self.assertEqual(len(revisions), 2)
            self.assertEqual(sorted(pd.read_parquet(p).value.iloc[0] for p in revisions), [0, 1000])

    def test_refresh_revisions_coverage_and_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.stage(root)
            publish_staged(1, 1, root)
            self.assertTrue(pq.read_schema(root / "data/test/sample.parquet").remove_metadata().equals(DATA_SCHEMA))
            self.assertEqual(validate(root)["failures"], [])
            first = load("sample", root)
            self.stage(root, offset=1000)
            promote_existing(root)
            build_catalog(root)
            self.assertEqual(load("sample", root).iloc[0]["value"], 1000)
            revision = list((root / "archive/revisions/sample").glob("*.parquet"))
            self.assertEqual(len(revision), 1)
            pd.testing.assert_frame_equal(pd.read_parquet(revision[0]), first)
            self.assertEqual(validate(root)["batch_counts"], {"1": 1})
            meta = read_metadata(root / "data/test/sample.parquet")
            (root / "archive/raw" / (meta["raw_sha256"][0] + ".bin")).write_bytes(b"corrupt")
            self.assertTrue(validate(root)["failures"])


if __name__ == "__main__":
    unittest.main()
