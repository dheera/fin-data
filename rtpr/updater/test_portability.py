"""Run from the dataset folder: python3 -m unittest discover -s updater -p 'test_*.py'."""
import contextlib
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from updater.dataset import Dataset, verify
from updater.article_metadata import enrich_article

spec = importlib.util.spec_from_file_location("rtpr_update", ROOT / "update.py")
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


@contextlib.contextmanager
def working_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def fixture(root):
    text = "Full text with Unicode: 完整新闻"
    row = enrich_article({"kind": "rtpr_historical_article", "article_id": "/a/example", "article_published_at": "2026-10-03T00:00:00.123456789Z",
           "receipt_utc": "2026-10-03T00:00:01.987654321Z", "receipt_unix_ns": 1790985601987654321,
           "receipt_monotonic_ns": 123456789, "text": text, "body_format": "text",
           "body_sha256": hashlib.sha256(text.encode()).hexdigest(), "body_characters": len(text)})
    page = {"kind": "rtpr_historical_feed_page", "receipt_utc": row["receipt_utc"],
            "page": {"articles": [{"article_url": "https://rtpr.io/a/example", "article_published_at": row["article_published_at"]}]}}
    source = root.parent / "source.gz"
    with gzip.open(source, "wt") as handle:
        for record in (row, page):
            handle.write(json.dumps(record) + "\n")
    with Dataset(root) as dataset:
        dataset.ingest(source, "history/fixture", "history")
        dataset.commit()


class PortabilityTests(unittest.TestCase):
    def test_default_root_follows_script_not_cwd(self):
        with tempfile.TemporaryDirectory() as directory, working_directory(directory):
            self.assertEqual(cli.parse_args([]).destination, ROOT)
            self.assertEqual(cli.parse_args(["--destination", "relative data"]).destination, Path(directory) / "relative data")

    def test_relative_key_in_config_uses_config_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "settings").mkdir()
            (root / "key.pem").write_text("test fixture, not a real SSH key")
            (root / "settings/source.json").write_text(json.dumps({"api_key_file": "../key.pem"}))
            with working_directory(root):
                args = cli.parse_args(["--config", "settings/source.json"])
                config = cli.settings(args)
            self.assertEqual(str(config["key_file"]), str(root / "key.pem"))

    def test_cli_relative_key_uses_cwd_and_overrides_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "key with spaces.pem").write_text("test fixture")
            (root / "config.json").write_text(json.dumps({"api_key_file": "missing-key"}))
            with working_directory(root):
                args = cli.parse_args(["--config", "config.json", "--api-key-file", "key with spaces.pem"])
                config = cli.settings(args)
            self.assertEqual(str(config["key_file"]), str(root / "key with spaces.pem"))

    def test_relocated_bundle_runs_isolated_without_repo_or_pythonpath(self):
        with tempfile.TemporaryDirectory(prefix="rtpr portable ") as directory:
            root = Path(directory)
            copied = root / "copied dataset"
            copied.mkdir()
            shutil.copyfile(ROOT / "update.py", copied / "update.py")
            shutil.copytree(ROOT / "updater", copied / "updater", ignore=shutil.ignore_patterns("__pycache__"))
            fixture(copied)
            elsewhere = root / "working directory"
            elsewhere.mkdir()
            command = [sys.executable, "-I", "../copied dataset/update.py"]
            status = subprocess.run(command + ["--status"], cwd=elsewhere, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(status.stdout)["destination"], str(copied))
            result = subprocess.run(command + ["--verify", "--destination", "../copied dataset"], cwd=elsewhere, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(result.stdout)["articles"], 1)
            self.assertFalse((elsewhere / "articles").exists())

    def test_documentation_is_not_overwritten_on_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            root.mkdir()
            (root / "README.txt").write_text("Local instructions stay here.\n")
            fixture(root)
            self.assertEqual((root / "README.txt").read_text(), "Local instructions stay here.\n")
            self.assertEqual(verify(root)["articles"], 1)



if __name__ == "__main__":
    unittest.main()
