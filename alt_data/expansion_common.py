"""Shared acquisition and validation for the expanded alternative-data library."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from email.utils import parsedate_to_datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = Path(__file__).resolve().parent
DATA_SCHEMA = pa.schema([("date", pa.timestamp("ns", tz="UTC")), ("value", pa.float64())])
DAILY_FREQUENCIES = {"daily", "business_daily", "hourly", "30min", "15min", "10min",
                     "5min", "1min", "3-hourly", "6-hourly", "12-hourly", "subhourly", "irregular_intraday"}


def atomic_bytes(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        os.fchmod(fd, 0o664)
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def archive_raw_manifest(hashes, *, description, root=ROOT):
    """Reference large sets of original responses without repeating them in every footer."""
    members = sorted(set(hashes))
    if not members or any(not re.fullmatch(r"[a-f0-9]{64}", h) for h in members):
        raise ValueError("Invalid raw manifest members")
    if any(not (Path(root) / "archive/raw" / (h + ".bin")).exists() for h in members):
        raise ValueError("Raw manifest member is missing")
    payload = json.dumps({"format": "alt_data_raw_manifest_v1", "description": description,
                          "members": members}, sort_keys=True).encode()
    digest = hashlib.sha256(payload).hexdigest()
    atomic_bytes(Path(root) / "archive/raw" / (digest + ".bin"), payload)
    return digest


def raw_provenance_hashes(meta, root=ROOT, manifest_cache=None):
    """Expand explicit provenance manifests; all returned objects must be hash-verified."""
    result = set(meta["raw_sha256"])
    cache = {} if manifest_cache is None else manifest_cache
    for digest in meta.get("raw_manifest_sha256", []):
        if not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("Invalid raw manifest checksum")
        if digest not in cache:
            payload = (Path(root) / "archive/raw" / (digest + ".bin")).read_bytes()
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ValueError("Raw manifest checksum mismatch")
            obj = json.loads(payload)
            if obj.get("format") != "alt_data_raw_manifest_v1" or not obj.get("members"):
                raise ValueError("Invalid raw provenance manifest")
            if any(not re.fullmatch(r"[a-f0-9]{64}", h) for h in obj["members"]):
                raise ValueError("Invalid raw manifest member checksum")
            cache[digest] = set(obj["members"])
        result.add(digest)
        result.update(cache[digest])
    return result


class Fetcher:
    """Archive response bytes and URL; cache by URL, with bounded retries."""

    def __init__(self, root=ROOT, refresh=False):
        self.root = Path(root)
        self.refresh = refresh

    def get(self, url, params=None, *, timeout=90, headers=None, max_age_hours=24):
        prepared = requests.Request("GET", url, params=params).prepare().url
        key = hashlib.sha256(prepared.encode()).hexdigest()
        index_path = self.root / "archive" / "requests" / (key + ".json")
        if not self.refresh and index_path.exists():
            meta = json.loads(index_path.read_text())
            raw = self.root / "archive" / "raw" / (meta["sha256"] + ".bin")
            age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta["retrieved_at"])).total_seconds()
            if 0 <= age < max_age_hours * 3600 and raw.exists():
                content = raw.read_bytes()
                if hashlib.sha256(content).hexdigest() == meta["sha256"]:
                    return content, meta["sha256"]
        request_headers = {"User-Agent": "FinAltDataResearch/1.0 (public time-series research)"}
        request_headers.update(headers or {})
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=(15, timeout), headers=request_headers)
                response.raise_for_status()
                content = response.content
                digest = hashlib.sha256(content).hexdigest()
                meta = {"url": response.url, "sha256": digest, "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "content_type": response.headers.get("Content-Type")}
                atomic_bytes(self.root / "archive" / "raw" / (digest + ".bin"), content)
                receipt_id = key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
                atomic_bytes(self.root / "archive/receipts" / (receipt_id + ".json"), json.dumps(meta, indent=2).encode())
                # URL-keyed receipts preserve provenance when different URLs have identical bytes.
                atomic_bytes(index_path, json.dumps(meta, indent=2).encode())
                return content, digest
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is not None and status not in {429, 500, 502, 503, 504}:
                    raise
                if attempt == 2:
                    raise
                delay = min(30, 3 ** (attempt + 1))
                retry_after = getattr(getattr(exc, "response", None), "headers", {}).get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        try:
                            delay = max(delay, (parsedate_to_datetime(retry_after) - pd.Timestamp.now(tz="UTC")).total_seconds())
                        except (ValueError, TypeError):
                            pass
                # Respect a longer server hold-off by surfacing it instead of retrying early.
                if delay > 60:
                    raise
                time.sleep(max(0, delay))


def normalize_frame(frame):
    """Require explicit numeric observations. Missing source values may be omitted by parsers."""
    frame = pd.DataFrame(frame)[["date", "value"]].copy()
    # Parquet sources may retain microsecond precision. Canonicalize before
    # hashing as well as writing so the checksum always describes DATA_SCHEMA.
    frame["date"] = pd.to_datetime(frame["date"], utc=True, errors="raise").astype("datetime64[ns, UTC]")
    frame["value"] = pd.to_numeric(frame["value"], errors="raise").astype(float)
    if frame.empty or frame["date"].isna().any() or not np.isfinite(frame["value"]).all():
        raise ValueError("Empty data, missing dates, or nonfinite values")
    if frame["date"].duplicated().any():
        raise ValueError("Duplicate observation dates; resolve using source semantics")
    if (frame["date"] > pd.Timestamp.now(tz="UTC") + pd.Timedelta(days=1)).any():
        raise ValueError("Future observations")
    return frame.sort_values("date").reset_index(drop=True)


def coverage(frame):
    dates = frame["date"]
    gaps = dates.diff().dt.total_seconds().dropna()
    span = (dates.iloc[-1] - dates.iloc[0]).total_seconds() / 86400
    days = dates.dt.floor("D").nunique()
    return {
        "rows": len(frame), "start": dates.iloc[0].isoformat(), "end": dates.iloc[-1].isoformat(),
        "span_days": round(span, 3), "observed_days": int(days),
        "day_coverage": round(days / ((dates.iloc[-1].floor("D") - dates.iloc[0].floor("D")).days + 1), 5),
        "median_interval_seconds": float(gaps.median()) if len(gaps) else None,
        "max_gap_days": float(gaps.max() / 86400) if len(gaps) else None,
        "last_value": float(frame["value"].iloc[-1]),
    }


def qualifies_daily(meta):
    return (meta["frequency"] in DAILY_FREQUENCIES and meta["rows"] >= 90
            and meta["span_days"] >= 90 and meta["observed_days"] >= 90
            and meta["median_interval_seconds"] <= 86400
            and meta["day_coverage"] >= 0.5)


def qualifies_weekly(meta):
    """Admit genuine weekly observations without expanding them into days."""
    return (meta["frequency"] == "weekly" and meta["rows"] >= 26
            and meta["observed_days"] >= 26 and meta["span_days"] >= 180
            and 5 * 86400 <= meta["median_interval_seconds"] <= 8 * 86400
            and meta["observed_days"] / (int(meta["span_days"] // 7) + 1) >= 0.5)


def qualifies_research(meta):
    """The requested hourly/daily/weekly research universe."""
    return qualifies_daily(meta) or qualifies_weekly(meta)


def parquet_bytes(frame, meta):
    table = pa.Table.from_pandas(frame, schema=DATA_SCHEMA, preserve_index=False)
    table = table.replace_schema_metadata({
        b"alt_data": json.dumps(meta, sort_keys=True).encode(),
        b"date_semantics": str(meta.get("timestamp_semantics",
            "UTC observation time/period label; not historical availability time")).encode(),
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def save_candidate(group, series_id, frame, *, title, provider, category, unit, frequency,
                   source_url, geography, hypothesis, limitations, raw_sha256,
                   root=ROOT, **extra):
    """Stage validated real data for subsequent group-scoped batch publication.

    frame: DataFrame or records with exactly date/value selected.
    raw_sha256: string or list of Fetcher response digests.
    Additional keyword metadata is preserved (e.g. license, native_frequency,
    timestamp_semantics, dataset_identity).
    """
    if not re.fullmatch(r"[a-z0-9_]+", series_id) or not re.fullmatch(r"[a-z0-9_]+", category):
        raise ValueError("Use snake_case series_id and category")
    frame = normalize_frame(frame)
    hashes = [raw_sha256] if isinstance(raw_sha256, str) else list(raw_sha256)
    if not hashes or any(not (Path(root) / "archive/raw" / (h + ".bin")).exists() for h in hashes):
        raise ValueError("Raw provenance is missing")
    meta = dict(series_id=series_id, title=title, provider=provider, category=category, unit=unit,
                frequency=frequency, source_url=source_url, geography=geography, hypothesis=hypothesis,
                limitations=limitations, raw_sha256=sorted(set(hashes)), **extra)
    meta.update(coverage(frame))
    meta["retrieved_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    meta["daily_eligible"] = qualifies_daily(meta)
    meta["research_eligible"] = qualifies_research(meta)
    if not meta["research_eligible"]:
        raise ValueError(f"{series_id}: insufficient genuine daily/weekly history: {coverage(frame)}")
    # Stable numeric content hash, independent of metadata/retrieval time.
    meta["content_sha256"] = hashlib.sha256(
        frame["date"].astype("int64").to_numpy().tobytes() + frame["value"].to_numpy().tobytes()
    ).hexdigest()
    folder = Path(root) / "archive/staging" / group
    atomic_bytes(folder / (series_id + ".parquet"), parquet_bytes(frame, meta))
    atomic_bytes(folder / (series_id + ".json"), json.dumps(meta, indent=2).encode())
    return meta
