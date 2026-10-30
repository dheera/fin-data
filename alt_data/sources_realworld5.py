#!/usr/bin/env python3
"""New physical activity signals: South American fire observations and European chemicals."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import timezone, timedelta
import hashlib
import io
import json
import re
import time
import threading
import unicodedata
from urllib.parse import urljoin
import zipfile

import numpy as np
import pandas as pd
import requests

from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes, coverage,
                              normalize_frame, qualifies_research, save_candidate)
from country_codes import normalize_country_codes

GROUP = "realworld5"
START = "2024-01-01"
END = (pd.Timestamp.now(tz="UTC").floor("D") - pd.Timedelta(days=1))
FAILURES = []
OMISSIONS = Counter()
SEEN = set()
BASELINE = None
REFRESH = False
WRITE_POOL = None
WRITE_SLOTS = threading.BoundedSemaphore(32)


def slug(value):
    plain = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", plain.lower()).strip("_")


def baseline():
    global BASELINE
    if BASELINE is None:
        catalog = pd.read_parquet(ROOT / "catalog.parquet")
        pinned = set(catalog.loc[catalog.batch == 7, "series_id"])
        old = catalog[catalog.batch < 7]
        BASELINE = {c: set(old[c].dropna()) for c in
                    ["series_id", "dataset_identity", "content_sha256"]}
        BASELINE["pinned_ids"] = pinned
        BASELINE["pinned_frequency"] = dict(catalog.loc[catalog.batch == 7, ["series_id", "frequency"]].values)
        selected_eea = catalog[(catalog.batch == 7) & catalog.series_id.str.startswith("eea_")]
        BASELINE["pinned_eea_countries"] = sorted({
            c for codes in selected_eea.country_codes
            for c in (json.loads(codes) if isinstance(codes, str) else codes)})
    return BASELINE


def save(sid, frame, **meta):
    old = baseline()
    if old["pinned_ids"] and sid not in old["pinned_ids"]:
        return None
    if frame.empty or frame.value.nunique() < 2:
        OMISSIONS["constant_or_empty"] += 1
        return None
    frame = normalize_frame(frame)
    if not qualifies_research(dict(coverage(frame), frequency=meta["frequency"])):
        OMISSIONS["insufficient_frequency_or_history"] += 1
        return None
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    if (sid in old["series_id"] or meta["dataset_identity"] in old["dataset_identity"]
            or digest in old["content_sha256"] or digest in SEEN):
        OMISSIONS["existing_identity_or_duplicate_values"] += 1
        return None
    SEEN.add(digest)
    codes = normalize_country_codes(meta.pop("country_codes", []))
    meta["country_codes"] = codes
    meta["country_code_standard"] = ("ISO 3166-1 alpha-2; XK user-assigned Kosovo extension"
                                      if "XK" in codes else "ISO 3166-1 alpha-2")
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("finer_frequency_available", False)
    meta["raw_sha256"] = sorted(set(meta["raw_sha256"]))
    cached_path = ROOT / "archive/staging" / GROUP / (sid + ".json")
    if cached_path.exists() and cached_path.with_suffix(".parquet").exists():
        previous = json.loads(cached_path.read_text())
        if previous.get("content_sha256") == digest and all(previous.get(k) == v for k, v in meta.items()):
            print("CACHED", sid, len(frame), flush=True)
            return previous
    def write():
        result = save_candidate(GROUP, sid, frame, **meta)
        print("OK", sid, len(frame), flush=True)
        return result
    if WRITE_POOL is not None:
        WRITE_SLOTS.acquire()
        future = WRITE_POOL.submit(write)
        future.add_done_callback(lambda _: WRITE_SLOTS.release())
        return future
    return write()


def post_read(url, body, *, max_age_hours=720):
    """Archive a read-only POST search, with its request body and original response."""
    key = hashlib.sha256(json.dumps({"url": url, "method": "POST", "body": body},
                                   sort_keys=True).encode()).hexdigest()
    path = ROOT / "archive/requests" / (key + ".json")
    if not REFRESH and path.exists():
        receipt = json.loads(path.read_text())
        raw_path = ROOT / "archive/raw" / (receipt["sha256"] + ".bin")
        age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(receipt["retrieved_at"])).total_seconds()
        if 0 <= age < max_age_hours * 3600 and raw_path.exists():
            raw = raw_path.read_bytes()
            if hashlib.sha256(raw).hexdigest() == receipt["sha256"]:
                return raw, receipt["sha256"]
    for attempt in range(3):
        response = requests.post(url, json=body, timeout=(15, 180),
                                 headers={"User-Agent": "FinAltDataResearch/1.0 (public research)"})
        if response.status_code == 429:
            response.raise_for_status()
        if response.status_code in {500, 502, 503, 504} and attempt < 2:
            time.sleep(3 ** (attempt + 1))
            continue
        response.raise_for_status()
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        receipt = {"url": url, "method": "POST", "body": body, "sha256": digest,
                   "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                   "content_type": response.headers.get("Content-Type")}
        atomic_bytes(ROOT / "archive/raw" / (digest + ".bin"), raw)
        atomic_bytes(path, json.dumps(receipt, indent=2).encode())
        atomic_bytes(ROOT / "archive/receipts" /
                     (key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + ".json"),
                     json.dumps(receipt, indent=2).encode())
        return raw, digest
    raise RuntimeError("Read-only search failed")


def bounded_results(pool, function, items, window=6):
    """Avoid accumulating thousands of finished full-resolution histories in memory."""
    iterator = iter(items)
    pending = {}
    for _ in range(window):
        item = next(iterator, None)
        if item is None:
            break
        pending[pool.submit(function, item)] = item
    while pending:
        finished, _ = wait(pending, return_when=FIRST_COMPLETED)
        for task in finished:
            item = pending.pop(task)
            yield item, task
            following = next(iterator, None)
            if following is not None:
                pending[pool.submit(function, following)] = following


INPE_BASE = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/"
INPE_COUNTRIES = {"Argentina": "AR", "Bolivia": "BO", "Brasil": "BR", "Chile": "CL",
                  "Colombia": "CO", "Ecuador": "EC", "French Guiana": "GF", "Guyana": "GY",
                  "Paraguay": "PY", "Peru": "PE", "Suriname": "SR", "Uruguay": "UY",
                  "Venezuela": "VE"}
INPE_SATELLITES = {"AQUA_M-T", "NOAA-20", "NOAA-21"}


def complete_fire_weeks(data):
    """Sum only full source-reporting weeks; no fill for outages or missing power."""
    grouped = data.set_index("date").value.resample("W-MON", closed="left", label="left").agg(["sum", "count"])
    return grouped[grouped["count"].eq(7)]["sum"].rename("value").reset_index()


def inpe_aggregate(raw, month):
    """One native satellite per panel; absent or invalid FRP never becomes zero."""
    frame = pd.read_csv(io.BytesIO(raw), dtype={"municipio_id": str, "estado_id": str,
                                               "pais_id": str})
    required = {"id", "data_hora_gmt", "satelite", "pais", "estado", "municipio",
                "pais_id", "estado_id", "municipio_id", "frp"}
    if not required.issubset(frame):
        raise ValueError("INPE monthly schema changed")
    dates = pd.to_datetime(frame.data_hora_gmt, utc=True, errors="raise")
    if not dates.dt.strftime("%Y%m").eq(month).all():
        raise ValueError("INPE detection outside requested month")
    frame["date"] = dates.dt.floor("D")
    frame = frame[frame.date.le(END) & frame.satelite.isin(INPE_SATELLITES)].copy()
    if frame.id.duplicated().any():
        duplicates = frame[frame.id.duplicated(False)]
        if duplicates.groupby("id", dropna=False).nunique(dropna=False).gt(1).any().any():
            raise ValueError("Conflicting INPE detection ID")
        frame = frame.drop_duplicates("id")
    if not set(frame.pais).issubset(INPE_COUNTRIES):
        raise ValueError("Unmapped INPE country")
    frame["frp"] = pd.to_numeric(frame.frp, errors="coerce")
    frame.loc[~np.isfinite(frame.frp) | frame.frp.lt(0), "frp"] = np.nan
    frame["country_code"] = frame.pais.map(INPE_COUNTRIES)
    frame["state"] = frame.estado.fillna("")
    frame["municipality"] = frame.municipio.fillna("")
    for c in ["pais_id", "estado_id", "municipio_id"]:
        frame[c] = frame[c].fillna("")
    columns = ["date", "satelite", "country_code", "pais", "state", "municipality",
               "pais_id", "estado_id", "municipio_id"]
    parts = []
    for level in ["municipality", "state", "country"]:
        local = frame.copy()
        if level in {"state", "country"}:
            local["municipality"] = ""
            local["municipio_id"] = ""
        if level == "country":
            local["state"] = ""
            local["estado_id"] = ""
        grouped = local.groupby(columns, dropna=False).agg(
            detections=("id", "size"), frp_sum=("frp", "sum"), frp_valid=("frp", "count")).reset_index()
        grouped.loc[grouped.frp_valid.ne(grouped.detections), "frp_sum"] = np.nan
        grouped["level"] = level
        parts.append(grouped)
    return pd.concat(parts, ignore_index=True)


def inpe(fetcher):
    global WRITE_POOL
    docs = []
    for url in [
        "https://www.terrabrasilis.dpi.inpe.br/queimadas/portal/pages/secao_downloads/dados-abertos/index.html",
        "https://data.inpe.br/queimadas/portal/pages/secao_informacoes/faq/index.html",
        INPE_BASE + "mensal/America_Sul/",
    ]:
        _, h = fetcher.get(url, max_age_hours=720)
        docs.append(h)
    months = pd.period_range(START, END.tz_localize(None), freq="M").strftime("%Y%m").tolist()
    cache = ROOT / "archive/realworld5_inpe_months"
    cache.mkdir(parents=True, exist_ok=True)

    def acquire(month):
        path = cache / (month + ".parquet")
        receipt_path = cache / (month + ".json")
        url = INPE_BASE + "mensal/America_Sul/focos_mensal_" + month + ".csv"
        raw, h = fetcher.get(url, timeout=300, max_age_hours=720 if month < months[-1] else 12)
        if path.exists() and receipt_path.exists():
            info = json.loads(receipt_path.read_text())
            if info.get("raw_sha256") == h and info.get("parser_version") == 1:
                return path, h
        table = inpe_aggregate(raw, month)
        table.to_parquet(path, index=False)
        atomic_bytes(receipt_path, json.dumps({"url": url, "raw_sha256": h,
                                               "parser_version": 1}).encode())
        print("MONTH", month, len(raw), len(table), flush=True)
        return path, h

    outcomes = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks = {pool.submit(acquire, m): m for m in months}
        for task in as_completed(tasks):
            month = tasks[task]
            try:
                outcomes.append(task.result())
            except Exception as exc:
                FAILURES.append({"provider": "inpe", "partition": month, "error": str(exc)})
                print("FAIL", "inpe", month, str(exc), flush=True)
    if not outcomes:
        return []
    manifest = archive_raw_manifest([h for _, h in outcomes] + docs,
                                    description="INPE South America original monthly detection CSVs and documentation")
    table = pd.concat([pd.read_parquet(p) for p, _ in outcomes], ignore_index=True)
    canonical = ["satelite", "country_code", "level", "estado_id", "municipio_id"]
    if table.groupby(canonical)[["pais", "state", "municipality"]].nunique().gt(1).any().any():
        raise ValueError("INPE geographic names changed for the same code; a reviewed crosswalk is required")
    observed = {sat: pd.DatetimeIndex(sorted(part.date.unique()))
                for sat, part in table.groupby("satelite")}
    keys = ["satelite", "country_code", "pais", "level", "state", "municipality",
            "pais_id", "estado_id", "municipio_id"]
    results = []
    # Disk-only concurrency amortizes durable filesystem writes. Source HTTP stays <=3.
    WRITE_POOL = ThreadPoolExecutor(max_workers=8)
    for key, part in table.groupby(keys, dropna=False, sort=True):
        sat, country, country_name, level, state, municipality, pid, stid, mid = key
        if level == "municipality" and (not state or not municipality or not mid or not stid):
            OMISSIONS["inpe_missing_geographic_identity"] += 1
            continue
        # Substantial repeated activity, rather than a single event plus thousands of zeros.
        active_weeks = part.date.dt.tz_localize(None).dt.to_period("W").nunique()
        if active_weeks < 26 or part.date.nunique() < 52:
            OMISSIONS["inpe_sparse_activity"] += 1
            continue
        if part.date.duplicated().any():
            raise ValueError("INPE geographic ID has multiple names on same day")
        idx = observed[sat]
        group_name = " / ".join(v for v in [country_name, state, municipality] if v)
        identity = f"inpe:active_fire:{sat}:{country}:{level}:{stid}:{mid}"
        suffix = hashlib.sha256(identity.encode()).hexdigest()[:12]
        sid_base = f"inpe_{slug(sat)}_{country.lower()}_{slug(level)}_{suffix}"
        common = dict(provider="INPE South America satellite fire observations",
                      source_url=INPE_BASE + "mensal/America_Sul/",
                      geography=group_name, geography_scope="non_us", country_codes=[country],
                      raw_sha256=docs, raw_manifest_sha256=[manifest],
                      source_satellite=sat, source_state=state, source_municipality=municipality,
                      source_state_id=stid, source_municipality_id=mid, geographic_level=level,
                      active_detection_weeks=int(active_weeks), active_detection_days=int(part.date.nunique()),
                      native_frequency="irregular satellite overpasses, usually several observations per day",
                      finer_frequency_available=True,
                      expected_publication_lag="Operational observations released after processing, typically near real time; monthly archive is revised and may be published later. No historical availability reconstruction.",
                      missing_value_policy="Zero means no reported detections in an acquired monthly archive on a day with this satellite reporting elsewhere in South America. Entire satellite days without reported detections and missing source months are omitted.",
                      limitations="Satellite detections are fire pixels, not unique fires or burnt area. Cloud, revisit frequency, reception outages and detection thresholds affect counts; activity elsewhere does not establish unobstructed local coverage. Platforms remain separate and overlap. Administrative names/boundaries are source definitions. Current revised history, not point-in-time data.")
        for metric in ["detections", "frp_sum"]:
            values = part.set_index("date")[metric].reindex(idx)
            absent = ~idx.isin(part.date)
            values.iloc[np.flatnonzero(absent)] = 0.0
            data = pd.DataFrame({"date": idx, "value": values.to_numpy()}).dropna()
            sid = sid_base + "_" + metric
            active_metric_days = int(data.value.gt(0).sum())
            cadence = baseline()["pinned_frequency"].get(sid, "daily" if active_metric_days >= 90 else "weekly")
            if cadence == "weekly":
                data = complete_fire_weeks(data)
                if data.value.gt(0).sum() < 26:
                    OMISSIONS["inpe_fewer_than_26_active_complete_weeks"] += 1
                    continue
            elif active_metric_days < 90:
                OMISSIONS["inpe_fewer_than_90_active_days"] += 1
                continue
            period = "UTC day" if cadence == "daily" else "complete UTC Monday–Sunday week"
            extra = (dict(title=f"{group_name}: {sat} detected fire pixels",
                          unit=f"reported satellite fire-pixel detections per {period}",
                          category="satellite_fire_activity",
                          hypothesis="Repeated observed vegetation-fire activity may precede agricultural supply losses, smoke-related transport disruption, insurance costs and commodity volatility.")
                     if metric == "detections" else
                     dict(title=f"{group_name}: {sat} summed detected fire radiative power",
                          unit=f"MW summed over reported detections per {period}; not energy",
                          category="satellite_fire_intensity",
                          hypothesis="Observed fire radiative power may reveal the intensity of vegetation burning and smoke pressure affecting crops, forestry, logistics and power infrastructure."))
            result = save(sid, data, frequency=cadence,
                          active_metric_days=active_metric_days,
                          timestamp_semantics=("UTC day of the source GMT satellite detection timestamp"
                                               if cadence == "daily" else
                                               "UTC Monday beginning a complete seven-day source-reporting week, aggregated from source GMT detection days"),
                          aggregation=("Native detection counts or measured FRP summed within a UTC day"
                                       if cadence == "daily" else
                                       "Sum of seven daily source-reported values; any week missing a day or containing a regional day with missing FRP is omitted"),
                          dataset_identity=identity + ":" + metric,
                          source_metric=metric, **common, **extra)
            if result:
                results.append(result)
    try:
        results = [r.result() if isinstance(r, Future) else r for r in results]
    finally:
        WRITE_POOL.shutdown(wait=True)
        WRITE_POOL = None
    if not baseline()["pinned_ids"]:
        accepted = {m["series_id"] for m in results}
        excluded = ROOT / "archive/realworld5_excluded/inpe_quality"
        excluded.mkdir(parents=True, exist_ok=True)
        for path in (ROOT / "archive/staging" / GROUP).glob("inpe_*.parquet"):
            if path.stem not in accepted:
                path.replace(excluded / path.name)
                sidecar = path.with_suffix(".json")
                if sidecar.exists():
                    sidecar.replace(excluded / sidecar.name)
    return results


EEA_API = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
EEA_DOC = "https://eeadmz1-downloads-webapp.azurewebsites.net/content/documentation/How_To_Downloads.pdf"
UTC_PLUS_ONE = timezone(timedelta(hours=1))


def eea_parse(frame):
    """Preserve only source-valid observations, fixed UTC+1 hourly labels, and native cadence."""
    columns = {"Samplingpoint", "Pollutant", "Start", "End", "Value", "Unit", "AggType",
               "Validity", "Verification", "ResultTime"}
    if not columns.issubset(frame):
        raise ValueError("EEA Parquet schema changed")
    frame = frame[frame.Validity.isin([1, 2, 3, 4])].copy()
    frame["Value"] = pd.to_numeric(frame.Value, errors="coerce")
    frame = frame[np.isfinite(frame.Value)]
    frame = frame[frame.AggType.isin(["hour", "day"])]
    output = []
    for key, part in frame.groupby(["Samplingpoint", "Pollutant", "Unit", "AggType"]):
        point, pollutant, unit, aggregation = key
        part = part.copy()
        start = pd.to_datetime(part.Start, errors="raise")
        if start.dt.tz is not None:
            raise ValueError("EEA unexpectedly supplied aware timestamps; inspect documented timezone")
        # Official download page: hourly and variable labels use fixed UTC+1.
        # Daily values are calendar period labels, not an assertion of release time.
        part["date"] = (start.dt.tz_localize(UTC_PLUS_ONE).dt.tz_convert("UTC")
                        if aggregation == "hour" else start.dt.tz_localize("UTC"))
        end = pd.to_datetime(part.End, errors="raise")
        expected = pd.Timedelta(hours=1) if aggregation == "hour" else pd.Timedelta(days=1)
        part = part[(end - start).eq(expected) & part.date.ge(pd.Timestamp(START, tz="UTC"))
                    & part.date.lt(END + pd.Timedelta(days=1))]
        if part.empty:
            continue
        # Latest source reporting edition wins; tied conflicting results reject the panel.
        part["ResultTime"] = pd.to_datetime(part.ResultTime, errors="raise")
        latest = part.groupby("date").ResultTime.transform("max")
        part = part[part.ResultTime.eq(latest)]
        if part.groupby("date").Value.nunique().gt(1).any():
            raise ValueError(f"Conflicting EEA latest results for {point}")
        part = part.drop_duplicates("date").sort_values("date")
        data = pd.DataFrame({"date": part.date, "value": part.Value.astype(float)})
        output.append((key, data, {
            "source_validity_codes": sorted(int(v) for v in part.Validity.unique()),
            "source_verification_codes": sorted(int(v) for v in part.Verification.unique()),
            "source_result_time_min": str(part.ResultTime.min()),
            "source_result_time_max": str(part.ResultTime.max()),
        }))
    return output


def eea(fetcher, countries=None, limit=None):
    country_raw, country_h = fetcher.get(EEA_API + "/Country", max_age_hours=720)
    pollutant_raw, pollutant_h = fetcher.get(EEA_API + "/Pollutant", max_age_hours=720)
    country_names = {c["countryCode"]: c["countryName"] for c in json.loads(country_raw)}
    pollutant_names = {int(p["code"]): p["notation"] for p in json.loads(pollutant_raw)
                       if p.get("code") is not None}
    docs = [country_h, pollutant_h]
    for url in [EEA_DOC, "https://eeadmz1-downloads-webapp.azurewebsites.net/",
                "https://dd.eionet.europa.eu/vocabulary/aq/observationvalidity/json",
                "https://dd.eionet.europa.eu/vocabulary/aq/observationverification/json"]:
        _, h = fetcher.get(url, max_age_hours=720)
        docs.append(h)
    targets = countries or (baseline()["pinned_eea_countries"] if baseline()["pinned_ids"]
                            else sorted(country_names))
    inventories = {}
    for country in targets:
        body = {"countries": [country], "cities": [], "pollutants": [], "dataset": 1, "source": "API"}
        raw, h = post_read(EEA_API + "/ParquetFile/urls", body)
        links = [line.strip() for line in raw.decode("utf-8-sig").splitlines()
                 if line.strip().startswith("https://")]
        if any("blob.core.windows.net/" not in u or not u.endswith(".parquet") for u in links):
            raise ValueError("Unexpected EEA raw file link")
        inventories[country] = (sorted(set(links)), h)
        print("EEA INVENTORY", country, len(links), flush=True)
        time.sleep(.15)
    results = []

    def acquire(country):
        links, inventory_h = inventories[country]
        if len(links) > 300:
            major = [["PM10", "PM2.5"], ["NO2", "NOX as NO2", "NO"], ["O3"]]
            used = {v for group in major for v in group}
            groups = major + [sorted(set(pollutant_names.values()) - used)]
        else:
            groups = [[]]
        panels = []
        for pollutants in groups:
            body = {"countries": [country], "cities": [], "pollutants": pollutants,
                    "dataset": 1, "source": "API", "dateTimeStart": f"{END.year}-01-01T00:00:00Z",
                    "dateTimeEnd": END.strftime("%Y-%m-%d") + "T23:59:59Z", "compress": True}
            raw, h = post_read(EEA_API + "/ParquetFile", body, max_age_hours=24)
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                for member in archive.namelist():
                    if not member.endswith(".parquet"):
                        raise ValueError("Unexpected file in EEA Parquet ZIP")
                    try:
                        parsed = eea_parse(pd.read_parquet(archive.open(member)))
                        for key, data, quality in parsed:
                            quality["source_archive_member"] = member
                            panels.append((key, data, quality, h))
                    except Exception as exc:
                        FAILURES.append({"provider": "eea", "partition": f"{country}/{member}",
                                         "error": str(exc)})
            print("EEA BULK", country, len(raw), "panels", len(panels), flush=True)
            time.sleep(.2)
        # Prefer native hourly measurements to daily summaries of the same sampling point.
        hourly = {(str(k[0]), int(k[1]), str(k[2])) for k, d, _, _ in panels
                  if k[3] == "hour" and qualifies_research(dict(coverage(d), frequency="hourly"))}
        qualified = []
        for entry in panels:
            key, data, _, _ = entry
            if key[3] == "day" and (str(key[0]), int(key[1]), str(key[2])) in hourly:
                OMISSIONS["eea_daily_alias_of_hourly"] += 1
                continue
            if data.value.nunique() > 1 and qualifies_research(
                    dict(coverage(data), frequency="hourly" if key[3] == "hour" else "daily")):
                qualified.append(entry)
        # Bounded, broad country coverage: uncommon chemicals first, then combustion metrics.
        # Once published, all published identities are eligible for refresh regardless of this cap.
        cap = len(qualified) if baseline()["pinned_ids"] else (limit or 150)
        chemical = [p for p in qualified if int(p[0][1]) not in {1, 5, 7, 8, 9, 10, 6001}]
        common = [p for p in qualified if int(p[0][1]) in {1, 5, 7, 8, 9, 10, 6001}]

        def balanced(entries, count):
            buckets = {}
            for entry in entries:
                buckets.setdefault(int(entry[0][1]), []).append(entry)
            for bucket in buckets.values():
                bucket.sort(key=lambda p: (-len(p[1]), str(p[0][0])))
            selected = []
            while buckets and len(selected) < count:
                for code in sorted(list(buckets)):
                    selected.append(buckets[code].pop(0))
                    if not buckets[code]:
                        del buckets[code]
                    if len(selected) == count:
                        break
            return selected

        selected = balanced(chemical, min(cap, max(cap // 3, min(len(chemical), 100))))
        selected += balanced(common, cap - len(selected))
        if len(selected) < cap:
            chosen_keys = {str(p[0]) for p in selected}
            selected += balanced([p for p in chemical if str(p[0]) not in chosen_keys], cap - len(selected))
        OMISSIONS["eea_country_diversity_cap"] += len(qualified) - len(selected)
        return country, EEA_API + "/ParquetFile", inventory_h, selected

    # Official bulk API avoids thousands of tiny requests; no more than three at a time.
    with ThreadPoolExecutor(max_workers=3) as pool:
        for number, (item, task) in enumerate(bounded_results(pool, acquire, targets, window=3), 1):
            try:
                country, url, inventory_h, panels = task.result()
                for key, data, quality, h in panels:
                    point, pollutant, unit, agg = key
                    code = int(pollutant)
                    name = pollutant_names.get(code, f"pollutant {code}")
                    ident = f"eea:observed:{point}:pollutant={code}:unit={unit}"
                    sid = "eea_" + slug(point) + "_" + str(code) + "_" + hashlib.sha256(str(unit).encode()).hexdigest()[:6]
                    category = ("industrial_trace_chemicals" if code not in {1, 5, 7, 8, 9, 10, 38, 6001, 6002}
                                else "european_combustion_pollution")
                    result = save(
                        sid, data, title=f"{country_names[country]} {point}: {name}",
                        provider="European Environment Agency observed air chemistry",
                        category=category, unit=str(unit), frequency="hourly" if agg == "hour" else "daily",
                        source_url=url, geography=f"{country_names[country]} / sampling point {point}",
                        geography_scope="non_us", country_codes=[country],
                        hypothesis="Measured pollutant concentrations can proxy industrial combustion, traffic, solvent use, agriculture and smoke disruptions; effects must be separated from meteorology before testing commodity, utility, travel or health-sector signals.",
                        limitations="E2a provider-valid but unverified current reporting data; subject to revision. Includes source-valid below-detection-limit values and source measurement noise, including negatives. Sampling points may measure overlapping local plumes. No gap filling, no modelled data, no claim of causal trading predictiveness or historical availability.",
                        raw_sha256=docs + [inventory_h, h], dataset_identity=ident,
                        source_sampling_point=str(point), source_pollutant_code=code,
                        source_pollutant_name=name, source_dataset="E2a unverified observations",
                        source_aggregation=str(agg), source_timezone="fixed UTC+01 for hourly intervals; source calendar labels for daily values",
                        timestamp_semantics=("Start of source measurement interval, converted from fixed UTC+01 to UTC"
                                             if agg == "hour" else "Source daily calendar period label; represented at UTC midnight"),
                        native_frequency="hourly" if agg == "hour" else "daily",
                        finer_frequency_available=agg == "hour",
                        expected_publication_lag="E2a observations transmitted continuously by national providers, usually hours to days; revisions can be later. ResultTime range recorded, not a complete historical as-of record.",
                        **quality)
                    if result:
                        results.append(result)
            except Exception as exc:
                FAILURES.append({"provider": "eea", "partition": item, "error": str(exc)})
                print("FAIL", "eea", item, str(exc), flush=True)
            print("EEA PROGRESS", number, len(targets), "accepted", len(results), flush=True)
    return results


def lice_parse(tables):
    table = pd.concat(tables, ignore_index=True)
    required = {"Site No", "Site Name", "Business Name", "Week Beginning", "Weekly Average AF",
                "Reason For No Count", "Year", "Week No"}
    if not required.issubset(table):
        raise ValueError("Scotland sea-lice schema changed")
    table["date"] = pd.to_datetime(table["Week Beginning"], utc=True, errors="raise")
    iso = table.date.dt.isocalendar()
    valid_week = (iso.year.eq(pd.to_numeric(table.Year))
                  & iso.week.eq(pd.to_numeric(table["Week No"]))
                  & table.date.dt.weekday.eq(0))
    OMISSIONS["lice_inconsistent_source_week_label"] += int((~valid_week).sum())
    table = table[valid_week].copy()
    table["value"] = pd.to_numeric(table["Weekly Average AF"], errors="coerce")
    # A no-count reason overrides any placeholder numeric zero in the CSV.
    table = table[table["Reason For No Count"].fillna("").str.strip().eq("")
                  & table.value.notna() & table.value.ge(0) & table.date.le(END)].copy()
    panels = []
    for site, part in table.groupby("Site No"):
        if part.groupby("date").value.nunique().gt(1).any():
            raise ValueError("Conflicting Scottish weekly sea-lice readings")
        part = part.sort_values("date").drop_duplicates("date")
        panels.append((str(site), part[["date", "value"]], part.iloc[-1].to_dict()))
    return panels


def scotland_lice(fetcher):
    hashes = []
    tables = []
    for name in ["archive", "current"]:
        url = f"https://aquaculture.scotland.gov.uk/csv/ms_sea_lice_{name}.csv"
        raw, h = fetcher.get(url, max_age_hours=24)
        tables.append(pd.read_csv(io.BytesIO(raw), dtype=str))
        hashes.append(h)
    for url in ["https://aquaculture.scotland.gov.uk/",
                "https://www.gov.scot/publications/fish-health-inspectorate-sea-lice-information/"]:
        _, h = fetcher.get(url, max_age_hours=720)
        hashes.append(h)
    results = []
    for site, data, latest in lice_parse(tables):
        result = save(
            "scotland_sealice_" + slug(site), data,
            title=f"Scotland {latest['Site Name']} ({site}): adult female sea lice per fish",
            provider="Scottish Government fish-farm health reporting",
            category="aquaculture_parasite_pressure", unit="weekly average adult female sea lice per fish",
            frequency="weekly", source_url="https://aquaculture.scotland.gov.uk/csv/ms_sea_lice_current.csv",
            geography=f"Scotland / {latest['Site Name']}", geography_scope="non_us", country_codes=["GB"],
            hypothesis="Sea-lice pressure can precede salmon treatment costs, harvest timing changes and seafood supply disruptions, with potential effects on listed aquaculture producers and seafood prices.",
            limitations="Industry-reported weekly averages; revised histories and incomplete monitoring. Missing or explicit no-count weeks omitted, including fallow periods. Farm ownership may change. Counts do not establish mortality or predict prices by themselves.",
            raw_sha256=hashes, dataset_identity=f"scotland:fish-health:site={site}:weekly-adult-female-sealice",
            source_site_id=site, source_site_name=latest["Site Name"],
            latest_reported_business_name=latest["Business Name"],
            native_frequency="weekly", finer_frequency_available=False,
            expected_publication_lag="Farm reports due within 8 days after week end; Scottish Government aims to publish 12 days after week end. Source revisions may occur.",
            timestamp_semantics="Monday beginning the source ISO reporting week; UTC midnight period label",
            license="Open Government Licence v3")
        if result:
            results.append(result)
    return results


PHYTO_METRICS = {
    "PseudoNitzschiaResultValue": "Pseudo-nitzschia spp.",
    "AlexandriumResultValue": "Alexandrium spp.",
    "DinophysisResultValue": "Dinophysis spp.",
    "ProrocentrumLimaResultValue": "Prorocentrum lima",
    "ProrocentrumCordatumResultValue": "Prorocentrum cordatum",
    "LingulaulaxPolyedraResultValue": "Lingulaulax polyedra (formerly Lingulodinium polyedrum)",
    "ProtoceratiumReticulatumResultValue": "Protoceratium reticulatum",
}


def phytoplankton_parse(table):
    required = {"Id", "CollectedTimestamp", "Sin", "SiteName", "AreaName"} | set(PHYTO_METRICS)
    if not required.issubset(table):
        raise ValueError("Cefas phytoplankton export schema changed")
    if table.Id.duplicated().any():
        raise ValueError("Repeated Cefas phytoplankton sample ID")
    table = table.reset_index(drop=True).copy()
    table["date"] = pd.to_datetime(table.CollectedTimestamp, dayfirst=True, errors="raise").dt.floor("D").dt.tz_localize("UTC")
    table = table[table.date.le(END)]
    for field in PHYTO_METRICS:
        # Blanks, rejected samples, inequality/censoring flags and narrative values are missing.
        table[field] = pd.to_numeric(table[field].astype(str).str.strip(), errors="coerce")
        table.loc[table[field].lt(0) | ~np.isfinite(table[field]), field] = np.nan
    old = pd.to_numeric(table.LingulodiniumPolyedrumResultValue.astype(str).str.strip(), errors="coerce")
    new = table.LingulaulaxPolyedraResultValue
    conflict = old.notna() & new.notna() & old.ne(new)
    # Taxonomic synonyms are one biological series, never separate additional histories.
    table["LingulaulaxPolyedraResultValue"] = new.where(new.notna(), old).mask(conflict)
    panels = []
    for site, part in table.groupby("Sin"):
        if not str(site).strip():
            continue
        for metric, title in PHYTO_METRICS.items():
            valid = part[part[metric].notna()]
            if valid.empty:
                continue
            # Distinct valid water samples on a day are equally weighted, with no interpolation.
            data = valid.groupby("date")[metric].mean().rename("value").reset_index()
            latest = valid.sort_values("date").iloc[-1].to_dict()
            panels.append((str(site), metric, title, data, latest,
                           int(valid.groupby("date").size().gt(1).sum())))
    return panels


def scotland_plankton(fetcher):
    url = "https://smc-api.cefas.co.uk/api/result/phytoplankton/csv/filtered"
    body = {"search": "", "filters": {}, "sortOrder": "-collectedTimestamp",
            "itemsPerPage": 10, "currentPage": 1}
    raw, h = post_read(url, body, max_age_hours=24)
    table = pd.read_csv(io.BytesIO(raw), dtype=str)
    # Check the full CSV is the whole source result set, not a silent paginated extract.
    summary_raw, summary_h = post_read("https://smc-api.cefas.co.uk/api/result/phytoplankton/list",
                                      dict(body, itemsPerPage=1), max_age_hours=24)
    if len(table) != int(json.loads(summary_raw)["totalItems"]):
        raise ValueError("Cefas CSV count differs from whole-query source count")
    hashes = [h, summary_h]
    for doc in [
        "https://www.foodstandards.gov.scot/business-guidance/industry-specific-advice/fish-and-shellfish/shellfish",
        "https://www.cefas.co.uk/media/sk3ftgc5/c7715-sampling-officer-shellfish-sampling-and-transport-protocol-scotland-v9-accessible-may2026-final.pdf",
        "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=160621",
    ]:
        _, digest = fetcher.get(doc, max_age_hours=720)
        hashes.append(digest)
    results = []
    for site, field, taxon, data, latest, replicates in phytoplankton_parse(table):
        metric = slug(field.replace("ResultValue", ""))
        result = save(
            f"scotland_plankton_{slug(site)}_{metric}", data,
            title=f"Scotland {latest['SiteName']} ({site}): {taxon} cell concentration",
            provider="Cefas and Food Standards Scotland shellfish monitoring",
            category="harmful_algal_bloom_pressure", unit="cells per litre of sampled seawater",
            frequency="weekly", source_url=url,
            geography=f"Scotland / {latest['LocalAuthorityName']} / {latest['AreaName']} / {latest['SiteName']}",
            geography_scope="non_us", country_codes=["GB"],
            hypothesis="Toxin-producing plankton can precede shellfish harvest closures, aquaculture disruption, food-supply effects and local marine operating constraints.",
            limitations="Sampling is weekly mainly March–September, less frequent in winter; only histories passing weekly coverage and spacing thresholds are retained. Native sampling dates remain irregular. Missing, rejected and censored results are omitted. Distinct same-day water samples are averaged. Local monitoring is not representative of all coastal water, and observed algae do not directly measure toxin concentration.",
            raw_sha256=hashes, dataset_identity=f"cefas:scotland:phytoplankton:sin={site}:taxon={metric}",
            source_site_id=site, source_field=field, source_taxon=taxon,
            same_day_replicate_days=replicates,
            native_frequency="weekly March–September; fortnightly October and reduced winter sampling",
            finer_frequency_available=False,
            expected_publication_lag="Results follow collection, transport and laboratory processing, generally on a scale of days, and feed the weekly monitoring programme. Exact publication timestamps are not supplied by this CSV.",
            timestamp_semantics="Actual source sample collection calendar date, represented as UTC midnight; not release time",
            license="Open Government Licence v3")
        if result:
            results.append(result)
    return results


def main():
    global REFRESH, WRITE_POOL
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=["all", "inpe", "eea", "scotland_lice", "scotland_plankton"], default="all")
    parser.add_argument("--refresh", action="store_true", help="Bypass archived-response caches")
    parser.add_argument("--countries", nargs="*")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    REFRESH = args.refresh
    fetcher = Fetcher(refresh=args.refresh)
    begin = pd.Timestamp.now(tz="UTC")
    results = []
    providers = ["inpe", "eea", "scotland_lice", "scotland_plankton"] if args.provider == "all" else [args.provider]
    for provider in providers:
        try:
            incoming = (inpe(fetcher) if provider == "inpe" else
                        eea(fetcher, args.countries, args.limit) if provider == "eea" else
                        scotland_lice(fetcher) if provider == "scotland_lice" else scotland_plankton(fetcher))
            results.extend(incoming)
        except Exception as exc:
            FAILURES.append({"provider": provider, "error": str(exc)})
            print("FAIL", provider, str(exc), flush=True)
        finally:
            if WRITE_POOL is not None:
                WRITE_POOL.shutdown(wait=True)
                WRITE_POOL = None
    report = {"provider": args.provider, "started_at": begin.isoformat(),
              "finished_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "count": len(results), "observations": sum(r["rows"] for r in results),
              "categories": dict(Counter(r["category"] for r in results)),
              "countries": dict(Counter(c for r in results for c in r["country_codes"])),
              "failures": FAILURES, "omissions": dict(OMISSIONS)}
    atomic_bytes(ROOT / "logs" / ("realworld5_" + args.provider + ".json"),
                 json.dumps(report, indent=2).encode())
    print("COMPLETE", json.dumps(report), flush=True)
    return int(bool(FAILURES))


if __name__ == "__main__":
    raise SystemExit(main())
