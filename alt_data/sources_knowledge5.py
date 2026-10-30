#!/usr/bin/env python3
"""Native gaming-market, desktop-software and cybersecurity observations.

The original provider responses are archived before parsing. A missing source
row is never converted into a zero or a carried-forward value.
"""
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from functools import lru_cache
import gzip
import hashlib
import io
import json
import re
import threading
import time
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests

from country_codes import COUNTRY_ALPHA2, normalize_country_codes
from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes,
                              coverage, normalize_frame, qualifies_research,
                              save_candidate)

GROUP = "knowledge5"
US_CODES = {"US", "AS", "GU", "MP", "PR", "UM", "VI"}
SEEN = {}
LOCK = threading.Lock()
OMISSIONS = []
FLATHUB_PARSER_VERSION = 3


class Omission(ValueError):
    pass


class ProviderAccess:
    """Stop queued requests after an explicit provider access/rate-limit block."""
    def __init__(self, fetcher):
        self.fetcher = fetcher
        self.refresh = fetcher.refresh
        self.stopped = threading.Event()

    def check(self):
        if self.stopped.is_set():
            raise Omission("Provider requests stopped after an access/rate-limit response")

    def get(self, *args, **kwargs):
        self.check()
        try:
            return self.fetcher.get(*args, **kwargs)
        except requests.RequestException as exc:
            if getattr(getattr(exc, "response", None), "status_code", None) in {401, 403, 429}:
                self.stopped.set()
            raise


class Pace:
    def __init__(self, interval):
        self.interval, self.next = interval, 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            pause = max(0, self.next - time.monotonic())
            self.next = time.monotonic() + pause + self.interval
        if pause:
            time.sleep(pause)


def check_access(fetcher):
    """Fail queued network work before reserving another paced request slot."""
    if isinstance(fetcher, ProviderAccess):
        fetcher.check()


def slug(text):
    return re.sub("[^a-z0-9]+", "_", str(text).lower()).strip("_")


def token(text):
    return slug(text)[:85] + "_" + hashlib.sha256(str(text).encode()).hexdigest()[:10]


@lru_cache(maxsize=1)
def baseline():
    path = ROOT / "archive/knowledge5_selection/baseline.json"
    if not path.exists():
        cat = pd.read_parquet(ROOT / "catalog.parquet")
        cat = cat[cat.batch.lt(7)]
        atomic_bytes(path, json.dumps({
            "series_ids": cat.series_id.tolist(),
            "identities": cat.dataset_identity.dropna().tolist(),
            "hashes": cat.content_sha256.tolist(),
        }).encode())
    obj = {k: set(v) for k, v in json.loads(path.read_text()).items()}
    cat = pd.read_parquet(ROOT / "catalog.parquet", columns=["series_id", "batch"])
    obj["pinned_ids"] = set(cat.loc[cat.batch.eq(7), "series_id"])
    return obj


def selection(name, discover):
    path = ROOT / "archive/knowledge5_selection" / (name + ".json")
    if path.exists():
        return json.loads(path.read_text())
    obj = discover()
    obj["selected_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(path, json.dumps(obj, ensure_ascii=False, indent=2).encode())
    return obj


def clean(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                          "value": pd.to_numeric(values, errors="raise")}).dropna()
    if frame.empty:
        raise Omission("No numeric source observations")
    if frame.groupby("date").value.nunique().gt(1).any():
        raise ValueError("Conflicting duplicate source dates")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    old = baseline()
    if old["pinned_ids"] and sid not in old["pinned_ids"]:
        raise Omission("Not in the published batch 7 selection")
    if sid in old["series_ids"] or meta["dataset_identity"] in old["identities"]:
        raise Omission("Identity already published before batch 7")
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2:
        raise Omission("Constant history")
    if not qualifies_research({"frequency": meta["frequency"], **coverage(frame)}):
        raise Omission("Insufficient actual daily/weekly coverage")
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    with LOCK:
        if digest in old["hashes"] or (digest in SEEN and SEEN[digest] != sid):
            raise Omission("Duplicate full numeric history")
        SEEN[digest] = sid
    meta["country_codes"] = normalize_country_codes(meta.get("country_codes", []))
    meta["country_code_standard"] = "ISO 3166-1 alpha-2" + (
        "; XK is the commonly used Kosovo extension" if "XK" in meta["country_codes"] else "")
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("finer_frequency_available", False)
    meta.setdefault("geography_scope", "global")
    meta.setdefault("timestamp_semantics",
                    "UTC observation-period label, not historical public availability time.")
    result = save_candidate(GROUP, sid, frame, **meta)
    print(f"OK {meta['provider']} {sid} {len(frame)}", flush=True)
    return result


def attempt(good, name, fn):
    try:
        good.append(fn())
    except Omission as exc:
        OMISSIONS.append({"candidate": name, "reason": str(exc)})


def require_activity(frame, minimum=90):
    active = frame.loc[frame.value.gt(0), "date"]
    if active.dt.floor("D").nunique() < minimum or (active.max() - active.min()).days < 90:
        raise Omission("Fewer than 90 genuinely active days over at least 90 days")
    return frame


def app_metadata(raw):
    root = ET.fromstring(gzip.decompress(raw))
    result = {}
    for component in root.findall("component"):
        app_id = component.findtext("id")
        if not app_id:
            continue
        info = {
            "name": component.findtext("name") or app_id,
            "categories": sorted({x.text for x in component.findall("categories/category") if x.text}),
            "component_type": component.get("type", ""),
        }
        if app_id in result and result[app_id] != info:
            # Multiple architecture records can differ; categories are set-valued.
            result[app_id]["categories"] = sorted(set(result[app_id]["categories"]) | set(info["categories"]))
        else:
            result[app_id] = info
    return result


def app_category(app_id, info):
    """A transparent grouping of provider AppStream categories, not inferred users."""
    cats = set(info["categories"])
    if "Security" in cats:
        return "desktop_security_privacy_demand"
    if "Game" in cats:
        return "desktop_gaming_demand"
    if "Finance" in cats:
        return "desktop_finance_software_demand"
    if "Science" in cats:
        return "desktop_scientific_tools_demand"
    if "Education" in cats:
        return "desktop_learning_demand"
    if "AudioVideo" in cats or "Audio" in cats or "Video" in cats:
        return "desktop_audio_video_demand"
    if "Graphics" in cats:
        return "desktop_graphics_creation_demand"
    if "Development" in cats:
        return "desktop_developer_tools_demand"
    if "Office" in cats:
        return "desktop_office_productivity_demand"
    if "Network" in cats:
        return "desktop_communications_demand"
    if "System" in cats:
        return "desktop_system_tools_demand"
    return "desktop_general_application_demand"


def flathub_country_completeness(obj):
    """Compare ALL IP-country counts, including unknown keys, with native app totals."""
    result = {}
    for app, countries in obj.get("ref_by_country", {}).items():
        arches = obj.get("refs", {}).get(app, {})
        tuples = list(arches.values()) + list(countries.values())
        if any(len(v) != 2 or any(not isinstance(n, int) or n < 0 for n in v) for v in tuples):
            raise ValueError("Invalid Flathub completeness count tuple")
        global_count = sum(v[0] for v in arches.values())
        country_count = sum(v[0] for v in countries.values())
        ratio = country_count / global_count if global_count else None
        complete = bool(arches) and global_count == country_count
        result[app] = {"global_downloads": global_count, "country_downloads": country_count,
                       "fraction": ratio, "complete": complete}
    return result


def parse_flathub_day(obj, day, selected):
    expected = pd.Timestamp(day).strftime("%Y/%m/%d")
    if obj.get("date") != expected:
        raise ValueError("Flathub response period mismatch")
    completeness = flathub_country_completeness(obj)
    rows = []
    for item in selected:
        app, country, metric = item["app"], item["country"], item["metric"]
        if country:
            if not completeness.get(app, {}).get("complete", False):
                continue
            values = obj.get("ref_by_country", {}).get(app, {}).get(country)
            if values is None:
                continue
            if len(values) != 2:
                raise ValueError("Invalid Flathub native country tuple")
            count = values[0]
        else:
            arches = obj.get("refs", {}).get(app)
            if not arches:
                continue
            if any(len(v) != 2 for v in arches.values()):
                raise ValueError("Invalid Flathub architecture tuple")
            count = sum(v[0 if metric == "downloads" else 1] for v in arches.values())
        if not isinstance(count, int) or count < 0:
            raise ValueError("Invalid Flathub native count")
        rows.append((item["key"], pd.Timestamp(day, tz="UTC"), float(count)))
    return pd.DataFrame(rows, columns=["key", "date", "value"])


def collect_flathub(fetcher, start, end, workers):
    good, errors = [], []
    source_url = "https://hub.flathub.org/stats/stable/"
    app_raw, app_hash = fetcher.get("https://flathub.org/repo/appstream/x86_64/appstream.xml.gz",
                                  max_age_hours=720, timeout=180)
    apps = app_metadata(app_raw)
    doc_raw, doc_hash = fetcher.get(
        "https://raw.githubusercontent.com/flathub-infra/flathub-stats/master/update-stats.py",
        max_age_hours=720)
    def discover():
        discovery_day = pd.Timestamp(end) - pd.Timedelta(days=1)
        raw, digest = fetcher.get(source_url + discovery_day.strftime("%Y/%m/%d.json"))
        obj = json.loads(raw)
        candidates = defaultdict(list)
        global_items = []
        for app in sorted(obj["refs"]):
            if "/" in app or app not in apps:
                continue
            info = apps[app]
            for metric in ("downloads", "updates"):
                global_items.append({"app": app, "country": "", "metric": metric,
                                     "key": app + "|" + metric, "info": info})
            for country, vals in obj.get("ref_by_country", {}).get(app, {}).items():
                if country not in COUNTRY_ALPHA2 or country in US_CODES or vals[0] < 5:
                    continue
                category = app_category(app, info)
                candidates[(country, category)].append((vals[0], app, info))
        buckets = {k: deque(sorted(v, key=lambda x: (-x[0], x[1]))) for k, v in candidates.items()}
        country_items = []
        while buckets and len(country_items) < 15000:
            for key in sorted(list(buckets)):
                count, app, info = buckets[key].popleft()
                country_items.append({"app": app, "country": key[0], "metric": "downloads",
                                      "key": app + "|" + key[0], "info": info})
                if not buckets[key]:
                    del buckets[key]
                if len(country_items) >= 15000:
                    break
        return {"items": global_items + country_items, "discovery_sha256": digest,
                "appstream_sha256": app_hash, "discovery_day": str(discovery_day.date()),
                "policy": "All current desktop apps; up to 15,000 non-US country/app cohorts, round-robin by country and AppStream category, descending discovery-day counts within bucket. Minimum 5 downloads on discovery day; minimum 90 active observed days applied later. Selection is ex post, not a historical investable universe."}
    selected = selection("flathub", discover)
    items = selected["items"]
    pace = Pace(0.5)
    cache = ROOT / "archive/knowledge5_flathub_days"
    cache.mkdir(parents=True, exist_ok=True)
    selection_hash = hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest()[:16]
    def day_one(day):
        day = day.strftime("%Y-%m-%d")
        receipt = cache / (day + "_" + selection_hash + ".json")
        target = receipt.with_suffix(".parquet")
        cached = json.loads(receipt.read_text()) if receipt.exists() else {}
        if not fetcher.refresh and target.exists() and cached.get("parser_version") == FLATHUB_PARSER_VERSION:
            return target, cached["raw_sha256"]
        if not fetcher.refresh and cached.get("raw_sha256"):
            digest = cached["raw_sha256"]
            raw = (ROOT / "archive/raw" / (digest + ".bin")).read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                raise ValueError("Archived Flathub raw checksum mismatch")
        else:
            check_access(fetcher)
            pace.wait()
            raw, digest = fetcher.get(source_url + pd.Timestamp(day).strftime("%Y/%m/%d.json"),
                                      max_age_hours=720, timeout=180)
        obj = json.loads(raw)
        complete = flathub_country_completeness(obj)
        frame = parse_flathub_day(obj, day, items)
        frame.to_parquet(target, index=False)
        atomic_bytes(receipt, json.dumps({"raw_sha256": digest, "selection_sha256": selection_hash,
                     "parser_version": FLATHUB_PARSER_VERSION, "country_breakdown_present_apps": len(complete),
                     "country_breakdown_rejected_apps": sorted(k for k, v in complete.items() if not v["complete"])}).encode())
        return target, digest
    paths, hashes = [], [selected["appstream_sha256"], selected["discovery_sha256"], doc_hash]
    days = pd.date_range(max(start, "2025-01-01"), pd.Timestamp(end) - pd.Timedelta(days=1))
    with ThreadPoolExecutor(max_workers=min(3, workers)) as pool:
        futures = {pool.submit(day_one, d): d for d in days}
        for future in as_completed(futures):
            try:
                p, h = future.result()
                paths.append(p); hashes.append(h)
                if len(paths) % 25 == 0:
                    print(f"Flathub complete days {len(paths)}/{len(days)}", flush=True)
            except Exception as exc:
                errors.append({"day": str(futures[future].date()), "error": str(exc)})
    manifest = archive_raw_manifest(hashes, description="Flathub original stable daily server-log summaries, AppStream catalog, selection day and provider parser.")
    data = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    infos = {i["key"]: i for i in items}
    rejected = defaultdict(list)
    daily_quality = []
    for path in sorted(paths):
        receipt = json.loads(path.with_suffix(".json").read_text())
        day = path.name[:10]
        rejected_apps = receipt["country_breakdown_rejected_apps"]
        for app in rejected_apps:
            rejected[app].append(day)
        daily_quality.append({"day": day, "raw_sha256": receipt["raw_sha256"],
                              "present_app_breakdowns": receipt["country_breakdown_present_apps"],
                              "quarantined_app_breakdowns": len(rejected_apps)})
    quality = {"criterion": "For each app/day, sum all native country download counts, including unknown country keys, must exactly equal sum all native architecture download counts. No discrepancy tolerance is allowed. Explicit matching zero totals are allowed. Absent app country keys stay missing. Failing app-days are omitted only from country series, retaining global totals and other complete apps.",
               "days": daily_quality, "excluded_app_days": dict(rejected)}
    atomic_bytes(ROOT / "logs/knowledge5_flathub_country_quarantine.json", json.dumps(quality, indent=2).encode())
    def stage_one(key, frame):
        item = infos[key]
        app, country, metric, info = item["app"], item["country"], item["metric"], item["info"]
        series = require_activity(clean(frame.date, frame.value))
        return save("flathub_" + token(app) + ("_" + country.lower() if country else "_global") + "_" + metric,
                series, title=f"{info['name']}: {country or 'global'} daily Flathub {metric}",
                provider="Flathub desktop application telemetry", category=app_category(app, info),
                unit="server-recorded downloads including updates" if metric == "downloads" else "server-recorded update downloads",
                frequency="daily", native_frequency="individual server requests aggregated daily",
                source_url=source_url, geography=f"Download IP country {country}" if country else "Worldwide Flathub client downloads",
                country_codes=[country] if country else [], geography_scope="non_us" if country else "global",
                hypothesis="Changes in demand for this application may proxy discretionary gaming, learning, communication, media creation, software development or computer-maintenance activity before related sector revenue changes.",
                limitations="Downloads include repeat installations, automated clients, updates, machines and possible bots; they are not unique people or paid purchases. IP geolocation can reflect VPNs and proxies. App categories and selection use the current AppStream catalog, creating survivor/selection bias. Missing app/country keys and unavailable days remain missing. Country breakdowns whose sum differs at all from native global app downloads are omitted for that app/day because some source country tables are partial. Download counts include update counts; their overlap is intentional and explicit.",
                raw_sha256=[manifest], raw_manifest_sha256=[manifest], dataset_identity=f"flathub:stable:{app}:{country or 'global'}:{metric}",
                expected_publication_lag="Daily log processing after period end; exact historical release times are not supplied and late log ingestion can revise totals.",
                app_id=app, appstream_categories=info["categories"], country_assignment="Source server-log IP geolocation; not nationality or user residence",
                aggregation="Sum native architecture counts for global app totals; native country/app count retained unchanged for country cohorts.",
                missing_policy="Only explicit source app/architecture or complete app/country observations; no calendar filling.",
                country_breakdown_completeness=quality["criterion"] if country else "Not applicable to global architecture totals",
                quarantined_country_app_days=rejected.get(app, []) if country else [],
                selection_policy=selected["policy"], unavailable_source_days=[e["day"] for e in errors])
    # Only disk writes are parallel here; at most 32 groups are queued.
    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = {}
        def drain(completed):
            for future in completed:
                key = pending.pop(future)
                try:
                    good.append(future.result())
                except Omission as exc:
                    OMISSIONS.append({"candidate": key, "reason": str(exc)})
        for key, frame in data.groupby("key", sort=True):
            pending[pool.submit(stage_one, key, frame)] = key
            if len(pending) >= 32:
                drain(wait(pending, return_when=FIRST_COMPLETED)[0])
        drain(list(pending))
    if not baseline()["pinned_ids"]:
        accepted = {m["series_id"] for m in good}
        quarantine = ROOT / "archive/knowledge5_quarantine/flathub_country_quality"
        for path in (ROOT / "archive/staging/knowledge5").glob("flathub_*.parquet"):
            if path.stem not in accepted:
                quarantine.mkdir(parents=True, exist_ok=True)
                path.replace(quarantine / path.name)
                sidecar = path.with_suffix(".json")
                if sidecar.exists():
                    sidecar.replace(quarantine / sidecar.name)
    return good, errors


OSRS_FIELDS = {
    "highPriceVolume": ("instant_buy_volume", "virtual_goods_purchase_volume", "item units traded"),
    "lowPriceVolume": ("instant_sell_volume", "virtual_goods_liquidation_volume", "item units traded"),
    "avgHighPrice": ("instant_buy_price", "virtual_goods_prices", "OSRS gold pieces per item"),
}


def parse_osrs_day(obj, day, mapping):
    stamp = int(pd.Timestamp(day, tz="UTC").timestamp())
    if obj.get("timestamp") != stamp:
        raise ValueError("OSRS response timestamp differs from requested complete daily period")
    rows = []
    for item, values in obj["data"].items():
        if item not in mapping:
            continue
        for field in OSRS_FIELDS:
            value = values.get(field)
            if value is not None:
                if not isinstance(value, (int, float)) or value < 0:
                    raise ValueError("Invalid native OSRS numeric observation")
                rows.append((str(item), field, pd.Timestamp(stamp, unit="s", tz="UTC"), float(value)))
    return pd.DataFrame(rows, columns=["item", "field", "date", "value"])


def collect_osrs(fetcher, start, end, workers):
    good, errors = [], []
    url = "https://prices.runescape.wiki/api/v1/osrs"
    def discover():
        raw, digest = fetcher.get(url + "/mapping", max_age_hours=720)
        return {"items": {str(o["id"]): o for o in json.loads(raw)},
                "mapping_sha256": digest,
                "policy": "Pinned source item-ID universe at initial acquisition; later delistings do not erase existing item identities or historical observations."}
    selected = selection("osrs", discover)
    mapping, mapping_hash = selected["items"], selected["mapping_sha256"]
    pace = Pace(1.0)
    paths, hashes = [], [mapping_hash]
    cache = ROOT / "archive/knowledge5_osrs_days"
    cache.mkdir(parents=True, exist_ok=True)
    def day_one(day):
        date = day.strftime("%Y-%m-%d")
        receipt = cache / (date + ".json")
        target = receipt.with_suffix(".parquet")
        if not fetcher.refresh and receipt.exists() and target.exists():
            return target, json.loads(receipt.read_text())["raw_sha256"]
        check_access(fetcher)
        pace.wait()
        raw, digest = fetcher.get(url + "/24h", params={"timestamp": int(day.tz_localize("UTC").timestamp())},
            headers={"User-Agent": "FinAltDataResearch/1.0 (historical aggregate virtual-economy research)"},
            max_age_hours=8760)
        data = parse_osrs_day(json.loads(raw), date, mapping)
        data.to_parquet(target, index=False)
        atomic_bytes(receipt, json.dumps({"raw_sha256": digest}).encode())
        return target, digest
    # Bulk 24-hour blocks keep requests low while preserving actual item observations.
    days = pd.date_range(max(start, "2024-01-01"), pd.Timestamp(end) - pd.Timedelta(days=1))
    with ThreadPoolExecutor(max_workers=min(3, workers)) as pool:
        futures = {pool.submit(day_one, d): d for d in days}
        for future in as_completed(futures):
            try:
                p, h = future.result(); paths.append(p); hashes.append(h)
                if len(paths) % 50 == 0:
                    print(f"OSRS complete days {len(paths)}/{len(days)}", flush=True)
            except Exception as exc:
                errors.append({"day": str(futures[future].date()), "error": str(exc)})
    manifest = archive_raw_manifest(hashes, description="OSRS Wiki/RuneLite original daily all-item transaction aggregates and item mapping.")
    data = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    for (item, field), frame in data.groupby(["item", "field"], sort=True):
        label, category, unit = OSRS_FIELDS[field]
        info = mapping[item]
        # Native volumes across goods are the main behavioral panel. Prices are
        # restricted to subscription bonds, tokens and prestige collectibles.
        if field == "avgHighPrice" and not any(x in info["name"].lower() for x in ("bond", "token", "3rd age")):
            continue
        if len(good) >= 8000:
            break
        def one():
            series = require_activity(clean(frame.date, frame.value))
            return save(f"osrs_item_{item}_{label}", series,
                title=f"Old School RuneScape {info['name']}: daily {label.replace('_', ' ')}",
                provider="OSRS Wiki RuneLite virtual-goods market", category=category,
                unit=unit, frequency="daily", native_frequency="individual player trades aggregated to 24-hour UTC blocks",
                finer_frequency_available=True, finer_frequencies="5-minute, hourly and 6-hour aggregates, with shorter API history",
                source_url=url + "/24h", geography="Global participating RuneLite clients",
                hypothesis="Virtual-goods trading and liquidation may proxy gaming engagement, discretionary attention, in-game liquidity and speculative risk appetite; relationships to listed gaming companies or broad risk assets require empirical validation.",
                limitations="Community-contributed RuneLite observations cover only participating clients, not every game trade. Gold pieces are virtual currency, not fiat revenue. Game patches, bots, item mechanics and platform-specific speculation dominate many moves. The high/low sides refer to instant-buy/instant-sell trade classification, not daily extrema. This is a retrospective current-item universe.",
                raw_sha256=[manifest], raw_manifest_sha256=[manifest], dataset_identity=f"osrs:runelite:24h:{item}:{field}",
                expected_publication_lag="Completed 24-hour trade block plus processing; this collector excludes the current day. Exact historical availability timestamps are not supplied.",
                item_id=item, item_name=info["name"], item_examine=info.get("examine", ""),
                source_field=field, source_members_only=bool(info.get("members")),
                aggregation="Provider-native daily volume or average execution price; no user-side rolling transformations.",
                missing_policy="Source null prices and absent item/day keys omitted, never filled.",
                unavailable_source_days=[e["day"] for e in errors])
        attempt(good, item + ":" + field, one)
    return good, errors


SANS_PORTS = {
    "remote_access": [22,23,3389,5900,5901,5985,5986,2222,2022,8022,10000],
    "web_services": [80,443,8000,8008,8080,8081,8443,8888,9000,9443,3000,5000],
    "databases": [1433,1434,1521,3306,5432,6379,9042,9200,9300,11211,27017,27018,5984],
    "industrial_control": [102,502,1911,1962,2404,2455,44818,47808,20000,9600,4840,18245],
    "file_sharing": [20,21,69,111,135,137,138,139,445,548,873,2049,2121],
    "messaging_email": [25,110,143,465,587,993,995,5222,5269,5672,61616,1883,8883],
    "network_management": [53,67,68,123,161,162,179,389,636,1900,5353,7547,8291,8728,8729],
    "vpn_identity": [500,1194,1701,1723,4500,1812,1813,88,464,51820],
    "media_games": [554,1935,5060,5061,6881,25565,27015,27016,32400,3074],
    "orchestration_compute": [2375,2376,2379,2380,6443,10250,10255,9090,9092,5601,6000,6001,7001],
}


def parse_sans(obj, start, end):
    rows = [v for v in obj.values() if isinstance(v, dict) and "date" in v]
    if not rows:
        raise Omission("No daily SANS source records")
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame.date, utc=True)
    frame = frame[frame.date.ge(pd.Timestamp(start, tz="UTC")) & frame.date.lt(pd.Timestamp(end, tz="UTC"))]
    if frame.date.duplicated().any():
        raise ValueError("Duplicate SANS source dates")
    for field in ["records", "sources", "targets"]:
        values = pd.to_numeric(frame[field], errors="raise")
        if values.dropna().lt(0).any():
            raise ValueError("Negative SANS count")
    return frame


def collect_sans(fetcher, start, end, workers):
    good, errors = [], []
    _, doc_hash = fetcher.get("https://isc.sans.edu/api/", max_age_hours=720)
    pace = Pace(2.0)
    def port_one(category, port):
        check_access(fetcher)
        pace.wait()
        url = f"https://isc.sans.edu/api/porthistory/{port}/{start}/{(pd.Timestamp(end)-pd.Timedelta(days=1)).date()}?json"
        raw, digest = fetcher.get(url, max_age_hours=24)
        data = parse_sans(json.loads(raw), start, end)
        result = []
        for field, unit in [("records", "submitted firewall records"), ("sources", "distinct source IP addresses"), ("targets", "distinct destination IP addresses")]:
            def one():
                series = require_activity(clean(data.date, data[field]))
                return save(f"sans_port_{port}_{field}", series,
                    title=f"DShield port {port}: daily {field}",
                    provider="SANS Internet Storm Center DShield", category="cyber_probing_" + category,
                    unit=unit, frequency="daily", source_url=url, geography="Global voluntary DShield sensors",
                    hypothesis="Shifts in scanning pressure against this service class may precede disruption, remediation demand, cloud workload changes or cybersecurity expenditure.",
                    limitations="Unfiltered voluntary firewall reports can include benign scanning and false positives; counts are not confirmed attacks, victims, attribution or a blocklist. Sensor participation and coverage change over time. Port classes are a research grouping of common service assignments; traffic need not use the usual protocol.",
                    raw_sha256=[digest, doc_hash], dataset_identity=f"dshield:port:{port}:{field}",
                    expected_publication_lag="Daily aggregation after sensor reports arrive; historical exact publication times are unavailable and backfills are possible.",
                    source_field=field, service_port=port, port_group=category,
                    missing_policy="Absent source dates omitted; no zero padding.",
                    aggregation="Native provider daily count retained unchanged.")
            attempt(result, f"{port}:{field}", one)
        return result
    with ThreadPoolExecutor(max_workers=min(3, workers)) as pool:
        futures = {pool.submit(port_one, category, port): port
                   for category, ports in SANS_PORTS.items() for port in ports}
        for future in as_completed(futures):
            try:
                good.extend(future.result())
            except Exception as exc:
                errors.append({"port": futures[future], "error": str(exc)})
    return good, errors


def nvd_metadata(obj):
    """Extract affected products, not non-vulnerable environment prerequisites."""
    result = {}
    for row in obj.get("vulnerabilities", []):
        cve = row["cve"]
        if cve.get("vulnStatus") == "Rejected":
            continue
        products = set()
        for config in cve.get("configurations", []):
            stack = list(config.get("nodes", []))
            while stack:
                node = stack.pop()
                stack.extend(node.get("children", []))
                for match in node.get("cpeMatch", []):
                    if not match.get("vulnerable"):
                        continue
                    parts = re.split(r"(?<!\\):", match["criteria"])
                    if len(parts) >= 5:
                        products.add((parts[2], parts[3], parts[4]))
        if not products:
            continue
        products = sorted(products)
        metrics = cve.get("metrics", {})
        score = next(iter(metrics.get("cvssMetricV31") or metrics.get("cvssMetricV30")
                          or metrics.get("cvssMetricV40") or metrics.get("cvssMetricV2") or [{}]), {})
        cvss = score.get("cvssData", {})
        vector = cvss.get("attackVector") or cvss.get("accessVector") or "UNSPECIFIED"
        weaknesses = sorted({v["value"] for w in cve.get("weaknesses", [])
                             for v in w.get("description", []) if v.get("lang") == "en"})
        descriptions = [d["value"] for d in cve.get("descriptions", []) if d.get("lang") == "en"]
        result[cve["id"]] = {
            "products": [list(p) for p in products[:30]],
            "affected_product_count": len(products),
            "vendor": products[0][1], "product": products[0][2], "part": products[0][0],
            "attack_vector": vector, "weaknesses": weaknesses,
            "description": descriptions[0] if descriptions else cve["id"],
            "published": cve.get("published"), "last_modified": cve.get("lastModified"),
        }
    return result


def parse_epss(raw, day, selected_ids):
    payload = gzip.decompress(raw)
    header = payload.splitlines()[0].decode()
    match = re.fullmatch(r"#model_version:([^,]+),score_date:(.+)", header)
    if not match:
        raise ValueError("Unexpected EPSS model/version header")
    model, published = match.groups()
    if pd.Timestamp(published).strftime("%Y-%m-%d") != day:
        raise ValueError("EPSS archive date mismatch")
    data = pd.read_csv(io.BytesIO(payload), comment="#", usecols=["cve", "epss"])
    if data.cve.duplicated().any() or data.epss.isna().any() or not data.epss.between(0, 1).all():
        raise ValueError("Invalid EPSS source population")
    data = data[data.cve.isin(selected_ids)].copy()
    data["date"] = pd.Timestamp(day, tz="UTC")
    data.rename(columns={"epss": "value"}, inplace=True)
    return data, {"model_version": model, "published_at": published}


def collect_epss(fetcher, start, end, workers):
    good, errors = [], []
    access_path = ROOT / "archive/knowledge5_selection/epss_access.json"
    access_limit = json.loads(access_path.read_text()) if access_path.exists() else {}
    requested_end = end
    if access_limit:
        end = min(end, access_limit["history_end_exclusive"])
        errors.append({"source_access": access_limit, "requested_end_exclusive": requested_end,
                       "retained_end_exclusive": end})
    pace = Pace(0.7)
    _, doc_hash = fetcher.get("https://www.first.org/epss/data.html", max_age_hours=720)
    _, faq_hash = fetcher.get("https://www.first.org/epss/faq", max_age_hours=720)
    def discover():
        latest_day = (pd.Timestamp(end) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        latest_raw, latest_hash = fetcher.get(
            f"https://epss.empiricalsecurity.com/epss_scores-{latest_day}.csv.gz", max_age_hours=720)
        scores = pd.read_csv(io.BytesIO(gzip.decompress(latest_raw)), comment="#")
        score_map = scores.set_index("cve").epss.to_dict()
        metadata, hashes = {}, [latest_hash]
        for year in range(2020, 2027):
            pace.wait()
            url = f"https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-{year}.json.gz"
            raw, digest = fetcher.get(url, max_age_hours=720, timeout=240)
            metadata.update(nvd_metadata(json.loads(gzip.decompress(raw))))
            hashes.append(digest)
            print(f"EPSS product metadata year {year}: {len(metadata)} CVEs", flush=True)
        buckets = defaultdict(list)
        cutoff = pd.Timestamp(end) - pd.Timedelta(days=100)
        for cve, info in metadata.items():
            if cve not in score_map or pd.Timestamp(info["published"]) > cutoff:
                continue
            key = (info["vendor"], info["part"], info["attack_vector"])
            buckets[key].append((score_map[cve], cve))
        queues = {k: deque(sorted(v, key=lambda x: (-x[0], x[1]))) for k, v in buckets.items()}
        chosen = []
        while queues and len(chosen) < 6000:
            # Deterministic hash order prevents a cap from favoring alphabetically
            # early vendors when there are more vendor/vector buckets than slots.
            for key in sorted(list(queues), key=lambda k: hashlib.sha256(json.dumps(k).encode()).hexdigest()):
                _, cve = queues[key].popleft()
                chosen.append(cve)
                if not queues[key]:
                    del queues[key]
                if len(chosen) >= 6000:
                    break
        return {"items": {cve: metadata[cve] for cve in chosen}, "raw_sha256": hashes,
                "policy": "6,000 CVEs published at least 100 days before selection; round-robin by vulnerable NVD vendor, product part and CVSS attack vector in deterministic hash order, descending selection-day EPSS within bucket. Current product metadata and selection are retrospective, not an as-of investment universe."}
    selected = selection("epss", discover)
    items = selected["items"]
    ids = set(items)
    paths, hashes, day_metadata = [], list(selected["raw_sha256"]) + [doc_hash, faq_hash], {}
    cache = ROOT / "archive/knowledge5_epss_days"
    cache.mkdir(parents=True, exist_ok=True)
    selection_hash = hashlib.sha256(json.dumps(sorted(ids)).encode()).hexdigest()[:16]
    def day_one(day):
        date = day.strftime("%Y-%m-%d")
        receipt = cache / (date + "_" + selection_hash + ".json")
        target = receipt.with_suffix(".parquet")
        if not fetcher.refresh and receipt.exists() and target.exists():
            meta = json.loads(receipt.read_text())
            return target, meta["raw_sha256"], date, meta["source_header"]
        check_access(fetcher)
        pace.wait()
        raw, digest = fetcher.get(f"https://epss.empiricalsecurity.com/epss_scores-{date}.csv.gz",
                                  max_age_hours=8760, timeout=180)
        data, header = parse_epss(raw, date, ids)
        data.to_parquet(target, index=False)
        atomic_bytes(receipt, json.dumps({"raw_sha256": digest, "source_header": header}).encode())
        return target, digest, date, header
    days = pd.date_range(max(start, "2024-01-01"), pd.Timestamp(end) - pd.Timedelta(days=1))
    with ThreadPoolExecutor(max_workers=min(3, workers)) as pool:
        futures = {pool.submit(day_one, d): d for d in days}
        for future in as_completed(futures):
            try:
                p, h, d, meta = future.result()
                paths.append(p); hashes.append(h); day_metadata[d] = meta
                if len(paths) % 50 == 0:
                    print(f"EPSS complete days {len(paths)}/{len(days)}", flush=True)
            except Exception as exc:
                errors.append({"day": str(futures[future].date()), "error": str(exc)})
    atomic_bytes(ROOT / "logs/knowledge5_epss_daily_publication_headers.json",
                 json.dumps(day_metadata, sort_keys=True, indent=2).encode())
    manifest = archive_raw_manifest(hashes, description="FIRST/Empirical Security original daily EPSS CSV forecasts, NVD affected-product metadata and FIRST methodology documentation.")
    data = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    model_spans = {}
    for day, header in sorted(day_metadata.items()):
        model = header["model_version"]
        model_spans.setdefault(model, {"start": day, "end": day})
        model_spans[model]["end"] = day
    def stage_one(cve, frame):
        info = items[cve]
        part = {"a": "applications", "o": "operating_systems", "h": "hardware"}.get(info["part"], "other")
        return save("epss_" + slug(cve) + "_exploitation_probability",
                clean(frame.date, frame.value),
                title=f"{cve}: {info['vendor']} {info['product']} daily EPSS exploitation probability",
                provider="FIRST EPSS vulnerability risk forecasts", category="cyber_exploitation_risk_" + part,
                unit="probability of exploitation in the next 30 days", frequency="daily",
                source_url="https://www.first.org/epss/data.html", geography="Worldwide affected software/hardware deployments",
                hypothesis="Rising forecast exploitation risk in widely deployed software may precede patching demand, vendor incidents, security spending or operational disruption; this hypothesis requires validation against actual exposures.",
                limitations="EPSS is a model forecast, not observed exploitation, incident count, loss or severity. Model version changes shift score distributions independently of vulnerability behavior; do not interpret jumps across the recorded model boundaries as new attacks. NVD product associations and this selection use the latest metadata and can revise. CVEs are not independent risk factors and forecast histories do not prove trading predictability. This acquisition retained only the contiguous 2024-01-01 through 2024-11-30 cached window after the next historical archive returned HTTP 403; these are stale historical research observations, not current risk readings.",
                raw_sha256=[manifest], raw_manifest_sha256=[manifest], dataset_identity=f"first:epss:{cve}:probability",
                timestamp_semantics="UTC date of the source CSV score_date label. The normalized midnight timestamp is an observation-day label, not a public release timestamp; a same-day midnight trading decision would introduce look-ahead.",
                expected_publication_lag="Published once daily. Current FIRST FAQ describes typical publication shortly after 13:30 UTC; historical exact availability is unverified. Each original CSV score_date timestamp is archived in provenance and logs/knowledge5_epss_daily_publication_headers.json and is not independently verified as first public availability.",
                model_version_periods=model_spans, cve_id=cve, vulnerable_vendor=info["vendor"],
                vulnerable_product=info["product"], affected_products=info["products"],
                product_label_selection="Lexicographically first vulnerable CPE product supplies title, vendor/product label and category bucket; it is not a provider-designated primary product or a measure of deployment exposure. Other affected products are retained subject to the disclosed list limit.",
                affected_product_count=info["affected_product_count"], affected_product_list_truncated=info["affected_product_count"] > 30,
                source_attack_vector=info["attack_vector"], source_weaknesses=info["weaknesses"],
                vulnerability_description=info["description"], cve_first_published=info["published"],
                missing_policy="Dates before a CVE receives a score and absent file/CVE observations remain missing; percentile omitted to avoid counting a rank transformation as another signal.",
                source_access_limitation=access_limit,
                selection_policy=selected["policy"], unavailable_source_days=[e["day"] for e in errors if "day" in e])
    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = {}
        def drain(completed):
            for future in completed:
                cve = pending.pop(future)
                try:
                    good.append(future.result())
                except Omission as exc:
                    OMISSIONS.append({"candidate": cve, "reason": str(exc)})
        for cve, frame in data.groupby("cve", sort=True):
            pending[pool.submit(stage_one, cve, frame)] = cve
            if len(pending) >= 32:
                drain(wait(pending, return_when=FIRST_COMPLETED)[0])
        drain(list(pending))
    return good, errors


COLLECTORS = {"flathub": collect_flathub, "osrs": collect_osrs, "sans": collect_sans,
              "epss": collect_epss}


def collect(fetcher=None, start="2024-01-01", end=None, workers=3, providers=None):
    fetcher = fetcher or Fetcher()
    end = end or pd.Timestamp.now(tz="UTC").normalize().strftime("%Y-%m-%d")
    good, errors = [], []
    for name in providers or COLLECTORS:
        try:
            rows, bad = COLLECTORS[name](ProviderAccess(fetcher), start, end, workers)
            good.extend(rows); errors.extend([{"provider": name, **e} for e in bad])
        except Exception as exc:
            errors.append({"provider": name, "error": repr(exc)})
            print(f"ERROR {name}: {exc!r}", flush=True)
    return good, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=list(COLLECTORS), action="append")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    rows, errors = collect(Fetcher(refresh=args.refresh), args.start, args.end, args.workers, args.provider)
    report = {"series_count": len(rows), "observations": sum(x["rows"] for x in rows),
              "provider_counts": pd.Series([x["provider"] for x in rows]).value_counts().to_dict(),
              "errors": errors, "omissions": OMISSIONS,
              "finished_at": pd.Timestamp.now(tz="UTC").isoformat()}
    suffix = "_".join(args.provider or ["all"])
    atomic_bytes(ROOT / "logs" / ("knowledge5_" + suffix + ".json"), json.dumps(report, indent=2).encode())
    print(json.dumps({k: v for k, v in report.items() if k != "omissions"}, indent=2), flush=True)
    raise SystemExit(bool(errors))
