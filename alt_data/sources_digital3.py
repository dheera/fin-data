#!/usr/bin/env python3
"""Further daily software, internet, blockchain and safety histories.

Discovery universes are frozen before acquisition. Every accepted series retains
the exact observations and raw responses; no resampling or missing-day filling.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
from functools import lru_cache
from urllib.parse import quote

import pandas as pd

from expansion_common import ROOT, Fetcher, atomic_bytes, save_candidate, normalize_frame, coverage, qualifies_daily
from sources_digital import slug, parse_npm, parse_wikimedia, year_windows, TOR_COUNTRIES
from sources_digital2 import get, parse_ripe, parse_fda

GROUP = "digital3"
OMISSIONS = []
# Historical GeoIP leftovers and supra-national/ambiguous codes are not counted
# as additional country histories. Kosovo (XK) and ordinary territories remain.
EXCLUDED_GEO_CODES = {"AN", "AP", "BP", "CS", "EU", "UK"}


class Omission(ValueError):
    """An identified candidate is real but does not meet the research gate."""


@lru_cache(maxsize=1)
def baseline():
    """Pinned exclusions permit later refreshes after these series are published."""
    path = ROOT / "archive/digital3_selection/baseline.json"
    if not path.exists():
        cat = pd.read_parquet(ROOT / "catalog.parquet")
        atomic_bytes(path, json.dumps({
            "series_ids": cat.series_id.tolist(),
            "identities": cat.dataset_identity.dropna().tolist(),
        }, indent=2).encode())
    return json.loads(path.read_text())


def new_identity(identity, sid=None):
    old = baseline()
    return identity not in set(old["identities"]) and (sid is None or sid not in set(old["series_ids"]))


def selection(name, discover):
    path = ROOT / "archive/digital3_selection" / (name + ".json")
    if path.exists():
        return json.loads(path.read_text())
    result = discover()
    result["selected_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(path, json.dumps(result, indent=2, ensure_ascii=False).encode())
    return result


def save(sid, frame, **meta):
    if frame.empty:
        raise Omission("No numeric observations returned")
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2:
        raise Omission("Constant observations")
    if not qualifies_daily({"frequency": meta["frequency"], **coverage(frame)}):
        raise Omission("Insufficient genuine daily coverage: " + json.dumps(coverage(frame)))
    meta.setdefault("source_release_frequency", "daily")
    meta.setdefault("native_frequency", "daily")
    # False records this collector's documented coverage, not proof that the
    # provider has no finer data product.
    meta.setdefault("finer_frequency_available", False)
    if not meta["finer_frequency_available"]:
        meta.setdefault("finer_frequency_note", "No finer feed documented by this collector; the provider may offer other frequencies.")
    meta.setdefault("timestamp_semantics", "Source UTC calendar date, not the historical public availability timestamp.")
    return save_candidate(GROUP, sid, frame, **meta)


def trim_download_history(frame):
    """Zero counts before a package's first use do not establish its history."""
    frame = normalize_frame(frame)
    if frame.value.lt(0).any():
        raise ValueError("Negative source download count")
    positive = frame.loc[frame.value.gt(0), "date"]
    if positive.dt.floor("D").nunique() < 90 or (positive.max() - positive.min()).days < 90:
        raise Omission("Fewer than 90 active download days or 90 days of positive-download history")
    return frame.loc[frame.date.ge(positive.min())].reset_index(drop=True)


def jobs(provider, specs, one, workers=3):
    good, bad = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(one, spec): spec for spec in specs}
        for future in as_completed(pending):
            spec = pending[future]
            try:
                result = future.result()
                if result is None:
                    continue
                result = result if isinstance(result, list) else [result]
                good.extend(result)
                for meta in result:
                    print(f"OK {provider} {meta['series_id']} {meta['rows']}", flush=True)
            except Omission as exc:
                OMISSIONS.append({"provider": provider, "candidate": str(spec)[:300], "reason": str(exc)})
            except Exception as exc:
                error = {"provider": provider, "candidate": str(spec)[:300], "error": str(exc)}
                bad.append(error)
                print("FAIL " + json.dumps(error), flush=True)
    return good, bad


CRAN_VIEWS = [
    "Agriculture", "ActuarialScience", "ChemPhys", "ClinicalTrials", "Econometrics",
    "Environmetrics", "Epidemiology", "Finance", "Hydrology", "MedicalImaging",
    "Omics", "Pharmacokinetics", "Spatial", "SpatioTemporal", "TimeSeries",
    "HighPerformanceComputing", "MachineLearning", "NaturalLanguageProcessing",
    "Optimization", "OfficialStatistics", "NetworkAnalysis", "Survival",
    "CausalInference", "ExperimentalDesign", "ExtremeValue", "ModelDeployment",
    "Databases", "ReproducibleResearch", "Tracking", "WebTechnologies",
]


def parse_cran_packages(raw):
    text = raw.decode() if isinstance(raw, bytes) else raw
    # Current task views use both canonical and relative CRAN package links.
    names = re.findall(r'(?:\.\./packages/|https?://CRAN\.R-project\.org/package=)([A-Za-z][A-Za-z0-9.]*)', text, re.I)
    return list(dict.fromkeys(names))


def collect_cran(fetcher, start, end, workers):
    def discover():
        queues, hashes, urls = {}, [], []
        for view in CRAN_VIEWS:
            url = f"https://cran.r-project.org/web/views/{view}.html"
            raw, digest = get(fetcher, url, interval=0.6)
            queues[view] = parse_cran_packages(raw)
            hashes.append(digest); urls.append(url)
        chosen, seen = [], set()
        # Round robin across applied fields, preserving expert task-view ordering.
        for i in range(max(map(len, queues.values()))):
            for view, packages in queues.items():
                if i >= len(packages):
                    continue
                package = packages[i]
                if package in seen or not new_identity(f"cranlogs/daily/{package}", "cran_" + slug(package) + "_downloads"):
                    continue
                seen.add(package); chosen.append([package, view])
                if len(chosen) == 550:
                    return {"specs": chosen, "raw_sha256": hashes, "source_urls": urls}
        return {"specs": chosen, "raw_sha256": hashes, "source_urls": urls}
    sel = selection("cran", discover)
    good, bad = [], []
    for offset in range(0, len(sel["specs"]), 20):
        batch = sel["specs"][offset:offset + 20]
        url = f"https://cranlogs.r-pkg.org/downloads/daily/{start}:{end}/" + ",".join(p for p, _ in batch)
        try:
            raw, digest = get(fetcher, url, interval=1.2)
            objects = {item["package"]: item for item in json.loads(raw)}
        except Exception as exc:
            bad.append({"provider": "cran", "batch": offset, "error": str(exc)})
            continue
        def one(spec):
            package, view = spec
            if package not in objects:
                raise Omission("Package absent from source response")
            frame = pd.DataFrame(objects[package]["downloads"]).rename(columns={"day": "date", "downloads": "value"})[["date", "value"]]
            return save("cran_" + slug(package) + "_downloads", trim_download_history(frame),
                title=f"CRAN RStudio mirror downloads: {package}", provider="CRAN R-hub",
                category="scientific_software_adoption", unit="package downloads", frequency="daily",
                source_url=url, geography="Global users of RStudio/cloud CRAN mirror",
                hypothesis=f"Installation activity for {package}, selected from the {view} CRAN task view, may proxy domain research, analytics deployment and associated compute and commercial tooling demand.",
                limitations="One CRAN mirror only. Downloads include CI, dependencies, repeated installs, education and bots, not unique users or revenue. Present-day task-view package selection has selection bias. Historical logs can be revised; absent source dates remain absent.",
                raw_sha256=[digest, *sel["raw_sha256"]], dataset_identity=f"cranlogs/daily/{package}",
                documentation_url="https://github.com/r-hub/cranlogs.app", package=package,
                market_theme=view, selection_rule="Round robin over packages listed by expert-maintained CRAN applied-domain task views; existing public identities excluded.",
                selection_source_urls=sel["source_urls"])
        g, b = jobs("cran", batch, one, 1); good.extend(g); bad.extend(b)
    return good, bad


NPM_THEMES = [
    "finance", "payments", "accounting", "insurance", "healthcare", "medical",
    "bioinformatics", "genomics", "robotics", "industrial", "manufacturing",
    "agriculture", "energy", "solar", "weather", "gis", "geospatial", "satellite",
    "telemetry", "iot", "mqtt", "cybersecurity", "authentication", "observability",
    "database", "data-warehouse", "machine-learning", "computer-vision", "speech",
    "ecommerce", "logistics", "supply-chain", "video", "gaming", "blockchain",
    "accessibility", "education", "scientific", "simulation", "optimization",
]


def collect_npm(fetcher, start, end, workers):
    def discover():
        queues, hashes, urls = {}, [], []
        for theme in NPM_THEMES:
            url = "https://registry.npmjs.org/-/v1/search"
            params = {"text": "keywords:" + theme, "size": 80}
            raw, digest = get(fetcher, url, params, interval=0.7)
            objects = json.loads(raw)["objects"]
            # Bulk history endpoint supports unscoped packages; choose established
            # packages with source-reported monthly installation activity.
            queues[theme] = [o["package"]["name"] for o in objects
                             if not o["package"]["name"].startswith("@")
                             and o.get("downloads", {}).get("monthly", 0) >= 1000]
            hashes.append(digest); urls.append(request_url(url, params))
        chosen, seen = [], set()
        for i in range(max(map(len, queues.values()))):
            for theme, packages in queues.items():
                if i >= len(packages):
                    continue
                package = packages[i]
                if package in seen or not new_identity(f"npm/downloads/{package}", "npm_" + slug(package) + "_downloads"):
                    continue
                seen.add(package); chosen.append([package, theme])
                if len(chosen) == 550:
                    return {"specs": chosen, "raw_sha256": hashes, "source_urls": urls}
        return {"specs": chosen, "raw_sha256": hashes, "source_urls": urls}
    sel = selection("npm", discover)
    good, bad = [], []
    package_info = {}
    for digest in sel["raw_sha256"]:
        obj = json.loads((fetcher.root / "archive/raw" / (digest + ".bin")).read_bytes())
        for row in obj.get("objects", []):
            package_info[row["package"]["name"]] = row["package"]
    for offset in range(0, len(sel["specs"]), 60):
        batch = sel["specs"][offset:offset + 60]
        frames = defaultdict(list); hashes = []; urls = []
        try:
            for begin, stop in year_windows(start, end):
                url = f"https://api.npmjs.org/downloads/range/{begin}:{stop}/" + ",".join(p for p, _ in batch)
                raw, digest = get(fetcher, url, interval=1.3)
                obj = json.loads(raw); hashes.append(digest); urls.append(url)
                for package, _ in batch:
                    if obj.get(package):
                        frames[package].append(parse_npm(json.dumps(obj[package])))
        except Exception as exc:
            bad.append({"provider": "npm", "batch": offset, "error": str(exc)})
            continue
        def one(spec):
            package, theme = spec
            if not frames[package]:
                raise Omission("No package history")
            info = package_info.get(package, {})
            if len(info.get("keywords", [])) > 50:
                raise Omission("Excessive unrelated publisher keywords make domain selection unreliable")
            return save("npm_" + slug(package) + "_downloads", trim_download_history(pd.concat(frames[package], ignore_index=True)),
                title=f"npm daily package downloads: {package}", provider="npm", category="software_adoption",
                unit="package downloads", frequency="daily", source_url=urls[0], geography="Global",
                hypothesis=f"Downloads of {package}, classified under {theme} in the npm registry, may proxy software deployment and related industry adoption, developer activity and infrastructure demand.",
                limitations="Downloads include dependencies, automated builds, retries and bots. Neither unique users nor revenue. Registry keywords are publisher supplied and can be noisy. Selection based on current search ranking and usage is not point-in-time; strong weekday seasonality. No missing-date filling.",
                raw_sha256=[*hashes, *sel["raw_sha256"]], dataset_identity=f"npm/downloads/{package}",
                documentation_url="https://github.com/npm/registry/blob/main/docs/download-counts.md",
                request_urls=urls, package=package, market_theme=theme,
                package_description=info.get("description"), package_keywords=info.get("keywords"),
                selection_rule="Round robin across applied-domain registry keyword searches; unscoped packages with >=1,000 source-reported monthly downloads; existing identities excluded.")
        g, b = jobs("npm", batch, one, 1); good.extend(g); bad.extend(b)
    return good, bad


def request_url(url, params):
    import requests
    return requests.Request("GET", url, params=params).prepare().url


def country_name(code, fallback=None):
    """Friendly display labels only; never alters provider geography codes."""
    try:
        from babel import Locale
        return Locale.parse("en").territories.get(code.upper(), fallback or code.upper())
    except ImportError:
        return fallback or code.upper()


def countries(fetcher, start, end):
    """Use the provider's observed geography universe; keep its discovery bytes."""
    def discover():
        url = "https://metrics.torproject.org/userstats-relay-country.csv"
        raw, digest = get(fetcher, url, {"start": start, "end": end}, interval=1.2, timeout=180)
        frame = pd.read_csv(io.BytesIO(raw), comment="#")
        codes = sorted(c for c in frame.country.dropna().unique() if re.fullmatch("[a-z]{2}", c))
        try:
            import pycountry
            specs = [[c.upper(), pycountry.countries.get(alpha_2=c.upper()).name]
                     for c in codes if pycountry.countries.get(alpha_2=c.upper())]
        except ImportError:
            specs = [[c.upper(), c.upper()] for c in codes]
        return {"specs": specs, "raw_sha256": [digest], "source_url": url}
    result = selection("countries", discover)
    result["specs"] = [[code, country_name(code, name)] for code, name in result["specs"]]
    return result


def collect_ripe(fetcher, start, end, workers):
    def discover():
        country_sel = countries(fetcher, start, end)
        specs = [[code, country] for code, country in country_sel["specs"]
                 if any(new_identity(f"RIPE/country-routing-stats/{code}/{m}/1d") for m in ("v4_prefixes_ris", "v6_prefixes_ris", "asns_ris"))]
        return {**country_sel, "specs": specs}
    sel = selection("ripe", discover)
    def one(spec):
        code, country = spec
        if code in EXCLUDED_GEO_CODES:
            raise Omission("Ambiguous, legacy or aggregate source geography code")
        country = country_name(code, country)
        url = "https://stat.ripe.net/data/country-routing-stats/data.json"
        params = {"resource": code, "starttime": start, "endtime": end, "resolution": "1d"}
        raw, digest = get(fetcher, url, params, interval=1.2, timeout=120)
        out = []
        for metric, title, unit in [
                ("v4_prefixes_ris", "announced IPv4 prefixes", "prefixes"),
                ("v6_prefixes_ris", "announced IPv6 prefixes", "prefixes"),
                ("asns_ris", "visible autonomous systems", "autonomous systems")]:
            identity = f"RIPE/country-routing-stats/{code}/{metric}/1d"
            if not new_identity(identity):
                continue
            try:
                out.append(save("ripe_" + code.lower() + "_" + metric, parse_ripe(raw, metric),
                    title=f"RIPE RIS {title}: {country}", provider="RIPE NCC RIS", category="internet_routing",
                    unit=unit, frequency="daily", source_url=url, geography=country,
                    hypothesis="Changes in country routing visibility can monitor internet disruption, network investment and digitization relevant to telecom, cloud and country-specific exposures.",
                    limitations="RIS collector and registration geography coverage, transfers and methodology affect levels. Not bandwidth, users or traffic. Daily aggregates can mask intraday disruption; retained source interval endpoints only, with no expansion of compressed intervals.",
                    raw_sha256=digest, dataset_identity=identity, source_parameters=params, source_geography_code=code,
                    documentation_url="https://stat.ripe.net/docs/data-api/", source_resolution="1d",
                    native_frequency="BGP observations aggregated daily by provider", finer_frequency_available=True,
                    aggregation="Only explicit source start/end interval timestamps; intermediate dates are not generated."))
            except Omission as exc:
                OMISSIONS.append({"provider": "ripe", "candidate": identity, "reason": str(exc)})
        return out
    return jobs("ripe", sel["specs"], one, workers)


def tor_identity(kind, code):
    # Retain the differing identity formats used by the earlier collection.
    if kind == "bridge":
        return f"Tor/userstats-bridge-country/{code}/users"
    return f"Tor/userstats-relay-country/{{'country': '{code}'}}/users/{{}}"


def collect_tor(fetcher, start, end, workers):
    country_map = dict(countries(fetcher, start, end)["specs"])
    good, bad = [], []
    for kind in ("relay", "bridge"):
        url = f"https://metrics.torproject.org/userstats-{kind}-country.csv"
        params = {"start": start, "end": end}
        raw, digest = get(fetcher, url, params, interval=1.2, timeout=180)
        frame = pd.read_csv(io.BytesIO(raw), comment="#")
        def discover():
            specs = []
            for code, part in frame.groupby("country"):
                if not re.fullmatch("[a-z]{2}", str(code)) or code.upper() not in country_map:
                    continue
                sid = f"tor_{kind}_users_{code}"
                if new_identity(tor_identity(kind, code), sid):
                    specs.append(code)
            return {"specs": sorted(specs), "raw_sha256": [digest], "source_url": url}
        sel = selection("tor_" + kind, discover)
        def one(code):
            if code.upper() in EXCLUDED_GEO_CODES:
                raise Omission("Ambiguous, legacy or aggregate source geography code")
            data = frame.loc[frame.country.eq(code), ["date", "users"]].rename(columns={"users": "value"}).dropna()
            return save(f"tor_{kind}_users_{code}", data,
                title=f"Estimated Tor {kind} users: {country_map[code.upper()]}",
                provider="Tor Project", category="internet_resilience", unit="estimated users", frequency="daily",
                source_url=url, geography=country_map[code.upper()],
                hypothesis="Country-level privacy-network usage can flag connectivity disruption, censorship or adoption changes relevant to country risk, telecommunications and security services.",
                limitations="Statistical users estimate, not a census. Geolocation, reporting coverage, bots and methodology cause breaks; privacy use is not evidence of illicit or political activity. Small-country counts can be noisy. No interpolation of missing dates.",
                raw_sha256=digest, dataset_identity=tor_identity(kind, code), source_parameters=params, source_geography_code=code.upper(),
                documentation_url="https://metrics.torproject.org/stats.html",
                selection_rule="Additional source country/territory codes with sufficient nonconstant daily history, excluding existing relay/bridge user identities.")
        g, b = jobs("tor", sel["specs"], one, 2); good.extend(g); bad.extend(b)
    return good, bad


COIN_METRICS = {
    "AdrActCnt": ("active addresses", "addresses"),
    "AdrBalCnt": ("addresses with nonzero balance", "addresses"),
    "TxCnt": ("transactions", "transactions"),
    "TxTfrCnt": ("transfer count", "transfers"),
    "FeeTotNtv": ("total transaction fees", "native units"),
    "SplyCur": ("current supply", "native units"),
    "BlkCnt": ("block count", "blocks"),
    "IssTotNtv": ("total issuance", "native units"),
    "FlowInExNtv": ("inflow to labeled exchanges", "native units"),
    "FlowOutExNtv": ("outflow from labeled exchanges", "native units"),
    "SplyExNtv": ("supply held on labeled exchanges", "native units"),
}
FLOW_ASSETS = {"flow", "flow_native", "flow_evm"}
COIN_QUALITY_URL = "https://gitbook-docs.coinmetrics.io/network-data/network-data-overview.md"


def coin_quality_metadata(asset, metric):
    if asset not in FLOW_ASSETS:
        return {}
    result = {
        "experimental": True,
        "provider_quality_notice_date": "2026-01-12",
        "provider_quality_notice": "Coin Metrics classifies the FLOW, FLOW native and FLOW EVM asset metrics as experimental because of node reliability issues.",
        "quality_documentation_url": COIN_QUALITY_URL,
        "asset_scope": {"flow": "Flow aggregate", "flow_native": "Flow native execution component",
                        "flow_evm": "Flow EVM execution component"}[asset],
    }
    if metric == "FeeTotNtv":
        result["aggregation_dependency"] = "FLOW aggregate fees combine the native and EVM execution components. The aggregate and component fee series are related and must not be summed together."
    return result


def coin_selection(obj, start, end, maximum=600):
    if obj.get("next_page_url"):
        raise ValueError("Coin Metrics catalog pagination is incomplete")
    queues = defaultdict(list)
    for asset in obj["data"]:
        for metric in asset["metrics"]:
            name = metric["metric"]
            if name not in COIN_METRICS or not new_identity(f"{asset['asset']}/{name}/1d"):
                continue
            eligible = [f for f in metric["frequencies"] if f["frequency"] == "1d" and f.get("community")
                        and pd.to_datetime(f["min_time"], utc=True) <= pd.Timestamp(end, tz="UTC") - pd.Timedelta(days=365)
                        and pd.to_datetime(f["max_time"], utc=True) >= pd.Timestamp(start, tz="UTC") + pd.Timedelta(days=365)]
            if eligible:
                queues[name].append([asset["asset"], name])
    chosen = []
    # Balance metric concepts; prefer full recent coverage then stable asset ID.
    for i in range(max(map(len, queues.values()), default=0)):
        for name in COIN_METRICS:
            if i < len(queues[name]):
                chosen.append(queues[name][i])
                if len(chosen) >= maximum:
                    return chosen
    return chosen


def parse_coin(raw, metric):
    frame = pd.read_csv(io.BytesIO(raw), usecols=["time", metric])
    return frame.rename(columns={"time": "date", metric: "value"}).dropna(subset=["value"])


def collect_coinmetrics(fetcher, start, end, workers):
    def discover():
        url = "https://community-api.coinmetrics.io/v4/catalog-all-v2/asset-metrics"
        raw, digest = get(fetcher, url, {"page_size": 10000}, interval=1.2, timeout=180)
        return {"specs": coin_selection(json.loads(raw), start, end), "raw_sha256": [digest], "source_url": url}
    sel = selection("coinmetrics", discover)
    quality_hashes = []
    if any(asset in FLOW_ASSETS for asset, _ in sel["specs"]):
        _, quality_digest = get(fetcher, COIN_QUALITY_URL, interval=1.2, timeout=120)
        quality_hashes.append(quality_digest)
    by_asset = defaultdict(list)
    for asset, metric in sel["specs"]:
        by_asset[asset].append(metric)
    def one(spec):
        asset, metrics = spec
        url = f"https://raw.githubusercontent.com/coinmetrics/data/master/csv/{quote(asset, safe='')}.csv"
        raw, digest = get(fetcher, url, interval=0.7, timeout=180)
        columns = pd.read_csv(io.BytesIO(raw), nrows=0).columns
        out = []
        for metric in metrics:
            if metric not in columns:
                raise ValueError(f"Selected community metric {metric} absent from CSV")
            frame = parse_coin(raw, metric)
            frame = frame[(frame.date >= start) & (frame.date <= end)]
            label, unit = COIN_METRICS[metric]
            try:
                out.append(save("coinmetrics_" + slug(asset) + "_" + metric.lower(), frame,
                    title=f"Coin Metrics {asset.upper()} {label}", provider="coin_metrics",
                    category="blockchain_activity", unit=f"{asset} native units" if unit == "native units" else unit,
                    frequency="daily", source_url=url, geography="Global blockchain network",
                    hypothesis=f"{label.capitalize()} for {asset.upper()} may monitor network adoption, transaction demand, token issuance and intermediary liquidity, with potential relevance to crypto infrastructure and digital-asset risk.",
                    limitations="Chain activity is not unique users, economic transactions or revenue. Bots, self-transfers, batching, migrations, forks and tagging changes affect metrics. Exchange labels can be revised with hindsight. Current asset coverage creates selection bias; discontinued assets may be stale. Native units are chain-specific and cannot be summed across assets. No missing-value interpolation." + (" Provider classifies FLOW histories as experimental due to node reliability; aggregate and component metrics are not independent." if asset in FLOW_ASSETS else ""),
                    raw_sha256=[digest, *sel["raw_sha256"], *(quality_hashes if asset in FLOW_ASSETS else [])], dataset_identity=f"{asset}/{metric}/1d",
                    documentation_url="https://docs.coinmetrics.io/network-data/network-data-overview",
                    metric_documentation_url=f"https://coverage.coinmetrics.io/asset-metrics-v2/{metric}",
                    asset=asset, source_column=metric, license="CC BY-NC 4.0",
                    source_release_frequency="daily archive from community API",
                    **coin_quality_metadata(asset, metric)))
            except Omission as exc:
                OMISSIONS.append({"provider": "coinmetrics", "candidate": f"{asset}/{metric}", "reason": str(exc)})
        return out
    return jobs("coinmetrics", list(by_asset.items()), one, workers)


FDA_GROUPS = [
    ("drug_ingredients", "drug/event", "receivedate", "patient.drug.openfda.substance_name.exact", 150, "drug_safety", "active ingredient"),
    ("drug_brands", "drug/event", "receivedate", "patient.drug.openfda.brand_name.exact", 150, "drug_safety", "drug brand"),
    ("device_productcodes", "device/event", "date_received", "device.device_report_product_code.exact", 150, "device_safety", "device product code"),
    ("drug_reactions", "drug/event", "receivedate", "patient.reaction.reactionmeddrapt.exact", 110, "drug_safety", "reported reaction"),
]


def parse_fda_classifications(raw, existing_identities):
    obj = json.loads(raw)
    if obj["meta"]["results"]["total"] > len(obj["results"]):
        raise ValueError("Device classification crosswalk query truncated")
    names = {}
    for row in obj["results"]:
        code, name = row["product_code"], row["device_name"]
        if code in names and names[code] != name:
            raise ValueError("Conflicting classification names for " + code)
        names[code] = name
    old = set()
    marker = "openFDA/device/event.json/device.openfda.device_name.exact="
    for identity in existing_identities:
        if identity.startswith(marker):
            old.add(slug(identity[len(marker):].rsplit("/count=", 1)[0]))
    excluded = sorted(code for code, name in names.items() if slug(name) in old)
    return names, excluded


def fda_device_crosswalk(fetcher, specs):
    def discover():
        codes = [s[6] for s in specs if s[0] == "device_productcodes"]
        url = "https://api.fda.gov/device/classification.json"
        params = {"search": "product_code:(" + " OR ".join('"' + c + '"' for c in codes) + ")", "limit": 1000}
        raw, digest = get(fetcher, url, params, interval=1.2, timeout=120)
        names, excluded = parse_fda_classifications(raw, baseline()["identities"])
        return {"names": names, "excluded_codes": excluded, "raw_sha256": [digest],
                "source_url": url, "source_parameters": params,
                "exclusion_rule": "Device product codes mapped to already-published harmonized device names represent the same classification and are excluded."}
    return selection("fda_device_crosswalk", discover)


def collect_fda(fetcher, start, end, workers):
    def discover():
        specs, hashes = [], []
        for label, endpoint, datefield, field, count, category, dimension in FDA_GROUPS:
            url = f"https://api.fda.gov/{endpoint}.json"
            params = {"count": field, "limit": count, "search": f'{datefield}:[{start.replace("-", "")} TO {end.replace("-", "")}]'}
            raw, digest = get(fetcher, url, params, interval=1.1, timeout=120)
            hashes.append(digest)
            for row in json.loads(raw)["results"]:
                term = row["term"]
                identity = f"openFDA/{endpoint}.json/{field}={term}/count={datefield}"
                if slug(term) in {"unknown", "other", "no_match"} or not new_identity(identity):
                    continue
                specs.append([label, endpoint, datefield, field, category, dimension, term, digest])
        return {"specs": specs, "raw_sha256": hashes}
    sel = selection("fda", discover)
    crosswalk = fda_device_crosswalk(fetcher, sel["specs"])
    def one(spec):
        label, endpoint, datefield, field, category, dimension, term, selection_hash = spec
        if label == "device_productcodes" and term in crosswalk["excluded_codes"]:
            raise Omission("Device code is an alias for an already-published harmonized device classification")
        url = f"https://api.fda.gov/{endpoint}.json"
        escaped = term.replace("\\", "\\\\").replace('"', '\\"')
        params = {"count": datefield, "search": f'{field}:"{escaped}" AND {datefield}:[{start.replace("-", "")} TO {end.replace("-", "")}]'}
        raw, digest = get(fetcher, url, params, interval=1.2, timeout=120)
        obj = json.loads(raw)
        display_term = term
        provenance = [digest, selection_hash]
        if label == "device_productcodes":
            if term not in crosswalk["names"]:
                raise Omission("Device product code absent from official classification crosswalk")
            display_term = term + ": " + crosswalk["names"][term]
            provenance.extend(crosswalk["raw_sha256"])
        return save("fda_" + label + "_" + slug(term), parse_fda(raw),
            title=f"FDA adverse-event reports by {dimension}: {display_term}", provider="US FDA openFDA",
            category=category, unit="adverse-event reports", frequency="daily", source_url=url,
            geography="Reports received by US FDA; may include foreign events",
            hypothesis="Unexpected changes in safety-report activity may identify research leads for product reliability, liability, recalls and regulatory scrutiny relevant to manufacturers, healthcare providers and insurers.",
            limitations="Reports do not establish causality, incidence or medical risk. Multiple ingredients, products and reactions overlap, so series are not additive. Reporting is affected by media, litigation and under-reporting. Latest database revisions and retrospective report dates can cause look-ahead bias. Publication is weekly for devices and quarterly for drugs, despite daily date bins. Source-returned bins only; missing dates are not filled.",
            raw_sha256=provenance,
            dataset_identity=f"openFDA/{endpoint}.json/{field}={term}/count={datefield}",
            documentation_url="https://open.fda.gov/apis/timeseries/", term=term, date_field=datefield,
            source_parameters=params, source_last_updated=obj["meta"].get("last_updated"),
            source_classification_name=crosswalk["names"].get(term) if label == "device_productcodes" else None,
            classification_documentation_url="https://open.fda.gov/apis/device/classification/" if label == "device_productcodes" else None,
            native_frequency="Report-level dates aggregated daily by the source",
            source_release_frequency="weekly" if category == "device_safety" else "quarterly",
            timestamp_semantics=f"Source {datefield} report calendar day, not incident time or first public availability.",
            selection_rule="Most frequently reported exact terms in controlled product/ingredient/reaction dimensions; existing identities excluded and selection frozen.")
    return jobs("fda", sel["specs"], one, workers)


WIKI_GROUPS = {
    "semiconductor equipment materials and compute": "ASML_Holding|Applied_Materials|Lam_Research|KLA_Corporation|Tokyo_Electron|Advantest|Teradyne|Synopsys|Cadence_Design_Systems|Arm_Holdings|Qualcomm|Broadcom|Marvell_Technology|MediaTek|SK_Hynix|Micron_Technology|Western_Digital|Seagate_Technology|Solid-state_drive|High_Bandwidth_Memory|Extreme_ultraviolet_lithography|Photolithography|Chiplet|Silicon_carbide|Gallium_nitride|Wafer_(electronics)|Electronic_design_automation|Application-specific_integrated_circuit|Field-programmable_gate_array|Photonic_integrated_circuit",
    "power equipment and electrification": "HVDC_converter_station|High-voltage_direct_current|Transformer|Switchgear|Circuit_breaker|Substation|Power_inverter|Uninterruptible_power_supply|Flywheel_energy_storage|Compressed-air_energy_storage|Sodium-ion_battery|Flow_battery|Vanadium_redox_battery|Lithium_iron_phosphate_battery|Vehicle-to-grid|Demand_response|Smart_grid|District_heating|Electrical_steel|Superconductivity|Fuel_cell|Electrolysis_of_water|Green_hydrogen|Offshore_wind_power|Wind_turbine|Combined_cycle_power_plant|Cogeneration|Gas_turbine|Steam_turbine|HVAC",
    "metals chemicals and industrial inputs": "Aluminium|Alumina|Magnesium|Manganese|Molybdenum|Tungsten|Cobalt|Chromium|Zinc|Lead|Palladium|Platinum|Rhodium|Iridium|Silver|Graphite|Vanadium|Niobium|Tantalum|Titanium|Zirconium|Ammonia|Urea|Nitric_acid|Sulfuric_acid|Caustic_soda|Chlorine|Ethylene|Propylene|Polyethylene|Polypropylene|Polyvinyl_chloride|Polycarbonate|Epoxy|Silicone|Carbon_fiber|Glass_fiber|Synthetic_rubber|Natural_rubber|Industrial_gas",
    "industrial construction and buildings": "Cement|Clinker_(cement)|Ready-mix_concrete|Rebar|Structural_steel|Lumber|Oriented_strand_board|Plywood|Gypsum|Drywall|Asphalt|Bitumen|Insulating_glass|Spray_foam|Mineral_wool|Roofing|Scaffolding|Excavator|Bulldozer|Tower_crane|Forklift|Backhoe_loader|Hydraulic_fracturing|Directional_drilling|Drilling_rig|Centrifugal_pump|Heat_exchanger|Industrial_valve|Ball_bearing|Air_compressor",
    "food agriculture and packaging": "Sugarcane|Sugar_beet|Wheat_rust|Soybean_rust|Locust|Fall_armyworm|Fusarium|Barley|Oat|Sorghum|Rapeseed|Canola|Sunflower_oil|Olive_oil|Orange_juice|Frozen_concentrated_orange_juice|Almond|Pistachio|Cashew|Vanilla|Black_pepper|Tea|Salmon|Shrimp|Aquaculture|Fish_meal|Milk_powder|Whey_protein|Egg_as_food|Broiler|Poultry_farming|Cattle_feed|Potato|Cassava|Sugar|Ethanol_fuel|Biodiesel|Corrugated_fiberboard|Containerboard|Aluminium_can",
    "transportation and supply chain equipment": "Intermodal_container|Refrigerated_container|LNG_carrier|Oil_tanker|Bulk_carrier|Roll-on/roll-off|Dry_bulk_cargo|Shipbuilding|Floating_production_storage_and_offloading|Crude_oil_washing|Rail_freight_transport|Diesel_locomotive|Electric_locomotive|Air_cargo|Aircraft_engine|Turbofan|Jet_fuel|Sustainable_aviation_fuel|Aircraft_leasing|Business_jet|General_aviation|Airport_ground_support_equipment|Truck_driver_shortage|Cold_chain|Last_mile_(transportation)|Freight_forwarder|Customs_broker|Bonded_warehouse|Shipping_container_architecture|Cruise_line",
    "healthcare tools procedures and products": "Continuous_glucose_monitor|Insulin_pump|Dialysis|Hemodialysis|Peritoneal_dialysis|Sleep_apnea|Continuous_positive_airway_pressure|Hearing_aid|Cochlear_implant|Dental_implant|Orthodontics|In_vitro_fertilisation|Robotic_surgery|Computed_tomography|Magnetic_resonance_imaging|Positron_emission_tomography|Ultrasonography|Liquid_biopsy|Next-generation_sequencing|Gene_therapy|CAR_T_cell|Bispecific_antibody|Antibody-drug_conjugate|Monoclonal_antibody|Immunotherapy|Biosimilar|Generic_drug|Radiopharmaceutical|Mammography|Medical_glove",
    "travel household and discretionary demand": "Glamping|Recreational_vehicle|Motorhome|Camping|Ski_resort|Theme_park|Water_park|Duty-free_shop|Airport_lounge|Private_jet|Luxury_good|Handbag|Luxury_watch|Lab-grown_meat|Meal_kit|Specialty_coffee|Energy_drink|Non-alcoholic_drink|Hard_seltzer|Whisky|Tequila|Cognac|Champagne|Wine|Pet_food|Dog_food|Cat_food|Veterinary_medicine|Electric_bicycle|Electric_motorcycle|Scooter_(motorcycle)|Motorcycle|Bicycle|Athletic_shoe|Running_shoe|Yoga|Pilates|Home_automation|Robot_vacuum_cleaner|Washing_machine",
    "enterprise infrastructure security and finance": "Cloud_computing|Edge_computing|Serverless_computing|Object_storage|Network-attached_storage|Storage_area_network|Optical_fiber|Optical_transceiver|Ethernet|Content_delivery_network|Database|Data_warehouse|Data_lake|Enterprise_resource_planning|Customer_relationship_management|Payroll|Human_resource_management_system|Point_of_sale|Payment_gateway|Merchant_account|Chargeback|Payment_card_industry|Identity_management|Zero_trust_security_model|Multi-factor_authentication|Endpoint_detection_and_response|Security_information_and_event_management|Distributed_denial-of-service|Phishing|Cyber_insurance|Factoring_(finance)|Invoice_discounting|Trade_finance|Letters_of_credit|Equipment_leasing|Securitization|Collateralized_loan_obligation|Municipal_bond|Credit_default_swap|Interest_rate_swap",
    "company operating platforms across sectors": "Siemens|Schneider_Electric|ABB|Eaton_Corporation|Rockwell_Automation|Emerson_Electric|Honeywell|Johnson_Controls|Trane_Technologies|Carrier_Global|Vertiv|Legrand|Prysmian_Group|Nexans|Vestas|GE_Vernova|First_Solar|Enphase_Energy|SolarEdge|CATL|BYD_Company|LG_Energy_Solution|Panasonic|Samsung_SDI|Fluence_Energy|Caterpillar_Inc.|Deere_%26_Company|Komatsu_Limited|CNH_Industrial|AGCO|Kubota|Sandvik|Epiroc|Atlas_Copco|Ingersoll_Rand|Parker_Hannifin|Danfoss|Grundfos|Xylem_Inc.|Pentair|Ecolab|Veolia|Suez_(company)|Waste_Management_(company)|Republic_Services|Clean_Harbors|Linde_plc|Air_Liquide|Air_Products|BASF|Dow_Inc.|DuPont|Celanese|Eastman_Chemical_Company|Huntsman_Corporation|Sherwin-Williams|PPG_Industries|AkzoNobel|Sika_AG|Saint-Gobain|Holcim_Group|Heidelberg_Materials|CRH_plc|Vulcan_Materials_Company|Martin_Marietta_Materials|Weyerhaeuser|International_Paper|Packaging_Corporation_of_America|Avery_Dennison|Ball_Corporation",
}


def parse_wiki_titles(raw):
    """Canonical main-namespace titles, excluding missing/non-article pages."""
    obj = json.loads(raw)
    if "error" in obj:
        raise ValueError(str(obj["error"]))
    return sorted({p["title"].replace(" ", "_") for p in obj["query"]["pages"].values()
                   if "missing" not in p and p.get("ns") == 0})


def balanced_wiki_specs(specs, per_theme=10):
    groups = defaultdict(list)
    for spec in specs:
        groups[spec[1]].append(spec)
    chosen = []
    for theme in WIKI_GROUPS:
        items = groups[theme]
        count = min(per_theme, len(items))
        if count <= 1:
            chosen.extend(items[:count])
        else:
            chosen.extend(items[round(i * (len(items) - 1) / (count - 1))] for i in range(count))
    return chosen


def collect_wikipedia(fetcher, start, end, workers):
    def discover():
        catalog = pd.read_parquet(ROOT / "catalog.parquet")
        old_articles = []
        for identity in baseline()["identities"]:
            if identity.startswith("pageviews/en.wikipedia.org/all-access/user/"):
                old_articles.append(identity.rsplit("/", 1)[-1])
        canonical_old = set()
        hashes = []
        url = "https://en.wikipedia.org/w/api.php"
        for offset in range(0, len(old_articles), 50):
            params = {"action": "query", "format": "json", "redirects": 1, "titles": "|".join(old_articles[offset:offset + 50])}
            raw, digest = get(fetcher, url, params, interval=1.2, timeout=120)
            canonical_old.update(parse_wiki_titles(raw)); hashes.append(digest)
        specs, seen = [], set(canonical_old)
        for theme, text in WIKI_GROUPS.items():
            topics = text.replace("%26", "&").split("|")
            for offset in range(0, len(topics), 50):
                params = {"action": "query", "format": "json", "redirects": 1, "titles": "|".join(topics[offset:offset + 50])}
                raw, digest = get(fetcher, url, params, interval=1.2, timeout=120)
                hashes.append(digest)
                for title in parse_wiki_titles(raw):
                    if title in seen:
                        continue
                    seen.add(title); specs.append([title, theme])
        return {"specs": balanced_wiki_specs(specs), "discovered_specs": specs, "raw_sha256": hashes,
                "selection_rule": "10 canonical articles evenly selected within each of 10 curated applied themes (100 maximum); redirects canonicalized and existing canonical English topics excluded."}
    sel = selection("wikipedia", discover)
    def one(spec):
        article, theme = spec
        url = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
               f'en.wikipedia.org/all-access/user/{quote(article, safe="")}/daily/'
               f'{start.replace("-", "")}00/{end.replace("-", "")}00')
        try:
            raw, digest = get(fetcher, url, interval=1.2, timeout=120)
        except Exception as exc:
            if getattr(getattr(exc, "response", None), "status_code", None) == 404:
                raise Omission("No pageview history for source-verified canonical article") from exc
            raise
        article_slug = slug(article) or hashlib.sha256(article.encode()).hexdigest()[:12]
        return save("wikipedia_en_" + article_slug + "_views", parse_wikimedia(raw),
            title="Wikipedia human page views: " + article.replace("_", " "),
            provider="Wikimedia", category="attention", unit="page views", frequency="daily",
            source_url=url, geography="Global readers of English Wikipedia",
            hypothesis=f"Research attention to {article.replace('_', ' ')} may monitor {theme}, including demand shifts, bottlenecks, product adoption and operating disruptions.",
            limitations="Attention is not sentiment, transactions or revenue. Education, news and entertainment affect readership; identified automated traffic only is excluded. Current topic selection creates hindsight bias. Redirect aliases are canonicalized during selection but their traffic is not summed into the target article. No missing-day interpolation.",
            raw_sha256=[digest, *sel["raw_sha256"]],
            dataset_identity=f"pageviews/en.wikipedia.org/all-access/user/{article}",
            documentation_url="https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html",
            article=article, language="en", market_theme=theme, selection_rule=sel["selection_rule"])
    return jobs("wikipedia", sel["specs"], one, workers)


PROVIDERS = ["tor", "cran", "npm", "ripe", "coinmetrics", "fda", "wikipedia"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS + ["all"], default="all")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default=(pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2)).date().isoformat())
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    baseline()
    fetcher = Fetcher(refresh=args.refresh)
    good, bad = [], []
    selected = PROVIDERS if args.provider == "all" else [args.provider]
    def run(provider):
        try:
            return provider, *globals()["collect_" + provider](fetcher, args.start, args.end, args.workers)
        except Exception as exc:
            return provider, [], [{"provider": provider, "error": str(exc)}]
    with ThreadPoolExecutor(max_workers=3) as pool:
        for provider, results, errors in pool.map(run, selected):
            good.extend(results); bad.extend(errors)
            report = {"created_at": pd.Timestamp.now(tz="UTC").isoformat(), "provider": provider,
                      "count": len(results), "successful_series": [x["series_id"] for x in results],
                      "errors": errors, "omissions": [x for x in OMISSIONS if x["provider"] == provider]}
            atomic_bytes(ROOT / "archive/digital3_reports" / (provider + ".json"), json.dumps(report, indent=2).encode())
            print("DONE " + json.dumps({"provider": provider, "count": len(results), "errors": len(errors)}), flush=True)
    report = {"created_at": pd.Timestamp.now(tz="UTC").isoformat(), "count": len(good),
              "successful_series": [x["series_id"] for x in good], "errors": bad, "omissions": OMISSIONS}
    atomic_bytes(ROOT / "archive/digital3_reports" / (args.provider + "_collection.json"), json.dumps(report, indent=2).encode())
    print(json.dumps({"count": len(good), "errors": len(bad), "omissions": len(OMISSIONS)}), flush=True)
    return int(bool(bad))


if __name__ == "__main__":
    raise SystemExit(main())
