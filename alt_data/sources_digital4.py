#!/usr/bin/env python3
"""Global internet attention, connectivity and crypto transaction histories.

Only source observations are retained. Discovery selections and raw response
bytes are archived. Prices, filled calendars and renamed history windows do not
count as new series.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import hashlib
import io
import json
import re
import threading
import time
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests

from expansion_common import ROOT, Fetcher, atomic_bytes, normalize_frame, coverage, qualifies_research, save_candidate, archive_raw_manifest
from sources_digital import parse_wikimedia
from sources_digital3 import WIKI_GROUPS

GROUP = "digital4"
START = "2024-01-01"
OMISSIONS = []
LOCK = threading.Lock()
SEEN = {}
PUBLICATION_LAGS = {
    "DeFiLlama protocol activity": "Adapter-specific publication delay; completed UTC daily periods generally require a subsequent adapter update. Historical availability timestamps are not supplied.",
    "Binance global spot trading": "Daily executions become complete after UTC bar close; incomplete current-day bars are excluded.",
    "Wikimedia multilingual attention": "Collected with a two-day buffer; historical per-observation release timestamps are not supplied.",
    "Google Transparency Report via IODA": "Underlying Google traffic typically approximately 2–3 hours behind; IODA ingestion can add delay.",
    "IODA global Internet connectivity": "Intraday source updates; ingestion outages can add delay. Historical release timestamps are not supplied.",
    "Alternative.me": "Published daily; source timestamps label index dates, with no historical release-time archive.",
    "Blockchain.com network telemetry": "Intraday source charts can lag current network conditions; historical publication times are not supplied.",
    "AMS-IX international Internet traffic": "Export reports completed weeks through its explicit end date; historical publication timestamps are not supplied.",
    "Tranco web popularity research": "Daily ranking publication combines the previous 30 days; exact list creation timestamps are archived.",
}


class Omission(ValueError):
    """A real candidate does not meet the inclusion gate."""


def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


@lru_cache(maxsize=1)
def baseline():
    path = ROOT / "archive/digital4_selection/baseline.json"
    if not path.exists():
        cat = pd.read_parquet(ROOT / "catalog.parquet")
        cat = cat[cat.batch.lt(6)]
        atomic_bytes(path, json.dumps({
            "series_ids": cat.series_id.tolist(),
            "identities": cat.dataset_identity.dropna().tolist(),
            "hashes": cat.content_sha256.dropna().tolist(),
        }, indent=2).encode())
    obj = json.loads(path.read_text())
    return {k: set(v) for k, v in obj.items()}


def selection(name, discover):
    path = ROOT / "archive/digital4_selection" / (name + ".json")
    if path.exists():
        return json.loads(path.read_text())
    obj = discover()
    obj["selected_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(path, json.dumps(obj, indent=2, ensure_ascii=False).encode())
    return obj


def request_url(url, params):
    return requests.Request("GET", url, params=params).prepare().url


def clean(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True), "value": pd.to_numeric(values, errors="raise")})
    frame = frame.dropna()
    if frame.empty:
        raise Omission("No numeric observations")
    if frame.groupby("date").value.nunique().gt(1).any():
        raise ValueError("Conflicting duplicate source timestamps")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    old = baseline()
    if sid in old["series_ids"] or meta["dataset_identity"] in old["identities"]:
        raise Omission("Identity already published before batch 6")
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2:
        raise Omission("Constant history")
    if not qualifies_research({"frequency": meta["frequency"], **coverage(frame)}):
        raise Omission("Insufficient observed coverage: " + json.dumps(coverage(frame)))
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes() + frame.value.to_numpy().tobytes()).hexdigest()
    with LOCK:
        if digest in old["hashes"] or (digest in SEEN and SEEN[digest] != sid):
            raise Omission("Duplicate complete numeric history")
        SEEN[digest] = sid
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("source_release_frequency", "daily")
    meta.setdefault("finer_frequency_available", False)
    meta.setdefault("timestamp_semantics", "UTC observation-period label, not the historical public availability timestamp.")
    meta.setdefault("geography_scope", "global")
    meta.setdefault("country_codes", [])
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2" +
                    ("; XK is the commonly used Kosovo extension" if "XK" in meta["country_codes"] else ""))
    meta.setdefault("expected_publication_lag", PUBLICATION_LAGS[meta["provider"]])
    result = save_candidate(GROUP, sid, frame, **meta)
    print(f"OK {meta['provider']} {sid} {len(frame)}", flush=True)
    return result


def attempt(good, provider, candidate, fn):
    try:
        result = fn()
        if result:
            good.append(result)
    except Omission as exc:
        OMISSIONS.append({"provider": provider, "candidate": str(candidate), "reason": str(exc)})


def parse_llama_breakdown(obj, start=START, end=None):
    """Missing protocol keys are missing, not zero; zero values remain observed."""
    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC") if end else pd.Timestamp.now(tz="UTC").normalize()
    rows = defaultdict(list)
    for stamp, values in obj["totalDataChartBreakdown"]:
        date = pd.Timestamp(stamp, unit="s", tz="UTC")
        if not start_ts <= date < end_ts:
            continue
        for name, value in values.items():
            if value is not None:
                if isinstance(value, (dict, list)):
                    raise ValueError("Unexpected nested protocol history")
                rows[name].append((date, value))
    return rows


LLAMA = [
    ("dexs", "dailyVolume", "dex_turnover", "decentralized_exchange_activity", "DEX traded volume"),
    ("fees", "dailyFees", "fees", "protocol_user_fees", "user fees"),
    ("fees", "dailyRevenue", "revenue", "protocol_earnings", "protocol revenue"),
    ("fees", "dailyHoldersRevenue", "holder_revenue", "token_holder_distributions", "token-holder revenue"),
    ("options", "dailyNotionalVolume", "option_notional", "decentralized_options_activity", "options notional volume"),
    ("options", "dailyPremiumVolume", "option_premium", "decentralized_options_activity", "options premium volume"),
    ("open-interest", None, "open_interest", "decentralized_leverage", "open interest"),
]


def collect_defillama(fetcher, start, end, workers):
    good, bad = [], []
    for endpoint, datatype, metric, category, label in LLAMA:
        url = "https://api.llama.fi/overview/" + endpoint
        params = {"excludeTotalDataChart": "false", "excludeTotalDataChartBreakdown": "false"}
        if datatype:
            params["dataType"] = datatype
        try:
            raw, digest = fetcher.get(url, params=params, timeout=180)
            obj = json.loads(raw)
            protocols = {p["name"]: p for p in obj["protocols"]}
            rows = parse_llama_breakdown(obj, start, end)
            for name, observations in rows.items():
                info = protocols.get(name)
                if not info:
                    OMISSIONS.append({"provider": "defillama", "candidate": name, "reason": "No current source identity metadata"})
                    continue
                identity = f"defillama:dimensions:{endpoint}:{datatype or 'open-interest'}:{info['slug']}"
                def one():
                    frame = clean([r[0] for r in observations], [r[1] for r in observations])
                    # Explicit zeros are valid, but launch padding does not create
                    # a qualifying history for a very recently active protocol.
                    nonzero = frame.loc[frame.value.ne(0), "date"]
                    if nonzero.dt.floor("D").nunique() < 90 or (nonzero.max() - nonzero.min()).days < 90:
                        raise Omission("Fewer than 90 active days or 90 days of active history")
                    frame = frame[frame.date.ge(nonzero.min())]
                    return save("llama_" + slug(info["slug"]) + "_" + metric, frame,
                        title=f"{name}: daily {label}", provider="DeFiLlama protocol activity",
                        category=category, unit="USD", frequency="daily",
                        source_url=request_url(url, params), geography="Global blockchain participants",
                        hypothesis=f"Changes in {label} at {name} may reveal risk-taking, demand for financial intermediation, collateral pressure or token cash-flow changes ahead of broad crypto-market repricing.",
                        limitations="Adapter-estimated blockchain activity with protocol-specific accounting. USD values also move with token prices. Fees, revenue, holder distributions and trading volume overlap economically and must not be added indiscriminately. Parent/child or migrated protocols may overlap. Historical revisions and source backfills are possible; current protocol metadata creates selection bias. Missing keys remain missing. Source negative fees/revenue, where reported, are retained as accounting adjustments or losses.",
                        raw_sha256=digest, dataset_identity=identity,
                        documentation_url="https://api-docs.defillama.com/",
                        protocol=info["slug"], protocol_category=info.get("category", ""),
                        chains=info.get("chains", []), source_metric=datatype or "open-interest",
                        methodology_url=info.get("methodologyURL", ""),
                        source_methodology=json.dumps(info.get("methodology", {}), sort_keys=True),
                        selection_rule="All source protocols with at least 90 nonzero observed days; exact numerical and pre-batch identities excluded.")
                attempt(good, "defillama", identity, one)
            print(f"DONE defillama {metric} accepted_so_far={len(good)}", flush=True)
        except Exception as exc:
            bad.append({"provider": "defillama", "endpoint": endpoint, "metric": metric, "error": str(exc)})
            print("FAIL " + json.dumps(bad[-1]), flush=True)
        time.sleep(1)
    return good, bad


def parse_binance_klines(rows, start, end):
    if isinstance(rows, dict):
        raise ValueError("Binance returned an error object: " + str(rows))
    if not rows:
        raise Omission("No Binance trades")
    if any(len(r) != 12 for r in rows):
        raise ValueError("Unexpected Binance kline schema")
    frame = pd.DataFrame(rows)
    # REST millisecond precision is explicit; archives use a different format.
    frame["date"] = pd.to_datetime(pd.to_numeric(frame[0], errors="raise"), unit="ms", utc=True)
    frame["close"] = pd.to_datetime(pd.to_numeric(frame[6], errors="raise"), unit="ms", utc=True)
    frame = frame[(frame.date >= pd.Timestamp(start, tz="UTC")) & (frame.close < pd.Timestamp(end, tz="UTC"))]
    if frame["date"].duplicated().any():
        raise ValueError("Duplicate Binance bars")
    for column in (7, 8, 10):
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if frame[column].lt(0).any():
            raise ValueError("Negative turnover/trade count")
    if frame[10].gt(frame[7] + 1e-6).any():
        raise ValueError("Taker buy turnover exceeds total turnover")
    return frame


def binance_symbol_id(symbol):
    """Unicode asset names must not collapse to the same ASCII quote suffix."""
    stem = slug(symbol)
    return stem if symbol.isascii() else stem + "_" + hashlib.sha256(symbol.encode()).hexdigest()[:12]


def collect_binance(fetcher, start, end, workers, symbols=None):
    base = "https://data-api.binance.vision/api/v3/"
    def discover():
        raw, digest = fetcher.get(base + "exchangeInfo", timeout=180)
        obj = json.loads(raw)
        specs = [{"symbol": s["symbol"], "baseAsset": s["baseAsset"], "quoteAsset": s["quoteAsset"], "status": s["status"]}
                 for s in obj["symbols"] if s.get("isSpotTradingAllowed")]
        return {"specs": specs, "raw_sha256": [digest]}
    sel = selection("binance", discover)
    good, bad = [], []
    def one(spec):
        url = base + "klines"
        parts, hashes, urls = [], [], []
        cursor = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
        until = int(pd.Timestamp(end, tz="UTC").timestamp() * 1000)
        while cursor < until:
            params = {"symbol": spec["symbol"], "interval": "1d", "startTime": cursor, "endTime": until - 1, "limit": 1000}
            raw, digest = fetcher.get(url, params=params, timeout=120)
            rows = json.loads(raw)
            if not isinstance(rows, list):
                raise ValueError(str(rows))
            if not rows:
                break
            parts.extend(rows); hashes.append(digest); urls.append(request_url(url, params))
            cursor = int(rows[-1][6]) + 1
            if len(rows) < 1000:
                break
            time.sleep(0.15)
        frame = parse_binance_klines(parts, start, end)
        # A source-provided zero bar is valid, but require a real trading history.
        positive = frame.loc[frame[8].gt(0), "date"]
        if positive.nunique() < 90 or (positive.max() - positive.min()).days < 90:
            raise Omission("Fewer than 90 days of executed trades")
        out = []
        for column, metric, label, unit in [
            (8, "trade_count", "executed trades", "trades"),
            (7, "quote_turnover", "traded quote-asset volume", spec["quoteAsset"]),
            (10, "taker_buy_quote", "buyer-initiated quote-asset volume", spec["quoteAsset"]),
        ]:
            attempt(out, "binance", spec["symbol"] + "/" + metric, lambda: save(
                "binance_spot_" + binance_symbol_id(spec["symbol"]) + "_" + metric,
                clean(frame.date, frame[column]),
                title=f"Binance spot {spec['baseAsset']}/{spec['quoteAsset']}: {label}",
                provider="Binance global spot trading", category="crypto_exchange_transactions" if column == 8 else "crypto_exchange_order_flow",
                unit=unit, frequency="daily", source_url=urls[0], source_urls=urls,
                geography="Global Binance spot venue; trader locations unavailable",
                hypothesis="Venue-level transaction intensity, turnover and buyer-initiated flow may expose shifts in retail participation, capital rotation and risk appetite before broad crypto indices respond.",
                limitations="One venue only; activity can reflect market making, wash trading, bot arbitrage, promotions and listings. Quote-asset units are not necessarily USD and must not be summed across different quotes without conversion. Taker-buy turnover is a component of total turnover. Active and archived symbols selected from current exchange metadata introduce survivorship bias. No price bars are published by this collector. Incomplete current-day bars are excluded.",
                raw_sha256=[*hashes, *sel["raw_sha256"]], dataset_identity=f"Binance:spot:{spec['symbol']}:{metric}",
                documentation_url="https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md",
                exchange_symbol=spec["symbol"], base_asset=spec["baseAsset"], quote_asset=spec["quoteAsset"],
                source_symbol_status=spec["status"], native_frequency="daily source aggregate of executions",
                finer_frequency_available=True, finer_frequency_note="The public API and official data.binance.vision archive support hourly, minute and individual trade histories.",
                timestamp_semantics="UTC daily bar open; all executions in that completed day. Value becomes complete after bar close."))
        return out
    with ThreadPoolExecutor(max_workers=min(workers, 4)) as pool:
        specs = [s for s in sel["specs"] if symbols is None or s["symbol"] in symbols]
        futures = {pool.submit(one, spec): spec["symbol"] for spec in specs}
        for future in as_completed(futures):
            try:
                good.extend(future.result())
            except Omission as exc:
                OMISSIONS.append({"provider": "binance", "candidate": futures[future], "reason": str(exc)})
            except Exception as exc:
                bad.append({"provider": "binance", "candidate": futures[future], "error": str(exc)})
                print("FAIL " + json.dumps(bad[-1]), flush=True)
    return good, bad


EXTRA_TOPICS = {
    "household_financial_stress": "Unemployment|Bankruptcy|Debt consolidation|Payday loan|Food bank|Foreclosure|Eviction|Wage|Minimum wage|Renting|Mortgage loan|Personal bankruptcy|Recession|Inflation|Deflation|Stagflation|Hyperinflation|Bank run|Deposit insurance|Capital flight|Currency crisis|Sovereign default",
    "fear_safety_and_health": "Panic buying|Hoarding|Survivalism|Emergency management|Earthquake|Drought|Flood|Wildfire|Heat wave|Pandemic|Epidemic|Influenza|Dengue fever|Measles|Cholera|Antibiotic|Antiviral drug|Vaccination|Air pollution|Water scarcity|Power outage|Cyberattack|Ransomware|Identity theft|Fraud",
    "household_purchase_intent": "Used car|Electric vehicle|Solar panel|Heat pump|Air conditioning|Refrigerator|Washing machine|Smartphone|Laptop|Mortgage calculator|Travel insurance|Life insurance|Health insurance|Pet insurance|Student loan|Buy now, pay later|Credit card|Debit card|Mobile payment|Digital wallet|Remittance|Foreign exchange market",
    "cross_border_and_logistics": "Emigration|Immigration|Visa policy|Passport|Work permit|Freight transport|Shipping container|Container ship|Port|Air cargo|Rail freight transport|Baltic Dry Index|Supply chain|Semiconductor|Lithium|Copper|Gold|Silver|Uranium|Natural gas|Liquefied natural gas|Petroleum|Diesel fuel|Jet fuel",
    "food_agriculture_and_consumption": "Wheat|Maize|Rice|Soybean|Coffee|Cocoa bean|Sugar|Palm oil|Olive oil|Cotton|Rubber|Fertilizer|Pesticide|Avian influenza|African swine fever|Foot-and-mouth disease|Salmon|Aquaculture|Beef|Pork|Milk|Egg as food|Beer|Wine|Tourism",
    "digital_risk_and_adoption": "Bitcoin|Ethereum|Stablecoin|Cryptocurrency exchange|Cryptocurrency wallet|Decentralized finance|Non-fungible token|Artificial intelligence|ChatGPT|DeepSeek|Data center|Cloud computing|Virtual private network|Tor (network)|Signal (software)|Telegram (software)|WhatsApp|WeChat|TikTok|E-commerce|Online shopping|Online gambling|Sports betting|Prediction market",
}

LANGUAGES = "es pt fr de it ru uk pl nl sv no da fi cs hu ro bg el tr ar fa he hi bn ur ta te mr id ms vi th zh ja ko sw am ha yo af zu tl ca eu gl sr hr sk sl lt lv et ka hy az kk uz mn ne si km lo my".split()


def wiki_topics():
    result = []
    seen = set()
    for theme, titles in EXTRA_TOPICS.items():
        for title in titles.split("|"):
            if title not in seen:
                seen.add(title); result.append([title, theme])
    # Add applied industrial themes without making this a software-only panel.
    for theme, titles in WIKI_GROUPS.items():
        for title in titles.replace("%26", "&").split("|")[:6]:
            if title not in seen:
                seen.add(title); result.append([title, slug(theme)])
    return result


def wikipedia_baseline_entities(fetcher):
    def discover():
        by_language = defaultdict(list)
        for identity in baseline()["identities"]:
            match = re.fullmatch(r"pageviews/([^.]+)\.wikipedia\.org/all-access/user/(.+)", identity)
            if match and match[1] != "en":
                by_language[match[1]].append(match[2])
        excluded, hashes = [], []
        for language, articles in sorted(by_language.items()):
            for offset in range(0, len(articles), 40):
                params = {"action": "query", "format": "json", "prop": "pageprops", "ppprop": "wikibase_item",
                          "redirects": 1, "converttitles": 1, "titles": "|".join(articles[offset:offset + 40])}
                raw, digest = fetcher.get(f"https://{language}.wikipedia.org/w/api.php", params=params, timeout=120)
                hashes.append(digest)
                obj = json.loads(raw)
                if "error" in obj:
                    raise ValueError(str(obj["error"]))
                for page in obj["query"]["pages"].values():
                    entity = page.get("pageprops", {}).get("wikibase_item")
                    if entity:
                        excluded.append([language, entity])
                time.sleep(0.2)
        return {"excluded": sorted(excluded), "raw_sha256": hashes,
                "purpose": "Exclude pre-batch Wikimedia articles by canonical entity, including renamed pages, redirects and Chinese title variants."}
    return selection("wikipedia_baseline_entities", discover)


def balanced_wikipedia_specs(specs, per_language=8):
    """Take varied themes per language, with rotating theme priority."""
    by_language = defaultdict(list)
    for spec in specs:
        by_language[spec[0]].append(spec)
    chosen = []
    for index, language in enumerate(sorted(by_language)):
        rows = by_language[language]
        themes = sorted({row[3] for row in rows})
        themes = themes[index % len(themes):] + themes[:index % len(themes)]
        queues = {theme: [row for row in rows if row[3] == theme] for theme in themes}
        count = 0
        for n in range(max(map(len, queues.values()))):
            for theme in themes:
                if n < len(queues[theme]):
                    chosen.append(queues[theme][n])
                    count += 1
                    if count == per_language:
                        break
            if count == per_language:
                break
    return chosen


def collect_wikipedia(fetcher, start, end, workers):
    def discover():
        topics = wiki_topics()
        specs, hashes, seen = [], [], set()
        for offset in range(0, len(topics), 40):
            batch = topics[offset:offset + 40]
            params = {"action": "wbgetentities", "sites": "enwiki", "titles": "|".join(t for t, _ in batch),
                      "props": "sitelinks", "format": "json"}
            raw, digest = fetcher.get("https://www.wikidata.org/w/api.php", params=params, timeout=120)
            hashes.append(digest)
            obj = json.loads(raw)
            if "error" in obj:
                raise ValueError(str(obj["error"]))
            themes = dict(batch)
            for entity in obj["entities"].values():
                links = entity.get("sitelinks", {})
                english = links.get("enwiki", {}).get("title")
                if english not in themes:
                    continue
                for language in LANGUAGES:
                    title = links.get(language.replace("-", "_") + "wiki", {}).get("title")
                    if title and (language, title) not in seen:
                        seen.add((language, title))
                        specs.append([language, title, english, themes[english], entity["id"]])
            time.sleep(0.4)
        # Languages and themes are interleaved so partially completed runs remain
        # geographically and economically broad.
        queues = defaultdict(list)
        for spec in specs:
            queues[(spec[0], spec[3])].append(spec)
        chosen = []
        for n in range(max(map(len, queues.values()))):
            for key in sorted(queues):
                if n < len(queues[key]):
                    chosen.append(queues[key][n])
        return {"specs": balanced_wikipedia_specs(chosen), "discovered_specs": chosen, "raw_sha256": hashes,
                "selection_rule": f"Fixed economic and behavioral topics translated using official Wikidata sitelinks across {len(LANGUAGES)} non-English language editions; up to eight candidates per language, rotating theme priority."}
    sel = selection("wikipedia", discover)
    canonical_old = wikipedia_baseline_entities(fetcher)
    old_entities = {tuple(x) for x in canonical_old["excluded"]}
    good, bad = [], []
    # Wikimedia's access policy recommends completing one request before the
    # next; this intentionally uses a single sequential stream.
    for spec in sel["specs"]:
        language, article, english, theme, entity = spec
        if (language, entity) in old_entities:
            OMISSIONS.append({"provider": "wikipedia", "candidate": str(spec), "reason": "Canonical article entity already exists in the pre-batch library"})
            continue
        identity = f"pageviews/{language}.wikipedia.org/all-access/user/{article.replace(' ', '_')}"
        sid = f"wikipedia_{slug(language)}_{entity.lower()}_views"
        stage = ROOT / "archive/staging" / GROUP / (sid + ".json")
        if stage.exists() and not fetcher.refresh:
            existing = json.loads(stage.read_text())
            with LOCK:
                SEEN[existing["content_sha256"]] = sid
            good.append(existing)
            continue
        url = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
               f"{language}.wikipedia.org/all-access/user/{quote(article.replace(' ', '_'), safe='')}/daily/"
               f"{start.replace('-', '')}00/{end.replace('-', '')}00")
        try:
            raw, digest = fetcher.get(url, timeout=120)
            def one():
                frame = parse_wikimedia(raw)
                nonzero = frame.loc[frame.value.gt(0), "date"]
                if nonzero.dt.floor("D").nunique() < 90:
                    raise Omission("Fewer than 90 observed days with human page views")
                return save(sid, frame, title=f"{language} Wikipedia human attention: {article} ({english})",
                    provider="Wikimedia multilingual attention", category="attention_" + slug(theme),
                    unit="human-classified page views", frequency="daily", source_url=url,
                    geography=f"Worldwide readers of {language} Wikipedia; language is not a country identifier",
                    hypothesis=f"Local-language research about {english} may reveal changes in {theme.replace('_', ' ')}, household intentions, perceived risk or technology demand before market aggregates respond.",
                    limitations="Page views measure attention, not sentiment, search queries, purchases or unique people. Language does not establish user location. Bots escaping source heuristics, school calendars and news can dominate. Source page names and present-day topic selection create selection bias; redirect traffic is not added. No dates are invented and missing dates remain missing.",
                    raw_sha256=[digest, *sel["raw_sha256"], *canonical_old["raw_sha256"]], dataset_identity=identity,
                    documentation_url="https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html",
                    language=language, article=article, english_topic=english, wikidata_entity=entity,
                    market_theme=theme, selection_rule=sel["selection_rule"], license="CC0 1.0")
            attempt(good, "wikipedia", identity, one)
        except requests.HTTPError as exc:
            status = exc.response.status_code
            if status == 404:
                OMISSIONS.append({"provider": "wikipedia", "candidate": identity, "reason": "No source page-view history"})
            else:
                bad.append({"provider": "wikipedia", "candidate": identity, "error": str(exc)})
                if status in {403, 429}:
                    break
        except Exception as exc:
            bad.append({"provider": "wikipedia", "candidate": identity, "error": str(exc)})
        time.sleep(0.15)
    return good, bad


def parse_tranco(raw):
    frame = pd.read_csv(io.BytesIO(raw), header=None, names=["rank", "domain"])
    frame["rank"] = pd.to_numeric(frame["rank"], errors="raise")
    if frame.domain.duplicated().any() or frame["rank"].duplicated().any() or frame["rank"].le(0).any():
        raise ValueError("Malformed Tranco rank table")
    return frame


def collect_tranco(fetcher, start, end, workers):
    request_lock = threading.Lock()
    blocked = threading.Event()
    next_request = [0.0]
    def limited_get(url):
        # The service applies a one-query-per-second quota. All calls share this
        # limiter, including metadata and download URLs; no parallel bursts.
        with request_lock:
            if blocked.is_set():
                raise Omission("Tranco acquisition paused after a service restriction")
            time.sleep(max(0, next_request[0] - time.monotonic()))
            next_request[0] = time.monotonic() + 1.2
        try:
            return fetcher.get(url, timeout=180)
        except requests.HTTPError as exc:
            if exc.response.status_code in {403, 429}:
                blocked.set()
            raise
    def download_day(day):
        url = "https://tranco-list.eu/api/lists/date/" + day.strftime("%Y%m%d")
        raw, meta_digest = limited_get(url)
        obj = json.loads(raw)
        if not obj.get("available") or obj.get("failed"):
            raise Omission("Tranco daily ranking unavailable")
        if obj["configuration"]["endDate"] != day.date().isoformat():
            raise ValueError("Tranco returned a different ranking date")
        download = obj["download"].rsplit("/", 1)[0] + "/100000"
        content, digest = limited_get(download)
        return day, parse_tranco(content), [meta_digest, digest], obj
    def discover():
        day, frame, hashes, meta = download_day(pd.Timestamp(start))
        from babel import Locale
        countries = {c.lower() for c in Locale.parse("en").territories if re.fullmatch("[A-Z]{2}", c)} - {"us", "zz", "xa", "xb", "un", "ez"}
        countries.update({"uk", "eu"})
        queues = defaultdict(list)
        for row in frame.itertuples():
            suffix = row.domain.rsplit(".", 1)[-1]
            if suffix in countries:
                queues[suffix].append(row.domain)
        specs = []
        # ccTLD is explicitly a domain registration clue, not traffic geolocation.
        for index in range(150):
            for suffix in sorted(queues):
                if index < len(queues[suffix]):
                    specs.append([queues[suffix][index], suffix])
                if len(specs) == 5000:
                    return {"specs": specs, "raw_sha256": hashes, "configuration": meta["configuration"]}
        return {"specs": specs, "raw_sha256": hashes, "configuration": meta["configuration"]}
    sel = selection("tranco", discover)
    chosen = {domain for domain, _ in sel["specs"]}
    observations, digests, configs, bad = defaultdict(list), [], [], []
    days = pd.date_range(start, pd.Timestamp(end) - pd.Timedelta(days=1), freq="D")
    with ThreadPoolExecutor(max_workers=min(workers, 3)) as pool:
        futures = {pool.submit(download_day, day): day for day in days}
        for index, future in enumerate(as_completed(futures)):
            try:
                day, frame, hashes, obj = future.result()
                digests.extend(hashes)
                config = obj["configuration"]
                configs.append({"date": str(day.date()), "created_on": obj["created_on"], "list_id": obj["list_id"],
                                "providers": config.get("providers"), "combinationMethod": config.get("combinationMethod")})
                for row in frame[frame.domain.isin(chosen)].itertuples():
                    observations[row.domain].append((day, row.rank))
                if index % 25 == 0:
                    print(f"PROGRESS tranco dates={index + 1}/{len(days)}", flush=True)
            except Omission as exc:
                OMISSIONS.append({"provider": "tranco", "candidate": str(futures[future].date()), "reason": str(exc)})
            except Exception as exc:
                bad.append({"provider": "tranco", "candidate": str(futures[future].date()), "error": str(exc)})
                print("FAIL " + json.dumps(bad[-1]), flush=True)
    atomic_bytes(ROOT / "archive/digital4_selection/tranco_daily_configurations.json", json.dumps(configs, indent=2).encode())
    manifest = archive_raw_manifest(digests, description="Tranco original daily list metadata and top-100,000 ranking responses")
    good = []
    for domain, suffix in sel["specs"]:
        rows = observations[domain]
        attempt(good, "tranco", domain, lambda: save(
            "tranco_" + slug(domain) + "_" + hashlib.sha256(domain.encode()).hexdigest()[:8] + "_rank",
            clean([r[0] for r in rows], [r[1] for r in rows]),
            title=f"Tranco daily global popularity rank: {domain}",
            provider="Tranco web popularity research", category="international_web_popularity",
            unit="global domain popularity rank; lower is more popular", frequency="daily",
            source_url="https://tranco-list.eu/", geography=f"Global traffic signals for .{suffix} domain {domain}; ccTLD is not user location",
            hypothesis="Relative popularity shifts for country-domain businesses, banks, media, government portals and online services may proxy commerce, credit concern, information demand and digital adoption.",
            limitations="An ordinal composite ranking, not visit counts or bandwidth. Each daily publication combines provider rankings over a rolling 30-day window, creating substantial serial dependence and publication lag. Inputs and ranking methodology can change. Domain country suffix does not identify users or ownership; .io/.ai/.co and other suffixes are widely used internationally. Only presence in the source top 100,000 is observed; absent ranks remain missing, never 100,001. Domains selected from the start-date ranking avoid later-survivor selection.",
            raw_sha256=sel["raw_sha256"], raw_manifest_sha256=[manifest], dataset_identity="Tranco:daily-standard:domain:" + domain,
            documentation_url="https://tranco-list.eu/api_documentation", domain=domain, domain_country_suffix=suffix,
            native_frequency="daily publication of a rolling 30-day composite",
            source_release_frequency="daily", timestamp_semantics="Ranking configuration end date in UTC; actual publication times are retained in archived per-day metadata.",
            selection_rule="Up to 150 high-ranked domains per non-US ccTLD, round robin, from the initial source ranking; maximum 5,000."))
    return good, bad


IODA_TYPES = {
    "gtr": ("Google Transparency Report via IODA", "country_google_product_traffic", "source relative traffic index; not visits"),
    "bgp": ("IODA global Internet connectivity", "country_internet_connectivity", "visible IPv4 /24 equivalents"),
    "ping-slash24": ("IODA global Internet connectivity", "country_internet_connectivity", "responsive IPv4 /24 blocks"),
    "merit-nt": ("IODA global Internet connectivity", "country_network_background_activity", "unique source IPs per native interval"),
}
US_TERRITORIES = {"AS", "GU", "MP", "PR", "UM", "VI"}


def parse_ioda(raw):
    obj = json.loads(raw)
    if obj.get("error"):
        raise ValueError(str(obj["error"]))
    result = []
    for group in obj["data"] or []:
        for series in group:
            if series["datasource"] not in IODA_TYPES:
                continue
            if series["step"] != 3600:
                raise ValueError(f"Expected source hourly step, got {series['step']}")
            values = series["values"]
            if any(isinstance(v, (dict, list)) for v in values if v is not None):
                raise ValueError("Unexpected nonnumeric IODA source values")
            dates = pd.to_datetime(series["from"] + np.arange(len(values)) * series["step"], unit="s", utc=True)
            frame = pd.DataFrame({"date": dates, "value": pd.to_numeric(values, errors="raise")}).dropna()
            # The provider may include an endpoint; keep source timestamps and
            # resolve only identical boundary duplicates after concatenation.
            result.append((series, frame))
    return result


def collect_ioda(fetcher, start, end, workers):
    def discover():
        raw, digest = fetcher.get("https://api.ioda.inetintel.cc.gatech.edu/v2/entities/query",
                                 params={"entityType": "country", "limit": 500}, timeout=120)
        docs, docs_digest = fetcher.get("https://api.ioda.inetintel.cc.gatech.edu/v2/datasources/")
        from babel import Locale
        valid = {c for c in Locale.parse("en").territories if re.fullmatch("[A-Z]{2}", c)} - {"ZZ", "XA", "XB", "UN", "EZ", "EU"}
        specs = [[r["code"], r["name"]] for r in json.loads(raw)["data"] if r["code"] in valid and r["code"] != "US"]
        return {"specs": sorted(specs), "raw_sha256": [digest, docs_digest]}
    sel = selection("ioda", discover)
    good, bad = [], []
    windows = []
    cursor = pd.Timestamp(start, tz="UTC")
    finish = pd.Timestamp(end, tz="UTC")
    while cursor < finish:
        # Fixed 63-day windows make the API's point budget select exactly 1h.
        stop = min(cursor + pd.Timedelta(days=63), finish)
        windows.append((cursor, stop))
        cursor = stop
    progress_path = ROOT / "archive/digital4_selection" / f"ioda_progress_{start}_{end}.json"
    completed = set()
    if progress_path.exists() and not fetcher.refresh:
        completed = set(json.loads(progress_path.read_text())["completed_countries"])
    for path in (ROOT / "archive/staging/digital4").glob("ioda_*.json"):
        meta = json.loads(path.read_text())
        if meta["country_codes"][0] in completed:
            with LOCK:
                SEEN[meta["content_sha256"]] = meta["series_id"]
            good.append(meta)
    progress_lock = threading.Lock()
    country_names = dict(sel["specs"])
    def one(codes):
        frames, source_meta = defaultdict(list), {}
        hashes, urls = defaultdict(list), defaultdict(list)
        url = "https://api.ioda.inetintel.cc.gatech.edu/v2/signals/raw/country/" + ",".join(codes)
        # Both country lists and explicit datasource filters are documented
        # API features. Avoid querying forecasting/telemetry backends unused here.
        for begin, stop in windows:
            for source in IODA_TYPES:
                params = {"from": int(begin.timestamp()), "until": int(stop.timestamp()),
                          "maxPoints": int((stop - begin).total_seconds() // 3600),
                          "datasource": source}
                if source == "gtr":
                    params["sourceParams"] = "WEB_SEARCH,YOUTUBE,GMAIL,DOCS,MAPS"
                raw, digest = fetcher.get(url, params=params, timeout=180)
                hashes[source].append(digest); urls[source].append(request_url(url, params))
                for meta, frame in parse_ioda(raw):
                    code = meta.get("entityCode")
                    if meta.get("entityType") != "country" or code not in codes or meta["datasource"] != source:
                        raise ValueError("IODA returned observations for a different country or datasource")
                    key = (code, source, meta.get("subtype", ""))
                    frames[key].append(frame); source_meta[key] = meta
                time.sleep(0.05)
        out = []
        for (code, source, subtype), pieces in frames.items():
            country = country_names[code]
            provider, category, unit = IODA_TYPES[source]
            detail = subtype or source
            label = {"bgp": "BGP visibility", "ping-slash24": "active-probe responsiveness",
                     "merit-nt": "network telescope background activity", "gtr": subtype}.get(source)
            def candidate():
                combined = pd.concat(pieces, ignore_index=True)
                frame = clean(combined.date, combined.value)
                return save("ioda_" + code.lower() + "_" + slug(source) + ("_" + slug(subtype) if subtype else ""), frame,
                    title=f"{country}: {label}, source hourly observations", provider=provider,
                    category=category, unit=unit, frequency="hourly", source_url=urls[source][0], source_urls=urls[source],
                    geography=country, geography_scope="us" if code in US_TERRITORIES else "non_us", country_codes=[code],
                    hypothesis="Changes in country-level web search, online work, navigation and video usage can reveal economic activity or risk-driven attention, while Internet availability and background-network anomalies can identify outages, unrest, cyber incidents and interruptions to commerce.",
                    limitations="Country geolocation and provider coverage are imperfect. Google product series are relative/proportional traffic indices in source units, not absolute visits or unique people, and scales are not comparable across countries/products. Network telescope traffic is unsolicited background traffic rather than total bandwidth. BGP visibility and responsive-block counts measure connectivity rather than users. The API produces hourly values from finer native measurements; these exact source values are retained, with source gaps omitted and no local interpolation. Source outages, methodology changes and retrospectively revised histories can occur.",
                    raw_sha256=[*hashes[source], *sel["raw_sha256"]], dataset_identity=f"IODA:country:{code}:{source}:{subtype}:hourly",
                    documentation_url="https://api.ioda.inetintel.cc.gatech.edu/v2/",
                    source_datasource=source, source_subtype=subtype,
                    native_frequency=f"{source_meta[(code, source, subtype)]['nativeStep']} seconds",
                    source_interval_seconds=3600,
                    finer_frequency_available=source_meta[(code, source, subtype)]["nativeStep"] < 3600,
                    finer_frequency_note="Source nativeStep retained in metadata. API maxPoints selects source hourly reductions; no local resampling is performed.",
                    source_release_frequency="intraday; Google traffic generally approximately 2–3 hours behind",
                    timestamp_semantics="UTC source hourly period timestamp constructed from the source's from/step fields; only nonnull source observations retained.",
                    license="IODA data copyright Georgia Tech Research Corporation; underlying Google and network-source terms also apply.")
            attempt(out, "ioda", code + "/" + detail, candidate)
        with progress_lock:
            completed.update(codes)
            atomic_bytes(progress_path, json.dumps({"start": start, "end": end,
                "completed_countries": sorted(completed)}, indent=2).encode())
        print(f"DONE ioda batch={','.join(codes)} accepted={len(out)}", flush=True)
        return out
    pending_codes = [code for code, _ in sel["specs"] if code not in completed]
    batches = [pending_codes[i:i + 10] for i in range(0, len(pending_codes), 10)]
    with ThreadPoolExecutor(max_workers=min(workers, 3)) as pool:
        futures = {pool.submit(one, batch): batch for batch in batches}
        for future in as_completed(futures):
            try:
                good.extend(future.result())
            except Exception as exc:
                bad.append({"provider": "ioda", "candidate": futures[future], "error": str(exc)})
                print("FAIL " + json.dumps(bad[-1]), flush=True)
    return good, bad


def collect_auxiliary(fetcher, start, end, workers):
    good, bad = [], []
    url = "https://api.alternative.me/fng/"
    params = {"limit": 0}
    try:
        raw, digest = fetcher.get(url, params=params, timeout=120)
        obj = json.loads(raw)
        if obj.get("metadata", {}).get("error"):
            raise ValueError(str(obj["metadata"]["error"]))
        frame = clean(pd.to_datetime([int(r["timestamp"]) for r in obj["data"]], unit="s", utc=True),
                      [r["value"] for r in obj["data"]])
        frame = frame[frame.date.lt(pd.Timestamp(end, tz="UTC"))]
        if not frame.value.between(0, 100).all():
            raise ValueError("Fear and Greed index outside source range")
        good.append(save("alternative_me_crypto_fear_greed", frame,
            title="Alternative.me Bitcoin Fear & Greed Index", provider="Alternative.me",
            category="crypto_sentiment", unit="index from 0 (extreme fear) to 100 (extreme greed)",
            frequency="daily", source_url=request_url(url, params), geography="Global Bitcoin market sentiment proxy",
            hypothesis="A documented sentiment-regime measure may contextualize crowding and overreaction risk when paired with independent Internet behavior or liquidity series.",
            limitations="A provider-produced composite, not a direct survey or an independent leading signal. It includes market price/volatility, momentum/volume, dominance, social activity and search-trend inputs; surveys are paused. Raw component histories are not exposed. Overlap with market prices and publication timing must be considered in backtests.",
            raw_sha256=digest, dataset_identity="Alternative.me:fng:bitcoin",
            documentation_url="https://alternative.me/crypto/fear-and-greed-index/",
            license="Commercial use permitted with prominent Alternative.me attribution beside any display of the data.",
            attribution="Alternative.me — https://alternative.me/crypto/fear-and-greed-index/"))
    except Exception as exc:
        bad.append({"provider": "alternative_me", "error": str(exc)})
    for metric in ["mempool-count", "mempool-size", "mempool-growth", "transactions-per-second"]:
        url = "https://api.blockchain.info/charts/" + metric
        params = {"timespan": "3years", "sampled": "false", "format": "json"}
        try:
            raw, digest = fetcher.get(url, params=params, timeout=120)
            obj = json.loads(raw)
            if obj.get("status") != "ok":
                raise ValueError(str(obj)[:500])
            frame = clean(pd.to_datetime([r["x"] for r in obj["values"]], unit="s", utc=True),
                          [r["y"] for r in obj["values"]])
            frame = frame[frame.date.lt(pd.Timestamp(end, tz="UTC"))]
            interval = coverage(frame)["median_interval_seconds"]
            frequency = {60: "1min", 300: "5min", 600: "10min", 900: "15min",
                         1800: "30min", 3600: "hourly", 86400: "daily"}.get(interval)
            if frequency is None:
                raise ValueError(f"Undocumented output interval {interval} seconds")
            good.append(save("blockchain_com_" + slug(metric), frame,
                title="Blockchain.com Bitcoin " + obj["name"], provider="Blockchain.com network telemetry",
                category="bitcoin_transaction_congestion", unit=obj["unit"], frequency=frequency,
                source_url=request_url(url, params), geography="Global Bitcoin network as observed by Blockchain.com",
                hypothesis="Transaction arrivals and unconfirmed backlogs can reveal bursts of settlement demand, fee pressure and exchange-related transfers before broader activity measures settle.",
                limitations="One provider's mempool/network view, not a complete or geographically representative census. Source chart values can be averages, explaining fractional transaction counts. Provider sampling and mempool policies may differ across nodes; histories are retrospectively queried and may revise. No local resampling or gap filling is applied.",
                raw_sha256=digest, dataset_identity="Blockchain.com:charts:" + metric,
                documentation_url="https://www.blockchain.com/explorer/charts/",
                source_description=obj.get("description", ""),
                native_frequency="Source chart period: " + obj.get("period", "unspecified"),
                source_interval_seconds=interval, source_release_frequency="intraday",
                finer_frequency_available=True,
                finer_frequency_note="The source identifies minute telemetry; this historical chart returns its actual timestamps at the documented observed interval.",
                timestamp_semantics="UTC source chart timestamp; values may summarize a provider interval rather than an instantaneous observation."))
        except Exception as exc:
            bad.append({"provider": "blockchain_com", "candidate": metric, "error": str(exc)})
        time.sleep(0.4)
    return good, bad


AMSIX_PAGES = [
    ("amsterdam_total", "Amsterdam total exchange", "NL", "https://stats.ams-ix.net/index.html"),
    ("amsterdam_peering", "Amsterdam public peering", "NL", "https://stats.ams-ix.net/isp.html"),
    ("amsterdam_grx", "Amsterdam mobile roaming exchange", "NL", "https://stats.ams-ix.net/grx.html"),
    ("amsterdam_ipx", "Amsterdam international IP exchange", "NL", "https://stats.ams-ix.net/ipx.html"),
    ("kolkata_total", "Kolkata exchange", "IN", "https://stats-kol.ams-ix.net/"),
    ("lagos_total", "Lagos exchange", "NG", "https://stats-lag.ams-ix.net/"),
    ("singapore_total", "Singapore exchange", "SG", "https://stats-sin.ams-ix.net/"),
    ("curacao_total", "Curaçao exchange", "CW", "https://stats-cw.ams-ix.net/"),
    ("djibouti_total", "Djibouti exchange", "DJ", "https://stats-dji.ams-ix.net/"),
]


def parse_amsix_table(table):
    """The 'weekly' workbook actually contains seven measured daily columns."""
    labels = table.iloc[:, 0].astype(str)
    start = pd.Timestamp(table.loc[labels.eq("Start date")].iloc[0, 1])
    end = pd.Timestamp(table.loc[labels.eq("End date")].iloc[0, 1])
    unit = str(table.loc[labels.eq("Unit")].iloc[0, 1])
    header_index = table.index[labels.eq("Week")][0]
    header = table.loc[header_index].tolist()
    if header[:9] != ["Week", "Average", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        raise ValueError("Unexpected AMS-IX daily column headings")
    if start.weekday() != 0:
        raise ValueError("AMS-IX export start date is not a Monday")
    rows = []
    for index, row in enumerate(table.loc[header_index + 1:].itertuples(index=False, name=None)):
        if pd.isna(row[0]):
            continue
        monday = start + pd.Timedelta(days=7 * index)
        expected = monday.strftime("%d/%m") + "-" + (monday + pd.Timedelta(days=6)).strftime("%d/%m")
        # NL source range labels sometimes display the following Monday at the
        # spring daylight-saving boundary. The seven explicit Mon–Sun columns
        # still identify the daily cells; do not manufacture an eighth day.
        monday_endpoint = monday.strftime("%d/%m") + "-" + (monday + pd.Timedelta(days=7)).strftime("%d/%m")
        if row[0] not in {expected, monday_endpoint}:
            raise ValueError(f"AMS-IX week label {row[0]!r} does not match source start date: {expected}")
        for day, value in enumerate(row[2:9]):
            date = monday + pd.Timedelta(days=day)
            if date < end and pd.notna(value):
                rows.append((date, value))
    return clean([r[0] for r in rows], [r[1] for r in rows]), unit


def collect_amsix(fetcher, start, end, workers):
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    good, bad = [], []
    for key, label, country, page_url in AMSIX_PAGES:
        try:
            page, page_digest = fetcher.get(page_url, timeout=120)
            soup = BeautifulSoup(page, "html.parser")
            links = [urljoin(page_url, a["href"]) for a in soup.select("a[href]")
                     if "/exports/weekly_volume/" in a["href"] and a["href"].endswith(".xlsx")]
            if len(set(links)) != 1:
                raise ValueError("Expected one source-linked daily-volume workbook")
            url = links[0]
            raw, digest = fetcher.get(url, timeout=120)
            sheets = pd.read_excel(io.BytesIO(raw), sheet_name=None, header=None)
            if len(sheets) != 1:
                raise ValueError("Unexpected multi-sheet AMS-IX export")
            frame, unit = parse_amsix_table(next(iter(sheets.values())))
            attempt(good, "amsix", key, lambda: save(
                "amsix_" + key + "_traffic_volume", frame,
                title=f"AMS-IX {label}: daily traffic volume", provider="AMS-IX international Internet traffic",
                category="internet_exchange_bandwidth", unit=unit, frequency="daily", source_url=url,
                geography=label + "; physical exchange location, not location of all end users",
                geography_scope="non_us", country_codes=[country],
                hypothesis="Physical Internet traffic and mobile roaming exchange activity may track online commerce, remote work, streaming, international travel and disruptions to communications infrastructure.",
                limitations="Traffic traversing a particular exchange rather than all national traffic. Network membership, private interconnect substitution and routing policies can move volume independently of end-user demand. Total and service-specific series overlap and should not be added. Daily cells are rounded by the publisher; no weekly average is counted as an extra series. The source supplies a rolling year of daily observations, which refresh preserves cumulatively.",
                raw_sha256=[page_digest, digest], dataset_identity="AMS-IX:daily-volume:" + key,
                documentation_url=page_url, native_frequency="daily values in the publisher's weekly-layout spreadsheet",
                finer_frequency_available=True,
                finer_frequency_note="The same public statistics pages show finer intraday traffic graphs; this collector retains the directly downloadable daily values.",
                timestamp_semantics="Source displayed calendar date normalized to midnight UTC. Export does not document the timezone of the daily traffic integration boundary.",
                source_release_frequency="weekly rolling export with explicit daily cells"))
        except Exception as exc:
            bad.append({"provider": "amsix", "candidate": key, "error": str(exc)})
            print("FAIL " + json.dumps(bad[-1]), flush=True)
        time.sleep(0.3)
    return good, bad


PROVIDERS = ["defillama", "binance", "wikipedia", "ioda", "auxiliary", "amsix"]
OPTIONAL_PROVIDERS = ["tranco"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS + OPTIONAL_PROVIDERS + ["all"], default="all")
    parser.add_argument("--start", default=START)
    parser.add_argument("--end", default=(pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2)).date().isoformat())
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    baseline()
    fetcher = Fetcher(refresh=args.refresh)
    selected = PROVIDERS if args.provider == "all" else [args.provider]
    good, bad = [], []
    def run(provider):
        try:
            return provider, *globals()["collect_" + provider](fetcher, args.start, args.end, args.workers)
        except Exception as exc:
            print("FAIL " + json.dumps({"provider": provider, "error": str(exc)}), flush=True)
            return provider, [], [{"provider": provider, "error": str(exc)}]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for provider, results, errors in pool.map(run, selected):
            good.extend(results); bad.extend(errors)
            print(f"COMPLETE {provider} accepted={len(results)} errors={len(errors)}", flush=True)
    report = {"group": GROUP, "accepted": len(good), "providers": selected, "errors": bad,
              "omissions": OMISSIONS, "series_ids": [m["series_id"] for m in good],
              "finished_at": pd.Timestamp.now(tz="UTC").isoformat()}
    atomic_bytes(ROOT / "logs" / f"digital4_{args.provider}.json", json.dumps(report, indent=2).encode())
    print(json.dumps({"accepted": len(good), "errors": len(bad), "omissions": len(OMISSIONS)}))
    return bool(bad)


if __name__ == "__main__":
    raise SystemExit(main())
