#!/usr/bin/env python3
"""Daily public attention, developer adoption, internet resilience and city activity.

Real source observations only. The acquisition receipt and original response are
archived by expansion_common; this module never interpolates or fills dates.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import threading
import time
from urllib.parse import quote

import pandas as pd
import requests

from expansion_common import Fetcher, ROOT, atomic_bytes, save_candidate


WIKI_DOCS = "https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/page-views.html"
NPM_DOCS = "https://github.com/npm/registry/blob/main/docs/download-counts.md"
TOR_DOCS = "https://metrics.torproject.org/stats.html"
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST = 0.0


def paced_get(fetcher, *args, interval=5.1, **kwargs):
    """Conservative spacing, including when several parser workers are active."""
    global _LAST_REQUEST
    params = args[1] if len(args) > 1 else kwargs.get("params")
    prepared = requests.Request("GET", args[0], params=params).prepare().url
    key = hashlib.sha256(prepared.encode()).hexdigest()
    receipt = fetcher.root / "archive" / "requests" / (key + ".json")
    if not fetcher.refresh and receipt.exists():
        meta = json.loads(receipt.read_text())
        age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(meta["retrieved_at"])).total_seconds()
        if 0 <= age < kwargs.get("max_age_hours", 24) * 3600:
            raw = fetcher.root / "archive" / "raw" / (meta["sha256"] + ".bin")
            if raw.exists() and hashlib.sha256(raw.read_bytes()).hexdigest() == meta["sha256"]:
                return fetcher.get(*args, **kwargs)
    with _REQUEST_LOCK:
        delay = interval - (time.monotonic() - _LAST_REQUEST)
        if delay > 0:
            time.sleep(delay)
        _LAST_REQUEST = time.monotonic()
    return fetcher.get(*args, **kwargs)

# Deliberately spans household decisions, bottlenecks, health, commodities,
# geopolitical chokepoints and technological adoption, rather than stock tickers.
WIKI_TOPICS = [
    ("Inflation", "macro", "Household inflation attention may precede changes in rate expectations and consumer spending."),
    ("Recession", "macro", "Recession concern may track defensive positioning and weaker discretionary demand."),
    ("Unemployment", "labor", "Job-market concern may lead changes in consumption and credit stress."),
    ("Bankruptcy", "credit", "Financial distress attention may help nowcast credit-cycle deterioration."),
    ("Bank_run", "credit", "Deposit-safety concern may coincide with bank funding pressure."),
    ("Mortgage_loan", "housing", "Mortgage research intensity may anticipate housing financing activity."),
    ("Refinancing", "housing", "Refinancing attention may lead mortgage origination demand."),
    ("Credit_card", "consumer_finance", "Consumer credit attention may track borrowing demand and financial stress."),
    ("Student_loan", "consumer_finance", "Student-debt attention may track policy shocks to household cash flow."),
    ("Foreclosure", "housing", "Foreclosure attention may flag housing stress."),
    ("Rent_control", "housing", "Rent-policy attention may accompany regulatory risk for residential property."),
    ("Gold", "metals", "Gold attention may reflect safe-haven interest and retail investment demand."),
    ("Copper", "metals", "Copper attention may capture industrial-cycle and supply-disruption interest."),
    ("Lithium", "metals", "Lithium attention may track battery demand and supply-chain news."),
    ("Uranium", "energy", "Uranium attention may signal changes in nuclear-fuel interest."),
    ("Rare-earth_element", "metals", "Rare-earth attention may reflect strategic mineral supply concern."),
    ("Natural_gas", "energy", "Natural-gas attention may track energy shortage and price concern."),
    ("Liquefied_natural_gas", "energy", "LNG attention may flag international gas substitution and infrastructure demand."),
    ("Petroleum", "energy", "Oil-related attention may capture energy-market shocks."),
    ("Strategic_Petroleum_Reserve_(United_States)", "energy", "Reserve-policy attention may accompany anticipated oil supply changes."),
    ("Heat_pump", "energy_transition", "Heat-pump research may precede equipment adoption and electrification demand."),
    ("Solar_panel", "energy_transition", "Solar research may track installation demand and policy response."),
    ("Nuclear_power", "energy_transition", "Nuclear attention may track policy-driven power generation investment."),
    ("Hydrogen", "energy_transition", "Hydrogen attention may accompany industrial decarbonization investment."),
    ("Electric_vehicle", "mobility", "EV research may provide a demand-side adoption signal."),
    ("Lithium-ion_battery", "energy_transition", "Battery attention may indicate storage and mobility investment interest."),
    ("Wheat", "agriculture", "Wheat attention may respond to food supply disruptions."),
    ("Maize", "agriculture", "Corn attention may capture crop, feed and biofuel demand shocks."),
    ("Soybean", "agriculture", "Soybean attention may reflect agricultural trade and feed-market changes."),
    ("Rice", "agriculture", "Rice attention may track staple-food shortage concern."),
    ("Coffee", "agriculture", "Coffee attention may track consumption interest and crop news."),
    ("Cocoa_bean", "agriculture", "Cocoa attention may capture ingredient supply and confectionery cost concern."),
    ("Palm_oil", "agriculture", "Palm-oil attention may track food and biofuel supply risks."),
    ("Fertilizer", "agriculture", "Fertilizer attention may flag crop input-cost pressure."),
    ("Avian_influenza", "health", "Avian flu attention may precede poultry disruption and vaccine demand."),
    ("African_swine_fever", "health", "Swine-fever attention may flag pork supply risk."),
    ("Drought", "climate", "Drought attention may coincide with crop, power and water-supply risks."),
    ("El_Niño", "climate", "El Niño attention may reflect anticipation of global crop and weather shocks."),
    ("Wildfire", "climate", "Wildfire attention may track insurance losses and utility risk."),
    ("Hurricane", "climate", "Storm attention may precede refinery, logistics and insurance disruptions."),
    ("Suez_Canal", "logistics", "Canal attention may identify global maritime bottleneck concern."),
    ("Panama_Canal", "logistics", "Canal attention may track shipping restrictions and rerouting."),
    ("Strait_of_Hormuz", "geopolitics", "Hormuz attention may track crude and LNG shipping risk."),
    ("Bab-el-Mandeb", "geopolitics", "Chokepoint attention may track Red Sea freight disruption."),
    ("Taiwan_Strait", "geopolitics", "Taiwan Strait attention may track semiconductor and maritime geopolitical risk."),
    ("Container_ship", "logistics", "Container-shipping attention may capture freight-cycle and disruption news."),
    ("Supply_chain", "logistics", "Supply-chain attention may flag broad industrial bottlenecks."),
    ("Strike_action", "labor", "Strike attention may track production and service disruption risk."),
    ("Tariff", "trade_policy", "Tariff attention may accompany import-cost and trade-policy shocks."),
    ("Economic_sanctions", "trade_policy", "Sanctions attention may flag disrupted commodity and capital flows."),
    ("Semiconductor", "technology", "Semiconductor attention may track chip-cycle and supply-chain news."),
    ("Graphics_processing_unit", "technology", "GPU attention may capture AI infrastructure and gaming demand."),
    ("Data_center", "technology", "Data-center attention may track compute, construction and power-demand investment."),
    ("Artificial_intelligence", "technology", "AI attention may track enterprise adoption and compute investment."),
    ("Quantum_computing", "technology", "Quantum attention may track emerging computing investment interest."),
    ("Cyberattack", "security", "Cyberattack attention may capture disruption and security-spending catalysts."),
    ("Ransomware", "security", "Ransomware attention may track corporate risk and security purchasing urgency."),
    ("Data_breach", "security", "Data-breach attention may track security incidents and compliance demand."),
    ("Satellite_Internet_access", "telecom", "Satellite internet attention may track connectivity adoption."),
    ("5G", "telecom", "5G attention may track telecommunications capital spending and adoption."),
    ("Semaglutide", "health", "Semaglutide attention may track obesity-drug demand and consumer behavior shifts."),
    ("Tirzepatide", "health", "Tirzepatide attention may track obesity-treatment adoption."),
    ("Insulin", "health", "Insulin attention may capture drug pricing and access developments."),
    ("Respiratory_syncytial_virus", "health", "RSV attention may capture seasonal health demand and vaccine interest."),
    ("Measles", "health", "Measles attention may flag vaccination and public-health demand."),
    ("Mpox", "health", "Mpox attention may capture outbreak concern and diagnostic demand."),
    ("Cruise_ship", "travel", "Cruise research may provide a travel demand attention signal."),
    ("Travel_insurance", "travel", "Travel-insurance interest may track trip demand and disruption concern."),
    ("Passport", "travel", "Passport research may track planned international travel."),
    ("Timeshare", "travel", "Timeshare interest may reflect leisure property demand."),
    ("Buy_now,_pay_later", "consumer_finance", "Installment-payment attention may track consumer credit adoption."),
    ("Food_delivery", "consumer", "Food-delivery attention may capture platform adoption."),
    ("Online_shopping", "consumer", "E-commerce research may track digital retail demand."),
    ("Used_car", "consumer", "Used-car attention may track vehicle affordability and demand."),
    ("Air_conditioning", "consumer", "Cooling attention may track weather-driven equipment and electricity demand."),
    ("Home_insurance", "insurance", "Home-insurance attention may track premium stress and weather risk."),
    ("Pet_insurance", "insurance", "Pet-insurance attention may track household spending and insurance adoption."),
    ("Sports_betting", "consumer", "Betting attention may track gaming market adoption and regulation."),
    ("Desalination", "water", "Desalination attention may track water-supply infrastructure demand."),
    ("Per-_and_polyfluoroalkyl_substances", "water", "PFAS attention may track remediation spending and legal exposure."),
    ("Carbon_capture_and_storage", "energy_transition", "Carbon-capture attention may track decarbonization investment."),
    ("Geothermal_energy", "energy_transition", "Geothermal attention may capture firm-power investment interest."),
    ("Small_modular_reactor", "energy_transition", "SMR attention may track nuclear project and vendor prospects."),
    ("Solid-state_battery", "technology", "Solid-state battery attention may track energy-storage technology interest."),
    ("Humanoid_robot", "technology", "Robot attention may track automation capital spending interest."),
    ("3D_printing", "technology", "Additive manufacturing attention may track industrial adoption."),
    ("Ozempic", "health", "Brand-specific drug attention may track treatment demand."),
    ("Insurance", "insurance", "Insurance attention may reflect pricing concern and purchase research."),
    ("Commercial_real_estate", "housing", "Commercial-property attention may reflect financing and vacancy risk."),
    ("Private_credit", "credit", "Private-credit attention may reflect capital allocation and liquidity risk."),
]

NPM_PACKAGES = [
    ("openai", "AI API adoption and inference demand"),
    ("@anthropic-ai/sdk", "alternative large-language-model API adoption"),
    ("@google/generative-ai", "Google generative-model ecosystem adoption"),
    ("@huggingface/inference", "open-model inference deployment"),
    ("@tensorflow/tfjs", "browser machine-learning deployment"),
    ("onnxruntime-node", "cross-platform inference deployment"),
    ("langchain", "AI application orchestration adoption"),
    ("@pinecone-database/pinecone", "vector database adoption"),
    ("@qdrant/js-client-rest", "vector-search infrastructure adoption"),
    ("chromadb", "embedding application database adoption"),
    ("aws-sdk", "Amazon cloud application development"),
    ("@google-cloud/storage", "Google cloud storage development"),
    ("@azure/storage-blob", "Azure cloud storage development"),
    ("firebase", "Google mobile and web backend adoption"),
    ("@supabase/supabase-js", "open-source backend platform adoption"),
    ("mongodb", "document database application adoption"),
    ("pg", "PostgreSQL application development"),
    ("mysql2", "MySQL application development"),
    ("redis", "in-memory database application deployment"),
    ("@elastic/elasticsearch", "search and observability platform deployment"),
    ("snowflake-sdk", "cloud data-warehouse application integration"),
    ("@databricks/sql", "lakehouse platform application integration"),
    ("clickhouse", "columnar analytics database adoption"),
    ("stripe", "digital payment integration activity"),
    ("@paypal/checkout-server-sdk", "PayPal checkout integration activity"),
    ("braintree", "Braintree payment integration activity"),
    ("square", "merchant payment and commerce integration activity"),
    ("plaid", "banking-data integration activity"),
    ("twilio", "communications API integration activity"),
    ("@sendgrid/mail", "transactional email platform integration"),
    ("@slack/web-api", "workplace messaging automation adoption"),
    ("discord.js", "community and social-platform bot development"),
    ("@shopify/shopify-api", "merchant platform development activity"),
    ("@sanity/client", "headless content-management adoption"),
    ("contentful", "enterprise content-platform adoption"),
    ("algoliasearch", "hosted search platform adoption"),
    ("@sentry/node", "application monitoring adoption"),
    ("dd-trace", "Datadog application observability adoption"),
    ("newrelic", "New Relic application monitoring adoption"),
    ("@opentelemetry/api", "open observability instrumentation adoption"),
    ("@auth0/auth0-react", "identity-platform integration activity"),
    ("@okta/okta-auth-js", "enterprise identity integration activity"),
    ("@clerk/clerk-react", "developer identity-platform adoption"),
    ("@launchdarkly/node-server-sdk", "enterprise feature-management adoption"),
    ("posthog-js", "product analytics platform adoption"),
    ("mapbox-gl", "commercial mapping platform integration"),
    ("@googlemaps/js-api-loader", "Google Maps application adoption"),
    ("mqtt", "connected-device and industrial messaging development"),
    ("modbus-serial", "industrial automation integration activity"),
    ("serialport", "hardware device integration activity"),
]

TOR_COUNTRIES = {
    "ru": "Russia", "ir": "Iran", "cn": "China", "tr": "Turkey", "vn": "Vietnam",
    "by": "Belarus", "ua": "Ukraine", "pk": "Pakistan", "in": "India", "eg": "Egypt",
    "sa": "Saudi Arabia", "ae": "United Arab Emirates", "bd": "Bangladesh",
    "mm": "Myanmar", "et": "Ethiopia",
}

def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def parse_wikimedia(payload):
    items = json.loads(payload)["items"]
    return pd.DataFrame({
        "date": [pd.to_datetime(i["timestamp"], format="%Y%m%d%H", utc=True) for i in items],
        "value": [i["views"] for i in items],
    })


def parse_npm(payload):
    data = json.loads(payload)["downloads"]
    return pd.DataFrame(data).rename(columns={"day": "date", "downloads": "value"})[["date", "value"]]


def parse_csv(payload, value, filters=None):
    data = pd.read_csv(io.BytesIO(payload), comment="#")
    for key, wanted in (filters or {}).items():
        data = data[data[key].astype(str) == str(wanted)]
    return data[["date", value]].rename(columns={value: "value"}).dropna(subset=["value"])


def parse_grouped_counts(payload, group_field, value):
    rows = pd.DataFrame(json.loads(payload))
    if rows.empty:
        raise ValueError("No source observations")
    return rows.loc[rows[group_field] == value, ["date", "value"]].copy()


def collect_wikipedia(fetcher, start, end, workers=3):
    def one(spec):
        article, theme, hypothesis = spec
        url = ("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
               f"en.wikipedia.org/all-access/user/{quote(article, safe='')}/daily/"
               f"{start.replace('-', '')}00/{end.replace('-', '')}00")
        raw, digest = paced_get(fetcher, url)
        return save_candidate(
            "digital", "wikipedia_" + slug(article) + "_views", parse_wikimedia(raw),
            title=f"Wikipedia human page views: {article.replace('_', ' ')}",
            provider="Wikimedia", category="attention", unit="page views", frequency="daily",
            source_url=url, geography="Global readers of English Wikipedia", hypothesis=hypothesis,
            limitations="Attention proxy, not sentiment, purchases or a proven leading indicator. English-language selection bias; news and education affect traffic. User-classified traffic excludes identified automated requests, not all bots. Redirect-title views are not merged. Current topic selection creates selection bias. Observation date is not historical publication availability.",
            raw_sha256=digest, dataset_identity=f"pageviews/en.wikipedia.org/all-access/user/{article}",
            documentation_url=WIKI_DOCS, native_frequency="daily", market_theme=theme,
            license="Wikimedia Analytics API open data; see documentation")
    return run_jobs("wikipedia", WIKI_TOPICS, one, workers)


def year_windows(start, end):
    cursor = pd.Timestamp(start)
    last = pd.Timestamp(end)
    while cursor <= last:
        stop = min(cursor + pd.Timedelta(days=364), last)
        yield cursor.date().isoformat(), stop.date().isoformat()
        cursor = stop + pd.Timedelta(days=1)


def collect_npm(fetcher, start, end, workers=3):
    # The provider supports up to 128 unscoped packages in one bulk request,
    # avoiding one request per package per year.
    unscoped = [package for package, _ in NPM_PACKAGES if not package.startswith("@")]
    bulk_frames = {p: [] for p in unscoped}
    bulk_hashes, bulk_urls = [], []
    for begin, stop in (year_windows(start, end) if unscoped else []):
        url = f"https://api.npmjs.org/downloads/range/{begin}:{stop}/" + ",".join(unscoped)
        raw, digest = paced_get(fetcher, url)
        obj = json.loads(raw)
        for package in unscoped:
            if not obj.get(package):
                continue
            bulk_frames[package].append(parse_npm(json.dumps(obj[package])))
        bulk_hashes.append(digest)
        bulk_urls.append(url)
    def one(spec):
        package, theme = spec
        frames, hashes, urls = [], [], []
        if package in bulk_frames:
            frames, hashes, urls = bulk_frames[package], bulk_hashes, bulk_urls
        else:
            for begin, stop in year_windows(start, end):
                url = f"https://api.npmjs.org/downloads/range/{begin}:{stop}/{quote(package, safe='')}"
                raw, digest = paced_get(fetcher, url)
                frames.append(parse_npm(raw))
                hashes.append(digest)
                urls.append(url)
        frame = pd.concat(frames, ignore_index=True)
        return save_candidate(
            "digital", "npm_" + slug(package) + "_downloads", frame,
            title=f"npm daily downloads: {package}", provider="npm", category="software_adoption",
            unit="package downloads", frequency="daily", source_url=urls[0], geography="Global",
            hypothesis=f"Developer download activity may help monitor {theme}; test changes against relevant cloud, software, payment or hardware exposures.",
            limitations="Downloads include CI builds, automated installs, dependencies, retries and version changes. They are not unique developers, paying customers, API usage or revenue. Weekend seasonality and changes in package distribution can dominate. Package selection is made today and is not point-in-time.",
            raw_sha256=hashes, dataset_identity=f"npm/downloads/{package}", native_frequency="daily",
            documentation_url=NPM_DOCS, request_urls=urls, package=package, market_theme=theme)
    return run_jobs("npm", NPM_PACKAGES, one, workers)


def collect_tor(fetcher, start, end, workers=3):
    specs = []
    for code, country in TOR_COUNTRIES.items():
        specs.append((f"relay_users_{code}", "userstats-relay-country", {"country": code},
                      "users", {}, f"Estimated Tor relay users: {country}", "estimated users", country))
    specs.extend([
        ("relay_users_global", "userstats-relay-country", {"country": "all"}, "users", {}, "Estimated Tor relay users: global", "estimated users", "Global"),
        ("bridge_users_global", "userstats-bridge-country", {"country": "all"}, "users", {}, "Estimated Tor bridge users: global", "estimated users", "Global"),
        ("advertised_bandwidth", "bandwidth", {}, "advbw", {}, "Tor advertised relay bandwidth", "Gbit/s", "Global"),
        ("consumed_bandwidth", "bandwidth", {}, "bwhist", {}, "Tor consumed relay bandwidth", "Gbit/s", "Global"),
        ("running_relays", "networksize", {}, "relays", {}, "Tor daily average running relays", "relays", "Global"),
        ("running_bridges", "networksize", {}, "bridges", {}, "Tor daily average running bridges", "bridges", "Global"),
    ])
    for direction in ("read", "write", "both"):
        specs.append((f"connections_{direction}", "connbidirect", {}, "md", {"direction": direction},
                      f"Tor median fraction of connections: {direction}", "fraction", "Global"))
    for transport in ("obfs4", "snowflake", "webtunnel"):
        specs.append((f"bridge_users_{transport}", "userstats-bridge-transport", {"transport": transport},
                      "users", {"transport": transport}, f"Estimated Tor bridge users: {transport}",
                      "estimated users", "Global"))
    # Cache avoids repeated HTTP requests for metrics in the same wide table.
    shared = {}
    for _, endpoint, params, _, _, _, _, _ in specs:
        key = (endpoint, tuple(sorted(params.items())))
        if key not in shared:
            url = f"https://metrics.torproject.org/{endpoint}.csv"
            try:
                raw, digest = paced_get(fetcher, url, {"start": start, "end": end, **params})
                shared[key] = (raw, digest, url)
            except Exception as exc:
                shared[key] = exc
    def one(spec):
        sid, endpoint, params, col, filters, title, unit, geography = spec
        result = shared[(endpoint, tuple(sorted(params.items())))]
        if isinstance(result, Exception):
            raise result
        raw, digest, url = result
        return save_candidate(
            "digital", "tor_" + sid, parse_csv(raw, col, filters),
            title=title, provider="Tor Project", category="internet_resilience", unit=unit,
            frequency="daily", source_url=url, geography=geography,
            hypothesis="Internet censorship, outages and avoidance activity may flag geopolitical disruption, country risk and demand for security/connectivity services. Network metrics help distinguish adoption from capacity problems.",
            limitations="Indirect proxy with no validated trading edge. Tor user counts are statistical estimates, not a census; IP geolocation, reporting fractions and protocol changes can create breaks. Privacy activity has many benign causes. Missing source dates remain absent. Some browser series are discontinued; inspect actual coverage.",
            raw_sha256=digest, dataset_identity=f"Tor/{endpoint}/{params}/{col}/{filters}",
            documentation_url=TOR_DOCS, native_frequency="daily", source_parameters={"start": start, "end": end, **params})
    return run_jobs("tor", specs, one, workers)


def collect_city(fetcher, start, end, city):
    """Aggregate source database rows by calendar date and service type.

    The server returns actual daily counts. We retain only returned dates and do
    not turn an absent date into a zero. Neither request addresses nor personal
    information is requested.
    """
    if city == "nyc":
        url, group_field = "https://data.cityofnewyork.us/resource/erm2-nwe9.json", "complaint_type"
        provider, geography = "NYC 311", "New York City, United States"
        chosen = [
            "HEAT/HOT WATER", "PLUMBING", "WATER LEAK", "Water System", "Sewer",
            "Street Condition", "Street Light Condition", "Traffic Signal Condition",
            "Construction Lead Dust", "Noise - Commercial", "Noise - Street/Sidewalk",
            "Noise - Helicopter", "Noise - Vehicle", "Illegal Parking", "Blocked Driveway",
            "Rodent", "Dirty Condition", "Missed Collection", "Sanitation Worker or Vehicle Complaint",
            "Food Establishment", "Food Poisoning", "Air Quality", "Lead",
            "Elevator", "ELECTRIC", "Non-Emergency Police Matter", "Building/Use",
            "Boilers", "General Construction/Plumbing", "Consumer Complaint",
        ]
    else:
        url, group_field = "https://data.cityofchicago.org/resource/v6vf-nfxy.json", "sr_type"
        provider, geography = "Chicago 311", "Chicago, Illinois, United States"
        chosen = [
            "Pothole in Street Complaint", "Street Light Out Complaint", "Alley Light Out Complaint",
            "Street Light Pole Damage Complaint", "Traffic Signal Out Complaint", "Water On Street Complaint",
            "Water in Basement Complaint", "Sewer Cave-In Inspection Request", "No Water Complaint",
            "Low Water Pressure Complaint", "Rodent Baiting/Rat Complaint", "Garbage Cart Maintenance",
            "Missed Garbage Pick-Up Complaint", "Fly Dumping Complaint", "Graffiti Removal Request",
            "Vacant/Abandoned Building Complaint", "Tree Debris Clean-Up Request", "Tree Emergency",
            "Building Violation", "Restaurant Complaint",
        ]
    params = {
        "$select": f"date_trunc_ymd(created_date) as date,{group_field},count(*) as value",
        "$where": f"created_date >= '{start}T00:00:00' AND created_date < '{(pd.Timestamp(end) + pd.Timedelta(days=1)).date()}T00:00:00'",
        "$group": f"date,{group_field}", "$order": "date", "$limit": 500000,
    }
    raw, digest = fetcher.get(url, params, timeout=180)
    all_rows = json.loads(raw)
    if len(all_rows) >= params["$limit"]:
        raise ValueError("City query hit limit; refusing truncated history")
    available = {r[group_field] for r in all_rows if r.get(group_field)}
    missing = sorted(set(chosen) - available)
    print(f"{city}: {len(all_rows):,} daily groups; {len(available)} service types; missing selections={missing}", flush=True)
    def one(service):
        frame = parse_grouped_counts(raw, group_field, service)
        return save_candidate(
            "digital", city + "_311_" + slug(service), frame,
            title=f"{provider} requests: {service}", provider=provider, category="urban_activity",
            unit="service requests", frequency="daily", source_url=url, geography=geography,
            hypothesis=f"Daily {service.lower()} requests may proxy infrastructure stress, weather impacts, property maintenance, commercial activity or local disruption relevant to utilities, insurers, real estate and services.",
            limitations="Complaints measure reporting, not objective prevalence. App adoption, agency routing, duplicate requests, classification changes, enforcement and weather can change counts. Local activity does not represent national activity. Returned dates only; absent dates are not zero-filled. Current historical database can be revised.",
            raw_sha256=digest, dataset_identity=f"{url}/{group_field}={service}",
            native_frequency="event-level floating timestamps", source_parameters=params, service_type=service,
            finer_frequency_available=True,
            aggregation="Source-side count of service requests by created_date calendar day and exact service type; no missing-day filling.",
            source_timezone="Unspecified offset in the source floating_timestamp field; source calendar dates retained.",
            timestamp_semantics="Source calendar day represented by UTC midnight as a date label, not UTC event time or availability time. Raw created_date has no UTC offset; source local hours around daylight-saving changes cannot be uniquely resolved, so the published file uses daily counts.",
            timestamp_documentation_url="https://dev.socrata.com/docs/datatypes/floating_timestamp.html",
            documentation_url=url.replace("/resource/", "/d/").replace(".json", ""))
    return run_jobs(city, chosen, one, 1)


def run_jobs(provider, specs, one, workers):
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        jobs = {pool.submit(one, spec): spec for spec in specs}
        for future in as_completed(jobs):
            spec = jobs[future]
            try:
                meta = future.result()
                results.append(meta)
                print(f"OK {meta['series_id']} {meta['rows']} rows {meta['start'][:10]}..{meta['end'][:10]}", flush=True)
            except Exception as exc:
                error = {"provider": provider, "candidate": str(spec), "error": str(exc)}
                errors.append(error)
                print(f"FAIL {error}", flush=True)
    return results, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=["wikipedia", "npm", "tor", "nyc", "chicago", "all"], default="all")
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default=(pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=2)).date().isoformat())
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    fetcher = Fetcher(refresh=args.refresh)
    results, errors = [], []
    for provider in ["wikipedia", "npm", "tor", "nyc", "chicago"]:
        if args.provider not in ("all", provider):
            continue
        try:
            if provider in ("nyc", "chicago"):
                good, bad = collect_city(fetcher, args.start, args.end, provider)
            else:
                func = globals()["collect_" + provider]
                good, bad = func(fetcher, args.start, args.end, args.workers)
            results.extend(good)
            errors.extend(bad)
        except Exception as exc:
            errors.append({"provider": provider, "error": str(exc)})
            print(f"FAIL provider={provider}: {exc}", flush=True)
    report = {"created_at": pd.Timestamp.now(tz="UTC").isoformat(), "provider": args.provider,
              "successful_series": [m["series_id"] for m in results], "count": len(results), "errors": errors}
    atomic_bytes(ROOT / "archive" / "digital_reports" / f"{args.provider}.json", json.dumps(report, indent=2).encode())
    print(json.dumps({"count": len(results), "errors": len(errors)}), flush=True)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
