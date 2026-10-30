#!/usr/bin/env python3
"""Real public activity histories: grid, blockchain, mobility, building and health.

Only observed values are saved. This script stages candidates; library.py publishes
the user-requested batches. Run --provider NAME to retry one independent family.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import time
from urllib.parse import urlencode

import pandas as pd

from expansion_common import Fetcher, ROOT, atomic_bytes, save_candidate


START = "2024-01-01"
END = (pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)).date().isoformat()
COLLECTION_ERRORS = []


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def record_failure(provider, error):
    COLLECTION_ERRORS.append({"provider": provider, "error": str(error)})
    print(f"FAIL {provider}: {error}", flush=True)


def clean_frame(dates, values):
    """Omit explicit nulls; don't fabricate dates or fill gaps."""
    frame = pd.DataFrame({"date": dates, "value": values})
    frame["value"] = pd.to_numeric(frame["value"], errors="raise")
    return frame.dropna(subset=["value"])


def remove_ambiguous_duplicates(frame):
    """Keep exact duplicate rows once; omit conflicting time labels entirely."""
    differing = frame.groupby("date")["value"].nunique()
    conflicting = differing[differing > 1].index
    return frame[~frame["date"].isin(conflicting)].drop_duplicates("date"), len(conflicting)


def save(sid, frame, **meta):
    if frame["value"].nunique() < 2:
        raise ValueError(f"{sid}: constant series omitted")
    result = save_candidate("activity", sid, frame, **meta)
    print(f"OK {sid}: {result['rows']} rows {result['start'][:10]}..{result['end'][:10]}", flush=True)
    return result


ENERGY_TYPES = {
    "lignite": "Fossil brown coal / lignite", "coal": "Fossil hard coal",
    "gas": "Fossil gas", "nuclear": "Nuclear", "wind_onshore": "Wind onshore",
    "wind_offshore": "Wind offshore", "solar": "Solar", "river_hydro": "Hydro Run-of-River",
    "reservoir_hydro": "Hydro water reservoir", "pumped_hydro": "Hydro pumped storage",
    "biomass": "Biomass", "geothermal": "Geothermal", "load": "Load",
}
ENERGY = {
    "de": ("Germany", "lignite coal gas wind_onshore wind_offshore solar load"),
    "fr": ("France", "nuclear gas river_hydro reservoir_hydro solar wind_onshore load"),
    "uk": ("Great Britain", "gas nuclear wind_onshore wind_offshore solar load"),
    "es": ("Spain", "nuclear gas reservoir_hydro wind_onshore solar load"),
    "it": ("Italy", "gas geothermal river_hydro solar wind_onshore load"),
    "nl": ("Netherlands", "gas coal wind_offshore solar load"),
    "pl": ("Poland", "lignite coal wind_onshore solar gas load"),
    "se": ("Sweden", "nuclear reservoir_hydro wind_onshore load"),
    "no": ("Norway", "reservoir_hydro river_hydro wind_onshore load"),
    "fi": ("Finland", "nuclear biomass wind_onshore load"),
    "at": ("Austria", "river_hydro pumped_hydro gas biomass load"),
    "dk": ("Denmark", "wind_offshore wind_onshore solar biomass load"),
    "ie": ("Ireland", "wind_onshore gas load"),
    "pt": ("Portugal", "reservoir_hydro wind_onshore solar gas load"),
    "gr": ("Greece", "lignite gas wind_onshore solar load"),
    "ro": ("Romania", "nuclear reservoir_hydro wind_onshore solar load"),
    "bg": ("Bulgaria", "nuclear lignite solar load"),
    "ch": ("Switzerland", "nuclear reservoir_hydro pumped_hydro load"),
    "hu": ("Hungary", "nuclear gas solar load"),
}


def parse_energy(payload, wanted):
    dates = pd.to_datetime(payload["unix_seconds"], unit="s", utc=True)
    available = {x["name"]: x["data"] for x in payload["production_types"]}
    result = {}
    for metric in wanted.split():
        name = ENERGY_TYPES[metric]
        if name not in available:
            continue
        values = available[name]
        if len(dates) != len(values):
            raise ValueError("Energy-Charts timestamp/value lengths differ")
        frame = clean_frame(dates, values)
        if len(frame) < 2:
            continue
        # The API can combine historic hourly and newer quarter-hour values.
        seconds = frame["date"].sort_values().diff().dt.total_seconds().dropna()
        interval = int(seconds.median())
        frequency = {900: "15min", 1800: "30min", 3600: "hourly"}.get(interval, "irregular_intraday")
        result[metric] = (frame, frequency, sorted(set(seconds.astype(int))))
    return result


def energy_country(client, country):
    geography, wanted = ENERGY[country]
    url = "https://api.energy-charts.info/public_power"
    params = {"country": country, "start": START, "end": END}
    content, digest = client.get(url, params, timeout=240)
    payload = json.loads(content)
    output = []
    for metric, (frame, freq, intervals) in parse_energy(payload, wanted).items():
        hypothesis = (
            "Electricity demand tracks industrial and household activity, affecting utility dispatch, gas burn and power prices."
            if metric == "load" else
            f"{ENERGY_TYPES[metric]} output affects regional generation scarcity, fuel demand, carbon allowance demand and exposed utility margins."
        )
        output.append(save(
            f"energy_charts_{country}_{metric}", frame,
            title=f"{geography}: {ENERGY_TYPES[metric]} power",
            provider="fraunhofer_energy_charts", category="electricity", unit="MW", frequency=freq,
            source_url=url + "?" + urlencode(params), geography=geography, hypothesis=hypothesis,
            limitations="Fraunhofer ISE Energy-Charts public net electricity production/load. MW measures power, not interval MWh. Source interval-start timestamps retained in UTC; intervals may change historically. Values can be revised and reporting gaps remain missing. Backfilled history does not establish original publication time.",
            raw_sha256=digest, dataset_identity=f"public_power/{country}/{ENERGY_TYPES[metric]}",
            native_frequency=freq, observed_intervals_seconds=intervals,
            documentation_url="https://api.energy-charts.info/", license="CC BY 4.0; attribute Energy-Charts / Fraunhofer ISE",
        ))
    return output


def collect_energy(client):
    output = []
    # Long histories are multi-MB responses. The public API applies a strict
    # quota, so serialize countries and space requests rather than flooding it.
    for country in ENERGY:
        try:
            output.extend(energy_country(client, country))
        except Exception as exc:
            record_failure(f"energy/{country}", exc)
        time.sleep(12)
    return output


NETWORK_ASSETS = {
    "btc": "Bitcoin", "eth": "Ethereum", "sol": "Solana", "ada": "Cardano",
    "avaxc": "Avalanche C-Chain", "ltc": "Litecoin", "doge": "Dogecoin",
    "xrp": "XRP Ledger", "trx": "Tron", "xmr": "Monero", "bch": "Bitcoin Cash", "xtz": "Tezos",
}
METRIC_HYPOTHESIS = {
    "AdrActCnt": "Active ledger addresses proxy network usage and potential fee/token demand; an address is not a distinct person.",
    "TxCnt": "Transaction throughput measures network activity and congestion, potentially affecting fee demand and infrastructure valuations.",
    "FeeTotNtv": "Native-unit transaction fees measure willingness to pay for blockspace and network validator or miner economics.",
    "HashRate": "Mining computation affects security, miner costs, electricity consumption and potential miner selling pressure.",
    "SplyCur": "Stablecoin native-unit supply tracks digital-dollar liquidity and may lead crypto collateral and trading demand.",
}


def coinmetrics_asset(client, asset, name, wanted, catalog, catalog_digest):
    metrics = []
    for metric in wanted:
        entry = catalog.get(metric)
        if entry and any(f["frequency"] == "1d" and asset in f.get("assets", []) for f in entry["frequencies"]):
            metrics.append(metric)
    if not metrics:
        return []
    url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
    params = {"assets": asset, "metrics": ",".join(metrics), "frequency": "1d",
              "start_time": START, "end_time": END, "page_size": 10000}
    content, digest = client.get(url, params)
    payload = json.loads(content)
    if payload.get("next_page_url"):
        raise ValueError("Unexpected pagination; do not silently truncate Coin Metrics")
    records = pd.DataFrame(payload["data"])
    output = []
    for metric in metrics:
        if metric not in records:
            continue
        entry = catalog[metric]
        frame = clean_frame(records["time"], records[metric])
        unit = entry["unit"]
        if metric == "HashRate":
            # Official asset-specific units; hash rates are not comparable
            # across mining algorithms.
            unit = {"btc": "TH/s", "ltc": "TH/s", "xmr": "MH/s"}[asset]
        elif unit == "Native units":
            unit = f"{asset} native units"
        output.append(save(
            f"coinmetrics_{asset}_{slug(metric)}", frame,
            title=f"{name}: {entry['full_name']}", provider="coin_metrics",
            category="blockchain_activity", unit=unit,
            frequency="daily", source_url=url + "?" + urlencode(params), geography=f"Global / {name}",
            hypothesis=METRIC_HYPOTHESIS[metric],
            limitations=entry["description"] + " Community coverage and methodologies differ by network. Bots, self-transfers and protocol changes may alter counts. Backfilled data can be revised and does not prove real-time predictability.",
            raw_sha256=[digest, catalog_digest], dataset_identity=f"{asset}/{metric}/1d", native_frequency="daily",
            documentation_url="https://docs.coinmetrics.io/api/v4/", license="Coin Metrics Community Creative Commons terms",
            unit_documentation_url="https://docs.coinmetrics.io/asset-metrics/mining/hashrate" if metric == "HashRate" else "https://docs.coinmetrics.io/api/v4/",
        ))
    return output


def collect_coinmetrics(client):
    raw, digest = client.get("https://community-api.coinmetrics.io/v4/catalog/asset-metrics")
    catalog = {x["metric"]: x for x in json.loads(raw)["data"]}
    output = []
    assets = [(asset, name, ["AdrActCnt", "TxCnt", "FeeTotNtv"] + (["HashRate"] if asset in {"btc", "ltc", "xmr"} else []))
              for asset, name in NETWORK_ASSETS.items()]
    assets += [(asset, name, ["SplyCur"]) for asset, name in [("usdt", "Tether"), ("usdc", "USD Coin"), ("dai", "DAI")]]
    for asset, name, metrics in assets:
        try:
            output.extend(coinmetrics_asset(client, asset, name, metrics, catalog, digest))
        except Exception as exc:
            record_failure(f"coinmetrics/{asset}", exc)
        time.sleep(0.7)  # documented community quota: ten requests per six seconds
    return output


PROTOCOLS = {
    "aave-v3": "collateralized lending", "lido": "liquid staking",
    "uniswap-v3": "automated market making", "eigenlayer": "restaking",
    "makerdao": "collateralized stablecoin issuance", "polymarket": "prediction-market collateral",
    "maple-finance": "institutional on-chain credit", "pendle": "yield tokenization",
    "curve-dex": "stable-asset liquidity", "gmx": "perpetual futures liquidity",
    "convex-finance": "liquidity incentives", "yearn-finance": "yield aggregation",
    "rocket-pool": "decentralized liquid staking", "ethena": "synthetic-dollar collateral",
    "justlend": "Tron lending", "venus": "BNB lending", "kamino": "Solana lending and liquidity",
    "jito": "Solana staking", "benqi": "Avalanche lending", "stargate": "cross-chain liquidity",
    "balancer-v2": "weighted liquidity pools", "nexus-mutual": "on-chain insurance capital",
    "flexa": "merchant payment collateral", "liquity": "decentralized stablecoin borrowing",
    "goldfinch": "uncollateralized real-world borrower credit",
}


def defillama_protocol(client, protocol, thesis):
    url = "https://api.llama.fi/protocol/" + protocol
    content, digest = client.get(url, timeout=120)
    payload = json.loads(content)
    history = payload["tvl"]
    frame = clean_frame(pd.to_datetime([r["date"] for r in history], unit="s", utc=True),
                        [r["totalLiquidityUSD"] for r in history])
    # Daily history endpoints sometimes append a current intraday observation.
    # Preserve its actual timestamp and only source duplicate timestamps are removed
    # if their values are exactly equal.
    if frame["date"].duplicated().any():
        if frame.groupby("date")["value"].nunique().max() > 1:
            raise ValueError("Conflicting DefiLlama timestamp duplicates")
        frame = frame.drop_duplicates("date")
    frame = frame[frame["date"] >= pd.Timestamp(START, tz="UTC")]
    return [save(
        f"defillama_{slug(protocol)}_tvl", frame,
        title=f"{payload.get('name', protocol)}: total value locked", provider="defillama",
        category="decentralized_finance", unit="USD", frequency="daily", source_url=url,
        geography="Global / " + ", ".join(payload.get("chains", [])),
        hypothesis=f"Capital committed to {thesis} may indicate demand, risk appetite, collateral stress and activity relevant to tokens and crypto intermediaries.",
        limitations="DefiLlama adapter-estimated TVL is the USD value of assets in the protocol, not net capital inflow, revenue or independent users. Token price changes, adapter revisions, migrations and double counting can affect levels. Daily snapshots and a possible final intraday observation are preserved; no historical availability timestamps.",
        raw_sha256=digest, dataset_identity=f"protocol/{protocol}/tvl", native_frequency="daily",
        protocol_category=payload.get("category"), protocol_business_model=thesis,
        documentation_url="https://api-docs.defillama.com/",
    )]


def collect_defillama(client):
    output = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = {pool.submit(defillama_protocol, client, p, thesis): p for p, thesis in PROTOCOLS.items()}
        for future in as_completed(jobs):
            try:
                output.extend(future.result())
            except Exception as exc:
                record_failure(f"defillama/{jobs[future]}", exc)
    return output


def socrata(client, domain, dataset, params):
    url = f"https://{domain}/resource/{dataset}.json"
    params = {**params, "$limit": 50000}
    raw, digest = client.get(url, params, timeout=180)
    data = json.loads(raw)
    if len(data) >= 50000:
        raise ValueError(f"{dataset}: pagination required, refusing truncated query")
    return pd.DataFrame(data), digest, url + "?" + urlencode(params)


def collect_transit(client):
    output = []
    frame, digest, url = socrata(client, "data.ny.gov", "sayj-mze2",
                                 {"$where": f"date >= '{START}T00:00:00'", "$order": "date,mode"})
    mode_names = {
        "AAR": ("Access-A-Ride", "trips"), "BT": ("Bridges and Tunnels", "vehicles"),
        "Bus": ("Buses", "rides"), "Subway": ("Subway", "rides"),
        "SIR": ("Staten Island Railway", "rides"), "LIRR": ("Long Island Rail Road", "rides"),
        "MNR": ("Metro-North Railroad", "rides"), "CBD Entries": ("Central Business District entries", "vehicles"),
        "CRZ Entries": ("Congestion Relief Zone entries", "vehicles"),
    }
    for mode, part in frame.groupby("mode"):
        name, unit = mode_names[mode]
        output.append(save(
            "mta_" + slug(mode) + "_daily_ridership", clean_frame(part["date"], part["count"]),
            title=f"MTA {name}: daily {'traffic' if unit == 'vehicles' else 'ridership'}", provider="mta", category="mobility",
            unit=unit, frequency="daily", source_url=url,
            geography="New York metropolitan area, US",
            hypothesis="Commuter, visitor and vehicle flows proxy office attendance, retail demand, travel activity and road fuel consumption.",
            limitations="Official daily estimates. Mode definitions, estimation methods and congestion-zone reporting can change. Bridge/tunnel and congestion-zone counts are vehicles; transit modes count rides or trips. Published retrospectively; date is local service day, represented at UTC midnight.",
            raw_sha256=digest, dataset_identity=f"sayj-mze2/{mode}/count", native_frequency="daily",
            documentation_url="https://data.ny.gov/d/sayj-mze2",
        ))
    frame, digest, url = socrata(client, "data.cityofchicago.org", "6iiy-9s97",
                                 {"$where": f"service_date >= '{START}T00:00:00'", "$order": "service_date"})
    for metric, title in [("bus", "bus boardings"), ("rail_boardings", "rail station boardings")]:
        output.append(save(
            "chicago_cta_" + metric, clean_frame(frame["service_date"], frame[metric]),
            title="Chicago CTA: " + title, provider="chicago_transit_authority", category="mobility",
            unit="boardings", frequency="daily", source_url=url, geography="Chicago, US",
            hypothesis="Transit boardings proxy commuting, tourism and urban consumer demand relevant to commercial real estate and local spending.",
            limitations="Counts boardings rather than distinct people. Service disruptions and fares affect usage. Seasonal and holiday effects are material. Daily service date is a local day label; source updates lag observation dates.",
            raw_sha256=digest, dataset_identity="6iiy-9s97/" + metric, native_frequency="daily",
        ))
    return output


BIKE_DATASETS = {
    "65db-xm6k": "Fremont Bridge", "2z5v-ecg8": "Burke Gilman Trail",
    "4qej-qvrz": "Elliott Bay Trail", "t8i6-tipf": "Thomas Street Overpass",
    "avwm-i8ym": "2nd Avenue Cycle Track", "upms-nr8w": "Spokane Street Bridge",
    "u38e-ybnc": "Mountains to Sound Trail", "m2yw-err8": "12th Avenue South",
    "4hd3-wm3h": "7th Avenue",
}


def bike_dataset(client, dataset, title):
    # Metadata reveals native column labels; select combined bicycle/pedestrian
    # counters, avoiding the mechanically redundant pair of direction columns.
    metadata_raw, metadata_digest = client.get(f"https://data.seattle.gov/api/views/{dataset}.json")
    metadata = json.loads(metadata_raw)
    columns = [(x["fieldName"], x["name"]) for x in metadata["columns"] if x.get("dataTypeName") == "number"]
    dates = [(x["fieldName"], x["name"]) for x in metadata["columns"] if x.get("dataTypeName") == "calendar_date"]
    if len(dates) != 1:
        raise ValueError("Bike counter date field is ambiguous")
    date_col = dates[0][0]
    frame, digest, url = socrata(client, "data.seattle.gov", dataset,
                                 {"$where": f"{date_col} >= '{START}T00:00:00'", "$order": date_col})
    # Timestamps are floating local clock readings, with no UTC offset for the
    # repeated DST hour. Drop only those ambiguous/nonexistent hours explicitly.
    local = pd.to_datetime(frame[date_col])
    utc = local.dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    pedestrian_fields = [field for field, label in columns if re.search(r"\bped", label, re.I)]
    bicycle_fields = [field for field, label in columns if re.search(r"\b(bike|bicycl)", label, re.I)]
    if pedestrian_fields and all(field in frame for field in pedestrian_fields + bicycle_fields):
        # Mixed-use trails have distinct pedestrian and bicycle counters.
        # Sum directions within each mode, and omit the redundant total.
        candidates = []
        for mode, selected in [("pedestrians", pedestrian_fields), ("bicycles", bicycle_fields)]:
            if len(selected) != 2:
                raise ValueError(f"Expected two directions for {mode}: {selected}")
            field = "_" + mode + "_both_directions"
            frame[field] = frame[selected].apply(pd.to_numeric).sum(axis=1, min_count=2)
            candidates.append((field, mode + " both directions"))
    else:
        # These datasets declare their total counter as the first number column;
        # validate its name rather than accidentally counting directions as
        # independent indicators.
        field, label = columns[0]
        if any(word in label.lower() for word in ["northbound", "southbound", "eastbound", "westbound"]):
            raise ValueError("First bike counter column is directional")
        candidates = [(field, label)]
    output = []
    for field, label in candidates:
        if field not in frame:
            continue
        parsed = clean_frame(utc, frame[field]).dropna(subset=["date"])
        parsed, conflicts = remove_ambiguous_duplicates(parsed)
        metric = "pedestrians" if field == "_pedestrians_both_directions" else (
            "bicycles" if field == "_bicycles_both_directions" else "passages")
        measure = {"pedestrians": "pedestrian passages", "bicycles": "bicycle passages", "passages": "counter passages"}[metric]
        output.append(save(
            f"seattle_{slug(title)}_{metric}", parsed,
            title=f"Seattle {title}: {measure}", provider="seattle_department_of_transportation",
            category="mobility", unit="counter passages", frequency="hourly", source_url=url,
            geography="Seattle, Washington, US",
            hypothesis="Hourly active travel tracks office attendance, recreation and neighborhood activity, potentially informing sports retail, urban property and consumer demand.",
            limitations="Automatic counter passages, not unique people. Bicycle counts include scooters where source labels specify this; combined counters may include pedestrians. Original column labels are retained in metadata. Sensor outages and classification error are possible; weather is a major confounder. Seattle publishes reviewed data monthly, so hourly granularity does not mean hourly publication. Ambiguous/nonexistent Pacific DST clock hours and timestamps with conflicting source duplicate values are omitted rather than guessed. Exact duplicate observations are kept once. No missing-hour or missing-day filling.",
            raw_sha256=[digest, metadata_digest], dataset_identity=f"{dataset}/{field}", native_frequency="hourly",
            conflicting_timestamps_omitted=conflicts,
            source_column_labels=dict(columns),
            documentation_url="https://seattle.gov/transportation/projects-and-programs/programs/bike-program/bike-counters",
        ))
    return output


def collect_bikes(client):
    output = []
    for dataset, title in BIKE_DATASETS.items():
        try:
            output.extend(bike_dataset(client, dataset, title))
        except Exception as exc:
            record_failure(f"bikes/{dataset}", exc)
    return output


def collect_permits(client):
    output = []
    configs = [
        ("nyc", "New York City", "data.cityofnewyork.us", "rbx6-tga4", "issued_date", "work_type"),
        ("chicago", "Chicago", "data.cityofchicago.org", "ydr8-5enu", "issue_date", "permit_type"),
    ]
    for city, title, domain, dataset, date_col, type_col in configs:
        params = {"$select": f"date_trunc_ymd({date_col}) as date,{type_col} as kind,count(*) as value",
                  "$where": f"{date_col} >= '{START}T00:00:00' AND {date_col} < '{END}T00:00:00'",
                  "$group": f"date_trunc_ymd({date_col}),{type_col}", "$order": "date,kind"}
        try:
            frame, digest, url = socrata(client, domain, dataset, params)
        except Exception as exc:
            record_failure(f"permits/{city}", exc)
            continue
        for kind, part in frame.groupby("kind"):
            try:
                output.append(save(
                    f"{city}_permits_{slug(kind)}", clean_frame(part["date"], part["value"]),
                    title=f"{title}: {kind} permits issued", provider=f"{city}_department_of_buildings",
                    category="construction", unit="issued permit records", frequency="business_daily",
                    source_url=url, geography=f"{title}, US",
                    hypothesis="Issued permits by construction trade may lead demand for building materials, equipment rental, contractors and property investment.",
                    limitations="Count of source permit records, including source-defined renewals or amendments; not unique projects, starts or completions. Days without records are omitted, not fabricated as zero. Weekends and administrative batch processing affect daily counts. Permitting platform migrations can cause structural breaks; original publication time is unavailable.",
                    raw_sha256=digest, dataset_identity=f"{dataset}/{kind}/daily_count",
                    native_frequency="event records aggregated by local issue date",
                ))
            except ValueError as exc:
                print(f"SKIP permits/{city}/{kind}: {exc}", flush=True)
    return output


HEALTH_METRICS = {
    "patients_currently": "Current COVID hospital patients",
    "patients_newly_admitted": "New COVID hospital admissions",
    "patients_discharged": "COVID hospital discharges",
    "patients_currently_in_icu": "Current COVID ICU patients",
    "patients_expired": "Hospital COVID deaths",
    "total_staffed_beds": "Total staffed hospital beds",
    "total_staffed_beds_currently": "Available staffed hospital beds",
    "total_staffed_icu_beds": "Total staffed ICU beds",
    "total_staffed_icu_beds_2": "Available staffed ICU beds",
    "total_new_admissions_reported": "All-cause new hospital admissions",
}


def collect_health(client):
    fields = ",".join(f"sum({field}) as v_{field},count({field}) as n_{field}" for field in HEALTH_METRICS)
    params = {"$select": "as_of_date as date," + fields,
              "$where": "as_of_date >= '2021-01-01T00:00:00'",
              "$group": "as_of_date", "$order": "as_of_date"}
    frame, digest, url = socrata(client, "health.data.ny.gov", "jw46-jpb7", params)
    output = []
    for field, title in HEALTH_METRICS.items():
        try:
            valid = pd.to_numeric(frame["n_" + field]) > 0
            output.append(save(
                f"ny_health_{field}", clean_frame(frame.loc[valid, "date"], frame.loc[valid, "v_" + field]),
                title="New York State: " + title, provider="new_york_state_department_of_health",
                category="healthcare", unit="beds" if "beds" in field else "patients",
                frequency="daily", source_url=url, geography="New York State, US",
                hypothesis="Hospital demand and capacity may affect staffing costs, medical supplies, pharmaceutical demand and local labor availability.",
                limitations="Archived daily HERDS dataset ending October 2025 when reporting switched to weekly. Statewide sum over reporting facilities, not a fixed hospital panel. Missing facility entries are excluded, not imputed. Reporting coverage and definitions change; weekends/holidays may be absent. Preliminary historical values can be revised. COVID patients are lab-confirmed; all-cause admissions are separately identified.",
                raw_sha256=digest, dataset_identity=f"jw46-jpb7/statewide/sum({field})", native_frequency="daily",
                reporting_facilities_min=int(pd.to_numeric(frame.loc[valid, "n_" + field]).min()),
                reporting_facilities_max=int(pd.to_numeric(frame.loc[valid, "n_" + field]).max()),
                discontinued=True, reporting_status="Daily dataset archived October 7, 2025; successor weekly data not counted as daily",
                documentation_url="https://health.data.ny.gov/d/jw46-jpb7",
            ))
        except ValueError as exc:
            print(f"SKIP health/{field}: {exc}", flush=True)
    return output


COLLECTORS = {
    "energy": collect_energy, "coinmetrics": collect_coinmetrics, "defillama": collect_defillama,
    "transit": collect_transit, "bikes": collect_bikes, "permits": collect_permits, "health": collect_health,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=list(COLLECTORS), action="append")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    client = Fetcher(refresh=args.refresh)
    output = []
    COLLECTION_ERRORS.clear()
    for provider in args.provider or list(COLLECTORS):
        try:
            records = COLLECTORS[provider](client)
            output.extend(records)
            print(f"DONE {provider}: {len(records)} series", flush=True)
        except Exception as exc:
            record_failure(provider, exc)
    failures = list(COLLECTION_ERRORS)
    report = {"series": len(output), "providers": args.provider or list(COLLECTORS), "failures": failures,
              "created_at": pd.Timestamp.now(tz="UTC").isoformat(), "series_ids": [x["series_id"] for x in output]}
    atomic_bytes(ROOT / "archive/staging/activity/collection_report.json", json.dumps(report, indent=2).encode())
    print(json.dumps(report, indent=2))
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
