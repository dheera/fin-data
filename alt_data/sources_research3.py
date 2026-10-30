#!/usr/bin/env python3
"""Daily fiscal/filing activity, financing, software development and marine conditions."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import time
import zipfile

import pandas as pd

from expansion_common import ROOT, Fetcher, atomic_bytes, coverage, normalize_frame, qualifies_daily, save_candidate
from sources_macro2 import SEC_FORMS, canonical_sec_records, parse_sec_index
from sources_activity2 import PORT_ROOT, arcgis_query

GROUP = "research3"
START = "2024-01-01"


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def baseline_identities():
    if not (ROOT / "catalog.parquet").exists():
        return set()
    catalog = pd.read_parquet(ROOT / "catalog.parquet")
    return set(catalog.loc[catalog.batch < 5, "dataset_identity"].dropna())


def clean(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                          "value": pd.to_numeric(values, errors="raise")}).dropna()
    if frame.empty:
        return frame
    if (frame.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting observations at the same timestamp")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    if frame.empty or frame.value.nunique() < 2:
        return None
    if not qualifies_daily(dict(coverage(frame), frequency=meta["frequency"])):
        return None
    result = save_candidate(GROUP, sid, frame, **meta)
    print("OK", sid, result["rows"], flush=True)
    return result


def keep(result, item):
    if item:
        result.append(item)


def ecb_curves(fetch):
    # Annual maturity knots plus short-end benchmarks, not every monthly point
    # on the same fitted smooth curve.
    maturities = ["3M", "6M", "9M"] + [f"{year}Y" for year in range(1, 31)]
    measures = [f"{kind}_{tenor}" for kind in ["SR", "IF", "PY"] for tenor in maturities]
    url = "https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM." + "+".join(measures)
    params = {"format": "csvdata", "startPeriod": START}
    raw, digest = fetch.get(url, params, timeout=240)
    data = pd.read_csv(io.BytesIO(raw))
    result = []
    for key, frame in data.groupby("KEY"):
        meta = frame.iloc[-1]
        if meta.FREQ != "B" or meta.UNIT != "PCPA" or meta.UNIT_MULT != 0:
            raise ValueError("Unexpected ECB frequency or units")
        values = clean(frame.TIME_PERIOD, frame.OBS_VALUE)
        keep(result, save(
            "ecb_curve_" + slug(meta.INSTRUMENT_FM + "_" + meta.DATA_TYPE_FM), values,
            title=str(meta.TITLE), provider="ECB sovereign yield curves", category="euro_sovereign_funding",
            unit="percent per annum", frequency="business_daily",
            source_url=url, geography="Euro area",
            hypothesis="Term-specific sovereign funding conditions and credit-quality differences may affect refinancing costs, duration exposure, capital investment and financial-sector margins.",
            limitations="ECB Svensson-model estimates derived from government bond prices; not directly observed quotes at every tenor. Spot, instantaneous forward and par curves share a fitted model and are strongly related. Annual tenor knots and short-end benchmarks only; no monthly-grid multiplication. Current revised histories are not historical publication vintages.",
            raw_sha256=digest, dataset_identity="ecb:" + key, native_frequency="business_daily",
            measurement_type="source-fitted yield curve", source_parameters=params,
            documentation_url="https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_area_yield_curves/html/index.en.html",
            expected_publication_lag="TARGET business-day publication around noon; source revisions possible",
        ))
    return result


def bis_effective(fetch):
    url = "https://data.bis.org/static/bulk/WS_EER_csv_col.zip"
    raw, digest = fetch.get(url)
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        data = pd.read_csv(io.BytesIO(archive.read("WS_EER_csv_col.csv")))
    # Broad and narrow baskets are related; use one broad nominal basket per economy.
    data = data[(data.FREQ == "D") & (data.EER_TYPE == "N") & (data.EER_BASKET == "B")]
    date_columns = [c for c in data if re.fullmatch(r"\d{4}-\d{2}-\d{2}", c) and c >= START]
    result = []
    for _, row in data.iterrows():
        frame = clean(date_columns, row[date_columns].to_numpy())
        keep(result, save(
            "bis_broad_neer_" + slug(row.REF_AREA), frame,
            title=f"{row['Reference area']}: broad nominal effective exchange rate",
            provider="BIS effective exchange rates", category="trade_competitiveness",
            unit="index, 2020=100", frequency="business_daily", source_url=url,
            geography=row["Reference area"],
            hypothesis="Trade-weighted currency strength may influence exporter competitiveness, imported inflation, commodity purchasing power and multinational revenue translation.",
            limitations="BIS trade-weighted nominal currency index, not a bilateral executable FX quote. Weights and histories can be revised. No monthly real-effective series are upsampled; only the source daily broad nominal basket is retained. Daily observations are released on a weekly schedule.",
            raw_sha256=digest, dataset_identity="BIS:EER:" + row.Series,
            native_frequency="business_daily", source_release_frequency="weekly",
            documentation_url="https://data.bis.org/topics/EER",
            source_series=row.Series,
        ))
    return result


def fiscal_cash(fetch):
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash"
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).date().isoformat()
    params = {
        "filter": f"record_date:gte:{START},record_date:lte:{cutoff},account_type:eq:Treasury General Account (TGA)",
        "fields": "record_date,transaction_type,transaction_catg,transaction_today_amt",
        "sort": "record_date,transaction_type,transaction_catg", "page[size]": 10000,
    }
    parts, hashes = [], []
    for page in range(1, 100):
        raw, digest = fetch.get(url, {**params, "page[number]": page})
        obj = json.loads(raw)
        if obj["meta"]["dataFormats"]["transaction_today_amt"] != "$1,000,000":
            raise ValueError("Treasury unit format changed")
        parts.extend(obj["data"])
        hashes.append(digest)
        if page >= obj["meta"]["total-pages"]:
            break
        time.sleep(.2)
    else:
        raise ValueError("Treasury pagination limit")
    data = pd.DataFrame(parts)
    excluded = baseline_identities()
    result = []
    for (direction, channel), frame in data.groupby(["transaction_type", "transaction_catg"]):
        identity = f"treasury:dts:{direction}:{channel}"
        if identity in excluded or channel in {"null", ""}:
            continue
        values = frame.transaction_today_amt.replace({"null": None})
        values = clean(frame.record_date, values)
        keep(result, save(
            "treasury_cash_" + slug(direction + "_" + channel), values,
            title=f"US Treasury {direction.lower()}: {channel}", provider="US Treasury Fiscal Data",
            category="government_cash_channels", unit="USD million", frequency="business_daily",
            source_url=url, geography="United States",
            hypothesis=f"Cash activity in {channel} may reveal government payment timing, program demand, taxpayer receipts or liquidity changes relevant to exposed industries and households.",
            limitations="Daily modified-cash-basis amounts rounded to millions. Receipt and processing dates are not accrual activity dates. Programs and category labels can change; components and totals are related. Only actual daily amounts are used; month/fiscal-year accumulations excluded. Explicit source zeros retained; missing dates omitted.",
            raw_sha256=hashes, dataset_identity=identity, native_frequency="business_daily",
            source_parameters=params, transaction_type=direction, transaction_category=channel,
            documentation_url="https://fiscal.treasury.gov/accounting/daily-treasury-statement",
        ))
    return result


def sec_profile_zip(raw):
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        data = pd.read_csv(io.BytesIO(archive.read("sub.txt")), sep="\t", dtype=str)
    required = {"cik", "sic", "countryba", "stprba", "filed", "accepted", "name"}
    if not required <= set(data):
        raise ValueError("SEC company profile columns changed")
    data = data[data.filed <= "20231231"].sort_values(["accepted", "filed"])
    return data.drop_duplicates("cik", keep="last")[sorted(required)]


def sec_sic_names(raw):
    table = pd.read_html(io.StringIO(raw.decode()), header=0)[0]
    if not {"SIC Code", "Industry Title"} <= set(table):
        raise ValueError("SEC SIC classification columns changed")
    return {str(int(row["SIC Code"])).zfill(4): row["Industry Title"]
            for _, row in table.iterrows()}


def sec_panels(fetch):
    # Freeze the pre-sample cohort and classification. The underlying archived
    # extract can still be reprocessed by SEC; this is not a PIT availability feed.
    profile_url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q4.zip"
    raw, profile_hash = fetch.get(profile_url, max_age_hours=24 * 3650, timeout=240)
    profiles = sec_profile_zip(raw)
    sic_url = "https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list"
    raw, sic_hash = fetch.get(sic_url, max_age_hours=720)
    names = sec_sic_names(raw)
    parts, hashes = [], [profile_hash, sic_hash]
    now = pd.Timestamp.now(tz="UTC")
    for year in range(2024, now.year + 1):
        for quarter in range(1, 5):
            if (year, quarter) > (now.year, now.quarter):
                break
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.zip"
            raw, digest = fetch.get(url, timeout=120)
            parts.append(parse_sec_index(raw))
            hashes.append(digest)
            time.sleep(.15)
    source = pd.concat(parts, ignore_index=True)
    days = sorted(source.date.unique())
    source = canonical_sec_records(source)
    data = source.merge(profiles, on="cik", how="inner", suffixes=("", "_profile"), validate="many_to_one")
    data["sic"] = data.sic.str.zfill(4)
    data["region"] = data.countryba.fillna("unknown")
    domestic = data.countryba.eq("US")
    data.loc[domestic, "region"] = "US-" + data.loc[domestic, "stprba"].fillna("unknown")
    result = []
    seen = set()
    # All filings plus selected exact forms: no transformations or rolling averages.
    forms = ["ALL"] + list(SEC_FORMS)
    for dimension in ["sic", "region"]:
        for group, subset in data.groupby(dimension):
            if group in {"0000", "9999", "unknown", "US-unknown", "nan"} or subset.cik.nunique() < 5:
                continue
            label = names.get(group, "SIC " + group) if dimension == "sic" else group
            for form in forms:
                chosen = subset if form == "ALL" else subset[subset.form == form]
                if chosen.empty:
                    continue
                # Unique documents per day, including for multi-form submissions.
                counts = chosen.groupby("date").filename.nunique()
                if len(counts) < 120 or counts.sum() < 300:
                    continue
                frame = clean(days, counts.reindex(days, fill_value=0).to_numpy())
                digest = hashlib.sha256(frame.value.to_numpy().tobytes()).hexdigest()
                if digest in seen:
                    continue
                seen.add(digest)
                identity = f"sec:edgar:2023q4_cohort:{dimension}:{group}:form:{form}:distinct_accessions"
                keep(result, save(
                    "sec_panel_" + slug(dimension + "_" + group + "_" + form), frame,
                    title=f"SEC {label}: daily {form} indexed filing count",
                    provider="SEC EDGAR filing indexes", category="industry_disclosure_activity" if dimension == "sic" else "regional_disclosure_activity",
                    unit="distinct indexed filings", frequency="business_daily", source_url="https://www.sec.gov/Archives/edgar/full-index/",
                    geography="US SEC filing system / " + (label if dimension == "region" else "global registrants"),
                    hypothesis=f"Changes in {form} filing activity among {label} registrants may indicate sector or regional financing, corporate events, ownership reporting and regulatory workload.",
                    limitations="Counts are indexed filings, not dollars, transaction direction or quarterly financial values. Fixed cohort/classification from filings in 2023Q4; excludes registrants absent from that extract and does not follow later industry/headquarters changes. Industry and geography panels overlap. Index CIK is the indexed filer and is not always the underlying issuer for ownership forms. Each accession counted once per panel/day; SC/SCHEDULE form renames combined. Zero means no matches on a day with other records in complete SEC quarterly indexes. Source retrospective revisions remain possible.",
                    raw_sha256=hashes, dataset_identity=identity, native_frequency="event filings aggregated daily",
                    finer_frequency_available=False, cohort_date="2023-12-31", cohort_companies=int(subset.cik.nunique()),
                    aggregation="Distinct indexed accession paths by filing date for a fixed SIC or business-address geography cohort",
                    source_profile_url=profile_url, source_form=form, source_group=group,
                    documentation_url="https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets",
                ))
    return result


GITHUB_TOPICS = [
    "machine-learning", "database", "kubernetes", "robotics", "bioinformatics", "geospatial",
    "computer-vision", "cybersecurity", "observability", "data-engineering", "simulation",
    "quantum-computing", "cad", "3d-printing", "home-automation", "blockchain", "payments",
    "ecommerce", "business-intelligence", "scientific-computing", "distributed-systems",
    "operating-system", "compiler", "networking", "storage", "devops", "infrastructure-as-code",
    "autonomous-driving", "medical-imaging", "genomics", "industrial-iot", "reinforcement-learning",
    "natural-language-processing", "speech-recognition", "computer-aided-design", "embedded", "fpga", "satellite",
]
GITHUB_EVENTS = {
    "PushEvent": ("push_events", "code push events"),
    "PullRequestEvent": ("opened_pull_requests", "opened pull requests"),
    "IssuesEvent": ("opened_issues", "opened issues"),
    "WatchEvent": ("star_events", "new star events"),
    "ForkEvent": ("fork_events", "fork events"),
}


def sql_literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def github_activity(fetch):
    url = "https://play.clickhouse.com/"
    selection_path = ROOT / "archive/research3_selection/github.json"
    if selection_path.exists():
        selection = json.loads(selection_path.read_text())
    else:
        topics_sql = "[" + ",".join(sql_literal(x) for x in GITHUB_TOPICS) + "]"
        query = ("SELECT full_name,description,topics,stargazers_count,created_at,updated_at FROM repos "
                 "WHERE NOT fork AND NOT archived AND NOT disabled AND created_at < '2024-01-01' "
                 f"AND stargazers_count>=500 AND hasAny(topics,{topics_sql}) "
                 "ORDER BY stargazers_count DESC LIMIT 10000 FORMAT JSON")
        raw, digest = fetch.get(url, {"user": "explorer", "query": query}, timeout=120)
        repos = json.loads(raw)["data"]
        queues = {topic: [r for r in repos if topic in r["topics"] and
                          not any(x in r["full_name"].lower() for x in ["awesome", "interview", "tutorial"])]
                  for topic in GITHUB_TOPICS}
        chosen, seen = [], set()
        for rank in range(25):
            for topic in GITHUB_TOPICS:
                queue = queues[topic]
                if rank < len(queue) and queue[rank]["full_name"] not in seen:
                    row = queue[rank]
                    chosen.append(dict(row, selected_topic=topic))
                    seen.add(row["full_name"])
                    if len(chosen) >= 450:
                        break
            if len(chosen) >= 450:
                break
        selection = {"repos": chosen, "raw_sha256": digest, "query": query}
        atomic_bytes(selection_path, json.dumps(selection, indent=2).encode())
    result = []
    cutoff = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).date().isoformat()
    for offset in range(0, len(selection["repos"]), 20):
        selected = selection["repos"][offset:offset + 20]
        names_sql = ",".join(sql_literal(x["full_name"]) for x in selected)
        events_sql = ",".join(sql_literal(x) for x in GITHUB_EVENTS)
        query = (
            "SELECT repo_name,toString(event_type) AS event_type,toDate(created_at,'UTC') AS date,count() AS value "
            f"FROM github_events WHERE repo_name IN ({names_sql}) AND event_type IN ({events_sql}) "
            f"AND created_at>='{START}' AND created_at<'{cutoff}' "
            "AND (event_type NOT IN ('PullRequestEvent','IssuesEvent') OR action='opened') "
            "GROUP BY repo_name,event_type,date ORDER BY repo_name,event_type,date FORMAT JSON"
        )
        raw, digest = fetch.get(url, {"user": "explorer", "query": query}, timeout=180)
        obj = json.loads(raw)
        if obj.get("rows_before_limit_at_least", 0) > obj.get("rows", 0):
            raise ValueError("GitHub aggregate result truncated")
        data = pd.DataFrame(obj["data"])
        for repo in selected:
            for event, (suffix, label) in GITHUB_EVENTS.items():
                rows = data[(data.repo_name == repo["full_name"]) & (data.event_type == event)] if len(data) else data
                if rows.empty:
                    continue
                frame = clean(rows.date, rows.value)
                keep(result, save(
                    "github_" + slug(repo["full_name"]) + "_" + suffix, frame,
                    title=f"GitHub {repo['full_name']}: daily {label}", provider="GH Archive via ClickHouse",
                    category="open_source_development", unit="public GitHub events", frequency="daily",
                    source_url=url, geography="Global public repository activity",
                    hypothesis=f"Activity in {repo['full_name']} may monitor adoption, development investment or support burden in {repo['selected_topic']}, with potential relevance to cloud, enterprise software and associated industries.",
                    limitations="Public GitHub event archive hosted by ClickHouse, with ingestion gaps/lag and snapshot limits; inspect end date. Collection excludes today and yesterday as an ingestion buffer. Events are not unique users, revenue, code quality or net stars. Bots and automated workflows contribute. Only returned event-day count bins are retained; absence is not filled with zero. Pull requests/issues count openings only. Pinned repository universe uses a historical repository metadata snapshot, topic balance and popularity; renamed repositories are not merged.",
                    raw_sha256=[digest, selection["raw_sha256"]], dataset_identity=f"GHArchive:{repo['full_name']}:{event}:opened" if event in {"PullRequestEvent", "IssuesEvent"} else f"GHArchive:{repo['full_name']}:{event}",
                    native_frequency="event timestamps aggregated daily", finer_frequency_available=True,
                    source_query=query, repository=repo["full_name"], source_topic=repo["selected_topic"],
                    selection_metadata_updated_at=repo["updated_at"], measurement_type="event-count aggregation",
                    documentation_url="https://clickhouse.com/demos/explore-github-with-clickhouse-powered-real-time-analytics",
                    upstream_documentation_url="https://www.gharchive.org/",
                ))
        time.sleep(.7)
    return result


def marine(fetch):
    selection_path = ROOT / "archive/research3_selection/marine.json"
    if selection_path.exists():
        selection = json.loads(selection_path.read_text())
    else:
        ports, hashes = arcgis_query(fetch, PORT_ROOT + "/PortWatch_ports_database/FeatureServer/0",
                                    {"where": "1=1", "outFields": "*", "orderByFields": "ObjectId"})
        ports = ports.sort_values("vessel_count_total", ascending=False)
        # Broad country panel first, then a second major port in each economy.
        chosen = pd.concat([ports.groupby("ISO3", sort=False).nth(rank) for rank in range(2)]).head(220)
        selection = {"ports": chosen[["portid", "portname", "country", "ISO3", "lat", "lon", "vessel_count_total"]].to_dict("records"),
                     "raw_sha256": hashes}
        atomic_bytes(selection_path, json.dumps(selection, indent=2).encode())
    result, seen = [], set()
    end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)).date().isoformat()
    url = "https://marine-api.open-meteo.com/v1/marine"
    for port in selection["ports"]:
        params = {"latitude": port["lat"], "longitude": port["lon"], "start_date": START,
                  "end_date": end, "hourly": "wave_height,wave_direction,wave_period",
                  "models": "era5_ocean", "timezone": "GMT"}
        raw, digest = fetch.get(url, params, timeout=180)
        obj = json.loads(raw)
        if obj.get("utc_offset_seconds") != 0:
            raise ValueError("Marine UTC offset changed")
        cell = f"{obj['latitude']:.4f},{obj['longitude']:.4f}"
        if cell in seen:
            continue
        seen.add(cell)
        for metric, unit, label in [
            ("wave_height", "m", "significant combined wave height"),
            ("wave_direction", "°", "mean wave direction"),
            ("wave_period", "s", "mean wave period"),
        ]:
            if obj["hourly_units"][metric] != unit:
                raise ValueError("Unexpected marine units")
            frame = clean(obj["hourly"]["time"], obj["hourly"][metric])
            keep(result, save(
                "era5_ocean_" + slug(cell.replace("-", "minus")) + "_" + metric, frame,
                title=f"Ocean grid near {port['portname']}, {port['country']}: {label}",
                provider="ERA5 Ocean via Open-Meteo", category="maritime_operating_conditions",
                unit=unit, frequency="hourly", source_url=url, geography=f"{port['country']} / offshore grid near {port['portname']}",
                hypothesis=f"Sea-state conditions near {port['portname']} may affect vessel access, cargo operations, fishing, offshore energy work and weather-sensitive maritime supply chains.",
                limitations="ECMWF ERA5 ocean reanalysis served by Open-Meteo; model estimates, not in-port sensor observations or forecasts available at the historical timestamp. Coarse ocean cells (about 0.5 degrees) can be far from estuaries/berths. Actual returned grid coordinates identify each series; duplicate cells are excluded. Wave direction is the direction waves come from, measured in degrees from true north; it is circular and requires circular statistics. Missing/land cells omitted without filling. Reanalysis and archive revisions can create look-ahead bias.",
                raw_sha256=[digest] + selection["raw_sha256"], dataset_identity=f"ERA5-Ocean:{cell}:{metric}",
                native_frequency="hourly", finer_frequency_available=False, source_parameters=params,
                latitude=obj["latitude"], longitude=obj["longitude"], port_id=port["portid"],
                requested_latitude=port["lat"], requested_longitude=port["lon"],
                measurement_type="ocean reanalysis", expected_publication_lag="About five days; seven-day collection buffer",
                documentation_url="https://open-meteo.com/en/docs/marine-weather-api",
                license="CC BY 4.0; attribute Open-Meteo and ECMWF ERA5",
            ))
        time.sleep(1)
    return result


PROVIDERS = {"ecb": ecb_curves, "bis": bis_effective, "fiscal": fiscal_cash,
             "sec": sec_panels, "github": github_activity, "marine": marine}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS, action="append")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    fetch = Fetcher(refresh=args.refresh)
    report = {"started_at": pd.Timestamp.now(tz="UTC").isoformat(), "successes": {}, "failures": []}
    names = args.provider or list(PROVIDERS)
    with ThreadPoolExecutor(max_workers=4) as pool:
        tasks = {pool.submit(PROVIDERS[name], fetch): name for name in names}
        for task in as_completed(tasks):
            name = tasks[task]
            try:
                rows = task.result()
                report["successes"][name] = [x["series_id"] for x in rows]
                print("PROVIDER", name, len(rows), flush=True)
            except Exception as exc:
                report["failures"].append({"provider": name, "error": f"{type(exc).__name__}: {exc}"})
                print("FAILED", name, str(exc), flush=True)
    report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(ROOT / "logs" / ("research3_" + "_".join(names) + ".json"), json.dumps(report, indent=2).encode())
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
