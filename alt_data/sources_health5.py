#!/usr/bin/env python3
"""Actual weekly disease surveillance and newly published research registrations.

Event dates describe the original reporting period. Current source classifications
and revisions are not historical point-in-time classifications.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import threading
import time

import pandas as pd

from country_codes import COUNTRY_ALPHA2
from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes,
                              coverage, normalize_frame, qualifies_research, save_candidate)

GROUP = "health5"
WHO_URL = "https://xmart-api-public.who.int/FLUMART/VIW_FNT?$format=csv"
WHO_DICT = ("https://xmart-api-public.who.int/FLUMART/VIW_FLU_METADATA?"
            "$format=csv&$filter=contains(DatasetName,%20%27FluNet%27)")
TRIAL_URL = "https://clinicaltrials.gov/api/v2/studies"
TRIAL_FIELDS = ("NCTId,StudyFirstPostDate,StudyFirstPostDateType,LocationCountry,"
                "ConditionMeshId,ConditionMeshTerm,InterventionType,Phase,StudyType,LeadSponsorClass")
US = {"US", "AS", "GU", "MP", "PR", "UM", "VI"}
LOCK = threading.Lock()
SEEN = set()
IDENTITIES = set()
PINNED = None
OMITTED = Counter()


def slug(x):
    return re.sub("[^a-z0-9]+", "_", str(x).lower()).strip("_")


def token(x):
    return slug(x)[:65] + "_" + hashlib.sha256(str(x).encode()).hexdigest()[:10]


def initialize():
    global PINNED
    cat = pd.read_parquet(ROOT / "catalog.parquet")
    baseline = cat[cat.batch.lt(7)]
    SEEN.update(baseline.content_sha256.dropna())
    IDENTITIES.update(baseline.dataset_identity.dropna())
    published = cat.loc[cat.batch.eq(7), "series_id"]
    PINNED = set(published) if len(published) else None


def stage(sid, frame, **meta):
    if PINNED is not None and sid not in PINNED:
        return False
    frame = normalize_frame(frame)
    active = int(frame.value.ne(0).sum())
    if (frame.value.nunique() < 2 or active < 26
            or not qualifies_research(dict(coverage(frame), frequency=meta["frequency"]))):
        OMITTED["constant_sparse_or_short"] += 1
        return False
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    with LOCK:
        if digest in SEEN or meta["dataset_identity"] in IDENTITIES:
            OMITTED["duplicate"] += 1
            return False
        SEEN.add(digest)
        IDENTITIES.add(meta["dataset_identity"])
    meta.update(country_code_standard="ISO 3166-1 alpha-2; XK Kosovo extension",
                active_observations=active, point_in_time=False,
                finer_frequency_available=meta.get("native_frequency") == "daily events")
    return save_candidate(GROUP, sid, frame, **meta)


def reported_frame(part, date_column, value_column):
    frame = pd.DataFrame({"date": pd.to_datetime(part[date_column], utc=True),
                          "value": pd.to_numeric(part[value_column], errors="raise")}).dropna()
    if frame.empty:
        return None
    if frame.groupby("date").value.nunique().gt(1).any():
        raise ValueError("Conflicting observations for one source series/date")
    return normalize_frame(frame.drop_duplicates("date"))


def who(fetch):
    raw, digest = fetch.get(WHO_URL, timeout=180)
    dictionary, dh = fetch.get(WHO_DICT)
    labels = pd.read_csv(io.BytesIO(dictionary)).set_index("FieldName").Description.to_dict()
    data = pd.read_csv(io.BytesIO(raw), low_memory=False, keep_default_na=False, na_values=[""])
    data = data[data.ISO_WEEKSTARTDATE.ge("2018-01-01")].copy()
    metrics = ["SPEC_PROCESSED_NB", "SPEC_RECEIVED_NB", "AH1N12009", "AH1", "AH3", "AH5",
               "AH7N9", "ANOTSUBTYPED", "ANOTSUBTYPABLE", "AOTHER_SUBTYPE", "INF_A",
               "BVIC_2DEL", "BVIC_3DEL", "BVIC_NODEL", "BVIC_DELUNK", "BYAM",
               "BNOTDETERMINED", "INF_B", "INF_ALL", "INF_NEGATIVE", "ADENO", "BOCA",
               "HUMAN_CORONA", "METAPNEUMO", "PARAINFLUENZA", "RHINO", "RSV_PROCESSED", "RSV"]
    saved = 0
    for (code, source), part in data.groupby(["ISO2", "ORIGIN_SOURCE"], dropna=False):
        if code not in COUNTRY_ALPHA2 or pd.isna(source):
            continue
        country = part.COUNTRY_AREA_TERRITORY.iloc[0]
        for metric in metrics:
            frame = reported_frame(part, "ISO_WEEKSTARTDATE", metric)
            if frame is None:
                continue
            category = ("respiratory_testing" if metric in {"SPEC_PROCESSED_NB", "SPEC_RECEIVED_NB", "RSV_PROCESSED", "INF_NEGATIVE"}
                        else "influenza_surveillance" if metric not in {"ADENO", "BOCA", "HUMAN_CORONA", "METAPNEUMO", "PARAINFLUENZA", "RHINO", "RSV"}
                        else "respiratory_virus_surveillance")
            result = stage(
                f"who_flunet_{slug(code)}_{slug(source)}_{slug(metric)}", frame,
                title=f"{country} / {source}: {labels[metric]}",
                provider="who_flunet", category=category,
                unit="negative influenza test results" if metric == "INF_NEGATIVE" else "specimens" if "SPEC_" in metric or metric == "RSV_PROCESSED" else "detections",
                frequency="weekly", native_frequency="weekly",
                source_url="https://www.who.int/tools/flunet",
                geography=f"{country}; {source} surveillance", country_codes=[code],
                geography_scope="us" if code in US else "non_us",
                hypothesis="Disease circulation and testing activity may precede changes in absenteeism, travel and health-product demand.",
                limitations="Laboratory surveillance, not population prevalence. Sampling, reporting and subtype testing vary by country and over time. Sentinel, non-sentinel and undefined systems may overlap; do not sum them. Nulls remain absent; reported zeros retained. Retrospective revisions and publication lags prevent treating period labels as availability times.",
                raw_sha256=[digest, dh], metric=metric, surveillance_type=source,
                aggregation="Source-reported weekly counts, unchanged; no interpolation or cross-system sum",
                timestamp_semantics="ISO week start Monday, UTC period label; original reporting calendars mapped by WHO",
                expected_publication_lag="Updated weekly; country-specific delays and revisions, commonly one or more weeks",
                dataset_identity=f"who:flunet:{code}:{source}:{metric}",
                source_country_field="ISO2", source_date_field="ISO_WEEKSTARTDATE",
                source_filter_start="2018-01-01", license="WHO data policy")
            saved += bool(result)
    return {"saved": saved, "source_rows": len(data), "raw_sha256": digest}


CAPITALS = {
    1200401: "Rio Branco", 2704302: "Maceió", 1600303: "Macapá", 1302603: "Manaus",
    2927408: "Salvador", 2304400: "Fortaleza", 5300108: "Brasília", 3205309: "Vitória",
    5208707: "Goiânia", 2111300: "São Luís", 5103403: "Cuiabá", 5002704: "Campo Grande",
    3106200: "Belo Horizonte", 1501402: "Belém", 2507507: "João Pessoa", 4106902: "Curitiba",
    2611606: "Recife", 2211001: "Teresina", 3304557: "Rio de Janeiro", 2408102: "Natal",
    4314902: "Porto Alegre", 1100205: "Porto Velho", 1400100: "Boa Vista",
    4205407: "Florianópolis", 3550308: "São Paulo", 2800308: "Aracaju", 1721000: "Palmas",
}


def dengue(fetch):
    docs, doc_hash = fetch.get("https://info.dengue.mat.br/services/api/doc")
    geo, geo_hash = fetch.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios")
    names = {v["id"]: v["nome"] for v in json.loads(geo)}
    now = pd.Timestamp.now(tz="UTC")

    def acquire(job):
        code, disease = job
        raw, digest = fetch.get("https://info.dengue.mat.br/api/alertcity", params={
            "geocode": code, "disease": disease, "format": "csv",
            "ew_start": 1, "ew_end": 53, "ey_start": 2021, "ey_end": now.year}, timeout=120)
        part = pd.read_csv(io.BytesIO(raw))
        count = 0
        for locality, area in part.groupby("Localidade_id", dropna=False):
            if pd.isna(locality):
                continue
            loc = str(int(locality))
            for metric, unit, description, category in [
                ("casos", "reported cases", "notified cases", "arbovirus_notifications"),
                ("p_rt1", "probability 0–1", "estimated probability of reproduction number above 1", "arbovirus_growth_risk"),
            ]:
                frame = reported_frame(area, "data_iniSE", metric)
                if frame is None:
                    continue
                frame = frame[frame.date.le(now)]
                name = names.get(code, CAPITALS[code])
                count += bool(stage(
                    f"infodengue_{code}_{disease}_{loc}_{metric}", frame,
                    title=f"{name}, Brazil / locality {loc}: {disease} {description}",
                    provider="infodengue", category=category, unit=unit,
                    frequency="weekly", native_frequency="weekly",
                    source_url="https://info.dengue.mat.br/services/api/doc",
                    geography=f"{name}, Brazil; source locality {loc}", country_codes=["BR"], geography_scope="non_us",
                    hypothesis="Mosquito-borne disease outbreaks may affect labor attendance, health supplies and local travel.",
                    limitations="Retrospectively revised suspected notifications are not all laboratory confirmed. p_rt1 is a source-model probability, not observed infections. Reporting delay, surveillance effort and model versions vary. Submunicipal localities stay separate. Series is a current revised history, not what traders knew then.",
                    raw_sha256=[digest, doc_hash, geo_hash], municipality_code=str(code),
                    locality_id=loc, pathogen=disease, metric=metric,
                    model_versions=sorted(area.versao_modelo.dropna().astype(str).unique().tolist()),
                    aggregation="Native weekly source observation; no zero filling",
                    measurement_type="source model probability" if metric == "p_rt1" else "notified cases",
                    timestamp_semantics="Sunday start of epidemiological week, UTC period label",
                    expected_publication_lag="Weekly updates, generally one or more weeks late; historical revisions",
                    dataset_identity=f"infodengue:{code}:{disease}:{loc}:{metric}",
                    license="InfoDengue public API; attribute source"))
        return {"municipality": code, "disease": disease, "saved": count}

    results, failures = [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = {pool.submit(acquire, (code, disease)): (code, disease)
                for code in CAPITALS for disease in ["dengue", "chikungunya", "zika"]}
        for future in as_completed(jobs):
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append({"job": jobs[future], "error": str(exc)})
            if (len(results) + len(failures)) % 10 == 0:
                print("InfoDengue", len(results), "completed", len(failures), "failures", flush=True)
    return {"saved": sum(r["saved"] for r in results), "requests": results, "failures": failures}


def trial_page(fetch, year, end_date):
    start = f"{year}-01-01"
    end = min(f"{year}-12-31", end_date)
    params = {"format": "json", "pageSize": 1000, "countTotal": "true", "fields": TRIAL_FIELDS,
              "query.term": f"AREA[StudyFirstPostDate]RANGE[{start},{end}]"}
    rows, hashes, ids, total = [], [], set(), None
    while True:
        raw, digest = fetch.get(TRIAL_URL, params=params, timeout=120)
        obj = json.loads(raw)
        total = obj.get("totalCount", total)
        hashes.append(digest)
        for study in obj.get("studies", []):
            sid = study["protocolSection"]["identificationModule"]["nctId"]
            if sid in ids:
                raise ValueError(f"Duplicate trial ID during pagination: {sid}")
            ids.add(sid)
            rows.append(study)
        if not obj.get("nextPageToken"):
            break
        params["pageToken"] = obj["nextPageToken"]
        params["countTotal"] = "false"
        time.sleep(.15)
    if total != len(rows):
        raise ValueError(f"Trial {year}: API total {total} differs from {len(rows)} unique records")
    print("ClinicalTrials.gov", year, len(rows), "records", len(hashes), "pages", flush=True)
    return rows, hashes, {"year": year, "api_total": total, "unique_records": len(rows)}


def country_names(fetch):
    raw, _ = fetch.get(WHO_URL, timeout=180)
    df = pd.read_csv(io.BytesIO(raw), usecols=["ISO2", "COUNTRY_AREA_TERRITORY"],
                     keep_default_na=False)
    mapping = {name.casefold(): code for code, name in df[["ISO2", "COUNTRY_AREA_TERRITORY"]].drop_duplicates().itertuples(index=False, name=None)
               if code in COUNTRY_ALPHA2}
    aliases = {
        "United States": "US", "United Kingdom": "GB", "South Korea": "KR", "Korea, Republic of": "KR",
        "Russia": "RU", "Taiwan": "TW", "Iran": "IR", "Viet Nam": "VN", "Vietnam": "VN",
        "Tanzania": "TZ", "Türkiye": "TR", "Turkey": "TR", "Moldova": "MD", "Bolivia": "BO",
        "Venezuela": "VE", "Czech Republic": "CZ", "Hong Kong": "HK", "Macao": "MO",
        "Congo, The Democratic Republic of the": "CD", "Palestinian Territories": "PS",
        "Kosovo": "XK", "Côte D'Ivoire": "CI", "Cote D'Ivoire": "CI", "Lao People's Democratic Republic": "LA",
        "Turkey (Türkiye)": "TR", "Netherlands": "NL", "Puerto Rico": "PR", "Monaco": "MC",
        "Reunion": "RE", "Eswatini": "SZ", "Botswana": "BW", "Malawi": "MW", "Cabo Verde": "CV",
        "Syria": "SY", "The Gambia": "GM", "Vanuatu": "VU", "Laos": "LA",
        "French Polynesia": "PF", "Burma": "MM", "Burundi": "BI", "Guam": "GU", "Macau": "MO",
        "Guinea-Bissau": "GW", "Andorra": "AD", "United States Minor Outlying Islands": "UM",
        "Brunei": "BN", "Benin": "BJ", "Republic of the Congo": "CG", "Samoa": "WS",
        "Mayotte": "YT", "Jersey": "JE", "Greenland": "GL", "Comoros": "KM",
        "American Samoa": "AS", "The Bahamas": "BS", "Faroe Islands": "FO", "Aland Islands": "AX",
        "Djibouti": "DJ", "Curacao": "CW", "Bonaire, Saint Eustatius and Saba ": "BQ",
        "San Marino": "SM", "Northern Mariana Islands": "MP", "Antarctica": "AQ",
        "French Southern and Antarctic Lands": "TF", "Christmas Island": "CX", "Niue": "NU",
    }
    mapping.update({name.casefold(): code for name, code in aliases.items()})
    return mapping


def trial_events(studies, country_map):
    """One study contributes once per country/facet; cohorts deliberately overlap."""
    events = defaultdict(Counter)
    names, labels, missing = {}, {}, Counter()
    for study in studies:
        protocol = study["protocolSection"]
        status = protocol.get("statusModule", {})
        date = status.get("studyFirstPostDateStruct", {}).get("date")
        if not date or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            continue
        if status.get("studyFirstPostDateStruct", {}).get("type") == "ESTIMATED":
            continue
        dt = pd.Timestamp(date)
        week = (dt - pd.Timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
        country_set = {"GLOBAL"}
        for location in protocol.get("contactsLocationsModule", {}).get("locations", []):
            name = location.get("country", "")
            code = country_map.get(name.casefold())
            if code:
                country_set.add(code)
                names[code] = name
            elif name:
                missing[name] += 1
        facets = {("all", "all")}
        for field, vals in [
            ("study_type", [protocol.get("designModule", {}).get("studyType")]),
            ("phase", protocol.get("designModule", {}).get("phases", [])),
            ("sponsor_class", [protocol.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {}).get("class")]),
            ("intervention_type", [v.get("type") for v in protocol.get("armsInterventionsModule", {}).get("interventions", [])]),
        ]:
            facets.update((field, value) for value in vals if value)
        for mesh in study.get("derivedSection", {}).get("conditionBrowseModule", {}).get("meshes", []):
            if mesh.get("id") and mesh.get("term"):
                facets.add(("condition", mesh["id"]))
                labels[mesh["id"]] = mesh["term"]
        for code in country_set:
            for facet in facets:
                events[(code, *facet)][week] += 1
    names["GLOBAL"] = "Global registry"
    return events, names, labels, missing


def trials(fetch):
    now = pd.Timestamp.now(tz="UTC").normalize()
    # Include only complete Monday–Sunday weeks, with no synthetic partial week.
    last_week = now - pd.Timedelta(days=now.weekday() + 7)
    end_date = (last_week + pd.Timedelta(days=6)).strftime("%Y-%m-%d")
    studies, hashes, checks = [], [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(trial_page, fetch, year, end_date) for year in range(2021, now.year + 1)]
        for future in as_completed(futures):
            rows, raw_hashes, check = future.result()
            studies.extend(rows)
            hashes.extend(raw_hashes)
            checks.append(check)
    ids = [s["protocolSection"]["identificationModule"]["nctId"] for s in studies]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate trial IDs across date partitions")
    events, names, labels, missing = trial_events(studies, country_names(fetch))
    _, schema_hash = fetch.get("https://clinicaltrials.gov/api/v2/studies/metadata")
    _, country_hash = fetch.get(WHO_URL, timeout=180)
    manifest = archive_raw_manifest(hashes, description="Complete first-posted year partitions, selected non-personal ClinicalTrials.gov API v2 fields")
    dates = pd.date_range("2021-01-04", last_week.strftime("%Y-%m-%d"), freq="W-MON", tz="UTC")
    saved = 0
    for (code, facet, key), counts in sorted(events.items()):
        if len(counts) < 26:
            continue
        values = [counts.get(date.strftime("%Y-%m-%d"), 0) for date in dates]
        frame = pd.DataFrame({"date": dates, "value": values})
        label = labels.get(key, key)
        saved += bool(stage(
            f"clinical_registrations_{slug(code)}_{facet}_{token(key)}", frame,
            title=f"{names[code]}: first-published clinical study registrations / {facet} {label}",
            provider="clinicaltrials_gov", category="biomedical_research_" + facet,
            unit="newly published study registrations", frequency="weekly", native_frequency="daily events",
            source_url="https://clinicaltrials.gov/data-api/api",
            geography=names[code] + ("; current listed study locations" if code != "GLOBAL" else ""),
            country_codes=[] if code == "GLOBAL" else [code],
            geography_scope="global" if code == "GLOBAL" else "us" if code in US else "non_us",
            hypothesis="Changes in research registrations may proxy biotech investment, clinical-service demand and therapeutic-area activity.",
            limitations="Registrations are not trial starts, patient counts, successful results or spending. ClinicalTrials.gov is incomplete for all research worldwide. Country means a currently listed study location, not sponsor domicile or spending allocation. Multi-country and multi-condition cohorts overlap. Present classifications and locations may have changed since first publication. Counts may be revised if records are removed or merged. No proven trading relationship.",
            raw_sha256=[schema_hash, country_hash], raw_manifest_sha256=[manifest],
            aggregation="Count distinct NCT IDs by first-publication week and single country/facet; zeros only within completely paginated time window",
            timestamp_semantics="Monday UTC label of week containing StudyFirstPostDate; available only after week end",
            expected_publication_lag="First-post date is original public posting; weekly total available after week end. Current classifications are retrospective.",
            dataset_identity=f"clinicaltrials:registrations:{code}:{facet}:{key}",
            metric="study_first_post_count", facet=facet, facet_key=key, facet_label=label,
            source_query_start="2021-01-01", source_query_end=end_date,
            country_exposure_definition="Current listed study locations; one count per country, no allocation of enrollment",
            license="ClinicalTrials.gov public data; attribute ClinicalTrials.gov"))
    return {"saved": saved, "unique_source_studies": len(ids), "partitions": checks,
            "unknown_country_rows_omitted": dict(missing), "raw_manifest_sha256": manifest}


def discover_ukhsa(fetch):
    path = ROOT / "archive/health5_selection/ukhsa_metrics.json"
    if path.exists():
        return json.loads(path.read_text())
    queue = ["https://api.ukhsa-dashboard.data.gov.uk/themes/infectious_disease"]
    seen, metrics = set(), []
    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        raw, _ = fetch.get(url)
        for item in json.loads(raw):
            if item.get("name") == "COVID-19":
                continue
            if url.endswith("/geography_types") and item.get("name") != "Nation":
                continue
            if url.endswith("/metrics"):
                metrics.append(item)
                continue
            queue.extend(v for v in item.values() if isinstance(v, str)
                         and v.startswith("https://api.ukhsa-dashboard.data.gov.uk/"))
    atomic_bytes(path, json.dumps(metrics, indent=2).encode())
    return metrics


def ukhsa_metric_unit(name):
    if any(word in name.lower() for word in ["average", "baseline", "rolling", "excluding", "byyear", "bymonth"]):
        return None
    if "positivityByWeek" in name:
        return "percent of tested specimens"
    if "_GPInHours_rateByDay" in name:
        return "consultations per 100,000 registered patients"
    if "casesByOnsetWeek" in name or name.endswith("_cases_count"):
        return "reported cases"
    return None


def ukhsa(fetch):
    definitions = [
        "https://ukhsa-dashboard.data.gov.uk/metrics-documentation/rsv-testing-positivity-by-week",
        "https://ukhsa-dashboard.data.gov.uk/syndromic-surveillance/respiratory-conditions",
        "https://ukhsa-dashboard.data.gov.uk/access-our-data/getting-started",
    ]
    docs = [fetch.get(url)[1] for url in definitions]
    metrics = [m for m in discover_ukhsa(fetch) if ukhsa_metric_unit(m["name"])]

    def acquire(metric):
        url = metric["link"] + "?page_size=365"
        rows, hashes, total, visited = [], [], None, set()
        while url:
            if url in visited:
                raise ValueError("Repeated UKHSA pagination URL")
            visited.add(url)
            raw, digest = fetch.get(url)
            obj = json.loads(raw)
            total = obj["count"] if total is None else total
            hashes.append(digest)
            rows.extend(obj["results"])
            url = obj.get("next")
            time.sleep(.1)
        if len(rows) != total:
            raise ValueError("UKHSA source pagination count mismatch")
        frame = pd.DataFrame(rows)
        saved = 0
        if frame.empty:
            return {"metric": metric["name"], "saved": 0}
        for (stratum, sex, age), part in frame.groupby(["stratum", "sex", "age"], dropna=False):
            if any(v in str(stratum).lower() for v in ["average", "baseline", "upper", "lower", "ci_"]):
                continue
            series = reported_frame(part, "date", "metric_value")
            if series is None or len(series) < 26:
                continue
            interval = series.date.diff().dt.total_seconds().median()
            frequency = "daily" if interval <= 86400 else "weekly" if 5 * 86400 <= interval <= 8 * 86400 else None
            if frequency is None:
                continue
            row = part.iloc[0]
            dims = f"{stratum}:{sex}:{age}"
            saved += bool(stage(
                f"ukhsa_{slug(row.geography_code)}_{slug(metric['name'])}_{token(dims)}", series,
                title=f"{row.geography}: {metric['name']} / stratum {stratum}, sex {sex}, age {age}",
                provider="ukhsa_health_surveillance", category="uk_" + slug(row.sub_theme) + "_health_activity",
                unit=ukhsa_metric_unit(metric["name"]), frequency=frequency, native_frequency=frequency,
                source_url=metric["link"], geography=row.geography, country_codes=["GB"], geography_scope="non_us",
                hypothesis="Illness circulation and primary-care demand may lead changes in absenteeism, consumer activity and health-product demand.",
                limitations="Surveillance sample, not a census of infections. Testing selection and participating GP practices vary. Rates, positivity and case counts have different denominators and should not be summed. Recent points can be revised; missing observations remain absent. Source sex/age/stratum cohorts may overlap.",
                raw_sha256=hashes + docs, metric=metric["name"], stratum=str(stratum), sex=str(sex), age=str(age),
                source_geography_code=row.geography_code,
                aggregation="Native published observations within exact geography/metric/stratum/sex/age",
                timestamp_semantics="Source observation date or week label in UTC, not historical availability timestamp",
                expected_publication_lag="Daily or weekly updates depending metric; recent reporting-delay points are provisional",
                reporting_delay_observations=int(part.in_reporting_delay_period.fillna(False).sum()),
                dataset_identity=f"ukhsa:{row.geography_code}:{metric['name']}:{dims}",
                license="UK Open Government Licence v3"))
        return {"metric": metric["name"], "saved": saved, "source_rows": len(rows)}

    results, failures = [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = {pool.submit(acquire, metric): metric["name"] for metric in metrics}
        for future in as_completed(jobs):
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append({"metric": jobs[future], "error": str(exc)})
    return {"saved": sum(r["saved"] for r in results), "metrics": results, "failures": failures}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["all", "who", "dengue", "trials", "ukhsa"], default="all")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    initialize()
    fetch = Fetcher(refresh=args.refresh)
    report = {"started_at": pd.Timestamp.now(tz="UTC").isoformat(), "sources": {}, "failures": []}
    for name, function in [("who", who), ("trials", trials), ("dengue", dengue), ("ukhsa", ukhsa)]:
        if args.source not in {"all", name}:
            continue
        try:
            report["sources"][name] = function(fetch)
            if report["sources"][name].get("failures"):
                report["failures"].append({"source": name, "error": "Some source requests failed; see source report"})
            print(name, report["sources"][name], flush=True)
        except Exception as exc:
            report["failures"].append({"source": name, "error": repr(exc)})
            print(name, "FAILED", repr(exc), flush=True)
        report["omissions"] = dict(OMITTED)
        report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        atomic_bytes(ROOT / f"logs/health5_{args.source}.json", json.dumps(report, indent=2).encode())
    return bool(report["failures"])


if __name__ == "__main__":
    raise SystemExit(main())
