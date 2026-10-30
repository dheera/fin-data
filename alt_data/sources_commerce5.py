#!/usr/bin/env python3
"""New foreign commerce panels: fuel quotes, court notices, procurement and housing."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import hashlib
import io
import json
import re
import threading
import time
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

import numpy as np
import pandas as pd

from expansion_common import (
    ROOT, Fetcher, archive_raw_manifest, atomic_bytes, coverage, normalize_frame,
    qualifies_research, save_candidate,
)

GROUP = "commerce5"
START = pd.Timestamp("2024-01-01", tz="UTC")
LOCK = threading.Lock()
SEEN = set()
OMISSIONS = []


def slug(value):
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@lru_cache
def baseline():
    catalog = pd.read_parquet(ROOT / "catalog.parquet")
    old = catalog[catalog.batch < 7]
    return {
        "ids": set(old.series_id),
        "hashes": set(old.content_sha256),
        "identities": set(old.dataset_identity.dropna()),
        "pinned": set(catalog.loc[catalog.batch == 7, "series_id"]),
        "frequencies": catalog.loc[catalog.batch == 7].set_index("series_id").frequency.to_dict(),
    }


def published_frequency(series_id):
    """Keep the original aggregation cadence when refreshing a published series."""
    return baseline()["frequencies"].get(series_id)


def check_frequency_requirement(frequency):
    if frequency not in {None, "daily", "weekly"}:
        raise ValueError(f"Unsupported published commerce frequency: {frequency!r}")


def cadence_frame(frame, frequency, required_frequency):
    """A pinned cadence that no longer qualifies is omitted, never replaced."""
    frame = normalize_frame(frame)
    if required_frequency is not None and not qualifies_research(
            dict(coverage(frame), frequency=frequency)):
        return frame.iloc[:0]
    return frame


def save(series_id, frame, **meta):
    if frame.empty:
        return None
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2 or not qualifies_research(dict(coverage(frame), frequency=meta["frequency"])):
        return None
    old = baseline()
    required_frequency = old["frequencies"].get(series_id)
    if required_frequency is not None and meta["frequency"] != required_frequency:
        return None
    if (series_id in old["ids"] or meta["dataset_identity"] in old["identities"]
            or (old["pinned"] and series_id not in old["pinned"])):
        return None
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    with LOCK:
        if digest in SEEN or digest in old["hashes"]:
            return None
        SEEN.add(digest)
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2")
    try:
        result = save_candidate(GROUP, series_id, frame, **meta)
    except Exception:
        with LOCK:
            SEEN.remove(digest)
        raise
    return result


def weekly_labels(dates):
    """Monday UTC period labels for local-calendar event dates; no timezone inference."""
    dates = pd.to_datetime(dates, utc=True)
    return dates.dt.floor("D") - pd.to_timedelta(dates.dt.weekday, unit="D")


def observed_quotes(part, required_frequency=None):
    """One canonical frequency per instrument; never fill an unreported price."""
    check_frequency_requirement(required_frequency)
    part = part[["date", "value"]].copy()
    part["date"] = pd.to_datetime(part.date, utc=True)
    part["value"] = pd.to_numeric(part.value, errors="raise").astype(float)
    if (part.value <= 0).any() or not np.isfinite(part.value).all():
        raise ValueError("Nonpositive/nonfinite source price; entire instrument excluded")
    if (part.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting prices at one source update time; entire instrument excluded")
    part = part.drop_duplicates("date").sort_values("date")
    day = part.date.dt.floor("D")
    daily = part.assign(date=day).drop_duplicates("date", keep="last")
    daily = normalize_frame(daily)
    if required_frequency != "weekly" and qualifies_research(dict(coverage(daily), frequency="daily")):
        return daily, "daily", "Last actual reported quote during each observed local calendar day"
    if required_frequency == "daily":
        return daily.iloc[:0], "daily", "Last actual reported quote during each observed local calendar day"
    weekly = part.assign(date=weekly_labels(part.date)).drop_duplicates("date", keep="last")
    return cadence_frame(weekly, "weekly", required_frequency), "weekly", "Last actual reported quote during each observed Monday-Sunday local calendar week"


def parse_french_fuel(raw):
    """Read annual official XML; retain wall-clock update labels without DST guessing."""
    rows, profiles, outages, closures = [], {}, [], []
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        files = [n for n in archive.namelist() if n.endswith(".xml")]
        if len(files) != 1:
            raise ValueError("Expected one official annual XML")
        with archive.open(files[0]) as stream:
            for _, node in ET.iterparse(stream, events=("end",)):
                if node.tag != "pdv":
                    continue
                station = node.attrib["id"]
                profiles[station] = {
                    "source_station_id": station, "city": node.findtext("ville"),
                    "postcode": node.attrib.get("cp"), "road_type": node.attrib.get("pop"),
                    "latitude": float(node.attrib.get("latitude") or "nan") / 100000,
                    "longitude": float(node.attrib.get("longitude") or "nan") / 100000,
                }
                for quote in node.findall("prix"):
                    q = quote.attrib
                    if not q:
                        # Official XML uses an empty element for stations with no quote.
                        continue
                    if not {"id", "nom", "maj", "valeur"} <= set(q):
                        raise ValueError("Partially populated official fuel quote")
                    rows.append((station, q["id"], q["nom"], q["maj"], float(q["valeur"])))
                for outage in node.findall("rupture"):
                    q = outage.attrib
                    if q.get("debut"):
                        outages.append((station, q.get("id"), q.get("fuel"), q["debut"], q.get("fin"), q.get("type")))
                for closure in node.findall("fermeture"):
                    q = closure.attrib
                    if q.get("debut"):
                        closures.append((station, q["debut"], q.get("fin"), q.get("type")))
                node.clear()
    frame = pd.DataFrame(rows, columns=["station", "fuel", "fuel_name", "date", "value"])
    frame["date"] = pd.to_datetime(frame.date, utc=True, format="mixed", errors="raise")
    return frame, profiles, outages, closures


def france_fuel(fetch):
    documentation = "https://www.prix-carburants.gouv.fr/rubrique/opendata/"
    doc_hash = fetch.get(documentation, max_age_hours=720)[1]
    years = list(range(2024, pd.Timestamp.now(tz="UTC").year + 1))

    def one_year(year):
        url = f"https://donnees.roulez-eco.fr/opendata/annee/{year}"
        raw, digest = fetch.get(url, timeout=300, max_age_hours=24 if year == years[-1] else 720)
        cache = ROOT / "archive/commerce5_fuel" / (digest + ".parquet")
        profile_cache = cache.with_suffix(".json")
        if cache.exists() and profile_cache.exists():
            data = pd.read_parquet(cache)
            obj = json.loads(profile_cache.read_text())
        else:
            data, profiles, outages, closures = parse_french_fuel(raw)
            obj = {"profiles": profiles, "outages": outages, "closures": closures}
            cache.parent.mkdir(parents=True, exist_ok=True)
            data.to_parquet(cache, index=False, compression="zstd")
            atomic_bytes(profile_cache, json.dumps(obj, ensure_ascii=False).encode())
        print("FUEL YEAR", year, len(data), digest, flush=True)
        return data, obj, digest

    with ThreadPoolExecutor(max_workers=2) as pool:
        parts = list(pool.map(one_year, years))
    data = pd.concat([p[0] for p in parts], ignore_index=True)
    data = data[(data.date >= START) & (data.date < pd.Timestamp.now(tz="UTC").normalize())]
    profiles = {k: v for _, obj, _ in parts for k, v in obj["profiles"].items()}
    manifest = archive_raw_manifest([p[2] for p in parts],
                                   description="Original complete France annual fuel price, outage and closure XML archives from 2024")
    result = []

    def one_series(key, part):
        station, fuel = key
        series_id = f"france_fuel_{station}_{fuel}_reported_price"
        profile = dict(profiles[station])
        for coordinate in ("latitude", "longitude"):
            if not np.isfinite(profile[coordinate]):
                profile[coordinate] = None
        try:
            frame, frequency, aggregation = observed_quotes(part, published_frequency(series_id))
        except ValueError as exc:
            with LOCK:
                OMISSIONS.append({"provider": "france_fuel", "station": station, "fuel": fuel, "reason": str(exc)})
            return None
        name = str(part.fuel_name.iloc[-1])
        return save(
            series_id, frame,
            title=f"France {profile['city']} station {station}: {name} reported retail price",
            provider="France Ministry of Economy fuel-price reporting",
            category="retail_motor_fuel_prices", unit="EUR per litre", frequency=frequency,
            native_frequency="irregular_intraday", finer_frequency_available=True,
            finer_frequency_note="Original annual XML retains every reported update with local wall-clock timestamps. Initial coverage selects one last actual quote per observed day or week; refresh preserves that published cadence even when reporting density changes.",
            source_url="https://donnees.roulez-eco.fr/opendata/annee",
            geography=f"France — {profile['postcode']} {profile['city']}",
            geography_scope="non_us", country_codes=[FRENCH_OVERSEAS.get(str(profile["postcode"])[:3], "FR")],
            hypothesis="Local retail fuel repricing can reveal refinery disruption, freight-cost pressure, consumer purchasing-power shocks and regional competition before slower inflation statistics.",
            limitations="Only actual price updates in official archives. Missing days or weeks stay missing; no forward filling or interpolated prices. The latest reported quote within each retained period is selected, not a time-weighted or volume-weighted paid price. Annual files may reflect surviving/reporting stations and historical corrections. One frequency is retained per station/fuel. Source wall-clock timestamps lack an explicit timezone offset, so local calendar periods are represented at UTC midnight and are not UTC execution times.",
            raw_sha256=[doc_hash], raw_manifest_sha256=[manifest],
            dataset_identity=f"FR_FUEL:{station}:{fuel}:last_reported_price",
            documentation_url=documentation, source_station_id=station,
            source_fuel_id=fuel, source_fuel_name=name,
            source_station_metadata=profile, aggregation=aggregation,
            timestamp_semantics="Observed local day or Monday-start local week label at UTC midnight; only periods containing an actual source price report",
            expected_publication_lag="Live feed updates within about 10 minutes; annual archive refreshes daily at 05:00 with data through the preceding day. Historical files can be corrected.",
            license="Licence Ouverte / Open Licence",
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(one_series, key, part) for key, part in data.groupby(["station", "fuel"], sort=True)]
        for i, future in enumerate(as_completed(futures), 1):
            item = future.result()
            if item:
                result.append(item)
            if i % 1000 == 0:
                print("FUEL STAGED", i, len(result), flush=True)
    return result


def complete_event_panel(daily, start, end, value_column="value", required_frequency=None):
    """Count/sum events in a verified complete export; do not create sparse panels."""
    check_frequency_requirement(required_frequency)
    observed = daily[["date", value_column]].rename(columns={value_column: "value"}).copy()
    observed["date"] = pd.to_datetime(observed.date, utc=True).dt.floor("D")
    observed = observed.groupby("date", as_index=False).value.sum()
    if (observed.value < 0).any():
        raise ValueError("Negative aggregate event count/value")
    active_days = int((observed.value > 0).sum())
    active_weeks = weekly_labels(observed.loc[observed.value > 0, "date"]).nunique()
    if active_weeks < 26:
        return pd.DataFrame(columns=["date", "value"]), required_frequency or "weekly", active_days
    calendar = pd.date_range(start, end - pd.Timedelta(days=1), freq="D")
    data = observed.set_index("date").value.reindex(calendar, fill_value=0)
    if active_days >= 90 and required_frequency != "weekly":
        frame = pd.DataFrame({"date": calendar, "value": data.to_numpy()})
        return cadence_frame(frame, "daily", required_frequency), "daily", active_days
    if required_frequency == "daily":
        return pd.DataFrame(columns=["date", "value"]), "daily", active_days
    weekly = observed.assign(date=weekly_labels(observed.date)).groupby("date").value.sum()
    if int((weekly > 0).sum()) < 26:
        return pd.DataFrame(columns=["date", "value"]), "weekly", active_days
    # Exclude incomplete edge weeks. Every retained zero is backed by full source coverage.
    first = start + pd.Timedelta(days=(-start.weekday()) % 7)
    last = end - pd.Timedelta(days=end.weekday())
    weeks = pd.date_range(first, last - pd.Timedelta(days=7), freq="7D")
    weekly = weekly.reindex(weeks, fill_value=0)
    if int((weekly > 0).sum()) < 26:
        return pd.DataFrame(columns=["date", "value"]), "weekly", active_days
    frame = pd.DataFrame({"date": weeks, "value": weekly.to_numpy()})
    return cadence_frame(frame, "weekly", required_frequency), "weekly", active_days


BODACC_CATEGORIES = {
    "creation": "business_establishment_creations",
    "immatriculation": "business_registrations",
    "radiation": "business_deregistrations",
    "collective": "business_insolvency_notices",
    "conciliation": "business_debt_conciliation",
    "retablissement_professionnel": "professional_debt_rehabilitation",
    "modification": "business_registry_changes",
    "dpc": "corporate_accounts_filings",
    "vente": "business_ownership_transfers",
}
FRENCH_OVERSEAS = {
    "971": "GP", "972": "MQ", "973": "GF", "974": "RE", "975": "PM",
    "976": "YT", "977": "BL", "978": "MF", "986": "WF", "987": "PF", "988": "NC",
}


def french_department_country(department):
    if department in FRENCH_OVERSEAS:
        return FRENCH_OVERSEAS[department]
    if re.fullmatch(r"(0[1-9]|[1-8][0-9]|9[0-5]|2[AB])", str(department)):
        return "FR"
    raise ValueError(f"Unknown French department geography: {department}")


def france_fuel_disruptions(fetch):
    documentation = "https://www.prix-carburants.gouv.fr/rubrique/opendata/"
    doc_hash = fetch.get(documentation, max_age_hours=720)[1]
    profiles, outages, closures, hashes = {}, [], [], []
    for year in range(2024, pd.Timestamp.now(tz="UTC").year + 1):
        raw, digest = fetch.get(f"https://donnees.roulez-eco.fr/opendata/annee/{year}",
                                timeout=300, max_age_hours=24)
        cache = ROOT / "archive/commerce5_fuel" / (digest + ".json")
        if cache.exists():
            obj = json.loads(cache.read_text())
        else:
            _, p, o, c = parse_french_fuel(raw)
            obj = {"profiles": p, "outages": o, "closures": c}
        profiles.update(obj["profiles"])
        outages.extend(obj["outages"])
        closures.extend(obj["closures"])
        hashes.append(digest)
    end = pd.Timestamp.now(tz="UTC").normalize()
    manifest = archive_raw_manifest(hashes, description="Original complete annual France fuel archives supporting reported temporary shortage/closure start counts")
    fuel_names = {"1": "Gazole", "2": "SP95", "3": "E85", "4": "GPLc", "5": "E10", "6": "SP98"}
    frames = [
        ("stockout_starts", pd.DataFrame(outages, columns=["station", "fuel", "fuel_name", "date", "end", "kind"])),
        ("closure_starts", pd.DataFrame(closures, columns=["station", "date", "end", "kind"]).assign(fuel="all")),
    ]
    result = []
    for metric, records in frames:
        records = records[records.kind == ("temporaire" if metric == "stockout_starts" else "T")].copy()
        records["date"] = pd.to_datetime(records.date, utc=True, format="mixed")
        records = records[(records.date >= START) & (records.date < end)]
        # Annual snapshots can repeat a still-open interval and later revise its end.
        records = records.drop_duplicates(["station", "fuel", "date"])
        records["date"] = records.date.dt.floor("D")
        records["postal_area"] = records.station.map(
            lambda x: str(profiles[x]["postcode"])[:3]
            if str(profiles[x]["postcode"]).startswith(("97", "98"))
            else str(profiles[x]["postcode"])[:2])
        for (area, fuel), part in records.groupby(["postal_area", "fuel"], sort=True):
            if fuel != "all" and fuel not in fuel_names:
                continue
            try:
                country = "FR" if area == "20" else french_department_country(area)
            except ValueError:
                continue
            counts = part.groupby("date").size().reset_index(name="value")
            series_id = f"france_fuel_postal_{slug(area)}_{fuel}_{metric}"
            frame, frequency, active_days = complete_event_panel(
                counts, START, end, required_frequency=published_frequency(series_id))
            title = "temporary fuel stockout starts" if metric == "stockout_starts" else "temporary service-station closure starts"
            item = save(
                series_id, frame,
                title=f"France postal area {area}: {fuel_names.get(fuel, 'all fuels')} {title}",
                provider="France Ministry of Economy fuel-price reporting",
                category="retail_fuel_stockout_reports" if metric == "stockout_starts" else "service_station_closure_reports",
                unit="reported interval starts", frequency=frequency, native_frequency="irregular_intraday",
                source_url="https://donnees.roulez-eco.fr/opendata/annee", geography=f"France jurisdiction — postal prefix {area}",
                geography_scope="non_us", country_codes=[country],
                hypothesis="Retail fuel shortages and service-station closures can indicate distribution bottlenecks, refinery strikes, panic buying and transportation disruption before slower fuel-volume or inflation releases.",
                limitations="Counts reported temporary outage/closure interval starts, not outage-days, active station counts, unmet litres or economic losses. Permanent non-distribution and permanent closures are excluded. Repeated annual copies of an identical station/fuel/start event are counted once, including when its end date changes. Counts refer only to events retained in current official annual archives; station reporting, retrospective corrections and survivorship may affect coverage. Postal prefix 20 combines Corsican postal areas and is not an administrative department. Zero means no matching event in the downloaded complete archive.",
                raw_sha256=[doc_hash], raw_manifest_sha256=[manifest],
                dataset_identity=f"FR_FUEL:postal_prefix={area}:fuel={fuel}:{metric}:temporary",
                documentation_url=documentation, source_postal_prefix=area, source_fuel_id=fuel,
                aggregation="Count distinct station/fuel/start-time temporary events by local start day; weekly totals use complete Monday-Sunday weeks",
                timestamp_semantics="Reported local interval-start date represented at UTC midnight; weekly labels denote Monday period start",
                minimum_active_days=active_days, finer_frequency_available=True,
                expected_publication_lag="Original live source updates within about 10 minutes; annual archives refresh daily at 05:00 for the preceding day. An interval start can be reported retrospectively.",
                license="Licence Ouverte / Open Licence",
            )
            if item:
                result.append(item)
    return result


def bodacc(fetch):
    base = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales"
    documentation = "https://www.data.gouv.fr/dataservices/api-bulletin-officiel-des-annonces-civiles-et-commerciales-bodacc"
    docs = [fetch.get(base, max_age_hours=720)[1], fetch.get(documentation, max_age_hours=720)[1]]
    end = pd.Timestamp.now(tz="UTC").normalize()
    fields = "dateparution,numerodepartement,familleavis,familleavis_lib"

    def one_month(begin):
        finish = min(begin + pd.offsets.MonthBegin(1), end)
        where = (f"dateparution >= '{begin.date()}' AND dateparution < '{finish.date()}'"
                 " AND typeavis='annonce'")
        params = {"select": fields + ",count(id) as notices", "where": where, "group_by": fields}
        raw, digest = fetch.get(base + "/exports/json", params, timeout=180, max_age_hours=720)
        frame = pd.DataFrame(json.loads(raw))
        if frame.empty:
            raise ValueError("Empty BODACC source month")
        count_raw, count_hash = fetch.get(
            base + "/records", {"select": "count(id) as notices", "where": where, "limit": 1},
            timeout=120, max_age_hours=720)
        total = int(json.loads(count_raw)["results"][0]["notices"])
        if int(frame.notices.sum()) != total:
            raise ValueError("BODACC aggregate export is incomplete or changed during acquisition")
        frame["date"] = pd.to_datetime(frame.dateparution, utc=True)
        if (frame.date < begin).any() or (frame.date >= finish).any():
            raise ValueError("BODACC ignored date filter")
        print("BODACC MONTH", begin.date(), len(frame), total, flush=True)
        time.sleep(.35)
        return frame, digest, count_hash

    with ThreadPoolExecutor(max_workers=2) as pool:
        parts = list(pool.map(one_month, pd.date_range(START, end - pd.Timedelta(days=1), freq="MS")))
    data = pd.concat([p[0] for p in parts], ignore_index=True)
    if data.duplicated(["date", "numerodepartement", "familleavis", "familleavis_lib"]).any():
        raise ValueError("Overlapping BODACC grouped cells")
    manifest = archive_raw_manifest([h for _, *hashes in parts for h in hashes],
                                   description="Complete monthly BODACC initial-notice daily department/family aggregate exports and matching total-count receipts")
    result = []
    for (department, family), part in data.groupby(["numerodepartement", "familleavis"], sort=True):
        if family not in BODACC_CATEGORIES:
            OMISSIONS.append({"provider": "bodacc", "family": family, "reason": "Unmapped notice family"})
            continue
        try:
            country = french_department_country(str(department))
        except ValueError as exc:
            OMISSIONS.append({"provider": "bodacc", "department": department, "reason": str(exc)})
            continue
        series_id = f"fr_bodacc_{slug(department)}_{family}_initial_notices"
        frame, frequency, active_days = complete_event_panel(
            part, START, end, "notices", required_frequency=published_frequency(series_id))
        item = save(
            series_id, frame,
            title=f"France BODACC department {department}: {part.familleavis_lib.iloc[-1]} initial notices",
            provider="France DILA BODACC commercial notices", category=BODACC_CATEGORIES[family],
            unit="published initial notices", frequency=frequency, native_frequency="daily",
            finer_frequency_available=frequency == "weekly",
            source_url=base + "/exports/json", geography=f"French jurisdiction — department {department}",
            geography_scope="non_us", country_codes=[country],
            hypothesis="Business registrations, insolvency proceedings, debt conciliation, ownership transfers and account-filing activity can reveal changes in regional business confidence, credit stress, restructuring and private-sector activity.",
            limitations="Counts of initial published legal notices, not unique firms or unique underlying legal events. Corrections and cancellation notices are excluded. Publication day is used rather than the potentially earlier underlying decision/transaction date. Zero counts are included only within complete monthly exports reconciled to official source totals. Fewer than 90 positive days use weekly totals only when at least 26 weeks contain reports. Historical source corrections remain possible. Departments in overseas territories use their ISO territory codes.",
            raw_sha256=docs, raw_manifest_sha256=[manifest],
            dataset_identity=f"FR_BODACC:department={department}:family={family}:type=annonce:count",
            documentation_url=documentation, source_department_code=str(department),
            source_notice_family=family, minimum_active_days=active_days,
            aggregation="Count initial notices by publication day; weekly panels sum complete Monday-Sunday publication weeks",
            timestamp_semantics="Official notice publication-date label at UTC midnight; weekly labels denote Monday period start",
            expected_publication_lag="Official notices are published on the displayed publication day; the public API is refreshed daily. Legal events can precede publication substantially.",
            license="Licence Ouverte 2.0",
        )
        if item:
            result.append(item)
    return result


PPD_COLUMNS = [
    "transaction_id", "price", "date", "postcode", "property_type", "new_build", "tenure",
    "paon", "saon", "street", "locality", "town", "district", "county", "category", "status",
]
PROPERTY_TYPES = {"D": "detached houses", "S": "semi-detached houses", "T": "terraced houses",
                  "F": "flats and maisonettes", "O": "other property"}


def parse_ppd(raw):
    data = pd.read_csv(io.BytesIO(raw), header=None, names=PPD_COLUMNS,
                       usecols=[0, 1, 2, 3, 4, 5, 6, 12, 13, 14, 15],
                       dtype=str, keep_default_na=False)
    if data.transaction_id.duplicated().any():
        raise ValueError("Duplicate unique sale IDs in a complete annual property file")
    if not set(data.property_type) <= set(PROPERTY_TYPES):
        raise ValueError("Unknown Land Registry property type")
    if not set(data.category) <= {"A", "B"}:
        raise ValueError("Unknown Price Paid Data category")
    if not set(data.status) <= {"A", ""}:
        raise ValueError("Annual property file unexpectedly contains unapplied change/delete records")
    data["date"] = pd.to_datetime(data.date, utc=True)
    data["price"] = pd.to_numeric(data.price, errors="raise")
    if (data.price <= 0).any():
        raise ValueError("Nonpositive property transaction price")
    # Public panels retain outward postal districts, not individual addresses.
    data["postal_district"] = data.postcode.str.extract(r"^([A-Z]{1,2}[0-9][A-Z0-9]?) [0-9][A-Z]{2}$")[0]
    return data


def property_medians(part, required_frequency=None):
    check_frequency_requirement(required_frequency)
    daily = part.groupby("date", as_index=False).price.median().rename(columns={"price": "value"})
    daily = normalize_frame(daily)
    if required_frequency != "weekly" and qualifies_research(dict(coverage(daily), frequency="daily")):
        return daily, "daily"
    if required_frequency == "daily":
        return daily.iloc[:0], "daily"
    weekly = part.assign(date=weekly_labels(part.date)).groupby("date", as_index=False).price.median()
    return cadence_frame(weekly.rename(columns={"price": "value"}), "weekly", required_frequency), "weekly"


def uk_property(fetch):
    documentation = "https://www.gov.uk/guidance/about-the-price-paid-data"
    downloads = "https://www.gov.uk/government/statistical-data-sets/price-paid-data-yearly-file"
    docs = [fetch.get(u, max_age_hours=720)[1] for u in (documentation, downloads)]
    years = range(2024, pd.Timestamp.now(tz="UTC").year + 1)

    def one_year(year):
        url = f"https://price-paid-data.publicdata.landregistry.gov.uk/pp-{year}.csv"
        raw, digest = fetch.get(url, timeout=300, max_age_hours=720)
        cache = ROOT / "archive/commerce5_property" / (digest + ".parquet")
        if cache.exists():
            data = pd.read_parquet(cache)
        else:
            data = parse_ppd(raw)
            cache.parent.mkdir(parents=True, exist_ok=True)
            data.to_parquet(cache, index=False, compression="zstd")
        if not data.date.dt.year.eq(year).all():
            raise ValueError("Annual Land Registry file has inconsistent transfer years")
        print("PROPERTY YEAR", year, len(data), data.date.min(), data.date.max(), flush=True)
        return data, digest

    with ThreadPoolExecutor(max_workers=2) as pool:
        parts = list(pool.map(one_year, years))
    data = pd.concat([p[0] for p in parts], ignore_index=True)
    if data.transaction_id.duplicated().any():
        raise ValueError("Overlapping annual Land Registry sale IDs")
    data = data[(data.date >= START) & (data.date < pd.Timestamp.now(tz="UTC").normalize())]
    end = data.date.max().normalize() + pd.Timedelta(days=1)
    manifest = archive_raw_manifest([p[1] for p in parts],
                                   description="Complete current HM Land Registry annual Price Paid Data files from 2024 with source corrections already applied")
    result = []

    def one_group(key, part):
        area, kind, category, new = key
        name = PROPERTY_TYPES[kind]
        cat_name = "standard residential" if category == "A" else "additional-category"
        age = "new-build" if new == "Y" else "existing"
        stem = f"uk_ppd_{slug(area)}_{kind.lower()}_{category.lower()}_{new.lower()}"
        common = dict(
            provider="HM Land Registry property transactions",
            native_frequency="daily", source_url=downloads,
            geography=f"England and Wales — postal district {area}",
            geography_scope="non_us", country_codes=["GB"],
            hypothesis="Local property turnover, completed-sale prices and new-build transactions can indicate household credit conditions, wealth effects, housebuilder demand and construction-linked consumption.",
            limitations="Latest revised transaction history grouped by transfer/deed date, not a historical publication-time panel. Registration typically follows sale by 2 weeks to 2 months and occasionally longer; the latest two months are materially incomplete. Annual source files already apply amendments/deletions. Category A denotes standard single residential transactions. Category B combines other transfers including some repossessions, corporate purchases, identifiable buy-to-lets and other property; it does not separately identify distressed sales. Prices are composition-sensitive medians of observed transactions, not repeat-sales indices. Missing price periods remain missing. Count zero means no transaction in the complete current source snapshot, not proof that no sale occurred.",
            raw_sha256=docs, raw_manifest_sha256=[manifest],
            documentation_url=documentation, source_postal_district=area,
            source_property_type=kind, source_transaction_category=category,
            source_new_build=new, source_districts=sorted(set(part.district)),
            timestamp_semantics="Local transfer/deed-date label at UTC midnight, or Monday-start local week label; not first-publication time",
            expected_publication_lag="Monthly publication; source reports typical sale-to-registration lag of 2 weeks to 2 months, sometimes longer. Most recent two months are incomplete.",
            license="Open Government Licence v3.0; address-derived fields subject to source Royal Mail/Ordnance Survey conditions",
            attribution="Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.",
        )
        out = []
        counts = part.groupby("date").size().reset_index(name="value")
        count_frame, count_freq, active_days = complete_event_panel(
            counts, START, end, required_frequency=published_frequency(stem + "_registered_sales"))
        item = save(
            stem + "_registered_sales", count_frame,
            title=f"England/Wales {area}: {age} {cat_name} {name}, registered sales",
            category="new_build_property_transactions" if new == "Y" else "property_transaction_activity",
            unit="registered transactions", frequency=count_freq,
            dataset_identity=f"GB_PPD:{area}:{kind}:{category}:{new}:transaction_count",
            aggregation="Count unique source transactions by deed day; weekly panels sum complete Monday-Sunday deed-date weeks",
            minimum_active_days=active_days, finer_frequency_available=count_freq == "weekly", **common,
        )
        if item:
            out.append(item)
        price_frame, price_freq = property_medians(part, published_frequency(stem + "_median_paid_price"))
        item = save(
            stem + "_median_paid_price", price_frame,
            title=f"England/Wales {area}: {age} {cat_name} {name}, median paid price",
            category="new_build_property_prices" if new == "Y" else "completed_property_sale_prices",
            unit="GBP per transaction", frequency=price_freq,
            dataset_identity=f"GB_PPD:{area}:{kind}:{category}:{new}:median_price",
            aggregation="Median source deed price of actual transactions within each observed local day or Monday-Sunday local week; no empty-period interpolation",
            finer_frequency_available=price_freq == "weekly", **common,
        )
        if item:
            out.append(item)
        return out

    groups = data.dropna(subset=["postal_district"]).groupby(
        ["postal_district", "property_type", "category", "new_build"], sort=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(one_group, key, part) for key, part in groups if key[3] in {"Y", "N"}]
        for i, future in enumerate(as_completed(futures), 1):
            result.extend(future.result())
            if i % 1000 == 0:
                print("PROPERTY STAGED", i, len(result), flush=True)
    return result


def clean_secop_records(data):
    """Collapse identical duplicate source records; omit contradictory contract IDs."""
    data = data.copy()
    data["date"] = pd.to_datetime(data.fecha_de_firma, utc=True).dt.floor("D")
    data["value"] = pd.to_numeric(data.valor_del_contrato, errors="coerce")
    data["family"] = data.codigo_de_categoria_principal.str.extract(r"^(?:V[0-9]+\.)?([0-9]{4})[0-9]{4}$")[0]
    compare = ["date", "departamento", "sector", "codigo_de_categoria_principal", "tipo_de_contrato", "value"]
    duplicates = data[data.id_contrato.duplicated(keep=False)]
    bad = set()
    if not duplicates.empty:
        bad = set(duplicates.groupby("id_contrato")[compare].nunique(dropna=False).max(axis=1).loc[lambda x: x > 1].index)
    data = data[~data.id_contrato.isin(bad)].drop_duplicates("id_contrato")
    data = data[data.id_contrato.notna() & data.departamento.notna() & data.family.notna()]
    return data, sorted(bad)


def colombia_procurement(fetch):
    url = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
    metadata_url = "https://www.datos.gov.co/api/views/jbjy-vk9h.json"
    documentation = "https://www.colombiacompra.gov.co/archivos/conjunto-datos-abier/contratos-electronicos"
    classification = "https://operaciones.colombiacompra.gov.co/clasificador-de-bienes-y-Servicios"
    docs = [fetch.get(u, max_age_hours=720)[1] for u in (metadata_url, documentation, classification)]
    metadata = json.loads((ROOT / "archive/raw" / (docs[0] + ".bin")).read_bytes())
    value_field = next(c for c in metadata["columns"] if c["fieldName"] == "valor_del_contrato")
    if value_field.get("format", {}).get("currencyStyle") != "COP":
        raise ValueError("Official SECOP contract-value currency no longer verifies as COP")
    end = pd.Timestamp.now(tz="UTC").normalize()
    select = (":id as source_row,id_contrato,fecha_de_firma,departamento,sector,"
              "codigo_de_categoria_principal,tipo_de_contrato,valor_del_contrato")

    def one_month(begin):
        finish = min(begin + pd.offsets.MonthBegin(1), end)
        where = f"fecha_de_firma >= '{begin.date()}T00:00:00' AND fecha_de_firma < '{finish.date()}T00:00:00'"
        raw, count_hash = fetch.get(url, {"$select": "count(*) as rows", "$where": where},
                                   timeout=180, max_age_hours=720)
        expected = int(json.loads(raw)[0]["rows"])
        pieces, hashes = [], [count_hash]
        for offset in range(0, expected, 50000):
            params = {"$select": select, "$where": where, "$order": ":id", "$limit": 50000, "$offset": offset}
            raw, digest = fetch.get(url, params, timeout=240, max_age_hours=720)
            frame = pd.DataFrame(json.loads(raw))
            if len(frame) != min(50000, expected - offset):
                raise ValueError(f"SECOP incomplete month/page {begin.date()}/{offset}")
            pieces.append(frame)
            hashes.append(digest)
            print("SECOP PAGE", begin.date(), offset, len(frame), flush=True)
            time.sleep(.5)
        if not pieces:
            raise ValueError("Empty SECOP source month")
        data = pd.concat(pieces, ignore_index=True)
        if len(data) != expected or data.source_row.duplicated().any():
            raise ValueError("SECOP paginated row count or unique source-row mismatch")
        dates = pd.to_datetime(data.fecha_de_firma, utc=True)
        if (dates < begin).any() or (dates >= finish).any():
            raise ValueError("SECOP source ignored month filter")
        return data, hashes

    with ThreadPoolExecutor(max_workers=2) as pool:
        parts = list(pool.map(one_month, pd.date_range(START, end - pd.Timedelta(days=1), freq="MS")))
    latest_metadata_raw, latest_metadata_hash = fetch.get(metadata_url, max_age_hours=0, timeout=180)
    initial_metadata = json.loads((ROOT / "archive/raw" / (docs[0] + ".bin")).read_bytes())
    latest_metadata = json.loads(latest_metadata_raw)
    if latest_metadata.get("rowsUpdatedAt") != initial_metadata.get("rowsUpdatedAt"):
        raise ValueError("SECOP source edition changed during paginated acquisition; retry with fresh cache")
    docs.append(latest_metadata_hash)
    data = pd.concat([p[0] for p in parts], ignore_index=True)
    if data.source_row.duplicated().any():
        raise ValueError("SECOP row IDs overlap across source months")
    original_rows = len(data)
    data, contradictory = clean_secop_records(data)
    OMISSIONS.extend({"provider": "colombia_procurement", "contract_id": value,
                      "reason": "Conflicting duplicate source contract ID; all copies omitted"}
                     for value in contradictory)
    print("SECOP CLEAN", original_rows, len(data), "conflicting IDs", len(contradictory), flush=True)
    manifest = archive_raw_manifest([h for _, hashes in parts for h in hashes],
                                   description="Complete paginated Colombian SECOP II contract records selected by signature month, with source row-count receipts")
    result = []
    for (department, family), part in data.groupby(["departamento", "family"], sort=True):
        # Hash protects distinct official spellings which transliterate to the same slug.
        dslug = slug(department) + "_" + hashlib.sha256(department.encode()).hexdigest()[:7]
        common = dict(
            provider="Colombia Compra Eficiente SECOP II contracts",
            native_frequency="daily", source_url=url,
            geography=f"Colombia — contracting authority department {department}",
            geography_scope="non_us", country_codes=["CO"],
            hypothesis="Government contract-signing activity can reveal changes in demand for construction, IT, professional services, medical supplies, transport and other procurement categories before vendor revenue or fiscal statistics.",
            limitations="Registered contracts grouped by reported signature date and the contracting authority's department; this may differ from the place of delivery. Values are the current recorded nominal contract face values of the signature-date cohort, not payments, supplier revenue or historical as-of commitments. Current records can incorporate amendments and later changes. Source duplicates with identical relevant fields are collapsed by contract ID; conflicting IDs are excluded entirely. Missing/negative face values prevent publication of that department/family value series. UNSPSC four-digit families combine the source eight-digit commodity codes. Zeros refer to a complete current export of records, not all procurement systems or all economic activity.",
            raw_sha256=docs, raw_manifest_sha256=[manifest],
            documentation_url=documentation, classification_documentation_url=classification,
            unit_documentation_url=metadata_url, source_currency="COP",
            source_department=department, source_unspsc_family=family,
            source_unspsc_commodity_codes=sorted(set(part.codigo_de_categoria_principal)),
            source_buyer_sectors=sorted(set(part.sector.dropna())),
            source_contract_types=sorted(set(part.tipo_de_contrato.dropna())),
            timestamp_semantics="Reported local contract signature-day label at UTC midnight; weekly labels denote Monday-start signature-date weeks",
            expected_publication_lag="Public source is refreshed daily from SECOP II; entity reporting delays, amendments, cancellations and historical corrections can change past signature-date cohorts.",
            license="Colombia public open data; source attribution retained",
        )
        counts = part.groupby("date").size().reset_index(name="value")
        count_id = f"co_secop_{dslug}_unspsc_{family}_signed_contracts"
        frame, frequency, active_days = complete_event_panel(
            counts, START, end, required_frequency=published_frequency(count_id))
        item = save(
            count_id, frame,
            title=f"Colombia {department}: UNSPSC family {family}, registered contract signatures",
            category="public_procurement_contract_signatures", unit="distinct contract IDs",
            frequency=frequency, finer_frequency_available=frequency == "weekly",
            dataset_identity=f"CO_SECOPII:{department}:UNSPSC_FAMILY={family}:contract_count",
            aggregation="Count distinct nonconflicting contract IDs by reported signature day; weekly panels sum complete calendar weeks",
            minimum_active_days=active_days, **common,
        )
        if item:
            result.append(item)
        if (part.value.isna().any() or (part.value < 0).any()
                or (part.value > 1e15).any() or not np.isfinite(part.value).all()):
            OMISSIONS.append({"provider": "colombia_procurement", "department": department,
                              "family": family, "reason": "Missing/negative/nonfinite or over 1e15 COP individual contract face value; full value panel conservatively omitted"})
            continue
        daily = part.groupby("date", as_index=False).value.sum()
        value_id = f"co_secop_{dslug}_unspsc_{family}_contract_face_value"
        frame, frequency, active_days = complete_event_panel(
            daily, START, end, required_frequency=published_frequency(value_id))
        item = save(
            value_id, frame,
            title=f"Colombia {department}: UNSPSC family {family}, current face value of contracts by signature date",
            category="public_procurement_contract_value", unit="COP",
            frequency=frequency, finer_frequency_available=frequency == "weekly",
            dataset_identity=f"CO_SECOPII:{department}:UNSPSC_FAMILY={family}:contract_face_value",
            aggregation="Sum current recorded contract face values by signature day; weekly panels sum complete calendar weeks",
            minimum_active_days=active_days, **common,
        )
        if item:
            result.append(item)
    return result


PROVIDERS = {"france_fuel": france_fuel, "france_fuel_disruptions": france_fuel_disruptions,
             "bodacc": bodacc, "uk_property": uk_property,
             "colombia_procurement": colombia_procurement}


def run(provider=None, refresh=False):
    # A caller can publish batch 7 between invocations in the same Python process.
    baseline.cache_clear()
    fetch = Fetcher(refresh=refresh)
    names = [provider] if provider else list(PROVIDERS)
    results = []
    for name in names:
        started = pd.Timestamp.now(tz="UTC").isoformat()
        items = PROVIDERS[name](fetch)
        report = {"provider": name, "started_at": started,
                  "finished_at": pd.Timestamp.now(tz="UTC").isoformat(),
                  "series": len(items), "observations": sum(x["rows"] for x in items),
                  "frequency_counts": pd.Series([x["frequency"] for x in items]).value_counts().to_dict(),
                  "omissions": [x for x in OMISSIONS if x.get("provider") == name]}
        atomic_bytes(ROOT / "logs" / f"commerce5_{name}.json", json.dumps(report, indent=2).encode())
        print(json.dumps(report, ensure_ascii=False), flush=True)
        results.extend(items)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=PROVIDERS)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    run(args.provider, args.refresh)
