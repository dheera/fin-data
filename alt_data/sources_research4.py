#!/usr/bin/env python3
"""International news risk, transport, commerce and positioning histories."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
from pathlib import Path
import re
import time
import zipfile

import numpy as np
import pandas as pd
from country_codes import normalize_country_codes

from expansion_common import (
    ROOT, Fetcher, archive_raw_manifest, atomic_bytes, coverage,
    normalize_frame, qualifies_research, save_candidate,
)

# ISO 3166-1 alpha-3 country filter; excludes CAMEO aggregate-region codes.
ISO3_COUNTRIES = frozenset((
    "ABW AFG AGO AIA ALA ALB AND ARE ARG ARM ASM ATA ATF ATG AUS AUT AZE BDI BEL BEN "
    "BES BFA BGD BGR BHR BHS BIH BLM BLR BLZ BMU BOL BRA BRB BRN BTN BVT BWA CAF CAN "
    "CCK CHE CHL CHN CIV CMR COD COG COK COL COM CPV CRI CUB CUW CXR CYM CYP CZE DEU "
    "DJI DMA DNK DOM DZA ECU EGY ERI ESH ESP EST ETH FIN FJI FLK FRA FRO FSM GAB GBR "
    "GEO GGY GHA GIB GIN GLP GMB GNB GNQ GRC GRD GRL GTM GUF GUM GUY HKG HMD HND HRV "
    "HTI HUN IDN IMN IND IOT IRL IRN IRQ ISL ISR ITA JAM JEY JOR JPN KAZ KEN KGZ KHM "
    "KIR KNA KOR KWT LAO LBN LBR LBY LCA LIE LKA LSO LTU LUX LVA MAC MAF MAR MCO MDA "
    "MDG MDV MEX MHL MKD MLI MLT MMR MNE MNG MNP MOZ MRT MSR MTQ MUS MWI MYS MYT NAM "
    "NCL NER NFK NGA NIC NIU NLD NOR NPL NRU NZL OMN PAK PAN PCN PER PHL PLW PNG POL "
    "PRI PRK PRT PRY PSE PYF QAT REU ROU RUS RWA SAU SDN SEN SGP SGS SHN SJM SLB SLE "
    "SLV SMR SOM SPM SRB SSD STP SUR SVK SVN SWE SWZ SXM SYC SYR TCA TCD TGO THA TJK "
    "TKL TKM TLS TON TTO TUN TUR TUV TWN TZA UGA UKR UMI URY USA UZB VAT VCT VEN VGB "
    "VIR VNM VUT WLF WSM YEM ZAF ZMB ZWE "
).split()) | {"XKX"}

GROUP = "research4"
START = "2024-01-01"
FAILURES = []
OMISSIONS = []
SEEN = set()
BASELINE = None


def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def baseline():
    global BASELINE
    if BASELINE is None:
        frame = pd.read_parquet(ROOT / "catalog.parquet")
        pinned = set(frame.loc[frame.batch == 6, "series_id"])
        frame = frame[frame.batch < 6]
        BASELINE = {key: set(frame[key].dropna())
                    for key in ["series_id", "dataset_identity", "content_sha256"]}
        BASELINE["pinned_ids"] = pinned
    return BASELINE


def clean(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                          "value": pd.to_numeric(values, errors="raise")}).dropna()
    if frame.empty:
        return frame
    if (frame.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting source observations at the same date")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    old = baseline()
    if old["pinned_ids"] and sid not in old["pinned_ids"]:
        return None
    if frame.empty or frame.value.nunique() < 2:
        return None
    frame = normalize_frame(frame)
    if not qualifies_research(dict(coverage(frame), frequency=meta["frequency"])):
        return None
    identity = meta["dataset_identity"]
    if sid in old["series_id"] or identity in old["dataset_identity"]:
        return None
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    if digest in old["content_sha256"] or digest in SEEN:
        return None
    SEEN.add(digest)
    previous_standard = meta.pop("country_code_standard", None)
    if previous_standard:
        meta["source_country_code_standard"] = previous_standard
    meta["country_codes"] = normalize_country_codes(meta.get("country_codes", []))
    meta["country_code_standard"] = ("ISO 3166-1 alpha-2; XK user-assigned Kosovo extension"
                                      if "XK" in meta["country_codes"] else "ISO 3166-1 alpha-2")
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("finer_frequency_available", False)
    result = save_candidate(GROUP, sid, frame, **meta)
    print("OK", sid, len(frame), flush=True)
    return result


GDELT_LOOKUPS = {
    "header": "CSV.header.dailyupdates.txt",
    "fips": "FIPS.country.txt",
    "events": "CAMEO.eventcodes.txt",
    "cameo": "CAMEO.country.txt",
    "types": "CAMEO.type.txt",
}
QUADS = {"1": "verbal cooperation", "2": "material cooperation",
         "3": "verbal conflict", "4": "material conflict"}
SECTOR_CODES = {
    "AGR": "agriculture", "BUS": "business", "COP": "police",
    "CVL": "civilians", "EDU": "education", "ELI": "elites",
    "ENV": "environment", "GOV": "government", "HLH": "health",
    "IGO": "intergovernmental organizations", "JUD": "judiciary",
    "LAB": "labor", "LEG": "legislature", "MED": "media",
    "MIL": "military", "NGO": "nongovernmental organizations",
    "OPP": "opposition", "REF": "refugees", "REL": "religion",
    "SPY": "intelligence", "UAF": "unidentified armed forces",
}


def gdelt_aggregate(raw, columns, day, fips, countries):
    wanted = ["GLOBALEVENTID", "Actor1CountryCode", "Actor2CountryCode",
              "Actor1Type1Code", "EventBaseCode", "QuadClass", "AvgTone",
              "ActionGeo_CountryCode", "DATEADDED"]
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = [n for n in archive.namelist() if n.endswith(".CSV")]
        if len(members) != 1:
            raise ValueError("GDELT daily archive member layout changed")
        frame = pd.read_csv(archive.open(members[0]), sep="\t", header=None,
                            names=columns, usecols=wanted, dtype=str,
                            keep_default_na=False)
    if frame.empty:
        raise ValueError("Empty GDELT archive")
    if not frame.DATEADDED.str.startswith(day.replace("-", "")).all():
        raise ValueError("GDELT archive date does not match source discovery date")
    if frame.GLOBALEVENTID.duplicated().any():
        raise ValueError("Repeated GDELT global event ID within daily archive")
    frame["AvgTone"] = pd.to_numeric(frame.AvgTone, errors="raise")
    if not np.isfinite(frame.AvgTone).all() or frame.AvgTone.abs().gt(100).any():
        raise ValueError("Invalid GDELT tone score")
    frame["EventBaseCode"] = frame.EventBaseCode.str.zfill(3)
    parts = []

    def aggregate(data, fields, kind):
        if data.empty:
            return
        table = data.groupby(fields).agg(count=("GLOBALEVENTID", "size"),
                                        tone=("AvgTone", "mean")).reset_index()
        table = table.rename(columns=dict(zip(fields, ["country", "detail", "detail2"])))
        if "detail2" not in table:
            table["detail2"] = ""
        table["kind"] = kind
        table["date"] = day
        parts.append(table[["date", "kind", "country", "detail", "detail2", "count", "tone"]])

    geo = frame[frame.ActionGeo_CountryCode.isin(fips) & frame.ActionGeo_CountryCode.ne("US")]
    aggregate(geo, ["ActionGeo_CountryCode", "EventBaseCode"], "location_event")
    sector = frame[frame.Actor1CountryCode.isin(countries) & frame.Actor1CountryCode.ne("USA")
                   & frame.Actor1Type1Code.isin(SECTOR_CODES)]
    aggregate(sector, ["Actor1CountryCode", "Actor1Type1Code"], "country_sector")
    pair = frame[frame.Actor1CountryCode.isin(countries) & frame.Actor2CountryCode.isin(countries)
                 & frame.Actor1CountryCode.ne(frame.Actor2CountryCode) & frame.QuadClass.isin(QUADS)]
    aggregate(pair, ["Actor1CountryCode", "Actor2CountryCode", "QuadClass"], "country_pair")
    return pd.concat(parts, ignore_index=True)


def gdelt_panel(rows, metric, completed_dates):
    if metric == "count":
        values = rows.set_index("date")["count"].reindex(completed_dates, fill_value=0)
        return clean(values.index, values.to_numpy())
    return clean(rows.date, rows.tone)


def gdelt(fetch):
    docs, docs_hash = fetch.get("https://gdeltproject.org/data.html", max_age_hours=720)
    lookups, lookup_hashes = {}, [docs_hash]
    # Primary names are linked from the provider documentation.
    for name, filename in GDELT_LOOKUPS.items():
        if name == "types":
            continue  # Explicit source actor types above; labels are explanatory.
        raw, digest = fetch.get("https://www.gdeltproject.org/data/lookups/" + filename,
                                max_age_hours=720)
        lookups[name] = raw.decode()
        lookup_hashes.append(digest)
    columns = lookups["header"].strip().split("\t")
    if len(columns) != 58:
        raise ValueError("GDELT 1.0 column count changed")
    fips = dict(line.split("\t", 1) for line in lookups["fips"].splitlines() if "\t" in line)
    names = dict(line.split("\t", 1) for line in lookups["cameo"].splitlines()[1:] if "\t" in line)
    events = dict(line.split("\t", 1) for line in lookups["events"].splitlines()[1:] if "\t" in line)
    # The old GDELT label file mislabels e.g. GV (Guinea) and LO (Slovakia).
    # Keep native FIPS IDs but use the published GeoNames FIPS/ISO crosswalk.
    raw, crosswalk_hash = fetch.get("https://download.geonames.org/export/dump/countryInfo.txt",
                                    max_age_hours=720)
    lookup_hashes.append(crosswalk_hash)
    geography = [line.split("\t") for line in raw.decode().splitlines()
                 if line and not line.startswith("#")]
    geo_iso = {row[3]: row[1] for row in geography
               if row[3] and row[1] in ISO3_COUNTRIES}
    geo_names = {row[3]: row[4] for row in geography if row[3] in geo_iso}
    us_codes = {"USA", "ASM", "GUM", "MNP", "PRI", "VIR", "UMI"}
    fips = {code: geo_names[code] for code in fips
            if code in geo_iso and geo_iso[code] not in us_codes}
    countries = ISO3_COUNTRIES & set(names)
    parser_signature = hashlib.sha256(json.dumps({
        "version": 2, "columns": columns, "fips": sorted(fips),
        "countries": sorted(countries), "sectors": sorted(SECTOR_CODES),
    }, sort_keys=True).encode()).hexdigest()
    cache = ROOT / "archive/research4_gdelt_daily"
    cache.mkdir(parents=True, exist_ok=True)
    end = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    dates = pd.date_range(pd.Timestamp(START, tz=end.tz), end)

    def one(date):
        day = date.date().isoformat()
        url = f"https://data.gdeltproject.org/events/{date:%Y%m%d}.export.CSV.zip"
        max_age = 24 * 365 if (end - date).days > 30 else 24
        raw, digest = fetch.get(url, timeout=120, max_age_hours=max_age)
        path = cache / (day + ".parquet")
        info_path = cache / (day + ".json")
        if path.exists() and info_path.exists():
            info = json.loads(info_path.read_text())
            if info.get("raw_sha256") == digest and info.get("parser_signature") == parser_signature:
                return path, digest
        frame = gdelt_aggregate(raw, columns, day, fips, countries)
        atomic_bytes(path, frame.to_parquet(index=False, compression="zstd"))
        atomic_bytes(info_path, json.dumps({"raw_sha256": digest, "source_url": url,
                                           "parser_version": 2, "parser_signature": parser_signature,
                                           "date": day}).encode())
        time.sleep(.6)
        return path, digest

    paths, hashes = [], []
    with ThreadPoolExecutor(max_workers=3) as pool:
        pending = {pool.submit(one, date): date for date in dates}
        for future in as_completed(pending):
            date = pending[future]
            try:
                path, digest = future.result()
                paths.append(path)
                hashes.append(digest)
                if len(paths) % 50 == 0:
                    print("GDELT downloaded daily archives", len(paths), "/", len(dates), flush=True)
            except Exception as exc:
                FAILURES.append({"provider": "gdelt", "day": str(date.date()), "error": str(exc)})
                print("FAILED GDELT", date, str(exc), flush=True)
    if len(paths) < 180:
        raise ValueError("Insufficient complete daily GDELT archives")
    manifest = archive_raw_manifest(hashes, description="Original GDELT 1.0 daily discovery archives used for research4", root=ROOT)
    data = pd.concat([pd.read_parquet(p) for p in sorted(paths)], ignore_index=True)
    observed_dates = sorted(data.date.unique())
    result = []
    for (kind, country, detail, detail2), rows in data.groupby(["kind", "country", "detail", "detail2"]):
        if rows.date.nunique() < 90:
            continue
        if kind == "location_event":
            if country not in fips:
                continue
            place, event = fips[country], events.get(detail, "CAMEO event " + detail)
            label = f"{place}: {event}"
            category = "foreign_news_event_attention"
            codes = [geo_iso[country]]
            standard = "ISO alpha-3, mapped from native GDELT FIPS geography"
            scope = "non_us"
        elif kind == "country_sector":
            label = f"{names[country]}: {SECTOR_CODES[detail]} actor activity"
            category = "foreign_sector_news_attention"
            codes, standard = [country], "ISO alpha-3 / CAMEO"
            scope = "us" if country in us_codes else "non_us"
        else:
            label = f"{names[country]} toward {names[detail]}: {QUADS[detail2]}"
            category = "cross_border_relations_attention"
            codes, standard = [country, detail], "ISO alpha-3 / CAMEO"
            scope = ("us" if all(c in us_codes for c in codes)
                     else "mixed" if any(c in us_codes for c in codes) else "non_us")
        for metric in ["count", "tone"]:
            if metric == "count":
                unit, suffix = "coded news events", "daily coded-event count"
            else:
                unit, suffix = "GDELT event-associated mean tone score", "mean news tone"
            frame = gdelt_panel(rows, metric, observed_dates)
            item = save(
                "gdelt_" + slug("_".join([kind, country, detail, detail2, metric])), frame,
                title=f"{label}: {suffix}", provider="GDELT 1.0 worldwide news events",
                category=category, unit=unit, frequency="daily", source_url="https://data.gdeltproject.org/events/",
                geography=label.split(":")[0], geography_scope=scope,
                country_codes=codes, country_code_standard=standard,
                source_geography_code=country,
                hypothesis="News attention, pessimism, cooperation and conflict involving this geography or sector may precede changes in perceived country risk, supply-chain disruption, financing conditions, commodities and risk appetite; predictive content requires out-of-sample testing.",
                limitations="Algorithmically coded GDELT 1.0 English-language news coverage; does not include the translated-language coverage of GDELT 2.0. Counts measure coded media events, not verified incidents, unique articles, casualties or actual commerce. Syndication, extraction errors and news-source composition affect levels. Country/sector/pair panels overlap; actor nationality differs from event location. Mean tone is an unweighted mean of event-associated article-tone scores, so articles linked to multiple events can contribute repeatedly. Date is source discovery/archive day (DATEADDED), not claimed historical event date. Count zero means no matching coded events in a successfully retrieved complete daily archive; unavailable archive days are omitted. Tone remains absent on zero-event days. Retrospective archives and classification revisions are not a historical as-of feed.",
                raw_sha256=lookup_hashes, raw_manifest_sha256=[manifest],
                dataset_identity=f"GDELT1:discovery:{kind}:{country}:{detail}:{detail2}:{metric}",
                native_frequency="Daily discovery-event files aggregated by actual discovery date",
                expected_publication_lag="Provider documents next-morning publication, typically by 06:00 US Eastern; archived records may be revised",
                documentation_url="https://gdeltproject.org/data.html",
                aggregation="Count unique source GLOBALEVENTID per panel/day or mean source AvgTone; only completed archive days",
                measurement_type="news event-count aggregation" if metric == "count" else "source text-tone aggregation",
                minimum_active_days=90,
            )
            if item:
                result.append(item)
    return result


CFTC_DATASETS = {"disaggregated": "72hh-3qpy", "financial": "gpe5-46if"}


def cftc_fields(columns):
    """Keep actual all-contract levels, participants and gross concentration."""
    result = {}
    for col in columns:
        name = col["name"]
        key = col["fieldName"]
        if not name.endswith("_All"):
            continue
        lower = name.lower()
        if (lower == "open_interest_all"
            or ("_positions_" in lower and not lower.startswith("tot_rept"))
            or (lower.startswith("traders_") and "tot_rept" not in lower)
            or lower.startswith("conc_gross_le_4_tdr_")):
            result[key] = name
    return result


def cftc_numeric(values):
    """Source suppression markers are missing observations, never zero."""
    text = values.astype("string").str.strip()
    text = text.replace({".": pd.NA, "": pd.NA, "N/A": pd.NA})
    return pd.to_numeric(text, errors="raise")


def cftc(fetch):
    result = []
    release_url = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/ReleaseSchedule/index.htm"
    docs_url = "https://publicreporting.cftc.gov/stories/s/COT-Help/p2fg-u73y/"
    for kind, dataset in CFTC_DATASETS.items():
        meta_url = f"https://publicreporting.cftc.gov/api/views/{dataset}.json"
        raw, schema_hash = fetch.get(meta_url, max_age_hours=720)
        schema = json.loads(raw)
        fields = cftc_fields(schema["columns"])
        identifiers = ["id", "market_and_exchange_names", "report_date_as_yyyy_mm_dd",
                       "cftc_contract_market_code", "contract_units", "commodity_name",
                       "commodity_subgroup_name", "commodity_group_name", "futonly_or_combined"]
        url = f"https://publicreporting.cftc.gov/resource/{dataset}.json"
        hashes, parts = [schema_hash], []
        for offset in range(0, 500000, 10000):
            raw, digest = fetch.get(url, params={
                "$select": ",".join(identifiers + list(fields)),
                "$where": f"report_date_as_yyyy_mm_dd >= '{START}T00:00:00'",
                "$order": "report_date_as_yyyy_mm_dd,cftc_contract_market_code,id",
                "$limit": 10000, "$offset": offset,
            }, timeout=180, max_age_hours=24)
            rows = json.loads(raw)
            if not isinstance(rows, list):
                raise ValueError("CFTC response is not a row array")
            hashes.append(digest)
            if rows:
                parts.append(pd.DataFrame(rows))
            if len(rows) < 10000:
                break
            time.sleep(.5)
        else:
            raise ValueError("CFTC pagination exhausted")
        data = pd.concat(parts, ignore_index=True)
        if data.id.duplicated().any():
            raise ValueError("Repeated CFTC report IDs across pagination")
        if not data.futonly_or_combined.eq("FutOnly").all():
            raise ValueError("CFTC endpoint includes combined futures/options reports")
        for code, rows in data.groupby("cftc_contract_market_code"):
            rows = rows.sort_values("report_date_as_yyyy_mm_dd")
            latest = rows.iloc[-1]
            market = latest.market_and_exchange_names
            for field, name in fields.items():
                values = cftc_numeric(rows[field])
                frame = clean(rows.report_date_as_yyyy_mm_dd, values)
                if frame.empty:
                    continue
                if (frame.value < 0).any():
                    raise ValueError("Negative source CFTC level")
                if field.startswith("conc_"):
                    if (frame.value > 100).any():
                        raise ValueError("CFTC concentration exceeds 100 percent")
                    unit, category = "percent of open interest", "futures_position_concentration"
                elif field.startswith("traders_"):
                    unit, category = "reporting traders", "futures_participant_counts"
                else:
                    unit, category = "futures contracts", "futures_positioning"
                title = name.removesuffix("_All").replace("_", " ")
                item = save(
                    f"cftc_{kind}_{slug(code)}_{field}", frame,
                    title=f"{market}: {title}", provider="CFTC Commitments of Traders",
                    category=category, unit=unit, frequency="weekly", source_url=url,
                    geography="US-reporting futures markets with global commodity, currency and financial exposures",
                    geography_scope="us", country_codes=["USA"],
                    hypothesis="Reported hedger, dealer, asset-manager and speculative positioning, participation and concentration can proxy inventory hedging demand, crowded trades, risk appetite and vulnerability to liquidation.",
                    limitations="US-reporting futures markets, not total worldwide spot FX flows, banking transactions or global investor holdings. Futures-only all-contract positions; trader categories can be reclassified. Long, short and spread positions and participant counts describe overlapping market exposure, not independent factors. Suppressed or missing participant values are omitted. The source historical database reflects revisions. Report date is the position observation date, not the date it became public. Publication is normally later that week and can be delayed substantially by holidays, outages or government shutdowns, including the September 2025–January 2026 backlog. No historical as-of availability timestamp is reconstructed.",
                    raw_sha256=hashes,
                    dataset_identity=f"CFTC:{dataset}:{code}:{field}",
                    native_frequency="weekly report-date observations",
                    expected_publication_lag="Normally Friday 15:30 America/New_York for the preceding Tuesday; exception schedules and delayed backlogs must be applied before backtesting",
                    documentation_url=docs_url, release_schedule_url=release_url,
                    publication_exception_url="https://www.cftc.gov/PressRoom/PressReleases/9138-25",
                    contract_units=latest.contract_units, commodity=latest.commodity_name,
                    commodity_subgroup=latest.commodity_subgroup_name,
                    commodity_group=latest.commodity_group_name,
                    market_code=code, report_type=kind, source_field=field,
                    measurement_type="source reported position/participant statistic",
                )
                if item:
                    result.append(item)
    return result


NZTA_SERVICE = "https://services.arcgis.com/CXBb7LAjgIIdcsPt/arcgis/rest/services/TMS_Telemetry_Sites/FeatureServer/0"
NZTA_ITEM = "898e77d403c443c4961ada073d144735"


def nzta_frame(rows):
    frame = pd.DataFrame(rows)
    required = {"OBJECTID", "startDate", "siteID", "regionName", "SiteRef",
                "classWeight", "siteDescription", "laneNumber", "flowDirection", "trafficCount"}
    if not required.issubset(frame.columns):
        raise ValueError("NZTA daily count schema changed")
    frame["date"] = pd.to_datetime(frame.startDate, unit="ms", utc=True)
    frame["trafficCount"] = pd.to_numeric(frame.trafficCount, errors="raise")
    if frame.date.isna().any() or frame.OBJECTID.duplicated().any():
        raise ValueError("NZTA invalid date or duplicate source object")
    present = frame.trafficCount.dropna()
    if not np.isfinite(present).all() or present.lt(0).any():
        raise ValueError("NZTA invalid traffic counts")
    return frame.drop(columns=["startDate"])


def nzta(fetch):
    docs_url = f"https://www.arcgis.com/sharing/rest/content/items/{NZTA_ITEM}"
    _, docs_hash = fetch.get(docs_url, params={"f": "json"}, max_age_hours=720)
    raw, schema_hash = fetch.get(NZTA_SERVICE, params={"f": "json"}, max_age_hours=1)
    schema = json.loads(raw)
    edition = schema.get("editingInfo", {}).get("dataLastEditDate")
    if not schema.get("advancedQueryCapabilities", {}).get("supportsQueryWithResultType"):
        raise ValueError("NZTA no longer supports documented standard query limits")
    page_size = min(32000, schema["standardMaxRecordCount"])
    query = NZTA_SERVICE + "/query"
    raw, profiles_hash = fetch.get(query, params={
        "f": "json", "where": f"startDate >= TIMESTAMP '{START} 00:00:00'",
        "outFields": "siteID,SiteRef,regionName,siteDescription", "returnGeometry": "false",
        "returnDistinctValues": "true", "resultType": "standard", "resultRecordCount": 32000,
        "orderByFields": "siteID,SiteRef,regionName,siteDescription",
    }, timeout=180, max_age_hours=24)
    obj = json.loads(raw)
    if obj.get("error") or obj.get("exceededTransferLimit"):
        raise ValueError("NZTA site profile query failed or was truncated")
    profiles = pd.DataFrame([r["attributes"] for r in obj["features"]])
    ambiguous_sites = set(profiles.loc[profiles.siteID.duplicated(keep=False), "siteID"])
    profiles = profiles.drop_duplicates("siteID")
    months = pd.date_range(START, pd.Timestamp.now().date(), freq="MS")
    jobs = []
    # Date partitions avoid very large ArcGIS offsets and keep pagination bounded.
    for month in months:
        after = month + pd.offsets.MonthBegin(1)
        where = (f"startDate >= TIMESTAMP '{month:%Y-%m-%d} 00:00:00' "
                 f"AND startDate < TIMESTAMP '{after:%Y-%m-%d} 00:00:00'")
        raw, count_hash = fetch.get(query, params={"f": "json", "where": where,
                                                  "returnCountOnly": "true"}, max_age_hours=1)
        monthly_count = json.loads(raw)["count"]
        jobs.append((str(month.date()), where, monthly_count, count_hash))
    folder = ROOT / "archive/research4_nzta_pages"
    folder.mkdir(parents=True, exist_ok=True)

    def page(where, offset, limit):
        raw, digest = fetch.get(query, params={
            "f": "json", "where": where,
            "outFields": "OBJECTID,startDate,siteID,classWeight,laneNumber,flowDirection,trafficCount",
            "returnGeometry": "false",
            "resultType": "standard", "resultRecordCount": limit,
            "orderByFields": "OBJECTID", "resultOffset": offset,
        }, timeout=180, max_age_hours=24)
        obj = json.loads(raw)
        if obj.get("error"):
            raise ValueError(f"NZTA query error for {where}, offset {offset}: {obj['error']}")
        rows = [r["attributes"] for r in obj["features"]]
        if not rows:
            raise ValueError("NZTA page is unexpectedly empty")
        path = folder / (digest + "_" + profiles_hash[:16] + ".parquet")
        if not path.exists():
            frame = pd.DataFrame(rows).merge(profiles, on="siteID", how="left", validate="many_to_one")
            if frame.SiteRef.isna().any():
                raise ValueError("NZTA observation references an unknown site")
            atomic_bytes(path, nzta_frame(frame).to_parquet(index=False, compression="zstd"))
        time.sleep(.5)
        return path, digest, len(rows), int(rows[-1]["OBJECTID"]), [int(r["OBJECTID"]) for r in rows]

    def month(job):
        label, where, monthly_count, count_hash = job
        month_paths, month_hashes = [], [count_hash]
        observed, last_id = 0, -1
        while observed < monthly_count:
            # Large OFFSET scans fail intermittently on this ArcGIS service.
            # Indexed OBJECTID cursors retain stable, complete pagination.
            use_cursor = monthly_count > 100000 and observed > 0
            predicate = where + f" AND OBJECTID > {last_id}" if use_cursor else where
            offset = 0 if use_cursor else observed
            limit = page_size
            try:
                result = page(predicate, offset, limit)
            except ValueError:
                # A smaller documented page is a bounded query repair, not a
                # retry of a restriction or an acceptance of incomplete data.
                limit = 2000
                result = page(predicate, offset, limit)
            path, digest, size, end_id, ids = result
            if size != min(limit, monthly_count - observed) or any(
                    b <= a for a, b in zip([last_id] + ids[:-1], ids)):
                raise ValueError("NZTA row count, ordering or pagination identity changed")
            month_paths.append(path)
            month_hashes.append(digest)
            observed += size
            last_id = end_id
        return label, month_paths, month_hashes, monthly_count

    paths, hashes, count, excluded_months = [], [], 0, []
    with ThreadPoolExecutor(max_workers=3) as pool:
        pending = {pool.submit(month, job): job[0] for job in jobs}
        for future in as_completed(pending):
            label = pending[future]
            try:
                _, month_paths, month_hashes, month_count = future.result()
                paths.extend(month_paths)
                hashes.extend(month_hashes)
                count += month_count
                print("NZTA complete month", label, month_count, "rows;", len(paths), "complete pages", flush=True)
            except Exception as exc:
                excluded_months.append(label)
                FAILURES.append({"provider": "nzta", "month": label, "error": str(exc)})
                print("FAILED NZTA month", label, str(exc), flush=True)
    if not paths:
        raise ValueError("No complete NZTA month partitions")
    raw, latest_hash = Fetcher(refresh=True).get(NZTA_SERVICE, params={"f": "json"})
    if json.loads(raw).get("editingInfo", {}).get("dataLastEditDate") != edition:
        raise ValueError("NZTA source changed while paging; rerun against a consistent edition")
    data = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    if len(data) != count or data.OBJECTID.duplicated().any():
        raise ValueError("NZTA row count or source object identity mismatch")
    manifest = archive_raw_manifest(hashes, description="NZTA complete monthly-partitioned daily traffic API pages and row counts since 2024; failed months excluded in full")
    result = []
    for (site, lane, direction, vehicle_class), rows in data.groupby(
            ["siteID", "laneNumber", "flowDirection", "classWeight"]):
        if site in ambiguous_sites or rows.SiteRef.nunique() > 1 or rows.siteDescription.nunique() > 1:
            OMISSIONS.append({"provider": "nzta", "site": int(site),
                              "reason": "Site configuration or description changes within history"})
            continue
        try:
            frame = clean(rows.date, rows.trafficCount)
        except ValueError as exc:
            OMISSIONS.append({"provider": "nzta", "site": int(site), "lane": int(lane),
                              "direction": int(direction), "class": vehicle_class, "reason": str(exc)})
            continue
        if frame.empty or not qualifies_research(dict(coverage(frame), frequency="daily")):
            continue
        latest = rows.sort_values("date").iloc[-1]
        item = save(
            f"nzta_{site}_{lane}_{direction}_{slug(vehicle_class)}", frame,
            title=f"NZ highway {latest.SiteRef} — {latest.siteDescription}: {vehicle_class} vehicles, lane {lane}, direction {direction}",
            provider="Waka Kotahi NZ Transport Agency traffic monitoring",
            category="new_zealand_road_traffic", unit="vehicles per day (source class allocation)",
            frequency="daily", source_url=query, geography=f"New Zealand — {latest.regionName}",
            geography_scope="non_us", country_codes=["NZL"],
            hypothesis="Heavy traffic may proxy freight and logistics demand; light traffic may proxy commuting, tourism and retail activity, with relevance to New Zealand consumption, transport and fuel demand.",
            limitations="One native site/lane/direction/vehicle-class observation per day. Sensor faults and survey schedules cause gaps, which remain missing. Counts are not total regional traffic or vehicle-kilometres. Dual-loop classification can allocate vehicles of intermediate length 50:50 between light and heavy, so fractional counts are legitimate source estimates. Site configuration and calibration may change. Native epoch-millisecond startDate is retained as a source daily period timestamp, not assumed to be release time. Daily observations can be uploaded weekly, fortnightly, monthly or quarterly; occasional four-week survey sites are excluded by history-density rules. Only fully retrieved and row-count-checked monthly API partitions are used; any failed partition is omitted in full and listed in excluded_source_months. Retrospective archive, not point-in-time releases.",
            raw_sha256=[docs_hash, schema_hash, profiles_hash, latest_hash],
            raw_manifest_sha256=[manifest],
            dataset_identity=f"NZTA:TMS:{site}:{lane}:{direction}:{vehicle_class}",
            native_frequency="daily count by native lane, direction and class",
            expected_publication_lag="Telemetry usually uploaded weekly Wednesday; other contracts fortnightly, monthly, quarterly or intermittent",
            documentation_url=f"https://www.arcgis.com/home/item.html?id={NZTA_ITEM}",
            license="CC BY 4.0 (NZTA data attribution)",
            site_id=str(site), site_reference=latest.SiteRef, lane=int(lane),
            direction=int(direction), vehicle_class=vehicle_class,
            excluded_source_months=sorted(excluded_months),
            measurement_type="source road sensor count with source vehicle-class allocation",
        )
        if item:
            result.append(item)
    return result


PROVIDERS = {"gdelt": gdelt, "cftc": cftc, "nzta": nzta}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=PROVIDERS, action="append")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    SEEN.clear()
    FAILURES.clear()
    fetch = Fetcher(refresh=args.refresh)
    successes = {}
    names = args.provider or list(PROVIDERS)
    for name in names:
        try:
            rows = PROVIDERS[name](fetch)
            successes[name] = [r["series_id"] for r in rows]
        except Exception as exc:
            FAILURES.append({"provider": name, "error": f"{type(exc).__name__}: {exc}"})
            print("FAILED", name, str(exc), flush=True)
    report = {"completed_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "successes": successes, "failures": FAILURES, "omissions": OMISSIONS}
    atomic_bytes(ROOT / "logs" / ("research4_" + "_".join(names) + ".json"),
                 json.dumps(report, indent=2).encode())
    print("COMPLETE", {k: len(v) for k, v in successes.items()}, "FAILURES", len(FAILURES), flush=True)
    return int(bool(FAILURES))


if __name__ == "__main__":
    raise SystemExit(main())
