"""Expanded observed air and water conditions, with reproducible source selection.

The pinned selections in logs/physical3_*_selection.json are the refresh universe.
Data are measurements or documented source summaries, never interpolated.
"""
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
from urllib.parse import urlencode
import zipfile

import numpy as np
import pandas as pd

from expansion_common import (
    Fetcher, ROOT, atomic_bytes, coverage, normalize_frame, qualifies_daily,
    save_candidate,
)

GROUP = "physical3"
START = "2022-01-01"
EPA_BASE = "https://aqs.epa.gov/aqsweb/airdata/"
HYDAT_BASE = "https://api.weather.gc.ca/collections/"
USGS_METADATA = "https://api.waterdata.usgs.gov/ogcapi/v1/collections/time-series-metadata/items"
USGS_DAILY = "https://waterservices.usgs.gov/nwis/dv/"


class EligibilityOmission(ValueError):
    """Source observations do not meet the library's eligibility requirements."""


def json_safe(value):
    return json.loads(json.dumps(value, default=lambda x: x.item() if hasattr(x, "item") else str(x)))


def old_identities():
    path = ROOT / "catalog.parquet"
    if not path.exists():
        return set()
    catalog = pd.read_parquet(path)
    # This acquisition belongs to expansion batch 5. Its own published series
    # must remain refreshable even if a selection manifest needs rebuilding.
    return set(catalog.loc[catalog["batch"] < 5, "dataset_identity"].dropna())


def pin(name, build):
    path = ROOT / "logs" / f"physical3_{name}_selection.json"
    if path.exists():
        return json.loads(path.read_text())
    selected = json_safe(build())
    atomic_bytes(path, json.dumps(selected, indent=2).encode())
    return selected


def balanced(items, key, total, rank):
    """Round robin actual geographic strata, with stable within-stratum ranking."""
    queues = defaultdict(list)
    for item in items:
        queues[key(item)].append(item)
    queues = {k: deque(sorted(v, key=rank)) for k, v in sorted(queues.items())}
    result = []
    while queues and len(result) < total:
        for k in list(queues):
            result.append(queues[k].popleft())
            if not queues[k]:
                del queues[k]
            if len(result) == total:
                break
    return result


def clean_frame(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                          "value": pd.to_numeric(values, errors="raise")})
    frame = frame.loc[frame.value.notna() & (frame.date >= START)].copy()
    if not np.isfinite(frame.value).all():
        raise ValueError("Nonfinite source values")
    if (frame.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting source values on the same observation date")
    return frame.drop_duplicates("date").sort_values("date").reset_index(drop=True)


def require_daily(frame):
    if frame.empty:
        raise EligibilityOmission("No numeric observations")
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2:
        raise EligibilityOmission("Constant series")
    if not qualifies_daily({**coverage(frame), "frequency": "daily"}):
        raise EligibilityOmission(f"Insufficient daily history: {coverage(frame)}")
    return frame


def quarantine(sid, reason):
    """Preserve a superseded ineligible candidate outside the publishable stage."""
    source = ROOT / "archive/staging" / GROUP
    if not (source / (sid + ".parquet")).exists():
        return
    target = ROOT / "archive/rejected" / GROUP / (
        pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + "_" + sid)
    target.mkdir(parents=True, exist_ok=True)
    for suffix in [".parquet", ".json"]:
        path = source / (sid + suffix)
        if path.exists():
            path.replace(target / path.name)
    atomic_bytes(target / "reason.json", json.dumps({"reason": str(reason)}).encode())


def save(sid, frame, **meta):
    try:
        frame = require_daily(frame)
    except EligibilityOmission as exc:
        quarantine(sid, exc)
        raise
    return save_candidate(GROUP, sid, frame, frequency="daily", **meta)


def parallel(items, worker, workers=3):
    rows, errors = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        tasks = {pool.submit(worker, item): item for item in items}
        for task in as_completed(tasks):
            item = tasks[task]
            try:
                result, omitted = task.result()
                rows.extend(result)
                errors.extend(omitted)
                print(f"OK {str(item)[:65]}: {len(result)} series", flush=True)
            except Exception as exc:
                errors.append({"item": str(item), "error": repr(exc), "kind": "unexpected_error"})
                print(f"ERROR {str(item)[:65]}: {exc}", flush=True)
    return rows, errors


def omit(item, exc):
    return {"item": str(item), "error": str(exc), "kind": "expected_omission"}


# One pollutant concentration metric per station. Collocated POC devices are not
# counted as additional economic exposures: the most complete eligible POC wins.
# Ozone daily files contain 8-hour rolling means, not a raw hourly daily mean.
EPA_METRICS = {
    "44201": dict(name="ozone", duration="8-HR RUN AVG BEGIN HOUR", standard="Ozone 8-hour 2015",
                  label="daily mean of eight-hour rolling ozone concentrations", limit=300),
    "42401": dict(name="sulfur_dioxide", duration="1 HOUR", standard="SO2 1-hour 2010",
                  label="daily mean sulfur dioxide concentration", limit=250),
    "42101": dict(name="carbon_monoxide", duration="1 HOUR", standard="CO 1-hour 1971",
                  label="daily mean carbon monoxide concentration", limit=200),
    "42602": dict(name="nitrogen_dioxide", duration="1 HOUR", standard="NO2 1-hour 2010",
                  label="daily mean nitrogen dioxide concentration", limit=250),
    "88101": dict(name="pm25", duration="1 HOUR", standard="",
                  label="daily mean PM2.5 concentration", limit=350),
    "81102": dict(name="pm10", duration="24-HR BLK AVG", standard="PM10 24-hour 2006",
                  label="daily mean PM10 concentration", limit=150),
}
EPA_COLUMNS = [
    "State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude",
    "Parameter Name", "Sample Duration", "Pollutant Standard", "Date Local", "Units of Measure",
    "Event Type", "Observation Count", "Observation Percent", "Arithmetic Mean",
    "Method Code", "Method Name", "Local Site Name", "State Name", "County Name", "City Name",
    "CBSA Name",
]


def parse_epa(content, parameter):
    config = EPA_METRICS[parameter]
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = [n for n in archive.namelist() if n.endswith(".csv")]
        if len(names) != 1:
            raise ValueError("Expected one EPA daily CSV")
        frame = pd.read_csv(archive.open(names[0]), usecols=EPA_COLUMNS, low_memory=False)
    if not set(frame["Parameter Code"].dropna().astype(int).astype(str)) <= {parameter}:
        raise ValueError("Unexpected EPA pollutant")
    # Retain genuine ambient conditions, including exceptional event pollution.
    # The excluded-event parallel records are policy versions, not extra series.
    event = frame["Event Type"].fillna("").str.lower()
    frame = frame.loc[(frame["Sample Duration"] == config["duration"])
                      & (frame["Pollutant Standard"].fillna("") == config["standard"])
                      & event.isin(["", "none", "no events", "included", "events included"])
                      & (frame["Observation Count"] > 0)
                      & frame["Arithmetic Mean"].notna()].copy()
    frame["site"] = (frame["State Code"].astype(int).astype(str).str.zfill(2)
                     + frame["County Code"].astype(int).astype(str).str.zfill(3)
                     + frame["Site Num"].astype(int).astype(str).str.zfill(4))
    frame["monitor"] = frame.site + "_" + frame.POC.astype(int).astype(str)
    return frame


def epa_identity(site, parameter):
    return f"EPA AirData site {site} parameter {parameter} daily ambient concentration"


def epa(fetcher):
    rows, errors = [], []
    current_year = pd.Timestamp.now(tz="UTC").year
    excluded = old_identities()
    for parameter, config in EPA_METRICS.items():
        frames, hashes, urls = [], [], []
        for year in range(int(START[:4]), current_year + 1):
            url = EPA_BASE + f"daily_{parameter}_{year}.zip"
            raw, digest = fetcher.get(url, timeout=180, max_age_hours=168)
            frame = parse_epa(raw, parameter)
            frames.append(frame)
            hashes.append(digest)
            urls.append(url)
            print(f"EPA {parameter} {year}: {len(frame)} usable daily summaries", flush=True)
        combined = pd.concat(frames, ignore_index=True)
        candidates, data = [], {}
        for monitor, subset in combined.groupby("monitor", sort=True):
            try:
                candidate = require_daily(clean_frame(subset["Date Local"], subset["Arithmetic Mean"]))
            except EligibilityOmission as exc:
                errors.append(omit(f"EPA {parameter} {monitor}", exc))
                continue
            units = list(subset["Units of Measure"].dropna().unique())
            if len(units) != 1:
                raise ValueError(f"EPA monitor {monitor}: changing units {units}")
            latest = subset.sort_values("Date Local").iloc[-1]
            item = dict(monitor=monitor, site=str(latest.site), parameter=parameter,
                        poc=int(latest.POC), unit=units[0], latitude=float(latest.Latitude),
                        longitude=float(latest.Longitude), state=str(latest["State Name"]),
                        county=str(latest["County Name"]), city=str(latest["City Name"]),
                        local_name=str(latest["Local Site Name"]), cbsa=str(latest["CBSA Name"]),
                        method_names=sorted(str(x) for x in subset["Method Name"].dropna().unique()),
                        rows=len(candidate), end=candidate.date.iloc[-1].isoformat(),
                        source_observation_percent_min=float(subset["Observation Percent"].min()))
            candidates.append(item)
            data[monitor] = candidate

        def select():
            # Best single instrument record at each site; no POC multiplication.
            best = {}
            for item in sorted(candidates, key=lambda x: (-x["rows"], -pd.Timestamp(x["end"]).value, x["poc"])):
                if epa_identity(item["site"], parameter) not in excluded:
                    best.setdefault(item["site"], item)
            return balanced(best.values(), lambda x: x["state"], config["limit"],
                            lambda x: (-x["rows"], -pd.Timestamp(x["end"]).value, x["site"]))

        selected = pin(f"epa_{parameter}", select)
        for item in selected:
            monitor = item["monitor"]
            if monitor not in data:
                quarantine(f"epa_monitor_{item['site']}_{config['name']}",
                           "Pinned monitor no longer has eligible history")
                errors.append(omit(f"EPA {parameter} {monitor}", "Pinned monitor no longer has eligible history"))
                continue
            rows.append(save(
                f"epa_monitor_{item['site']}_{config['name']}", data[monitor],
                title=f"{item['county']} County, {item['state']} site {item['site']}: {config['label']}",
                provider="us_epa_airdata", category="ambient_air_pollutants",
                unit=item["unit"], source_url=urls[-1], source_urls=urls,
                geography=f"{item['county']} County, {item['state']}, United States",
                hypothesis="Research hypothesis: local ambient pollution may reflect industrial activity, freight traffic, fuel combustion, wildfire disruption and outdoor labor constraints. Weather, transport and policy confound activity signals; test seasonal and meteorological controls.",
                limitations="EPA historical ambient concentration summaries, not emissions or production measures. Files are generally refreshed twice yearly and can be revised; dates are observation days, not historical availability. Exceptional-event pollution is retained; excluded-event parallel records are discarded. One POC per site/pollutant is pinned to avoid instrument duplicates. Missing observations are omitted. Source values below zero can be valid baseline-corrected sensor readings. Ozone uses the source mean of eight-hour rolling concentrations.",
                raw_sha256=hashes, dataset_identity=epa_identity(item["site"], parameter),
                source_station_metadata=item, latitude=item["latitude"], longitude=item["longitude"],
                native_frequency="hourly", finer_frequency_available=True,
                source_summary_duration=config["duration"], source_pollutant_standard=config["standard"],
                expected_publication_lag="Regulatory reporting can lag roughly six months; bulk files generally update in June and December",
                timestamp_semantics="Monitor local-standard calendar day represented at UTC midnight; not release timestamp",
                documentation_url=EPA_BASE + "FileFormats.html", license="US EPA public data",
            ))
        print(f"EPA {parameter}: staged {len(selected)} selected monitors", flush=True)
    return rows, errors


USGS_PARAMETERS = {
    "00060": ("discharge", "river_discharge", 550, "river navigation, irrigation, hydropower and industrial intake constraints"),
    "00065": ("gauge_height", "river_water_levels", 150, "flood exposure, river navigation and industrial access"),
    "00010": ("water_temperature", "freshwater_temperature", 150, "power-plant cooling, aquaculture and ecological operating restrictions"),
    "00095": ("specific_conductance", "freshwater_quality", 150, "saltwater intrusion, drought concentration and industrial water treatment"),
    "00300": ("dissolved_oxygen", "freshwater_quality", 100, "aquaculture, fisheries and wastewater treatment constraints"),
    "63680": ("turbidity", "freshwater_quality", 100, "sediment disruption, dredging and water treatment load"),
    "62614": ("lake_elevation", "reservoir_water_levels", 100, "reservoir water availability, hydropower and recreation"),
    "72019": ("groundwater_depth", "groundwater_levels", 100, "groundwater irrigation costs and industrial water availability"),
    "00054": ("reservoir_storage", "reservoir_storage", 100, "irrigation supply, hydropower dispatch and drought exposure"),
}
# At this station the gauge-height record is the same water-surface elevation
# as parameter 62614 on all 1,090 common dates, with a shorter start date.
# Keep the explicitly defined, longer NGVD 1929 elevation history once.
USGS_SEMANTIC_ALIASES = {
    ("12453690", "00065"): "USGS 12453690 parameter 62614 statistic 00003",
}


def usgs_identity(site, parameter):
    return f"USGS {site} parameter {parameter} statistic 00003"


def usgs_metadata(fetcher, parameter):
    params = dict(f="json", limit=10000, computation_period_identifier="Daily",
                  statistic_id="00003", parameter_code=parameter,
                  filter="end > TIMESTAMP('2026-01-01T00:00:00Z')")
    raw, digest = fetcher.get(USGS_METADATA, params=params, timeout=180, max_age_hours=168)
    result, hashes = [], [digest]
    while True:
        payload = json.loads(raw)
        if "features" not in payload:
            raise ValueError(f"Unexpected USGS metadata response: {str(payload)[:200]}")
        result.extend(payload["features"])
        next_urls = [x["href"] for x in payload.get("links", []) if x.get("rel") == "next"]
        if not next_urls:
            break
        raw, digest = fetcher.get(next_urls[0], timeout=180, max_age_hours=168)
        hashes.append(digest)
    selected = []
    for feature in result:
        item = feature["properties"]
        if (item["parameter_code"] != parameter or item["statistic_id"] != "00003"
                or item["computation_period_identifier"] != "Daily"):
            raise ValueError("USGS ignored metadata filters")
        if item.get("primary") != "Primary" or item.get("sublocation_identifier") is not None:
            continue
        if not item["monitoring_location_id"].startswith("USGS-"):
            continue
        if pd.Timestamp(item["begin"]) > pd.Timestamp("2024-01-01", tz="UTC"):
            continue
        coords = feature["geometry"]["coordinates"]
        selected.append(dict(site=item["monitoring_location_id"].removeprefix("USGS-"),
                             parameter=parameter, time_series_id=item["id"],
                             parameter_description=item["parameter_description"],
                             latitude=coords[1], longitude=coords[0],
                             begin=item["begin"], end=item["end"],
                             metadata_raw_sha256=hashes))
    return selected


def parse_usgs(payload):
    if "value" not in payload or "timeSeries" not in payload["value"]:
        raise ValueError("Unexpected USGS daily response")
    result = []
    for source in payload["value"]["timeSeries"]:
        info, variable = source["sourceInfo"], source["variable"]
        site = str(info["siteCode"][0]["value"])
        parameter = str(variable["variableCode"][0]["value"])
        statistics = [x["optionCode"] for x in variable.get("options", {}).get("option", [])
                      if x.get("name") == "Statistic"]
        if statistics != ["00003"]:
            raise ValueError(f"USGS returned unexpected statistic: {statistics}")
        no_data = float(variable["noDataValue"])
        for block in source["values"]:
            method_ids = {str(x.get("methodID")) for x in block.get("method", [])}
            # Legacy WaterML can place multiple instruments/sublocations under
            # the same site/parameter. Conflicts trigger a primary-series query.
            points = []
            qualifiers = set()
            for point in block["value"]:
                value = float(point["value"])
                if value == no_data or set(point.get("qualifiers", [])) & {"<", ">"}:
                    continue
                points.append((point["dateTime"][:10], value))
                qualifiers.update(point.get("qualifiers", []))
            if not points:
                continue
            coords = info["geoLocation"]["geogLocation"]
            result.append(dict(site=site, parameter=parameter, name=info["siteName"],
                               unit=variable["unit"]["unitCode"], variable=variable,
                               latitude=coords["latitude"], longitude=coords["longitude"],
                               frame=clean_frame([x[0] for x in points], [x[1] for x in points]),
                               qualifiers=sorted(qualifiers), method_ids=sorted(method_ids)))
    # WaterML may return disjoint method blocks; merge only agreeing dates.
    merged = {}
    for item in result:
        key = (item["site"], item["parameter"])
        if key in merged:
            old = merged[key]
            if old["unit"] != item["unit"]:
                raise ValueError(f"USGS changing units for {key}")
            combined = pd.concat([old["frame"], item["frame"]], ignore_index=True)
            if old.get("requires_primary_query") or (combined.groupby("date").value.nunique() > 1).any():
                old["requires_primary_query"] = True
            else:
                old["frame"] = clean_frame(combined.date, combined.value)
            old["qualifiers"] = sorted(set(old["qualifiers"] + item["qualifiers"]))
            old["method_ids"] = sorted(set(old["method_ids"] + item["method_ids"]))
        else:
            merged[key] = item
    return list(merged.values())


def usgs_primary(fetcher, meta):
    """Resolve collocated legacy methods through the modern primary series ID."""
    url = "https://api.waterdata.usgs.gov/ogcapi/v1/collections/daily/items"
    params = dict(f="json", limit=10000, time_series_id=meta["time_series_id"], datetime=f"{START}/..")
    raw, digest = fetcher.get(url, params=params, timeout=120)
    payload = json.loads(raw)
    if any(x.get("rel") == "next" for x in payload.get("links", [])):
        raise ValueError("USGS primary daily query requires pagination")
    points, qualifiers, units = [], set(), set()
    for feature in payload["features"]:
        item = feature["properties"]
        if (item["time_series_id"] != meta["time_series_id"] or item["parameter_code"] != meta["parameter"]
                or item["monitoring_location_id"] != "USGS-" + meta["site"]
                or item["statistic_id"] != "00003"):
            raise ValueError("USGS primary series filter ignored")
        qualifier = str(item.get("qualifier") or "")
        if item["value"] is not None and not any(x in qualifier.lower() for x in ["<", ">", "less than", "greater than"]):
            points.append((item["time"], float(item["value"])))
            qualifiers.add(str(item["approval_status"]))
            if item.get("qualifier"):
                qualifiers.add(str(item["qualifier"]))
            units.add(item["unit_of_measure"])
    if len(units) > 1:
        raise ValueError(f"USGS primary series changes units: {units}")
    return clean_frame([x[0] for x in points], [x[1] for x in points]), digest, sorted(qualifiers), url + "?" + urlencode(params)


def usgs_finer_inventory(fetcher, selected):
    """Verify subdaily availability from provider metadata rather than infer it."""
    selected_keys = {(x["site"], x["parameter"]) for x in selected}
    inventory = []
    for parameter in sorted({x["parameter"] for x in selected}):
        params = dict(f="json", limit=10000, computation_period_identifier="Points",
                      computation_identifier="Instantaneous", parameter_code=parameter,
                      filter="end > TIMESTAMP('2024-01-01T00:00:00Z')")
        raw, digest = fetcher.get(USGS_METADATA, params=params, timeout=180, max_age_hours=168)
        while True:
            payload = json.loads(raw)
            for feature in payload["features"]:
                item = feature["properties"]
                if item["computation_period_identifier"] != "Points" or item["parameter_code"] != parameter:
                    raise ValueError("USGS ignored continuous metadata filters")
                key = (item["monitoring_location_id"].removeprefix("USGS-"), parameter)
                if key in selected_keys and item.get("primary") == "Primary":
                    inventory.append(dict(site=key[0], parameter=parameter,
                                          time_series_id=item["id"],
                                          begin=item["begin"], end=item["end"],
                                          raw_sha256=digest))
            links = [x["href"] for x in payload.get("links", []) if x.get("rel") == "next"]
            if not links:
                break
            raw, digest = fetcher.get(links[0], timeout=180, max_age_hours=168)
    return inventory


def usgs(fetcher):
    excluded = old_identities()
    alias_omissions = []
    for (site, parameter), retained_identity in USGS_SEMANTIC_ALIASES.items():
        sid = f"usgs_{site}_{USGS_PARAMETERS[parameter][0]}"
        reason = f"Semantic alias of longer retained history {retained_identity}; identical on every overlapping date"
        quarantine(sid, reason)
        alias_omissions.append(omit(sid, reason))

    def select():
        selected = []
        for parameter, (name, category, limit, mechanism) in USGS_PARAMETERS.items():
            available = [x for x in usgs_metadata(fetcher, parameter)
                         if usgs_identity(x["site"], parameter) not in excluded
                         and (x["site"], parameter) not in USGS_SEMANTIC_ALIASES]
            # Geographic cells diversify river basins. The oldest active records
            # lead each cell; selection never depends on asset returns.
            unique = {x["site"]: x for x in available}
            selected.extend(balanced(unique.values(),
                                    lambda x: (int(np.floor(x["longitude"] / 4)), int(np.floor(x["latitude"] / 3))),
                                    limit, lambda x: (x["begin"], x["site"])))
            print(f"USGS {parameter}: {len(available)} active long-history metadata records", flush=True)
        return selected

    selected = [x for x in pin("usgs", select) if x["site"].isdigit()
                and (x["site"], x["parameter"]) not in USGS_SEMANTIC_ALIASES]
    selected_map = {(x["site"], x["parameter"]): x for x in selected}
    finer = defaultdict(list)
    for item in pin("usgs_finer", lambda: usgs_finer_inventory(fetcher, selected)):
        finer[(item["site"], item["parameter"])].append(item)
    sites = sorted({x["site"] for x in selected})
    chunks = [sites[i:i+30] for i in range(0, len(sites), 30)]
    end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    def worker(sites_chunk):
        params = dict(format="json", sites=",".join(sites_chunk), startDT=START, endDT=end,
                      parameterCd=",".join(USGS_PARAMETERS), statCd="00003", siteStatus="all")
        raw, digest = fetcher.get(USGS_DAILY, params=params, timeout=180)
        rows, errors, found = [], [], set()
        for item in parse_usgs(json.loads(raw)):
            key = (item["site"], item["parameter"])
            if key not in selected_map:
                continue
            found.add(key)
            meta = selected_map[key]
            name, category, _, mechanism = USGS_PARAMETERS[item["parameter"]]
            source_url = USGS_DAILY + "?" + urlencode(params)
            data_hashes = [digest]
            if item.get("requires_primary_query"):
                item["frame"], extra_digest, item["qualifiers"], source_url = usgs_primary(fetcher, meta)
                data_hashes.append(extra_digest)
                item["method_ids"] = ["modern primary time_series_id " + meta["time_series_id"]]
            try:
                rows.append(save(
                    f"usgs_{item['site']}_{name}", item["frame"],
                    title=f"{item['name']}: daily mean {item['variable']['variableName']}",
                    provider="usgs_water", category=category, unit=item["unit"],
                    source_url=source_url,
                    geography=f"{item['name']}, United States",
                    hypothesis=f"Research hypothesis: measured {name.replace('_', ' ')} may affect {mechanism}. Local operating thresholds and catchment context matter; test seasonal deviations and nonlinear extremes.",
                    limitations="USGS source daily means, including published approved and provisional measurements. Discharge depends on rating curves and can include source-estimated values; qualifiers are logged. No-data sentinels and censored less-than/greater-than values are omitted. No missing dates are filled. Gauge elevations/depths use parameter-specific local or vertical datums and cannot be compared across sites without datum conversion. Conflicting collocated legacy methods are resolved using the modern source-designated primary time series. Historical observations can be revised and this is not point-in-time availability data.",
                    raw_sha256=data_hashes + meta["metadata_raw_sha256"] + [x["raw_sha256"] for x in finer[key]],
                    dataset_identity=usgs_identity(item["site"], item["parameter"]),
                    latitude=item["latitude"], longitude=item["longitude"],
                    native_frequency="continuous sensor observations (site-specific cadence)" if finer[key] else "daily",
                    finer_frequency_available=bool(finer[key]), finer_frequency_source_metadata=finer[key],
                    source_qualifiers=item["qualifiers"],
                    source_method_ids=item["method_ids"], source_station_metadata=meta,
                    timestamp_semantics="Source local daily period represented at UTC midnight; not publication time",
                    expected_publication_lag="Daily summaries typically published after the observation day; provisional values subject to later approval/revision",
                    documentation_url="https://nwis.waterservices.usgs.gov/docs/dv-service/daily-values-service-details/",
                    modern_metadata_documentation_url="https://api.waterdata.usgs.gov/docs/ogcapi/",
                    license="USGS public data",
                ))
            except EligibilityOmission as exc:
                errors.append(omit(f"USGS {key}", exc))
        for key in selected_map:
            if key[0] in sites_chunk and key not in found:
                errors.append(omit(f"USGS {key}", "Metadata record has no returned daily observations"))
        return rows, errors

    rows, errors = parallel(chunks, worker, workers=3)
    return rows, errors + alias_omissions


HYDAT_MECHANISMS = {
    "AB": "oil sands and petrochemical water supply, irrigation and Prairie flood exposure",
    "BC": "hydropower, forestry transport, salmon fisheries and flood exposure",
    "MB": "hydropower, Prairie grain production and flood exposure",
    "SK": "potash and uranium operations, grain irrigation and water availability",
    "ON": "industrial water intake, Great Lakes navigation and hydropower",
    "QC": "hydropower, aluminum electricity supply and St Lawrence shipping",
    "NL": "hydropower, mining water supply and flood exposure",
    "NB": "pulp and paper operations, hydropower and flooding",
    "NS": "forestry, fisheries and transport flooding",
    "PE": "potato agriculture, irrigation and freshwater availability",
    "YT": "mining access, hydropower and northern transport",
    "NT": "mining logistics, northern river navigation and hydropower",
    "NU": "mining water supply and northern infrastructure flooding",
}


def parse_hydat(content):
    if not content.strip():
        raise EligibilityOmission("Empty HYDAT CSV response; verify source zero-match response")
    frame = pd.read_csv(io.BytesIO(content))
    required = {"STATION_NUMBER", "DATE", "DISCHARGE", "LEVEL"}
    if not required <= set(frame.columns):
        raise ValueError("Unexpected HYDAT daily CSV schema")
    if len(frame) >= 10000:
        raise ValueError("HYDAT query may be truncated; pagination required")
    return frame


def hydat(fetcher):
    raw, meta_digest = fetcher.get(HYDAT_BASE + "hydrometric-stations/items",
                                  params=dict(limit=10000, f="json"), timeout=120, max_age_hours=720)
    payload = json.loads(raw)
    if len(payload["features"]) < payload.get("numberMatched", 0):
        raise ValueError("HYDAT station metadata pagination required")
    excluded = old_identities()

    def select():
        candidates = []
        for feature in payload["features"]:
            item = feature["properties"]
            province, station = item["PROV_TERR_STATE_LOC"], item["STATION_NUMBER"]
            if item.get("STATUS_EN") != "Active" or province not in HYDAT_MECHANISMS:
                continue
            if all(f"Environment Canada HYDAT {station} daily {param}" in excluded
                   for param in ["DISCHARGE", "LEVEL"]):
                continue
            coords = feature["geometry"]["coordinates"]
            candidates.append(dict(station=station, station_name=item["STATION_NAME"],
                                   province=province, drainage_area=item.get("DRAINAGE_AREA_GROSS") or 0,
                                   vertical_datum=item.get("VERTICAL_DATUM"),
                                   latitude=coords[1], longitude=coords[0],
                                   excluded_parameters=[p for p in ["DISCHARGE", "LEVEL"]
                                                        if f"Environment Canada HYDAT {station} daily {p}" in excluded]))
        return balanced(candidates, lambda x: x["province"], 850,
                        lambda x: (-x["drainage_area"], x["station"]))

    selected = pin("hydat", select)

    def worker(item):
        params = dict(STATION_NUMBER=item["station"], datetime=f"{START}/..", limit=10000, f="csv")
        raw, digest = fetcher.get(HYDAT_BASE + "hydrometric-daily-mean/items",
                                  params=params, timeout=120, max_age_hours=168)
        try:
            frame = parse_hydat(raw)
        except EligibilityOmission:
            # GeoMet returns no CSV header for a valid zero-match query. Confirm
            # the structured response rather than treating an empty body as data.
            verify_raw, verify_digest = fetcher.get(
                HYDAT_BASE + "hydrometric-daily-mean/items",
                params={**params, "f": "json"}, timeout=120, max_age_hours=168)
            verify = json.loads(verify_raw)
            if verify.get("features") == [] and verify.get("numberMatched") == 0:
                return [], [omit(f"HYDAT {item['station']}", "Source confirms zero observations since " + START)]
            raise ValueError("Empty HYDAT CSV did not correspond to a verified zero-match response")
        if not set(frame.STATION_NUMBER.astype(str)) <= {item["station"]}:
            raise ValueError("HYDAT station filter ignored")
        rows, errors = [], []
        for parameter, unit, category, label in [
            ("DISCHARGE", "m3/s", "canadian_river_flow", "daily mean discharge"),
            ("LEVEL", "m", "canadian_water_levels", "daily mean water level"),
        ]:
            if parameter in item["excluded_parameters"]:
                continue
            candidate = clean_frame(frame.DATE, frame[parameter])
            try:
                rows.append(save(
                    f"hydat_{item['station'].lower()}_{parameter.lower()}", candidate,
                    title=f"{item['station_name']}: {label}",
                    provider="environment_canada_hydat", category=category, unit=unit,
                    source_url=HYDAT_BASE + "hydrometric-daily-mean/items?" + urlencode(params),
                    geography=f"{item['province']}, Canada",
                    hypothesis=f"Research hypothesis: measured {label} may affect {HYDAT_MECHANISMS[item['province']]}. Evaluate seasonal departures and nonlinear water constraints rather than assuming a universal direction.",
                    limitations="Historical HYDAT archive can lag recent observations by months or years and can be revised. No missing dates are filled. Source daily means may contain estimated or ice-affected values; published qualifiers are retained in metadata. Water levels use local station datum and are not comparable in absolute magnitude across gauges. Drainage area ranks diversify economically relevant large catchments but do not establish causal asset exposure.",
                    raw_sha256=[digest, meta_digest],
                    dataset_identity=f"Environment Canada HYDAT {item['station']} daily {parameter}",
                    source_station_metadata=item, latitude=item["latitude"], longitude=item["longitude"],
                    source_qualifiers=sorted(str(x) for x in frame[parameter + "_SYMBOL_EN"].dropna().unique()),
                    native_frequency="daily", finer_frequency_available=False,
                    expected_publication_lag="Historical archive can lag by months or years; inspect actual end date",
                    timestamp_semantics="Station's local calendar-day label represented at UTC midnight; not release time",
                    documentation_url="https://eccc-msc.github.io/open-data/msc-data/obs_hydrometric/readme_hydrometric_en/",
                    license="Open Government Licence - Canada",
                ))
            except EligibilityOmission as exc:
                errors.append(omit(f"HYDAT {item['station']} {parameter}", exc))
        return rows, errors

    return parallel(selected, worker, workers=4)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--provider", choices=["epa", "usgs", "hydat"], action="append")
    args = parser.parse_args(argv)
    providers = args.provider or ["epa", "usgs", "hydat"]
    fetcher = Fetcher(refresh=args.refresh)
    results = {"created_at": pd.Timestamp.now(tz="UTC").isoformat(), "group": GROUP,
               "providers": {}, "series_ids": {}, "errors": [], "expected_omissions": []}
    for name in providers:
        try:
            rows, errors = globals()[name](fetcher)
            results["providers"][name] = len(rows)
            results["series_ids"][name] = sorted(x["series_id"] for x in rows)
            results["errors"].extend(x for x in errors if x.get("kind") != "expected_omission")
            results["expected_omissions"].extend(x for x in errors if x.get("kind") == "expected_omission")
        except Exception as exc:
            results["errors"].append({"provider": name, "error": repr(exc), "kind": "unexpected_error"})
            print(f"ERROR provider {name}: {exc}", flush=True)
    suffix = "_" + "_".join(providers) if args.provider else ""
    atomic_bytes(ROOT / "logs" / f"physical3_collection{suffix}.json", json.dumps(results, indent=2).encode())
    print(json.dumps({k: v for k, v in results.items() if k not in {"expected_omissions", "series_ids"}}, indent=2), flush=True)
    return 1 if results["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
