"""Batch 11 physical operating constraints: Rhine gauges and Eskom operations.

Public archives are fetched with original bytes and request receipts. Short or
sparse histories remain supplementary. NASA SMAP discovery records the concrete
retrieval collection and credential blocker; it never substitutes modeled soil
moisture for measured satellite retrievals.
"""
from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
from pathlib import Path
import re
import time
import zipfile

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests

from expansion_common import (Fetcher, ROOT, archive_raw_manifest, atomic_bytes,
                              coverage, normalize_frame, parquet_bytes,
                              qualifies_research, save_candidate)

GROUP = "physical11"
PEGEL = "https://pegelonline.wsv.de"
ESKOM = "https://www.eskom.co.za/dataportal/"
ESKOM_ENERGY_UNITS = "https://www.nersa.org.za/wp-content/uploads/2024/12/Eskom.pdf"
RHINE = ("MAXAU", "SPEYER", "MANNHEIM", "WORMS", "MAINZ", "OESTRICH", "KAUB",
         "KOBLENZ", "BONN", "KÖLN", "DUISBURG-RUHRORT", "EMMERICH")
SMAP_TARGETS = [
    {"region": "Mato Grosso", "country": "BR", "bbox": [-58, -17, -52, -10], "exposure": "soybeans and maize"},
    {"region": "Parana", "country": "BR", "bbox": [-54, -26, -49, -22], "exposure": "soybeans and maize"},
    {"region": "Buenos Aires and Santa Fe crop belt", "country": "AR", "bbox": [-64, -38, -58, -30], "exposure": "soybeans maize and wheat"},
    {"region": "Punjab and Haryana", "country": "IN", "bbox": [73, 28, 78, 33], "exposure": "wheat and rice"},
    {"region": "Madhya Pradesh", "country": "IN", "bbox": [74, 21, 82, 26], "exposure": "soybeans and wheat"},
    {"region": "Central Thailand", "country": "TH", "bbox": [99, 14, 102, 18], "exposure": "rice"},
    {"region": "Mekong Delta", "country": "VN", "bbox": [104, 9, 107, 12], "exposure": "rice"},
    {"region": "Sumatra", "country": "ID", "bbox": [99, -5, 105, 2], "exposure": "palm oil; dense vegetation may invalidate retrievals"},
]


def sha(content):
    return hashlib.sha256(content).hexdigest()


def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", value.lower().replace("ö", "oe")).strip("_")


def archive_response(response, request, root=ROOT):
    """Archive a public POST download as well as its explicit request parameters."""
    response.raise_for_status()
    content = response.content
    digest = sha(content)
    receipt = dict(request=request, url=response.url, sha256=digest,
                   retrieved_at=pd.Timestamp.now(tz="UTC").isoformat(),
                   content_type=response.headers.get("Content-Type"))
    key = sha(json.dumps(request, sort_keys=True).encode())
    atomic_bytes(Path(root) / "archive/raw" / (digest + ".bin"), content)
    atomic_bytes(Path(root) / "archive/requests" / (key + ".json"), json.dumps(receipt, indent=2).encode())
    atomic_bytes(Path(root) / "archive/receipts" / (key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + ".json"),
                 json.dumps(receipt, indent=2).encode())
    return content, digest


def public_post(url, params, *, root=ROOT, refresh=False):
    request = dict(method="POST", url=url, form=params)
    key = sha(json.dumps(request, sort_keys=True).encode())
    path = Path(root) / "archive/requests" / (key + ".json")
    if path.exists() and not refresh:
        receipt = json.loads(path.read_text())
        raw = Path(root) / "archive/raw" / (receipt["sha256"] + ".bin")
        if raw.exists() and sha(raw.read_bytes()) == receipt["sha256"]:
            return raw.read_bytes(), receipt["sha256"]
    for attempt in range(3):
        response = requests.post(url, data=params, timeout=(20, 240),
                                 headers={"User-Agent": "FinAltDataResearch/1.0 (public time-series research)"})
        if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
            time.sleep(3 ** (attempt + 1))
            continue
        return archive_response(response, request, root)


def clean_values(frame):
    frame = frame[["date", "value"]].copy()
    frame["date"] = pd.to_datetime(frame.date, utc=True, errors="raise")
    frame["value"] = pd.to_numeric(frame.value, errors="raise")
    frame = frame.dropna(subset=["value"])
    if (frame.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting values for one timestamp within a source response")
    return normalize_frame(frame.drop_duplicates("date"))


def persist(series_id, frame, *, root=ROOT, **meta):
    """Use exactly the same eligibility rules for publication and supplements."""
    frame = clean_values(frame)
    stats = coverage(frame)
    meta.update(native_frequency=meta.get("native_frequency", meta["frequency"]),
                point_in_time=False,
                locally_available_at=pd.Timestamp.now(tz="UTC").isoformat())
    if qualifies_research({**stats, "frequency": meta["frequency"]}):
        result = save_candidate(GROUP, series_id, frame, root=root, **meta)
        result["storage_class"] = "research_candidate"
    else:
        result = dict(series_id=series_id, **meta, **stats, daily_eligible=False,
                      research_eligible=False, storage_class="supplementary",
                      retrieved_at=pd.Timestamp.now(tz="UTC").isoformat())
        result["content_sha256"] = sha(frame.date.astype("int64").to_numpy().tobytes() + frame.value.to_numpy().tobytes())
        path = Path(root) / "supplementary/physical11" / (series_id + ".parquet")
        atomic_bytes(path, parquet_bytes(frame, result))
        atomic_bytes(path.with_suffix(".json"), json.dumps(result, indent=2).encode())
    opposite = Path(root) / ("supplementary/physical11" if result["research_eligible"] else "archive/staging/physical11") / (series_id + ".parquet")
    if opposite.exists():
        archive_superseded(opposite, root)
    return result


def archive_superseded(path, root=ROOT):
    destination = Path(root) / "archive/superseded/physical11" / sha(path.read_bytes())
    destination.mkdir(parents=True, exist_ok=True)
    path.replace(destination / path.name)
    sidecar = path.with_suffix(".json")
    if sidecar.exists():
        sidecar.replace(destination / sidecar.name)


def parse_pegel(content):
    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = [n for n in z.namelist() if n.endswith(".json")]
            if len(names) != 1:
                raise ValueError("Expected a single historical JSON gauge member")
            records = json.loads(z.read(names[0]))
    else:
        records = json.loads(content)
    if not records:
        raise ValueError("Gauge returned no observations")
    frame = pd.DataFrame(records)
    if not frame.timestamp.astype(str).str.contains(r"(?:Z|[+-]\d\d:\d\d)$", regex=True).all():
        raise ValueError("Gauge timestamp lacks an explicit source UTC offset")
    if set(frame.columns) - {"timestamp", "value"}:
        raise ValueError("Gauge schema changed; review flags or forecast fields")
    return clean_values(frame.rename(columns={"timestamp": "date"}))


def pegel(fetcher, start="2000-01-01", end=None):
    root = fetcher.root
    end = end or pd.Timestamp.now(tz="UTC").floor("D").isoformat()
    docs = []
    for url in [PEGEL + "/webservice/dokuRestapi", PEGEL + "/gast/nutzungsbedingungen",
                PEGEL + "/gast/downloads", PEGEL + "/gast/hilfe", PEGEL + "/js/wiski-download-modal.js"]:
        _, digest = fetcher.get(url, max_age_hours=168)
        docs.append(digest)
    content, stations_hash = fetcher.get(PEGEL + "/webservices/rest-api/v2/stations.json",
                                        params={"waters": "RHEIN", "includeTimeseries": "true"})
    stations = [s for s in json.loads(content) if s["shortname"] in RHINE]
    if {s["shortname"] for s in stations} != set(RHINE):
        raise ValueError("Pinned Rhine station selection changed")
    atomic_bytes(root / "logs/physical11_pegel_selection.json", json.dumps(stations, indent=2).encode())
    rows, errors = [], []
    for station in stations:
        page_url = PEGEL + "/gast/stammdaten"
        page, page_hash = fetcher.get(page_url, params={"pegelnr": station["number"]}, max_age_hours=168)
        soup = BeautifulSoup(page, "html.parser")
        for ts in station["timeseries"]:
            if ts["shortname"] not in {"W", "Q"}:
                continue
            try:
                form = next(f for f in soup.select("form.wiski-download-form")
                            if f.find("input", {"name": "parameter"})["value"] == ts["longname"])
                action = PEGEL + form["action"]
                if action != PEGEL + "/gast/historische-zeitreihen/prepare-download":
                    raise ValueError("Unknown historical download action")
                params = dict(uuid=station["uuid"], parameter=ts["longname"],
                              start=pd.Timestamp(start, tz="UTC").isoformat() if pd.Timestamp(start).tz is None else pd.Timestamp(start).isoformat(),
                              end=pd.Timestamp(end).isoformat(), format="json")
                raw, digest = public_post(action, params, root=root, refresh=fetcher.refresh)
                frame = parse_pegel(raw)
                frame = frame.loc[(frame.date >= pd.to_datetime(start, utc=True)) & (frame.date <= pd.to_datetime(end, utc=True))]
                metric = "water_level" if ts["shortname"] == "W" else "discharge"
                frequency = {5: "5min", 10: "10min", 15: "15min", 30: "30min", 60: "hourly"}.get(ts["equidistance"], "irregular_intraday")
                result = persist("pegel_" + slug(station["shortname"]) + "_" + metric, frame, root=root,
                    title=f"Rhine {station['longname']}: observed {metric.replace('_', ' ')}",
                    provider="German Waterways and Shipping Administration (WSV) PEGELONLINE", category="inland_freight_constraints",
                    unit=ts["unit"], frequency=frequency, source_url=page_url + "?pegelnr=" + station["number"],
                    geography="Germany; Rhine; " + station["longname"], country_codes=["DE"], geography_scope="non_us",
                    dataset_identity=f"PEGELONLINE|{station['uuid']}|{ts['shortname']}|observed_native",
                    hypothesis="River water levels and flow may anticipate barge load restrictions, freight cost and industrial supply disruptions.",
                    limitations="Unvalidated raw gauge observations; outliers and gaps retained, no interpolation. Gauge-relative level is not water depth or absolute elevation. Gauge datum changes may create breaks. Latest downloaded history does not establish first release.",
                    native_frequency=f"Source nominal {ts['equidistance']}-minute observations; historical cadence may vary.",
                    timestamp_semantics="Observed instant; explicit source MEZ/MESZ offsets converted to UTC. Not publication time.",
                    expected_publication_lag="Operational near-real-time feed; exact historical delay unknown and not reconstructed.",
                    revision_policy="Raw historical values may be revised. Archived original responses retained; latest accepted response determines the candidate.",
                    station_uuid=station["uuid"], station_number=station["number"], source_metric=ts["shortname"],
                    latitude=station["latitude"], longitude=station["longitude"], source_gauge_metadata=ts,
                    license="Datenlizenz Deutschland Zero 2.0 (DL-DE-Zero-2.0)", raw_sha256=docs + [stations_hash, page_hash, digest])
                rows.append(result)
                print(f"PEGEL {station['shortname']} {metric}: {result['rows']} {result['storage_class']}", flush=True)
            except Exception as exc:
                errors.append(dict(source="pegel", station=station["shortname"], metric=ts["shortname"], error=str(exc)))
                print(f"PEGEL ERROR {station['shortname']} {ts['shortname']}: {exc}", flush=True)
    return rows, errors


ESKOM_FILES = {
    "Weekly_Eskom_generation_capacity_breakdown": ("weekly_capacity", "weekly", "outage-performance/weekly-eskom-generation-capacity-breakdown/"),
    "Weekly_Peak_Demand_Financial_Year": ("weekly_peak", "weekly", "demand-side/weekly-peak-demand/"),
    "Weekly_Energy_Demand_Financial_Year": ("weekly_energy", "weekly", "demand-side/weekly-energy-demand/"),
    "Station_Build_Up": ("generation", "hourly", "supply-side/station-build-up-for-the-last-7-days/"),
    "System_hourly_actual_and_forecasted_demand": ("demand", "hourly", "demand-side/system-hourly-actual-and-forecasted-demand/"),
    "System_hourly_demand_and_available_capacity": ("capacity", "hourly", "demand-side/system-hourly-demand-and-available-capacity/"),
    "Pumped_storage_gen_hours_gas_generation_and_manual_load_reduction": ("storage", "hourly", "supply-side/pumped-storage-generating-hours-gas-generation-and-manual-load-reduction/"),
}


def fiscal_week_start(year):
    """Eskom defines the financial week year using the Monday nearest April 1."""
    date = pd.Timestamp(year=int(year), month=4, day=1)
    return date + pd.Timedelta(days=((3 - date.weekday()) % 7) - 3)


class SourceFormatOmission(ValueError):
    """A provider export has ambiguous malformed fields; retain raw, omit values."""


def read_eskom_csv(content, kind):
    text = content.decode("utf-8-sig")
    first = text.splitlines()[0]
    separator = ";" if ";" in first else ","
    records = list(csv.reader(io.StringIO(text), delimiter=separator))
    header = records[0]
    parsed = []
    for record in records[1:]:
        if not record:
            continue
        if len(record) != len(header):
            # Old weekly energy exports contain a single numeric field between
            # date and textual legend, with unquoted comma decimals. Its repair
            # is uniquely determined; multi-metric exports are ambiguous.
            if separator == "," and kind == "weekly_energy" and len(header) == 3 and len(record) == 4 and re.fullmatch(r"[+-]?\d+", record[1]) and re.fullmatch(r"\d+(?:[Ee][+-]?\d+)?", record[2]) and re.match(r"^\d{4}-\d{2}-", record[-1]):
                record = [record[0], record[1] + "." + record[2], record[-1]]
            else:
                raise SourceFormatOmission("Ambiguous malformed source CSV: unquoted decimal commas create more fields than header; no values guessed")
        parsed.append(record)
    source = pd.DataFrame(parsed, columns=header)
    # Semicolon exports explicitly disambiguate comma decimals. Only numeric
    # fields are converted; the first column and textual legends remain exact.
    if separator == ";":
        for col in source.columns[1:]:
            if col not in {"Forecast_Or_Actual_Value_Legend", "Fin_Year"}:
                source[col] = source[col].str.replace(",", ".", regex=False)
    return source.mask(source.eq(""))


def parse_eskom(content, kind):
    source = read_eskom_csv(content, kind)
    source.columns = source.columns.str.strip()
    if kind in {"weekly_peak", "weekly_energy"}:
        cols = {"WeekStartDate", "Forecast_Or_Actual_Value", "Forecast_Or_Actual_Value_Legend"}
        if not cols <= set(source):
            raise ValueError("Unexpected Eskom weekly demand schema")
        source["local_date"] = pd.to_datetime(source.WeekStartDate, format="mixed")
        source["legend"] = source.Forecast_Or_Actual_Value_Legend.astype(str)
        split = source.legend.str.extract(r"^(\d{4})-\d{2}-(.+)$")
        if split.isna().any().any():
            raise ValueError("Unknown fiscal-year legend")
        source["fy"] = split[0].astype(int)
        source["metric"] = split[1]
        # Prior-year comparisons are plotted on current-year dates. Do not shift
        # or misdate them. Older attachments supply their own current-year data.
        aligned = source.apply(lambda r: fiscal_week_start(r.fy) <= r.local_date < fiscal_week_start(r.fy + 1), axis=1)
        source = source.loc[aligned & ~source.metric.str.contains("Forecast", case=False)].copy()
        source["date"] = source.local_date.dt.tz_localize("Africa/Johannesburg").dt.tz_convert("UTC")
        value = pd.to_numeric(source.Forecast_Or_Actual_Value, errors="raise")
        source["value"] = value
        return {metric: clean_values(part) for metric, part in source.dropna(subset=["value"]).groupby("metric")}
    date_col = {"weekly_capacity": "Week Start Date", "generation": "Date_Time_Hour_Beginning",
                "demand": "DateTimeKey", "storage": "Date"}.get(kind, source.columns[0])
    if date_col not in source:
        raise ValueError("Unexpected Eskom time column")
    dates = pd.to_datetime(source[date_col], format="mixed", errors="raise")
    dates = dates.dt.tz_localize("Africa/Johannesburg").dt.tz_convert("UTC")
    outputs = {}
    for col in source:
        if col in {date_col, "Year", "Fin_Year"} or "forecast" in col.lower():
            continue
        # These columns repeat measures already covered by the dedicated
        # demand and generation exports. Keep independent operating measures.
        if kind == "capacity" and col in {"Residual Demand", "RSA Contracted Demand"}:
            continue
        if kind == "storage" and "hours" not in col.lower():
            continue
        values = pd.to_numeric(source[col], errors="raise")
        if not values.notna().any():
            continue
        outputs[col] = clean_values(pd.DataFrame({"date": dates, "value": values}))
    if kind == "weekly_capacity":
        expected = {"Weekly EAF", "Weekly PCLF", "Weekly UCLF", "Weekly OCLF"}
        if set(outputs) != expected:
            raise ValueError("Eskom capacity factor schema changed")
        totals = pd.concat([v.set_index("date").value for v in outputs.values()], axis=1).sum(axis=1)
        if not np.allclose(totals, 100, atol=0.03):
            raise ValueError("Eskom capacity factors fail 100-percent consistency check")
    return outputs


def discover_eskom(fetcher, stem):
    """Only documented public WordPress media API listings supply file URLs."""
    items, hashes = [], []
    for page in range(1, 41):
        raw, digest = fetcher.get(ESKOM + "wp-json/wp/v2/media", params={"search": stem, "per_page": 100, "page": page}, max_age_hours=24)
        batch = json.loads(raw)
        if not isinstance(batch, list):
            raise ValueError("WordPress media listing is not a list")
        hashes.append(digest)
        for item in batch:
            if re.fullmatch(re.escape(stem) + r"(?:-\d+)?\.csv", item["source_url"].rsplit("/", 1)[-1], flags=re.I):
                items.append({k: item.get(k) for k in ["id", "date_gmt", "modified_gmt", "source_url"]})
        if len(batch) < 100:
            break
    else:
        raise ValueError("WordPress media listing exceeded pagination safety bound")
    return items, hashes


def select_eskom_attachments(items, frequency):
    # All distinct weekly exports are small; for rolling hourly CSVs choose the
    # last attachment in each UTC day. That preserves overlapping seven-day
    # windows while avoiding dozens of duplicate intraday generated exports.
    if frequency == "weekly":
        return sorted(items, key=lambda x: (x["modified_gmt"], x["id"]))
    grouped = {}
    for item in sorted(items, key=lambda x: (x["modified_gmt"], x["id"])):
        grouped[item["date_gmt"][:10]] = item
    return sorted(grouped.values(), key=lambda x: (x["modified_gmt"], x["id"]))


def eskom(fetcher):
    root = fetcher.root
    docs = []
    for url in [ESKOM + "glossary/", ESKOM + "data-request-form/", ESKOM + "disclaimer/", ESKOM_ENERGY_UNITS]:
        _, digest = fetcher.get(url, max_age_hours=168)
        docs.append(digest)
    rows, errors = [], []
    selections = {}
    for stem, (kind, frequency, page) in ESKOM_FILES.items():
        _, page_hash = fetcher.get(ESKOM + page, max_age_hours=168)
        items, index_hashes = discover_eskom(fetcher, stem)
        items = select_eskom_attachments(items, frequency)
        selections[stem] = items
        print(f"ESKOM {stem}: {len(items)} public exports selected", flush=True)
        metric_frames = {}
        successful_hashes = []
        receipt_rows = []
        def get_item(item):
            raw, digest = fetcher.get(item["source_url"], timeout=90, max_age_hours=168)
            return item, digest, parse_eskom(raw, kind)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(get_item, x): x for x in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    item, digest, parsed = future.result()
                    successful_hashes.append(digest)
                    for metric, frame in parsed.items():
                        frame = frame.copy()
                        frame["attachment_modified"] = item["modified_gmt"]
                        frame["attachment_id"] = item["id"]
                        frame["raw_sha256"] = digest
                        metric_frames.setdefault(metric, []).append(frame)
                    receipt_rows.append(dict(**item, raw_sha256=digest, metrics=list(parsed)))
                except requests.HTTPError as exc:
                    errors.append(dict(source="eskom", stem=stem, url=item["source_url"], error=f"HTTP {exc.response.status_code}", kind="unavailable_public_attachment"))
                except SourceFormatOmission as exc:
                    errors.append(dict(source="eskom", stem=stem, url=item["source_url"], error=str(exc), kind="malformed_provider_export"))
                except Exception as exc:
                    errors.append(dict(source="eskom", stem=stem, url=item["source_url"], error=str(exc), kind="parse_or_network_error"))
        if not metric_frames:
            continue
        collection = dict(format="physical11_eskom_collection_v1", stem=stem, source_files=receipt_rows,
                          overlap_policy="Largest source attachment modified_gmt then attachment id wins per timestamp; not historical first publication")
        payload = json.dumps(collection, sort_keys=True, indent=2).encode()
        collection_hash = sha(payload)
        atomic_bytes(root / "archive/raw" / (collection_hash + ".bin"), payload)
        atomic_bytes(root / "logs" / ("physical11_eskom_" + kind + "_collection.json"), payload)
        manifest = archive_raw_manifest(docs + [page_hash, collection_hash] + index_hashes + successful_hashes,
                                        description="Eskom public media listings, original CSV exports, selection and overlap policy", root=root)
        for metric, frames in sorted(metric_frames.items()):
            panel = pd.concat(frames, ignore_index=True).sort_values(["date", "attachment_modified", "attachment_id"])
            conflicts = int((panel.groupby("date").value.nunique() > 1).sum())
            frame = panel.drop_duplicates("date", keep="last")[["date", "value"]]
            frame = frame.loc[frame.date <= pd.Timestamp.now(tz="UTC")]
            if frame.empty:
                continue
            sid = "eskom_" + kind + "_" + slug(metric)
            unit = "%" if kind == "weekly_capacity" else "hours" if "hours" in metric.lower() else "MW"
            unit_note = ""
            if kind == "weekly_energy":
                unit = "GWh"
                unit_note = " Weekly energy scale is documented in Eskom's MYPD6 presentation to NERSA, page30 (financial-year weekly residual and RSA contracted energy demand). CSV values are unchanged."
            try:
                result = persist(sid, frame, root=root, title="South Africa Eskom: " + metric,
                    provider="Eskom Data Portal", category="south_africa_power_operations", unit=unit, frequency=frequency,
                    source_url=ESKOM + page, geography="South Africa", country_codes=["ZA"], geography_scope="non_us",
                    dataset_identity=f"Eskom|{stem}|{metric}|{frequency}", source_metric=metric,
                    hypothesis="Electricity availability, demand, emergency generation and stored generating hours may reveal South African industrial constraints and mining output risk.",
                    limitations="Public CSV exports may have gaps and retrospective corrections. Historical first publication is unknown. Earlier-financial-year chart comparisons and forecast columns are excluded. Semicolon exports use comma decimals; malformed single-value weekly energy rows can be unambiguously repaired, but malformed multi-value exports are withheld. No interpolation." + unit_note,
                    timestamp_semantics="Source South Africa local time (UTC+02:00) converted to UTC. Hourly labels are source hour labels; weekly dates are start of full reporting week, not release time.",
                    native_frequency=frequency, finer_frequency_available=(frequency == "weekly"),
                    expected_publication_lag="Not verified. Operational public downloads refreshed periodically; weekly statistics only cover completed weeks. Use archived retrieval times for prospective work.",
                    revision_policy="Overlapping public CSVs resolved by latest attachment modified_gmt then attachment id; original responses retained. These metadata are not treated as historical availability timestamps.",
                    revised_timestamp_count=conflicts, raw_sha256=[page_hash, collection_hash], raw_manifest_sha256=[manifest],
                    source_timezone="Africa/Johannesburg", license="Public Eskom Data Portal; provider disclaimer and terms apply.")
                rows.append(result)
                print(f"ESKOM {sid}: {result['rows']} {result['storage_class']} ({result['start']} to {result['end']})", flush=True)
            except Exception as exc:
                errors.append(dict(source="eskom", series_id=sid, error=str(exc), kind="validation_error"))
    atomic_bytes(root / "logs/physical11_eskom_selection.json", json.dumps(selections, indent=2).encode())
    return rows, errors


def finish_and_audit(root=ROOT):
    """Replay every candidate from archived provider bytes and seal one inventory.

    This performs no acquisitions except archiving the provider's units document
    when upgrading an initial CSV whose units had not yet been confirmed.
    """
    root = Path(root)
    stage = root / "archive/staging" / GROUP
    supplemental = root / "supplementary" / GROUP
    files, supplements, checks, problems = [], [], [], []
    prior_errors = []
    for name in ["pegel", "eskom", "smap_access"]:
        report_path = root / "logs" / f"physical11_{name}_inventory.json"
        if report_path.exists():
            prior_errors.extend(json.loads(report_path.read_text()).get("errors", []))
    omission_kinds = {"unavailable_public_attachment", "malformed_provider_export"}
    omissions = [r for r in prior_errors if r.get("kind") in omission_kinds]
    problems.extend(r for r in prior_errors if r.get("kind") not in omission_kinds)
    collection_cache = {}
    for folder in [stage, supplemental]:
        for path in sorted(folder.glob("*.parquet")):
            if folder == supplemental and (stage / path.name).exists():
                archive_superseded(path, root)
                continue
            meta = json.loads(path.with_suffix(".json").read_text())
            sid = meta["series_id"]
            frame = pd.read_parquet(path)
            try:
                duplicate_export = (sid.startswith("eskom_capacity_") and meta.get("source_metric") in {"Residual Demand", "RSA Contracted Demand"}) or (sid.startswith("eskom_storage_") and "hours" not in meta.get("source_metric", "").lower())
                if duplicate_export:
                    rejected = root / "archive/rejected/physical11_duplicate_export"
                    rejected.mkdir(parents=True, exist_ok=True)
                    path.replace(rejected / path.name)
                    path.with_suffix(".json").replace(rejected / path.with_suffix(".json").name)
                    omissions.append(dict(series_id=sid, reason="Repeated economic measure from dedicated demand/generation export; retained in raw source and rejected-export archive"))
                    continue
                if sid.startswith("pegel_"):
                    sources = []
                    for digest in meta["raw_sha256"]:
                        raw = (root / "archive/raw" / (digest + ".bin")).read_bytes()
                        if sha(raw) != digest:
                            raise ValueError("Raw checksum mismatch")
                        if raw[:2] == b"PK":
                            sources.append(raw)
                    if len(sources) != 1:
                        raise ValueError("Expected exactly one gauge archive")
                    replay = parse_pegel(sources[0])
                    replay = replay.loc[(replay.date >= frame.date.min()) & (replay.date <= frame.date.max())].reset_index(drop=True)
                elif sid.startswith("eskom_"):
                    collections = []
                    for digest in meta["raw_sha256"]:
                        raw = (root / "archive/raw" / (digest + ".bin")).read_bytes()
                        if sha(raw) != digest:
                            raise ValueError("Raw checksum mismatch")
                        if raw[:1] == b"{":
                            obj = json.loads(raw)
                            if obj.get("format") == "physical11_eskom_collection_v1":
                                collections.append((digest, obj))
                    if len(collections) != 1:
                        raise ValueError("Expected exactly one Eskom source collection")
                    digest, collection = collections[0]
                    kind = ESKOM_FILES[collection["stem"]][0]
                    if digest not in collection_cache:
                        parsed_metrics = {}
                        for item in collection["source_files"]:
                            raw = (root / "archive/raw" / (item["raw_sha256"] + ".bin")).read_bytes()
                            if sha(raw) != item["raw_sha256"]:
                                raise ValueError("CSV checksum mismatch")
                            for metric, part in parse_eskom(raw, kind).items():
                                part = part.copy()
                                part["attachment_modified"] = item["modified_gmt"]
                                part["attachment_id"] = item["id"]
                                parsed_metrics.setdefault(metric, []).append(part)
                        collection_cache[digest] = {
                            metric: pd.concat(parts).sort_values(["date", "attachment_modified", "attachment_id"]).drop_duplicates("date", keep="last")[["date", "value"]].sort_values("date").reset_index(drop=True)
                            for metric, parts in parsed_metrics.items()}
                    replay = collection_cache[digest][meta["source_metric"]]
                    replay = replay.loc[(replay.date >= frame.date.min()) & (replay.date <= frame.date.max())].reset_index(drop=True)
                    if kind == "weekly_energy" and meta["unit"] != "GWh":
                        _, unit_hash = Fetcher(root=root).get(ESKOM_ENERGY_UNITS, max_age_hours=876000)
                        meta["unit"] = "GWh"
                        meta["unit_source_url"] = ESKOM_ENERGY_UNITS
                        meta["raw_sha256"] = sorted(set(meta["raw_sha256"] + [unit_hash]))
                        meta["limitations"] = meta["limitations"].replace(" Weekly energy CSV lacks an explicit unit/scale; values preserved without conversion.", " Weekly energy scale GWh is documented in Eskom's MYPD6 presentation to NERSA page30; CSV values unchanged.")
                        atomic_bytes(path, parquet_bytes(frame, meta))
                        atomic_bytes(path.with_suffix(".json"), json.dumps(meta, indent=2).encode())
                else:
                    raise ValueError("Unknown physical11 series")
                pd.testing.assert_frame_equal(frame.reset_index(drop=True), replay, check_dtype=False, check_exact=True)
                eligible = qualifies_research({**coverage(frame), "frequency": meta["frequency"]})
                if eligible != (folder == stage):
                    raise ValueError("Storage class does not match history eligibility")
                item = dict(series_id=sid, path=str(path.relative_to(root)), parquet_sha256=sha(path.read_bytes()),
                            metadata_sha256=sha(path.with_suffix(".json").read_bytes()), rows=len(frame))
                if folder == stage:
                    files.append(item)
                    checks.append(dict(series_id=sid, parquet_sha256=item["parquet_sha256"], rows=len(frame), source_values_verified=True))
                else:
                    supplements.append(dict(path=item["path"], sha256=item["parquet_sha256"], series_id=sid, rows=len(frame),
                                            reason="Native observations lack the required span or coverage; no interpolation"))
                print(f"AUDIT {sid}: {len(frame)} source observations verified", flush=True)
            except Exception as exc:
                problems.append(dict(series_id=sid, error=str(exc), kind="source_audit_error"))
    source_hash = sha(Path(__file__).read_bytes())
    access_path = root / "logs/physical11_smap_access.json"
    access = json.loads(access_path.read_text()) if access_path.exists() else {}
    report = dict(source_sha256=source_hash, files=files, supplementary_files=supplements,
                  series_count=len(files), observations=sum(x["rows"] for x in files),
                  supplementary_series_count=len(supplements), supplementary_observations=sum(x["rows"] for x in supplements),
                  errors=problems, omissions=omissions, access=access, finished_at=pd.Timestamp.now(tz="UTC").isoformat())
    audit = dict(source_sha256=source_hash, series_checks=checks, supplementary_checks=supplements,
                 errors=problems, finished_at=pd.Timestamp.now(tz="UTC").isoformat())
    atomic_bytes(root / "logs/physical11_inventory.json", json.dumps(report, indent=2).encode())
    atomic_bytes(root / "logs/physical11_source_audit.json", json.dumps(audit, indent=2).encode())
    return report


def smap_access(fetcher):
    params = dict(short_name="SPL3SMP", version="009", temporal="2025-01-01T00:00:00Z,2025-01-02T23:59:59Z",
                  bounding_box="-58,-17,-52,-10", page_size=1)
    raw, digest = fetcher.get("https://cmr.earthdata.nasa.gov/search/granules.json", params=params, max_age_hours=168)
    entries = json.loads(raw)["feed"]["entry"]
    if len(entries) != 1:
        raise ValueError("No concrete SMAP probe granule")
    entry = entries[0]
    link = next(x["href"] for x in entry["links"] if x.get("rel", "").endswith("/data#") and x["href"].endswith(".h5"))
    # HEAD may expose object metadata without authorizing GET. Probe the real
    # download with streaming so an unexpectedly public large file is not read.
    response = requests.get(link, timeout=(15, 45), allow_redirects=True, stream=True)
    auth_status = response.status_code
    auth_type = response.headers.get("Content-Type")
    auth_host = requests.utils.urlparse(response.url).hostname
    response.close()
    if auth_status not in {401, 403}:
        raise ValueError(f"SMAP download authentication behavior changed: HTTP{auth_status}; review access before claiming a credential blocker")
    report = dict(status="blocked_requires_earthdata_login", source="NASA NSIDC SMAP observed Level-3 daily retrievals",
        collection="C2938664585-NSIDC_CPRD", short_name="SPL3SMP", version="009", granule_id=entry["id"],
        granule_title=entry["title"], sample_download_url=link, public_metadata_sha256=digest,
        unauthenticated_http_status=auth_status, unauthenticated_content_type=auth_type, final_host=auth_host,
        signup_url="https://urs.earthdata.nasa.gov/users/new",
        authentication_docs="https://nsidc.org/data/user-resources/help-center/downloading-nsidc-daac-earthdata-cloud-data-using-earthaccess",
        credential_setup="Free Earthdata account; scoped earthaccess environment or .netrc login. Do not send password in logs. NSIDC application authorization may be required.",
        intended_regions=SMAP_TARGETS, planned_history_start="2023-01-01", native_frequency="Daily files; AM and PM overpasses; individual cells not observed daily",
        required_quality_rules="Use source retrieval quality and surface flags; preserve observed-valid pixel counts, coverage fraction, retrieval error and AM/PM distinction. No interpolation, no weather-model substitution. Region mean is not crop-weighted until a documented crop mask exists.",
        alternatives_checked=[dict(source="University of Hamburg ICDC daily measured ASCAT 5-day composites", url="https://icdc.cen.uni-hamburg.de/thredds-int/aggregationAscatSoilMoistureCatalog.html", result="HTTP401; credentials required; not downloaded")],
        checked_at=pd.Timestamp.now(tz="UTC").isoformat())
    atomic_bytes(fetcher.root / "logs/physical11_smap_access.json", json.dumps(report, indent=2).encode())
    return report


def collect(source="all", refresh=False, start="2000-01-01", end=None):
    if source == "all":
        for name in ["pegel", "eskom", "smap-access"]:
            collect(name, refresh, start, end)
        return finish_and_audit()
    fetcher = Fetcher(refresh=refresh)
    rows, errors, access = [], [], {}
    for name, fn in [("pegel", lambda: pegel(fetcher, start, end)), ("eskom", lambda: eskom(fetcher))]:
        if source in {name, "all"}:
            result, failed = fn()
            rows.extend(result); errors.extend(failed)
            partial = dict(series_count=sum(x["research_eligible"] for x in rows), supplementary_series_count=sum(not x["research_eligible"] for x in rows), errors=errors, status="running")
            atomic_bytes(ROOT / "logs/physical11_progress.json", json.dumps(partial, indent=2).encode())
    if source in {"smap-access", "all"}:
        access = smap_access(fetcher)
    files = []
    for meta in rows:
        parent = "archive/staging/physical11" if meta["research_eligible"] else "supplementary/physical11"
        path = ROOT / parent / (meta["series_id"] + ".parquet")
        files.append(dict(series_id=meta["series_id"], storage_class=meta["storage_class"], path=str(path.relative_to(ROOT)),
                          rows=meta["rows"], parquet_sha256=sha(path.read_bytes()), metadata_sha256=sha(path.with_suffix(".json").read_bytes())))
    report = dict(source=source, status="complete", series_count=sum(x["research_eligible"] for x in rows),
                  observations=sum(x["rows"] for x in rows if x["research_eligible"]),
                  supplementary_series_count=sum(not x["research_eligible"] for x in rows),
                  supplementary_observations=sum(x["rows"] for x in rows if not x["research_eligible"]),
                  files=files, errors=errors, access=access, source_sha256=sha(Path(__file__).read_bytes()),
                  finished_at=pd.Timestamp.now(tz="UTC").isoformat())
    atomic_bytes(ROOT / "logs" / ("physical11_" + source.replace("-", "_") + "_inventory.json"), json.dumps(report, indent=2).encode())
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=["all", "pegel", "eskom", "smap-access", "audit"], default="all")
    p.add_argument("--refresh", action="store_true")
    p.add_argument("--start", default="2000-01-01")
    p.add_argument("--end")
    args = p.parse_args()
    report = finish_and_audit() if args.source == "audit" else collect(args.source, args.refresh, args.start, args.end)
    print(json.dumps({k: v for k, v in report.items() if k not in {"files", "errors", "access"}}, indent=2))
