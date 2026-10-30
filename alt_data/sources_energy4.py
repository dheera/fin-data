#!/usr/bin/env python3
"""Foreign physical energy activity, source-native observations, expansion 4.

All data are downloaded through Fetcher. Missing intervals are not generated.
Observation timestamps must not be treated as historical publication timestamps.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import hashlib
import json
from pathlib import Path
import re
import time
import unicodedata
from urllib.parse import urljoin
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd

from expansion_common import (ROOT, Fetcher, atomic_bytes, coverage,
                              normalize_frame, qualifies_research, save_candidate, archive_raw_manifest)

GROUP = "energy4"
START = "2024-01-01"
END = pd.Timestamp.now(tz="UTC").normalize()
STAGING = ROOT / "archive/staging" / GROUP
ERRORS, OMISSIONS = [], []
BASELINE = None
SEEN_HASHES, SEEN_IDENTITIES = set(), set()
ONS_API = "https://dados.ons.org.br/api/3/action/package_show"
ENTSOG_API = "https://transparency.entsog.eu/api/v1"


def slug(value):
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def baseline():
    global BASELINE
    if BASELINE is None:
        data = pd.read_parquet(ROOT / "catalog.parquet")
        data = data[data.batch < 6]
        BASELINE = {"ids": set(data.series_id), "identities": set(data.dataset_identity.dropna()),
                    "hashes": set(data.content_sha256.dropna())}
    return BASELINE


def omit(sid, reason):
    OMISSIONS.append({"source": sid, "reason": str(reason)})


def clean(dates, values):
    frame = pd.DataFrame({"date": dates, "value": pd.to_numeric(values, errors="raise")})
    frame["date"] = pd.to_datetime(frame.date, utc=True)
    frame = frame.dropna(subset=["date", "value"])
    if frame.empty:
        return frame
    if frame.groupby("date").value.nunique().max() > 1:
        raise ValueError("Conflicting measurements for an observation interval")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    base = baseline()
    ident = meta["dataset_identity"]
    if sid in base["ids"] or ident in base["identities"] or ident in SEEN_IDENTITIES:
        omit(sid, "Underlying series already represented")
        return None
    if frame.empty or frame.value.nunique() < 2:
        omit(sid, "Empty or constant history")
        return None
    frame = normalize_frame(frame)
    if not qualifies_research({**coverage(frame), "frequency": meta["frequency"]}):
        omit(sid, "Insufficient source-native observation coverage")
        return None
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                            + frame.value.to_numpy().tobytes()).hexdigest()
    if digest in base["hashes"] or digest in SEEN_HASHES:
        omit(sid, "Identical full numeric history")
        return None
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("finer_frequency_available", False)
    meta.setdefault("geography_scope", "non_us")
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2")
    existing = STAGING / (sid + ".json")
    if existing.exists() and json.loads(existing.read_text())["dataset_identity"] != ident:
        raise ValueError(f"Series ID collision for {sid}")
    result = save_candidate(GROUP, sid, frame, **meta)
    SEEN_IDENTITIES.add(ident)
    SEEN_HASHES.add(digest)
    print("OK", sid, result["rows"], result["observed_days"], flush=True)
    return result


def resource_date(resource):
    found = re.search(r"_(20\d\d)(?:_(\d\d))?(?:_(\d\d))?\.(?:parquet|csv)", resource["url"])
    if not found:
        return None
    return pd.Timestamp("-".join([found[1], found[2] or "01", found[3] or "01"]))


def ons_package(fetch, name):
    raw, digest = fetch.get(ONS_API, {"id": name}, timeout=120)
    obj = json.loads(raw)
    if not obj.get("success"):
        raise ValueError("ONS metadata request unsuccessful")
    return obj["result"], digest


def ons_dictionary(fetch, package):
    item = next(x for x in package["resources"] if x["name"] == "Dicionário de Dados Json")
    raw, digest = fetch.get(item["url"])
    data = json.loads(raw.decode("utf-8-sig"))
    return {x["codigo"]: x["descricao"] for x in data["dicionario_simplificado"]}, digest, item["url"]


def ons_local_time(values):
    # ONS dispatch is reported in Brasília operational time, including plants
    # geographically located in another Brazilian time zone.
    dates = pd.to_datetime(values, errors="raise")
    if dates.dt.tz is not None:
        raise ValueError("Unexpected already-zoned ONS timestamp")
    return dates.dt.tz_localize("America/Sao_Paulo", ambiguous="raise",
                               nonexistent="raise").dt.tz_convert("UTC")


HYDRAULIC = {
    "val_nivelmontante": ("upstream_level", "Upstream reservoir level", "m"),
    "val_niveljusante": ("downstream_level", "Downstream water level", "m"),
    "val_volumeutil": ("usable_volume", "Usable reservoir volume", "percent"),
    "val_vazaoafluente": ("inflow", "Reservoir water inflow", "m3/s"),
    "val_vazaodefluente": ("outflow", "Reservoir water outflow", "m3/s"),
    "val_vazaoturbinada": ("turbine_flow", "Water flow through turbines", "m3/s"),
    "val_vazaovertida": ("spill_flow", "Spillway water flow", "m3/s"),
    "val_vazaooutrasestruturas": ("other_structure_flow", "Other structures water flow", "m3/s"),
    "val_vazaotransferida": ("transfer_flow", "Transferred water flow", "m3/s"),
    "val_vazaovertidanaoturbinavel": ("unusable_spill_flow", "Non-turbinable spilled water", "m3/s"),
}
DAILY_HYDRAULIC = {
    "val_vazaonatural": ("naturalized_flow", "Naturalized river flow", "m3/s"),
    "val_vazaoartificial": ("artificial_flow", "Artificial water flow", "m3/s"),
    "val_vazaoincremental": ("incremental_flow", "Incremental basin water flow", "m3/s"),
    "val_vazaoevaporacaoliquida": ("net_evaporation_flow", "Net evaporation water flow", "m3/s"),
    "val_vazaousoconsuntivo": ("consumptive_flow", "Consumptive water-use flow", "m3/s"),
    "val_vazaoincrementalbruta": ("gross_incremental_flow", "Gross incremental water flow", "m3/s"),
}
THERMAL = {
    "val_proggeracao": ("scheduled_generation", "Scheduled generation"),
    "val_progdisponibilidade": ("scheduled_availability", "Availability declared for daily scheduling"),
    "val_verifordemmerito": ("economic_dispatch", "Actual generation dispatched in economic merit order"),
    "val_verifinflexibilidade": ("inflexible_dispatch", "Actual generation attributed to inflexibility"),
    "val_verifrazaoeletrica": ("grid_security_dispatch", "Actual generation for electrical system security"),
    "val_verifgarantiaenergetica": ("energy_security_dispatch", "Actual generation for energy security"),
    "val_verifgfom": ("fuel_contingency_dispatch", "Actual generation compensating future fuel unavailability"),
    "val_verifreposicaoperdas": ("loss_replacement_dispatch", "Actual generation to replace transmission losses"),
    "val_verifexportacao": ("export_dispatch", "Actual generation for electricity exports"),
    "val_verifreservapotencia": ("reserve_dispatch", "Actual generation to replenish operating reserves"),
    "val_verifgsub": ("fuel_substitution_dispatch", "Actual generation replacing unavailable-fuel generation"),
    "val_verifunitcommitment": ("commitment_dispatch", "Actual generation for unit commitment constraints"),
    "val_verifconstrainedoff": ("constrained_dispatch", "Actual merit-order generation constrained off"),
    "val_geracaodespachada": ("instructed_generation", "Real-time generation instructed for the national system"),
}
ONS_CONFIG = {
    "generation": ("geracao-usina-2", "hourly",
                   {"val_geracao": ("generation", "Reported plant or plant-group generation", "MW")}),
    "hydraulic": ("dados_hidrologicos_ho", "hourly", HYDRAULIC),
    "water_balance": ("dados-hidrologicos-res", "daily", DAILY_HYDRAULIC),
    "availability": ("disponibilidade_usina", "hourly", {
        "val_dispoperacional": ("operational_availability", "Operational generation capacity available", "MW"),
        "val_dispsincronizada": ("synchronized_availability", "Synchronized generation capacity available", "MW")}),
    "thermal": ("geracao-termica-despacho-2", "hourly",
                {field: (*item, "MW") for field, item in THERMAL.items()}),
    "wind": ("restricao_coff_eolica_detail", "30min", {
        "val_geracaoverificada": ("generation", "Verified wind-plant generation", "MW"),
        "val_geracaoestimada": ("unconstrained_generation_estimate", "Source estimate of uncurtailed wind generation", "MW"),
        "flg_geracaorestrita": ("generation_restricted", "Wind plant-group generation restricted indicator", "0 or 1")}),
    "solar": ("restricao_coff_fotovoltaica_detail", "30min", {
        "val_geracaoverificada": ("generation", "Verified solar-plant generation", "MW"),
        "val_geracaoestimada": ("unconstrained_generation_estimate", "Source estimate of uncurtailed solar generation", "MW"),
        "flg_geracaorestrita": ("generation_restricted", "Solar plant-group generation restricted indicator", "0 or 1")}),
}


def collect_ons(fetch, variant):
    dataset, frequency, metrics = ONS_CONFIG[variant]
    package, package_hash = ons_package(fetch, dataset)
    dictionary, dict_hash, dict_url = ons_dictionary(fetch, package)
    resources = {r["url"]: r for r in package["resources"]
                 if r.get("format", "").upper() == "PARQUET"
                 and resource_date(r) is not None
                 and pd.Timestamp(START) <= resource_date(r) <= END.tz_localize(None)}
    if not resources:
        raise ValueError("ONS returned no historical Parquet resources")
    frames, hashes, descriptions = [], [package_hash, dict_hash], {}
    identifier = "id_reservatorio" if variant in {"hydraulic", "water_balance"} else "id_ons"
    for url, item in sorted(resources.items(), key=lambda item: resource_date(item[1])):
        raw, digest = fetch.get(url, timeout=180)
        part = pd.read_parquet(BytesIO(raw))
        hashes.append(digest)
        part = part.loc[pd.to_datetime(part.din_instante) >= pd.Timestamp(START)].copy()
        if variant == "generation":
            # Individual wind/solar plants have richer half-hourly data in
            # the dedicated panels. Keep only their aggregate plant groups,
            # whose ONS identifiers and physical coverage are different.
            is_renewable = part.nom_tipousina.str.contains("EOL|FOTOV", case=False, na=False)
            is_group = part.id_ons.astype(str).str.startswith("CJU_") | part.id_ons.isna()
            part = part.loc[~is_renewable | is_group].copy()
        if variant == "thermal":
            # CEG is an official persistent plant identity; no changing names.
            identifier = "ceg"
        namecol = "nom_reservatorio" if "nom_reservatorio" in part else "nom_usina"
        if identifier == "id_ons":
            # Some official small-generation groups have no ONS code. They
            # are grouped by the source's explicit name and state, not split
            # into invented plants.
            part[identifier] = part[identifier].fillna("GROUP:" + part.get("id_estado", "") + ":" + part[namecol])
        part[identifier] = part[identifier].astype(str).str.strip()
        for key, row in part.drop_duplicates(identifier, keep="last").set_index(identifier).iterrows():
            descriptions[key] = {k: str(row[k]).strip() for k in [
                namecol, "id_estado", "nom_estado", "nom_subsistema", "nom_bacia",
                "nom_tipocombustivel", "nom_combustivel", "cod_modalidadeoperacao", "ceg"
            ] if k in row and pd.notna(row[k])}
        # Only source-native measurements; null fields remain missing.
        fields = [f for f in metrics if f in part]
        if not fields:
            raise ValueError(f"{dataset}: source schema lacks the selected metrics")
        compact = part[[identifier, "din_instante", *fields]].copy()
        compact["date"] = (pd.to_datetime(compact.pop("din_instante"), utc=True)
                           if frequency == "daily" else ons_local_time(compact.pop("din_instante")))
        for field in fields:
            compact[field] = pd.to_numeric(compact[field], errors="raise").astype("float64")
        compact = compact[compact.date < END]
        compact[identifier] = compact[identifier].astype("category")
        frames.append(compact)
        print("FETCH", variant, item["name"], len(compact), flush=True)
    data = pd.concat(frames, ignore_index=True)
    del frames
    out = []
    for key, part in data.groupby(identifier, observed=True, sort=True):
        desc = descriptions[str(key)]
        name = desc.get("nom_usina", desc.get("nom_reservatorio", str(key)))
        geography = "Brazil / " + " / ".join(desc[k] for k in ["nom_estado", "id_estado", "nom_bacia"]
                                             if k in desc)
        for field, (metric, title, unit) in metrics.items():
            if field not in part:
                continue
            # A group restriction flag is common to all constituent plants.
            # Use full-history deduplication as an additional guard, but label
            # it as group-level rather than claim individual plant exposure.
            subset = part[["date", field]].dropna().copy()
            conflicts = subset.groupby("date")[field].nunique()
            conflicts = conflicts[conflicts > 1].index
            if len(conflicts) and variant == "thermal":
                # Some combined-cycle CEGs have mutually exclusive planning
                # configurations P0/P1/... with no unambiguous plant-level
                # availability aggregation. Do not invent a combining rule.
                omit(f"{dataset}/{key}/{field}", "Concurrent thermal operating configurations or conflicting plant reports")
                continue
            if len(conflicts):
                subset = subset[~subset.date.isin(conflicts)]
            frame = clean(subset.date, subset[field])
            entity = desc.get("ceg", str(key))
            if entity in {"-", "", "<NA>"}:
                entity = str(key)
            identity = f"ONS:{'reservoir' if 'reservatorio' in identifier else 'plant'}:{entity}:{metric}"
            category = ("hydropower_water_balance" if variant == "water_balance" else
                        "hydropower_operations" if variant == "hydraulic" else
                        "renewable_power_operations" if variant in {"wind", "solar"} else
                        "power_plant_availability" if variant == "availability" else
                        "thermal_dispatch" if variant == "thermal" else "power_plant_generation")
            hypothesis = ("Plant output, dispatch constraints, fuel-substitution demand, synchronized reserves and reservoir operating conditions can lead Brazilian power tightness, utility production, gas/LNG and coal demand, industrial power costs and hydro-sensitive regional assets. Different plants and basins provide physical geographic exposures; related components should not be treated as independent signals.")
            limitations = ("Official ONS source histories, revised after publication. Native operating intervals are retained; explicit zeros retained and missing intervals never filled. Power values are interval-average MW, not MWh. Brasília operational clock is interpreted using America/Sao_Paulo. Source publication time is not the observation timestamp. This is a present-day historical panel and not a historical point-in-time universe. Plant groups and component measures overlap; do not sum them with individual plants without checking coverage.")
            if variant == "hydraulic":
                limitations += " Hourly hydraulic reports are submitted by generation agents and explicitly not consistency-checked by ONS. Timestamp is the source hour ending."
            elif variant == "water_balance":
                limitations += " Naturalized, incremental, artificial, evaporation and consumptive flows are ONS water-balance estimates rather than independent direct gauge measurements. Date is the source daily calendar label at UTC midnight."
            elif variant in {"wind", "solar"}:
                limitations += " Unconstrained-generation estimates are source-produced theoretical output, using wind/irradiance curves or generation history; they are not measured generation. The binary restriction flag describes the plant group and may be shared across constituent plants. The counterfactual estimate and verified production are related, not independent observations."
            elif variant == "generation":
                limitations += " Small plant-group (Tipo III / MMGD) generation can be source estimates or forecasts; named individual plants and Tipo II-C groups are verified operating data. Source operational modality is preserved."
            elif variant == "thermal":
                limitations += " Scheduled and instructed dispatch are operator plans/instructions, not realized production. Actual dispatch-reason components overlap total generation and sometimes each other. No duplicate total-generation variant is added."
            if len(conflicts):
                limitations += f" {len(conflicts)} conflicting source timestamps are omitted because the file does not provide a revision ordering for them."
            item = save(f"ons_{slug(key)}_{metric}", frame, title=f"Brazil {name}: {title}",
                        provider="Brazil ONS", category=category, unit=unit, frequency=frequency,
                        source_url=f"https://dados.ons.org.br/dataset/{dataset}", geography=geography,
                        hypothesis=hypothesis, limitations=limitations, raw_sha256=hashes,
                        dataset_identity=identity, documentation_url=dict_url, source_column=field,
                        source_dataset=dataset, source_entity_id=str(key), source_entity_metadata=desc,
                        source_description=dictionary.get(field, title), source_urls=list(resources),
                        conflicting_source_timestamps_omitted=len(conflicts),
                        expected_publication_lag="Operational data updated after observation, revised in subsequent monthly historical files; daily water balances also revised",
                        timestamp_semantics="Source daily label at UTC midnight" if frequency == "daily" else "Brasília operational interval; UTC conversion",
                        value_kind="source_model_estimate" if ("estimate" in metric or variant == "water_balance"
                            or variant == "generation" and "Pequenas" in desc.get("cod_modalidadeoperacao", "")) else "reported_operational_value",
                        country_codes=["BR"], license="Creative Commons Attribution; ONS")
            if item:
                out.append(item)
    return out


ENTSOG_METRICS = ["Physical Flow", "Allocation", "Nomination", "Renomination",
                  "GCV", "Wobbe Index"]
ENTSOG_LOCATION_DOCS = ["https://www.tap-ag.com/shippers",
                        "https://www.bblcompany.com/about-bbl"]


def entsog_geography(info):
    iso = lambda code: {"UK": "GB"}.get(code, code)
    operator, point = info["operatorKey"], info["pointKey"]
    if operator == "AL-TSO-0001":
        codes = {"ITP-00008": ["IT"], "ITP-00274": ["GR"], "ITP-00275": ["GR"],
                 "ITP-00427": ["GR"], "VTP-00044": ["GR"]}.get(point, [])
        return codes, "TAP-published point location; virtual point uses the source Greek balancing zone"
    if operator == "UK-TSO-0004":
        codes = {"ITP-00063": ["NL"], "ITP-00207": ["GB"]}.get(point, [])
        return codes, "BBL-published physical terminal location"
    codes = sorted({iso(str(info[k])) for k in ["tSOCountry", "adjacentCountry"] if info.get(k)})
    return codes, "Countries of the reporting and adjacent gas networks in ENTSOG metadata; not exact station coordinates"


def parse_entsog(raw):
    obj = json.loads(raw)
    keys = [k for k in obj if k.lower() == "operationaldata"]
    if len(keys) != 1:
        raise ValueError("Unexpected ENTSOG response")
    data = pd.DataFrame(obj[keys[0]])
    if data.empty:
        return data
    data["date"] = pd.to_datetime(data.periodFrom, utc=True, format="mixed")
    end = pd.to_datetime(data.periodTo, utc=True, format="mixed")
    duration = (end - data.date).dt.total_seconds() / 3600
    # Validity ranges are not daily observations. In particular, capacities
    # valid for months/years are never expanded into fabricated daily values.
    data = data[(duration >= 23) & (duration <= 25) & data.value.notna()
                & ~data.isNA.eq(True)].copy()
    data["value"] = pd.to_numeric(data.value, errors="raise")
    return data


def collect_entsog(fetch):
    raw, meta_hash = fetch.get(ENTSOG_API + "/operatorPointDirections", {"limit": -1})
    points = pd.DataFrame(json.loads(raw)["operatorPointDirections"])
    points = points[points.hasData.eq(True)]
    location_hashes = [fetch.get(url)[1] for url in ENTSOG_LOCATION_DOCS]
    pointmap = {(r.operatorKey, r.pointKey, r.directionKey): r._asdict()
                for r in points.itertuples(index=False)}
    out = []
    # Monthly windows and one indicator at a time follow ENTSOG's explicit
    # fair-use guidance; this provider is intentionally queried serially.
    starts = pd.date_range(START, END.tz_localize(None), freq="MS")
    for metric in ENTSOG_METRICS:
        frames, hashes = [], [meta_hash, *location_hashes]
        for start in starts:
            stop = min(start + pd.offsets.MonthBegin(), END.tz_localize(None))
            if start >= stop:
                continue
            params = {"from": start.date().isoformat(), "to": stop.date().isoformat(),
                      "indicator": metric, "periodType": "day", "limit": -1,
                      "includeExemptions": 0, "timeZone": "WET"}
            raw, digest = fetch.get(ENTSOG_API + "/operationaldata", params, timeout=90)
            part = parse_entsog(raw)
            hashes.append(digest)
            if len(part):
                frames.append(part)
            print("FETCH ENTSOG", metric, start.date(), len(part), flush=True)
            time.sleep(0.5)
        if not frames:
            continue
        data = pd.concat(frames, ignore_index=True)
        data = data[data.date < END]
        for key, part in data.groupby(["operatorKey", "pointKey", "directionKey"], sort=True):
            if key not in pointmap:
                omit(str(key), "Not in active TSO point-direction reference panel")
                continue
            info = pointmap[key]
            if info.get("isDoubleReporting") is True:
                omit(str(key), "ENTSOG explicitly marks this point as double reporting another point")
                continue
            # Latest source revision wins, but conflicting records at the
            # same source revision are rejected rather than averaged.
            part = part.copy()
            part["_updated"] = pd.to_datetime(part.lastUpdateDateTime, utc=True, format="mixed")
            latest = part.groupby("date")["_updated"].transform("max")
            part = part[part["_updated"].eq(latest)]
            if part.unit.nunique() != 1:
                omit(str(key), "Source unit changes within history")
                continue
            frame = clean(part.date, part.value)
            countries, country_basis = entsog_geography(info)
            category = ("gas_quality" if metric in {"GCV", "Wobbe Index", "Methane Content", "Hydrogen Content", "Oxygen Content"}
                        else "gas_capacity_booking" if any(w in metric for w in ["Booked", "Available"])
                        else "gas_pipeline_flows")
            item = save("entsog_" + "_".join(slug(x) for x in (*key, metric)), frame,
                        title=f"{'/'.join(countries) or 'European network'} {info['operatorLabel']} {info['pointLabel']} {key[2]}: {metric}",
                        provider="ENTSOG Transparency", category=category,
                        unit=str(part.unit.iloc[0]), frequency="daily",
                        source_url=ENTSOG_API + "/operationaldata",
                        geography=f"{'/'.join(countries) or 'European network'} gas connection / {info['pointLabel']}",
                        hypothesis="Cross-border, LNG, storage and domestic-exit gas flow, shipper nominations, booking pressure and calorific composition can lead European gas scarcity, supply rerouting, industrial gas demand, power fuel switching and energy-sensitive margins.",
                        limitations="Reported operator-point-direction daily periods only; validity intervals longer than 25 hours are omitted rather than expanded. Nominations and renominations are shipper intentions, allocations are settlement accounting, and physical flows are operational reports. These metrics are related and may coincide. Counterparty directions may report the same physical crossing; identities retain the reporting TSO, and exact numeric duplicates are excluded. Figures can be provisional, corrected and backfilled. kWh/d is the source daily energy-rate unit; no conversion to gas volume. Daily start timestamps include the source UTC offset; clock-change days retain 23/25-hour gas periods. Dates are not historical publication availability. Virtual points and station aggregates overlap the physical network.",
                        raw_sha256=hashes, dataset_identity="ENTSOG:" + ":".join((*key, metric)),
                        documentation_url="https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/ENTSOG_TP_API_UserManual_v3.0.pdf",
                        source_column="value", source_indicator=metric,
                        source_operator_key=key[0], source_point_key=key[1], source_direction=key[2],
                        source_point_type=info.get("pointType"), source_adjacent_country=info.get("adjacentCountry"),
                        country_codes=countries, country_code_basis=country_basis,
                        source_operator_country=info["tSOCountry"],
                        geography_documentation_urls=ENTSOG_LOCATION_DOCS,
                        expected_publication_lag="Operational publication and subsequent TSO revisions; recent periods can be provisional",
                        native_frequency="daily gas-period observations",
                        finer_frequency_available=False,
                        finer_frequency_note="API also supports hourly reports for some operator points; availability has not been verified for this individual series",
                        timestamp_semantics="Source gas-day period start, original offset converted to UTC")
            if item:
                out.append(item)
    return out


ENERGI_CONFIG = {
    "PrivateConsumptionHeatingHour": ("TimeUTC", ["MunicipalityCode", "HousingCategory", "HeatingCategory"],
                                     ["ConsumptionkWh"], "household_energy_behavior"),
    "ConsumptionGridAreaHour": ("TimeUTC", ["GridArea"], ["ConsumptionkWh"], "local_electricity_demand"),
    "MrStationsGasquality": ("HourUTC", ["MRStation"],
                            ["GrossCalorificValueMJ", "WobbeIndexMJ", "NormalDensity", "StationFlow"], "local_gas_delivery"),
    "ProductionRegionHour": ("TimeUTC", ["RegionName", "ProductionType2", "ProductionType3"],
                            ["Production_kWh"], "regional_power_generation"),
    "AfrrReservesNordic": ("TimeUTC", ["PriceArea"],
                          ["UpDemandMW", "UpProcuredMW", "UpPriceEUR", "DownDemandMW", "DownProcuredMW", "DownPriceEUR"],
                          "electricity_reserve_markets"),
    "FcrNdDK2": ("HourUTC", ["PriceArea", "ProductName", "AuctionType"],
                ["PurchasedVolumeLocal", "PurchasedVolumeTotal", "PriceTotalEUR"], "electricity_reserve_markets"),
    "mFRRCapacityMarket": ("TimeUTC", ["PriceArea"],
                         ["UpDemandMW", "UpProcuredMW", "UpPriceEUR", "DownDemandMW", "DownProcuredMW", "DownPriceEUR"],
                         "electricity_reserve_markets"),
    "InertiaNordicSyncharea": ("HourUTC", [],
                              ["InertiaNordicGWs", "InertiaDK2GWs", "InertiaNOGWs", "InertiaSEGWs", "InertiaFIGWs"],
                              "grid_stability"),
}


def energinet_geography(dataset, fields, values, metric):
    names = {"DK": "Denmark", "NO": "Norway", "SE": "Sweden", "FI": "Finland"}
    if dataset == "InertiaNordicSyncharea":
        code = {"InertiaDK2GWs": "DK", "InertiaNOGWs": "NO",
                "InertiaSEGWs": "SE", "InertiaFIGWs": "FI"}.get(metric)
        return ((names[code], [code]) if code else
                ("Nordic synchronous area", ["DK", "NO", "SE", "FI"]))
    if dataset == "FcrNdDK2" and metric in {"PurchasedVolumeTotal", "PriceTotalEUR"}:
        return "Denmark–Sweden joint reserve market", ["DK", "SE"]
    if "PriceArea" in fields:
        code = values[fields.index("PriceArea")][:2]
        if code not in names:
            raise ValueError(f"Unknown Nordic bidding-area country: {code}")
        return names[code], [code]
    return "Denmark", ["DK"]


def collect_energinet(fetch):
    out = []
    base = "https://api.energidataservice.dk"
    for dataset, (datecol, keys, fields, category) in ENERGI_CONFIG.items():
        raw, meta_hash = fetch.get(base + "/meta/dataset/" + dataset)
        metadata = json.loads(raw)
        columnmeta = {c["dbColumn"]: c for c in metadata["columns"]}
        hashes, frames = [meta_hash], []
        boundaries = list(pd.date_range(START, END.tz_localize(None), freq="MS")) + [END.tz_localize(None)]
        if dataset != "PrivateConsumptionHeatingHour":
            boundaries = [pd.Timestamp(START), END.tz_localize(None)]
        for start, end in zip(boundaries[:-1], boundaries[1:]):
            raw, digest = fetch.get(base + "/dataset/" + dataset + "/download", {
                "format": "csv", "start": start.date().isoformat(), "end": end.date().isoformat(),
                "limit": 0}, timeout=240)
            data = pd.read_csv(BytesIO(raw), sep=";", decimal=",",
                               dtype={key: str for key in keys})
            hashes.append(digest)
            if data.empty:
                continue
            data = data[[datecol, *keys, *fields]].copy()
            for key in keys:
                data[key] = data[key].fillna("(source unspecified)").astype("category")
            data[datecol] = pd.to_datetime(data[datecol], utc=True)
            frames.append(data)
            print("FETCH Energinet", dataset, start.date(), len(data), flush=True)
        if not frames:
            continue
        data = pd.concat(frames, ignore_index=True)
        for grouping, part in (data.groupby(keys, observed=True, dropna=False) if keys else [((), data)]):
            grouping = grouping if isinstance(grouping, tuple) else (grouping,)
            labels = [str(x) for x in grouping]
            for field in fields:
                definition = columnmeta[field]
                if not definition.get("unit"):
                    raise ValueError("Missing explicit Energinet unit")
                frame = clean(part[datecol], part[field])
                if len(frame) < 2:
                    continue
                seconds = frame.date.diff().dt.total_seconds().median()
                frequency = {900: "15min", 3600: "hourly", 86400: "daily"}.get(seconds, "irregular_intraday")
                identity = dataset + "/" + ("/".join(labels) + "/" if labels else "") + field
                place, countries = energinet_geography(dataset, keys, labels, field)
                item = save("energinet_" + slug(identity), frame,
                            title=f"{place} {' / '.join(labels)}: {definition['displayName']}",
                            provider="energinet", category=category, unit=definition["unit"],
                            frequency=frequency, geography=place + (" / " + " / ".join(labels) if labels else ""),
                            source_url=base + "/dataset/" + dataset,
                            hypothesis="Household heating and housing-category electricity use can track consumer heating response, heat-pump adoption and cold-weather demand. Grid-area demand and local gas delivery can reveal industrial operating load. Reserve procurement and Nordic synchronous inertia measure grid stress, flexibility scarcity and renewable balancing requirements affecting power prices and utilities.",
                            limitations=str(metadata["description"]) + " " + str(metadata.get("caution", "")) + " "
                                + str(definition.get("description", "")) + " Missing and privacy-suppressed observations are omitted, never filled or reconstructed. Physical-unit aliases and currency-converted versions are not added. UTC timestamps are the source's explicit UTC fields. Component groups and national totals can overlap. Present historical data can be revised and are not a point-in-time release archive.",
                            raw_sha256=hashes, dataset_identity="energinet:" + identity,
                            documentation_url=base + "/meta/dataset/" + dataset,
                            source_column=field, source_dataset=dataset,
                            country_codes=countries,
                            source_group_fields=keys, source_group_values=labels,
                            expected_publication_lag="9–15 days" if "Consumption" in dataset else "Operational publication with dataset-specific revisions",
                            source_resolution=metadata.get("resolution"))
                if item:
                    out.append(item)
    return out


NZ_CONFIG = {
    "nz_generation": ("Generation/Generation_MD", "Generation_MD", ["Gen_Code"], "kWh", "power_plant_generation"),
    "nz_offtake": ("Metered_data/Grid_export", "Grid_export",
                   ["POC", "Nwk_Code", "Generation_Type", "Trader", "Flow_Direction"], "kWh", "grid_offtake"),
    "nz_reactive": ("Metered_data/Reactive_power", "Reactive_power",
                    ["POC", "Nwk_Code", "Generation_Type", "Trader", "Flow_Direction"], "kVArh", "reactive_power"),
}


def nz_trading_periods(data, keys):
    periods = [c for c in data if re.fullmatch(r"TP\d+", c)]
    long = data.melt(id_vars=[*keys, "Trading_Date"], value_vars=periods,
                     var_name="period", value_name="value").dropna(subset=["value"])
    days = pd.to_datetime(long.Trading_Date, errors="raise")
    start = days.dt.tz_localize("Pacific/Auckland").dt.tz_convert("UTC")
    next_start = (days + pd.Timedelta(days=1)).dt.tz_localize("Pacific/Auckland").dt.tz_convert("UTC")
    number = long.period.str[2:].astype(int)
    expected = (next_start - start).dt.total_seconds() / 1800
    if (number > expected).any():
        raise ValueError("Nonmissing NZ trading period outside DST calendar day")
    long["date"] = start + pd.to_timedelta((number - 1) * 30, unit="m")
    return long[[*keys, "date", "value"]]


def collect_nz(fetch, variant):
    folder, filename, keys, unit, category = NZ_CONFIG[variant]
    base = "https://www.emi.ea.govt.nz"
    index_url = base + "/Wholesale/Datasets/" + folder
    raw, index_hash = fetch.get(index_url)
    soup = BeautifulSoup(raw, "html.parser")
    urls = sorted({urljoin(base, a["href"]) for a in soup.find_all("a", href=True)
                   if re.search(r"/20\d{4}_" + filename + r"\.csv$", a["href"])
                   and int(a["href"].split("/")[-1][:6]) >= 202401})
    _, docs_hash = fetch.get(base + "/Content/About_datasets.pdf")
    hashes, frames = [index_hash, docs_hash], []
    for url in urls:
        raw, digest = fetch.get(url, timeout=120)
        data = pd.read_csv(BytesIO(raw))
        if variant != "nz_generation":
            if set(data.Unit_Measure.dropna()) != {unit}:
                raise ValueError("NZ source unit changed")
            # All observed source files are final metered data. Refuse a new
            # unrecognised status rather than double-count revisions.
            # Retain source-final ("F") records only. For example, March
            # 2026 contains seven "I" rows at LTN2201; these are omitted.
            data = data[data.Status.eq("F")].copy()
        long = nz_trading_periods(data, keys)
        # A plant can inject at multiple physical connection points. The
        # documented plant series is the sum of those nonoverlapping meters.
        if variant == "nz_generation":
            long = long.groupby([*keys, "date"], as_index=False).value.sum(min_count=1)
        for key in keys:
            long[key] = long[key].astype("category")
        frames.append(long)
        hashes.append(digest)
        print("FETCH", variant, url.split("/")[-1], len(long), flush=True)
    data = pd.concat(frames, ignore_index=True)
    out = []
    for grouping, part in data.groupby(keys, observed=True, dropna=False):
        grouping = grouping if isinstance(grouping, tuple) else (grouping,)
        labels = [str(x) for x in grouping]
        if variant == "nz_generation" and labels[0].casefold() == "waipori":
            # The archive uses Waipori for HWB0331 and waipori for BWK1101
            # / WPG0331 over the same dates. Do not silently merge these
            # case-distinct source groupings or overwrite their readable ID.
            omit("NZEA:Generation_MD:" + labels[0],
                 "Ambiguous case-distinct Waipori plant mapping across connection-point sets")
            continue
        metric = {"nz_generation": "plant_generation", "nz_offtake": "grid_offtake",
                  "nz_reactive": "reactive_energy"}[variant]
        item = save("nz_" + slug("_".join(labels)) + "_" + metric, clean(part.date, part.value),
                    title=f"New Zealand {' / '.join(labels)}: {metric.replace('_', ' ')}",
                    provider="New Zealand Electricity Authority", category=category,
                    unit=unit, frequency="30min", geography="New Zealand / " + labels[0],
                    source_url=index_url,
                    hypothesis="Plant production, local grid offtake and reactive-energy demand can reveal New Zealand industrial operating intensity, hydro/thermal supply changes, aluminum-smelter demand and local network stress. Reactive power responds to industrial motors and network conditions and is distinct from active energy use.",
                    limitations="Official monthly final metered histories. Grid exports mean offtake from the transmission grid, not exports from New Zealand; they are not exact end-customer consumption because of distribution losses, embedded production and multiple downstream users. Reactive energy is signed kVArh, not active kWh. Plant generation is the authority's estimate obtained by mapping/apportioning metered injections to plants; its constituent connection points are summed as the authority documents. Native half-hour periods retain 46/48/50-period New Zealand DST days. Source date plus period number is converted from Pacific/Auckland to UTC. No absent periods are filled. Publication generally occurs in the following month; source dates are not historical availability.",
                    raw_sha256=hashes, dataset_identity="NZEA:" + filename + ":" + ":".join(labels),
                    documentation_url=base + "/Content/About_datasets.pdf",
                    expected_publication_lag="Following month; subsequent corrections possible",
                    country_codes=["NZ"], source_group_fields=keys, source_group_values=labels,
                    timestamp_semantics="New Zealand local trading date and half-hour period; UTC interval start",
                    value_kind="source_apportioned_generation_estimate" if variant == "nz_generation" else "metered_energy",
                    source_urls=urls)
        if item:
            out.append(item)
    return out


SETTLEMENT_RANK = {"II": 0, "SF": 1, "R1": 2, "R2": 3, "R3": 4, "RF": 5, "DF": 6}


def parse_elexon_sample(raw):
    data = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(json.loads(raw))
    if data.empty:
        return data
    if not data.settlementPeriod.eq(25).all():
        raise ValueError("Elexon did not apply the settlement-period filter")
    dates = pd.to_datetime(data.settlementDate)
    expected_end = (dates.dt.tz_localize("Europe/London").dt.tz_convert("UTC")
                    + pd.Timedelta(minutes=25 * 30))
    supplied_end = pd.to_datetime(data.halfHourEndTime, utc=True, format="mixed")
    if not supplied_end.eq(expected_end).all():
        raise ValueError("Elexon end timestamp conflicts with UK settlement date/period")
    unknown = set(data.settlementRunType.dropna()) - set(SETTLEMENT_RANK)
    if unknown or data.settlementRunType.isna().any():
        raise ValueError(f"Unknown Elexon settlement sequence: {unknown}")
    data["date"] = supplied_end
    data["value"] = pd.to_numeric(data.quantity, errors="raise")
    data["rank"] = data.settlementRunType.map(SETTLEMENT_RANK)
    latest = data.groupby(["bmUnit", "date"])["rank"].transform("max")
    data = data[data["rank"].eq(latest)].copy()
    if data.groupby(["bmUnit", "date"]).value.nunique().max() > 1:
        raise ValueError("Conflicting Elexon values in the same settlement revision")
    return data.drop_duplicates(["bmUnit", "date"])


def select_elexon_units(data, reference, limit=1000):
    """A stable balanced panel, not thousands of similar supplier portfolios."""
    from collections import defaultdict, deque
    path = ROOT / "archive/energy4_selection/elexon.json"
    eligible, buckets = {}, defaultdict(list)
    for bm, part in data.groupby("bmUnit", observed=True, sort=True):
        nonzero = part.loc[part.value.abs() > 0, "date"].dt.floor("D").nunique()
        if nonzero < 45 or part.value.nunique() < 2:
            continue
        frame = clean(part.date, part.value)
        if not qualifies_research(dict(coverage(frame), frequency="daily")):
            continue
        bm = str(bm)
        eligible[bm] = frame
        ref = reference.get(bm, {})
        positive = int(part.value.gt(0).sum())
        negative = int(part.value.lt(0).sum())
        pattern = ("bidirectional" if positive >= 30 and negative >= 30 else
                   "consumption" if negative > positive else "generation")
        bucket = tuple(str(ref.get(k) or "unavailable") for k in
                       ["bmUnitType", "fuelType", "gspGroupId"]) + (pattern,)
        recent = part[part.date >= END - pd.Timedelta(days=100)]
        rank = (-int(recent.value.ne(0).sum()), -len(frame), -int(nonzero),
                hashlib.sha256(bm.encode()).hexdigest())
        buckets[bucket].append((rank, bm))
    if path.exists():
        selected = json.loads(path.read_text())["selected_bm_units"]
    else:
        queues = {key: deque(bm for rank, bm in sorted(values)) for key, values in sorted(buckets.items())}
        selected = []
        while len(selected) < limit and any(queues.values()):
            for queue in queues.values():
                if queue and len(selected) < limit:
                    selected.append(queue.popleft())
        atomic_bytes(path, json.dumps({
            "selected_bm_units": selected,
            "selection_rule": "Round-robin across source BM-unit type, fuel type, GSP group and observed generation/consumption/bidirectional sign pattern. Within buckets prefer recent activity, history length, active days, then stable SHA-256 tie break. At most 1,000 units; identities persist on refresh.",
            "eligible_units": len(eligible), "strata": len(queues),
        }, indent=2).encode())
    return [(bm, eligible[bm]) for bm in selected if bm in eligible]


def collect_elexon(fetch):
    base = "https://data.elexon.co.uk/bmrs/api/v1"
    raw, ref_hash = fetch.get(base + "/reference/bmunits/all", timeout=120)
    reference = {r["elexonBmUnit"]: r for r in json.loads(raw)}
    _, spec_hash = fetch.get("https://data.elexon.co.uk/swagger/v1/swagger.json", timeout=120)
    _, guide_hash = fetch.get("https://www.elexon.co.uk/bsc/article/bsc-insight-how-to-get-more-detailed-metered-volume-data-from-our-insights-solution/")
    hashes, frames = [], []
    # The stream API's period bounds describe a continuous range, not a
    # recurring period-of-day filter. The scalar endpoint is unambiguous.
    days = pd.date_range(START, (END - pd.Timedelta(days=10)).tz_localize(None), freq="D")
    for day in days:
        raw, digest = fetch.get(base + "/datasets/B1610", {
            "settlementDate": day.date().isoformat(), "settlementPeriod": 25,
            "format": "csv"}, timeout=120)
        parsed = pd.read_csv(BytesIO(raw))
        parsed.columns = [c[0].lower() + c[1:] for c in parsed.columns]
        data = parse_elexon_sample(parsed)
        hashes.append(digest)
        if len(data):
            part = data[["bmUnit", "date", "value", "rank"]].copy()
            part.bmUnit = part.bmUnit.astype("category")
            frames.append(part)
        print("FETCH Elexon SP25", day.date(), len(data), flush=True)
        if not getattr(fetch, "cache_only", False):
            time.sleep(0.25)
    data = pd.concat(frames, ignore_index=True)
    data = data[data.date < END]
    # The interval endpoints can be inclusive at both adjacent requests.
    rank = data.groupby(["bmUnit", "date"], observed=True)["rank"].transform("max")
    data = data[data["rank"].eq(rank)]
    manifest = archive_raw_manifest(hashes, description="Elexon B1610 daily source CSV responses, settlement period 25")
    out = []
    for bm, frame in select_elexon_units(data, reference):
        ref = reference.get(str(bm), {})
        name = ref.get("bmUnitName") or str(bm)
        group = ref.get("gspGroupName") or "Great Britain"
        item = save("elexon_" + slug(bm) + "_sp25_metered_energy", frame,
                    title=f"Great Britain {name}: SP25 metered energy (daily half-hour sample)",
                    provider="Elexon Insights", category="uk_metered_energy",
                    unit="MWh in the selected half-hour settlement period",
                    frequency="daily", native_frequency="30min", finer_frequency_available=True,
                    geography="United Kingdom / " + group, country_codes=["GB"],
                    source_url=base + "/datasets/B1610",
                    hypothesis="Metered supplier-consumption portfolios, generating units, batteries and industrial balancing units can reveal electricity-use intensity, utility customer-load shifts, local industrial demand, storage behavior and plant dispatch. A consistent midday settlement sample gives a daily operational indicator without confusing it with total daily energy.",
                    limitations="One genuine half-hour energy observation per UK settlement day, always settlement period 25; this is a daily sample, not a daily total, daily mean or instantaneous MW. Signed MWh includes consumption (negative), generation (positive), or zero and may include estimated meter readings in settlement. The source report name mentions generation but the endpoint covers all BM units including supplier demand portfolios and batteries. A BM unit can aggregate multiple sites; current registration metadata is not a historical company-ownership mapping. Period 25 is usually 12:00–12:30 local, but its clock time changes on the 46/50-period DST transition dates. UTC endpoint is checked against Europe/London midnight plus 25 half-hours. Initial observations arrive after about five working days and subsequent settlement runs revise them for months; the latest available sequence II, SF, R1, R2, R3, RF, DF is selected. This is not point-in-time availability. Missing dates and source nulls remain absent.",
                    raw_sha256=[ref_hash, spec_hash, guide_hash], raw_manifest_sha256=[manifest],
                    dataset_identity=f"ELEXON:B1610:{bm}:SP25",
                    documentation_url="https://www.elexon.co.uk/bsc/article/bsc-insight-how-to-get-more-detailed-metered-volume-data-from-our-insights-solution/",
                    source_bm_unit=str(bm), source_entity_name=name,
                    source_lead_party=ref.get("leadPartyName"), source_fuel_type=ref.get("fuelType"),
                    source_bm_unit_type=ref.get("bmUnitType"), source_gsp_group=ref.get("gspGroupId"),
                    expected_publication_lag="Approximately five working days initially; settlement reconciliation continues for months",
                    sampling_method="Exactly settlement period 25 each local settlement day; other native half-hours not downloaded",
                    selection_rule="Stable maximum-1000 panel balanced across source BM-unit type, fuel type, GSP group and observed metering direction; selection manifest in archive/energy4_selection/elexon.json",
                    timestamp_semantics="UTC end of the selected half-hour interval",
                    license="BMRS data © Elexon Limited copyright and database right")
        if item:
            out.append(item)
    return out


JAPAN_GRIDS = {
    "tepco": ("Tokyo", "TEPCO Power Grid",
              "https://www4.tepco.co.jp/forecast/html/area_jukyu-j.html"),
    "hokkaido": ("Hokkaido", "Hokkaido Electric Power Network",
                 "https://www.hepco.co.jp/network/con_service/public_document/supply_demand_results/index.html"),
}
JAPAN_FIELDS = {
    "エリア需要": ("demand", "Area electricity demand"),
    "原子力": ("nuclear", "Nuclear generation"),
    "火力(LNG)": ("lng", "LNG-fired generation"),
    "火力(石炭)": ("coal", "Coal-fired generation"),
    "火力(石油)": ("oil", "Oil-fired generation"),
    "火力(その他)": ("other_thermal", "Other thermal generation"),
    "火力出力制御量": ("thermal_curtailment", "Thermal output curtailed"),
    "水力": ("hydro", "Hydropower excluding pumped storage"),
    "地熱": ("geothermal", "Geothermal generation"),
    "バイオマス": ("biomass", "Dedicated biomass generation"),
    "バイオマス出力制御量": ("biomass_curtailment", "Biomass output curtailed"),
    "太陽光発電実績": ("solar", "Reported solar generation"),
    "太陽光出力制御量": ("solar_curtailment", "Solar output curtailed"),
    "風力発電実績": ("wind", "Reported wind generation"),
    "風力出力制御量": ("wind_curtailment", "Wind output curtailed"),
    "揚水": ("pumped_storage_net", "Net pumped-storage power"),
    "蓄電池": ("battery_net", "Net battery power"),
    "連系線": ("intertie_net", "Net intertie power"),
    "その他": ("other_generation", "Other unclassified generation"),
}


def parse_japan_grid(raw):
    try:
        text = raw.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        text = raw.decode("cp932")
        encoding = "cp932"
    if "MW平均" not in text.splitlines()[0]:
        raise ValueError("Japanese grid source does not declare average MW units")
    data = pd.read_csv(BytesIO(raw), encoding=encoding, skiprows=1)
    if not {"DATE", "TIME", "エリア需要"}.issubset(data):
        raise ValueError("Japanese grid source schema changed")
    data["date"] = pd.to_datetime(data.DATE + " " + data.TIME, errors="raise").dt.tz_localize(
        "Asia/Tokyo").dt.tz_convert("UTC")
    for field in JAPAN_FIELDS:
        if field in data:
            data[field] = pd.to_numeric(data[field], errors="raise")
    return data


def collect_japan_grid(fetch, grid):
    region, provider, index = JAPAN_GRIDS[grid]
    raw, index_hash = fetch.get(index)
    urls = {}
    for link in BeautifulSoup(raw, "html.parser").find_all("a", href=True):
        url = urljoin(index, link["href"])
        match = re.search(r"/eria_jukyu_(20\d{2})(\d{2})_\d+\.csv$", url)
        if match:
            month = pd.Timestamp(f"{match[1]}-{match[2]}-01")
            if pd.Timestamp(START) <= month <= END.tz_localize(None):
                urls[url] = month
    if not urls:
        raise ValueError("No monthly Japanese grid archives discovered")
    frames, hashes = [], [index_hash]
    for url in sorted(urls, key=urls.get):
        raw, digest = fetch.get(url)
        frames.append(parse_japan_grid(raw))
        hashes.append(digest)
        print("FETCH", grid, urls[url].date(), len(frames[-1]), flush=True)
    data = pd.concat(frames, ignore_index=True)
    data = data[(data.date >= pd.Timestamp(START, tz="UTC")) & (data.date < END)]
    out = []
    for field, (metric, title) in JAPAN_FIELDS.items():
        if field not in data:
            continue
        frame = clean(data.date, data[field])
        item = save(f"japan_{grid}_{metric}", frame, title=f"Japan {region}: {title}",
                    provider=provider, category="grid_supply_demand", unit="MW interval average",
                    frequency="30min", native_frequency="30min", country_codes=["JP"],
                    source_url=index, documentation_url=index, geography=f"Japan / {region} grid area",
                    source_column=field, source_urls=list(urls), source_dataset="area_supply_demand",
                    dataset_identity=f"JapanGrid:{grid}:{metric}", raw_sha256=hashes,
                    expected_publication_lag=("Within one hour of the actual interval; subsequent revisions possible" if grid == "tepco" else
                                              "Operational half-hour observations in monthly files; the index does not guarantee a publication lag"),
                    timestamp_semantics="Start of source 30-minute interval in Japan Standard Time, converted to UTC",
                    hypothesis="Regional industrial and household power use, LNG/coal dispatch, renewable curtailment, storage dispatch and interregional balancing can help research Japanese fuel demand, power scarcity, utility margins and industrial production changes.",
                    limitations="Source CSV explicitly states interval-average MW. Reported generation includes estimates; historical files may be revised without a release archive. Area demand is calculated from grid-side generation and exchanges, and components overlap the area total. The redundant supply total is excluded. Pumped storage is positive while generating and negative while pumping; batteries are positive while discharging and negative while charging; interties are positive for regional inflows and negative for outflows. No missing intervals are filled. Constant histories are excluded. Observation timestamps are not point-in-time publication timestamps.",
                    value_kind="reported_grid_value_including_source_estimates")
        if item:
            out.append(item)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", action="append", choices=["entsog", "energinet", "elexon", *ONS_CONFIG, *NZ_CONFIG, *JAPAN_GRIDS])
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    providers = args.provider or ["entsog", *ONS_CONFIG, "energinet", *NZ_CONFIG, "elexon", *JAPAN_GRIDS]
    fetch = Fetcher(refresh=args.refresh)
    results = []
    # Provider families run independently; requests within each are serial.
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = {}
        for p in providers:
            if p == "entsog":
                job = pool.submit(collect_entsog, fetch)
            elif p == "energinet":
                job = pool.submit(collect_energinet, fetch)
            elif p == "elexon":
                job = pool.submit(collect_elexon, fetch)
            elif p in NZ_CONFIG:
                job = pool.submit(collect_nz, fetch, p)
            elif p in JAPAN_GRIDS:
                job = pool.submit(collect_japan_grid, fetch, p)
            else:
                job = pool.submit(collect_ons, fetch, p)
            jobs[job] = p
        for future in as_completed(jobs):
            provider = jobs[future]
            try:
                rows = future.result()
                results.extend(rows)
                print("DONE", provider, len(rows), flush=True)
            except Exception as exc:
                ERRORS.append({"provider": provider, "error": repr(exc)})
                print("ERROR", provider, repr(exc), flush=True)
    report = {"group": GROUP, "providers": providers, "series": len(results),
              "results": [r["series_id"] for r in results], "errors": ERRORS, "omissions": OMISSIONS}
    atomic_bytes(ROOT / "logs" / ("energy4_" + "_".join(providers) + ".json"),
                 json.dumps(report, indent=2).encode())
    return int(bool(ERRORS))


if __name__ == "__main__":
    raise SystemExit(main())
