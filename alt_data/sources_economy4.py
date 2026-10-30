#!/usr/bin/env python3
"""Foreign agricultural commerce and public financial-system activity."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import threading
import time

import numpy as np
import pandas as pd

from expansion_common import ROOT, Fetcher, archive_raw_manifest, atomic_bytes, coverage, normalize_frame, qualifies_research, save_candidate

GROUP = "economy4"
START = "2024-01-01"
SEEN = set()
SEEN_LOCK = threading.Lock()
OMISSIONS = []
US_EXPOSURE_CODES = {"US", "AS", "GU", "MP", "PR", "UM", "VI"}
# ISO 3166-1 alpha-2; non-country customs pseudo-codes remain only in source fields.
ISO_COUNTRIES = set("AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW".split())


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def clean(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                          "value": pd.to_numeric(values, errors="raise")}).dropna()
    if frame.empty:
        return frame
    if (frame.groupby("date").value.nunique() > 1).any():
        raise ValueError("Conflicting duplicate source observations")
    return normalize_frame(frame.drop_duplicates("date"))


def save(sid, frame, **meta):
    if frame.empty or frame.value.nunique() < 2:
        return None
    if not qualifies_research(dict(coverage(frame), frequency=meta["frequency"])):
        return None
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes() +
                            frame.value.to_numpy().tobytes()).hexdigest()
    with SEEN_LOCK:
        if digest in SEEN:
            return None
        SEEN.add(digest)
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2")
    try:
        result = save_candidate(GROUP, sid, frame, **meta)
    except Exception:
        with SEEN_LOCK:
            SEEN.remove(digest)
        raise
    print("OK", sid, len(frame), flush=True)
    return result


def roc_date(value):
    """The source's Republic of China calendar label denotes a local trade day."""
    value = str(value)
    year, month, day = (map(int, value.split(".")) if "." in value
                        else (int(value[:-4]), int(value[-4:-2]), int(value[-2:])))
    return pd.Timestamp(year=year + 1911, month=month, day=day, tz="UTC")


def roc_label(value):
    value = pd.Timestamp(value)
    return f"{value.year - 1911}.{value.month:02d}.{value.day:02d}"


def moa_unit(kind, metric):
    """Official auction units distinguish flower bunches from produce mass."""
    if kind not in {"N04", "N05", "N06", "aquatic"}:
        raise ValueError("Unknown MOA product category; trade unit is ambiguous")
    denominator = "bunch" if kind == "N06" else "kilogram"
    return (f"TWD per {denominator}" if metric == "average_price"
            else "bunches" if kind == "N06" else "kilograms")


def parse_moa(raw, digest, aquatic=False):
    records = json.loads(raw)
    if not isinstance(records, list) or (records and "errMsg" in records[0]):
        raise ValueError(f"MOA error payload: {str(records)[:300]}")
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    if aquatic:
        frame = frame.rename(columns={"品種代碼": "作物代號", "魚貨名稱": "作物名稱"})
        frame["市場代號"] = frame["市場名稱"].map(lambda s: hashlib.sha256(s.encode()).hexdigest()[:10])
        frame["種類代碼"] = "aquatic"
    required = {"交易日期", "種類代碼", "作物代號", "作物名稱", "市場代號", "市場名稱", "平均價", "交易量"}
    if not required <= set(frame):
        raise ValueError("MOA agricultural fields changed")
    frame["date"] = frame["交易日期"].map(roc_date)
    frame["_raw_sha256"] = digest
    for col in ["市場代號", "作物代號", "種類代碼"]:
        frame[col] = frame[col].astype(str)
    return frame


def taiwan_agriculture(fetch, aquatic=False):
    endpoint = "AquaticTransData" if aquatic else "FarmTransData"
    url = f"https://data.moa.gov.tw/Service/OpenData/FromM/{endpoint}.aspx"
    end = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).normalize()
    months = pd.date_range(pd.Timestamp(START, tz="UTC"), end, freq="MS")

    def month_data(begin):
        finish = min(begin + pd.offsets.MonthEnd(0), end)
        pieces = []
        seen_pages = set()
        for skip in range(0, 1000000, 10000):
            params = {"StartDate": roc_label(begin), "EndDate": roc_label(finish),
                      "$top": 10000, "$skip": skip}
            if aquatic:
                params = {k: (v.replace(".", "") if k.endswith("Date") else v) for k, v in params.items()}
            raw, digest = fetch.get(url, params, timeout=180, max_age_hours=24 * 30)
            frame = parse_moa(raw, digest, aquatic=aquatic)
            if frame.empty:
                break
            if digest in seen_pages:
                raise ValueError("MOA pagination repeated a page")
            seen_pages.add(digest)
            if frame.date.min() < begin or frame.date.max() > finish:
                raise ValueError("MOA ignored requested date bounds")
            pieces.append(frame)
            print("MOA PAGE", endpoint, begin.date(), skip, len(frame), flush=True)
            if len(frame) < 10000:
                break
            time.sleep(.5)
        else:
            raise ValueError("MOA pagination ceiling")
        return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()

    pieces = []
    # Two independent month requests at a time, with a pause between pages.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(month_data, month): month for month in months}
        for future in as_completed(futures):
            pieces.append(future.result())
    data = pd.concat(pieces, ignore_index=True)
    doc_urls = (["https://data.moa.gov.tw/open_detail.aspx?id=039"] if aquatic else
                ["https://data.moa.gov.tw/open_detail.aspx?id=037",
                 "https://ebook.afa.gov.tw/afa/108/files/basic-html/page101.html"])
    doc_hashes = [fetch.get(doc, max_age_hours=720)[1] for doc in doc_urls]
    manifest = archive_raw_manifest(data["_raw_sha256"].unique().tolist(),
                                   description=f"Complete paginated {endpoint} daily source panel from 2024")
    result = []
    for (kind, market, crop), part in data.groupby(["種類代碼", "市場代號", "作物代號"], sort=True):
        if kind not in ({"aquatic"} if aquatic else {"N04", "N05", "N06"}):
            OMISSIONS.append({"source": endpoint, "market": market, "product": crop,
                              "source_category": kind,
                              "reason": "Unknown source product category; unit cannot be established; entire category/market/product history excluded"})
            continue
        newest = part.sort_values("date").iloc[-1]
        for column, metric in [("平均價", "average_price"), ("交易量", "traded_volume")]:
            unit = moa_unit(kind, metric)
            frame = clean(part.date, part[column])
            if (frame.value < 0).any():
                print("QUALITY OMIT negative source level", endpoint, kind, market, crop, metric, flush=True)
                OMISSIONS.append({"source": endpoint, "market": market, "product": crop,
                                  "metric": metric, "reason": "negative source level; entire series excluded"})
                continue
            item = save(
                f"taiwan_moa_{slug(kind)}_{slug(market)}_{slug(crop)}_{metric}", frame,
                title=f"Taiwan {newest['市場名稱']}: {newest['作物名稱']} wholesale {metric.replace('_', ' ')}",
                provider="Taiwan Fisheries Agency wholesale markets" if aquatic else "Taiwan Ministry of Agriculture wholesale markets",
                category=("seafood_" if aquatic else "flower_" if kind == "N06" else "agricultural_") + ("wholesale_prices" if metric == "average_price" else "traded_volume"),
                unit=unit, frequency="daily", native_frequency="daily", source_url=url,
                geography=f"Taiwan — {newest['市場名稱']}",
                geography_scope="non_us", country_codes=["TW"],
                hypothesis="Local wholesale food and flower supply, demand, storm disruption and input-cost pressure may precede consumer inflation, retailer margins, agricultural imports and food-sector revenue changes.",
                limitations="Actual market/product daily auction reports, with market holidays and missing reports omitted. Source average prices and traded quantities only, not an invented full panel or repeated high/middle/low bands. Produce and fish quantities are kilograms; flower category N06 uses auction bunches (whose composition varies by product). Unknown product categories are excluded because their trade unit is ambiguous. Seasonal products may have gaps. Historical corrections are possible; observation dates are not first publication times. Chinese source names are preserved because the source has no official English crop-name mapping.",
                raw_sha256=doc_hashes, raw_manifest_sha256=[manifest],
                dataset_identity=f"TW_MOA:{endpoint}:{kind}:{market}:{crop}:{column}",
                documentation_url="https://data.gov.tw/dataset/7299" if aquatic else "https://data.gov.tw/en/datasets/8066",
                unit_documentation_url="https://efish.fa.gov.tw/efish/" if aquatic else "https://ebook.afa.gov.tw/afa/108/files/basic-html/page101.html",
                timestamp_semantics="Taiwan local transaction-day label represented at UTC midnight, not a UTC execution timestamp",
                source_market_code=market, source_crop_code=crop, source_category_code=kind,
                source_market_name=str(newest["市場名稱"]), source_crop_name=str(newest["作物名稱"]),
                expected_publication_lag="Source refreshes several times daily; individual market reporting gaps and revisions occur",
                license="Taiwan Open Government Data License 1.0",
            )
            if item:
                result.append(item)
    return result


EU_PRICE_PATHS = {
    "beef": "beef/prices", "beef_cuts": "beef/cuts/prices",
    "live_cattle": "liveAnimal/prices", "cereals": "cereal/prices",
    "dairy": "dairy/prices", "fruit_vegetables": "fruitAndVegetable/pricesSupplyChain",
    "olive_oil": "oliveOil/prices", "oilseeds": "oilseeds/prices",
    "pigmeat": "pigmeat/prices", "pork_cuts": "pigmeat/cuts/prices",
    "poultry": "poultry/prices", "eggs": "poultry/egg/prices",
    "rice": "rice/prices", "sheep_goats": "sheepAndGoat/prices",
    "wine": "wine/prices",
}


def parse_eu_price(raw):
    data = pd.DataFrame(json.loads(raw))
    if data.empty:
        return data
    if not {"beginDate", "endDate", "price", "memberStateCode", "unit"} <= set(data):
        raise ValueError("EU price fields changed")
    if "periodType" in data:
        data = data[data.periodType == "Week"].copy()
    data = data.dropna(subset=["price"]).copy()
    data["date"] = pd.to_datetime(data.endDate, format="%d/%m/%Y", utc=True)
    begin = pd.to_datetime(data.beginDate, format="%d/%m/%Y", utc=True)
    if ((data.date - begin).dt.days > 8).any():
        raise ValueError("Non-weekly price period in weekly endpoint")
    if not data.price.astype(str).str.startswith("€").all():
        raise ValueError("Unexpected source price currency")
    data["value"] = pd.to_numeric(data.price.str.removeprefix("€").str.replace(",", "", regex=False))
    return data


def eu_prices(fetch):
    base = "https://api.tech.ec.europa.eu/agrifood/api/"
    finish = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).strftime("%d/%m/%Y")
    result = []
    for family, path in EU_PRICE_PATHS.items():
        url = base + path
        params = {"beginDate": "01/01/2024", "endDate": finish}
        raw, digest = fetch.get(url, params, timeout=240, max_age_hours=24 * 30)
        data = parse_eu_price(raw)
        if data.empty:
            continue
        excluded = {"beginDate", "endDate", "price", "date", "value", "weekNumber",
                    "week", "period", "year", "periodType", "marketingYear",
                    "referencePeriod", "isCalculated", "isRegulated", "memberStateName"}
        dimensions = sorted(set(data) - excluded)
        for keys, part in data.groupby(dimensions, dropna=False, sort=True):
            info = {k: (None if pd.isna(v) else str(v)) for k, v in zip(dimensions, keys)}
            identity = json.dumps(info, sort_keys=True, ensure_ascii=False)
            sid_hash = hashlib.sha256(identity.encode()).hexdigest()[:12]
            latest = part.sort_values("date").iloc[-1]
            frame = clean(part.date, part.value)
            labels = [str(latest[d]) for d in dimensions if d not in {"unit", "memberStateCode"}]
            label = "; ".join(labels)
            code = str(latest.memberStateCode)
            country_codes = [{"EL": "GR", "UK": "GB"}.get(code, code)] if len(code) == 2 and code != "EU" else []
            item = save(
                f"eu_agri_{family}_{slug(code)}_{sid_hash}", frame,
                title=f"{latest.memberStateName}: {family.replace('_', ' ')} price — {label}",
                provider="European Commission agricultural market prices",
                category="european_" + family + "_prices",
                unit=("EUR per tonne" if latest.unit == "TONNES" else str(latest.unit)),
                source_unit_label=str(latest.unit), frequency="weekly", native_frequency="weekly",
                source_url=url, source_parameters=params,
                geography=str(latest.memberStateName), geography_scope="non_us", country_codes=country_codes,
                hypothesis="Weekly farmgate, wholesale, processor and retail input-price shifts can lead food inflation, consumer purchasing-power pressure and margins for food manufacturers, grocers and restaurants.",
                limitations="Member-state price notifications for explicit product, quality, market and supply-chain stage; no daily upsampling. Seasonal gaps remain. Product classes and regional prices may be correlated; these are not independent trading signals. Revisions and reporting delays mean period-end labels are not original availability times.",
                raw_sha256=digest, dataset_identity=f"EU_AGRI:{path}:{identity}",
                source_dimensions=info, source_currency="EUR",
                documentation_url="https://agridata.ec.europa.eu/extensions/dataportal/prices.html",
                timestamp_semantics="End of the source weekly reference period, represented at UTC midnight",
                expected_publication_lag="Weekly source reporting; country-specific lags and historical revisions",
            )
            if item:
                result.append(item)
        print("EU FAMILY", family, len(result), flush=True)
        time.sleep(1)
    return result


CO_DATASETS = {
    "4v6r-wu98": "Cybercrime", "csb4-y6v2": "Vehicle theft",
    "kvjj-d2ay": "Land invasion and usurpation", "i7h7-wmjc": "Financial institution robbery",
    "4rxi-8m8d": "Personal theft", "q2ib-t9am": "Extortion",
    "9zck-qfvc": "Environmental offenses", "ntej-qq7v": "Traffic accident injuries",
    "sutf-7dyz": "Road cargo piracy", "7mn7-vzqp": "Residential theft",
    "uav5-b85g": "Traffic accident deaths", "7i2x-h5vp": "Commercial premises theft",
    "p88b-5ac7": "Livestock theft", "ec2r-4byk": "Pipeline attacks",
    "m98b-cdys": "Bridge and road attacks", "fr5q-rz2x": "Disaster risk management actions",
}


def observed_weekly(frame, source_start, source_end):
    """Sum reported event quantities; absent weeks remain absent, boundary weeks omitted."""
    frame = frame.copy()
    frame["date"] = frame.date + pd.to_timedelta(6 - frame.date.dt.weekday, unit="D")
    frame = frame.groupby("date", as_index=False).value.sum(min_count=1)
    frame = frame[(frame.date - pd.Timedelta(days=6) >= source_start) &
                  (frame.date <= source_end)]
    return normalize_frame(frame) if not frame.empty else frame


def colombia_security(fetch):
    result = []
    for dataset, title in CO_DATASETS.items():
        doc_url = f"https://www.datos.gov.co/api/views/{dataset}.json"
        raw, doc_hash = fetch.get(doc_url, max_age_hours=720)
        info = json.loads(raw)
        columns = {x["fieldName"]: x for x in info["columns"]}
        if not {"fecha_hecho", "cod_muni", "cod_depto", "cantidad"} <= set(columns):
            raise ValueError("Colombia reporting schema changed")
        url = f"https://www.datos.gov.co/resource/{dataset}.json"
        group = "fecha_hecho,cod_depto,departamento,cod_muni,municipio"
        parts, hashes = [], [doc_hash]
        for offset in range(0, 2000000, 50000):
            params = {"$select": group + ",sum(cantidad) as value",
                      "$group": group, "$where": "fecha_hecho >= '2024-01-01T00:00:00'",
                      "$order": group, "$limit": 50000, "$offset": offset}
            raw, digest = fetch.get(url, params, timeout=180, max_age_hours=720)
            rows = json.loads(raw)
            if not isinstance(rows, list):
                raise ValueError("Colombia API error response")
            hashes.append(digest)
            parts.extend(rows)
            if len(rows) < 50000:
                break
            time.sleep(.5)
        else:
            raise ValueError("Colombia pagination limit")
        if not parts:
            continue
        data = pd.DataFrame(parts)
        data["date"] = pd.to_datetime(data.fecha_hecho, utc=True)
        data["value"] = pd.to_numeric(data.value, errors="raise")
        source_start, source_end = data.date.min(), data.date.max()
        for level, code_column, name_column in [("municipality", "cod_muni", "municipio"),
                                                ("department", "cod_depto", "departamento")]:
            for code, part in data.groupby(code_column):
                name = str(part.sort_values("date").iloc[-1][name_column])
                frame = part.groupby("date", as_index=False).value.sum(min_count=1)
                frame = observed_weekly(frame, source_start, source_end)
                item = save(
                    f"colombia_{slug(dataset)}_{level}_{slug(code)}", frame,
                    title=f"Colombia {name}: weekly reported {title.lower()}",
                    provider="Colombia Ministry of Defence security observatory",
                    category="economic_security_and_disruption", unit="source-reported count",
                    frequency="weekly", native_frequency="daily", finer_frequency_available=True,
                    source_release_frequency="monthly",
                    source_url=url, geography=f"Colombia — {name}", geography_scope="non_us",
                    country_codes=["CO"], dataset_identity=f"CO_MDN:{dataset}:{level}:{code}:weekly_sum",
                    hypothesis="Reported theft, extortion, cybercrime, transport harm and infrastructure disruption may help explain local consumption, security expenditure, insurance losses, freight risk and commodity-supply conditions.",
                    limitations="Weekly sums of the source's daily reported quantities. Depending on offense, the source counts victims, cases, vehicles or actions; these are not a uniform count of criminal incidents. Only weeks containing actual reports are stored; absent weeks are not filled with zeros. First and last incomplete national reference weeks are omitted. Monthly publication and retrospective corrections prevent treating incident dates as real-time signal availability. Geographic and legal reporting changes affect comparability.",
                    raw_sha256=hashes, documentation_url=f"https://www.datos.gov.co/d/{dataset}",
                    source_description=info.get("description", ""),
                    source_dataset_name=info["name"], geographic_level=level,
                    timestamp_semantics="Sunday ending the weekly period of reported local incident dates; not release date",
                    expected_publication_lag="Monthly reporting, often several weeks after the incident period; source revisions possible",
                    aggregation="Sum of official daily quantities by administrative area; positive/report-present weeks only",
                )
                if item:
                    result.append(item)
        print("COLOMBIA FAMILY", dataset, len(result), flush=True)
    return result


TAXUD_SECTORS = [
    "Coffee And Tea", "Cocoa And Chocolate", "Fish", "Fertilisers", "Machinery",
    "Beer", "Non-Alcoholic Drinks", "Beef", "Milk And Milk Products", "Eggs",
    "Honey", "Animal Feed And By-Products", "Cotton", "Flax",
]


def taxud_week_date(year, week):
    """Monday/Sunday reporting weeks; ambiguous calendar-boundary weeks are omitted."""
    if not re.fullmatch(r"20\d{2}", str(year)):
        return pd.NaT
    year, week = int(year), int(week)
    # These calendar years all use the documented ISO-aligned middle weeks.
    # Split marketing years and calendar-boundary weeks are deliberately excluded.
    if year not in {2024, 2025, 2026} or not 2 <= week <= 52:
        return pd.NaT
    return pd.Timestamp.fromisocalendar(year, week, 7).tz_localize("UTC")


def parse_taxud(raw, *, flag_ambiguous=False):
    records = json.loads(raw)
    if not isinstance(records, list):
        raise ValueError("Unexpected TAXUD payload")
    data = pd.DataFrame(records)
    if data.empty:
        return data
    required = {"sector", "marketingYear", "week", "memberStateCode", "partnerCode",
                "product", "cn8ProductCode", "taric10ProductCode", "kg", "euroValue"}
    if not required <= set(data):
        raise ValueError("TAXUD schema changed")
    data["date"] = [taxud_week_date(y, w) for y, w in zip(data.marketingYear, data.week)]
    data = data.dropna(subset=["date"]).copy()
    data = data[data.date <= pd.Timestamp.now(tz="UTC").normalize()]
    for field in ["kg", "euroValue"]:
        data[field] = pd.to_numeric(data[field], errors="raise")
    dimensions = [c for c in ["sector", "marketingYear", "week", "memberStateCode",
                              "partnerCode", "product", "cn8ProductCode",
                              "taric10ProductCode", "procedure", "preference"] if c in data]
    conflicting = (data.groupby(dimensions, dropna=False)[["kg", "euroValue"]].transform("nunique") > 1).any(axis=1)
    if conflicting.any() and not flag_ambiguous:
        raise ValueError("Conflicting duplicate TAXUD source records")
    data["_ambiguous_source_cell"] = conflicting
    return data.drop_duplicates(dimensions)


def trade_geography(member, partner):
    codes = [{"EL": "GR", "UK": "GB"}.get(str(c), str(c)) for c in [member, partner]]
    codes = [c for c in codes if c in ISO_COUNTRIES]
    return ("mixed" if US_EXPOSURE_CODES.intersection(codes) or len(codes) != 2 else "non_us"), sorted(set(codes))


def brazil_payments(fetch):
    result = []
    # Intraday endpoints expose rolling average profiles rather than historical
    # timestamps; they are intentionally not mistaken for intraday histories.
    selections = [
        ("SPI", "v1", "PixLiquidadosAtual", [], [
            ("Quantidade", "transactions", "total settled Pix count"),
            ("CanalPrimario", "transactions", "Pix primary-channel count"),
            ("CanalSecundario", "transactions", "Pix secondary-channel count"),
            ("Total", "BRL thousands", "settled Pix value"),
        ]),
        ("STR", "v2", "STRLiquidadosAtual", [], [("Quantidade", "transfers", "reserve-transfer count")]),
        ("STR", "v2", "STRLiquidadosClientesAtual", [], [("Quantidade", "transfers", "customer reserve-transfer count")]),
        ("STR", "v2", "STRLiquidadosClientesFaixaValor", ["FaixaValor"], [
            ("Quantidade", "transfers", "customer reserve-transfer count by value band")]),
        ("STR", "v2", "STREstatisticasTransferenciasLiquidacaoDocumentosComCodigoBarras", [], [
            ("Quantidade", "transfers", "barcode-document settlement count")]),
        ("mecir_dinheiro_em_circulacao", "v1", "informacoes_diarias_com_categoria",
         ["Categoria", "Denominacao", "Especie"], [("Quantidade", "banknotes or coins", "currency units in circulation")]),
    ]
    docs = {}
    for service, version, endpoint, dimensions, metrics in selections:
        base = f"https://olinda.bcb.gov.br/olinda/servico/{service}/versao/{version}"
        if base not in docs:
            raw, docs[base] = fetch.get(base + "/documentacao", max_age_hours=720)
            if b'"ANONIMO"' not in raw:
                raise ValueError("BCB service is no longer documented as anonymous public access")
        url = base + "/odata/" + endpoint
        # These services currently reject date comparisons as Boolean/Date type
        # mismatches. Read the documented public table with stable pagination.
        params = {"$format": "json", "$top": 100000,
                  "$orderby": ",".join(["Data"] + dimensions)}
        if service in {"SPI", "STR"}:
            params = {"$format": "json", "$top": 100000}
        parts, hashes = [], []
        for skip in range(0, 2000000, 100000):
            page_params = dict(params, **{"$skip": skip}) if service not in {"SPI", "STR"} else params
            raw, digest = fetch.get(url, page_params, timeout=180, max_age_hours=720)
            obj = json.loads(raw)
            if digest in hashes:
                raise ValueError("BCB pagination repeated a page")
            hashes.append(digest)
            page = pd.DataFrame(obj["value"])
            parts.append(page)
            print("BCB PAGE", endpoint, skip, len(page), flush=True)
            if len(page) < 100000:
                break
            if service in {"SPI", "STR"}:
                raise ValueError("BCB small settlement table unexpectedly requires pagination")
        else:
            raise ValueError("BCB pagination ceiling")
        data = pd.concat(parts, ignore_index=True)
        if data.empty:
            continue
        data = data[pd.to_datetime(data.Data, utc=True) >= pd.Timestamp(START, tz="UTC")].copy()
        groups = data.groupby(dimensions, dropna=False) if dimensions else [((), data)]
        for key, part in groups:
            key = key if isinstance(key, tuple) else (key,)
            info = dict(zip(dimensions, [str(k) for k in key]))
            identity = json.dumps(info, sort_keys=True, ensure_ascii=False)
            suffix = hashlib.sha256(identity.encode()).hexdigest()[:10] if dimensions else "all"
            for column, unit, label in metrics:
                if service == "mecir_dinheiro_em_circulacao":
                    unit = {"Cédulas": "banknotes", "Moedas": "coins"}[info["Especie"]]
                frame = clean(part.Data, part[column])
                item = save(
                    f"brazil_bcb_{slug(endpoint)}_{suffix}_{slug(column)}", frame,
                    title=f"Brazil {label}" + (": " + ", ".join(info.values()) if info else ""),
                    provider="Banco Central do Brasil payments and currency",
                    category="payments_and_cash_demand", unit=unit, frequency="daily", native_frequency="daily",
                    source_url=url, source_parameters=params, geography="Brazil",
                    geography_scope="non_us", country_codes=["BR"],
                    dataset_identity=f"BCB_OLINDA:{service}:{endpoint}:{identity}:{column}",
                    documentation_url=base + "/documentacao", raw_sha256=[docs[base]] + hashes,
                    source_dimensions=info,
                    hypothesis="Payment settlement activity and demand for cash denominations may reveal commerce, payroll and tax timing, liquidity stress and shifts in household transaction behavior.",
                    limitations="Official aggregate settlement and circulating-currency statistics; no customer records. SPI covers interparticipant Pix transfers and refunds, excluding purely internal book transfers. Counts, channel splits and totals are related. Cash denomination value is not duplicated as a second series because it is a fixed multiple of count. Historical revisions and changing Pix adoption affect comparability. Intraday rolling average profiles are excluded.",
                    timestamp_semantics="Brazilian source operating-day label represented at UTC midnight",
                    expected_publication_lag="Source daily statistics; exact original release time is not supplied",
                )
                if item:
                    result.append(item)
    return result


def brazil_fx_interventions(fetch):
    url = "https://www.bcb.gov.br/conteudo/dadosabertos/BCBDepin/historico-atuacoes-mercado-cambio.csv"
    doc = "https://www.bcb.gov.br/conteudo/dadosabertos/BCBDepin/documentacao-atuacoes-mercado-cambio.pdf"
    raw, digest = fetch.get(url, max_age_hours=720)
    _, doc_hash = fetch.get(doc, max_age_hours=720)
    data = pd.read_csv(io.BytesIO(raw), decimal=",")
    data["date"] = pd.to_datetime(data["Data"], utc=True)
    data = data[data.date >= pd.Timestamp(START, tz="UTC")]
    result = []
    for (instrument, modality), part in data.groupby(["Instrumento", "Modalidade"]):
        totals = part.groupby("date", as_index=False)["Volume USD Aceito"].sum(min_count=1)
        frame = clean(totals.date, totals["Volume USD Aceito"])
        identity = f"{instrument}:{modality}"
        suffix = hashlib.sha256(identity.encode()).hexdigest()[:12]
        item = save(
            f"brazil_bcb_fx_intervention_{suffix}_accepted_usd", frame,
            title=f"Brazil central-bank FX intervention — {instrument}, {modality}: accepted USD volume",
            provider="Banco Central do Brasil foreign-exchange interventions",
            category="central_bank_fx_intervention", unit="USD notional", frequency="business_daily",
            native_frequency="daily", source_url=url, documentation_url=doc,
            geography="Brazil", geography_scope="non_us", country_codes=["BR"],
            raw_sha256=[digest, doc_hash], dataset_identity=f"BCB_FX_INTERVENTION:{identity}:accepted_usd",
            source_instrument=str(instrument), source_modality=str(modality),
            hypothesis="Central-bank currency intervention volume may reflect exchange-market pressure, dollar-hedging demand and liquidity conditions affecting Brazilian assets.",
            limitations="Actual accepted notional summed across the day's published auction/maturity rows; no artificial zeros on days without reported interventions. Offered volume is excluded because auction totals can repeat across maturities. Original announcements contain time labels, but those are not the availability time of accepted auction results. This consolidated historical file is updated monthly and does not include cancellations or early payments after an intervention.",
            timestamp_semantics="Operation-day label represented at UTC midnight",
            expected_publication_lag="Consolidated file is updated by the last business day of a month with the previous month's interventions; individual notices were published earlier",
            aggregation="Sum of source Volume USD Aceito within instrument, modality and operation day",
        )
        if item:
            result.append(item)
    return result


def eu_trade(fetch, only_tasks=None):
    doc_url = "https://agridata.ec.europa.eu/extensions/API_Documentation/taxud.html"
    _, doc_hash = fetch.get(doc_url, max_age_hours=720)
    sectors_url = "https://api.tech.ec.europa.eu/agrifood/api/taxud/weeklyData/sectors"
    raw, sectors_hash = fetch.get(sectors_url, max_age_hours=720)
    available = set(json.loads(raw))
    selected = [s for s in TAXUD_SECTORS if s in available]
    result = []

    def collect(direction, sector):
        import requests
        url = f"https://api.tech.ec.europa.eu/agrifood/api/taxud/weeklyData/{direction}"
        parts, hashes = [], [doc_hash, sectors_hash]
        for year in [2024, 2025, 2026]:
            params = {"sectors": sector, "marketingYears": str(year)}
            try:
                raw, digest = fetch.get(url, params, timeout=360, max_age_hours=24 * 30)
            except requests.HTTPError as exc:
                if exc.response.status_code == 404:
                    print("TAXUD NO CALENDAR DATA", direction, sector, year, flush=True)
                    continue
                raise
            frame = parse_taxud(raw, flag_ambiguous=True)
            if not frame.empty and not frame.sector.eq(sector).all():
                raise ValueError("TAXUD ignored requested sector")
            hashes.append(digest)
            parts.append(frame)
            print("TAXUD SOURCE", direction, sector, year, len(frame), flush=True)
            time.sleep(1)
        if not parts:
            return []
        data = pd.concat(parts, ignore_index=True)
        output = []
        dimensions = ["memberStateCode", "partnerCode", "product"]
        for (member, partner, product), part in data.groupby(dimensions):
            source_codes = [{"EL": "GR", "UK": "GB"}.get(str(c), str(c)) for c in [member, partner]]
            if any(code not in ISO_COUNTRIES for code in source_codes):
                OMISSIONS.append({"source": "EU TAXUD", "direction": direction, "sector": sector,
                                  "member": member, "partner": partner, "product": product,
                                  "reason": "Regional or unspecified customs geography excluded; retained series use verified ISO country pairs"})
                continue
            if part["_ambiguous_source_cell"].any():
                OMISSIONS.append({"source": "EU TAXUD", "direction": direction, "sector": sector,
                                  "member": member, "partner": partner, "product": product,
                                  "reason": "Conflicting records with identical published source dimensions; entire bilateral product history excluded"})
                continue
            latest = part.sort_values("date").iloc[-1]
            identity = f"{direction}:{sector}:{member}:{partner}:{product}"
            sid_hash = hashlib.sha256(identity.encode()).hexdigest()[:12]
            for field, metric, unit in [("kg", "mass", "kilograms"),
                                         ("euroValue", "declared_value", "EUR")]:
                totals = part.groupby("date", as_index=False)[field].sum(min_count=1)
                frame = clean(totals.date, totals[field])
                scope, codes = trade_geography(member, partner)
                item = save(
                    f"eu_taxud_{direction}_{slug(sector)}_{slug(member)}_{slug(partner)}_{sid_hash}_{metric}",
                    frame,
                    title=f"{latest.memberStateName} {direction}s — {product} with {latest.partner}: weekly {metric.replace('_', ' ')}",
                    provider="European Commission TAXUD customs surveillance",
                    category="trade_" + slug(sector), unit=unit, frequency="weekly", native_frequency="weekly",
                    source_url=url, geography=f"{latest.memberStateName} / {latest.partner}",
                    geography_scope=scope, country_codes=codes,
                    dataset_identity="EU_TAXUD:" + identity + ":" + field,
                    source_sector=sector, source_product=str(product),
                    trade_direction=direction, trade_partner=str(partner), source_member_state_code=str(member),
                    hypothesis="Weekly bilateral declared trade value and physical volume can reveal changes in supply chains, commodity demand, trade finance, consumer-goods demand, import substitution and exporter earnings before slower national trade releases.",
                    limitations="Source weekly customs-surveillance declarations summed across tariff codes, procedures and preferences within a named product and bilateral flow. Not unique shipments, final national-accounts trade, or intraday cash transfers. Missing weeks remain missing. Calendar-year datasets only; split marketing years and boundary weeks 1/53 are omitted to avoid partial-year and overlapping-week ambiguity. Entire bilateral product histories are excluded if any source cells conflict despite identical published dimensions. Revisions, late declarations and reporting gaps can be material. The verified calendar mapping currently covers 2024–2026 only.",
                    raw_sha256=hashes, documentation_url=doc_url,
                    timestamp_semantics="Sunday ending the full source reporting week; not the first publication date",
                    source_parameters={"sectors": sector, "marketingYears": "2024,2025,2026"},
                    aggregation="Sum of reported mass/value across tariff10/procedure/preference records within product-country-pair-week",
                    expected_publication_lag="Weekly surveillance release; latest weeks subject to incomplete declarations and revision",
                )
                if item:
                    output.append(item)
        print("TAXUD FAMILY", direction, sector, len(output), flush=True)
        return output

    with ThreadPoolExecutor(max_workers=3) as pool:
        tasks = [(direction, sector) for sector in selected for direction in ["import", "export"]]
        if only_tasks is not None:
            tasks = [task for task in tasks if task in only_tasks]
        futures = [pool.submit(collect, direction, sector) for direction, sector in tasks]
        for future in as_completed(futures):
            result.extend(future.result())
    return result


PROVIDERS = {"taiwan": taiwan_agriculture, "taiwan_fish": lambda f: taiwan_agriculture(f, aquatic=True),
             "eu_prices": eu_prices, "colombia": colombia_security, "eu_trade": eu_trade,
             "brazil": brazil_payments, "brazil_fx": brazil_fx_interventions}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=PROVIDERS, action="append")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    chosen = args.provider or list(PROVIDERS)
    baseline = pd.read_parquet(ROOT / "catalog.parquet")
    SEEN.update(baseline.loc[baseline.batch < 6, "content_sha256"].dropna())
    fetch = Fetcher(refresh=args.refresh)
    reports = {}
    errors = {}
    for name in chosen:
        try:
            items = PROVIDERS[name](fetch)
            reports[name] = {"series": len(items), "rows": sum(x["rows"] for x in items)}
        except Exception as exc:
            import traceback
            traceback.print_exc()
            errors[name] = f"{type(exc).__name__}: {exc}"
    report = {"group": GROUP, "providers": reports, "errors": errors, "quality_omissions": OMISSIONS,
              "completed_at": pd.Timestamp.now(tz="UTC").isoformat()}
    atomic_bytes(ROOT / "logs" / ("economy4_" + "_".join(chosen) + ".json"),
                 json.dumps(report, indent=2).encode())
    print(json.dumps(report, indent=2), flush=True)
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
