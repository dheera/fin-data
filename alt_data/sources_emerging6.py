#!/usr/bin/env python3
"""Domestic high-frequency prices and financial activity in India, Mexico and Indonesia."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import io
import json
import re
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests

from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes,
                              coverage, normalize_frame, qualifies_research, save_candidate)

GROUP = "emerging6"
START = pd.Timestamp("2023-01-01", tz="UTC")
SEEN = set()
IDENTITIES = set()
IDS = set()
PINNED = {}
OMITTED = Counter()


def initialize():
    global PINNED
    c = pd.read_parquet(ROOT / "catalog.parquet")
    old = c[c.batch < 8]
    SEEN.clear(); IDENTITIES.clear(); IDS.clear(); OMITTED.clear()
    SEEN.update(old.content_sha256.dropna())
    IDENTITIES.update(old.dataset_identity.dropna())
    IDS.update(old.series_id)
    PINNED = c[c.batch == 8].set_index("series_id").frequency.to_dict()


def slug(value):
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def numeric_hash(frame):
    return hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes()
                          + frame.value.to_numpy().tobytes()).hexdigest()


def stage(sid, frame, **meta):
    if sid in IDS or (PINNED and sid not in PINNED):
        OMITTED["existing_or_not_selected"] += 1
        return None
    if PINNED.get(sid, meta["frequency"]) != meta["frequency"]:
        OMITTED["frequency_changed"] += 1
        return None
    if frame.empty:
        return None
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2 or not qualifies_research(dict(coverage(frame), frequency=meta["frequency"])):
        OMITTED["short_constant_or_sparse"] += 1
        return None
    digest = numeric_hash(frame)
    if digest in SEEN or meta["dataset_identity"] in IDENTITIES:
        OMITTED["duplicate"] += 1
        return None
    meta.setdefault("point_in_time", False)
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2")
    meta.setdefault("geography_scope", "non_us")
    meta.setdefault("finer_frequency_available", False)
    result = save_candidate(GROUP, sid, frame, **meta)
    SEEN.add(digest); IDENTITIES.add(meta["dataset_identity"]); IDS.add(sid)
    return result


def reported_frame(dates, values):
    frame = pd.DataFrame({"date": pd.to_datetime(dates, utc=True), "value": pd.to_numeric(values, errors="raise")})
    frame = frame.dropna()
    if frame.empty:
        return frame
    if frame.groupby("date").value.nunique().gt(1).any():
        raise ValueError("Conflicting values at a source observation date")
    return normalize_frame(frame.drop_duplicates("date"))


class PublicPostFetcher(Fetcher):
    """Archive ordinary public HTML-form/JSON read requests as well as GETs."""

    def session_get(self, url, session, *, query=None, timeout=90):
        """Maintain a public form's ordinary anonymous session; archive returned bytes."""
        response = session.get(url, timeout=(15, timeout),
                               headers={"User-Agent": "FinAltDataResearch/1.0 (public time-series research)"})
        response.raise_for_status()
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        semantic = {"method": "GET", "url": url, "anonymous_form_query": query}
        key = hashlib.sha256(json.dumps(semantic, sort_keys=True).encode()).hexdigest()
        meta = dict(semantic, sha256=digest, retrieved_at=pd.Timestamp.now(tz="UTC").isoformat(),
                    content_type=response.headers.get("Content-Type"))
        atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), raw)
        atomic_bytes(self.root / "archive/receipts" / (key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + ".json"), json.dumps(meta, indent=2).encode())
        return raw, digest

    def post(self, url, *, data=None, json_body=None, cache_id=None, session=None,
             timeout=90, max_age_hours=24):
        semantic = cache_id if cache_id is not None else {"data": data, "json": json_body}
        request_key = json.dumps({"method": "POST", "url": url, "query": semantic}, sort_keys=True)
        key = hashlib.sha256(request_key.encode()).hexdigest()
        index = self.root / "archive/requests" / (key + ".json")
        if not self.refresh and index.exists():
            old = json.loads(index.read_text())
            path = self.root / "archive/raw" / (old["sha256"] + ".bin")
            age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(old["retrieved_at"])).total_seconds()
            if 0 <= age < max_age_hours * 3600 and path.exists():
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() == old["sha256"]:
                    return raw, old["sha256"]
        client = session or requests
        response = client.post(url, data=data, json=json_body, timeout=(15, timeout),
                               headers={"User-Agent": "FinAltDataResearch/1.0 (public time-series research)"})
        response.raise_for_status()
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        meta = {"url": response.url, "method": "POST", "query": semantic,
                "sha256": digest, "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "content_type": response.headers.get("Content-Type")}
        atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), raw)
        atomic_bytes(self.root / "archive/receipts" / (key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + ".json"), json.dumps(meta, indent=2).encode())
        atomic_bytes(index, json.dumps(meta, indent=2).encode())
        return raw, digest


BI_BASE = "https://www.bi.go.id/hargapangan"


def parse_pihps(raw, commodity_names):
    obj = json.loads(raw)
    rows = obj.get("data")
    if not isinstance(rows, list):
        raise ValueError("PIHPS missing data array")
    data = pd.DataFrame(rows)
    if data.empty or list(data.columns) == []:
        return pd.DataFrame(columns=["date", "value", "commodity_id"])
    if not {"date", "price", "name"} <= set(data):
        raise ValueError("PIHPS chart did not return observations")
    data["commodity_name"] = data.name.str.replace(r"\s*\([^)]*\)\s*$", "", regex=True).str.strip()
    if not set(data.commodity_name) <= set(commodity_names):
        raise ValueError("PIHPS returned an unrequested commodity")
    data["commodity_id"] = data.commodity_name.map(commodity_names)
    data["date"] = pd.to_datetime(data.date, format="%d %b %y", utc=True, errors="raise")
    data["value"] = pd.to_numeric(data.price, errors="raise")
    # Missing prices are absent, never free food or a carried local observation.
    data = data[data.value.notna() & data.value.gt(0)].copy()
    if not np.isfinite(data.value).all():
        raise ValueError("PIHPS nonfinite price")
    return data[["date", "value", "commodity_id"]]


def indonesia_food(fetch):
    documentation = BI_BASE + "/Informasi/FAQ"
    docs = [fetch.get(u, max_age_hours=720)[1] for u in [
        documentation, BI_BASE + "/TabelHarga/PasarTradisionalDaerah",
        "https://www.bi.go.id/id/publikasi/ruang-media/news-release/Pages/sp_2517423.aspx",
    ]]
    raw, ph = fetch.get(BI_BASE + "/WebSite/TabelHarga/GetRefProvince", max_age_hours=720)
    provinces = json.loads(raw)["data"]
    raw, ch = fetch.get(BI_BASE + "/WebSite/TabelHarga/GetRefCommodityAndCategory", max_age_hours=720)
    # FAQ specifies oil per litre, whereas API dictionary/chart label says kg.
    # Omit the three oil grades until this source-unit inconsistency is resolved.
    commodities = {x["id"]: x for x in json.loads(raw)["data"]
                   if x["id"].startswith("com_") and x["cat_id"] != "cat_9"}
    names = {x["name"].strip(): key for key, x in commodities.items()}
    end = (pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)).date().isoformat()
    endpoint = BI_BASE + "/WebSite/TabelHarga/GetChartDaerah"

    def download(province):
        params = dict(price_type_id=1, comcat_id=",".join(commodities),
                      province_id=str(province["id"]), regency_id="", market_id="",
                      tipe_laporan=6, start_date="2023-01-01", end_date=end)
        raw, digest = fetch.get(endpoint, params, timeout=180, max_age_hours=24)
        data = parse_pihps(raw, names)
        if data.empty:
            raise ValueError("No PIHPS price observations for " + province["name"])
        if (data.date < START).any() or (data.date > pd.Timestamp(end, tz="UTC")).any():
            raise ValueError("PIHPS date filter was not honored")
        time.sleep(.35)
        return province, data, digest, params

    result = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        for province, data, digest, params in pool.map(download, provinces):
            count = 0
            for commodity, part in data.groupby("commodity_id", sort=True):
                info = commodities[commodity]
                frame = reported_frame(part.date, part.value)
                sid = f"id_pihps_province_{province['id']}_{commodity}_traditional_price"
                item = stage(
                    sid, frame, title=f"Indonesia {province['name']}: {info['name'].strip()} surveyed traditional-market retail price",
                    provider="Bank Indonesia PIHPS retail food survey",
                    category="indonesia_observed_retail_food_prices", unit="IDR per kg",
                    frequency="business_daily", native_frequency="business_daily published survey observations",
                    source_url=endpoint, documentation_url=documentation,
                    geography=f"Indonesia — {province['name']} province survey aggregate", country_codes=["ID"],
                    source_province_id=province["id"], source_commodity_id=commodity,
                    source_commodity_name=info["name"].strip(), source_commodity_metadata=info,
                    source_market_type="Pasar Tradisional", source_query=params,
                    hypothesis="Observed provincial staple-food transaction prices can track food-inflation shocks, supply disruptions and household purchasing-power pressure affecting Indonesian rates, currency and consumer-sector research.",
                    limitations="Published survey prices, not expenditure, quantities or a national CPI basket. Province averages combine surveyed city/market averages; sampling, trader substitution and price rounding affect comparability. The publisher may reuse the preceding price when disasters close a market; those cases are not separately flagged. No local forward filling, interpolation, zero-price insertion or weekend expansion. Current history may contain revisions and is not a historical as-of feed. Three cooking-oil grades are omitted because the API labels kg while the methodology specifies litres.",
                    aggregation="Source-reported provincial average of surveyed traditional-market prices; one actual published date/value per leaf commodity grade",
                    timestamp_semantics="Indonesian source survey calendar date represented at UTC midnight; not an execution or release timestamp",
                    expected_publication_lag="Traditional-market survey 09:00–11:00 WIB on business days; reporting 10:00–12:00 WIB and target publication 13:00 WIB. Revisions can continue into the following day.",
                    raw_sha256=[digest, ph, ch, *docs], dataset_identity=f"BI_PIHPS:type=1:province={province['id']}:commodity={commodity}:price",
                    license="Bank Indonesia public statistical portal; source attribution retained",
                )
                if item:
                    result.append(item); count += 1
            print("PIHPS", province["name"], len(data), count, flush=True)
    return result


CDSL_FORM = "https://www.cdslindia.com/Publications/FIITrends.aspx"
CDSL_REPORT = "https://www.cdslindia.com/Publications/ArchiveDataAfter2009.aspx"


def number(value):
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "nan", "None"}:
        return np.nan
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    return float(text)


def parse_cdsl(raw):
    tables = pd.read_html(io.StringIO(raw.decode("utf-8")))
    if len(tables) != 2 or any(t.shape[1] != 8 for t in tables):
        raise ValueError("Unexpected CDSL report tables")
    result = []
    cash, derivatives = tables
    # Forward filling here decodes HTML group headings, never missing observations.
    cash[0] = cash[0].ffill()
    cash = cash[cash[0].astype(str).str.fullmatch(r"\d{2}-\d{2}-\d{4}")]
    for row in cash.itertuples(index=False, name=None):
        date, asset, route = row[:3]
        if str(asset).lower() == "total" or "total" in str(route).lower():
            continue
        result.append(dict(date=pd.to_datetime(date, format="%d-%m-%Y", utc=True),
                           family="cash", instrument=str(asset), route=str(route),
                           measure="net_investment", value=number(row[5])))
    derivatives[0] = derivatives[0].ffill()
    derivatives = derivatives[derivatives[0].astype(str).str.fullmatch(r"\d{2}-[A-Za-z]{3}-\d{4}")]
    for row in derivatives.itertuples(index=False, name=None):
        date, instrument = row[:2]
        for col, measure in [(3, "buy_notional"), (5, "sell_notional"), (6, "open_interest_contracts")]:
            value = number(row[col])
            if value < 0:
                raise ValueError("Negative CDSL gross activity or position")
            result.append(dict(date=pd.to_datetime(date, format="%d-%b-%Y", utc=True),
                               family="derivatives", instrument=str(instrument), route="exchange",
                               measure=measure, value=value))
    if not result:
        raise ValueError("CDSL returned no dated observations")
    return pd.DataFrame(result).dropna(subset=["value"])


def form_cache(fetch, key, download, validate=None):
    """Cache completed semantic public-form queries, independently of session tokens."""
    index = fetch.root / "archive/emerging6_forms" / (hashlib.sha256(key.encode()).hexdigest() + ".json")
    if index.exists() and not fetch.refresh:
        obj = json.loads(index.read_text())
        raw = fetch.root / "archive/raw" / (obj["sha256"] + ".bin")
        age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(obj["retrieved_at"])).total_seconds()
        if age < 24 * 3600 and raw.exists():
            content = raw.read_bytes()
            if hashlib.sha256(content).hexdigest() == obj["sha256"] and (validate is None or validate(content)):
                return content, obj["sha256"]
    raw, digest = download()
    if validate is not None and not validate(raw):
        raise ValueError("Public form response did not match semantic query")
    atomic_bytes(index, json.dumps(dict(query=key, sha256=digest, retrieved_at=pd.Timestamp.now(tz="UTC").isoformat())).encode())
    return raw, digest


def cdsl_month(fetch, end):
    def validate(raw):
        data = parse_cdsl(raw)
        return bool((data.date.dt.strftime("%Y-%m") == end.strftime("%Y-%m")).all() and (data.date <= end).all())
    def download():
        with requests.Session() as session:
            raw, _ = fetch.session_get(CDSL_FORM, session)
            soup = BeautifulSoup(raw, "html.parser")
            payload = {x["name"]: x.get("value", "") for x in soup.select('input[type="hidden"][name]')}
            payload.update(grpArchive="rdbAfter", selectedDate=end.strftime("%B %d, %Y"), ctl30="Go")
            raw, _ = fetch.post(CDSL_FORM, data=payload, session=session,
                                cache_id={"archive_through": end.date().isoformat()}, max_age_hours=0)
            if b"ArchiveDataAfter2009.aspx" not in raw:
                raise ValueError("CDSL form did not offer the historical report")
            return fetch.session_get(CDSL_REPORT, session, query={"archive_through": end.date().isoformat()})
    raw, digest = form_cache(fetch, "CDSL:month-through:" + end.date().isoformat(), download, validate)
    data = parse_cdsl(raw)
    if (data.date.dt.strftime("%Y-%m") != end.strftime("%Y-%m")).any() or (data.date > end).any():
        raise ValueError("CDSL archive returned dates outside requested month")
    time.sleep(.25)
    print("CDSL", end.date(), len(data), flush=True)
    return data, digest


def india_foreign_flows(fetch):
    documentation = "https://www.sebi.gov.in/curation/fpi.html"
    docs = [fetch.get(u, max_age_hours=720)[1] for u in [documentation, CDSL_FORM]]
    yesterday = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    ends = list(pd.date_range(START, yesterday, freq="ME"))
    if not ends or ends[-1] != yesterday:
        ends.append(yesterday)
    # The public archive can share selected-date state across anonymous sessions.
    # Serialize complete form/report cycles and independently verify each month.
    downloaded = [cdsl_month(fetch, day) for day in ends]
    data = pd.concat([x[0] for x in downloaded], ignore_index=True)
    manifest = archive_raw_manifest([x[1] for x in downloaded], description="CDSL monthly public archive reports containing daily reporting-date cash and derivative FPI activity")
    result = []
    for (family, instrument, route, measure), part in data.groupby(["family", "instrument", "route", "measure"], sort=True):
        frame = reported_frame(part.date, part.value)
        sid = "in_cdsl_fpi_" + "_".join(map(slug, [family, instrument, route, measure]))
        unit = "contracts" if measure == "open_interest_contracts" else "INR crore"
        item = stage(sid, frame,
                     title=f"India FPI {instrument}, {route}: {measure.replace('_', ' ')}",
                     provider="Central Depository Services India FPI activity", category="india_foreign_portfolio_" + family,
                     unit=unit, frequency="business_daily", native_frequency="daily reporting-date observations",
                     source_url=CDSL_REPORT, documentation_url=documentation,
                     geography="India securities markets", country_codes=["IN"],
                     source_instrument=instrument, source_investment_route=route, source_measure=measure,
                     source_family=family, source_history_start=START.date().isoformat(),
                     dataset_identity=f"CDSL:FPI:{family}:{instrument}:{route}:{measure}",
                     raw_sha256=docs + [manifest], raw_manifest_sha256=[manifest],
                     aggregation="Source-reported net cash investment or gross derivative notional/contract open interest; total rows, USD conversions and cash subtotals excluded",
                     timestamp_semantics="Source reporting date at UTC midnight, NOT execution date. Cash reports include confirmed custodian trades on and up to previous trading days; derivative reports describe previous trading-day activity/positions.",
                     expected_publication_lag="Cash: custodian reporting after trade date, potentially multiple days. Derivatives: exchange reports for the previous trading day. Intraday release times are not retained.",
                     hypothesis="Foreign portfolio capital flows and derivatives participation can describe external financing pressure, hedging demand and positioning around Indian equity, rates and rupee markets.",
                     limitations="Revised current historical reports, not historical as-of vintages. Reporting dates can differ from economic trade dates. Primary-market/other cash flows include corporate actions and are not pure exchange purchases. Gross derivatives notionals do not measure investor P&L or market value; contract specifications and category coverage may change. Missing observations are omitted; explicit source zeros are retained. No USD aliases or total/subtotal duplication.",
                     license="CDSL public statistical publication; source attribution retained")
        if item:
            result.append(item)
    return result


MX_BASE = "https://www.economia-sniim.gob.mx/Nuevo/Consultas/MercadosNacionales/PreciosDeMercado/Agricolas/"
MX_FORM = MX_BASE + "ConsultaFrutasYHortalizas.aspx"
MX_REPORT = MX_BASE + "ResultadosConsultaFechaFrutasYHortalizas.aspx"
MX_MARKETS = [33, 100, 140, 190, 310, 306]


def parse_sniim(raw, expected_per_kg=False):
    soup = BeautifulSoup(raw, "html.parser")
    if expected_per_kg and "por kilogramo calculado" not in soup.get_text(" ", strip=True).lower():
        raise ValueError("SNIIM response did not confirm calculated per-kilogram prices")
    pagination = soup.select_one("#lblPaginacion")
    table = soup.select_one("#tblResultados")
    if pagination is None or table is None:
        raise ValueError("SNIIM response lacks pagination or price table")
    match = re.search(r"(\d+)\s+de\s+(\d+)", pagination.get_text(" ", strip=True))
    if not match:
        raise ValueError("Unrecognized SNIIM pagination")
    period = re.search(r"Registros del\s+(\d{2}/\d{2}/\d{4})\s+al\s+(\d{2}/\d{2}/\d{4})", soup.get_text(" ", strip=True))
    single_date = period[1] if period and period[1] == period[2] else None
    rows = []
    for tr in table.select("tr"):
        cells = [x.get_text(" ", strip=True) for x in tr.find_all(["td", "th"], recursive=False)]
        if len(cells) == 8 and cells[0] != "Producto":
            if single_date is None:
                raise ValueError("SNIIM omitted row dates outside a single-date query")
            cells.insert(0, single_date)
        if not cells or not re.fullmatch(r"\d{2}/\d{2}/\d{4}", cells[0]):
            continue
        if len(cells) != 9:
            raise ValueError("SNIIM dated row has wrong number of columns")
        price = number(cells[7])
        if pd.isna(price) or price <= 0:
            continue
        rows.append(dict(date=pd.to_datetime(cells[0], format="%d/%m/%Y", utc=True),
                         product=cells[1], quality=cells[2], presentation=cells[3],
                         origin=cells[4], value=price, source_note=cells[8]))
    return pd.DataFrame(rows), int(match[1]), int(match[2])


def sniim_market(fetch, market, name, end):
    # Use stateless bounded date partitions: legacy "next page" sessions reset.
    # The public report's page-size query accepts 5,000 rows (~2 MB), enough
    # for most market/months. Split dates further whenever pagination remains.
    base = dict(ProductoId=-1, OrigenId=-1, Origen="Todos", DestinoId=market,
                Destino=name, PreciosPorId=2, RegistrosPorPagina=5000)
    parts, hashes, partitions = [], [], []

    def partition(first, last):
        params = dict(base, fechaInicio=first.strftime("%d/%m/%Y"), fechaFinal=last.strftime("%d/%m/%Y"))
        raw, digest = fetch.get(MX_REPORT, params, timeout=180, max_age_hours=24)
        data, page, pages = parse_sniim(raw, expected_per_kg=True)
        if page != 1:
            raise ValueError("SNIIM stateless query returned a later page")
        if pages > 1:
            if first >= last:
                raise ValueError("SNIIM single day exceeds bounded page size")
            middle = first + pd.Timedelta(days=(last - first).days // 2)
            partition(first, middle)
            partition(middle + pd.Timedelta(days=1), last)
            return
        if not data.empty and ((data.date < first).any() or (data.date > last).any()):
            raise ValueError("SNIIM stateless date filter ignored")
        parts.append(data); hashes.append(digest)
        partitions.append(dict(start=first.date().isoformat(), end=last.date().isoformat(),
                               sha256=digest, rows=len(data), pages=pages))
        time.sleep(.25)

    starts = pd.date_range(pd.Timestamp("2025-01-01", tz="UTC"), end, freq="MS")
    for first in starts:
        last = min(first + pd.offsets.MonthEnd(0), end)
        partition(first, last)
        print("SNIIM", market, first.strftime("%Y-%m"), "complete", flush=True)
    if not any(not p.empty for p in parts):
        raise ValueError("SNIIM market returned no observations")
    query = dict(base, fechaInicio="01/01/2025", fechaFinal=end.strftime("%d/%m/%Y"), partitioning="complete monthly or smaller one-page date partitions")
    return market, name, pd.concat(parts, ignore_index=True), hashes, query, partitions


def quote_frame(part, required=None):
    """One cadence per quote instrument; weekly medians use original source quotes."""
    # Exact duplicate quotes can straddle source pagination; count them only once.
    quotes = part.drop_duplicates(["date", "presentation", "value"])
    daily = normalize_frame(quotes.groupby("date", as_index=False).value.median())
    if required in {None, "business_daily"} and qualifies_research(dict(coverage(daily), frequency="business_daily")):
        return daily, "business_daily"
    if required == "business_daily":
        return None, None
    if required not in {None, "weekly"}:
        raise ValueError("Unsupported pinned quote frequency")
    weeks = quotes.date.dt.tz_localize(None).dt.to_period("W-SUN").dt.start_time.dt.tz_localize("UTC")
    current = pd.Timestamp.now(tz="UTC").normalize()
    current_week = current - pd.Timedelta(days=current.weekday())
    finished = quotes.assign(date=weeks).loc[lambda x: x.date < current_week]
    if finished.empty:
        return None, None
    weekly = normalize_frame(finished.groupby("date", as_index=False).value.median())
    if qualifies_research(dict(coverage(weekly), frequency="weekly")):
        return weekly, "weekly"
    return None, None


def mexico_wholesale_food(fetch):
    documentation = "https://www.economia-sniim.gob.mx/nuevo/inf_disp.asp"
    docs = [fetch.get(documentation, max_age_hours=720)[1]]
    raw, form_hash = fetch.get(MX_FORM, max_age_hours=24)
    soup = BeautifulSoup(raw, "html.parser")
    names = {int(x["value"]): x.get_text(" ", strip=True) for x in soup.select("#ddlDestino option")}
    prices = {x["value"]: x.get_text(" ", strip=True) for x in soup.select("#ddlPrecios option")}
    if "2" not in prices or "kilogramo" not in prices["2"].lower():
        raise ValueError("SNIIM calculated per-kilogram unit option changed")
    end = pd.Timestamp.now(tz="UTC").normalize() - pd.Timedelta(days=1)
    result = []
    # Date-partitioned GETs are stateless; bound concurrency to two markets.
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(sniim_market, fetch, m, names[m], end): m for m in MX_MARKETS}
        for future in as_completed(futures):
            market, name, data, hashes, params, partitions = future.result()
            manifest = archive_raw_manifest(hashes, description=f"SNIIM market {market} complete paginated wholesale fruit/vegetable price query")
            # The portal's generic kg header also covers item-based presentations.
            # Only explicit weight presentations support an unambiguous MXN/kg unit.
            weight = data.presentation.str.contains(r"(?i)kilogramo|\bkg\b", regex=True)
            OMITTED["nonweight_presentation_quotes"] += int((~weight).sum())
            data = data[weight].copy()
            for (product, quality, origin), part in data.groupby(["product", "quality", "origin"], sort=True):
                identity = f"SNIIM:market={market}:product={product}:quality={quality}:origin={origin}:modal_price_kg"
                sid = f"mx_sniim_{market}_" + slug(product) + "_" + hashlib.sha256(identity.encode()).hexdigest()[:12]
                if PINNED and sid not in PINNED:
                    continue
                frame, frequency = quote_frame(part, PINNED.get(sid))
                if frame is None:
                    OMITTED["short_or_sparse_quote"] += 1
                    continue
                item = stage(sid, frame, title=f"Mexico {name}: {product}, {quality}, origin {origin} wholesale price",
                             provider="Mexico Economy SNIIM wholesale market quotes", category="mexico_observed_wholesale_food_prices",
                             unit="MXN per kg", frequency=frequency, native_frequency="business-day wholesale price survey quotations",
                             finer_frequency_available=frequency == "weekly", source_url=MX_REPORT, documentation_url=documentation,
                             geography="Mexico — " + name, country_codes=["MX"], source_market_id=market,
                             source_product=product, source_quality=quality, source_origin=origin,
                             source_presentations=sorted(part.presentation.unique().tolist()), source_query=params, source_partitions=partitions,
                             source_notes=sorted(set(part.source_note) - {""})[:100], dataset_identity=identity,
                             raw_sha256=[form_hash, *docs, manifest], raw_manifest_sha256=[manifest],
                             aggregation=("Median of actual source modal price quotations per observed day; source prices calculated per kilogram" if frequency == "business_daily" else "Median of original actual source modal price quotations within each Monday–Sunday week; source prices calculated per kilogram; missing weeks omitted"),
                             timestamp_semantics=("Mexican source market survey date represented at UTC midnight" if frequency == "business_daily" else "Monday start of Monday–Sunday survey week at UTC midnight; value requires observations through the end of that week"),
                             expected_publication_lag=("Source describes daily price reporting; exact intraday release lag unavailable" if frequency == "business_daily" else "Weekly aggregation is available only after the labelled Monday–Sunday week ends, plus source reporting delay; exact source release times unavailable"),
                             hypothesis="Regional wholesale food prices can expose agricultural supply shocks, import-cost transmission and food-inflation pressure relevant to Mexican rates, currency and food producers/retailers.",
                             limitations="Surveyed frequent/modal wholesale quotes, not transaction-weighted prices, spending or volumes. Only explicit kilogram or kg-weight package presentations are retained; piece/bunch/item-based packaging is excluded because the generic kg header does not disambiguate those units. Source supplies calculated per-kilogram prices while retaining original packaging descriptions. Distinct weight-package quotes are combined by a median within market/product/quality/origin; exact duplicate quote rows count once. Coverage varies by seasonal product, market and origin. Weekly series omit the current unfinished calendar week. No forward fill or invented missing-day prices. Current revised history is not an historical as-of feed.",
                             license="Mexico Secretariat of Economy public SNIIM information; source attribution retained")
                if item:
                    result.append(item)
            print("SNIIM finished", market, len(data), "quotes", flush=True)
    return result


PROVIDERS = {"indonesia_food": indonesia_food, "india_foreign_flows": india_foreign_flows,
             "mexico_wholesale_food": mexico_wholesale_food}


def run(provider=None, refresh=False):
    initialize()
    fetch = PublicPostFetcher(refresh=refresh)
    results, failures = [], {}
    for name in [provider] if provider else PROVIDERS:
        start = pd.Timestamp.now(tz="UTC").isoformat()
        try:
            items = PROVIDERS[name](fetch)
            results.extend(items)
            report = {"provider": name, "started_at": start,
                      "series": len(items), "observations": sum(x["rows"] for x in items),
                      "frequency_counts": dict(Counter(x["frequency"] for x in items)),
                      "omitted": dict(OMITTED), "errors": []}
        except Exception as exc:
            import traceback
            traceback.print_exc()
            failures[name] = f"{type(exc).__name__}: {exc}"
            report = {"provider": name, "started_at": start, "errors": [failures[name]]}
        report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        atomic_bytes(ROOT / "logs" / f"emerging6_{name}.json", json.dumps(report, indent=2).encode())
        print(json.dumps(report), flush=True)
    return results, failures


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=PROVIDERS)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    _, failures = run(args.provider, args.refresh)
    raise SystemExit(bool(failures))
