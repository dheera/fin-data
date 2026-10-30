#!/usr/bin/env python3
"""China and Korea native inventory, financing and operational-demand histories."""
from __future__ import annotations

import argparse
from collections import defaultdict
from functools import lru_cache
import hashlib
import io
import json
import re
import time
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from country_codes import normalize_country_codes
from expansion_common import (ROOT, Fetcher, archive_raw_manifest, atomic_bytes,
                              coverage, normalize_frame, qualifies_research, save_candidate)

GROUP = "asia6"
SEEN = {}
OMISSIONS = []


class Omission(ValueError):
    pass


class AsiaFetcher(Fetcher):
    """Fetcher with archived read-only JSON/form query POST support."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocked_hosts = set()

    def get(self, url, params=None, *, timeout=90, headers=None, max_age_hours=24):
        host = urlparse(url).netloc
        if host in self.blocked_hosts:
            raise Omission("Provider access denied earlier in this process: " + host)
        prepared = requests.Request("GET", url, params=params).prepare().url
        key = hashlib.sha256(prepared.encode()).hexdigest()
        index = self.root / "archive/requests" / (key + ".json")
        if not self.refresh and index.exists():
            info = json.loads(index.read_text())
            age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(info["retrieved_at"])).total_seconds()
            raw = self.root / "archive/raw" / (info["sha256"] + ".bin")
            if 0 <= age < max_age_hours * 3600 and raw.exists():
                content = raw.read_bytes()
                if hashlib.sha256(content).hexdigest() == info["sha256"]:
                    return content, info["sha256"]
        request_headers = {"User-Agent": "FinAltDataResearch/1.0 (public historical data research)", **(headers or {})}
        for attempt_no in range(3):
            try:
                response = requests.get(url, params=params, timeout=(15, timeout), headers=request_headers)
                response.raise_for_status()
                content = response.content
                digest = hashlib.sha256(content).hexdigest()
                info = {"url": response.url, "sha256": digest, "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "content_type": response.headers.get("Content-Type")}
                atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), content)
                stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
                atomic_bytes(self.root / "archive/receipts" / (key + "_" + stamp + ".json"), json.dumps(info, indent=2).encode())
                atomic_bytes(index, json.dumps(info, indent=2).encode())
                return content, digest
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in {401, 403, 429}:
                    self.blocked_hosts.add(host)
                if status in {401, 403, 429} or (status is not None and status not in {500, 502, 503, 504}) or attempt_no == 2:
                    raise
                time.sleep(3 ** (attempt_no + 1))

    def post(self, url, payload, *, form=False, timeout=45, max_age_hours=720):
        host = urlparse(url).netloc
        if host in self.blocked_hosts:
            raise Omission("Provider access denied earlier in this process: " + host)
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        key = hashlib.sha256(b"POST\n" + url.encode() + b"\n" + encoded).hexdigest()
        index = self.root / "archive/requests" / (key + ".json")
        if not self.refresh and index.exists():
            info = json.loads(index.read_text())
            age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(info["retrieved_at"])).total_seconds()
            raw = self.root / "archive/raw" / (info["sha256"] + ".bin")
            if 0 <= age < max_age_hours * 3600 and raw.exists():
                content = raw.read_bytes()
                if hashlib.sha256(content).hexdigest() == info["sha256"]:
                    return content, info["sha256"]
        headers = {"User-Agent": "FinAltDataResearch/1.0 (public historical data research)"}
        for attempt in range(3):
            try:
                response = requests.post(url, data=payload if form else None,
                                         json=None if form else payload, headers=headers,
                                         timeout=(15, timeout))
                response.raise_for_status()
                content = response.content
                digest = hashlib.sha256(content).hexdigest()
                meta = {"url": response.url, "method": "POST", "query_payload": payload,
                        "sha256": digest, "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                        "content_type": response.headers.get("Content-Type")}
                atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), content)
                stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ")
                atomic_bytes(self.root / "archive/receipts" / (key + "_" + stamp + ".json"),
                             json.dumps(meta, ensure_ascii=False, indent=2).encode())
                atomic_bytes(index, json.dumps(meta, ensure_ascii=False, indent=2).encode())
                return content, digest
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in {401, 403, 429}:
                    self.blocked_hosts.add(host)
                if status in {401, 403, 429} or (status is not None and status not in {500, 502, 503, 504}) or attempt == 2:
                    raise
                time.sleep(3 ** (attempt + 1))


@lru_cache(maxsize=1)
def baseline():
    cat = pd.read_parquet(ROOT / "catalog.parquet")
    old = cat[cat.batch.lt(8)]
    new = cat[cat.batch.eq(8)]
    return {"ids": set(old.series_id), "identities": set(old.dataset_identity.dropna()),
            "hashes": set(old.content_sha256),
            "pinned": dict(zip(new.series_id, new.frequency))}


def clean(dates, values):
    data = pd.DataFrame({"date": pd.to_datetime(dates, utc=True),
                         "value": pd.to_numeric(values, errors="raise")}).dropna()
    if data.empty:
        raise Omission("No native numeric observations")
    if data.groupby("date").value.nunique().gt(1).any():
        raise ValueError("Conflicting native observations for the same timestamp")
    return normalize_frame(data.drop_duplicates("date"))


def save(sid, frame, **meta):
    old = baseline()
    if sid in old["ids"] or meta["dataset_identity"] in old["identities"]:
        raise Omission("Previously published identity")
    if old["pinned"] and old["pinned"].get(sid) != meta["frequency"]:
        raise Omission("Outside pinned batch8 IDs/cadence")
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2 or not qualifies_research({"frequency": meta["frequency"], **coverage(frame)}):
        raise Omission("Constant or insufficient native history")
    digest = hashlib.sha256(frame.date.astype("int64").to_numpy().tobytes() + frame.value.to_numpy().tobytes()).hexdigest()
    if digest in old["hashes"] or (digest in SEEN and SEEN[digest] != sid):
        raise Omission("Exact numerical history already retained")
    SEEN[digest] = sid
    meta["country_codes"] = normalize_country_codes(meta["country_codes"])
    meta.setdefault("country_code_standard", "ISO 3166-1 alpha-2")
    meta.setdefault("geography_scope", "non_us")
    meta.setdefault("native_frequency", meta["frequency"])
    meta.setdefault("point_in_time", False)
    meta.setdefault("finer_frequency_available", False)
    meta.setdefault("timestamp_semantics", "Source local observation-day label stored at UTC midnight; not the actual historical release or observation instant.")
    result = save_candidate(GROUP, sid, frame, **meta)
    print(f"OK {sid} {len(frame)}", flush=True)
    return result


def attempt(good, name, fn):
    try:
        good.append(fn())
    except Omission as exc:
        OMISSIONS.append({"candidate": name, "reason": str(exc)})


def token(value):
    return re.sub("[^a-z0-9]+", "_", str(value).lower()).strip("_")[:50] + "_" + hashlib.sha256(str(value).encode()).hexdigest()[:12]


KPX_URL = "https://epsis.kpx.or.kr/epsisnew/selectEkgeEpsMepChart.do?locale=e&menuId=030100"
KPX_METRICS = {
    "Value": ("installed_capacity", "installed generation capacity", "설비용량"),
    "Value2": ("supply_capability", "available supply capability", "공급능력"),
    "Value3": ("maximum_demand", "maximum electricity demand", "최대전력"),
    "Value5": ("minimum_demand", "minimum electricity demand", "최소전력"),
}


def parse_kpx(raw, start):
    text = raw.decode("utf-8")
    objects = [json.loads(v) for v in re.findall(r"chartData\.push\((\{.*?\})\);", text, re.S)]
    if not objects:
        raise ValueError("No native KPX chart records")
    data = pd.DataFrame(objects)
    data["date"] = pd.to_datetime(data.Date, format="%Y/%m/%d", utc=True)
    data = data[data.date.ge(pd.Timestamp(start, tz="UTC"))]
    for field in KPX_METRICS:
        data[field] = pd.to_numeric(data[field], errors="raise")
        # The provider's own grid renders missing minimum demand zero as '-'.
        if field == "Value5":
            data.loc[data[field].eq(0), field] = np.nan
        elif data[field].le(0).any():
            raise ValueError("Nonpositive national supply/demand is not a valid observation")
    return data


def collect_kpx(fetcher, start, end):
    raw, digest = fetcher.get(KPX_URL, max_age_hours=24, timeout=90)
    data = parse_kpx(raw, start)
    data = data[data.date.lt(pd.Timestamp(end, tz="UTC"))]
    good = []
    for field, (metric, label, native_label) in KPX_METRICS.items():
        attempt(good, "kpx:" + metric, lambda field=field, metric=metric, label=label, native_label=native_label:
            save("kr_kpx_" + metric, clean(data.date, data[field]),
                 title="South Korea: daily " + label,
                 provider="Korea Power Exchange EPSIS", category="korean_electricity_operations",
                 frequency="daily", unit="MW", source_url=KPX_URL,
                 country_codes=["KR"], geography="South Korean electricity system",
                 raw_sha256=[digest], dataset_identity="kpx:epsis:030100:" + field,
                 source_metric=field, source_metric_label=native_label,
                 hypothesis="National peak and minimum power demand may reveal industrial utilization, cooling/heating consumption and operational stress before monthly production releases; available supply helps distinguish demand from supply constraints.",
                 limitations="National aggregates mix industry, households and services and are strongly affected by weather and holidays. Historical figures can revise. Daily extrema are not average load or consumed energy. The source states minimum demand is unavailable before 2025-11-24; zero placeholders for unavailable minimum demand are omitted; no calendar interpolation.",
                 expected_publication_lag="Updated daily with prior-day operating results according to the source page; exact historical first publication clocks are not supplied.",
                 source_timezone="Asia/Seoul", native_frequency="daily operating extrema and capacity",
                 aggregation="Native provider daily field; no resampling or derived ratios.",
                 missing_policy="Absent dates/values omitted. Native zero placeholders for minimum demand are omitted as the source grid displays them as missing."))
    return good, []


SHFE_BASE = "https://www.shfe.cn"
CN_REGIONS = set("北京 天津 河北 山西 内蒙古 辽宁 吉林 黑龙江 上海 江苏 浙江 安徽 福建 江西 山东 河南 湖北 湖南 广东 广西 海南 重庆 四川 贵州 云南 西藏 陕西 甘肃 青海 宁夏 新疆".split())
SHFE_UNITS = {"1": "kg", "2": "tonnes", "3": "barrels"}
# Commodity translations follow the same exchange's bilingual dated reports.
SHFE_ENGLISH = {"铜": "copper", "铜BC": "bonded copper", "铝": "aluminium", "氧化铝": "alumina",
    "锌": "zinc", "铅": "lead", "镍": "nickel", "锡": "tin", "黄金": "gold", "白银": "silver",
    "螺纹钢": "steel rebar", "线材": "steel wire rod", "热轧卷板": "hot-rolled steel coil", "不锈钢": "stainless steel",
    "天然橡胶": "natural rubber", "20号胶": "TSR 20 rubber", "丁二烯橡胶": "butadiene rubber", "纸浆": "wood pulp",
    "石油沥青": "bitumen", "燃料油": "fuel oil", "低硫燃料油": "low-sulphur fuel oil", "中质含硫原油": "medium sour crude oil", "铸造铝合金": "cast aluminium alloy", "胶版印刷纸": "offset printing paper"}



def commodity_name(label):
    return re.sub(r"[()（）\s]", "", label.split("$$")[0]).replace("厂库", "").replace("仓库", "")


def native_number(value):
    if value is None or str(value).strip() in {"", "--", "-", "nan"}:
        return None
    result = float(str(value).replace(",", ""))
    if not np.isfinite(result) or result < 0:
        raise ValueError("Invalid nonnegative native inventory count")
    return result


def shfe_record(day, commodity, warehouse_type, region, warehouse, unit, metric, value, **extra):
    region, warehouse = str(region).strip(), str(warehouse).strip()
    if region not in CN_REGIONS or not warehouse or any(x in warehouse for x in ("合计", "总计", "小计")):
        return None
    value = native_number(value)
    if value is None:
        return None
    identity = f"shfe:inventory:{commodity}:{warehouse_type}:{region}:{warehouse}:{metric}"
    return {"date": pd.Timestamp(day, tz="UTC"), "value": value, "key": identity,
            "commodity": commodity, "warehouse_type": str(warehouse_type), "region": region,
            "warehouse": warehouse, "unit": unit, "metric": metric, **extra}


def parse_shfe_json(raw, day, weekly=False):
    obj = json.loads(raw)
    if str(obj.get("o_tradingday")) != pd.Timestamp(day).strftime("%Y%m%d"):
        raise ValueError("SHFE native report date mismatch")
    rows = []
    for row in obj["o_cursor"]:
        if str(row.get("ROWSTATUS")) != "0":
            continue
        commodity = commodity_name(row["VARNAME"])
        warehouse_type = str(row["WHTYPE"])
        if weekly and (warehouse_type != "1" or commodity == "黄金"):
            continue
        fields = [("available_warrant_capacity", "WHSTOCKS"), ("deliverable_inventory", "SPOTWGHTS")] if weekly else [("on_warrant", "WRTWGHTS")]
        for metric, field in fields:
            value = row.get(field)
            # Some report families use zero placeholders for unavailable spot stock.
            if field == "SPOTWGHTS" and native_number(value) is not None:
                warrant = native_number(row.get("WRTWGHTS"))
                if warrant is not None and native_number(value) < warrant:
                    continue
            result = shfe_record(day, commodity, warehouse_type,
                row["REGNAME"].split("$$")[0], row["WHABBRNAME"].split("$$")[0],
                SHFE_UNITS[str(row["WGHTUNIT"])], metric, value,
                source_field=field, source_commodity_code=str(row.get("VARID", "")).strip(),
                source_commodity_label=row["VARNAME"], source_warehouse_label=row["WHABBRNAME"])
            if result:
                rows.append(result)
    releases = {k: obj.get(k) for k in ("report_date", "update_date", "print_date")}
    return pd.DataFrame(rows), releases


def parse_shfe_html(raw, day, weekly=False):
    soup = BeautifulSoup(raw, "html.parser")
    intro = soup.select_one("table.content_intro")
    if intro is None:
        raise ValueError("Missing SHFE report header")
    match = re.search(r"\d{4}-\d{2}-\d{2}", intro.get_text(" ", strip=True))
    if match is None or match[0] != day:
        raise ValueError("SHFE HTML report date mismatch")
    rows = []
    for table in soup.select("table.el-table_table"):
        heading = table.select_one("tr.special_row_type")
        if heading is None:
            continue
        cells = heading.find_all("td")
        label = cells[0].get_text(" ", strip=True)
        commodity = commodity_name(label)
        warehouse_type = "2" if "厂库" in label else "1"
        if weekly and (warehouse_type != "1" or commodity == "黄金"):
            continue
        unit_label = cells[-1].get_text(" ", strip=True).split("：")[-1]
        unit = {"吨": "tonnes", "千克": "kg", "桶": "barrels"}[unit_label]
        frame = pd.read_html(io.StringIO(str(table)), keep_default_na=False)[0]
        if not weekly and len(frame.columns) != 4:
            raise ValueError("Unexpected SHFE daily warehouse table layout")
        if weekly and len(frame.columns) not in {8, 11}:
            # Separate oil-grade and explanatory tables are not these warehouse metrics.
            continue
        for values in frame.iloc[1:].itertuples(index=False, name=None):
            region, warehouse = str(values[0]).strip(), str(values[1]).strip()
            if region not in CN_REGIONS or any(x in warehouse for x in ("合计", "总计", "小计")):
                continue
            if weekly:
                fields = [("available_warrant_capacity", "WHSTOCKS", values[9] if len(values) == 11 else values[6])]
                if len(values) == 11:
                    stock, warrant = native_number(values[4]), native_number(values[5])
                    if stock is not None and (warrant is None or stock >= warrant):
                        fields.append(("deliverable_inventory", "SPOTWGHTS", stock))
            else:
                fields = [("on_warrant", "WRTWGHTS", values[2])]
            for metric, field, value in fields:
                result = shfe_record(day, commodity, warehouse_type, region, warehouse, unit,
                                     metric, value, source_field=field,
                                     source_commodity_code="", source_commodity_label=label,
                                     source_warehouse_label=warehouse)
                if result:
                    rows.append(result)
    return pd.DataFrame(rows), {"report_date": day, "update_date": None, "print_date": None}


def shared_capacity_aliases(combined):
    """Source notes define pooled Cu/Al and rebar/wire capacity at a 3:1 ratio.

    Omit only same-facility histories with identical dates and exact equality after
    converting the source's whole-tonne reporting precision. Distinct stock and
    warrant metrics never enter this comparison.
    """
    candidates = combined[combined.metric.eq("available_warrant_capacity") & combined.unit.eq("tonnes")]
    groups = {}
    for key, part in candidates.groupby("key", sort=True):
        m = part.iloc[0]
        groups[(m.region, m.warehouse, m.warehouse_type, m.commodity)] = (key, part.drop_duplicates("date").set_index("date").value.sort_index())
    result = []
    for low, high in [("铜", "铝"), ("螺纹钢", "线材")]:
        for (region, warehouse, wtype, commodity), (key, values) in groups.items():
            if commodity != low or (region, warehouse, wtype, high) not in groups:
                continue
            other_key, other = groups[(region, warehouse, wtype, high)]
            if len(values) < 26 or not values.index.equals(other.index) or values.nunique() < 2 or other.nunique() < 2:
                continue
            if np.array_equal(np.round(values.to_numpy() / 3), other.to_numpy()):
                result.append({"retained_identity": key, "excluded_identity": other_key,
                    "observations_compared": len(values), "conversion": "excluded = round(retained / 3) in native whole tonnes",
                    "maximum_unrounded_residual_tonnes": float(np.max(np.abs(values.to_numpy() - 3 * other.to_numpy()))),
                    "source_definition": "SHFE weekly note 1: pooled facilities allocate one aluminium lot three copper lots of capacity, and one wire-rod lot three rebar lots of capacity.",
                    "reason": "Deterministic same-facility shared-capacity conversion, including native whole-tonne reporting precision; not an independent capacity history."})
    return result


def collect_shfe(fetcher, start, end, weekly=False):
    good, errors = [], []
    kind = "weeklystock" if weekly else "dailystock"
    cache = ROOT / "archive/asia6_shfe" / kind
    cache.mkdir(parents=True, exist_ok=True)
    _, doc_hash = fetcher.get(SHFE_BASE + "/images/api.js", max_age_hours=720)
    data, hashes, releases, absent = [], [doc_hash], [], []
    dates = pd.date_range(start, pd.Timestamp(end) - pd.Timedelta(days=1), freq="W-FRI" if weekly else "B")
    def one(day):
        date = day.strftime("%Y-%m-%d")
        receipt = cache / (date + ".json")
        target = receipt.with_suffix(".parquet")
        if not fetcher.refresh and receipt.exists() and target.exists():
            info = json.loads(receipt.read_text())
            if info.get("parser_version", 1) < 2:
                raw = (ROOT / "archive/raw" / (info["raw_sha256"] + ".bin")).read_bytes()
                if hashlib.sha256(raw).hexdigest() != info["raw_sha256"]:
                    raise ValueError("SHFE original raw checksum mismatch")
                frame, release = (parse_shfe_json if info["format"] == "json" else parse_shfe_html)(raw, date, weekly)
                if frame.empty or frame.groupby("key").value.nunique().gt(1).any():
                    raise ValueError("Invalid or ambiguous SHFE cached reparse")
                frame = frame.drop_duplicates("key")
                frame.to_parquet(target, index=False)
                info.update(parser_version=2, source_release_fields=release)
                atomic_bytes(receipt, json.dumps(info, ensure_ascii=False, indent=2).encode())
                return frame, info
            return pd.read_parquet(target), info
        compact = day.strftime("%Y%m%d")
        formats = ["json", "html"] if compact < "20260101" else ["html", "json"]
        for fmt in formats:
            if fmt == "json":
                url = f"{SHFE_BASE}/data/tradedata/future/{'weeklydata' if weekly else 'dailydata'}/{compact}{kind}.dat"
            else:
                url = f"{SHFE_BASE}/data/tradedata/future/stockdata/{kind}_{compact}/ZH/all.html"
            time.sleep(.6)
            try:
                raw, digest = fetcher.get(url, max_age_hours=720, timeout=40)
            except requests.HTTPError as exc:
                if exc.response.status_code == 404:
                    continue
                raise
            frame, release = (parse_shfe_json if fmt == "json" else parse_shfe_html)(raw, date, weekly)
            if frame.empty:
                raise ValueError("No supported native mainland warehouse observations")
            if frame.groupby("key").value.nunique().gt(1).any():
                raise ValueError("Ambiguous duplicate Chinese warehouse identity")
            frame = frame.drop_duplicates("key")
            frame.to_parquet(target, index=False)
            info = {"parser_version": 2, "raw_sha256": digest, "url": url, "format": fmt, "source_date": date,
                    "source_release_fields": release}
            atomic_bytes(receipt, json.dumps(info, ensure_ascii=False, indent=2).encode())
            return frame, info
        return None, None
    for i, day in enumerate(dates):
        try:
            frame, info = one(day)
            if frame is None and weekly:
                frame, info = one(day - pd.Timedelta(days=1))
            if frame is None:
                absent.append(str(day.date()))
                continue
            data.append(frame); hashes.append(info["raw_sha256"])
            releases.append({"source_date": info["source_date"], "raw_sha256": info["raw_sha256"], **info["source_release_fields"]})
            if len(data) % 25 == 0:
                print(f"SHFE {kind}: {len(data)} native reports / {i+1} requested periods", flush=True)
        except Exception as exc:
            errors.append({"day": str(day.date()), "error": repr(exc)})
            if getattr(getattr(exc, "response", None), "status_code", None) in {401, 403, 429} or fetcher.blocked_hosts:
                break
    if not data:
        return good, errors
    release_path = ROOT / "archive/asia6_supplemental" / (kind + "_source_times.parquet")
    release_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(releases).to_parquet(release_path, index=False)
    manifest = archive_raw_manifest(hashes, description="SHFE/INE original dated warehouse quantity reports and official endpoint definitions.")
    combined = pd.concat(data, ignore_index=True)
    aliases = shared_capacity_aliases(combined) if weekly else []
    excluded_aliases = {a["excluded_identity"] for a in aliases}
    if weekly:
        atomic_bytes(ROOT / "archive/asia6_selection/shfe_shared_capacity_aliases.json",
                     json.dumps({"aliases": aliases, "errors": []}, ensure_ascii=False, indent=2).encode())
    for key, part in combined.groupby("key", sort=True):
        if key in excluded_aliases:
            OMISSIONS.append({"candidate": key, "reason": "Deterministic shared-capacity unit-conversion alias; see archive/asia6_selection/shfe_shared_capacity_aliases.json"})
            continue
        meta = part.iloc[-1]
        metric = meta.metric
        if part.unit.nunique() != 1:
            raise ValueError("SHFE unit changed within a warehouse metric identity")
        def one_series():
            title_metric = {"on_warrant": "quantity on warrant", "deliverable_inventory": "delivery-quality warehouse inventory", "available_warrant_capacity": "available warrant-issuance capacity"}[metric]
            return save("cn_shfe_" + metric + "_" + token(key), clean(part.date, part.value),
                title=f"China {SHFE_ENGLISH.get(meta.commodity, meta.commodity)}: {meta.region} {meta.warehouse} {title_metric}",
                provider="Shanghai Futures Exchange warehouse reports", category="chinese_commodity_" + metric,
                frequency="weekly" if weekly else "daily", unit=meta.unit,
                source_url=SHFE_BASE + "/reports/tradedata/dailyandweeklydata/",
                country_codes=["CN"], geography=f"{meta.region}, mainland China; {meta.warehouse}",
                raw_sha256=[manifest], raw_manifest_sha256=[manifest], dataset_identity=key,
                source_metric=meta.source_field, source_commodity_label=meta.commodity,
                source_commodity_english=SHFE_ENGLISH.get(meta.commodity, meta.commodity),
                source_commodity_codes=sorted({v for v in part.source_commodity_code if v}),
                source_region=meta.region, source_warehouse=meta.warehouse,
                source_warehouse_type="factory warehouse" if meta.warehouse_type == "2" else "delivery warehouse",
                hypothesis="Commodity-specific registered stock, delivery-quality physical inventory and remaining warrant capacity can reveal raw-material tightness, destocking and financing collateral conditions before broader industrial production releases.",
                limitations="Only exchange-approved facilities and report definitions are covered, not all Chinese inventories. Factory warrants are delivery obligations and do not prove goods are already physically stored. Available warrant capacity is the quantity eligible for further warrant issuance, not empty building space; approved capacity may be shared across commodities. Native Chinese warehouse labels are used to avoid ambiguous English aliases; renamings are not inferred or stitched. Holiday reports may be absent, and historical source files can revise.",
                expected_publication_lag="After the Chinese trading session/report date. Native report/update/print fields, when supplied, are retained separately but are not verified first public availability times.",
                source_timezone="Asia/Shanghai", source_release_fields_path=str(release_path.relative_to(ROOT)),
                aggregation="Native facility quantity retained unchanged; subtotal/total rows and duplicate weekly warrant balances are excluded.",
                missing_policy="No filling or interpolation. Non-mainland/unknown region rows, unreported markers and spot-stock placeholders smaller than on-warrant stock are omitted. Weekly sampling uses native Friday reports, or Thursday when Friday is unavailable.",
                absent_requested_report_dates=absent)
        attempt(good, key, one_series)
    return good, errors


KOFIA_BASE = "https://freesis.kofia.or.kr"
KOFIA_JS = KOFIA_BASE + "/ui/app/com/main/Main.clx.js"
KOFIA_CLASSES = {"02": "equity", "03": "hybrid_equity", "04": "hybrid_bond", "01": "bond",
                 "05": "short_term_finance", "06": "derivatives", "07": "real_estate", "08": "real_asset",
                 "09": "fund_of_funds", "10": "variable_insurance", "11": "special_asset", "12": "hybrid_asset"}


def kofia_configs():
    """Explicit economically distinct source selections, not generated transformations."""
    configs = []
    def add(key, service, family, label, params, fields, *, counts=False):
        configs.append(dict(key=key, service=service, family=family, label=label, params=params,
                            fields=fields, counts=counts))
    add("credit", "STATSCU0100000070", "brokerage_credit", "brokerage credit", {"tmpV1": "RD"}, {
        3: "KOSPI_margin_loan_balance", 4: "KOSDAQ_margin_loan_balance", 6: "KOSPI_margin_short_sale_credit_balance",
        7: "KOSDAQ_margin_short_sale_credit_balance", 8: "subscription_loan_balance", 9: "securities_collateralized_loan_balance"})
    add("market_cash", "STATSCU0100000060", "brokerage_liquidity", "brokerage liquidity", {"tmpV1": "RD"}, {
        2: "investor_deposits_excluding_derivatives", 3: "exchange_derivative_client_deposits",
        4: "customer_RP_sell_balance", 5: "brokerage_unsettled_receivables", 6: "forced_liquidation_amount"})
    cma_fields = {2: "RP_type", 3: "MMF_type", 4: "merchant_bank_type", 5: "issued_note_type", 6: "other_including_MMW"}
    for cohort, cohort_code in [("individual", "1"), ("corporate", "2")]:
        for metric, service, counts in [("accounts", "STATSCU0100000100", True), ("committed_balance", "STATSCU0100000110", False)]:
            add("cma_" + cohort + "_" + metric, service, "cash_management_accounts",
                "CMA " + cohort + " " + ("subscription principal balance" if metric == "committed_balance" else metric), {"tmpV1": "RD", "tmpV59": cohort_code}, cma_fields, counts=counts)
    for fund_kind, kind_code in [("public", "1"), ("private", "2")]:
        for class_code, asset_class in KOFIA_CLASSES.items():
            add("fund_flows_" + fund_kind + "_" + asset_class, "STATFND0100100030", "fund_subscriptions_redemptions",
                fund_kind + " " + asset_class + " funds", {"tmpV37": "0", "tmpV5": "", "tmpV7": kind_code,
                "tmpV3": class_code, "tmpV11": "", "tmpV19": "Y"},
                {5: "domestic_reported_subscriptions", 6: "domestic_reported_redemptions",
                 8: "overseas_reported_subscriptions", 9: "overseas_reported_redemptions"})
    stock_fields = {2: "equity", 3: "hybrid_equity", 4: "hybrid_bond", 5: "bond", 6: "fund_of_funds",
                    7: "short_term_finance", 8: "derivatives", 9: "real_estate", 10: "real_asset", 11: "special_asset",
                    12: "hybrid_asset", 13: "institutional_private_equity", 14: "other_discretionary", 19: "investment_contract", 20: "corporate_growth"}
    for area, area_code in [("domestic", "1"), ("overseas", "2")]:
        for metric, metric_code in [("committed_capital", "1"), ("funds_and_contracts_count", "3")]:
            add("fund_stock_" + area + "_" + metric, "STATFND0100100260", "fund_capital_and_formation",
                area + " funds and mandates " + ("subscription principal and contract amount" if metric == "committed_capital" else metric),
                {"tmpV100": metric_code, "tmpV10": "0", "tmpV102": area_code, "tmpV5": "", "tmpV11": ""},
                stock_fields, counts=metric_code == "3")
    return configs


def kofia_headers(raw, fields):
    obj = json.loads(raw)
    by_id = {r["HEADER_ID"]: r for r in obj["dsGrid"]}
    labels = {}
    for field in fields:
        row = by_id[f"H{int(field):03}"]
        if row.get("HEADER_TYP") != "N" or row.get("HIDE_YN") != "N":
            raise ValueError("KOFIA selected metric is not a visible numeric field")
        label = row["HEADER_NM"].replace("\n", " ")
        parent = row.get("PARENT_HDRID")
        if parent:
            label = by_id[parent]["HEADER_NM"] + ": " + label
        labels[int(field)] = label
    return labels, obj["dsGridInfo"][0]


def parse_kofia(raw, fields, start, end, *, counts=False, signed_components=False):
    obj = json.loads(raw)
    if not isinstance(obj.get("ds1"), list):
        raise ValueError("KOFIA response lacks native records")
    rows = obj["ds1"]
    if not rows:
        return pd.DataFrame(columns=["date", *[f"TMPV{k}" for k in fields]])
    if any(not re.fullmatch(r"[0-9]{8}", str(r.get("TMPV1", ""))) for r in rows):
        raise ValueError("KOFIA expected native YYYYMMDD daily labels")
    data = pd.DataFrame(rows)
    data["date"] = pd.to_datetime(data.TMPV1, format="%Y%m%d", utc=True)
    if data.date.lt(pd.Timestamp(start, tz="UTC")).any() or data.date.gt(pd.Timestamp(end, tz="UTC")).any():
        raise ValueError("KOFIA response escaped requested date interval")
    if data.date.duplicated().any():
        raise ValueError("KOFIA repeated native dates: unexpected categorical cross-section")
    for field in fields:
        col = f"TMPV{field}"
        data[col] = pd.to_numeric(data[col], errors="raise")
        actual = data[col].dropna()
        if not np.isfinite(actual).all() or (actual.lt(0).any() and not signed_components):
            raise ValueError("KOFIA invalid negative/nonfinite balance, count or gross flow")
        if counts and not np.equal(actual, np.floor(actual)).all():
            raise ValueError("KOFIA account/fund counts unexpectedly fractional")
    return data[["date", *[f"TMPV{k}" for k in fields]]]


def collect_kofia(fetcher, start, end):
    from concurrent.futures import ThreadPoolExecutor
    good, errors = [], []
    config_path = ROOT / "archive/asia6_kofia/selection.json"
    configs = kofia_configs()
    atomic_bytes(config_path, json.dumps(configs, ensure_ascii=False, indent=2).encode())
    _, js_sha = fetcher.get(KOFIA_JS, max_age_hours=720)
    metadata = {}
    for service in sorted({c["service"] for c in configs}):
        raw, sha = fetcher.post(KOFIA_BASE + "/meta/getSrvData.do",
                                {"dmSearchData": {"strSvrId": service, "strGetCode": "Y"}})
        metadata[service] = (raw, sha)
    # Two query workers at most; the independent SHFE downloader uses one request.
    with ThreadPoolExecutor(max_workers=2) as pool:
        for config in configs:
            frames, receipts = [], []
            fields = config["fields"]
            metadata_raw, metadata_sha = metadata[config["service"]]
            labels, info = kofia_headers(metadata_raw, fields)
            def request_year(year):
                left = max(start, f"{year}-01-01")
                right = min(str((pd.Timestamp(end) - pd.Timedelta(days=1)).date()), f"{year}-12-31")
                if right < left:
                    return None
                params = {**config["params"], "tmpV40": "1" if config["counts"] else "1000000", "tmpV41": "1",
                          "OBJ_NM": config["service"] + "BO"}
                if config["service"].startswith("STATSCU"):
                    params.update(tmpV45=left.replace("-", ""), tmpV46=right.replace("-", ""))
                else:
                    params.update(tmpV30=left.replace("-", ""), tmpV31=right.replace("-", ""))
                payload = {"dmSearch": params}
                raw, sha = fetcher.post(KOFIA_BASE + "/meta/getMetaDataList.do", payload, timeout=90)
                frame = parse_kofia(raw, fields, left, right, counts=config["counts"], signed_components=config["family"] == "fund_subscriptions_redemptions")
                time.sleep(0.8)
                return frame, {"sha256": sha, "start": left, "end": right, "query_payload": payload, "rows": len(frame)}
            try:
                for result in pool.map(request_year, range(pd.Timestamp(start).year, pd.Timestamp(end).year + 1)):
                    if result:
                        frames.append(result[0]); receipts.append(result[1])
                receipt_path = ROOT / "archive/asia6_kofia" / (config["key"] + ".json")
                atomic_bytes(receipt_path, json.dumps({"config": config, "metadata_sha256": metadata_sha,
                    "js_sha256": js_sha, "responses": receipts}, ensure_ascii=False, indent=2).encode())
                data = pd.concat(frames, ignore_index=True)
                manifest = archive_raw_manifest([metadata_sha, js_sha, *[r["sha256"] for r in receipts]],
                    description="KOFIA original daily financial-activity query responses, service metadata and official field-mapping JavaScript.")
                for field, label in fields.items():
                    def one_series():
                        return save("kr_kofia_" + config["key"] + "_" + token(label), clean(data.date, data[f"TMPV{field}"]),
                            title="South Korea: " + config["label"].replace("_", " ") + ": " + label.replace("_", " "),
                            provider="Korea Financial Investment Association FreeSIS", category="korean_" + config["family"],
                            frequency="daily", unit=("accounts" if config["key"].startswith("cma_") else "funds and contracts") if config["counts"] else "KRW million",
                            source_url=KOFIA_BASE + "/stat/FreeSIS.do?parentDivId=MSIS10000000000000&serviceId=" + config["service"],
                            country_codes=["KR"], geography="South Korean regulated financial-intermediary reporting universe; overseas investment labels describe underlying allocation, not foreign account-holder nationality.",
                            raw_sha256=[manifest], raw_manifest_sha256=[manifest],
                            dataset_identity="kofia:" + config["service"] + ":" + json.dumps(config["params"], sort_keys=True) + ":TMPV" + str(field),
                            source_metric="TMPV" + str(field), source_metric_label=labels[field], source_service_id=config["service"],
                            source_service_label=info["SERVICE_NM"], source_query_parameters=config["params"],
                            source_native_notes=BeautifulSoup(info.get("PRINCIPLE_ATC", ""), "html.parser").get_text(" "),
                            source_query_receipt_path=str(receipt_path.relative_to(ROOT)),
                            source_metadata_sha256=metadata_sha,
                            negative_native_observations=int(data[f"TMPV{field}"].lt(0).sum()),
                            hypothesis="Native reported fund subscriptions/redemptions, fund formation and committed capital, brokerage leverage, unsettled receivables, forced sales and cash-management deposits can reveal risk appetite, collateral pressure and financing constraints before slower macro releases.",
                            limitations="Coverage is the association's reporting intermediaries, not all Korean households or transactions. The credit-stock-lending component (신용거래대주) is brokerage margin short-sale credit, not all institutional securities lending. Accounts, funds and contracts are not unique people. Overseas allocations remain activity of Korean intermediaries. Fund flow statistics use confirmed/settlement activity and exclude reinvestment; subscription/redemption components occasionally contain negative source-reported amounts, retained unchanged with no assertion that these are positive gross economically executed cash. Their adjustment/correction reasons are unverified; fund capital/counts are operation-date balances and mandates, not market returns. The principal/contract measure is original subscription principal plus mandate contract amount, not NAV or unfunded private-market commitments. CMA balances are subscription principal (설정금액), not a measure of undrawn credit. Derivative deposits exclude futures-company deposits. Forced liquidation changes to executed-transaction amount from 2023-10-25. Historical data can revise or lag and the provider disclaims accuracy. No sector/company exposure inferred.",
                            expected_publication_lag="Published after the source settlement/operation day; exact historical first-publication clocks and uniform lag are not supplied.",
                            source_timezone="Asia/Seoul", native_frequency="daily Korean business-date reports",
                            aggregation="Native source metrics and public/private, asset-class, investment-region or customer-type selections. Signed source-reported domestic/overseas subscription/redemption components retained; totals, net changes and ratios excluded. Source unit selector requests millions of KRW or unscaled counts; no local rescaling.",
                            missing_policy="Only explicit numeric observations retained; native nulls and absent days omitted, explicit numeric zero retained. No calendar padding/interpolation.")
                    attempt(good, config["key"] + ":" + str(field), one_series)
                print("KOFIA query complete " + config["key"], flush=True)
            except Exception as exc:
                errors.append({"query": config["key"], "error": repr(exc)})
                print("KOFIA ERROR " + config["key"] + " " + repr(exc), flush=True)
                if fetcher.blocked_hosts:
                    break
    return good, errors


def collect(fetcher=None, start="2020-01-01", end=None, providers=None, workers=1):
    fetcher = fetcher or AsiaFetcher()
    end = end or pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
    good, errors = [], []
    for provider in providers or ["shfe_daily", "shfe_weekly", "kpx", "kofia"]:
        try:
            if provider.startswith("shfe_"):
                rows, bad = collect_shfe(fetcher, max(start, "2024-01-01"), end,
                                         weekly=provider == "shfe_weekly")
            elif provider == "kofia":
                rows, bad = collect_kofia(fetcher, start, end)
            else:
                rows, bad = collect_kpx(fetcher, start, end)
            good.extend(rows)
            errors.extend({"provider": provider, **e} for e in bad)
        except Exception as exc:
            errors.append({"provider": provider, "error": repr(exc)})
            print(f"ERROR {provider}: {exc!r}", flush=True)
    return good, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", action="append", choices=["shfe_daily", "shfe_weekly", "kpx", "kofia"])
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    rows, errors = collect(AsiaFetcher(refresh=args.refresh), args.start, args.end, args.provider, args.workers)
    report = {"series_count": len(rows), "series_ids": sorted(r["series_id"] for r in rows), "observations": sum(r["rows"] for r in rows),
              "provider_counts": pd.Series([r["provider"] for r in rows]).value_counts().to_dict(),
              "errors": errors, "omissions": OMISSIONS, "finished_at": pd.Timestamp.now(tz="UTC").isoformat()}
    suffix = "_".join(args.provider or ["all"])
    atomic_bytes(ROOT / "logs" / ("asia6_" + suffix + ".json"), json.dumps(report, ensure_ascii=False, indent=2).encode())
    print(json.dumps({k: v for k, v in report.items() if k not in {"omissions", "series_ids"}}, ensure_ascii=False, indent=2), flush=True)
    raise SystemExit(bool(errors))
