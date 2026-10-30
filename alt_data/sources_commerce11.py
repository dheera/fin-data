#!/usr/bin/env python3
"""Public CBRT sector card-spending histories; no credential or recurring job needed."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from pathlib import Path

import pandas as pd
import requests

from expansion_common import ROOT, Fetcher, atomic_bytes, normalize_frame, save_candidate

GROUP = "commerce11"
BASE = "https://evds3.tcmb.gov.tr/igmevdsms-dis"
GROUPS = {"bie_kkhartut": ("card_expenditure", "thousand TRY"),
          "bie_kkislade": ("card_transactions", "transactions")}


class PublicFetch:
    def __init__(self, root=ROOT, refresh=False):
        self.root, self.refresh = Path(root), refresh

    def get(self, url):
        return Fetcher(self.root, self.refresh).get(url)

    def post(self, endpoint, body):
        if endpoint not in {"/fe", "/serieNotes"}:
            raise ValueError("Unexpected public data endpoint")
        identity = {"method": "POST", "url": BASE + endpoint, "body": body}
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        index = self.root / "archive/requests" / (key + ".json")
        if index.exists() and not self.refresh:
            receipt = json.loads(index.read_text())
            age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(receipt["retrieved_at"])).total_seconds()
            path = self.root / "archive/raw" / (receipt["sha256"] + ".bin")
            if 0 <= age < 86400 and path.exists():
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() == receipt["sha256"]:
                    return raw, receipt["sha256"]
        for attempt in range(3):
            try:
                response = requests.post(BASE + endpoint, json=body, timeout=(15, 90),
                                         headers={"User-Agent": "FinAltDataResearch/1.0"})
                response.raise_for_status()
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        raw = response.content
        digest = hashlib.sha256(raw).hexdigest()
        receipt = dict(identity, sha256=digest, collector_group=GROUP,
                       retrieved_at=pd.Timestamp.now(tz="UTC").isoformat(),
                       content_type=response.headers.get("Content-Type"))
        atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), raw)
        atomic_bytes(index, json.dumps(receipt, indent=2).encode())
        name = key + "_" + pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S%fZ") + ".json"
        atomic_bytes(self.root / "archive/receipts" / name, json.dumps(receipt, indent=2).encode())
        return raw, digest


def query_body(codes, start, end):
    return {"type": "json", "series": "-".join(codes),
            "aggregationTypes": "-".join(["sum"] * len(codes)),
            "formulas": "-".join(["0"] * len(codes)),
            "startDate": pd.Timestamp(start).strftime("%d-%m-%Y"),
            "endDate": pd.Timestamp(end).strftime("%d-%m-%Y"),
            "frequency": "0", "decimalSeperator": ".", "decimal": "8",
            "dateFormat": "0", "lang": "en", "yon": 0, "sira": 0,
            "ozelFormuller": [], "groupSeperator": False, "isRaporSayfasi": False}


def parse_weekly(raw, code):
    obj = json.loads(raw)
    if not isinstance(obj.get("items"), list) or obj.get("totalCount") != len(obj["items"]):
        raise ValueError("Incomplete EVDS response")
    name = code.replace(".", "_")
    if name not in obj.get("seriesNames", {}):
        raise ValueError("Requested series missing")
    rows = []
    for item in obj["items"]:
        label = item["Tarih"]
        if not re.fullmatch(r"\d{2}-\d{2}-\d{4}", label) or "YEARWEEK" not in item:
            raise ValueError("Expected native weekly dates, not a frequency conversion")
        date = pd.to_datetime(label, format="%d-%m-%Y", utc=True)
        if date.weekday() != 4:
            raise ValueError("Expected Friday reference-period label")
        value = item.get(name)
        if value in (None, "", "ND", "N.D.", "null"):
            continue
        value = float(value)
        if value < 0:
            raise ValueError("Negative card expenditure/transaction count")
        # Preserve the provider's calendar date as a period label. The Unix clock
        # describes local midnight and is not a historical release timestamp.
        rows.append({"date": date, "value": value})
    return normalize_frame(rows)


def collect(refresh=False, root=ROOT):
    root = Path(root)
    fetch = PublicFetch(root, refresh)
    start, end = "2014-03-01", pd.Timestamp.now(tz="UTC").date().isoformat()
    report = {"group": GROUP, "started_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "files": [], "requests": [], "errors": [], "supplementary_files": []}
    catraw, cathash = fetch.get(BASE + "/categories/withDatagroups/type=json")
    groups = {d["DATAGROUP_CODE"]: d for c in json.loads(catraw) for d in c.get("DATAGROUPS", [])}
    for group, (measure, unit) in GROUPS.items():
        gm = groups[group]
        if gm["FREQUENCY_STR"] != "HAFTALIK(CUMA)":
            raise ValueError("Provider changed group cadence")
        sr, sh = fetch.get(BASE + "/serieList/fe/type=json&code=" + group)
        series = json.loads(sr)
        for offset in range(0, len(series), 10):
            selected = series[offset:offset + 10]
            codes = [s["SERIE_CODE"] for s in selected]
            nr, nh = fetch.post("/serieNotes", codes)
            notes = {r["seri"]["serieCode"]: r for r in json.loads(nr)}
            raw, h = fetch.post("/fe", query_body(codes, start, end))
            report["requests"].append({"series_codes": codes, "raw_sha256": h,
                                       "notes_raw_sha256": nh, "series_metadata_sha256": sh,
                                       "category_metadata_sha256": cathash})
            for sm in selected:
                code = sm["SERIE_CODE"]
                df = parse_weekly(raw, code)
                if measure == "card_transactions" and not df.value.mod(1).eq(0).all():
                    raise ValueError("Transaction counts must be integers")
                note = notes[code]
                docurl = note["seri"]["metaDataLinkEng"].strip()
                _, dochash = fetch.get(docurl)
                sid = "tr_cb11_" + code.lower().replace(".", "_")
                meta = save_candidate(GROUP, sid, df, root=root,
                    title="Türkiye weekly " + measure.replace("_", " ") + ": " + sm["SERIE_NAME_ENG"],
                    provider="Central Bank of the Republic of Türkiye — EVDS",
                    category="payments_and_cash_demand", unit=unit, frequency="weekly",
                    source_url="https://evds3.tcmb.gov.tr/", geography="Türkiye",
                    hypothesis="Sector spending and transaction volumes may reveal shifts in consumption, tourism, inflation pressure and domestic business activity.",
                    limitations="Latest revised historical observations, not first-release vintages. Nominal values, not seasonally adjusted. Domestic bank POS (including virtual POS), domestic and foreign cards used in Türkiye; cash withdrawals/advances excluded. Online shopping, mail/phone shopping and customs tax payments are informational categories excluded from headline total; categories are not all additive. No missing weeks filled.",
                    raw_sha256=[h, nh, sh, cathash, dochash],
                    dataset_identity="tcmb_evds|" + code + "|native_weekly|level",
                    collector_group=GROUP, native_frequency="weekly", finer_frequency_available=False,
                    geography_scope="non_us", country_codes=["TR"], country_code_standard="ISO 3166-1 alpha-2",
                    timestamp_semantics="Friday reference-week date labeled at 00:00 UTC for joins; not source publication time or local-midnight timestamp.",
                    expected_publication_lag="Normally following Thursday 14:30 Europe/Istanbul (about 6 calendar days after Friday label); holiday schedule exceptions. Source describes 4 working days; historical release clocks not reconstructed.",
                    publication_lag_basis="Official CBRT dataset metadata; schedule estimate, not row-level observed release times.",
                    source_documentation_url=docurl, source_series_code=code,
                    source_group_code=group, source_sector_name=sm["SERIE_NAME_ENG"],
                    source_note=(note.get("seriNotlari") or {}).get("noteEng"),
                    acquisition_method="Unauthenticated public EVDS3 frontend export, native frequency=0, formulas=0; same public data request as website.")
                p = root / "archive/staging" / GROUP / (sid + ".parquet")
                report["files"].append({"series_id": sid, "path": str(p.relative_to(root)),
                    "rows": meta["rows"], "start": meta["start"], "end": meta["end"],
                    "parquet_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                    "metadata_sha256": hashlib.sha256(p.with_suffix(".json").read_bytes()).hexdigest()})
            print(group, offset + len(selected), "/", len(series), flush=True)
    report.update(series_count=len(report["files"]), observations=sum(x["rows"] for x in report["files"]),
                  source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  completed_at=pd.Timestamp.now(tz="UTC").isoformat())
    atomic_bytes(root / "logs/commerce11_inventory.json", json.dumps(report, indent=2).encode())
    print(json.dumps({k: v for k, v in report.items() if k not in {"files", "requests"}}, indent=2))
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--refresh", action="store_true")
    args = p.parse_args()
    collect(args.refresh)
