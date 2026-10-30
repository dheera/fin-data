#!/usr/bin/env python3
"""One-shot component inventory/pricing pilot; current observations only, no schedule."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

import pandas as pd
import requests

from expansion_common import ROOT, atomic_bytes

URL = "https://api.mouser.com/api/v1/search/partnumber"
KEY_PATH = Path("/data/fin/api_keys/mouser.json")
# Fixed identifiers keep future observations comparable. Unmatched parts are
# omissions, never assumed out of stock. Different packaging stays distinct.
PARTS = ["STM32F103C8T6", "STM32H743VIT6", "ATMEGA328P-AU", "RP2040",
         "ESP32-WROOM-32E", "LM358DR", "ADS1115IDGSR", "ADXL345BCCZ",
         "MCP2562FD-E/SN", "INA219AIDCNR", "TPS5430DDAR", "LM2596S-ADJ/NOPB",
         "W25Q128JVSIQ", "IRLZ44NPBF", "IPB017N10N5ATMA1", "SBR10U45SP5-13",
         "GRM188R71H104KA93D", "RC0603FR-0710KL", "SN74LVC1G14DBVR",
         "PC817X2NSZ0F", "BSS138", "BME280", "USB4110-GF-A", "B3F-1000"]


def listing_eligibility(part):
    """A restricted/unidentified listing can carry default zero lead-time text."""
    identifier = str(part.get("MouserPartNumber") or "").strip()
    invalid = identifier.casefold() in {"", "n/a", "na", "n.a.", "none", "null", "-"}
    restriction = str(part.get("RestrictionMessage") or "").strip()
    reasons = []
    if invalid:
        reasons.append("Missing or placeholder distributor part identifier")
    if restriction:
        reasons.append("Provider supplied a restriction message")
    return {"invalid_distributor_id": invalid, "restricted_listing": bool(restriction),
            "numeric_observations_eligible": not reasons,
            "numeric_exclusion_reason": "; ".join(reasons) if reasons else None,
            "restriction_message": restriction or None}


def replace_processed_scopes(previous, incoming, scopes, keys):
    """Replace recomputed receipt/request scopes, including empty new results.

    A parser correction can retract an observation from the same response.
    Retrieval time is part of the scope, so equal bytes observed on older dates
    remain untouched. The requested part distinguishes response reuse.
    """
    prior = previous.copy()
    new = incoming.copy()
    normalized = {(pd.Timestamp(t), part, digest) for t, part, digest in scopes}
    if len(prior):
        processed = [(pd.Timestamp(row.collected_at), row.requested_part, row.raw_sha256) in normalized
                     for row in prior.itertuples(index=False)]
        prior = prior.loc[[not flag for flag in processed]].copy()
    if new.empty:
        return prior.reset_index(drop=True)
    frame = pd.concat([prior, new], ignore_index=True) if len(prior) else new
    return frame.drop_duplicates(keys, keep="last").sort_values(
        ["collected_at", "requested_part", "mouser_part_number"]).reset_index(drop=True)


def parse_numbers(part):
    """Parse only explicit units/quantities; retain other source text separately."""
    result = []
    stock_raw = part.get("AvailabilityInStock")
    stock = "" if stock_raw is None else str(stock_raw).strip()
    if not re.fullmatch(r"[\d,]+", stock):
        m = re.fullmatch(r"([\d,]+)\s+In Stock", str(part.get("Availability") or "").strip(), re.I)
        stock = m[1] if m else ""
    if re.fullmatch(r"\d{1,3}(?:,\d{3})*|\d+", stock):
        result.append(("stock", float(stock.replace(",", "")), "pieces", None))
    lead = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(Days?|Weeks?)", str(part.get("LeadTime") or "").strip(), re.I)
    if lead:
        result.append(("lead_time", float(lead[1]) * (7 if lead[2].lower().startswith("week") else 1), "days", None))
    for price in part.get("PriceBreaks") or []:
        value = str(price.get("Price") or "").strip()
        # Currency comes from the separate provider field; never infer from '$'.
        currency, quantity = price.get("Currency"), price.get("Quantity")
        # Only a documented currency label/symbol may precede a price. Never
        # strip a minus sign or text such as "Call for price" into a number.
        symbols = {"USD": ["$", "US$"], "CAD": ["$", "C$", "CA$"], "AUD": ["$", "A$", "AU$"],
                   "EUR": ["€"], "GBP": ["£"], "JPY": ["¥"], "CNY": ["¥"], "INR": ["₹"]}
        clean = value
        for prefix in sorted([currency] + symbols.get(currency, []) if currency else [], key=len, reverse=True):
            if clean.startswith(prefix):
                clean = clean[len(prefix):].strip()
                break
        if currency and isinstance(quantity, int) and quantity > 0 and re.fullmatch(r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?", clean):
            result.append(("unit_price", float(clean.replace(",", "")), currency + "/piece", quantity))
    return result


def collect(root=ROOT):
    root = Path(root)
    report = {"started_at": pd.Timestamp.now(tz="UTC").isoformat(), "provider": "Mouser Search API",
              "history_type": "prospective only", "scheduled": False,
              "requested_parts": PARTS, "omissions": [], "excluded_listings": [],
              "errors": [], "new_main_series": 0}
    if not KEY_PATH.exists():
        report.update(status="credential_required", signup="https://www.mouser.com/en/api-search/",
                      credential_file=str(KEY_PATH), reason="Search API requires its own free key.")
        atomic_bytes(root / "logs/mouser11_status.json", json.dumps(report, indent=2).encode())
        print("Mouser pilot ready; waiting for Search API credential.")
        return report
    key = json.loads(KEY_PATH.read_text())["api_key"]
    parts, measurements, scopes, requests_log = [], [], [], []
    today = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
    for number in PARTS:
        body = {"SearchByPartRequest": {"mouserPartNumber": number, "partSearchOptions": "Exact"}}
        identity = {"method": "POST", "url": URL, "body": body, "collection_date": today}
        rid = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        index = root / "archive/requests" / (rid + ".json")
        if index.exists():
            receipt = json.loads(index.read_text())
            raw = (root / "archive/raw" / (receipt["sha256"] + ".bin")).read_bytes()
            if hashlib.sha256(raw).hexdigest() != receipt["sha256"]:
                raise ValueError("Mouser cached response checksum mismatch")
        else:
            try:
                response = requests.post(URL, params={"apiKey": key}, json=body,
                                         timeout=(15, 60), allow_redirects=False)
            except requests.RequestException:
                raise RuntimeError("Mouser network request failed; credential-bearing details withheld") from None
            if response.status_code != 200:
                raise RuntimeError("Mouser request HTTP " + str(response.status_code))
            raw = response.content
            if key.encode() in raw:
                raise RuntimeError("Refused source response containing credential")
            obj = json.loads(raw)
            if obj.get("Errors"):
                raise RuntimeError("Mouser returned API errors; response details withheld")
            digest = hashlib.sha256(raw).hexdigest()
            receipt = dict(identity, sha256=digest, collector_group="mouser11",
                           retrieved_at=pd.Timestamp.now(tz="UTC").isoformat(), authentication="query credential omitted")
            atomic_bytes(root / "archive/raw" / (digest + ".bin"), raw)
            atomic_bytes(index, json.dumps(receipt, indent=2).encode())
            atomic_bytes(root / "archive/receipts" / (rid + ".json"), json.dumps(receipt, indent=2).encode())
            time.sleep(2.1)
        obj = json.loads(raw)
        if key.encode() in raw:
            raise RuntimeError("Refused cached or fresh response containing credential")
        if obj.get("Errors"):
            raise RuntimeError("Mouser returned API errors; response details withheld")
        scopes.append((receipt["retrieved_at"], number, receipt["sha256"]))
        requests_log.append({"request_id": rid, "requested_part": number,
                             "collected_at": receipt["retrieved_at"], "raw_sha256": receipt["sha256"]})
        found = (obj.get("SearchResults") or {}).get("Parts") or []
        total = (obj.get("SearchResults") or {}).get("NumberOfResult", len(found))
        if int(total) > len(found):
            raise ValueError("Mouser exact-part response truncated")
        matched = [p for p in found if str(p.get("ManufacturerPartNumber", "")).casefold() == number.casefold()
                   or str(p.get("MouserPartNumber", "")).casefold() == number.casefold()]
        if not matched:
            report["omissions"].append({"requested_part": number, "reason": "No exact part returned; not a zero-stock observation"})
        for part in matched:
            base = {"collected_at": pd.Timestamp(receipt["retrieved_at"]), "requested_part": number,
                    "mouser_part_number": part.get("MouserPartNumber"),
                    "manufacturer_part_number": part.get("ManufacturerPartNumber"),
                    "manufacturer": part.get("Manufacturer"), "raw_sha256": receipt["sha256"]}
            eligibility = listing_eligibility(part)
            parts.append(dict(base, **eligibility, source_fields_json=json.dumps(part, sort_keys=True)))
            if not eligibility["numeric_observations_eligible"]:
                report["excluded_listings"].append(dict(requested_part=number,
                    mouser_part_number=part.get("MouserPartNumber"),
                    manufacturer_part_number=part.get("ManufacturerPartNumber"),
                    manufacturer=part.get("Manufacturer"), **eligibility))
                continue
            for measure, value, unit, quantity in parse_numbers(part):
                measurements.append(dict(base, measure=measure, value=value, unit=unit, price_break_quantity=quantity))
    outputs = []
    part_keys = ["collected_at", "requested_part", "mouser_part_number", "manufacturer_part_number", "manufacturer"]
    for name, rows, keys in [("parts", parts, part_keys),
                             ("measurements", measurements, part_keys + ["measure", "unit", "price_break_quantity"])]:
        p = root / "supplementary/mouser11" / (name + ".parquet")
        if not rows and not p.exists():
            continue
        previous = pd.read_parquet(p) if p.exists() else pd.DataFrame()
        frame = replace_processed_scopes(previous, pd.DataFrame(rows), scopes, keys)
        atomic_bytes(p, frame.to_parquet(index=False, compression="zstd"))
        outputs.append({"path": str(p.relative_to(root)), "rows": len(frame), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    report.update(status="complete", collected_parts=len(parts), numeric_observations=len(measurements), files=outputs,
                  numeric_eligible_listings=sum(x["numeric_observations_eligible"] for x in parts),
                  requests=requests_log, source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  completed_at=pd.Timestamp.now(tz="UTC").isoformat(),
                  limitations="Current distributor inventory is neither total manufacturer stock nor demand. Snapshot retrieval time is actual local availability. No historical observations created. Currency, price-break quantity and packaging retained; unknown text is not coerced to zero. Restricted listings and missing/placeholder distributor IDs remain in the part table with explicit flags but contribute no numeric observations. Reprocessed receipt/request scopes replace their old rows, preserving observations from prior retrieval times.")
    atomic_bytes(root / "logs/mouser11_status.json", json.dumps(report, indent=2).encode())
    print(json.dumps({k: v for k, v in report.items() if k != "requested_parts"}, indent=2))
    return report


if __name__ == "__main__":
    collect()
