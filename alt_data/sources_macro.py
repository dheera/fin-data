#!/usr/bin/env python3
"""Daily fiscal cash flows, funding, financial stress, policy attention and maritime trade."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import re

import pandas as pd

from expansion_common import ROOT, Fetcher, atomic_bytes, save_candidate


def slug(text):
    return re.sub("[^a-z0-9]+", "_", text.lower()).strip("_")


def save(sid, dates, values, **meta):
    values = pd.Series(values).replace({"null": None, ".": None, "": None})
    frame = pd.DataFrame({"date": dates, "value": pd.to_numeric(values, errors="raise")}).dropna()
    return save_candidate("macro", sid, frame, **meta)


def ofr(fetch):
    url = "https://www.financialresearch.gov/financial-stress-index/data/fsi.csv"
    content, digest = fetch.get(url)
    data = pd.read_csv(io.BytesIO(content))
    result = []
    for column in data.columns[1:]:
        result.append(save(
            "ofr_fsi_" + slug(column), data["Date"], data[column],
            title="OFR financial stress: " + column, provider="Office of Financial Research",
            category="financial_stress", unit="index contribution" if column != "OFR FSI" else "index",
            frequency="business_daily", source_url=url, geography=column if column in
            {"United States", "Other advanced economies", "Emerging markets"} else "Global",
            hypothesis="Funding and risk-aversion stress may affect credit spreads, equity risk premia and safe-haven demand.",
            limitations="Published composite or component contribution, not an independent underlying variable. Latest revised history; publisher reports a two-business-day publication lag. Components overlap with the overall index.",
            raw_sha256=digest, dataset_identity="ofr:financial-stress-index:" + column,
            documentation_url="https://www.financialresearch.gov/financial-stress-index/",
            native_frequency="business_daily", expected_publication_lag="2 business days"))
    return result


def nyfed(fetch):
    url = "https://markets.newyorkfed.org/api/rates/all/search.json"
    content, digest = fetch.get(url, {"startDate": "2020-01-01", "endDate": str(pd.Timestamp.now(tz="UTC").date())})
    data = pd.DataFrame(json.loads(content)["refRates"])
    result = []
    names = {"EFFR": "Effective federal funds", "OBFR": "Overnight bank funding",
             "SOFR": "Secured overnight financing", "TGCR": "Tri-party general collateral",
             "BGCR": "Broad general collateral"}
    for kind, title in names.items():
        group = data[data["type"] == kind]
        if group["effectiveDate"].duplicated().any():
            raise ValueError("NYFed duplicate effective dates")
        for field, label, unit in [("percentRate", "rate", "percent"), ("volumeInBillions", "volume", "USD billion")]:
            result.append(save(
                f"nyfed_{kind.lower()}_{label}", group["effectiveDate"], group[field],
                title=f"{title} {label}", provider="Federal Reserve Bank of New York",
                category="money_markets", unit=unit, frequency="business_daily", source_url=url,
                geography="United States", hypothesis="Secured versus unsecured financing demand may reveal bank liquidity, collateral scarcity and monetary-policy transmission.",
                limitations="Effective date labels; publication follows the underlying transaction day. Overlapping reference-rate universes are not independent. Latest revisions retained, no synthetic weekend values.",
                raw_sha256=digest, dataset_identity=f"nyfed:rates:{kind}:{field}",
                documentation_url="https://markets.newyorkfed.org/static/docs/markets-api.html",
                native_frequency="business_daily"))
    return result


# Deliberately selected distinct fiscal channels, not arbitrary copies of all Treasury fields.
FISCAL = [
    ("Deposits", "DHS - Customs Duties, Taxes, and Fees", "customs_receipts", "trade_tariffs", "Import activity and tariff incidence may affect importers, retailers and inflation."),
    ("Deposits", "Taxes - Corporate Income", "corporate_tax_receipts", "tax_receipts", "Corporate tax payments may provide a noisy check on profits and cash generation."),
    ("Deposits", "Taxes - Withheld Individual/FICA", "payroll_tax_receipts", "tax_receipts", "Payroll withholding may track aggregate employment and wage income."),
    ("Deposits", "Taxes - Non Withheld Ind/SECA Electronic", "self_employed_tax_receipts", "tax_receipts", "Estimated income-tax payments may reflect investment income and self-employment."),
    ("Deposits", "Taxes - Miscellaneous Excise", "excise_tax_receipts", "tax_receipts", "Excise receipts may reflect taxed consumption and industrial activity."),
    ("Deposits", "United States Postal Service (USPS)", "postal_receipts", "postal_activity", "Postal cash receipts may reflect shipping demand and commerce."),
    ("Deposits", "DOI - Gas and Oil Lease Sales Proceeds", "oil_gas_lease_receipts", "energy_leasing", "Federal oil and gas leasing payments may track producer appetite and investment."),
    ("Deposits", "HUD - Federal Housing Admin (FHA)", "fha_receipts", "housing_finance", "Mortgage-insurance cash flows may reflect mortgage originations and housing turnover."),
    ("Deposits", "Dept of Education (ED)", "education_receipts", "education_finance", "Education-related repayments may affect consumer disposable income."),
    ("Deposits", "Small Business Administration (SBA)", "sba_receipts", "small_business", "Small-business loan repayments may reflect business financial health."),
    ("Withdrawals", "USDA - Commodity Credit Corporation", "farm_support_payments", "agriculture_support", "Farm support disbursements may affect producer liquidity and agricultural input demand."),
    ("Withdrawals", "USDA - Federal Crop Insurance Corp Fund", "crop_insurance_payments", "agriculture_support", "Crop insurance disbursements may reveal weather losses and farm cash-flow support."),
    ("Withdrawals", "USDA - Supp Nutrition Assist Prog (SNAP)", "snap_payments", "consumer_transfers", "Food-assistance disbursements may affect grocery spending timing."),
    ("Withdrawals", "Dept of Defense (DoD) - misc", "defense_payments", "defense_spending", "Defense cash outlays may influence contractor cash receipts."),
    ("Withdrawals", "HHS - Federal Hospital Insr Trust Fund", "medicare_hospital_payments", "healthcare_spending", "Hospital insurance payments may inform healthcare utilization and provider cash flows."),
    ("Withdrawals", "HHS - Grants to States for Medicaid", "medicaid_payments", "healthcare_spending", "Medicaid cash flows may affect managed-care and healthcare provider revenues."),
    ("Withdrawals", "HHS - Medicare Prescription Drugs", "medicare_drug_payments", "healthcare_spending", "Prescription-drug disbursements may inform drug spending and reimbursement."),
    ("Withdrawals", "DHS - Fed Emergency Mgmt Agency (FEMA)", "fema_payments", "disaster_recovery", "Disaster relief payments may precede reconstruction and building-material demand."),
    ("Withdrawals", "DOL - Unemployment Benefits", "unemployment_payments", "labor_income", "Benefit disbursements may reflect labor-market stress and consumer income support."),
    ("Withdrawals", "DOT - Federal Highway Administration", "highway_payments", "infrastructure_spending", "Highway disbursements may inform construction, aggregate and equipment demand."),
    ("Withdrawals", "DOT - Federal Transit Administration", "transit_grants", "infrastructure_spending", "Transit grants may affect equipment orders and civil construction."),
    ("Withdrawals", "DOT - Federal Aviation Administration", "aviation_payments", "infrastructure_spending", "Aviation infrastructure payments may inform airport and systems investment."),
    ("Withdrawals", "Taxes - Individual Tax Refunds (EFT)", "individual_tax_refunds", "consumer_transfers", "Refund timing may affect retail, travel and discretionary spending."),
    ("Withdrawals", "Dept of Energy (DOE)", "energy_department_payments", "energy_investment", "Energy program cash flows may affect nuclear, research and grid contractors."),
    ("Withdrawals", "Dept of Veterans Affairs (VA)", "veterans_payments", "healthcare_spending", "Veterans spending may affect healthcare and household transfers."),
    ("Withdrawals", "Federal Salaries (EFT)", "federal_payroll", "labor_income", "Federal pay timing and disruptions may affect regional consumption."),
    ("Withdrawals", "Interest on Treasury Securities", "treasury_interest_payments", "fiscal_liquidity", "Coupon payments move cash between Treasury and investors and may influence liquidity."),
    ("Withdrawals", "SSA - Benefits Payments", "social_security_payments", "consumer_transfers", "Benefit payment timing may affect household spending."),
    ("Withdrawals", "NASA", "space_program_payments", "space_industry", "Space program spending may inform aerospace contractor cash flow."),
    ("Withdrawals", "US Army Corps of Engineers", "army_corps_payments", "infrastructure_spending", "Civil works spending may track waterway, flood protection and construction demand."),
]


def fiscal_one(fetch, spec):
    direction, category, sid, sector, hypothesis = spec
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/deposits_withdrawals_operating_cash"
    params = {
        "filter": f"record_date:gte:2024-01-01,account_type:eq:Treasury General Account (TGA),transaction_type:eq:{direction},transaction_catg:eq:{category}",
        "fields": "record_date,transaction_today_amt,transaction_type,transaction_catg",
        "sort": "record_date", "page[size]": 10000,
    }
    content, digest = fetch.get(url, params)
    payload = json.loads(content)
    if payload["meta"]["total-pages"] != 1:
        raise ValueError("Treasury results need pagination")
    data = pd.DataFrame(payload["data"])
    if data.empty:
        raise ValueError(f"No observations: {category}")
    return [save(
        "treasury_" + sid, data["record_date"], data["transaction_today_amt"],
        title=f"US Treasury {direction.lower()}: {category}", provider="US Treasury Fiscal Data",
        category=sector, unit="USD million", frequency="business_daily", source_url=url,
        geography="United States", hypothesis=hypothesis,
        limitations="Actual daily cash flows on modified cash basis; source rounding to millions, timing, calendar effects and agency reclassifications matter. Not accrual revenue, spending commitments, or historical release-time data. Source zeros retained; weekends not filled.",
        raw_sha256=digest, dataset_identity=f"treasury:dts:{direction}:{category}",
        documentation_url="https://fiscal.treasury.gov/accounting/daily-treasury-statement",
        native_frequency="business_daily", source_query=params)]


def cash(fetch):
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/operating_cash_balance"
    content, digest = fetch.get(url, {"filter": "record_date:gte:2024-01-01", "sort": "record_date", "page[size]": 10000})
    payload = json.loads(content)
    if payload["meta"]["total-pages"] != 1:
        raise ValueError("Cash data needs pagination")
    data = pd.DataFrame(payload["data"])
    selected = data[data["account_type"] == "Treasury General Account (TGA) Closing Balance"]
    return [save(
        "treasury_tga_closing_balance", selected["record_date"], selected["open_today_bal"],
        title="Treasury General Account closing cash balance", provider="US Treasury Fiscal Data",
        category="fiscal_liquidity", unit="USD million", frequency="business_daily",
        source_url=url, geography="United States",
        hypothesis="Treasury cash accumulation and drawdowns may affect bank reserves and short-term funding conditions.",
        limitations="Post-2022 Treasury table encodes the selected closing-balance row in open_today_bal; use the row label, not the misleading column name. Business-day actual balances; not historical release timestamps.",
        raw_sha256=digest, dataset_identity="treasury:dts:tga:closing",
        documentation_url="https://fiscal.treasury.gov/accounting/daily-treasury-statement",
        native_frequency="business_daily")]


def policy(fetch):
    result = []
    for country, filename in [("United States", "All_Daily_Policy_Data.csv"), ("United Kingdom", "UK_Daily_Policy_Data.csv")]:
        url = "https://www.policyuncertainty.com/media/" + filename
        content, digest = fetch.get(url)
        data = pd.read_csv(io.BytesIO(content))
        data.columns = [c.strip().lower() for c in data.columns]
        date = pd.to_datetime(data[["year", "month", "day"]])
        values = [c for c in data.columns if c not in {"year", "month", "day"}]
        if len(values) != 1:
            raise ValueError(f"Unexpected policy columns: {data.columns}")
        result.append(save(
            "epu_" + slug(country), date, data[values[0]], title=country + " daily economic policy uncertainty",
            provider="Economic Policy Uncertainty researchers", category="policy_uncertainty",
            unit="normalized news index", frequency="daily", source_url=url, geography=country,
            hypothesis="Policy uncertainty news intensity may affect capital spending, volatility and risk appetite.",
            limitations="Newspaper text-count index, not an exogenous shock or demonstrated trading signal. News coverage and normalization can change; latest revised source history.",
            raw_sha256=digest, dataset_identity="epu:daily:" + country,
            documentation_url="https://www.policyuncertainty.com/uk_daily.html" if country == "United Kingdom" else "https://www.policyuncertainty.com/us_daily.html",
            native_frequency="daily"))
    return result


def canada(fetch):
    url = "https://www.bankofcanada.ca/valet/observations/group/CORRA/json"
    content, digest = fetch.get(url, {"start_date": "2020-01-01"})
    payload = json.loads(content)
    result = []
    for code, detail in payload["seriesDetail"].items():
        if code not in {"AVG.INTWO", "CORRA_TOTAL_VOLUME", "CORRA_NUMBER_OF_SUBMITTERS",
                        "CORRA_RATE_AT_PERCENTILE_5", "CORRA_RATE_AT_PERCENTILE_95"}:
            continue
        metadata = detail.get("metadata", {})
        if "daily" not in metadata.get("frequency", "").lower():
            continue
        points = [(r["d"], r[code]["v"]) for r in payload["observations"] if r.get(code, {}).get("v") not in {None, ""}]
        if not points:
            continue
        result.append(save(
            "boc_" + slug(code), [x[0] for x in points], [x[1] for x in points],
            title=detail["label"], provider="Bank of Canada", category="canadian_funding",
            unit=metadata.get("unit", "see source"), frequency="business_daily", source_url=url,
            geography="Canada", hypothesis="Canadian secured funding activity and transaction distribution may affect CAD rates and collateral markets.",
            limitations=detail.get("description", "") + " Latest published reference-rate history, not executable prices. No weekend filling. See publisher terms.",
            raw_sha256=digest, dataset_identity="boc:valet:" + code,
            documentation_url="https://www.bankofcanada.ca/valet/docs/",
            native_frequency="business_daily"))
    return result


PORT_BASE = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0"


def portwatch(fetch):
    url = PORT_BASE + "/query"
    content, _ = fetch.get(url, {"f": "json", "where": "1=1", "outFields": "portid,portname",
                               "returnDistinctValues": "true", "returnGeometry": "false"})
    ports = [r["attributes"] for r in json.loads(content)["features"]]
    result = []
    for port in sorted(ports, key=lambda p: p["portid"]):
        pages, hashes = [], []
        offset = 0
        while True:
            content, digest = fetch.get(url, {
                "f": "json", "where": f"portid='{port['portid']}' AND date >= '2022-01-01'",
                "outFields": "date,portid,portname,n_total,n_tanker", "returnGeometry": "false",
                "orderByFields": "date,ObjectId", "resultOffset": offset, "resultRecordCount": 1000,
            })
            payload = json.loads(content)
            if "error" in payload:
                raise ValueError(payload["error"])
            records = [x["attributes"] for x in payload["features"]]
            if not records:
                break
            pages.extend(records)
            hashes.append(digest)
            if not payload.get("exceededTransferLimit"):
                break
            offset += len(records)
        data = pd.DataFrame(pages)
        for field, label in [("n_total", "tracked cargo and tanker transits"), ("n_tanker", "tanker transits")]:
            result.append(save(
                f"portwatch_{slug(port['portname'])}_{field}", data["date"], data[field],
                title=f"{port['portname']}: {label}", provider="IMF PortWatch",
                category="maritime_chokepoints", unit="vessel transits", frequency="daily",
                source_url=PORT_BASE, geography=port["portname"],
                hypothesis="Chokepoint disruption and tanker rerouting may affect energy availability, freight rates, delivery times and inventory demand.",
                limitations="AIS-derived modeled transit estimates, not customs trade value. Detection and vessel classification errors, recent-data lag and revisions are possible. Total transits include tankers; measures are related.",
                raw_sha256=hashes, dataset_identity=f"imf:portwatch:{port['portid']}:{field}",
                documentation_url="https://portwatch.imf.org/",
                native_frequency="daily"))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--provider", choices=["ofr", "nyfed", "treasury", "policy", "canada", "portwatch"])
    args = parser.parse_args()
    fetch = Fetcher(refresh=args.refresh)
    jobs = [(name, lambda fn=fn: fn(fetch)) for name, fn in [
        ("ofr", ofr), ("nyfed", nyfed), ("policy", policy), ("canada", canada), ("portwatch", portwatch)
    ] if args.provider in {None, name}]
    if args.provider in {None, "treasury"}:
        jobs += [("treasury_" + s[2], lambda s=s: fiscal_one(fetch, s)) for s in FISCAL]
        jobs.append(("treasury_cash", lambda: cash(fetch)))
    report = {"successes": [], "failures": []}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fn): name for name, fn in jobs}
        for future in as_completed(futures):
            name = futures[future]
            try:
                items = future.result()
                report["successes"].extend(x["series_id"] for x in items)
                print(f"OK {name}: {len(items)} datasets", flush=True)
            except Exception as exc:
                report["failures"].append({"collector": name, "error": str(exc)})
                print(f"FAILED {name}: {exc}", flush=True)
    atomic_bytes(ROOT / "logs/macro_collection.json", json.dumps(report, indent=2).encode())
    print(f"Staged {len(report['successes'])} datasets; {len(report['failures'])} failures")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
