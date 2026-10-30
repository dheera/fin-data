#!/usr/bin/env python3
"""New daily financing, collateral, corporate filing and euro money-market histories."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import re
import time
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd

from expansion_common import ROOT, Fetcher, atomic_bytes, coverage, normalize_frame, qualifies_daily, save_candidate


START = "2024-01-01"
GROUP = "macro2"


def slug(text):
    return re.sub("[^a-z0-9]+", "_", text.lower()).strip("_")


def accepted(sid, points, **meta):
    frame = pd.DataFrame(points, columns=["date", "value"])
    frame["value"] = pd.to_numeric(frame["value"], errors="raise")
    frame = frame.dropna(subset=["value"])
    if frame.empty:
        return None
    frame = normalize_frame(frame)
    if frame.value.nunique() < 2:
        return None
    if not qualifies_daily(dict(coverage(frame), frequency=meta["frequency"])):
        return None
    item = save_candidate(GROUP, sid, frame, **meta)
    print(f"OK {sid}: {item['rows']} observations", flush=True)
    return item


def parse_cp_zip(content, start=START):
    """Read genuine business-day observations; never use weekly outstanding amounts."""
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        with archive.open("CP_data.xml") as stream:
            for _, element in ET.iterparse(stream, events=["end"]):
                if not element.tag.endswith("}Series"):
                    continue
                family = element.tag.split("}")[0].split("/")[-1]
                attrs = dict(element.attrib)
                if family not in {"CP_VOL", "CP_RATES"} or attrs["FREQ"] != "9":
                    element.clear()
                    continue
                points = []
                for obs in element:
                    if not obs.tag.endswith("}Obs"):
                        continue
                    if obs.attrib["OBS_STATUS"] != "A" or obs.attrib["TIME_PERIOD"] < start:
                        continue
                    value = float(obs.attrib["OBS_VALUE"])
                    if value == -9999:
                        raise ValueError("Valid CP observation has missing sentinel")
                    points.append((obs.attrib["TIME_PERIOD"], value))
                descriptions = [e.text for e in element.iter() if e.tag.endswith("}AnnotationText") and e.text]
                element.clear()
                yield attrs, family, descriptions, points


def commercial_paper(fetch):
    url = "https://www.federalreserve.gov/releases/cp/data/FRB_CP_xml.zip"
    content, digest = fetch.get(url)
    result = []
    for attrs, family, descriptions, points in parse_cp_zip(content):
        if not points:
            continue
        multiplier = float(attrs["UNIT_MULT"])
        if attrs["UNIT"] == "Currency" and attrs["CURRENCY"] == "USD":
            if multiplier != 1_000_000:
                raise ValueError(f"Unrecognized CP dollar multiplier: {attrs}")
            unit = "USD million"
        elif attrs["UNIT"] == "Percent" and multiplier == 1:
            unit = "percent"
        elif multiplier == 1 and attrs["UNIT"] in {"Number", "Count"}:
            unit = "issues"
        else:
            raise ValueError(f"Unrecognized CP units: {attrs}")
        item = accepted(
            "frb_cp_" + slug(attrs["SERIES_NAME"]), points,
            title="Commercial paper: " + descriptions[0], provider="Federal Reserve commercial paper",
            category="corporate_funding_issuance" if family == "CP_VOL" else "corporate_funding_rates",
            unit=unit, frequency="business_daily", source_url=url, geography="United States",
            hypothesis="Issuer quality and maturity-specific funding activity may reveal rollover pressure, liquidity demand and credit conditions affecting corporate borrowers.",
            limitations="Latest revised DTCC-derived Federal Reserve statistics. Issuance amount/count and fitted rates are related measures. Source missing/suppressed observations are omitted; no weekend values. Daily issuance is distinct from the excluded weekly outstanding series.",
            raw_sha256=digest, dataset_identity="frb:CP:" + attrs["SERIES_NAME"],
            native_frequency="business_daily",
            documentation_url="https://www.federalreserve.gov/releases/cp/about.htm",
            source_definition=attrs,
            expected_publication_lag="Usually one business day",
        )
        if item:
            result.append(item)
    return result


def latest_repo_points(preliminary, final):
    """Final records override preliminary ones, including disclosure-edited nulls."""
    observations = dict(preliminary)
    observations.update(dict(final))
    return sorted(observations.items())


def repo(fetch):
    url = "https://data.financialresearch.gov/v1/series/dataset"
    content, digest = fetch.get(url, {"dataset": "repo", "start_date": START})
    payload = json.loads(content)
    result = []
    for key, series in payload["timeseries"].items():
        # Preliminary and final are versions of one indicator, not two new datasets.
        if not key.endswith("-F"):
            continue
        meta = series["metadata"]
        if meta["schedule"]["observation_frequency"].lower() != "daily":
            raise ValueError("OFR unexpected observation frequency")
        final = series["timeseries"]["aggregation"]
        preliminary = payload["timeseries"].get(key.removesuffix("-F") + "-P", {}).get(
            "timeseries", {}).get("aggregation", [])
        points = latest_repo_points(preliminary, final)
        description = meta["description"]
        unit = meta["unit"]["name"]
        magnitude = meta["unit"]["magnitude"]
        # The API's magnitude describes how values are represented.
        unit_label = unit if magnitude == 0 else f"{unit} (10^{magnitude})"
        subtype = description.get("subtype", "")
        category = "repo_rates" if "Rate" in subtype else "repo_collateral_activity"
        item = accepted(
            "ofr_repo_" + slug(key.removesuffix("-F")), points,
            title=description["name"].replace(" (Final)", ""), provider="OFR repo markets", category=category,
            unit=unit_label, frequency="business_daily", source_url=url, geography="United States",
            hypothesis="Collateral mix, repo tenor and financing volumes may reveal secured funding pressure, balance-sheet constraints and demand for safe collateral.",
            limitations=description.get("description", "") + " " + description.get("notes", "")
            + " One history per indicator: final observations override preliminary observations, including null disclosure edits; preliminary observations are retained where final records are unavailable. Missing disclosure-edited dates remain absent. Components are related and can be nested; these are research statistics, not executable quotes.",
            raw_sha256=digest, dataset_identity="ofr:repo:" + key.removesuffix("-F"),
            native_frequency="business_daily", source_metadata=meta,
            final_through=max((x[0] for x in final), default=None),
            retracted_dates=[date for date, value in final if value is None],
            expected_publication_lag="Preliminary: generally 1 business day for centrally cleared repo, 2 for tri-party. Final: quarterly release for the prior quarter.",
            availability_documentation_url="https://www.financialresearch.gov/short-term-funding-monitor/documentation/",
            documentation_url="https://www.financialresearch.gov/short-term-funding-monitor/datasets/repo/",
        )
        if item:
            result.append(item)
    return result


def sponsored_repo(fetch):
    url = "https://data.financialresearch.gov/hf/v1/series/dataset"
    content, digest = fetch.get(url, {"dataset": "ficc", "start_date": START})
    result = []
    for key, series in json.loads(content)["timeseries"].items():
        meta = series["metadata"]
        item = accepted(
            "ofr_" + slug(key), series["timeseries"]["aggregation"],
            title=meta["description"]["name"], provider="OFR sponsored repo", category="repo_collateral_activity",
            unit=meta["unit"]["name"], frequency="business_daily", source_url=url, geography="United States",
            hypothesis="Sponsored repo participation may reflect leveraged financing demand and Treasury-market intermediation.",
            limitations="FICC sponsored financing is a subset of the broader repo market; related to aggregate repo indicators. Daily source observations, latest revised history; historical publication timestamps unavailable.",
            raw_sha256=digest, dataset_identity="ofr:ficc:" + key,
            native_frequency="business_daily", source_metadata=meta,
            documentation_url="https://www.financialresearch.gov/hedge-fund-monitor/",
        )
        if item:
            result.append(item)
    return result


def estr(fetch):
    url = "https://data-api.ecb.europa.eu/service/data/EST/B.EU000A2X2A25."
    content, digest = fetch.get(url, {"format": "csvdata", "startPeriod": START})
    data = pd.read_csv(io.BytesIO(content))
    result = []
    units = {"NB": "banks", "NT": "transactions", "TT": "EUR million",
             "R25": "percent", "R75": "percent", "VL": "percent", "WT": "percent"}
    for key, frame in data.groupby("DATA_TYPE_EST"):
        if key not in units:
            continue
        if frame.TIME_PERIOD.duplicated().any():
            raise ValueError("ECB duplicate observation periods")
        metadata = frame.iloc[-1]
        if key == "TT" and (metadata.UNIT_MEASURE != "EUR" or metadata.UNIT_MULT != 6):
            raise ValueError("ECB volume units changed")
        item = accepted(
            "ecb_estr_" + key.lower(), list(zip(frame.TIME_PERIOD, frame.OBS_VALUE)),
            title=metadata.TITLE, provider="European Central Bank ESTR",
            category="euro_funding", unit=units[key], frequency="business_daily", source_url=url,
            geography="Euro area",
            hypothesis="Euro unsecured borrowing volumes, concentration and rate dispersion may reveal banking liquidity and monetary-policy transmission.",
            limitations="ECB benchmark and supporting transaction statistics; effective-date labels precede publication. Percentiles are related to the aggregate benchmark. Calculation-method and publication-status flags are excluded.",
            raw_sha256=digest, dataset_identity="ecb:" + metadata.KEY,
            native_frequency="business_daily", expected_publication_lag="Next TARGET business day",
            documentation_url="https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_short-term_rate/html/index.en.html",
        )
        if item:
            result.append(item)
    return result


SEC_FORMS = {
    "4": "Insider ownership transactions",
    "4/A": "Insider transaction amendments",
    "3": "Initial insider ownership",
    "5": "Annual insider ownership reporting",
    "8-K": "Current corporate events",
    "8-K/A": "Current corporate event amendments",
    "6-K": "Foreign issuer current reports",
    "10-K": "Annual corporate reports",
    "10-K/A": "Annual report amendments",
    "10-Q": "Quarterly corporate reports",
    "10-Q/A": "Quarterly report amendments",
    "20-F": "Foreign issuer annual reports",
    "S-1": "Initial securities registrations",
    "S-1/A": "Initial registration amendments",
    "S-3": "Shelf securities registrations",
    "S-3/A": "Shelf registration amendments",
    "S-8": "Employee benefit securities registrations",
    "F-1": "Foreign initial securities registrations",
    "F-1/A": "Foreign registration amendments",
    "F-3": "Foreign shelf securities registrations",
    "424B2": "Structured security prospectuses",
    "424B3": "Prospectus updates",
    "424B5": "Shelf offering prospectuses",
    "FWP": "Free-writing prospectuses",
    "D": "Private offering notices",
    "D/A": "Private offering notice amendments",
    "DEF 14A": "Definitive proxy statements",
    "DEFA14A": "Additional definitive proxy materials",
    "DEFM14A": "Merger proxy statements",
    "PRE 14A": "Preliminary proxy statements",
    "SC 13G": "Passive beneficial ownership disclosures",
    "SC 13G/A": "Passive ownership disclosure amendments",
    "SC 13D": "Active beneficial ownership disclosures",
    "SC 13D/A": "Active ownership disclosure amendments",
    "13F-HR": "Institutional investment holdings reports",
    "13F-HR/A": "Institutional holdings report amendments",
    "N-PORT-P": "Public investment fund portfolio reports",
    "N-CSR": "Investment company shareholder reports",
    "N-CEN": "Investment company census",
    "485BPOS": "Effective fund registration amendments",
    "485APOS": "Fund registration amendments",
    "497": "Investment company prospectuses",
    "497K": "Fund summary prospectuses",
    "NT 10-K": "Late annual report notices",
    "NT 10-Q": "Late quarterly report notices",
    "144": "Proposed restricted securities sales",
    "CORRESP": "Issuer correspondence",
    "UPLOAD": "SEC staff correspondence",
}


def parse_sec_index(content):
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        raw = archive.read("master.idx").decode("latin1")
    lines = raw.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("CIK|Company Name|Form Type|Date Filed|Filename"))
    records = [line.split("|") for line in lines[start + 2:] if line.strip()]
    if any(len(record) != 5 for record in records):
        raise ValueError("SEC index row layout changed")
    return pd.DataFrame(records, columns=["cik", "company", "form", "date", "filename"])


def canonical_sec_records(data):
    """A submission may legitimately be indexed under several form types."""
    data = data.copy()
    data["form"] = data["form"].replace({
        "SCHEDULE 13G": "SC 13G", "SCHEDULE 13G/A": "SC 13G/A",
        "SCHEDULE 13D": "SC 13D", "SCHEDULE 13D/A": "SC 13D/A",
    })
    if (data.groupby(["filename", "form"])["date"].nunique() > 1).any():
        raise ValueError("SEC accession/form has conflicting filing dates")
    return data.drop_duplicates(["filename", "form"])


def sec_category(form):
    if form.startswith(("S-", "F-", "424")) or form in {"FWP", "D", "D/A"}:
        return "capital_formation"
    if form.startswith(("N-", "485", "497")):
        return "fund_disclosures"
    if form.startswith(("SC ", "13F")) or form in {"3", "4", "4/A", "5", "144"}:
        return "ownership_disclosures"
    return "corporate_disclosures"


def sec(fetch):
    frames, hashes = [], []
    now = pd.Timestamp.now(tz="UTC")
    for year in range(2024, now.year + 1):
        for quarter in range(1, 5):
            if (year, quarter) > (now.year, now.quarter):
                break
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.zip"
            content, digest = fetch.get(url, headers={"User-Agent": "FinAltDataResearch/1.0 public economic research"}, timeout=90)
            frames.append(parse_sec_index(content))
            hashes.append(digest)
            time.sleep(0.4)
    data = canonical_sec_records(pd.concat(frames, ignore_index=True))
    # Include only actually indexed filing days. Count absent matches as zero on
    # those covered days; weekends and days absent from the entire index stay absent.
    filing_days = sorted(data.date.unique())
    result = []
    for form, description in SEC_FORMS.items():
        rows = data[data.form == form]
        counts = rows.groupby("date").size()
        if len(counts) < 150 or counts.sum() < 1000:
            continue
        values = counts.reindex(filing_days, fill_value=0)
        item = accepted(
            "sec_daily_filings_" + slug(form), list(zip(values.index, values)),
            title=f"SEC daily {form} filing count: {description}",
            provider="SEC EDGAR filing indexes", category=sec_category(form), unit="filings",
            frequency="business_daily",
            source_url="https://www.sec.gov/Archives/edgar/full-index/",
            geography="United States filing system; domestic and foreign filers",
            hypothesis=f"{description} activity may indicate changes in financing demand, corporate events, disclosure risk or ownership attention relevant to market liquidity and sectors.",
            limitations="Counts of published indexed documents, not dollar flows or transaction direction. Includes amendments only in explicitly named amendment series. SC 13D/G and renamed SCHEDULE 13D/G labels are combined into the same indicator. Multi-form submissions can contribute to multiple form categories but only once per category. Zeros mean zero matches on a day with other indexed filings in the complete quarterly files; unobserved calendar days are absent. Latest index revisions and posting delays apply.",
            raw_sha256=hashes, dataset_identity="sec:edgar:daily_filing_count:" + form,
            native_frequency="event filings, aggregated daily",
            finer_frequency_available=False,
            aggregation="Count distinct accession paths per canonical Form Type and Date Filed; no company identifiers in output",
            documentation_url="https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data",
        )
        if item:
            result.append(item)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--provider", choices=["cp", "repo", "ficc", "ecb", "sec"])
    args = parser.parse_args()
    fetch = Fetcher(refresh=args.refresh)
    jobs = {"cp": commercial_paper, "repo": repo, "ficc": sponsored_repo, "ecb": estr, "sec": sec}
    report = {"started_at": pd.Timestamp.now(tz="UTC").isoformat(), "successes": [], "failures": []}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn, fetch): name for name, fn in jobs.items() if args.provider in {None, name}}
        for future in as_completed(futures):
            name = futures[future]
            try:
                items = future.result()
                report["successes"].extend(x["series_id"] for x in items)
                print(f"PROVIDER {name}: {len(items)} accepted", flush=True)
            except Exception as exc:
                report["failures"].append({"provider": name, "error": f"{type(exc).__name__}: {exc}"})
                print(f"FAILED {name}: {exc}", flush=True)
    report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(ROOT / "logs/macro2_collection.json", json.dumps(report, indent=2).encode())
    print(f"Staged {len(report['successes'])} candidates, failures={len(report['failures'])}")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
