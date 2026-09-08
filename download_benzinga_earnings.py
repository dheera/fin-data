#!/usr/bin/env python3
"""
Download Benzinga earnings (historical + upcoming) from Massive API.
https://massive.com/docs/rest/partners/benzinga/earnings

Requires the "Benzinga Earnings" add-on on the Massive/Polygon key.
History: 2010-04-30 .. present. EPS/revenue actual + estimate + surprise,
BMO/AMC `time`, date_status (projected/confirmed), importance 0-5.

Usage:
  python download_benzinga_earnings.py --smoke     # verify entitlement (1 page)
  python download_benzinga_earnings.py             # full pull, chunked by year, resumable

Output:
  benzinga/earnings/{year}.parquet   (chunk cache, safe to re-run)
  benzinga/earnings.parquet          (merged, all columns kept verbatim)
"""
import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_URL = "https://api.massive.com"
ENDPOINT = "/benzinga/v1/earnings"
PAGE_LIMIT = 50000
SLEEP_SEC = 0.15
FIRST_YEAR = 2010


def resolve_root(cli_root: str | None) -> Path:
    if cli_root:
        return Path(cli_root)
    nas = Path("/data/fin")
    return nas if nas.exists() else SCRIPT_DIR


def load_api_key(root: Path) -> str:
    for d in (root, SCRIPT_DIR):
        for name in ("massive.json", "polygon.json"):
            p = d / name
            if p.exists():
                k = json.load(open(p)).get("api_key")
                if k:
                    return k
    k = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY")
    if k:
        return k
    raise SystemExit("No API key: put it in massive.json/polygon.json or MASSIVE_API_KEY env.")


def _url_with_api_key(url: str, api_key: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["apiKey"] = [api_key]
    return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))


def fetch_json(session: requests.Session, url: str, api_key: str, params: dict | None = None,
               retries: int = 8) -> dict:
    for i in range(retries):
        try:
            r = session.get(url, params=params, headers={"Accept": "application/json"}, timeout=60)
        except requests.RequestException as e:
            print(f"  network error ({e.__class__.__name__}), retry {i + 1}")
            time.sleep(2 * (i + 1))
            continue
        if r.status_code in (402, 403):
            raise SystemExit(
                f"HTTP {r.status_code} NOT AUTHORIZED — the Benzinga Earnings add-on is not "
                f"active on this key yet. Body: {r.text[:300]}"
            )
        if r.status_code == 429:
            print("  429 rate-limited, sleeping 15s")
            time.sleep(15)
            continue
        if r.status_code >= 500:
            time.sleep(2 * (i + 1))
            continue
        r.raise_for_status()
        data = r.json()
        if data.get("status") not in ("OK", "DELAYED"):
            raise RuntimeError(f"Unexpected API response: {str(data)[:300]}")
        return data
    raise RuntimeError(f"Giving up after {retries} retries: {url}")


def fetch_all(session: requests.Session, api_key: str, params: dict) -> list[dict]:
    results: list[dict] = []
    url = BASE_URL + ENDPOINT
    params = {**params, "apiKey": api_key}
    while url is not None:
        data = fetch_json(session, url, api_key, params=params)
        results.extend(data.get("results") or [])
        url = data.get("next_url")
        if url:
            url = _url_with_api_key(url, api_key)
            params = None
            time.sleep(SLEEP_SEC)
    return results


def pull_year(session: requests.Session, api_key: str, year: int) -> pd.DataFrame:
    rows = fetch_all(session, api_key, {
        "date.gte": f"{year}-01-01",
        "date.lt": f"{year + 1}-01-01",
        "limit": PAGE_LIMIT,
        "sort": "date.asc",
    })
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--smoke", action="store_true", help="fetch 1 page, print sample, exit")
    ap.add_argument("--root", help="output root (default: /data/fin if present, else script dir)")
    ap.add_argument("--start-year", type=int, default=FIRST_YEAR)
    ap.add_argument("--end-year", type=int, default=dt.date.today().year + 1,
                    help="inclusive; default next year to catch upcoming confirmed dates")
    args = ap.parse_args()

    root = resolve_root(args.root)
    global OUT_DIR, MERGED
    OUT_DIR = root / "benzinga" / "earnings"
    MERGED = root / "benzinga" / "earnings.parquet"
    api_key = load_api_key(root)
    session = requests.Session()

    if args.smoke:
        data = fetch_json(session, BASE_URL + ENDPOINT, api_key,
                          params={"apiKey": api_key, "limit": 5, "sort": "date.asc"})
        res = data.get("results") or []
        print(f"OK — {len(res)} rows on first page (limit=5), has next_url={bool(data.get('next_url'))}")
        for r in res[:3]:
            print(json.dumps(r, indent=2)[:600])
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for year in range(args.start_year, args.end_year + 1):
        out = OUT_DIR / f"{year}.parquet"
        # Always refresh the current+future years (they gain rows); skip finished past years
        if out.exists() and year < dt.date.today().year:
            print(f"{year}: exists, skip")
            continue
        t0 = time.time()
        df = pull_year(session, api_key, year)
        if df.empty:
            print(f"{year}: 0 rows")
            continue
        df.to_parquet(out, index=False)
        print(f"{year}: {len(df)} rows in {time.time() - t0:.0f}s -> {out}")

    parts = sorted(OUT_DIR.glob("*.parquet"))
    if parts:
        df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(["date", "ticker"]).reset_index(drop=True)
        df.to_parquet(MERGED, index=False)
        print(f"MERGED: {len(df)} rows, {df.ticker.nunique()} tickers, "
              f"{df.date.min().date()} .. {df.date.max().date()} -> {MERGED}")


if __name__ == "__main__":
    main()
