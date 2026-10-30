#!/usr/bin/env python3
"""
Download ETF Global daily fund flows (+ NAV, shares outstanding) from Massive API.
https://massive.com/docs/rest/partners/etf-global/fundflows

Requires the "ETF Global Fund Flows" add-on on the Massive/Polygon key.
History: 2017-04-03 .. present, daily, ~3000+ US ETFs.
Fields: composite_ticker, effective_date, processed_date, fund_flow (net $ creation/
redemption flow), nav, shares_outstanding.

Usage:
  python download_etfglobal_fundflows.py --smoke        # verify entitlement (1 page)
  python download_etfglobal_fundflows.py                # full pull, chunked by month, resumable
  python download_etfglobal_fundflows.py --workers 8

Output:
  etf_global/fund_flows/{YYYY-MM}.parquet   (chunk cache, safe to re-run)
  etf_global/fund_flows.parquet             (merged)
"""
import argparse
import datetime as dt
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_URL = "https://api.massive.com"
ENDPOINT = "/etf-global/v1/fund-flows"
PAGE_LIMIT = 5000
SLEEP_SEC = 0.1
FIRST_MONTH = "2017-04"


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
                f"HTTP {r.status_code} NOT AUTHORIZED — the ETF Global Fund Flows add-on is "
                f"not active on this key yet. Body: {r.text[:300]}"
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


def month_range(first: str, last: str) -> list[str]:
    out, cur = [], dt.date.fromisoformat(first + "-01")
    end = dt.date.fromisoformat(last + "-01")
    while cur <= end:
        out.append(cur.strftime("%Y-%m"))
        cur = (cur.replace(day=28) + dt.timedelta(days=5)).replace(day=1)
    return out


def pull_month(api_key: str, month: str, out_dir: Path) -> str:
    out = out_dir / f"{month}.parquet"
    empty = out_dir / f"{month}.empty"
    this_month = dt.date.today().strftime("%Y-%m")
    if (out.exists() or empty.exists()) and month < this_month:
        return f"{month}: exists, skip"
    start = dt.date.fromisoformat(month + "-01")
    nxt = (start.replace(day=28) + dt.timedelta(days=5)).replace(day=1)
    session = requests.Session()
    rows = fetch_all(session, api_key, {
        "effective_date.gte": start.isoformat(),
        "effective_date.lt": nxt.isoformat(),
        "limit": PAGE_LIMIT,
    })
    if not rows:
        empty.touch()
        return f"{month}: 0 rows"
    df = pd.DataFrame(rows)
    df.to_parquet(out, index=False)
    return f"{month}: {len(df)} rows, {df.composite_ticker.nunique()} ETFs"


def main():
    ap = argparse.ArgumentParser(description="Download ETF Global fund flows to parquet.")
    ap.add_argument("--smoke", action="store_true", help="fetch 1 page, print sample, exit")
    ap.add_argument("--root", help="output root (default: /data/fin if present, else script dir)")
    ap.add_argument("--start-month", default=FIRST_MONTH)
    ap.add_argument("--end-month", default=dt.date.today().strftime("%Y-%m"))
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--no-merge", action="store_true")
    args = ap.parse_args()

    root = resolve_root(args.root)
    out_dir = root / "etf_global" / "fund_flows"
    api_key = load_api_key(root)

    if args.smoke:
        session = requests.Session()
        data = fetch_json(session, BASE_URL + ENDPOINT, api_key,
                          params={"apiKey": api_key, "limit": 5})
        res = data.get("results") or []
        print(f"OK — {len(res)} rows on first page (limit=5), has next_url={bool(data.get('next_url'))}")
        for r in res[:3]:
            print(json.dumps(r, indent=2)[:400])
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    months = month_range(args.start_month, args.end_month)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(pull_month, api_key, m, out_dir): m for m in months}
        for i, f in enumerate(as_completed(futs)):
            print(f"[{i + 1}/{len(months)}] {f.result()}", flush=True)
    print(f"chunks done in {(time.time() - t0) / 60:.1f} min")

    if not args.no_merge:
        parts = sorted(out_dir.glob("*.parquet"))
        df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        for c in ("effective_date", "processed_date"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        df = df.sort_values(["effective_date", "composite_ticker"]).reset_index(drop=True)
        merged = root / "etf_global" / "fund_flows.parquet"
        df.to_parquet(merged, index=False)
        print(f"MERGED: {len(df)} rows, {df.composite_ticker.nunique()} ETFs, "
              f"{df.effective_date.min().date()} .. {df.effective_date.max().date()} -> {merged}")


if __name__ == "__main__":
    main()
