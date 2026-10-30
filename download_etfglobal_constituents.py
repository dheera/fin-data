#!/usr/bin/env python3
"""
Download ETF Global point-in-time ETF constituents/holdings from Massive API.
https://massive.com/docs/rest/partners/etf-global/constituents

Requires the "ETF Global Constituents" add-on on the Massive/Polygon key.
History: 2017-04-03 .. present, daily. THIS IS POINT-IN-TIME: each row has
effective_date (when the holding was true) + processed_date (when ETFG ingested
it) — unlike our etf.com scrape (us_stocks_sip/etf_holdings), which is a
today-snapshot and future-biased for any historical use.

Fields per row: composite_ticker, constituent_ticker, constituent_name, weight,
shares_held, market_value, constituent_rank, asset_class, security_type,
isin/figi/sedol/us_code, exchange, currency.

Intended 1-day-access battle plan (all modes resumable — re-run freely):
  1. --smoke                      verify entitlement (seconds)
  2. --probe-day 2024-06-14       one full day -> rows/pages/min, extrapolates full sweep
  3. (default: priority tickers)  full history for ETFs we care about (guaranteed win)
  4. --all-by-date --workers 16   full universe sweep, one chunk per weekday

Output:
  etf_global/constituents/by_ticker/{ETF}/{year}.parquet
  etf_global/constituents/by_date/{YYYY-MM-DD}.parquet
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
ENDPOINT = "/etf-global/v1/constituents"
PAGE_LIMIT = 5000
SLEEP_SEC = 0.05
FIRST_DATE = "2017-04-03"

# Core list if no --tickers given and no etf_holdings dir to derive from:
# index + sector + factor + bond + LETF names our books actually use.
DEFAULT_TICKERS = [
    "SPY", "QQQ", "IWM", "DIA", "MDY", "IJH", "IJR", "VTI", "VOO", "RSP",
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
    "SMH", "XBI", "IBB", "KRE", "XHB", "ITB", "XRT", "XOP", "OIH", "GDX",
    "MTUM", "VLUE", "QUAL", "USMV", "SPLV", "IWD", "IWF", "IWO", "IWN",
    "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "AGG", "BND", "TIP", "MBB",
    "GLD", "SLV", "USO", "UNG",
    "TQQQ", "SQQQ", "UPRO", "SPXU", "SPXL", "SPXS", "SOXL", "SOXS",
    "TNA", "TZA", "UDOW", "SDOW", "TMF", "TMV", "UVXY", "SVXY", "LABU", "LABD",
    "ARKK", "EEM", "EFA", "FXI", "EWJ", "EWZ", "BITX",
]


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
            r = session.get(url, params=params, headers={"Accept": "application/json"}, timeout=120)
        except requests.RequestException as e:
            print(f"  network error ({e.__class__.__name__}), retry {i + 1}")
            time.sleep(2 * (i + 1))
            continue
        if r.status_code in (402, 403):
            raise SystemExit(
                f"HTTP {r.status_code} NOT AUTHORIZED — the ETF Global Constituents add-on is "
                f"not active on this key yet. Body: {r.text[:300]}"
            )
        if r.status_code == 429:
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


def fetch_all(session: requests.Session, api_key: str, params: dict) -> tuple[list[dict], int]:
    results: list[dict] = []
    url = BASE_URL + ENDPOINT
    params = {**params, "apiKey": api_key}
    pages = 0
    while url is not None:
        data = fetch_json(session, url, api_key, params=params)
        results.extend(data.get("results") or [])
        pages += 1
        url = data.get("next_url")
        if url:
            url = _url_with_api_key(url, api_key)
            params = None
            time.sleep(SLEEP_SEC)
    return results, pages


def write_parquet(df: pd.DataFrame, out: Path):
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out, index=False, compression="zstd")
    except (ValueError, ImportError):
        df.to_parquet(out, index=False)


def chunk_done(out: Path) -> bool:
    return out.exists() or out.with_suffix(".empty").exists()


def pull_ticker_year(api_key: str, ticker: str, year: int, out_dir: Path) -> str:
    out = out_dir / "by_ticker" / ticker / f"{year}.parquet"
    if chunk_done(out) and year < dt.date.today().year:
        return f"{ticker} {year}: exists, skip"
    session = requests.Session()
    rows, pages = fetch_all(session, api_key, {
        "composite_ticker": ticker,
        "effective_date.gte": f"{year}-01-01",
        "effective_date.lt": f"{year + 1}-01-01",
        "limit": PAGE_LIMIT,
    })
    if not rows:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.with_suffix(".empty").touch()
        return f"{ticker} {year}: 0 rows"
    write_parquet(pd.DataFrame(rows), out)
    return f"{ticker} {year}: {len(rows)} rows / {pages} pages"


def pull_date(api_key: str, date: str, out_dir: Path) -> str:
    out = out_dir / "by_date" / f"{date}.parquet"
    if chunk_done(out):
        return f"{date}: exists, skip"
    session = requests.Session()
    rows, pages = fetch_all(session, api_key, {
        "effective_date": date,
        "limit": PAGE_LIMIT,
    })
    if not rows:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.with_suffix(".empty").touch()
        return f"{date}: 0 rows"
    df = pd.DataFrame(rows)
    write_parquet(df, out)
    return f"{date}: {len(df)} rows / {pages} pages / {df.composite_ticker.nunique()} ETFs"


def run_pool(jobs, workers: int):
    t0, done = time.time(), 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(*j) for j in jobs]
        for f in as_completed(futs):
            done += 1
            rate = done / max(time.time() - t0, 1e-9)
            eta = (len(futs) - done) / rate / 60 if rate > 0 else float("nan")
            print(f"[{done}/{len(futs)}, eta {eta:.0f}m] {f.result()}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Download ETF Global PIT constituents to parquet.")
    ap.add_argument("--smoke", action="store_true", help="fetch 1 page, print sample, exit")
    ap.add_argument("--probe-day", metavar="YYYY-MM-DD",
                    help="pull ONE full day, report size + extrapolated full-sweep cost, exit")
    ap.add_argument("--root", help="output root (default: /data/fin if present, else script dir)")
    ap.add_argument("--tickers", nargs="+", help="priority ETF tickers (full history per ticker)")
    ap.add_argument("--tickers-from-dir", metavar="DIR",
                    help="derive priority tickers from parquet basenames in DIR "
                         "(e.g. us_stocks_sip/etf_holdings)")
    ap.add_argument("--all-by-date", action="store_true",
                    help="full-universe sweep, one chunk per weekday since 2017-04-03")
    ap.add_argument("--start", default=FIRST_DATE)
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()

    root = resolve_root(args.root)
    out_dir = root / "etf_global" / "constituents"
    api_key = load_api_key(root)

    if args.smoke:
        session = requests.Session()
        data = fetch_json(session, BASE_URL + ENDPOINT, api_key,
                          params={"apiKey": api_key, "limit": 3})
        res = data.get("results") or []
        print(f"OK — {len(res)} rows on first page (limit=3), has next_url={bool(data.get('next_url'))}")
        for r in res[:2]:
            print(json.dumps(r, indent=2)[:700])
        return

    if args.probe_day:
        t0 = time.time()
        msg = pull_date(api_key, args.probe_day, out_dir)
        mins = (time.time() - t0) / 60
        print(msg)
        print(f"elapsed {mins:.1f} min for one day (single-threaded).")
        n_days = len(pd.bdate_range(args.start, args.end))
        print(f"full sweep = {n_days} weekdays -> ~{n_days * mins / args.workers / 60:.1f} h "
              f"at --workers {args.workers} (linear extrapolation)")
        out = out_dir / "by_date" / f"{args.probe_day}.parquet"
        if out.exists():
            sz = out.stat().st_size / 1e6
            print(f"chunk size {sz:.1f} MB -> full sweep ~{sz * n_days / 1e3:.0f} GB")
        return

    if args.all_by_date:
        days = [d.date().isoformat() for d in pd.bdate_range(args.start, args.end)]
        jobs = [(pull_date, api_key, d, out_dir) for d in days]
        run_pool(jobs, args.workers)
        return

    # Default mode: priority tickers, full history each
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
    elif args.tickers_from_dir:
        tickers = sorted(p.stem for p in Path(args.tickers_from_dir).glob("*.parquet"))
    else:
        tickers = DEFAULT_TICKERS
    y0, y1 = int(args.start[:4]), int(args.end[:4])
    jobs = [(pull_ticker_year, api_key, t, y, out_dir)
            for t in tickers for y in range(y0, y1 + 1)]
    print(f"{len(tickers)} tickers x {y1 - y0 + 1} years = {len(jobs)} chunks")
    run_pool(jobs, args.workers)


if __name__ == "__main__":
    main()
