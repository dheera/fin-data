#!/usr/bin/env python3
"""
Download corporate events from Massive TMX API (Wall Street Horizon).
https://massive.com/docs/rest/partners/tmx/corporate-events

Follows the same pattern as download_splits.py / download_dividends.py:
- Config from JSON (massive.json or MASSIVE_API_KEY env), paginate via next_url, write parquet.

Usage:
  # Fetch all corporate events (requires Massive API key):
  python download_corporate_events.py

  # Restrict to QQQ + VOO constituents (run download_etf_holdings QQQ VOO first):
  python download_corporate_events.py --tickers-from-etf QQQ VOO
"""
import argparse
import json
import os
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests

DEFAULT_BASE_URL = "https://api.massive.com"
CONFIG_PATH = "massive.json"
OUTPUT_PATH = "us_stocks_sip/corporate_events.parquet"
ETF_HOLDINGS_DIR = "us_stocks_sip/etf_holdings"
PAGE_LIMIT = 5000  # API allows up to 50000; use 5k for safer pagination
SLEEP_SEC = 0.2


def load_config():
    cfg = {}
    path = Path(CONFIG_PATH)
    if path.exists():
        with open(path) as f:
            cfg = json.load(f)
    api_key = cfg.get("api_key") or os.environ.get("MASSIVE_API_KEY")
    # Polygon was Massive's former name; same key works for both
    if not api_key and Path("polygon.json").exists():
        with open("polygon.json") as f:
            api_key = json.load(f).get("api_key")
    if not api_key:
        raise SystemExit(
            f"Set API key via massive.json 'api_key', env MASSIVE_API_KEY, or polygon.json 'api_key'.\n"
            f"Example massive.json: {{\"api_key\": \"YOUR_KEY\", \"base_url\": \"https://api.massive.com\"}}"
        )
    cfg["api_key"] = api_key
    cfg.setdefault("base_url", DEFAULT_BASE_URL)
    return cfg


def _url_with_api_key(url: str, api_key: str) -> str:
    """Append apiKey to URL for pagination (next_url may not include it)."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["apiKey"] = [api_key]
    new_query = urlencode(qs, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_corporate_events(
    session: requests.Session,
    *,
    base_url: str,
    api_key: str,
    tickers: list[str] | None = None,
    date_gte: str | None = None,
    date_lte: str | None = None,
    limit: int = PAGE_LIMIT,
) -> list[dict]:
    """Fetch all pages of corporate events, optionally filtered by tickers or date."""
    url = f"{base_url.rstrip('/')}/tmx/v1/corporate-events"
    headers = {"Accept": "application/json"}
    # Polygon/Massive accept apiKey as query param (same as splits/dividends)
    results: list[dict] = []
    params: dict = {"apiKey": api_key, "limit": limit, "sort": "date.desc"}
    if date_gte:
        params["date.gte"] = date_gte
    if date_lte:
        params["date.lte"] = date_lte
    # API supports ticker.any_of as comma-separated list
    if tickers:
        # Batch tickers to avoid huge query strings (e.g. 200 per request)
        batch_size = 200
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i : i + batch_size]
            params_batch = {**params, "ticker.any_of": ",".join(batch)}
            page_url: str | None = url
            while page_url is not None:
                r = session.get(
                    page_url,
                    headers=headers,
                    params=params_batch if page_url == url else None,
                )
                r.raise_for_status()
                data = r.json()
                if data.get("status") == "ERROR":
                    raise RuntimeError(f"API error: {data.get('error', data)}")
                if data.get("status") != "OK":
                    raise RuntimeError(f"Unexpected API response: {data}")
                results.extend(data.get("results") or [])
                print(f"  Fetched {len(results)} events (ticker batch)")
                page_url = data.get("next_url")
                if page_url:
                    page_url = _url_with_api_key(page_url, api_key)
                    params_batch = None  # next_url is full URL
                    time.sleep(SLEEP_SEC)
            time.sleep(SLEEP_SEC)
        return results
    # No ticker filter: paginate with next_url
    page_url = url
    while page_url is not None:
        r = session.get(
            page_url,
            headers=headers,
            params=params if page_url == url else None,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "ERROR":
            raise RuntimeError(f"API error: {data.get('error', data)}")
        if data.get("status") != "OK":
            raise RuntimeError(f"Unexpected API response: {data}")
        results.extend(data.get("results") or [])
        print(f"Fetched {len(results)} events")
        page_url = data.get("next_url")
        if page_url:
            page_url = _url_with_api_key(page_url, api_key)
            params = None
        time.sleep(SLEEP_SEC)
    return results


def load_tickers_from_etf_parquets(etf_tickers: list[str]) -> list[str]:
    """Load constituent tickers from us_stocks_sip/etf_holdings/{TICKER}.parquet."""
    out = set()
    for etf in etf_tickers:
        path = Path(ETF_HOLDINGS_DIR) / f"{etf}.parquet"
        if not path.exists():
            print(f"Warning: {path} not found, run download_etf_holdings first for {etf}")
            continue
        df = pd.read_parquet(path)
        col = "ticker" if "ticker" in df.columns else "symbol"
        if col not in df.columns:
            print(f"Warning: no ticker/symbol column in {path}")
            continue
        # Keep latest row per ticker if multiple dates
        if "asOf" in df.columns:
            df = df.sort_values("asOf", ascending=False).drop_duplicates(subset=[col], keep="first")
        out.update(df[col].astype(str).str.strip().dropna().tolist())
    return sorted(out)


def main():
    parser = argparse.ArgumentParser(
        description="Download TMX corporate events from Massive API to parquet."
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_PATH,
        help=f"Output parquet path (default: {OUTPUT_PATH})",
    )
    parser.add_argument(
        "--tickers-from-etf",
        nargs="+",
        metavar="ETF",
        help="Restrict to tickers from these ETF holdings (e.g. QQQ VOO). Uses us_stocks_sip/etf_holdings/*.parquet.",
    )
    parser.add_argument(
        "--date-gte",
        metavar="YYYY-MM-DD",
        help="Only events on or after this date.",
    )
    parser.add_argument(
        "--date-lte",
        metavar="YYYY-MM-DD",
        help="Only events on or before this date.",
    )
    args = parser.parse_args()

    config = load_config()
    base_url = config["base_url"].rstrip("/")
    api_key = config["api_key"]

    tickers = None
    if args.tickers_from_etf:
        tickers = load_tickers_from_etf_parquets(args.tickers_from_etf)
        print(f"Restricting to {len(tickers)} tickers from ETF(s) {args.tickers_from_etf}")
        if not tickers:
            raise SystemExit("No tickers found from ETF parquets. Run download_etf_holdings first.")

    session = requests.Session()
    events = get_corporate_events(
        session,
        base_url=base_url,
        api_key=api_key,
        tickers=tickers,
        date_gte=args.date_gte,
        date_lte=args.date_lte,
    )

    if not events:
        print("No events returned.")
        return

    df = pd.DataFrame(events)
    # Normalize date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    # Ensure tmx_company_id is int if present (sample had string "2208")
    if "tmx_company_id" in df.columns:
        df["tmx_company_id"] = pd.to_numeric(df["tmx_company_id"], errors="coerce")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"Wrote {len(df)} events to {out_path}")


if __name__ == "__main__":
    main()
