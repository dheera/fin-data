#!/usr/bin/env python3
"""
Download the raw material for following a security across symbol changes (QQQ was QQQQ
2004-12 .. 2011-03, META was FB until 2022-06):

    us_stocks_sip/ticker_changes.parquet     Polygon ticker events, one row per ticker_change:
                                             composite_figi, cik, name, ticker, date (the ticker is
                                             in effect from `date`), fetched_utc
    us_stocks_sip/tickers_inactive.parquet   every inactive (delisted / renamed) stock ticker:
                                             ticker, name, type, primary_exchange, cik, composite_figi,
                                             share_class_figi, delisted_utc, last_updated_utc

Ticker events are keyed by composite FIGI and only exist for tickers active today, and
Polygon's event history has gaps (QQQ's events don't mention QQQQ). Old inactive tickers
mostly have no FIGI, but they do carry the issuer's SEC CIK and a delisting date, which is
how a predecessor like QQQQ (CIK 0001067839, delisted 2011-03-23) links to its successor.
Linking is left to a later generation step; this script only downloads.

Run after download_tickers.py (it reads us_stocks_sip/tickers.parquet for the FIGIs).
Events are fetched incrementally: FIGIs not seen before, FIGIs whose current ticker in
tickers.parquet differs from the one it had when we last fetched (a change just happened),
and FIGIs last fetched more than --refresh-days ago (plus a per-FIGI stagger of up to
another --refresh-days, so about 1/N of them refresh each day). (Comparing against the latest event
instead would refetch ~280 FIGIs every day whose Polygon event history disagrees with
tickers.parquet, e.g. BF vs BF.A, ERICY vs ERIC.) --full refetches everything (~11k requests).
"""
import argparse
import json
import os
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests

BASE = Path("us_stocks_sip")
EVENTS_FILE = BASE / "ticker_changes.parquet"
STATE_FILE = BASE / "ticker_changes_state.parquet"     # composite_figi, fetched_utc, found, ticker_at_fetch
INACTIVE_FILE = BASE / "tickers_inactive.parquet"
INACTIVE_COLS = ["ticker", "name", "type", "primary_exchange", "cik", "composite_figi", "share_class_figi",
                 "delisted_utc", "last_updated_utc"]

with open("polygon.json", "r") as f:
    config = json.loads(f.read())

session = requests.Session()


def get(url, params=None, tries=6):
    """GET with the API key; retries 429/5xx and connection errors with backoff.
    Returns (status, json) - 404 is a normal answer ("no events for this FIGI")."""
    params = {**(params or {}), "apiKey": config["api_key"]}
    for i in range(tries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return r.status_code, r.json()
        except (requests.RequestException, ValueError) as e:
            if i == tries - 1:
                raise
            wait = 2 ** i
            print(f"  {e}; retrying in {wait}s")
            time.sleep(wait)


def write_atomic(df: pd.DataFrame, path: Path):
    tmp = path.with_name(path.name + ".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def download_inactive():
    url, params, rows = "https://api.polygon.io/v3/reference/tickers", {"active": "false", "market": "stocks", "limit": 1000}, []
    while url is not None:
        _, j = get(url, params)
        rows += j.get("results", [])
        url, params = j.get("next_url"), None      # next_url carries the cursor and filters
        print(f"Fetched {len(rows)} inactive tickers")
        time.sleep(0.1)
    df = pd.DataFrame(rows)
    for c in INACTIVE_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[INACTIVE_COLS].drop_duplicates()
    df["delisted_utc"] = pd.to_datetime(df["delisted_utc"], utc=True, errors="coerce")
    df["last_updated_utc"] = pd.to_datetime(df["last_updated_utc"], utc=True, errors="coerce")
    df = df.sort_values(["ticker", "delisted_utc"]).reset_index(drop=True)
    print(f"Writing {INACTIVE_FILE} ({len(df)} rows)")
    write_atomic(df, INACTIVE_FILE)


def fetch_events(figi: str):
    status, j = get(f"https://api.polygon.io/vX/reference/tickers/{figi}/events", {"types": "ticker_change"})
    if status == 404:
        return figi, None, []
    if status != 200:
        raise RuntimeError(f"{figi}: HTTP {status}: {j}")
    res = j.get("results", {})
    events = [{"composite_figi": res.get("composite_figi") or figi, "cik": res.get("cik"), "name": res.get("name"),
               "ticker": (e.get("ticker_change") or {}).get("ticker"), "date": e.get("date")}
              for e in res.get("events", []) if e.get("type") == "ticker_change"]
    return figi, res, events


def download_events(full: bool, refresh_days: int, workers: int):
    tickers = pd.read_parquet(BASE / "tickers.parquet", columns=["ticker", "composite_figi"])
    current = tickers.dropna(subset=["composite_figi"]).drop_duplicates("composite_figi").set_index("composite_figi")["ticker"]
    old = pd.read_parquet(EVENTS_FILE) if EVENTS_FILE.exists() and not full else pd.DataFrame(
        columns=["composite_figi", "cik", "name", "ticker", "date", "fetched_utc"])
    state = pd.read_parquet(STATE_FILE) if STATE_FILE.exists() and not full else pd.DataFrame(
        columns=["composite_figi", "fetched_utc", "found", "ticker_at_fetch"])
    now = pd.Timestamp.now(tz="UTC")

    if "ticker_at_fetch" not in state.columns:
        state["ticker_at_fetch"] = None
    st = state.set_index("composite_figi")
    todo = []
    for figi, tk in current.items():
        if figi not in st.index:
            todo.append(figi)                                   # new FIGI
        elif st.at[figi, "ticker_at_fetch"] is not None and st.at[figi, "ticker_at_fetch"] != tk:
            todo.append(figi)                                   # its ticker changed since we last looked
        elif now - st.at[figi, "fetched_utc"] > pd.Timedelta(days=refresh_days + zlib.crc32(figi.encode()) % refresh_days):
            # periodic refresh, staggered per FIGI so a full first run doesn't all come due on one day
            todo.append(figi)
    print(f"Fetching ticker events for {len(todo)} of {len(current)} FIGIs")

    new_rows, new_state = [], []
    with ThreadPoolExecutor(workers) as ex:
        for i, (figi, res, events) in enumerate(ex.map(fetch_events, todo), 1):
            new_state.append({"composite_figi": figi, "fetched_utc": now, "found": res is not None,
                              "ticker_at_fetch": current[figi]})
            for e in events:
                new_rows.append({**e, "fetched_utc": now})
            if i % 500 == 0:
                print(f"  {i}/{len(todo)} FIGIs, {len(new_rows)} events")

    refetched = set(todo)
    keep = [old[~old.composite_figi.isin(refetched)], pd.DataFrame(new_rows, columns=old.columns)]
    events = pd.concat([f for f in keep if len(f)] or [old.iloc[:0]], ignore_index=True)
    events["date"] = pd.to_datetime(events["date"], errors="coerce").dt.date
    events["fetched_utc"] = pd.to_datetime(events["fetched_utc"], utc=True)
    events = events.dropna(subset=["ticker", "date"]).drop_duplicates(["composite_figi", "ticker", "date"])
    events = events.sort_values(["composite_figi", "date"]).reset_index(drop=True)
    keep = [state[~state.composite_figi.isin(refetched)], pd.DataFrame(new_state, columns=state.columns)]
    state = pd.concat([f for f in keep if len(f)] or [state.iloc[:0]], ignore_index=True)
    state["fetched_utc"] = pd.to_datetime(state["fetched_utc"], utc=True)

    n_multi = (events.groupby("composite_figi").size() > 1).sum()
    print(f"Writing {EVENTS_FILE} ({len(events)} events, {n_multi} FIGIs with more than one ticker)")
    write_atomic(events, EVENTS_FILE)
    write_atomic(state, STATE_FILE)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--full", action="store_true", help="refetch events for every FIGI")
    ap.add_argument("--refresh-days", type=int, default=30, help="refetch FIGIs older than this (default: %(default)s)")
    ap.add_argument("--workers", type=int, default=4, help="concurrent event requests (default: %(default)s)")
    ap.add_argument("--skip-inactive", action="store_true", help="don't refresh tickers_inactive.parquet")
    args = ap.parse_args()
    if not args.skip_inactive:
        download_inactive()
    download_events(args.full, args.refresh_days, args.workers)
