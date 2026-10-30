#!/usr/bin/env python3

# Downloads Kalshi perpetual futures trades and builds per-day trades / minute bars /
# day bars, delta-updating up to the latest complete UTC day.
#
# ./download_perpetuals_kalshi.py                       # through the latest complete day
# ./download_perpetuals_kalshi.py --through 2026-10-01  # stop earlier
#
# Output (perpetuals/kalshi), same layout and writer as global_xstocks/ (see download_xstocks.py):
#   trades/<date>.parquet        tick trades, rows ordered by (ticker, participant_timestamp)
#       ticker                 string   Kalshi market ticker, e.g. "KXBTCPERP"
#       participant_timestamp  timestamp[ns, tz=UTC]   (Kalshi reports microseconds)
#       price                  double   USD per contract (KXBTCPERP: 1 contract = 0.0001 BTC)
#       size                   double   contracts
#       conditions             int8     1 = sell aggressor (taker_side "ask"), 2 = buy ("bid")
#       id                     string   Kalshi trade_id (UUID)
#   minute_aggs/<date>.parquet   bars from trades, index (ticker, window_start)
#   day_aggs/<date>.parquet      UTC-day bars, index (ticker, window_start)
#   markets.parquet              market metadata incl. contract_size, refreshed each run
#
# A trades/<date>.parquet only exists once the day is complete (aggs are written first).
#
# Source: public Kalshi perps REST (GET /margin/trades), unauthenticated on purpose so it
# never competes with the account's API limits. Paced to 10 requests/s; a busy day is
# ~450 calls (~45 s).

import os
import sys
import time
import argparse
import threading
import datetime as dt
import concurrent.futures

import numpy as np
import pandas as pd
import pyarrow as pa
import requests

from download_xstocks import write_day, pending_days, day_ns, log, UTC_TS

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "perpetuals", "kalshi")
API = "https://external-api.kalshi.com/trade-api/v2"
START = dt.date(2026, 6, 3)  # first Kalshi perp (KXBTCPERP) went live
SETTLE = dt.timedelta(minutes=10)  # wait this long after midnight UTC before a day counts as complete

TRADES_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("participant_timestamp", UTC_TS),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("conditions", pa.int8()),
    ("id", pa.string()),
])

session = requests.Session()
session.headers["User-Agent"] = "fin-perps-downloader/1.0"


class Pacer:
    """Spaces requests evenly across all threads so we stay under the rate limit
    (Kalshi's basic tier is 20 reads/s; 4 unpaced workers drew 429s)."""

    def __init__(self, per_second):
        self.interval, self.next, self.lock = 1.0 / per_second, 0.0, threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next)
            self.next = slot + self.interval
        time.sleep(max(0.0, slot - now))


pacer = Pacer(10)


def get(path, params=None):
    for attempt in range(10):
        try:
            pacer.wait()
            r = session.get(f"{API}{path}", params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            wait = min(60, 2 ** attempt)
            log(f"  retry in {wait}s: {path} {e}")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {path} {params}")


def markets():
    """All perps markets (active and inactive) as a DataFrame."""
    m = pd.json_normalize(get("/margin/markets")["markets"])
    keep = ["ticker", "title", "asset_class", "status", "contract_size", "tick_size",
            "underlying_multiplier", "market_id", "market_version"]
    m = m[[c for c in keep if c in m.columns]].sort_values("ticker").reset_index(drop=True)
    for c in ("contract_size", "tick_size", "underlying_multiplier"):
        m[c] = m[c].astype("float64")
    return m


def market_day(ticker, day):
    """All trades of one market on one UTC day."""
    lo, hi = day_ns(day) // 10**9, day_ns(day) // 10**9 + 86_400
    rows, cursor = [], None
    while True:
        p = {"ticker": ticker, "limit": 1000, "min_ts": lo, "max_ts": hi}
        if cursor:
            p["cursor"] = cursor
        j = get("/margin/trades", p)
        rows += j.get("trades") or []
        cursor = j.get("cursor")
        if not cursor or not j.get("trades"):
            break
    if not rows:
        return pa.Table.from_pylist([], schema=TRADES_SCHEMA)
    df = pd.DataFrame(rows).drop_duplicates("trade_id")
    ts = pd.to_datetime(df["created_time"], utc=True, format="ISO8601")
    # min_ts/max_ts are whole seconds and may be inclusive; keep exactly [day, day+1).
    ok = (ts >= pd.Timestamp(day, tz="UTC")) & (ts < pd.Timestamp(day, tz="UTC") + pd.Timedelta(days=1))
    df, ts = df[ok.to_numpy()], ts[ok]
    side = df["taker_side"].map({"ask": 1, "bid": 2})
    if side.isna().any():
        raise ValueError(f"{ticker} {day}: unknown taker_side {set(df['taker_side'][side.isna()])}")
    # The API's order within one timestamp (a single taker sweeping several levels) is not
    # execution order. A sweep fills best price first: ascending for buys, descending for
    # sells. With that order our bar open/close match Kalshi's own candles exactly.
    order = np.lexsort((np.where(side == 2, 1, -1) * df["price"].astype("float64").to_numpy(),
                        ts.astype("int64").to_numpy()))
    df, ts, side = df.iloc[order], ts.iloc[order], side.iloc[order]
    return pa.table({
        "ticker": pa.array(df["ticker"].astype(str), pa.string()),
        "participant_timestamp": pa.array(ts.astype("int64").to_numpy(), pa.int64()).cast(UTC_TS),
        "price": pa.array(df["price"].astype("float64").to_numpy()),
        "size": pa.array(df["count"].astype("float64").to_numpy()),
        "conditions": pa.array(side.astype("int8").to_numpy()),
        "id": pa.array(df["trade_id"].astype(str), pa.string()),
    }, schema=TRADES_SCHEMA)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--through", type=dt.date.fromisoformat,
                    help="last UTC day to build (default: the latest complete UTC day)")
    ap.add_argument("--workers", type=int, default=4, help="markets fetched in parallel")
    args = ap.parse_args()
    latest = (dt.datetime.now(dt.timezone.utc) - SETTLE).date() - dt.timedelta(days=1)
    through = min(args.through or latest, latest)

    os.makedirs(ROOT, exist_ok=True)
    m = markets()
    m.to_parquet(os.path.join(ROOT, "markets.parquet"), index=False)
    days = pending_days(START, ROOT, through)
    if not days:
        log(f"kalshi perps: up to date through {through}")
        return
    log(f"kalshi perps: {len(m)} markets, days {days[0]} .. {days[-1]}")
    with concurrent.futures.ThreadPoolExecutor(args.workers) as ex:
        for day in days:
            t0 = time.time()
            tables = list(ex.map(lambda t: market_day(t, day), m["ticker"]))
            n = write_day(ROOT, day, pa.concat_tables(tables))
            log(f"kalshi perps {day}: {n:,} trades in {time.time() - t0:.1f}s")



if __name__ == "__main__":
    sys.exit(main())
