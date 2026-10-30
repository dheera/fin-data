#!/usr/bin/env python3

# Downloads xStocks (Backed tokenized equities) trades from Kraken and Bybit spot and
# builds per-day trades / minute bars / day bars, delta-updating up to the latest
# complete UTC day.
#
# ./download_xstocks.py                       # both venues, through the latest complete day
# ./download_xstocks.py --venue bybit         # one venue
# ./download_xstocks.py --through 2026-10-01  # stop earlier
#
# Output, per venue (global_xstocks/kraken, global_xstocks/bybit), same schemas for both:
#   trades/<date>.parquet        tick trades, rows ordered by (ticker, participant_timestamp)
#       ticker                 string   "X:AAPLx-USD" (Kraken) / "X:AAPLx-USDT" (Bybit)
#       participant_timestamp  timestamp[ns, tz=UTC]
#       price                  double
#       size                   double   (fractional shares)
#       conditions             int8     1 = sell aggressor, 2 = buy aggressor (as global_crypto/trades)
#       id                     int64    Kraken: venue trade id per pair.
#                                       Bybit: sequence number within Bybit's published daily file.
#       zstd, delta-encoded timestamps, 1M-row row groups (same as global_crypto/trades)
#   minute_aggs/<date>.parquet   bars built from trades, index (ticker, window_start)
#   day_aggs/<date>.parquet      UTC-day bars, index (ticker, window_start)
#       volume:double, open/close/high/low:double, transactions:int32, window_start UTC
#       Same layout as global_crypto/*_aggs except volume/prices are double (fractional
#       share volume). Minutes with no trades have no row.
#   symbols.parquet              venue symbol -> ticker map, refreshed each run
#
# Days are UTC. A trades/<date>.parquet only exists once that day is complete; aggs are
# written before it, so its presence == the whole day is done. Every day from the
# venue's first trade onwards gets a file, even if it has no rows.
#
# Sources (both public, no key):
#   Kraken: REST /0/public/Trades paged from the start of history (asset_class=tokenized_asset).
#           Fetched trades are spooled per pair in kraken/trades_v1/ so an interrupted
#           backfill resumes; the spool is deleted once the day files are written.
#   Bybit:  public.bybit.com/spot/<SYM>/<SYM>_<date>.csv.gz daily trade dumps, published
#           the following day. xStock symbols are found by matching Kraken's xStock bases
#           (AAPLx -> AAPLXUSDT), since Bybit's API is geo-blocked here.

import io
import os
import re
import sys
import time
import shutil
import argparse
import datetime as dt
import concurrent.futures

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "global_xstocks")
KRAKEN_API = "https://api.kraken.com/0/public"
BYBIT_DUMPS = "https://public.bybit.com/spot"
VENUE_START = {"kraken": dt.date(2025, 6, 30), "bybit": dt.date(2025, 6, 30)}

UTC_TS = pa.timestamp("ns", tz="UTC")
TRADES_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("participant_timestamp", UTC_TS),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("conditions", pa.int8()),
    ("id", pa.int64()),
])
ROW_GROUP_ROWS = 1_000_000
NS_PER_DAY = 86_400 * 10**9

session = requests.Session()
session.headers["User-Agent"] = "fin-xstocks-downloader/1.0"


def log(msg):
    print(f"{dt.datetime.now():%H:%M:%S} {msg}", flush=True)


def day_ns(day):
    return int(pd.Timestamp(day, tz="UTC").value)


def get_json(url, params=None, retries=8):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=30)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            j = r.json()
            errs = j.get("error") or []
            if any("Too many requests" in e or "Unavailable" in e or "Busy" in e for e in errs):
                raise requests.HTTPError(", ".join(errs))
            if errs:
                raise RuntimeError(f"{url} {params}: {errs}")
            return j["result"]
        except (requests.RequestException, ValueError) as e:
            wait = min(60, 2 ** attempt)
            log(f"  retry in {wait}s: {e}")
            time.sleep(wait)
    raise RuntimeError(f"giving up on {url} {params}")


def empty_trades():
    return pa.Table.from_pylist([], schema=TRADES_SCHEMA)


def trades_table(ticker, ts_ns, price, size, conditions, ids):
    n = len(ts_ns)
    return pa.table({
        "ticker": pa.array([ticker] * n, pa.string()),
        "participant_timestamp": pa.array(np.asarray(ts_ns, dtype="int64"), pa.int64()).cast(UTC_TS),
        "price": pa.array(np.asarray(price, dtype="float64")),
        "size": pa.array(np.asarray(size, dtype="float64")),
        "conditions": pa.array(np.asarray(conditions, dtype="int8")),
        "id": pa.array(np.asarray(ids, dtype="int64")),
    }, schema=TRADES_SCHEMA)


# ---------------------------------------------------------------------------
# Writing day files (shared by both venues)
# ---------------------------------------------------------------------------

def bars(df, freq):
    """OHLCV bars from a (ticker, participant_timestamp)-sorted trades frame."""
    cols = ["volume", "open", "close", "high", "low", "transactions"]
    if df.empty:
        idx = pd.MultiIndex.from_arrays(
            [pd.Series([], dtype="string"), pd.DatetimeIndex([], tz="UTC")], names=["ticker", "window_start"])
        out = pd.DataFrame({c: pd.Series([], dtype="float64") for c in cols[:-1]}, index=idx)
        out["transactions"] = pd.Series([], dtype="int32")
        return out
    g = df.groupby(["ticker", df["participant_timestamp"].dt.floor(freq).rename("window_start")], sort=True)
    out = g["price"].agg(open="first", close="last", high="max", low="min")
    out.insert(0, "volume", g["size"].sum())
    out["transactions"] = g.size().astype("int32")
    return out[cols]


def write_parquet_atomic(path, write):
    tmp = path + ".tmp"
    write(tmp)
    os.replace(tmp, path)


def write_day(venue_dir, day, table):
    """Write minute/day aggs then trades for one day. Trades last: its presence == done."""
    table = table.sort_by([("ticker", "ascending"), ("participant_timestamp", "ascending")])
    df = table.to_pandas()
    df["ticker"] = df["ticker"].astype(str)
    name = f"{day:%Y-%m-%d}.parquet"
    for sub, freq in (("minute_aggs", "min"), ("day_aggs", "D")):
        os.makedirs(os.path.join(venue_dir, sub), exist_ok=True)
        b = bars(df, freq)
        write_parquet_atomic(os.path.join(venue_dir, sub, name), lambda p: b.to_parquet(p))
    os.makedirs(os.path.join(venue_dir, "trades"), exist_ok=True)
    write_parquet_atomic(os.path.join(venue_dir, "trades", name), lambda p: pq.write_table(
        table, p, row_group_size=ROW_GROUP_ROWS,
        compression="zstd", compression_level=9,
        use_dictionary=["ticker", "conditions"],
        column_encoding={"participant_timestamp": "DELTA_BINARY_PACKED"},
        sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1)],
    ))
    return table.num_rows


def pending_days(first_day, venue_dir, through):
    """Days after the last complete trades/<date>.parquet (else from first_day), up to `through`."""
    tdir = os.path.join(venue_dir, "trades")
    done = sorted(f[:10] for f in os.listdir(tdir) if re.fullmatch(r"\d{4}-\d\d-\d\d\.parquet", f)) \
        if os.path.isdir(tdir) else []
    start = dt.date.fromisoformat(done[-1]) + dt.timedelta(days=1) if done else first_day
    return [start + dt.timedelta(days=i) for i in range((through - start).days + 1)]


# ---------------------------------------------------------------------------
# Kraken
# ---------------------------------------------------------------------------

def kraken_pairs():
    """Unique xStock pairs as {altname: ticker}. Kraken lists each pair twice
    (AAPLxUSD and AAPLSPVUSD) with the same altname."""
    res = get_json(f"{KRAKEN_API}/AssetPairs", {"aclass_base": "tokenized_asset"})
    pairs = {}
    for v in res.values():
        quote = {"ZUSD": "USD"}.get(v["quote"], v["quote"])
        pairs[v["altname"]] = dict(symbol=v["altname"], ticker=f"X:{v['base']}-{quote}",
                                   base=v["base"], quote=quote, status=v.get("status"))
    return dict(sorted(pairs.items()))


def kraken_fetch_pair(pair, ticker, start_ns, end_ns, spool_path):
    """All trades of `pair` with start_ns <= t < end_ns, resuming from a spool file.
    Returns an Arrow table. The spool is rewritten every so often while paging."""
    parts = []
    since = start_ns
    if os.path.exists(spool_path):
        t = pq.read_table(spool_path)
        through_ns = int((t.schema.metadata or {}).get(b"complete_through_ns", 0))
        if through_ns >= end_ns:
            return t.filter(pc.less(t["participant_timestamp"], pa.scalar(end_ns, UTC_TS)))
        if t.num_rows:
            parts.append(t)
            since = t["participant_timestamp"].cast(pa.int64()).to_numpy().max() - 1

    def save(complete):
        tbl = pa.concat_tables(parts) if parts else empty_trades()
        meta = {b"complete_through_ns": str(end_ns).encode()} if complete else {}
        tbl = tbl.replace_schema_metadata({**(tbl.schema.metadata or {}), **meta})
        write_parquet_atomic(spool_path, lambda p: pq.write_table(tbl, p, compression="zstd"))

    pages = 0
    while True:
        res = get_json(f"{KRAKEN_API}/Trades",
                       {"pair": pair, "since": since, "count": 1000, "asset_class": "tokenized_asset"})
        rows = next(v for k, v in res.items() if k != "last")
        last = int(res["last"])
        if rows:
            ts = np.array([round(float(r[2]) * 1e6) for r in rows], dtype="int64") * 1000
            keep = (ts >= start_ns) & (ts < end_ns)
            if keep.any():
                k = np.nonzero(keep)[0]
                parts.append(trades_table(
                    ticker, ts[k], [float(rows[i][0]) for i in k], [float(rows[i][1]) for i in k],
                    [1 if rows[i][3] == "s" else 2 for i in k], [int(rows[i][6]) for i in k]))
        pages += 1
        if pages % 50 == 0:
            save(False)
            log(f"  {pair}: {sum(p.num_rows for p in parts):,} trades so far")
        # Done once we've paged past the window or reached the head of the tape.
        if not rows or len(rows) < 1000 or ts.max() >= end_ns or last <= since:
            break
        since = last

    tbl = pa.concat_tables(parts) if parts else empty_trades()
    # Resuming from max(ts)-1 can refetch a few trades; ids are unique per pair.
    if tbl.num_rows:
        tbl = tbl.take(np.unique(tbl["id"].to_numpy(), return_index=True)[1])
    parts[:] = [tbl]
    save(True)
    return tbl


def update_kraken(through):
    vdir = os.path.join(ROOT, "kraken")
    os.makedirs(vdir, exist_ok=True)
    days = pending_days(VENUE_START["kraken"], vdir, through)
    pairs = kraken_pairs()
    pd.DataFrame(pairs.values()).to_parquet(os.path.join(vdir, "symbols.parquet"), index=False)
    if not days:
        log(f"kraken: up to date through {through}")
        return
    log(f"kraken: {len(pairs)} pairs, days {days[0]} .. {days[-1]}")
    start_ns, end_ns = day_ns(days[0]), day_ns(days[-1]) + NS_PER_DAY
    spool = os.path.join(vdir, "trades_v1", f"from_{days[0]}")
    os.makedirs(spool, exist_ok=True)

    tables = []
    for i, (pair, info) in enumerate(pairs.items(), 1):
        t = kraken_fetch_pair(pair, info["ticker"], start_ns, end_ns, os.path.join(spool, f"{pair}.parquet"))
        log(f"kraken [{i}/{len(pairs)}] {pair}: {t.num_rows:,} trades")
        tables.append(t)

    all_trades = pa.concat_tables(tables) if tables else empty_trades()
    day_idx = all_trades["participant_timestamp"].cast(pa.int64()).to_numpy() // NS_PER_DAY
    for day in days:
        mask = day_idx == day_ns(day) // NS_PER_DAY
        n = write_day(vdir, day, all_trades.filter(pa.array(mask)))
        log(f"kraken {day}: {n:,} trades")
    shutil.rmtree(spool)


# ---------------------------------------------------------------------------
# Bybit
# ---------------------------------------------------------------------------

def bybit_list(symbol):
    """{date: url} of the daily dump files for a symbol ({} if no such symbol)."""
    r = session.get(f"{BYBIT_DUMPS}/{symbol}/", timeout=30)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return {dt.date.fromisoformat(d): f"{BYBIT_DUMPS}/{symbol}/{f}"
            for f, d in re.findall(rf'href="({symbol}_(\d{{4}}-\d\d-\d\d)\.csv\.gz)"', r.text)}


def bybit_symbols():
    """xStock symbols on Bybit spot, as {symbol: info}, matched against Kraken's xStock bases."""
    r = session.get(f"{BYBIT_DUMPS}/", timeout=30)
    r.raise_for_status()
    listed = set(re.findall(r'href="([A-Z0-9]+)/?"', r.text))
    out = {}
    for info in kraken_pairs().values():
        sym = re.sub(r"[^A-Z0-9]", "", info["base"].upper()) + "USDT"
        if sym in listed:
            out[sym] = dict(symbol=sym, ticker=f"X:{info['base']}-USDT", base=info["base"], quote="USDT")
    return dict(sorted(out.items()))


def bybit_day_file(url, ticker, day):
    # Bybit's CDN answers bursts with temporary 403s, so back off and retry those too.
    for attempt in range(10):
        try:
            r = session.get(url, timeout=60)
            r.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt == 9:
                raise
            log(f"  retry in {min(120, 2 ** attempt)}s: {e}")
            time.sleep(min(120, 2 ** attempt))
    # Header is "id,timestamp,price,volume,side" but newer rows carry a 6th field (rpi).
    df = pd.read_csv(io.BytesIO(r.content), compression="gzip", header=None, skiprows=1,
                     names=["id", "timestamp", "price", "volume", "side", "rpi"], usecols=range(5))
    ts = df["timestamp"]
    if pd.api.types.is_integer_dtype(ts) and (ts.empty or ts.max() >= 1e11):
        ts_ns = ts.to_numpy("int64") * 1_000_000  # epoch ms
    else:
        ts_ns = (ts.to_numpy("float64") * 1e9).round().astype("int64")  # epoch seconds
    t = trades_table(ticker, ts_ns, df["price"], df["volume"],
                     np.where(df["side"].str.lower() == "sell", 1, 2), df["id"])
    lo, hi = day_ns(day), day_ns(day) + NS_PER_DAY
    ok = (ts_ns >= lo) & (ts_ns < hi)
    if not ok.all():
        log(f"  WARNING {url}: dropped {(~ok).sum()} rows outside {day}")
        t = t.filter(pa.array(ok))
    return t


def update_bybit(through, workers):
    vdir = os.path.join(ROOT, "bybit")
    os.makedirs(vdir, exist_ok=True)
    syms = bybit_symbols()
    with concurrent.futures.ThreadPoolExecutor(workers) as ex:
        listings = dict(zip(syms, ex.map(bybit_list, syms)))
    # Some bases collide with older crypto tokens (VRTXUSDT is not Vertiv's VRTx); a real
    # xStock has no dumps before the xStocks launch.
    for s in [s for s, v in listings.items() if v and min(v) < VENUE_START["bybit"]]:
        log(f"bybit: skipping {s}, trades since {min(listings[s])} predate xStocks")
        del syms[s], listings[s]
    pd.DataFrame(syms.values()).to_parquet(os.path.join(vdir, "symbols.parquet"), index=False)
    # Bybit publishes day D on D+1 for all symbols at once; don't go past the newest dump.
    published = max((max(v) for v in listings.values() if v), default=None)
    if published is None:
        log("bybit: no dumps found")
        return
    days = pending_days(VENUE_START["bybit"], vdir, min(through, published))
    if not days:
        log(f"bybit: up to date through {min(through, published)}")
        return
    log(f"bybit: {len(syms)} symbols ({', '.join(syms)}), days {days[0]} .. {days[-1]}")
    with concurrent.futures.ThreadPoolExecutor(workers) as ex:
        for day in days:
            jobs = [ex.submit(bybit_day_file, listings[s][day], syms[s]["ticker"], day)
                    for s in syms if day in listings[s]]
            tables = [j.result() for j in jobs]
            n = write_day(vdir, day, pa.concat_tables(tables) if tables else empty_trades())
            log(f"bybit {day}: {n:,} trades from {len(jobs)} symbols")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--venue", nargs="+", choices=["kraken", "bybit"], default=["kraken", "bybit"])
    ap.add_argument("--through", type=dt.date.fromisoformat,
                    help="last UTC day to build (default: yesterday UTC, the latest complete day)")
    ap.add_argument("--workers", type=int, default=4, help="parallel Bybit downloads")
    args = ap.parse_args()
    through = args.through or (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1))
    if "kraken" in args.venue:
        update_kraken(through)
    if "bybit" in args.venue:
        update_bybit(through, args.workers)


if __name__ == "__main__":
    sys.exit(main())
