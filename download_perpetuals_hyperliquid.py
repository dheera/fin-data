#!/usr/bin/env python3

# Downloads Hyperliquid perps history (core + every HIP-3 dex) from Hydromancer's free
# requester-pays S3 archive and writes per-day parquet, delta-updating up to the latest
# day Hydromancer has published for core Hyperliquid (it uploads roughly weekly).
#
# ./download_perpetuals_hyperliquid.py                       # everything new
# ./download_perpetuals_hyperliquid.py --through 2025-08-31  # stop earlier
#
# Needs aws.json ({"access_key_id", "secret_access_key"}); you pay AWS egress
# (~$0.11/GB from ap-northeast-1; a full backfill was ~245 GB in Oct 2026).
#
# Output (perpetuals/hyperliquid). Tickers are Hyperliquid's own: "BTC" for core,
# "<dex>:<coin>" for HIP-3 ("xyz:NVDA"). All times UTC.
#   trades/<date>.parquet        one row per trade (the taker's fill), same schema and
#                                writer as global_xstocks/ and perpetuals/kalshi/:
#                                ticker, participant_timestamp, price, size,
#                                conditions (1 sell / 2 buy aggressor), id (= trade_id,
#                                unique per ticker only)
#   minute_aggs/, day_aggs/      bars from trades, index (ticker, window_start)
#   fills/<date>/<date>-<dex>.parquet
#                                every fill, both sides, rows ordered by (ticker, time):
#                                Hydromancer's columns with coin -> ticker,
#                                timestamp -> participant_timestamp (ns), decimals -> double,
#                                uint64 -> int64; dex/asset_class/base/quote dropped
#                                (implied by the ticker). dex "hyperliquid" = core.
#   Hydromancer's 1 s candles are not downloaded: they are redundant with trades.
#   books_1m/<date>.parquet       20-level L2 snapshot about once a minute:
#                                ticker, block_time, block_number,
#                                bids/asks: list<struct<price double, size double, n int32>>
#   positions/<date>.parquet     Hydromancer's daily snapshot of every open perp position:
#                                snapshot_time, block_number, user, ticker, size, notional,
#                                entry_price, liquidation_price, leverage_type, leverage,
#                                funding_pnl, account_value, account_mode
#
# trades/<date>.parquet is written last, so its presence == that whole day is done.
# fills/<date>/ is built in fills/<date>.tmp/ and renamed into place.
# A dataset with no source rows for a day (e.g. books_1m before 2025-12-15) gets no file.

import os
import re
import sys
import json
import time
import shutil
import argparse
import datetime as dt
import urllib.parse
import concurrent.futures

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from download_xstocks import write_day, pending_days, log, UTC_TS

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "perpetuals", "hyperliquid")
STAGING = "/scratch1/hyperliquid_staging"  # local disk, never the NFS mount
BUCKET, REGION = "hydromancer-reservoir", "ap-northeast-1"
CORE = "hyperliquid"
START = dt.date(2025, 7, 28)  # first day of core fills in the archive
ROW_GROUP_ROWS = 1_000_000

TRADES_SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("participant_timestamp", UTC_TS),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("conditions", pa.int8()),
    ("id", pa.int64()),
])
FILLS_DICT_COLUMNS = ["ticker", "side", "direction", "fee_token", "address", "builder",
                      "liquidation_method", "crossed", "is_liquidation"]
LEVEL = pa.struct([("price", pa.float64()), ("size", pa.float64()), ("n", pa.int32())])


def s3():
    c = json.load(open(os.path.join(HERE, "aws.json")))
    return boto3.Session(aws_access_key_id=c["access_key_id"],
                         aws_secret_access_key=c["secret_access_key"]).client(
        "s3", region_name=REGION, config=Config(retries={"max_attempts": 10, "mode": "adaptive"},
                                                max_pool_connections=64))


def ls(client, prefix, dirs=False):
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/" if dirs else "", RequestPayer="requester"):
        out += [p["Prefix"] for p in page.get("CommonPrefixes", [])] if dirs else \
               [o["Key"] for o in page.get("Contents", [])]
    return out


def fetch(client, key, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    client.download_file(BUCKET, key, dest, ExtraArgs={"RequestPayer": "requester"},
                         Config=TransferConfig(max_concurrency=16, multipart_chunksize=16 << 20))
    return dest


def write_table(table, path, **kw):
    tmp = path + ".tmp"
    pq.write_table(table, tmp, row_group_size=ROW_GROUP_ROWS, compression="zstd",
                   compression_level=9, **kw)
    os.replace(tmp, path)


def to_float(col):
    return pc.cast(col, pa.float64()) if pa.types.is_decimal(col.type) else col


def ms_to_ns(col):
    return pc.cast(pc.cast(col, pa.timestamp("ms", tz="UTC")), UTC_TS)


# ---------------------------------------------------------------------------
# Per-dataset conversions (one day, all dexes)
# ---------------------------------------------------------------------------

def convert_fills(t):
    """Hydromancer fills -> our fills table, ordered by (ticker, time, trade_id)."""
    cols = {}
    for name in t.column_names:
        c = t[name]
        if name in ("dex", "asset_class", "base_symbol", "quote_symbol"):
            continue
        if name == "coin":
            name = "ticker"
        elif name == "timestamp":
            name, c = "participant_timestamp", ms_to_ns(c)
        elif pa.types.is_decimal(c.type):
            c = to_float(c)
        elif pa.types.is_uint64(c.type):
            c = pc.cast(c, pa.int64())
        cols[name] = c
    out = pa.table(cols)
    # Stable sort: trades sharing a millisecond keep the archive's execution order
    # (trade_id is not chronological), so first/last per bar are the true open/close.
    return out.sort_by([("ticker", "ascending"), ("participant_timestamp", "ascending")])


def trades_from_fills(f, label):
    """One row per trade: the taker (crossed) side. "Net Child Vaults" fills are internal
    vault position netting, not market trades (Hydromancer's own candles exclude them too)."""
    f = f.filter(pc.invert(pc.equal(f["direction"], "Net Child Vaults")))
    tk = f.filter(f["crossed"])
    # trade_id is only unique per coin (different coins occasionally share one).
    n_ids = f.group_by(["ticker", "trade_id"]).aggregate([]).num_rows
    if tk.num_rows != n_ids:
        log(f"  WARNING {label}: {tk.num_rows:,} taker fills for {n_ids:,} trade ids")
    return pa.table({
        "ticker": tk["ticker"],
        "participant_timestamp": tk["participant_timestamp"],
        "price": tk["price"],
        "size": tk["size"],
        "conditions": pc.cast(pc.if_else(pc.equal(tk["side"], "buy"), 2, 1), pa.int8()),
        "id": tk["trade_id"],
    }, schema=TRADES_SCHEMA)


def convert_levels(col):
    """list<struct<px:str, sz:str, n:uint32>> -> list<struct<price:double, size:double, n:int32>>."""
    col = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    flat = col.values
    levels = pa.StructArray.from_arrays(
        [pc.cast(pc.struct_field(flat, "px"), pa.float64()), pc.cast(pc.struct_field(flat, "sz"), pa.float64()),
         pc.cast(pc.struct_field(flat, "n"), pa.int32())], fields=list(LEVEL), mask=flat.is_null())
    return pa.ListArray.from_arrays(pa.array(col.offsets.to_numpy()), levels,
                                    type=pa.list_(pa.field("element", LEVEL)), mask=col.is_null())


def convert_book(parts):
    """parts: [(ticker, table)] -> one table ordered by (ticker, block_time)."""
    out = []
    for ticker, t in parts:
        if not t.num_rows:
            continue
        t = t.sort_by("block_time_ms")
        out.append(pa.table({
            "ticker": pa.array([ticker] * t.num_rows, pa.string()),
            "block_time": ms_to_ns(t["block_time_ms"]),
            "block_number": pc.cast(t["block_number"], pa.int64()),
            "bids": convert_levels(t["bids"]),
            "asks": convert_levels(t["asks"]),
        }))
    return pa.concat_tables(out).sort_by([("ticker", "ascending"), ("block_time", "ascending")]) if out else None


def convert_positions(parts):
    """parts: [(snapshot key, table)] -> one table ordered by (ticker, snapshot_time, user)."""
    out = []
    for key, t in parts:
        block, ms = map(int, re.match(r"(\d+)_(\d+)\.parquet$", key.rsplit("/", 1)[1]).groups())
        n = t.num_rows
        cols = {
            "snapshot_time": pc.cast(pa.array(np.full(n, ms, "int64"), pa.int64()).cast(pa.timestamp("ms", tz="UTC")), UTC_TS),
            "block_number": pa.array(np.full(n, block, "int64")),
        }
        for name in t.column_names:
            cols["ticker" if name == "market" else name] = \
                pc.cast(t[name], pa.int32()) if name == "leverage" else t[name]
        out.append(pa.table(cols))
    if not out:
        return None
    return pa.concat_tables(out, promote_options="permissive").sort_by(
        [("ticker", "ascending"), ("snapshot_time", "ascending"), ("user", "ascending")])


# ---------------------------------------------------------------------------
# One day
# ---------------------------------------------------------------------------

def book_ticker(dex, key):
    coin = urllib.parse.unquote(key.rsplit("/", 1)[1][:-len(".parquet")])
    return coin if dex == CORE or ":" in coin else f"{dex}:{coin}"


def do_day(day, dexes):
    t0 = time.time()
    client = s3()
    d = f"{day:%Y-%m-%d}"
    stage = os.path.join(STAGING, d)
    shutil.rmtree(stage, ignore_errors=True)
    os.makedirs(stage)
    try:
        # List what exists for this day, per dex and dataset.
        keys = {}
        for dex in dexes:
            base = f"by_dex/{dex}"
            keys[(dex, "fills")] = ls(client, f"{base}/fills/perp/all/date={d}/")
            keys[(dex, "book")] = ls(client, f"{base}/orderbook/1m/perps/date={d}/")
            keys[(dex, "positions")] = ls(client, f"{base}/snapshots/perp/date={d}/")
        jobs = [(k, os.path.join(stage, k)) for ks in keys.values() for k in ks]
        with concurrent.futures.ThreadPoolExecutor(32) as ex:
            list(ex.map(lambda kd: fetch(client, *kd), jobs))
        nbytes = sum(os.path.getsize(p) for _, p in jobs)
        local = lambda k: os.path.join(stage, k)

        # books_1m
        book = convert_book([(book_ticker(dex, k), pq.read_table(local(k)))
                             for dex in dexes for k in keys[(dex, "book")]])
        if book is not None:
            os.makedirs(os.path.join(ROOT, "books_1m"), exist_ok=True)
            write_table(book, os.path.join(ROOT, "books_1m", f"{d}.parquet"),
                        use_dictionary=["ticker"], sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1)],
                        column_encoding={"block_time": "DELTA_BINARY_PACKED", "block_number": "DELTA_BINARY_PACKED"})

        # positions
        pos = convert_positions([(k, pq.read_table(local(k)))
                                 for dex in dexes for k in keys[(dex, "positions")]])
        if pos is not None:
            os.makedirs(os.path.join(ROOT, "positions"), exist_ok=True)
            write_table(pos, os.path.join(ROOT, "positions", f"{d}.parquet"),
                        sorting_columns=[pq.SortingColumn(2)])

        # fills (per dex) and trades
        fdir = os.path.join(ROOT, "fills", d)
        tmpdir = fdir + ".tmp"
        shutil.rmtree(tmpdir, ignore_errors=True)
        os.makedirs(tmpdir)
        trades, nfills = [], 0
        for dex in dexes:
            for k in keys[(dex, "fills")]:
                f = convert_fills(pq.read_table(local(k)))
                if not f.num_rows:
                    continue
                nfills += f.num_rows
                write_table(f, os.path.join(tmpdir, f"{d}-{dex}.parquet"),
                            use_dictionary=[c for c in FILLS_DICT_COLUMNS if c in f.column_names],
                            sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1)],
                            column_encoding={"participant_timestamp": "DELTA_BINARY_PACKED"})
                trades.append(trades_from_fills(f, f"{d} {dex}"))
                del f
        shutil.rmtree(fdir, ignore_errors=True)
        os.replace(tmpdir, fdir)
        ntrades = write_day(ROOT, day, pa.concat_tables(trades) if trades else
                            pa.Table.from_pylist([], schema=TRADES_SCHEMA))
        return (f"{d}: {nbytes / 1e9:.2f} GB from S3, {nfills:,} fills, {ntrades:,} trades, "
                f"{0 if book is None else book.num_rows:,} books, {0 if pos is None else pos.num_rows:,} positions "
                f"in {time.time() - t0:.0f}s")
    finally:
        shutil.rmtree(stage, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--through", type=dt.date.fromisoformat,
                    help="last UTC day to build (default: latest day Hydromancer has for core)")
    ap.add_argument("--workers", type=int, default=3, help="days processed in parallel")
    args = ap.parse_args()

    client = s3()
    dexes = sorted(p.split("/")[1] for p in ls(client, "by_dex/", dirs=True))
    published = [dt.date.fromisoformat(p.rstrip("/").split("date=")[1])
                 for p in ls(client, f"by_dex/{CORE}/fills/perp/all/", dirs=True)]
    latest = max(published)
    through = min(args.through or latest, latest)
    days = pending_days(START, ROOT, through)
    if not days:
        log(f"hyperliquid: up to date through {through}")
        return
    log(f"hyperliquid: dexes {', '.join(dexes)}; days {days[0]} .. {days[-1]}")
    os.makedirs(STAGING, exist_ok=True)

    # Days run in parallel, so a later day can finish before an earlier one fails. Since
    # pending_days() resumes after the newest trades/<date>.parquet, un-commit every day
    # after a failure (once in-flight workers have stopped) so no gap is skipped.
    failed = None
    with concurrent.futures.ProcessPoolExecutor(args.workers) as ex:
        futs = {day: ex.submit(do_day, day, dexes) for day in days}
        for day in days:
            try:
                log(f"hyperliquid {futs[day].result()}")
            except Exception as e:
                log(f"hyperliquid {day}: FAILED {e!r}; it and later days will be redone next run")
                failed = day
                for f in futs.values():
                    f.cancel()
                break
    if failed:
        for later in days[days.index(failed) + 1:]:
            p = os.path.join(ROOT, "trades", f"{later:%Y-%m-%d}.parquet")
            if os.path.exists(p):
                os.remove(p)
        return 1


if __name__ == "__main__":
    sys.exit(main())
