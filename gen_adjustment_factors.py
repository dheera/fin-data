#!/usr/bin/env python3
"""
Build us_stocks_sip/adjustment_factors.parquet: one row per (security, event date) with the
cumulative split and split+dividend adjustment factors, from splits.parquet,
dividends.parquet, security_history.parquet and day_aggs_by_ticker/ (for the close before
each ex-date).

Factors are per security (security_history.parquet), not per symbol: QQQ's history includes
its QQQQ years, so a QQQQ-era event adjusts the whole chain, and META's factors don't pick up
the Roundhill ETF that held the symbol first. Each ticker's split/dividend lands on the
security that held that ticker on the event date.

Usage: multiply a raw price on New York date d by the factor of the security's FIRST row
with date > d (1.0 when there is none). Factors are anchored so the latest bar is 1.0,
so the file changes every time a new event lands; it is rebuilt from scratch each run.

    split_adj    splits only (share-count adjusted prices)
    total_adj    splits and cash dividends (total-return prices)

    sid = security_id from security_history.parquet (e.g. "BBG000BSWKH7" for QQQ)
    f = pd.read_parquet("us_stocks_sip/adjustment_factors.parquet", filters=[("security_id", "=", sid)])
    i = np.searchsorted(f["date"].values, bar_dates, side="right")      # first event after d
    adj = np.r_[f["split_adj"].values, 1.0][i] * raw

Columns: security_id, ticker (the security's current ticker), date, and per event
(CRSP FACPR-style, multiply prices before `date` by them):

    split_ratio  split_from / split_to (product of every split that day)
    div_factor   1 - dividend / ref_close, where ref_close is the last close strictly before
                 `date`, put in post-split terms if a split executes the same day
    dividend     summed USD cash amount (regular + special) that day; 0 for split-only rows
    ref_close    the close used for div_factor (NaN for split-only rows)

Unlike gen_aggs_adjusted.py, events apply to every bar strictly before their date even when
there is no bar ON that date (holiday/weekend split dates, halted days), non-USD
dividends are skipped, and events after the security's last bar (declared future
dividends) are left out until the bars catch up.

Run after gen_security_history.py. Writes <out>.tmp and renames it into place. File
metadata holds the build time, asof (the latest bar date seen) and the input mtimes.
"""
import argparse
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

NY_TZ = "America/New_York"
RELABEL_DAYS = 30


def load_events(splits_file: Path, dividends_file: Path):
    sp = pd.read_parquet(splits_file).reset_index()
    sp = sp[np.isfinite(sp.split_from) & np.isfinite(sp.split_to) & (sp.split_from > 0) & (sp.split_to > 0)
            & sp.execution_date.notna()]
    sp = pd.DataFrame({"ticker": sp.ticker.astype(str), "date": pd.to_datetime(sp.execution_date).values.astype("datetime64[D]"),
                       "split_ratio": sp.split_from / sp.split_to})

    dv = pd.read_parquet(dividends_file, columns=["cash_amount", "currency"]).reset_index()
    n_all = len(dv)
    dv = dv[(dv.currency == "USD") & np.isfinite(dv.cash_amount) & (dv.cash_amount > 0) & dv.ex_dividend_date.notna()]
    print(f"dividends: kept {len(dv):,} of {n_all:,} (USD, positive amount)")
    dv = pd.DataFrame({"ticker": dv.ticker.astype(str), "date": pd.to_datetime(dv.ex_dividend_date).values.astype("datetime64[D]"),
                       "dividend": dv.cash_amount.astype(float)})
    return sp, dv


def assign_to_securities(events: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    """Tag each (ticker, date) event with the security holding that ticker then: the segment
    with start <= date, until that security's next segment starts (or the day after its last bar)."""
    h = hist.sort_values(["security_id", "start"])
    start = h["start"].values.astype("datetime64[D]")
    nxt = h.groupby("security_id")["start"].shift(-1).values.astype("datetime64[D]")
    until = np.where(np.isnat(nxt), h["end"].values.astype("datetime64[D]") + np.timedelta64(1, "D"), nxt)
    windows = defaultdict(list)                          # ticker -> [(security_id, start, until)]
    for sid, tk, a, b in zip(h["security_id"], h["ticker"], start, until):
        windows[tk].append((sid, a, b))
    out = []
    for tk, g in events.groupby("ticker", sort=False):
        if tk not in windows:
            continue
        d = g["date"].values.astype("datetime64[D]")
        sid = np.full(len(d), None, dtype=object)
        for s_, a, b in windows[tk]:
            sid[(d >= a) & (d < b)] = s_
        # Polygon re-labels some events with the symbol a security took later (QQQ's 2011-03-18
        # dividend, paid while it traded as QQQQ): give an unplaced event to the security that
        # takes this ticker within RELABEL_DAYS
        for s_, a, b in windows[tk]:
            sid[(sid == None) & (d < a) & (d >= a - np.timedelta64(RELABEL_DAYS, "D"))] = s_  # noqa: E711
        out.append(g.assign(security_id=sid)[sid != None])  # noqa: E711
    return pd.concat(out, ignore_index=True)


def security_bars(segs: pd.DataFrame, day_dir: Path):
    """Daily closes of one security: each segment's ticker clipped to [start, end]."""
    days, closes = [], []
    for tk, a, b in zip(segs["ticker"], segs["start"].values.astype("datetime64[D]"), segs["end"].values.astype("datetime64[D]")):
        p = day_dir / f"{tk}.parquet"
        if not p.exists():
            continue
        t = pq.read_table(p, columns=["window_start", "close"]).to_pandas()
        if "window_start" not in t.columns:
            t = t.reset_index()
        ws = pd.to_datetime(t["window_start"])
        ws = ws.dt.tz_convert(NY_TZ).dt.tz_localize(None) if ws.dt.tz is not None else ws
        bd = ws.values.astype("datetime64[D]")
        c = t["close"].to_numpy(np.float64)
        m = (bd >= a) & (bd <= b) & np.isfinite(c) & (c > 0)
        days.append(bd[m])
        closes.append(c[m])
    if not days:
        return np.empty(0, "datetime64[D]"), np.empty(0)
    bd, close = np.concatenate(days), np.concatenate(closes)
    o = np.argsort(bd, kind="stable")
    return bd[o], close[o]


def security_factors(sid: str, current: str, segs: pd.DataFrame, ev: pd.DataFrame, day_dir: Path):
    """Factors for one security's events, or (None, reason) when its bars don't allow any."""
    bd, close = security_bars(segs, day_dir)
    if not len(bd):
        return None, "no_bars"

    # an event only matters if some bar precedes it; one after the last bar hasn't happened in our data yet
    ev = ev[(ev.date > bd[0]) & (ev.date <= bd[-1])]
    if ev.empty:
        return None, "out_of_range"
    d = ev.date.values.astype("datetime64[D]")
    split_ratio = ev.split_ratio.to_numpy(np.float64)
    dividend = ev.dividend.to_numpy(np.float64)

    j = np.searchsorted(bd, d, side="left") - 1          # last bar strictly before the event
    ref = close[j] * split_ratio                         # same-day split: dividends are quoted post-split
    div_factor = np.ones(len(d))
    has_div = dividend > 0
    ok = has_div & (dividend < ref)
    div_factor[ok] = 1.0 - dividend[ok] / ref[ok]
    bad_div = int((has_div & ~ok).sum())

    keep = (split_ratio != 1.0) | ok
    if not keep.any():
        return None, "out_of_range"
    d, split_ratio, div_factor = d[keep], split_ratio[keep], div_factor[keep]
    dividend = np.where(ok, dividend, 0.0)[keep]
    ref = np.where(ok, ref, np.nan)[keep]

    # reverse cumulative product: bars before event i are scaled by every event from i onward
    split_adj = np.cumprod(split_ratio[::-1])[::-1]
    total_adj = np.cumprod((split_ratio * div_factor)[::-1])[::-1]
    out = pd.DataFrame({"security_id": sid, "ticker": current, "date": d, "split_ratio": split_ratio,
                        "div_factor": div_factor, "dividend": dividend, "ref_close": ref,
                        "split_adj": split_adj, "total_adj": total_adj})
    return (out, bd[-1], bad_div), "ok"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="us_stocks_sip", help="us_stocks_sip directory (default: %(default)s)")
    ap.add_argument("--output", default=None, help="default: <base>/adjustment_factors.parquet")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    base = Path(args.base)
    splits_file, dividends_file = base / "splits.parquet", base / "dividends.parquet"
    hist_file, day_dir = base / "security_history.parquet", base / "day_aggs_by_ticker"
    out_file = Path(args.output) if args.output else base / "adjustment_factors.parquet"
    t0 = time.time()

    hist = pd.read_parquet(hist_file, columns=["security_id", "current_ticker", "ticker", "start", "end"])
    hist["start"] = pd.to_datetime(hist["start"])
    hist["end"] = pd.to_datetime(hist["end"])
    sp, dv = load_events(splits_file, dividends_file)
    sp, dv = assign_to_securities(sp, hist), assign_to_securities(dv, hist)
    # the same split or dividend can be reported under both an old and a new symbol: count it once
    sp = sp.drop_duplicates(["security_id", "date", "split_ratio"]).groupby(["security_id", "date"], as_index=False).split_ratio.prod()
    dv = dv.drop_duplicates(["security_id", "date", "dividend"]).groupby(["security_id", "date"], as_index=False).dividend.sum()
    ev = pd.merge(sp, dv, on=["security_id", "date"], how="outer")
    ev["split_ratio"] = ev.split_ratio.fillna(1.0)
    ev["dividend"] = ev.dividend.fillna(0.0)
    groups = {sid: g.sort_values("date") for sid, g in ev.groupby("security_id", sort=False)}
    segs_of = {sid: g for sid, g in hist[hist.security_id.isin(groups)].groupby("security_id", sort=False)}
    print(f"{len(ev):,} event days across {len(groups):,} securities ({time.time() - t0:.1f}s)")

    frames, asof, stats = [], None, defaultdict(int)
    bad_div = 0
    with ThreadPoolExecutor(args.workers) as ex:
        work = ((sid, segs_of[sid]["current_ticker"].iloc[0], segs_of[sid], g) for sid, g in groups.items())
        for res, why in ex.map(lambda w: security_factors(*w, day_dir), work):
            stats[why] += 1
            if res is not None:
                f, last, nbad = res
                frames.append(f)
                bad_div += nbad
                asof = last if asof is None else max(asof, last)
    if not frames:
        raise SystemExit("no adjustment factors computed")
    out = pd.concat(frames, ignore_index=True).sort_values(["security_id", "date"], kind="stable").reset_index(drop=True)
    print(f"securities: {stats['ok']:,} with factors, {stats['no_bars']:,} without day bars, "
          f"{stats['out_of_range']:,} with no events inside their bar history")
    print(f"skipped {bad_div:,} dividends >= the prior close (bad data)")

    table = pa.Table.from_pandas(out, preserve_index=False)
    table = table.cast(pa.schema([("security_id", pa.string()), ("ticker", pa.string()), ("date", pa.date32()),
                                  ("split_ratio", pa.float64()), ("div_factor", pa.float64()), ("dividend", pa.float64()),
                                  ("ref_close", pa.float64()), ("split_adj", pa.float64()), ("total_adj", pa.float64())]))
    meta = {"built": pd.Timestamp.now(tz=NY_TZ).isoformat(timespec="seconds"), "asof": str(asof),
            "inputs": {str(p): os.path.getmtime(p) for p in (splits_file, dividends_file, hist_file)},
            "usage": "price_adj = raw * factor of the security's first row with date > bar date (1.0 if none)"}
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"adjustment_factors": json.dumps(meta).encode()})
    tmp = out_file.with_name(out_file.name + ".tmp")
    pq.write_table(table, tmp, row_group_size=256_000)
    os.replace(tmp, out_file)
    print(f"wrote {out_file}: {len(out):,} rows, asof {asof} ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
