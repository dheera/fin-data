#!/usr/bin/env python3
"""
Build us_stocks_sip/security_history.parquet: which symbol each security traded under, when.

Bars, splits and dividends are keyed by the symbol as it was on the day, so one security's
history can be spread over several symbols (QQQ was QQQQ 2004-12-01 .. 2011-03-22, META was
FB) and one symbol's file can hold several securities (META.parquet starts with Roundhill's
Metaverse ETF). Each output row is a segment: the bars of `ticker` dated start..end
(inclusive, New York dates) belong to `security_id`.

    security_id     composite FIGI when the security has one, else "<ticker>@<first date>"
    current_ticker  the ticker of the security's latest segment (what you'd type today)
    ticker, start, end, n_bars, first_close, last_close
    source          figi_event   assigned by a Polygon ticker_change event (ticker_changes.parquet)
                    figi_current the FIGI's current ticker in tickers.parquet, no event for it
                    default      no FIGI claims these bars; one security per unclaimed run, split
                                 at gaps of a year or more (TT 2003-2005 vs Trane from 2007)
    link            how this segment joined the previous one of the same security:
                    figi | handoff (same CIK, predecessor delisted the day the successor starts) |
                    continuity (same CIK, next bar within 10 days at a similar price) | "" (first)
    cik, name

To read a security's full history: take every row with its security_id, sorted by start,
and read each `ticker`'s bars clipped to [start, end].

Rules:
- FIGI claims come from ticker_change events: a FIGI owns a ticker from its event date until
  its next event. Its current ticker (tickers.parquet) owns from the last event for that
  ticker, or, with no such event, from the start of the ticker's final contiguous run of bars.
  When claims overlap, the ticker's current owner wins, then the later claim.
- A claim extends back over earlier unclaimed bars of the same ticker as long as there's no
  gap of a year or more (Polygon's event dates are often late: QQQ's only event is 2018-06-04).
- Unclaimed runs (mostly delisted tickers, which have no FIGI) link to a same-CIK segment that
  starts within 10 days after they end: on a handoff (delisting date = successor's first bar)
  regardless of price (KFT -> MDLZ dropped 34% on the Kraft Foods Group spinoff), otherwise
  only at a similar price (within 15%, or a split ratio). Both sides must be the same security
  type (common, ETF, unit, warrant, ...) and the CIK must not have more than MAX_CIK_PEERS
  securities trading around the boundary. Two FIGIs are never merged. The segment just before an unclaimed
  run links the same way; it may lack a CIK if it has the same ticker as the run's successor
  (QQQ 2003-2004 -> QQQQ -> QQQ). Ambiguous matches (several candidates at similar prices,
  e.g. share classes) are not linked.
- Within a security, a segment overlapping an earlier one (a when-issued ticker like MDLZV
  trading alongside KFT) is trimmed to start after it, or dropped.

Run after gen_aggs_by_ticker.py --agg_type day and download_ticker_changes.py, before
gen_adjustment_factors.py. Writes <out>.tmp and renames it into place.
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
EXTEND_GAP_DAYS = 365      # a claim extends back over unclaimed bars until a gap this long
LINK_DAYS = 10             # successor's first bar within this many calendar days
HANDOFF_DAYS = 3           # delisting date within this many days of the successor's first bar
PRICE_TOL = 0.15           # |log price ratio| for a continuity link
AMBIGUOUS_MARGIN = 0.05    # runner-up must be this much worse (in |log ratio|) to accept the best
HANDOFF_MAX_RATIO = 3.0    # a handoff may jump in price (spinoffs) but not by more than this
MAX_CIK_PEERS = 4          # skip CIKs with more securities than this trading around the boundary
                           # (fund trusts: iShares has hundreds of $25 ETFs under one CIK)

EPOCH = np.datetime64("1970-01-01", "D")
to_day = lambda d: int((np.datetime64(d, "D") - EPOCH).astype(np.int64))
to_date = lambda n: (EPOCH + np.timedelta64(int(n), "D")).astype(object)


class UnionFind:
    def __init__(self, n):
        self.p = list(range(n))

    def find(self, a):
        while self.p[a] != a:
            self.p[a] = self.p[self.p[a]]
            a = self.p[a]
        return a

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)


def load_claims(base: Path):
    """dict ticker -> [(figi, start_day or None, end_day or None, priority, source)]"""
    events = pd.read_parquet(base / "ticker_changes.parquet")
    active = pd.read_parquet(base / "tickers.parquet", columns=["ticker", "composite_figi", "cik", "name", "type"])
    current = active.dropna(subset=["composite_figi"]).drop_duplicates("composite_figi").set_index("composite_figi")
    owner_of = dict(zip(current["ticker"], current.index))      # ticker -> FIGI holding it today
    claims = defaultdict(list)
    meta = {}                                                   # figi -> (cik, name)
    for figi, g in events.groupby("composite_figi", sort=False):
        g = g.sort_values("date")
        meta[figi] = (g["cik"].iloc[-1], g["name"].iloc[-1])
        days = [to_day(d) for d in g["date"]]
        tks = list(g["ticker"])
        cur = current["ticker"].get(figi)
        for i, (tk, d) in enumerate(zip(tks, days)):
            # the last event stays open-ended even when it isn't today's ticker (GOOGL's FIGI lists
            # only "GOOG 2015-10-06"); it has low priority unless this FIGI holds the ticker today
            end = days[i + 1] - 1 if i + 1 < len(days) else None
            claims[tk].append((figi, d, end, 1 if owner_of.get(tk) == figi else 0, "figi_event"))
        if cur is not None and cur not in tks:
            claims[cur].append((figi, None, None, 1, "figi_current"))
    for figi, row in current.iterrows():
        if figi not in meta:
            meta[figi] = (row["cik"], row["name"])
            claims[row["ticker"]].append((figi, None, None, 1, "figi_current"))
    return claims, meta, active


def ticker_segments(ticker: str, path: Path, claims: list):
    """Split one ticker's bars into segments by owner."""
    t = pq.read_table(path, columns=["window_start", "close"]).to_pandas()
    if "window_start" not in t.columns:
        t = t.reset_index()
    ws = pd.to_datetime(t["window_start"])
    ws = ws.dt.tz_convert(NY_TZ).dt.tz_localize(None) if ws.dt.tz is not None else ws
    day = ((ws.values.astype("datetime64[D]") - EPOCH).astype(np.int64))
    close = t["close"].to_numpy(np.float64)
    o = np.argsort(day, kind="stable")
    day, close = day[o], close[o]
    ok = np.isfinite(close) & (close > 0)
    day, close = day[ok], close[ok]
    if not len(day):
        return []
    last = np.r_[day[1:] != day[:-1], True]           # one bar per day: keep the last
    day, close = day[last], close[last]

    owner = np.full(len(day), -1)
    src = [None]
    runs_start = np.r_[0, np.flatnonzero(np.diff(day) >= EXTEND_GAP_DAYS) + 1]
    # low priority first so the ticker's current owner, then later claims, overwrite
    for figi, s, e, prio, source in sorted(claims, key=lambda c: (c[3], c[1] if c[1] is not None else -10**9)):
        if s is None:      # no event for its current ticker: the final contiguous run
            s = day[runs_start[-1]]
        m = (day >= s) & ((day <= e) if e is not None else True)
        if m.any():
            src.append((figi, source))
            owner[m] = len(src) - 1
    # extend claims back over unclaimed bars until a long gap
    for i in range(len(day) - 2, -1, -1):
        if owner[i] == -1 and owner[i + 1] != -1 and day[i + 1] - day[i] < EXTEND_GAP_DAYS:
            owner[i] = owner[i + 1]
    # new segment where the owner changes, and within unclaimed bars at a long gap (a reused symbol)
    cut = np.flatnonzero((owner[1:] != owner[:-1])
                         | ((owner[1:] == -1) & (owner[:-1] == -1) & (np.diff(day) >= EXTEND_GAP_DAYS))) + 1
    segs = []
    for a, b in zip(np.r_[0, cut], np.r_[cut, len(day)]):
        k = owner[a]
        figi, source = src[k] if k >= 0 else (None, "default")
        segs.append(dict(ticker=ticker, start=int(day[a]), end=int(day[b - 1]), n_bars=int(b - a),
                         first_close=float(close[a]), last_close=float(close[b - 1]), composite_figi=figi, source=source))
    return segs


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="us_stocks_sip")
    ap.add_argument("--output", default=None, help="default: <base>/security_history.parquet")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()
    base = Path(args.base)
    out_file = Path(args.output) if args.output else base / "security_history.parquet"
    t0 = time.time()

    claims, figi_meta, active = load_claims(base)
    inactive = pd.read_parquet(base / "tickers_inactive.parquet", columns=["ticker", "cik", "name", "type", "delisted_utc"])
    inactive = inactive.dropna(subset=["delisted_utc"])
    inactive["delisted"] = inactive["delisted_utc"].dt.tz_convert(NY_TZ).dt.tz_localize(None).values.astype("datetime64[D]")
    inactive_by = {tk: g for tk, g in inactive.groupby("ticker")}
    active_by = active.drop_duplicates("ticker").set_index("ticker")
    type_of_figi = active.dropna(subset=["composite_figi"]).drop_duplicates("composite_figi").set_index("composite_figi")["type"]
    sp = pd.read_parquet(base / "splits.parquet").reset_index()
    split_ratios = defaultdict(list)                         # ticker -> [(day, from/to)]
    for tk, d, f, to in zip(sp["ticker"], sp["execution_date"], sp["split_from"], sp["split_to"]):
        if f > 0 and to > 0:
            split_ratios[tk].append((to_day(d), f / to))

    day_dir = base / "day_aggs_by_ticker"
    files = sorted((p for p in os.scandir(day_dir) if p.name.endswith(".parquet") and not p.name.startswith((".", "_"))),
                   key=lambda p: p.name)
    print(f"scanning {len(files):,} ticker files ({time.time() - t0:.1f}s)")
    segs = []
    with ThreadPoolExecutor(args.workers) as ex:
        futs = [ex.submit(ticker_segments, f.name[:-8], Path(f.path), claims.get(f.name[:-8], [])) for f in files]
        for i, fu in enumerate(futs, 1):
            try:
                segs += fu.result()
            except Exception as e:
                print(f"  {files[i - 1].name}: {e}")
            if i % 5000 == 0:
                print(f"  {i:,}/{len(files):,} files, {len(segs):,} segments ({time.time() - t0:.0f}s)")
    latest_day = max(s["end"] for s in segs)

    # CIK and name per segment
    for s in segs:
        cik = name = typ = None
        if s["composite_figi"]:
            cik, name = figi_meta.get(s["composite_figi"], (None, None))
            typ = type_of_figi.get(s["composite_figi"])
        else:
            if s["ticker"] in active_by.index and s["end"] >= latest_day - LINK_DAYS:
                r = active_by.loc[s["ticker"]]
                cik, name, typ = r["cik"], r["name"], r["type"]
            elif s["ticker"] in inactive_by:
                g = inactive_by[s["ticker"]]
                gap = np.abs((g["delisted"].values.astype("datetime64[D]") - EPOCH).astype(np.int64) - s["end"])
                j = int(np.argmin(gap))
                if gap[j] <= 30:
                    cik, name, typ = g["cik"].iloc[j], g["name"].iloc[j], g["type"].iloc[j]
                    s["delisted"] = int((g["delisted"].values.astype("datetime64[D]")[j] - EPOCH).astype(np.int64))
        s["cik"], s["name"] = (cik if isinstance(cik, str) and cik else None), name
        s["type"] = typ if isinstance(typ, str) and typ else None

    # ---- link unclaimed runs to their same-CIK neighbours
    by_cik = defaultdict(list)
    for i, s in enumerate(segs):
        if s["cik"]:
            by_cik[s["cik"]].append(i)
    by_ticker = defaultdict(list)
    for i, s in enumerate(segs):
        by_ticker[s["ticker"]].append(i)

    def ratio_ok(tk_a, tk_b, d, r):
        lr = abs(np.log(r))
        if lr < PRICE_TOL:
            return lr
        for tk in (tk_a, tk_b):
            for sd, f in split_ratios.get(tk, []):
                if abs(sd - d) <= LINK_DAYS and abs(np.log(r / f)) < PRICE_TOL:
                    return abs(np.log(r / f))
        return None

    def best(cands):
        """cands: [(score, idx, link)] -> idx of a clear winner or None"""
        if not cands:
            return None, None
        cands.sort()
        if len(cands) > 1 and cands[1][0] - cands[0][0] < AMBIGUOUS_MARGIN:
            return None, "ambiguous"
        return cands[0][1], cands[0][2]

    # FIGI segments of one security are linked by construction; do these first so the
    # inferred links below can refuse to merge two FIGIs
    uf = UnionFind(len(segs))
    first_of_figi = {}
    for i, s_ in enumerate(segs):
        if s_["composite_figi"]:
            if s_["composite_figi"] in first_of_figi:
                uf.union(i, first_of_figi[s_["composite_figi"]])
            else:
                first_of_figi[s_["composite_figi"]] = i
    figi_of_root = {uf.find(i): f for f, i in first_of_figi.items()}

    def join(a, b) -> bool:
        ra, rb = uf.find(a), uf.find(b)
        fa, fb = figi_of_root.get(ra), figi_of_root.get(rb)
        if ra == rb or (fa and fb and fa != fb):
            return False
        uf.union(a, b)
        if fa or fb:
            figi_of_root[uf.find(b)] = fa or fb
        return True

    def compatible(a, b) -> bool:
        return a["type"] is None or b["type"] is None or a["type"] == b["type"]

    def crowded(cik, day) -> bool:
        return sum(1 for k in by_cik[cik] if segs[k]["start"] <= day + 30 and segs[k]["end"] >= day - 30) > MAX_CIK_PEERS + 1

    link = [""] * len(segs)
    stats = defaultdict(int)
    for i, x in enumerate(segs):
        if x["source"] != "default" or not x["cik"]:
            continue
        if crowded(x["cik"], x["end"]) and crowded(x["cik"], x["start"]):
            stats["crowded cik"] += 1
            continue
        after = []
        if not crowded(x["cik"], x["end"]):
            for j in by_cik[x["cik"]]:
                y = segs[j]
                if j == i or not (x["end"] < y["start"] <= x["end"] + LINK_DAYS) or not compatible(x, y):
                    continue
                r = y["first_close"] / x["last_close"]
                lr = ratio_ok(x["ticker"], y["ticker"], y["start"], r)
                if x.get("delisted") is not None and abs(x["delisted"] - y["start"]) <= HANDOFF_DAYS:
                    if lr is not None or 1 / HANDOFF_MAX_RATIO <= r <= HANDOFF_MAX_RATIO:
                        after.append((0.0 if lr is not None else 0.01, j, "handoff"))
                elif lr is not None:
                    after.append((lr, j, "continuity"))
        j, how = best(after)
        if how == "ambiguous":
            stats["ambiguous"] += 1
        if j is not None and join(i, j):
            link[j] = link[j] or how
            stats[how] += 1
        succ_ticker = segs[j]["ticker"] if j is not None else None
        before = []
        if not crowded(x["cik"], x["start"]):
            for k in set(by_cik[x["cik"]]) | set(by_ticker.get(succ_ticker, []) if succ_ticker else []):
                w = segs[k]
                if k == i or not (x["start"] - LINK_DAYS <= w["end"] < x["start"]) or not compatible(w, x):
                    continue
                if w["cik"] != x["cik"] and not (w["cik"] is None and w["ticker"] == succ_ticker):
                    continue
                if (lr := ratio_ok(w["ticker"], x["ticker"], x["start"], x["first_close"] / w["last_close"])) is not None:
                    before.append((lr, k, "continuity"))
        k, how = best(before)
        if how == "ambiguous":
            stats["ambiguous"] += 1
        if k is not None and join(k, i):
            link[i] = link[i] or how
            stats[f"{how} (predecessor)"] += 1

    groups = defaultdict(list)
    for i in range(len(segs)):
        groups[uf.find(i)].append(i)
    rows, conflicts = [], 0
    for members in groups.values():
        members.sort(key=lambda i: (segs[i]["start"], segs[i]["end"]))
        figis = {segs[i]["composite_figi"] for i in members if segs[i]["composite_figi"]}
        conflicts += len(figis) > 1
        latest = max(members, key=lambda i: segs[i]["end"])
        sid = segs[latest]["composite_figi"] or next(iter(sorted(figis)), None) \
            or f"{segs[members[0]]['ticker']}@{to_date(segs[members[0]]['start'])}"
        covered_to = None
        for i in members:
            s = segs[i]
            start = s["start"]
            if covered_to is not None and start <= covered_to:      # overlaps an earlier segment
                if s["end"] <= covered_to:
                    stats["dropped overlapping"] += 1
                    continue
                start = covered_to + 1
                stats["trimmed overlapping"] += 1
            covered_to = s["end"] if covered_to is None else max(covered_to, s["end"])
            how = "" if not rows or rows[-1]["security_id"] != sid else (link[i] or ("figi" if s["composite_figi"] else "continuity"))
            rows.append(dict(security_id=sid, current_ticker=segs[latest]["ticker"], ticker=s["ticker"],
                             start=to_date(start), end=to_date(s["end"]), n_bars=s["n_bars"],
                             first_close=s["first_close"], last_close=s["last_close"], source=s["source"],
                             link=how, composite_figi=s["composite_figi"], cik=s["cik"], name=s["name"]))
    out = pd.DataFrame(rows).sort_values(["security_id", "start"], kind="stable").reset_index(drop=True)
    print(f"{len(segs):,} ticker segments -> {out.security_id.nunique():,} securities; links: "
          + ", ".join(f"{k} {v:,}" for k, v in sorted(stats.items()))
          + f"; {conflicts} groups joined more than one FIGI")

    table = pa.Table.from_pandas(out, preserve_index=False)
    meta = {"built": pd.Timestamp.now(tz=NY_TZ).isoformat(timespec="seconds"), "asof": str(to_date(latest_day)),
            "usage": "rows of one security_id, sorted by start: bars of `ticker` clipped to [start, end]"}
    table = table.replace_schema_metadata({**(table.schema.metadata or {}), b"security_history": json.dumps(meta).encode()})
    tmp = out_file.with_name(out_file.name + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, out_file)
    print(f"wrote {out_file}: {len(out):,} rows ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
