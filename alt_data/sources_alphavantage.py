#!/usr/bin/env python3
"""Collect one daily earnings-estimate snapshot for a small global watchlist.

Values remain supplementary until genuine observation history accumulates.
No fiscal-target date or sparse lookback is converted into a collection date.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from alphavantage_client import AccessError, Client
from alphavantage_estimates import parse_estimates
from expansion_common import ROOT, atomic_bytes

# Company exposure is international; these symbols are US-listed ADRs/equities.
# 20 symbols leave five daily calls for investigation/other use of the free key.
WATCHLIST = ["TSM", "ASML", "BABA", "PDD", "INFY", "IBN", "HDB", "SAP", "NVO",
             "TM", "SONY", "VALE", "PBR", "SHEL", "RIO", "BHP", "MELI", "SE", "NVDA", "WMT"]


def collect(root=ROOT, symbols=WATCHLIST, client=None):
    root = Path(root)
    client = client or Client(root)
    destination = root / "supplementary/alphavantage/earnings_estimates.parquet"
    previous = pd.read_parquet(destination) if destination.exists() else None
    frames = []
    report = dict(started_at=pd.Timestamp.now(tz="UTC").isoformat(), provider="Alpha Vantage",
                  requested_symbols=list(symbols), succeeded=[], failures=[],
                  public_catalog_additions=0, output=str(destination.relative_to(root)),
                  date_semantics="collected_at is local retrieval time; fiscal_period_end is a target/label, not historical availability",
                  limitations=["One collection day initially; no reconstructed daily consensus backhistory.",
                               "Source does not identify currency or share/ADR basis; do not assume USD or comparability across companies or fiscal periods.",
                               "Sparse lookback and trailing-revision metrics remain source fields; their exact publication clocks are unverified."],
                  units="Source-native; currency/share basis unspecified by endpoint", schedule_installed=False)
    for symbol in dict.fromkeys(symbols):
        try:
            obj, receipt = client.get("EARNINGS_ESTIMATES", symbol)
            frame = parse_estimates(obj, symbol, receipt["retrieved_at"], receipt["sha256"])
            if frame.empty:
                raise ValueError("No numeric estimates returned")
            frames.append(frame)
            report["succeeded"].append(dict(symbol=symbol, rows=len(frame), raw_sha256=receipt["sha256"],
                                             collected_at=receipt["retrieved_at"]))
            print(f"{symbol}: {len(frame)} estimate fields archived", flush=True)
        except (AccessError, ValueError) as exc:
            # Parser errors are local; API exceptions are sanitized by Client.
            report["failures"].append(dict(symbol=symbol, error=str(exc)))
            print(f"{symbol}: collection unavailable", flush=True)
            if isinstance(exc, AccessError) and any(s in str(exc) for s in ("quota", "rate_limited", "premium", "provider_error")):
                break
    if frames:
        all_frames = ([previous] if previous is not None else []) + frames
        combined = pd.concat(all_frames, ignore_index=True)
        key = ["collected_at", "symbol", "fiscal_period_end", "horizon", "metric"]
        # A cached response is idempotent; a conflicting record must never win silently.
        counts = combined.groupby(key, dropna=False)[["value", "raw_sha256"]].nunique(dropna=False)
        if counts.gt(1).any().any():
            raise ValueError("Conflicting values or provenance for a previously collected estimate record")
        combined = combined.drop_duplicates(key).sort_values(key).reset_index(drop=True)
        table = pa.Table.from_pandas(combined, preserve_index=False)
        table = table.replace_schema_metadata({b"alt_data_supplementary": json.dumps({
            "provider": "Alpha Vantage", "function": "EARNINGS_ESTIMATES",
            "timestamp_semantics": report["date_semantics"], "limitations": report["limitations"],
            "units": report["units"], "public_catalog_member": False,
        }).encode()})
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        content = sink.getvalue().to_pybytes()
        atomic_bytes(destination, content)
        report.update(total_rows=len(combined), total_symbols=int(combined.symbol.nunique()),
                      observation_days=int(combined.collected_at.dt.floor("D").nunique()),
                      parquet_sha256=hashlib.sha256(content).hexdigest())
    report["local_usage_by_utc_day"] = client.ledger["days"]
    report["finished_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    atomic_bytes(root / "logs/alphavantage_latest.json", json.dumps(report, indent=2).encode())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=WATCHLIST)
    args = parser.parse_args()
    with open(ROOT / ".alphavantage.lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(2, "Another Alpha Vantage collector is running\n")
        try:
            report = collect(symbols=args.symbols)
        except AccessError as exc:
            parser.exit(2, f"{exc}\n")
    print(json.dumps({k:report[k] for k in ("public_catalog_additions", "output", "failures")}))
    return int(bool(report["failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
