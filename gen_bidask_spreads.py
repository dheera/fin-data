#!/usr/bin/env python3
"""Compute per-ticker bid/ask spread summaries from date-level TQ parquet."""
import argparse
from pathlib import Path

import pandas as pd


def reject_outliers(series):
    mean, std = series.mean(), series.std()
    return series[(series >= mean - 3 * std) & (series <= mean + 3 * std)]


def summarize(ticker, frame):
    frame = frame.copy()
    frame["window_start"] = pd.to_datetime(frame["window_start"])
    frame = frame.set_index("window_start")
    frame["spread"] = frame["ask"] - frame["bid"]
    frame["spread_frac"] = frame["spread"] / frame["last"]
    result = {"ticker": ticker}
    for name, start, end in (("pre", "04:00", "09:29:59"),
                             ("day", "09:30", "15:59:59"),
                             ("post", "16:00", "20:00")):
        period = frame.between_time(start, end)
        for column, suffix in (("spread", ""), ("spread_frac", "_frac")):
            values = reject_outliers(period[column]) if not period.empty else pd.Series(dtype=float)
            result[f"{name}_mean{suffix}"] = values.mean()
            result[f"{name}_std{suffix}"] = values.std()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--input-dir", default="us_stocks_sip/tq_aggs")
    parser.add_argument("--output-dir", default="us_stocks_sip/bidask_spreads")
    args = parser.parse_args()
    source = Path(args.input_dir) / f"{args.date}.parquet"
    if not source.exists():
        raise FileNotFoundError(source)
    data = pd.read_parquet(source)
    if "ticker" not in data or "window_start" not in data:
        raise ValueError(f"{source} is not date-level TQ data")
    results = [summarize(ticker, frame) for ticker, frame in data.groupby("ticker", sort=False)]
    output = Path(args.output_dir) / f"{args.date}.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).set_index("ticker").to_parquet(output)
    print(f"Saved {output}")


if __name__ == "__main__":
    main()
