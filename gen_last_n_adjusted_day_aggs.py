#!/usr/bin/env python3
"""
Read a combined day_aggs Parquet (e.g. last730.parquet) indexed by (date, ticker),
adjust for splits and dividends using single splits.parquet and dividends.parquet.
Fully vectorized: pandas merges + groupby transform, no ProcessPool/ThreadPool.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

NY_TZ = "America/New_York"


def to_ny_normalized(series):
    """Convert a datetime series to NY time, normalize to midnight, drop tz."""
    series = pd.to_datetime(series)
    if series.dt.tz is None:
        series = series.dt.tz_localize(NY_TZ)
    else:
        series = series.dt.tz_convert(NY_TZ)
    return series.dt.normalize().dt.tz_localize(None)


def main():
    parser = argparse.ArgumentParser(
        description="Adjust a combined day_aggs Parquet (index date, ticker) for splits and dividends; write adjusted file."
    )
    parser.add_argument(
        "input",
        help="Path to combined day_aggs Parquet with index (date, ticker) (e.g. last730.parquet, last365.parquet).",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output path (default: same dir as input, filename adjusted_<input name>).",
    )
    parser.add_argument(
        "--splits",
        default="us_stocks_sip/splits.parquet",
        help="Path to splits.parquet (columns: ticker, execution_date, split_from, split_to).",
    )
    parser.add_argument(
        "--dividends",
        default="us_stocks_sip/dividends.parquet",
        help="Path to dividends.parquet (columns: ticker, ex_dividend_date, cash_amount).",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input not found: {input_path}")
        return 1
    output_path = Path(args.output) if args.output else input_path.parent / f"adjusted_{input_path.name}"
    splits_path = Path(args.splits)
    dividends_path = Path(args.dividends)

    print("Reading day_aggs...")
    df = pd.read_parquet(input_path).reset_index()
    if "date" not in df.columns or "ticker" not in df.columns:
        print("Input must have index (date, ticker).")
        return 1

    df["date_norm"] = to_ny_normalized(pd.to_datetime(df["date"]))

    # Single-file splits: ticker, execution_date, split_from, split_to -> ticker, date_norm, split_factor
    print("Reading splits...")
    splits = pd.read_parquet(splits_path).reset_index()
    splits = splits.assign(
        date_norm=to_ny_normalized(pd.to_datetime(splits["execution_date"])),
        split_factor=(splits["split_to"] / splits["split_from"]).astype(np.float64),
    )[["ticker", "date_norm", "split_factor"]]

    # Single-file dividends: ticker, ex_dividend_date, cash_amount -> ticker, date_norm, dividend
    print("Reading dividends...")
    dividends = pd.read_parquet(dividends_path).reset_index()
    date_col = "ex_dividend_date" if "ex_dividend_date" in dividends.columns else "date"
    amount_col = "cash_amount" if "cash_amount" in dividends.columns else "dividend"
    dividends = dividends.assign(
        date_norm=to_ny_normalized(pd.to_datetime(dividends[date_col])),
        dividend=dividends[amount_col].astype(np.float64),
    )[["ticker", "date_norm", "dividend"]]

    df = df.merge(splits, on=["ticker", "date_norm"], how="left")
    df["split_factor"] = df["split_factor"].fillna(1.0).astype(np.float64)

    df = df.merge(dividends, on=["ticker", "date_norm"], how="left")
    df["dividend"] = df["dividend"].fillna(0.0).astype(np.float64)

    # Vectorized event factor and reverse cumprod per ticker
    close = df["close"].astype(np.float64)
    div_factor = np.where(close == 0, 1.0, (close - df["dividend"]) / close)
    df["event_factor"] = (1.0 / df["split_factor"]) * div_factor

    df = df.sort_values(["ticker", "date_norm"])
    df["cum_factor"] = (
        df.groupby("ticker", sort=False)["event_factor"]
        .transform(lambda g: g.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0).astype(np.float64))
    )
    # Volume adjusts by split factor only (same reverse cumprod of 1/split_factor)
    df["volume_event_factor"] = 1.0 / df["split_factor"]
    df["cum_volume_factor"] = (
        df.groupby("ticker", sort=False)["volume_event_factor"]
        .transform(lambda g: g.iloc[::-1].cumprod().iloc[::-1].shift(-1).fillna(1.0).astype(np.float64))
    )

    price_cols = ["open", "high", "low", "close"]
    df = df.assign(**{f"adj_{c}": df[c].astype(np.float64) * df["cum_factor"] for c in price_cols})
    if "volume" in df.columns:
        df["adj_volume"] = (df["volume"].astype(np.float64) * df["cum_volume_factor"]).astype(np.int64)

    out = df.drop(
        columns=["date_norm", "split_factor", "dividend", "event_factor", "cum_factor", "volume_event_factor", "cum_volume_factor"],
        errors="ignore",
    )
    out = out.set_index(["date", "ticker"]).sort_index()
    out.to_parquet(output_path, compression="snappy")
    print(f"Wrote {output_path} ({len(out):,} rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
