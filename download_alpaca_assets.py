#!/usr/bin/env python3

from __future__ import annotations

import json
import uuid
from enum import Enum
from typing import Any

import pandas as pd
from alpaca.trading.client import TradingClient


def to_plain(obj: Any) -> Any:
    """Recursively convert SDK objects into Parquet-friendly Python values."""
    if obj is None:
        return None
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, list):
        return [to_plain(x) for x in obj]
    if isinstance(obj, tuple):
        return [to_plain(x) for x in obj]
    if isinstance(obj, dict):
        return {k: to_plain(v) for k, v in obj.items()}

    # Alpaca SDK models often expose one of these
    if hasattr(obj, "model_dump"):
        return to_plain(obj.model_dump())
    if hasattr(obj, "dict"):
        return to_plain(obj.dict())
    if hasattr(obj, "__dict__"):
        return to_plain(vars(obj))

    # Fallback
    return str(obj)


def cast_dataframe_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to sensible nullable pandas dtypes."""
    bool_cols = [
        "tradable",
        "marginable",
        "shortable",
        "easy_to_borrow",
        "fractionable",
    ]

    float_cols = [
        "maintenance_margin_requirement",
        "min_order_size",
        "min_trade_increment",
        "price_increment",
    ]

    string_cols = [
        "id",
        "asset_class",
        "exchange",
        "symbol",
        "name",
        "status",
    ]

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Keep list-like attributes as JSON strings for maximum Parquet compatibility
    if "attributes" in df.columns:
        df["attributes"] = df["attributes"].apply(
            lambda x: json.dumps(x) if x is not None else None
        ).astype("string")

    return df


def main() -> None:
    with open("alpaca.json", "r") as f:
        config = json.load(f)

    trading_client = TradingClient(config["key"], config["secret_key"])

    assets = trading_client.get_all_assets()

    rows = [to_plain(asset) for asset in assets]
    df = pd.DataFrame(rows)
    df = cast_dataframe_dtypes(df)

    # index by symbol
    df = df.set_index("symbol").sort_index()

    output_path = "alpaca_assets.parquet"
    df.to_parquet(output_path, engine="pyarrow")

    print(f"Wrote {len(df)} rows to {output_path}")
    print(df.dtypes)


if __name__ == "__main__":
    main()
