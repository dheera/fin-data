"""Parse current Alpha Vantage EARNINGS_ESTIMATES responses without network I/O.

Each row is known locally at ``collected_at``. The source ``date`` is a fiscal
period target, even if it is in the past; it is never an observation timestamp.
The 7/30/60/90-day lookbacks remain distinct provider-named metrics, not invented
historical snapshots. The response does not identify currency or share basis,
and neither is inferred or converted here. In particular, values for different
fiscal targets should not be assumed to have consistent currency/share basis.
"""

from __future__ import annotations

import math
import re
from numbers import Real

import pandas as pd


METRIC_FIELDS = (
    "eps_estimate_average",
    "eps_estimate_high",
    "eps_estimate_low",
    "eps_estimate_analyst_count",
    "eps_estimate_average_7_days_ago",
    "eps_estimate_average_30_days_ago",
    "eps_estimate_average_60_days_ago",
    "eps_estimate_average_90_days_ago",
    "eps_estimate_revision_up_trailing_7_days",
    "eps_estimate_revision_down_trailing_7_days",
    "eps_estimate_revision_up_trailing_30_days",
    "eps_estimate_revision_down_trailing_30_days",
    "revenue_estimate_average",
    "revenue_estimate_high",
    "revenue_estimate_low",
    "revenue_estimate_analyst_count",
)
COLUMNS = (
    "collected_at", "symbol", "fiscal_period_end", "horizon", "metric",
    "value", "raw_sha256",
)
HORIZONS = frozenset(("fiscal quarter", "fiscal year"))
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def _numeric_value(value: object, metric: str, row_number: int) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.lower() in ("", "none", "-"):
            return None
        if not _NUMBER.fullmatch(value):
            raise ValueError(f"Estimate row {row_number}: {metric} is not numeric")
    elif isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"Estimate row {row_number}: {metric} is not numeric")
    try:
        numeric = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"Estimate row {row_number}: {metric} is not finite numeric") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"Estimate row {row_number}: {metric} is not finite numeric")
    return numeric


def parse_estimates(
    payload: dict, symbol: str, retrieved_at: str, raw_sha256: str
) -> pd.DataFrame:
    """Return long-form observations from one retrieved estimates response.

    ``retrieved_at`` must include a timezone and is normalized to UTC nanoseconds.
    ``raw_sha256`` is the 64-character hex digest of the original response body.
    A response symbol must match the requested symbol, ignoring case/whitespace.
    Only the explicit ``METRIC_FIELDS`` allowlist is emitted; unknown extension
    fields are ignored. Missing and provider-null numeric fields are omitted,
    while an explicit zero is preserved. Malformed known fields raise ValueError.

    Duplicate fiscal-period/horizon records are rejected, including identical
    repeats. Annual and quarterly estimates for the same date remain distinct.
    An empty estimates list (or only null metrics) returns a typed empty frame.
    Provider error messages raise ValueError without echoing response text, which
    might contain request parameters. No currency/share-basis units are invented.
    """
    if not isinstance(payload, dict):
        raise ValueError("Alpha Vantage response must be a JSON object")
    for error_key in ("Error Message", "Information", "Note"):
        if error_key in payload:
            raise ValueError(f"Alpha Vantage response contains {error_key}; estimates unavailable")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Requested symbol must be a nonempty string")
    canonical_symbol = symbol.strip().upper()
    response_symbol = payload.get("symbol")
    if not isinstance(response_symbol, str) or response_symbol.strip().upper() != canonical_symbol:
        raise ValueError("Alpha Vantage response symbol does not match requested symbol")
    if not isinstance(raw_sha256, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", raw_sha256):
        raise ValueError("raw_sha256 must be a 64-character SHA-256 hexadecimal digest")
    if not isinstance(retrieved_at, str):
        raise ValueError("retrieved_at must be a timezone-aware timestamp string")
    try:
        collected_at = pd.Timestamp(retrieved_at)
        if pd.isna(collected_at) or collected_at.tzinfo is None:
            raise ValueError("missing timestamp or timezone")
        collected_at = pd.array([collected_at.tz_convert("UTC")], dtype="datetime64[ns, UTC]")[0]
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("retrieved_at must be a valid timezone-aware nanosecond timestamp") from exc
    estimates = payload.get("estimates")
    if not isinstance(estimates, list):
        raise ValueError("Alpha Vantage response estimates must be a list")

    rows = []
    seen_targets = set()
    for row_number, record in enumerate(estimates):
        if not isinstance(record, dict):
            raise ValueError(f"Estimate row {row_number} must be an object")
        date = record.get("date")
        if not isinstance(date, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
            raise ValueError(f"Estimate row {row_number}: date must be a fiscal YYYY-MM-DD target")
        try:
            target = pd.array([pd.Timestamp(date, tz="UTC")], dtype="datetime64[ns, UTC]")[0]
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"Estimate row {row_number}: invalid fiscal target date") from exc
        horizon = record.get("horizon")
        if not isinstance(horizon, str) or horizon not in HORIZONS:
            raise ValueError(f"Estimate row {row_number}: unsupported or missing horizon")
        identity = (target, horizon)
        if identity in seen_targets:
            raise ValueError(f"Estimate row {row_number}: duplicate fiscal target and horizon")
        seen_targets.add(identity)
        for metric in METRIC_FIELDS:
            numeric = _numeric_value(record.get(metric), metric, row_number)
            if numeric is not None:
                rows.append((
                    collected_at, canonical_symbol, target, horizon, metric,
                    numeric, raw_sha256.lower(),
                ))

    frame = pd.DataFrame.from_records(rows, columns=COLUMNS)
    frame["collected_at"] = pd.array(frame["collected_at"], dtype="datetime64[ns, UTC]")
    frame["fiscal_period_end"] = pd.array(frame["fiscal_period_end"], dtype="datetime64[ns, UTC]")
    frame["value"] = frame["value"].astype("float64")
    return frame.sort_values(["fiscal_period_end", "horizon", "metric"]).reset_index(drop=True)
