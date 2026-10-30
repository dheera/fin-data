"""Credential-safe Alpha Vantage requests with a conservative local daily budget.

Callers hold .alphavantage.lock while using this client. No retries: failed
requests may consume quota too. The ledger cannot account for other API clients.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time

import requests

from expansion_common import ROOT, atomic_bytes

ENDPOINT = "https://www.alphavantage.co/query"
DEFAULT_CREDENTIAL = Path(__file__).resolve().parents[1] / "api_keys/alphavantage.json"


class AccessError(RuntimeError):
    pass


def load_key(path=DEFAULT_CREDENTIAL):
    key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    if not key:
        try:
            key = json.loads(Path(path).read_text())["api_key"].strip()
        except (OSError, ValueError, KeyError, AttributeError):
            raise AccessError("Set ALPHAVANTAGE_API_KEY or the private credential file") from None
    if not re.fullmatch(r"[A-Za-z0-9]+", key):
        raise AccessError("API key has an unexpected format")
    return key


def response_status(payload):
    """Return a safe classification, without echoing vendor text or URLs."""
    if not isinstance(payload, dict):
        return "invalid_json_object"
    messages = " ".join(str(payload.get(k) or "") for k in ("Information", "Note", "Error Message")).strip().lower()
    if messages:
        if any(s in messages for s in ("rate limit", "call frequency", "requests per day", "requests per minute")):
            return "rate_limited"
        if "premium" in messages:
            return "premium_required"
        return "provider_error"
    return "ok"


class Client:
    def __init__(self, root=ROOT, key=None, session=None, now=None, sleep=time.sleep):
        self.root = Path(root)
        self.key = load_key() if key is None else key
        self.session = session or requests.Session()
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep
        self.ledger_path = self.root / "archive/work/alphavantage_usage.json"
        self.ledger = json.loads(self.ledger_path.read_text()) if self.ledger_path.exists() else {"days": {}}
        self.last_request_at = 0.0
        self._import_initial_probe()

    def _import_initial_probe(self):
        """Account for and reuse the one key-verification request made at setup."""
        p = self.root / "logs/alphavantage_initial_probe.json"
        if not p.exists() or self.ledger.get("initial_probe_imported"):
            return
        receipt = json.loads(p.read_text())
        day = receipt["retrieved_at"][:10]
        self.ledger["days"][day] = self.ledger["days"].get(day, 0) + 1
        self.ledger["initial_probe_imported"] = True
        self._save_ledger()
        raw = self.root / "archive/raw" / (receipt["sha256"] + ".bin")
        if raw.exists() and hashlib.sha256(raw.read_bytes()).hexdigest() == receipt["sha256"]:
            obj = json.loads(raw.read_bytes())
            receipt["result"] = response_status(obj)
            self._store_receipt(receipt, day)

    def _save_ledger(self):
        atomic_bytes(self.ledger_path, json.dumps(self.ledger, indent=2).encode())

    def _cache_path(self, params, day):
        digest = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        return self.root / "archive/requests" / f"alphavantage_{day}_{digest}.json"

    def _store_receipt(self, receipt, day):
        cache = self._cache_path(receipt["parameters"], day)
        stamp = re.sub(r"[^0-9]", "", receipt["retrieved_at"])
        immutable = self.root / "archive/receipts" / f"{cache.stem}_{stamp}.json"
        encoded = json.dumps(receipt, indent=2).encode()
        atomic_bytes(immutable, encoded)
        atomic_bytes(cache, encoded)

    def get(self, function, symbol):
        if function != "EARNINGS_ESTIMATES" or not re.fullmatch(r"[A-Z0-9.\-]{1,24}", symbol):
            raise ValueError("This collector only requests earnings estimates for explicit symbols")
        params = {"function": function, "symbol": symbol}
        today = self.now().date().isoformat()
        cache = self._cache_path(params, today)
        if cache.exists():
            receipt = json.loads(cache.read_text())
            path = self.root / "archive/raw" / (receipt["sha256"] + ".bin")
            if path.exists():
                content = path.read_bytes()
                if hashlib.sha256(content).hexdigest() == receipt["sha256"]:
                    if receipt["result"] != "ok":
                        raise AccessError(receipt["result"])
                    return json.loads(content), receipt
        if self.ledger["days"].get(today, 0) >= 25:
            raise AccessError("local_daily_quota_exhausted")
        # Space network requests, including when multiple symbols are collected.
        elapsed = time.monotonic() - self.last_request_at
        delay = max(0.0, 13 - elapsed) if self.last_request_at else 0.0
        previous = self.ledger.get("last_request_at")
        if previous:
            wall_elapsed = (self.now() - datetime.fromisoformat(previous)).total_seconds()
            delay = max(delay, min(13.0, max(0.0, 13 - wall_elapsed)))
        if delay:
            self.sleep(delay)
        # Reserve before sending, so crashes and network errors never undercount.
        today = self.now().date().isoformat()
        if self.ledger["days"].get(today, 0) >= 25:
            raise AccessError("local_daily_quota_exhausted")
        self.ledger["days"][today] = self.ledger["days"].get(today, 0) + 1
        self.ledger["last_request_at"] = self.now().isoformat()
        self._save_ledger()
        self.last_request_at = time.monotonic()
        try:
            response = self.session.get(
                ENDPOINT, params={**params, "apikey": self.key}, timeout=(15, 60),
                allow_redirects=False, headers={"User-Agent": "FinAltDataResearch/1.0"},
            )
        except requests.RequestException:
            raise AccessError("network_error; authenticated URL omitted") from None
        content = response.content
        if self.key.encode() in content:
            raise AccessError("response_echoed_credential; response not archived")
        retrieved = self.now().isoformat()
        digest = hashlib.sha256(content).hexdigest()
        atomic_bytes(self.root / "archive/raw" / (digest + ".bin"), content)
        try:
            payload = json.loads(content)
            result = response_status(payload)
        except (ValueError, UnicodeError):
            payload, result = None, "invalid_json"
        if response.status_code != 200:
            result = "rate_limited" if response.status_code == 429 else f"http_{response.status_code}"
        receipt = dict(url=ENDPOINT, parameters=params, authentication="local API key omitted",
                       sha256=digest, retrieved_at=retrieved, status_code=response.status_code,
                       content_type=response.headers.get("Content-Type"), result=result)
        self._store_receipt(receipt, today)
        if result != "ok":
            raise AccessError(result)
        return payload, receipt
