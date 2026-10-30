"""Load one provider's local credential without exposing unrelated secrets."""
from __future__ import annotations
import os
from pathlib import Path
import re
import json
from .import_key import extract

ROOT=Path(__file__).resolve().parent

def load_credentials(service: str) -> dict:
    """Read a native multi-field account profile (e.g. Alpaca or Kalshi)."""
    if not re.fullmatch(r'[a-z0-9_]+',service):raise ValueError('Invalid provider identifier')
    value=json.loads((ROOT/(service+'.json')).read_text())
    if not isinstance(value,dict):raise ValueError('Credential profile must be a JSON object')
    return value

def load_key(service: str, *, key_file: Path | None = None,
             env_var: str | None = None, required: bool = True) -> str:
    """Explicit file, explicit environment override, then api_keys/<service>.json.

    Explicit files may use the shared JSON schema or contain one plaintext key.
    Error messages never include credential values.
    """
    if not re.fullmatch(r'[a-z0-9_]+',service):
        raise ValueError('Invalid provider identifier')
    if key_file is not None:
        return extract(Path(key_file).expanduser(),None)
    if env_var and os.environ.get(env_var):
        return os.environ[env_var].strip()
    path=ROOT/(service+'.json')
    if path.is_file():return extract(path,None)
    if required:raise FileNotFoundError(f'Credential missing: api_keys/{service}.json')
    return ''
