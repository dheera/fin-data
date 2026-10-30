#!/usr/bin/env python3
"""Atomically import a provider key without emitting the secret."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path


def extract(source: Path, field: str | None) -> str:
    raw = source.read_text().strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        names = [field] if field else [
            "api_key", "key", "token", "sportradar_api_key",
            "collegefootballdata_api_key",
        ]
        for name in names:
            if name and isinstance(parsed.get(name), str) and parsed[name].strip():
                return parsed[name].strip()
        raise ValueError("JSON source does not contain a recognized key field")
    if not raw or "\n" in raw:
        raise ValueError("plain-text source must contain exactly one non-empty line")
    return raw


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--access", action="append", default=[])
    parser.add_argument("--field")
    parser.add_argument("--history-line", type=int)
    parser.add_argument("--token-hash-prefix")
    args = parser.parse_args()

    source = args.source.expanduser()
    if args.history_line is not None:
        if not args.token_hash_prefix:
            raise ValueError("--history-line requires --token-hash-prefix")
        lines = source.read_text(errors="strict").splitlines()
        line = lines[args.history_line - 1]
        matches = [
            token for token in re.findall(
                r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{24,96}(?![A-Za-z0-9_-])", line
            )
            if hashlib.sha256(token.encode()).hexdigest().startswith(args.token_hash_prefix)
        ]
        if len(matches) != 1:
            raise ValueError(f"expected one matching history token, found {len(matches)}")
        key = matches[0]
    else:
        key = extract(source, args.field)
    destination = args.destination.expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination.parent, 0o700)
    payload = {"api_key": key, "service": args.service, "access": args.access}
    handle, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    try:
        os.fchmod(handle, 0o600)
        with os.fdopen(handle, "w") as stream:
            json.dump(payload, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    print(f"imported {args.service} credential to {destination} (value redacted)")


if __name__ == "__main__":
    main()
