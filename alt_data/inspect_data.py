#!/usr/bin/env python3
"""Compatibility entry point: search the catalog and validate actual Parquets."""
import argparse
import json
from pathlib import Path
from expansion_common import ROOT
from library import build_catalog, catalog, validate


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT, help='Library root')
    p.add_argument('--search', default='')
    p.add_argument('--json', action='store_true')
    p.add_argument('--snapshots', '--short-history', action='store_true', dest='short_history')
    p.add_argument('--save', action='store_true', help='Rebuild catalog and validate data')
    args = p.parse_args()
    if args.save:
        build_catalog(args.output)
        result = validate(args.output)
        if result['failures']:
            print(json.dumps(result, indent=2))
            return 1
    frame = catalog(args.output)
    if args.short_history:
        frame = frame[frame['status'] == 'short_history']
    if args.search:
        text = frame[['series_id', 'title', 'category', 'provider']].fillna('').agg(' '.join, axis=1)
        frame = frame[text.str.contains(args.search, case=False, regex=False)]
    if args.json:
        print(frame.to_json(orient='records', indent=2))
    else:
        print(frame[['series_id', 'frequency', 'rows', 'start', 'end']].to_string(index=False))
        print(f'{len(frame)} datasets')
    return 0

if __name__=='__main__':raise SystemExit(main())
