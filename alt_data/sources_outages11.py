#!/usr/bin/env python3
"""Normalize GB REMIT capacity revisions and profiles; historical knowledge unverified."""
import hashlib
import json
from pathlib import Path

import pandas as pd

from expansion_common import ROOT, atomic_bytes, archive_raw_manifest
from sources_grid7 import BASE, SPEC, get, windows


def normalize_messages(messages):
    frame = pd.DataFrame(messages)
    if frame.id.duplicated().any():
        raise ValueError("Duplicate revision IDs")
    profiles = []
    for row in messages:
        for i, profile in enumerate(row.get("outageProfile") or []):
            profiles.append({"id": row["id"], "mrid": row["mrid"], "revisionNumber": row["revisionNumber"],
                             "publishTime": row["publishTime"], "segment_index": i,
                             "startTime": profile["startTime"], "endTime": profile["endTime"],
                             "source_capacity": profile["capacity"], "point_in_time_verified": False})
    frame = frame.drop(columns=["outageProfile"], errors="ignore")
    frame["outageProfile_json"] = [json.dumps(row.get("outageProfile"), sort_keys=True, allow_nan=False)
                                    for row in messages]
    for key in ["publishTime", "createdTime", "eventStartTime", "eventEndTime"]:
        frame[key] = pd.to_datetime(frame[key], utc=True)
    frame = frame.sort_values(["mrid", "publishTime", "revisionNumber", "id"]).reset_index(drop=True)
    frame["duration_hours"] = (frame.eventEndTime - frame.eventStartTime).dt.total_seconds() / 3600
    frame["invalid_effective_interval"] = frame.duration_hours.lt(0)
    frame["notice_hours_before_start"] = (frame.eventStartTime - frame.publishTime).dt.total_seconds() / 3600
    frame["point_in_time_verified"] = False
    prev = frame.groupby("mrid", sort=False)[["id", "revisionNumber", "assetId", "unavailableCapacity", "eventEndTime"]].shift()
    adjacent = frame.revisionNumber.eq(prev.revisionNumber + 1) & frame.assetId.eq(prev.assetId)
    frame["previous_returned_revision_id"] = prev.id.astype("Int64")
    frame["adjacent_previous_revision_available"] = adjacent
    # These are differences between returned revision fields, not a net regional
    # capacity series. Do not infer dismissed/inactive capacity is zero.
    frame["reported_unavailable_capacity_change_mw"] = (frame.unavailableCapacity - prev.unavailableCapacity).where(adjacent)
    frame["reported_end_time_change_hours"] = ((frame.eventEndTime - prev.eventEndTime).dt.total_seconds() / 3600).where(adjacent)
    profile_frame = pd.DataFrame(profiles)
    if len(profile_frame):
        for key in ["publishTime", "startTime", "endTime"]:
            profile_frame[key] = pd.to_datetime(profile_frame[key], utc=True)
        profile_frame["duration_seconds"] = (profile_frame.endTime - profile_frame.startTime).dt.total_seconds()
        profile_frame["nonpositive_duration"] = profile_frame.duration_seconds.le(0)
        # The API calls this field capacity, without an explicit available vs
        # unavailable definition. Keep the source name; do not integrate it.
    return frame, profile_frame


def collect(root=ROOT):
    root = Path(root)
    prior = json.loads((root / "logs/grid7_remit_requests.json").read_text())
    records, hashes, partitions = [], [], []
    for item in prior["detail_batches"]:
        p = root / "archive/raw" / (item["sha256"] + ".bin")
        raw = p.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("Original REMIT raw checksum mismatch")
        obj = json.loads(raw)["data"]
        if {x["id"] for x in obj} != set(item["ids"]) or len(obj) != item["rows"]:
            raise ValueError("Original detail membership changed")
        records.extend(obj)
        hashes.append(item["sha256"])
    hashes.extend(x["sha256"] for x in prior["partitions"])
    _, sh = get(SPEC)
    hashes.append(sh)
    end = pd.Timestamp.now(tz="UTC").floor("D").date().isoformat()
    for a, b in windows(prior["end_exclusive"], end, 7):
        raw, h = get(BASE + "/remit/list/by-publish", {"from": a.isoformat(), "to": b.isoformat(),
                      "latestRevisionOnly": "false", "profileOnly": "false", "format": "json"})
        ids = [x for x in json.loads(raw)["data"] if a <= pd.Timestamp(x["publishTime"]) < b]
        partitions.append({"start": a.isoformat(), "end": b.isoformat(), "sha256": h, "rows": len(ids)})
        hashes.append(h)
        for i in range(0, len(ids), 400):
            wanted = ids[i:i + 400]
            raw, dh = get(BASE + "/remit", {"messageId": [x["id"] for x in wanted], "format": "json"})
            rows = json.loads(raw)["data"]
            if {x["id"] for x in rows} != {x["id"] for x in wanted} or len(rows) != len(wanted):
                raise ValueError("Incomplete fresh REMIT detail response")
            references = {x["id"]: x for x in wanted}
            for row in rows:
                ref = references[row["id"]]
                if row["mrid"] != ref["mrid"] or row["revisionNumber"] != ref["revisionNumber"] or pd.Timestamp(row["publishTime"]) != pd.Timestamp(ref["publishTime"]):
                    raise ValueError("REMIT list/detail revision or publication mismatch")
            records.extend(rows)
            hashes.append(dh)
    frame, profiles = normalize_messages(records)
    frame["local_materialized_at"] = pd.Timestamp.now(tz="UTC")
    manifest = archive_raw_manifest(hashes, root=root, description="GB REMIT exact revision detail responses and publication lists, including batch11 update")
    files = []
    for name, data in [("capacity_revisions", frame), ("outage_profile_segments", profiles)]:
        p = root / "supplementary/outages11" / (name + ".parquet")
        atomic_bytes(p, data.to_parquet(index=False, compression="zstd"))
        files.append({"path": str(p.relative_to(root)), "rows": len(data), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    report = {"status": "complete", "source": "Elexon REMIT revision archive", "files": files,
              "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "raw_manifest_sha256": [manifest], "new_publication_partitions": partitions,
              "first_publication": frame.publishTime.min().isoformat(), "last_publication": frame.publishTime.max().isoformat(),
              "materialized_at": pd.Timestamp.now(tz="UTC").isoformat(),
              "distinct_outages_or_notices": int(frame.mrid.nunique()), "revision_rows": len(frame),
              "point_in_time_verified": False, "new_main_series": 0, "errors": [],
              "limitations": "Capacity fields and profiles are retrospective exact-ID responses. Original source first availability and immutable historical capacity content are not verified; profiles can include later information. Publication time, effective interval, materialization time and revision ID stay separate. No regional unavailable-MW totals or historical as-known capacity reconstruction claimed. Revision differences require consecutive returned source revision numbers and the same asset. Dismissed/inactive statuses retained without assuming zero capacity. Profile capacity semantics remain the source field; zero-duration segments flagged and not integrated."}
    atomic_bytes(root / "logs/outages11_summary.json", json.dumps(report, indent=2).encode())
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    collect()
