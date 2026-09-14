"""Build uncovered MFL season/ID intervals for the quota campaign.

Coverage is treated as attempted when a range is complete, partial, or
unverified.  This prevents a pending/partial run from being probed again while
keeping its results separate from the reconciled register.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def intervals(rows: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for lo, hi in sorted(rows):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [tuple(x) for x in merged]


def subtract(full: tuple[int, int], covered: list[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    cursor = full[0]
    for lo, hi in covered:
        if hi < cursor:
            continue
        if lo > full[1]:
            break
        if lo > cursor:
            out.append((cursor, min(full[1], lo - 1)))
        cursor = max(cursor, hi + 1)
        if cursor > full[1]:
            break
    if cursor <= full[1]:
        out.append((cursor, full[1]))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ranges", type=Path, required=True)
    ap.add_argument("--quota", type=Path, required=True)
    ap.add_argument("--pending", type=Path)
    ap.add_argument("--start-year", type=int, default=1993)
    ap.add_argument("--end-year", type=int, default=2018)
    ap.add_argument("--id-upper", type=int, default=80000)
    ap.add_argument("--target", type=int, default=10000)
    ap.add_argument("--window-probes", type=int, default=50000)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    covered: dict[int, list[tuple[int, int]]] = {}
    with args.ranges.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            year = int(row["year"])
            if args.start_year <= year <= args.end_year:
                covered.setdefault(year, []).append((int(row["id_start"]), int(row["id_end"])))
    if args.pending and args.pending.exists():
        with args.pending.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get("state") in {"partially_scanned_unreconciled", "reserved_unreconciled"}:
                    lo, hi = int(row["id_start"]), int(row["id_end"])
                    for year in range(int(row["start_year"]), int(row["end_year"]) + 1):
                        if args.start_year <= year <= args.end_year:
                            covered.setdefault(year, []).append((lo, hi))

    quota: dict[int, int] = {}
    with args.quota.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            year = int(row["year"])
            if args.start_year <= year <= args.end_year:
                quota[year] = int(row["samples_remaining"])

    tasks = []
    for year in range(args.end_year, args.start_year - 1, -1):
        if quota.get(year, args.target) <= 0:
            continue
        seen = intervals(covered.get(year, []))
        for lo, hi in subtract((0, args.id_upper), seen):
            tasks.append({"year": year, "id_start": lo, "id_end": hi, "probe_count": hi - lo + 1})

    windows = []
    current = []
    current_count = 0
    for task in tasks:
        lo = task["id_start"]
        while lo <= task["id_end"]:
            room = max(1, args.window_probes - current_count)
            take_hi = min(task["id_end"], lo + room - 1)
            piece = {"year": task["year"], "id_start": lo, "id_end": take_hi, "probe_count": take_hi - lo + 1}
            current.append(piece)
            current_count += piece["probe_count"]
            lo = take_hi + 1
            if current_count >= args.window_probes:
                windows.append({"window_index": len(windows), "probe_count": current_count, "tasks": current})
                current = []
                current_count = 0
    if current:
        windows.append({"window_index": len(windows), "probe_count": current_count, "tasks": current})

    payload = {
        "schema_version": 1,
        "unit": "uncovered (season, league_id) interval",
        "start_year": args.start_year,
        "end_year": args.end_year,
        "id_upper": args.id_upper,
        "target_samples": args.target,
        "tasks": tasks,
        "task_count": len(tasks),
        "probe_count": sum(x["probe_count"] for x in tasks),
        "window_probe_limit": args.window_probes,
        "windows": windows,
        "window_count": len(windows),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ("task_count", "probe_count", "window_count")}, sort_keys=True))


if __name__ == "__main__":
    main()
