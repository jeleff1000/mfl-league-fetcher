"""Audit JSON/JSONL/CSV candidate files for directly promotable facts.

Most structured artifacts are status or validation reports.  This pass walks
their nested records and distinguishes those from records carrying canonical
league/week/player signals.  It never changes the cache and never promotes a
record merely because a filename looks relevant.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterator


SIGNALS = {
    "win", "loss", "tie", "team_points", "source_team_points", "is_playoffs",
    "champion", "is_championship", "final_playoff_seed", "made_playoffs",
    "clutch_equity", "source_clutch_equity",
}
SETTINGS = {
    "pass_td", "scoring_pass_td", "pass_td_fill", "playoff_teams", "playoff_teams_fill",
    "roster_flx", "roster_flx_fill", "roster_super_flex", "roster_super_flex_fill",
    "roster_idp", "roster_idp_fill", "best_ball", "best_ball_fill", "sleeper_best_ball",
}


def records(value: Any, parent: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    context = dict(parent or {})
    if isinstance(value, dict):
        own = {str(k): v for k, v in value.items() if not isinstance(v, (dict, list))}
        merged = {**context, **own}
        if own:
            yield merged
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from records(child, merged)
    elif isinstance(value, list):
        for child in value:
            yield from records(child, context)


def load(path: Path) -> Iterator[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            yield from records(list(csv.DictReader(handle)))
        return
    if suffix == ".jsonl":
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.strip():
                try:
                    yield from records(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return
    if suffix == ".json":
        try:
            yield from records(json.loads(path.read_text(encoding="utf-8", errors="replace")))
        except json.JSONDecodeError:
            return


def present(row: dict[str, Any]) -> set[str]:
    return {str(k).strip().lower() for k, v in row.items() if v not in (None, "", [], {})}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    report = []
    for path in sorted(args.candidates.rglob("*")):
        if path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
            continue
        row_count = signal_rows = settings_rows = 0
        fields: set[str] = set()
        for row in load(path):
            keys = present(row)
            fields |= keys
            has_identity = "db_name" in keys and "year" in keys
            has_week_identity = "week" in keys and ("team_key" in keys or "manager" in keys or "nfl_player_id" in keys)
            has_signal = bool(keys & SIGNALS)
            has_setting = bool(keys & SETTINGS)
            row_count += 1
            if has_identity and has_week_identity and has_signal:
                signal_rows += 1
            if has_identity and has_setting:
                settings_rows += 1
        if signal_rows:
            disposition = "structured_direct_signal_payload"
        elif settings_rows:
            disposition = "structured_settings_payload"
        else:
            disposition = "structured_metadata_or_status"
        report.append({
            "file": str(path), "rows_seen": row_count, "signal_rows": signal_rows,
            "settings_rows": settings_rows, "fields": sorted(fields), "disposition": disposition,
        })
    result = {
        "files": len(report),
        "direct_signal_files": sum(r["signal_rows"] > 0 for r in report),
        "settings_payload_files": sum(r["settings_rows"] > 0 for r in report),
        "metadata_or_status_files": sum(r["disposition"] == "structured_metadata_or_status" for r in report),
        "promotable_structured_rows": sum(r["signal_rows"] for r in report),
        "settings_rows": sum(r["settings_rows"] for r in report),
        "records": report,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: result[k] for k in result if k != "records"}, sort_keys=True))


if __name__ == "__main__":
    main()
