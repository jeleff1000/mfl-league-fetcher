"""Select and validate the season-scoped MFL execution set.

The discovery register is keyed by ``(season, league_id)``.  This module keeps
that coordinate intact all the way through the Actions campaign and refuses a
receipt that reports a different season, even when the database name looks
plausible.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import ijson


PLAYER_REQUIRED = {
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager",
}
MATCHUP_REQUIRED = {
    "db_name", "year", "week", "manager", "team_points", "win", "loss",
    "is_playoffs", "final_playoff_seed", "champion", "is_championship",
}
SIGNALS = ("manager", "win", "is_playoffs", "champion", "clutch_equity")


def _quality(row: dict) -> tuple[int, int, int, str, str]:
    return (
        0 if row.get("draft_status") == "available" else 1,
        0 if str(row.get("name") or "").strip() else 1,
        0 if row.get("link_type") == "seed" else 1,
        str(row.get("league_id")),
        str(row.get("name") or "").lower(),
    )


def select_manifest_rows(
    rows: Iterable[dict], *, start_year: int, end_year: int, per_year: int = 10
) -> list[dict]:
    grouped: dict[int, dict[str, dict]] = {}
    for raw in rows:
        try:
            season = int(raw["season"])
            league_id = str(raw["league_id"])
        except (KeyError, TypeError, ValueError):
            continue
        if not start_year <= season <= end_year or not league_id:
            continue
        grouped.setdefault(season, {})[league_id] = raw

    missing = {
        year: len(grouped.get(year, {}))
        for year in range(start_year, end_year + 1)
        if len(grouped.get(year, {})) < per_year
    }
    if missing:
        detail = ", ".join(f"{year} has {count}/{per_year}" for year, count in missing.items())
        raise ValueError(f"MFL execution set is underpopulated: {detail}")

    selected = []
    for year in range(start_year, end_year + 1):
        for raw in sorted(grouped[year].values(), key=_quality)[:per_year]:
            selected.append({
                "season": year,
                "league_id": str(raw["league_id"]),
                "seed": f"{year}:{raw['league_id']}",
                "name": raw.get("name") or "",
                "lineage_id": raw.get("lineage_id"),
                "link_type": raw.get("link_type") or "seed",
                "draft_status": raw.get("draft_status") or "unknown",
            })
    return selected


def _columns(receipt: dict, table: str) -> set[str]:
    raw = receipt.get("schema_columns", {}).get(table, [])
    return {str(value) for value in raw}


def validate_receipt(
    manifest: Iterable[dict], receipt: Iterable[dict], *,
    start_year: int, end_year: int, per_year: int = 10,
) -> list[str]:
    expected = {(int(row["season"]), str(row["league_id"])) for row in manifest}
    actual_rows = list(receipt)
    actual = {(int(row.get("season", -1)), str(row.get("league_id", ""))) for row in actual_rows}
    errors: list[str] = []
    if actual != expected:
        errors.append(f"receipt keys differ: missing={sorted(expected - actual)[:10]} extra={sorted(actual - expected)[:10]}")
    counts = {year: 0 for year in range(start_year, end_year + 1)}
    for row in actual_rows:
        season = int(row.get("season", -1))
        league_id = str(row.get("league_id", ""))
        if (season, league_id) not in expected:
            continue
        counts[season] = counts.get(season, 0) + 1
        db_name = str(row.get("db_name") or "")
        if not db_name:
            errors.append(f"{season}:{league_id} missing db_name")
        if int(row.get("row_counts", {}).get("player_fantasy", 0) or 0) <= 0:
            errors.append(f"{season}:{league_id} missing player_fantasy rows")
        if int(row.get("row_counts", {}).get("matchup", 0) or 0) <= 0:
            errors.append(f"{season}:{league_id} missing matchup rows")
        for signal in SIGNALS:
            if int(row.get("non_null_counts", {}).get(signal, 0) or 0) <= 0:
                errors.append(f"{season}:{league_id} missing {signal} population")
        if not PLAYER_REQUIRED <= _columns(row, "player_fantasy"):
            errors.append(f"{season}:{league_id} player_fantasy schema is incomplete")
        if not MATCHUP_REQUIRED <= _columns(row, "matchup"):
            errors.append(f"{season}:{league_id} matchup schema is incomplete")
    bad_counts = {year: count for year, count in counts.items() if count != per_year}
    if bad_counts:
        errors.append(f"receipt does not have {per_year} rows per year: {bad_counts}")
    return errors


def load_register_rows(path: Path) -> Iterable[dict]:
    with path.open("rb") as handle:
        yield from ijson.items(handle, "observations.item")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--register", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--start-year", type=int, default=1993)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--per-year", type=int, default=10)
    args = parser.parse_args()
    selected = select_manifest_rows(
        load_register_rows(args.register),
        start_year=args.start_year, end_year=args.end_year, per_year=args.per_year,
    )
    payload = {
        "schema_version": "mfl_execution_manifest_v1",
        "identity_rule": "season is the MFL API observation year; never infer it from db_name",
        "start_year": args.start_year,
        "end_year": args.end_year,
        "per_year": args.per_year,
        "rows": selected,
        "summary": {"years": args.end_year - args.start_year + 1, "rows": len(selected)},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
