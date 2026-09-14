#!/usr/bin/env python3
"""Collapse stale inferred playoff paths to the DDL championship-label final.

Use this only for audit rows whose actual winner source is
``championship_label``. It does not rearrange matchups or infer a new bracket.
It preserves the played games and scores, then keeps only the labeled final as
the championship bracket path for seasons where prior inferred flags disagree
with that final.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

from multi_league.core.db_reader import get_reader
from multi_league.core.fly_writer import FlyWriter

DATABASE = "___leagues"
CHUNK_SIZE = 50


@dataclass(frozen=True)
class FinalRepair:
    db_name: str
    year: int
    playoff_start_week: int
    final_weeks: tuple[int, ...]
    did_winner: str
    runner_up: str
    did_winner_name: str
    runner_up_name: str


def _load_env() -> None:
    for env_file in (ROOT / ".env", ROOT / "frontend" / ".env.local"):
        if not env_file.exists():
            continue
        for raw in env_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _sql_str(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_weeks(raw: str | None) -> tuple[int, ...]:
    if not raw:
        return ()
    text = raw.strip()
    if "-" in text:
        start, end = text.split("-", 1)
        start_i = _to_int(start)
        end_i = _to_int(end)
        if start_i and end_i and end_i >= start_i:
            return tuple(range(start_i, end_i + 1))
    weeks = tuple(_to_int(part.strip()) for part in text.split(",") if _to_int(part.strip()))
    return weeks


def _read_mismatches(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "MISMATCH":
                continue
            if row.get("source") != "championship_label":
                continue
            if not row.get("did_winner"):
                continue
            if not _parse_weeks(row.get("final_weeks")):
                continue
            rows.append(row)
    return rows


def _load_playoff_starts(reader, rows: list[dict[str, Any]]) -> dict[tuple[str, int], int]:
    if not rows:
        return {}
    values = ", ".join(f"({_sql_str(row['db_name'])}, {_to_int(row['year'])})" for row in rows)
    settings = reader.query(
        f"""
        SELECT db_name, year, playoff_start_week
        FROM public.league_settings
        WHERE (db_name, year) IN (VALUES {values})
        """,
        database=DATABASE,
    )
    return {(str(row["db_name"]), int(row["year"])): _to_int(row.get("playoff_start_week"), 1) for row in settings}


def _resolve_final_repairs(reader, audit_rows: list[dict[str, Any]]) -> list[FinalRepair]:
    starts = _load_playoff_starts(reader, audit_rows)
    repairs: list[FinalRepair] = []
    for row in audit_rows:
        db_name = str(row["db_name"])
        year = _to_int(row["year"])
        did_winner = str(row["did_winner"])
        final_weeks = _parse_weeks(row.get("final_weeks"))
        if not year or not final_weeks:
            continue
        week_values = ", ".join(str(week) for week in final_weeks)
        final_rows = reader.query(
            f"""
            SELECT week, franchise_id, opponent_franchise_id, manager, opponent
            FROM public.matchup
            WHERE db_name = {_sql_str(db_name)}
              AND year = {year}
              AND week IN ({week_values})
              AND franchise_id = {_sql_str(did_winner)}
              AND opponent_franchise_id IS NOT NULL
              AND lower(trim(coalesce(opponent, ''))) NOT IN ('', 'bye', 'none', 'null', 'nan')
            ORDER BY week DESC
            LIMIT 1
            """,
            database=DATABASE,
        )
        if not final_rows:
            print(f"SKIP {db_name} {year}: did_winner final row not found")
            continue
        final_row = final_rows[0]
        runner_up = str(final_row["opponent_franchise_id"])
        repairs.append(
            FinalRepair(
                db_name=db_name,
                year=year,
                playoff_start_week=starts.get((db_name, year), min(final_weeks)),
                final_weeks=final_weeks,
                did_winner=did_winner,
                runner_up=runner_up,
                did_winner_name=str(row.get("did_winner_name") or final_row.get("manager") or did_winner),
                runner_up_name=str(row.get("runner_up_name") or final_row.get("opponent") or runner_up),
            )
        )
    return repairs


def _update_sql(repairs: list[FinalRepair]) -> str:
    values = ",\n".join(
        "("
        f"{_sql_str(r.db_name)}, {r.year}, {r.playoff_start_week}, "
        f"{_sql_str(','.join(str(w) for w in r.final_weeks))}, "
        f"{_sql_str(r.did_winner)}, {_sql_str(r.runner_up)}"
        ")"
        for r in repairs
    )
    return f"""
    UPDATE public.matchup AS m
       SET champion = CASE
               WHEN m.franchise_id = v.did_winner
                AND list_contains(string_split(v.final_weeks_csv, ','), CAST(m.week AS VARCHAR))
               THEN 1 ELSE 0 END,
           is_championship = CASE
               WHEN m.franchise_id IN (v.did_winner, v.runner_up)
                AND list_contains(string_split(v.final_weeks_csv, ','), CAST(m.week AS VARCHAR))
               THEN TRUE ELSE FALSE END,
           is_playoffs = CASE
               WHEN m.franchise_id IN (v.did_winner, v.runner_up)
                AND list_contains(string_split(v.final_weeks_csv, ','), CAST(m.week AS VARCHAR))
               THEN TRUE ELSE FALSE END,
           is_consolation = CASE
               WHEN m.franchise_id IN (v.did_winner, v.runner_up)
                AND list_contains(string_split(v.final_weeks_csv, ','), CAST(m.week AS VARCHAR))
               THEN FALSE ELSE TRUE END,
           playoff_round = CASE
               WHEN m.franchise_id IN (v.did_winner, v.runner_up)
                AND list_contains(string_split(v.final_weeks_csv, ','), CAST(m.week AS VARCHAR))
               THEN 'championship' ELSE NULL END,
           playoff_round_num = NULL,
           playoff_week_index = NULL
      FROM (
        VALUES
        {values}
      ) AS v(db_name, year, playoff_start_week, final_weeks_csv, did_winner, runner_up)
     WHERE m.db_name = v.db_name
       AND m.year = v.year
       AND m.week >= v.playoff_start_week
       AND COALESCE(m.postseason, TRUE)
    """


def _apply_repairs(repairs: list[FinalRepair]) -> int:
    writer = FlyWriter()
    updated = 0
    for start in range(0, len(repairs), CHUNK_SIZE):
        chunk = repairs[start : start + CHUNK_SIZE]
        result = writer.execute(_update_sql(chunk), database=DATABASE)
        if result and isinstance(result, list):
            first = result[0]
            for key in ("Count", "count", "updated", "affected_rows"):
                if key in first:
                    updated += _to_int(first[key])
                    break
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-csv", type=Path, required=True)
    parser.add_argument("--db", action="append")
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    _load_env()
    rows = _read_mismatches(args.audit_csv)
    if args.db:
        dbs = set(args.db)
        rows = [row for row in rows if row.get("db_name") in dbs]
    if args.year:
        years = {str(year) for year in args.year}
        rows = [row for row in rows if row.get("year") in years]

    reader = get_reader()
    repairs = _resolve_final_repairs(reader, rows)
    for repair in repairs:
        print(
            f"{repair.db_name} {repair.year}: final weeks {repair.final_weeks} "
            f"{repair.did_winner_name} over {repair.runner_up_name}"
        )

    print(f"\nFinal-label repairs: {len(repairs)}")
    if not repairs:
        return 0
    if not args.apply:
        print("Dry-run only. Re-run with --apply to write these repairs.")
        return 0

    updated = _apply_repairs(repairs)
    print(f"Applied final-label repair UPDATE statements. Reported updated rows: {updated or 'unknown'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
