#!/usr/bin/env python3
"""Repair live Sacko flags from a DDL loser-path audit.

This script does not rearrange matchups, scores, playoff flags, or bracket
labels. It only clears stale ``sacko`` flags for mismatched seasons and writes
``sacko = 1`` on the DDL-derived Sacko row from
``ddl_sacko_bracket_results_audit.py``.
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

from multi_league.core.fly_writer import FlyWriter

DATABASE = "___leagues"
CHUNK_SIZE = 100


@dataclass(frozen=True)
class SackoRepair:
    db_name: str
    year: int
    week: int
    ddl_sacko: str
    ddl_sacko_name: str
    current_sacko: str | None
    current_sacko_name: str | None


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
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_last_week(raw: str | None) -> int:
    if not raw:
        return 0
    text = raw.strip()
    if "-" in text:
        _start, end = text.split("-", 1)
        return _to_int(end)
    weeks = [_to_int(part.strip()) for part in text.split(",")]
    weeks = [week for week in weeks if week]
    return max(weeks) if weeks else 0


def _read_repairs(path: Path) -> list[SackoRepair]:
    repairs: list[SackoRepair] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "MISMATCH":
                continue
            ddl_sacko = row.get("ddl_sacko")
            year = _to_int(row.get("year"))
            week = _parse_last_week(row.get("final_weeks"))
            if not ddl_sacko or not year or not week:
                continue
            repairs.append(
                SackoRepair(
                    db_name=str(row["db_name"]),
                    year=year,
                    week=week,
                    ddl_sacko=str(ddl_sacko),
                    ddl_sacko_name=str(row.get("ddl_sacko_name") or ddl_sacko),
                    current_sacko=str(row.get("current_sacko") or "") or None,
                    current_sacko_name=str(row.get("current_sacko_name") or "") or None,
                )
            )
    return repairs


def _filter_repairs(
    repairs: list[SackoRepair], db_names: list[str] | None, years: list[int] | None
) -> list[SackoRepair]:
    if db_names:
        allowed = set(db_names)
        repairs = [repair for repair in repairs if repair.db_name in allowed]
    if years:
        allowed_years = set(years)
        repairs = [repair for repair in repairs if repair.year in allowed_years]
    return repairs


def _update_sql(repairs: list[SackoRepair]) -> str:
    values = ",\n".join(f"({_sql_str(r.db_name)}, {r.year}, {r.week}, {_sql_str(r.ddl_sacko)})" for r in repairs)
    return f"""
    UPDATE public.matchup AS m
       SET sacko = CASE
           WHEN m.week = v.week AND m.franchise_id = v.ddl_sacko THEN 1
           ELSE 0
       END
      FROM (
        VALUES
        {values}
      ) AS v(db_name, year, week, ddl_sacko)
     WHERE m.db_name = v.db_name
       AND m.year = v.year
       AND (
            COALESCE(CAST(m.sacko AS INTEGER), 0) = 1
            OR (m.week = v.week AND m.franchise_id = v.ddl_sacko)
       )
    """


def _refresh_derived_sql(table: str, db_names: list[str]) -> str:
    db_filter = ", ".join(_sql_str(db_name) for db_name in db_names)
    if table == "matchup_season":
        return f"""
        UPDATE public.matchup_season AS ms
           SET is_sacko = COALESCE((
                SELECT MAX(CAST(sacko AS INT))
                  FROM public.matchup AS m
                 WHERE m.db_name = ms.db_name
                   AND m.year = ms.year
                   AND m.franchise_id = ms.franchise_id
           ), 0)
         WHERE ms.db_name IN ({db_filter})
        """
    if table == "matchup_career":
        return f"""
        UPDATE public.matchup_career AS mc
           SET sacko_seasons = COALESCE((
                SELECT SUM(CASE WHEN is_sacko = 1 THEN 1 ELSE 0 END)
                  FROM public.matchup_season AS ms
                 WHERE ms.db_name = mc.db_name
                   AND ms.franchise_id = mc.franchise_id
           ), 0)
         WHERE mc.db_name IN ({db_filter})
        """
    if table == "homepage_manager_profiles":
        return f"""
        UPDATE public.homepage_manager_profiles AS hp
           SET sacko_bowls = COALESCE((
                SELECT SUM(CASE WHEN is_sacko = 1 THEN 1 ELSE 0 END)
                  FROM public.matchup_season AS ms
                 WHERE ms.db_name = hp.db_name
                   AND ms.manager = hp.manager
           ), 0)
         WHERE hp.db_name IN ({db_filter})
        """
    raise ValueError(f"Unknown derived table: {table}")


def _count_result(result: list[dict]) -> int:
    if result and isinstance(result, list):
        first = result[0]
        for key in ("Count", "count", "updated", "affected_rows"):
            if key in first:
                return _to_int(first[key])
    return 0


def _apply_repairs(repairs: list[SackoRepair]) -> tuple[int, dict[str, int]]:
    writer = FlyWriter()
    updated = 0
    for start in range(0, len(repairs), CHUNK_SIZE):
        chunk = repairs[start : start + CHUNK_SIZE]
        result = writer.execute(_update_sql(chunk), database=DATABASE)
        updated += _count_result(result)

    db_names = sorted({repair.db_name for repair in repairs})
    derived_counts = {}
    for table in ("matchup_season", "matchup_career", "homepage_manager_profiles"):
        derived_counts[table] = _count_result(writer.execute(_refresh_derived_sql(table, db_names), database=DATABASE))
    return updated, derived_counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-csv", type=Path, required=True)
    parser.add_argument("--db", action="append")
    parser.add_argument("--year", type=int, action="append")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    _load_env()
    repairs = _filter_repairs(_read_repairs(args.audit_csv), args.db, args.year)

    print(f"Candidate Sacko flag repairs: {len(repairs)}")
    for repair in repairs[:20]:
        current = repair.current_sacko_name or repair.current_sacko or "<none>"
        print(f"- {repair.db_name} {repair.year} week {repair.week}: " f"{current} -> {repair.ddl_sacko_name}")
    if len(repairs) > 20:
        print(f"... {len(repairs) - 20} more")

    if not args.apply:
        print("Dry run only. Re-run with --apply to write Fly.")
        return 0

    updated, derived_counts = _apply_repairs(repairs)
    print(f"Applied Sacko flag repairs: seasons={len(repairs)} updated_rows={updated}")
    for table, count in derived_counts.items():
        print(f"Refreshed {table}: updated_rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
