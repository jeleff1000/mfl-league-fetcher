"""Rebuild the five damaged aggregate tables from intact persisted facts.

This is an offline storage-recovery helper. It deliberately calls only the
existing canonical aggregators for the five affected tables and validates only
those five outputs. Provider data is never fetched and no source or unrelated
derived table is rewritten.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PIPELINE_ROOT = _REPO_ROOT / "fantasy_football_data_scripts"
if str(_PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_ROOT))

import duckdb

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS
from multi_league.transformations.aggregation.aggregate_fantasy_context import (
    aggregate_fantasy_season,
    aggregate_fantasy_season_all,
)
from multi_league.transformations.aggregation.aggregate_matchup_context import (
    aggregate_matchup_h2h,
)
from multi_league.transformations.aggregation.aggregate_standings import (
    aggregate_standings,
)
from multi_league.transformations.aggregation.aggregation_utils import (
    replace_scoped_aggregate_table_from_dataframe,
)
from multi_league.transformations.aggregation.homepage_summary import (
    compute_manager_rankings,
)


TARGET_TABLES = (
    "homepage_manager_rankings",
    "matchup_h2h_career",
    "player_fantasy_season",
    "player_fantasy_season_all",
    "standings_by_year",
)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def discover_leagues(conn) -> list[str]:
    """Return every league represented in the two persisted fact tables."""
    rows = conn.execute(
        "SELECT DISTINCT db_name FROM ("
        "SELECT db_name FROM public.matchup "
        "UNION ALL SELECT db_name FROM public.player_fantasy"
        ") WHERE db_name IS NOT NULL AND TRIM(db_name) <> '' ORDER BY db_name"
    ).fetchall()
    return [str(row[0]) for row in rows]


def reaggregate_one_league(conn, db_name: str) -> dict[str, int]:
    """Rebuild exactly the five affected outputs for one persisted chain."""
    years = [
        int(row[0])
        for row in conn.execute(
            "SELECT DISTINCT TRY_CAST(year AS INTEGER) FROM public.matchup "
            "WHERE db_name = ? AND TRY_CAST(year AS INTEGER) IS NOT NULL ORDER BY 1",
            [db_name],
        ).fetchall()
    ]

    aggregate_fantasy_season(conn, db_name)
    aggregate_fantasy_season_all(conn, db_name)
    aggregate_matchup_h2h(conn, db_name, season_years=set())
    aggregate_standings(conn, db_name, years)
    rankings = compute_manager_rankings(conn, db_name)
    replace_scoped_aggregate_table_from_dataframe(
        conn,
        db_name,
        "homepage_manager_rankings",
        rankings,
    )

    return {
        table: int(
            conn.execute(
                f"SELECT COUNT(*) FROM public.{_quote_identifier(table)} WHERE db_name = ?",
                [db_name],
            ).fetchone()[0]
        )
        for table in TARGET_TABLES
    }


def validate_targets(conn) -> dict[str, dict[str, int]]:
    """Validate only the five rebuilt tables and their canonical keys."""
    results: dict[str, dict[str, int]] = {}
    for table in TARGET_TABLES:
        ref = f"public.{_quote_identifier(table)}"
        rows = int(conn.execute(f"SELECT COUNT(*) FROM {ref}").fetchone()[0])
        keys = ("db_name", *AGGREGATE_TABLE_SPECS[table].primary_key)
        key_sql = ", ".join(_quote_identifier(key) for key in keys)
        distinct_keys = int(
            conn.execute(
                f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM {ref} GROUP BY {key_sql})"
            ).fetchone()[0]
        )
        if rows <= 0:
            raise RuntimeError(f"{table} is empty after reaggregation")
        if rows != distinct_keys:
            raise RuntimeError(
                f"{table} has duplicate canonical keys: rows={rows}, distinct={distinct_keys}"
            )
        results[table] = {"rows": rows, "distinct_keys": distinct_keys}
    return results


def reaggregate_all(conn, *, db_names: list[str] | None = None) -> dict:
    """Reaggregate the five targets for all leagues, one transaction at a time."""
    leagues = sorted(set(db_names or discover_leagues(conn)))
    completed = 0
    for db_name in leagues:
        conn.execute("BEGIN TRANSACTION")
        try:
            reaggregate_one_league(conn, db_name)
            conn.execute("COMMIT")
            completed += 1
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(
                json.dumps(
                    {"completed": completed, "failed_db_name": db_name, "error": str(exc)},
                    sort_keys=True,
                )
            ) from exc
    targets = validate_targets(conn)
    conn.execute("CHECKPOINT")
    return {"leagues": completed, "targets": targets}


def _attach_if_present(conn, path: Path, catalog: str) -> None:
    if path.is_file():
        conn.execute(
            f"ATTACH '{path.as_posix().replace(chr(39), chr(39) * 2)}' "
            f"AS {_quote_identifier(catalog)} (READ_ONLY)"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--ops", type=Path)
    parser.add_argument("--ops-nfl", type=Path)
    parser.add_argument("--db-name", action="append", default=[])
    args = parser.parse_args()

    conn = duckdb.connect(str(args.database.resolve()))
    try:
        if args.ops_nfl:
            _attach_if_present(conn, args.ops_nfl.resolve(), "___ops_nfl")
        if args.ops:
            _attach_if_present(conn, args.ops.resolve(), "___ops")
        result = reaggregate_all(conn, db_names=args.db_name or None)
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
