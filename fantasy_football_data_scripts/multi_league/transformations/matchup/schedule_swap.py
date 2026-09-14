#!/usr/bin/env python3
"""
Schedule Swap Table Builder

Answers the question: "If franchise A had played franchise B's actual opponent
this week, would A have won?"

Produces a fixed-schema relational table that replaces dynamic pivot columns
(w_vs_jason_sched, l_vs_jason_sched) with rows keyed by
(year, week, franchise_id, schedule_of_franchise_id).

Usage:
    python schedule_swap.py --db kmffl
    python schedule_swap.py --db kmffl --dry-run
    python schedule_swap.py --context path/to/league_context.json  # legacy
"""

import argparse
import json

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.db_utils import get_pipeline_connection, sanitize_database_name
from multi_league.core.sql_utils import execute_scoped

DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    db_name VARCHAR,
    year INTEGER,
    week INTEGER,
    franchise_id VARCHAR,
    schedule_of_franchise_id VARCHAR,
    result VARCHAR,
    my_points DOUBLE,
    their_opponent_points DOUBLE
)
"""

INSERT_SQL = """
INSERT INTO {table_name} (
    db_name,
    year,
    week,
    franchise_id,
    schedule_of_franchise_id,
    result,
    my_points,
    their_opponent_points
)
SELECT
    '{db_name}' AS db_name,
    a.year,
    a.week,
    a.franchise_id,
    b.franchise_id AS schedule_of_franchise_id,
    CASE
        WHEN a.team_points > b.opponent_points THEN 'W'
        WHEN a.team_points < b.opponent_points THEN 'L'
        ELSE 'T'
    END AS result,
    a.team_points AS my_points,
    b.opponent_points AS their_opponent_points
FROM {matchup_table} a
CROSS JOIN {matchup_table} b
WHERE a.db_name = '{db_name}'
  AND b.db_name = '{db_name}'
  AND a.year = b.year
  AND a.week = b.week
  AND (a.franchise_id = b.franchise_id OR b.opponent_franchise_id != a.franchise_id)
  AND a.franchise_id IS NOT NULL
  AND b.franchise_id IS NOT NULL
  AND a.team_points IS NOT NULL
  AND b.opponent_points IS NOT NULL
{playoff_filters}
"""


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def build_schedule_swap(db_name: str, conn, dry_run: bool = False) -> dict:
    """Replace scoped rows and populate the schedule_swap table.

    Returns a stats dict with row_count (or 0 for dry runs).
    """
    stats = {"row_count": 0, "dry_run": dry_run}
    catalog = conn.execute("SELECT current_database()").fetchone()[0]
    table_name = f'"{catalog}".public.schedule_swap'
    matchup_table = f'"{catalog}".public.matchup'

    # Verify matchup table exists and has the columns we need
    print("\n[Checking] matchup table in MotherDuck...")
    try:
        check = conn.execute(f"SELECT COUNT(*) FROM {matchup_table} WHERE db_name = '{db_name}'").fetchone()
        total_matchup = check[0] if check else 0
        print(f"  matchup rows: {total_matchup:,}")
    except Exception as e:
        print(f"  [FAIL] Could not access matchup table: {e}")
        return stats

    # Verify required columns exist
    required_cols = {"year", "week", "franchise_id", "opponent_franchise_id", "team_points", "opponent_points"}
    try:
        col_rows = conn.execute(f"DESCRIBE {matchup_table}").fetchall()
        existing_cols = {r[0] for r in col_rows}
    except Exception:
        print("  [FAIL] Could not inspect matchup columns")
        return stats

    missing = required_cols - existing_cols
    if missing:
        print(f"  [FAIL] matchup table is missing required columns: {sorted(missing)}")
        print("  Run discover_franchises.py first to populate franchise_id.")
        return stats

    playoff_filters = ""
    if {"is_playoffs", "is_consolation"}.issubset(existing_cols):
        playoff_filters = """
  AND COALESCE(a.is_playoffs, 0) = 0
  AND COALESCE(a.is_consolation, 0) = 0
  AND COALESCE(b.is_playoffs, 0) = 0
  AND COALESCE(b.is_consolation, 0) = 0"""

    print(f"  Required columns present: {sorted(required_cols)}")

    if dry_run:
        # Show what the INSERT would produce, without writing
        print("\n[DRY RUN] Previewing schedule_swap rows (first 5)...")
        try:
            select_sql = f"""
SELECT
    '{db_name}' AS db_name,
    a.year,
    a.week,
    a.franchise_id,
    b.franchise_id AS schedule_of_franchise_id,
    CASE
        WHEN a.team_points > b.opponent_points THEN 'W'
        WHEN a.team_points < b.opponent_points THEN 'L'
        ELSE 'T'
    END AS result,
    a.team_points AS my_points,
    b.opponent_points AS their_opponent_points
FROM {matchup_table} a
CROSS JOIN {matchup_table} b
WHERE a.db_name = '{db_name}'
  AND b.db_name = '{db_name}'
  AND a.year = b.year
  AND a.week = b.week
  AND (a.franchise_id = b.franchise_id OR b.opponent_franchise_id != a.franchise_id)
  AND a.franchise_id IS NOT NULL
  AND b.franchise_id IS NOT NULL
  AND a.team_points IS NOT NULL
  AND b.opponent_points IS NOT NULL
{playoff_filters}
""".format(db_name=db_name, matchup_table=matchup_table, playoff_filters=playoff_filters)
            count_result = conn.execute(f"SELECT COUNT(*) FROM ({select_sql}) sub").fetchone()
            projected_rows = count_result[0] if count_result else 0
            print(f"  Projected rows: {projected_rows:,}")

            sample = conn.execute(select_sql + " LIMIT 5").df()
            print(sample.to_string(index=False))
        except Exception as e:
            print(f"  Could not preview: {e}")
        return stats

    # Replace scoped rows
    print(f"\n[Rebuilding] schedule_swap in {db_name}...")
    try:
        conn.execute(DDL.format(table_name=table_name))
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS db_name VARCHAR")
    except Exception:
        pass

    try:
        execute_scoped(
            conn,
            f"DELETE FROM {table_name} WHERE db_name = '{db_name}'",
            db_name,
            label="schedule_swap:delete",
        )
        print("  Cleared existing schedule_swap rows for this db_name")
    except Exception as e:
        print(f"  [FAIL] Could not clear schedule_swap: {e}")
        return stats

    # Populate
    print("  Inserting rows via CROSS JOIN...")
    try:
        execute_scoped(
            conn,
            INSERT_SQL.format(
                table_name=table_name,
                matchup_table=matchup_table,
                db_name=db_name,
                playoff_filters=playoff_filters,
            ),
            db_name,
            label="schedule_swap:insert",
        )
    except Exception as e:
        print(f"  [FAIL] INSERT failed: {e}")
        return stats

    # Count
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE db_name = '{db_name}'").fetchone()
        stats["row_count"] = result[0] if result else 0
    except Exception as e:
        print(f"  WARNING: Could not count rows: {e}")

    print(f"  Inserted {stats['row_count']:,} rows")
    return stats


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def _resolve_db_name(args) -> str:
    """Resolve db_name from --db or --context."""
    if getattr(args, "db", None):
        return sanitize_database_name(args.db)
    if not getattr(args, "context", None):
        raise SystemExit("Must provide --context or --db")

    with open(args.context) as f:
        ctx_data = json.load(f)
    league_name = ctx_data.get("league_name", "")
    if not league_name:
        raise SystemExit("No league_name in context JSON")
    return sanitize_database_name(league_name)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main(args):
    """Main entry point."""
    print("\n" + "=" * 80)
    print("SCHEDULE SWAP TABLE BUILDER")
    print("=" * 80)

    db_name = _resolve_db_name(args)
    print(f"\n[Database] {db_name}")

    data_dir = None
    if getattr(args, "context", None):
        with open(args.context) as f:
            ctx_data = json.load(f)
        data_dir = ctx_data.get("data_directory")

    conn = get_pipeline_connection(db_name, data_dir, qualified=True)

    if args.dry_run:
        print("\n[DRY RUN] No changes will be made")

    stats = build_schedule_swap(db_name, conn, dry_run=args.dry_run)

    conn.close()

    print("\n" + "=" * 80)
    print("[SUMMARY]")
    if args.dry_run:
        print("  Dry run complete — no changes written")
    else:
        print(f"  schedule_swap rows written: {stats['row_count']:,}")
        expected_note = "  (expected: managers x (managers-1) x weeks, e.g. 10x9x16 = 1,440)"
        print(expected_note)
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the schedule_swap table in MotherDuck")
    parser.add_argument("--context", help="Path to league_context.json (legacy)")
    parser.add_argument("--db", help="MotherDuck database name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be written without making changes",
    )
    _args = parser.parse_args()
    main(_args)
