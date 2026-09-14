#!/usr/bin/env python3
"""
All-Play Table Builder

Creates a normalized `all_play` table in MotherDuck answering:
"Would franchise A have outscored franchise B this week?" for ALL pairs,
regardless of actual opponent — via a CROSS JOIN of weekly matchup scores.

Replaces dynamic pivot columns (w_vs_jason, l_vs_jason) with a fixed-schema
relational table that works regardless of roster changes or team count.

Usage:
    python all_play.py --db kmffl
    python all_play.py --db kmffl --dry-run
    python all_play.py --context path/to/context.json  # legacy
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


# ---------------------------------------------------------------------------
# DDL / SQL
# ---------------------------------------------------------------------------

CENTRAL_DB_NAME = "___leagues"
ALL_PLAY_TABLE = f"{CENTRAL_DB_NAME}.public.all_play"
MATCHUP_TABLE = f"{CENTRAL_DB_NAME}.public.matchup"

DDL = """
CREATE TABLE IF NOT EXISTS ___leagues.public.all_play (
    db_name         VARCHAR,
    year            INTEGER,
    week            INTEGER,
    franchise_id    VARCHAR,
    opponent_franchise_id VARCHAR,
    result          VARCHAR,
    points          DOUBLE,
    opponent_points DOUBLE
)
"""

INSERT_SQL = """
INSERT INTO {all_play_table}
SELECT
    '{db_name}'                                           AS db_name,
    a.year,
    a.week,
    a.franchise_id,
    b.franchise_id                                          AS opponent_franchise_id,
    CASE
        WHEN a.team_points > b.team_points THEN 'W'
        WHEN a.team_points < b.team_points THEN 'L'
        ELSE 'T'
    END                                                     AS result,
    a.team_points                                           AS points,
    b.team_points                                           AS opponent_points
FROM  {matchup_table} a
CROSS JOIN {matchup_table} b
WHERE a.db_name         = '{db_name}'
  AND b.db_name         = '{db_name}'
  AND a.year            = b.year
  AND a.week            = b.week
  AND a.franchise_id   != b.franchise_id
  AND a.franchise_id   IS NOT NULL
  AND b.franchise_id   IS NOT NULL
  AND a.team_points    IS NOT NULL
  AND b.team_points    IS NOT NULL
"""


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def build_all_play(db_name: str, conn, dry_run: bool = False) -> int:
    """Ensure, replace scoped rows, and populate the all_play table.

    Args:
        db_name:  MotherDuck database name.
        conn:     Open DuckDB connection.
        dry_run:  If True, show what would happen without writing.

    Returns:
        Row count inserted (0 for dry-run).
    """
    # Verify matchup table exists and has franchise_id + team_points
    try:
        check = conn.execute(
            f"SELECT COUNT(*) FROM {MATCHUP_TABLE} "
            f"WHERE db_name = '{db_name}' "
            f"AND franchise_id IS NOT NULL AND team_points IS NOT NULL"
        ).fetchone()
        eligible_rows = check[0]
    except Exception as e:
        raise RuntimeError(f"Cannot read matchup table in {db_name}: {e}") from e

    print(f"  matchup rows eligible (franchise_id + team_points not null): {eligible_rows:,}")

    if eligible_rows == 0:
        print("  [WARN] No eligible matchup rows — all_play will be empty.")

    if dry_run:
        # Estimate output size: N*(N-1) per week
        est = conn.execute(
            f"""
            SELECT SUM(cnt * (cnt - 1)) AS estimated_rows
            FROM (
                SELECT year, week, COUNT(DISTINCT franchise_id) AS cnt
                FROM {MATCHUP_TABLE}
                WHERE db_name = '{db_name}'
                  AND franchise_id IS NOT NULL AND team_points IS NOT NULL
                GROUP BY year, week
            )
            """
        ).fetchone()
        print(f"  [DRY RUN] Estimated all_play rows: {est[0] if est[0] else 0:,}")
        print("  [DRY RUN] No changes written.")
        return 0

    # Ensure the centralized table exists before scoped replacement.
    print("  Ensuring centralized all_play table exists...")
    conn.execute(DDL)
    try:
        conn.execute(f"ALTER TABLE {ALL_PLAY_TABLE} ADD COLUMN IF NOT EXISTS db_name VARCHAR")
    except Exception:
        pass

    print("  Replacing scoped all_play rows...")
    execute_scoped(conn, f"DELETE FROM {ALL_PLAY_TABLE} WHERE db_name = '{db_name}'", db_name, label="all_play:delete")

    print("  Inserting cross-join records...")
    execute_scoped(
        conn,
        INSERT_SQL.format(
            all_play_table=ALL_PLAY_TABLE,
            matchup_table=MATCHUP_TABLE,
            db_name=db_name,
        ),
        db_name,
        label="all_play:insert",
    )

    row_count = conn.execute(f"SELECT COUNT(*) FROM {ALL_PLAY_TABLE} WHERE db_name = '{db_name}'").fetchone()[0]

    print(f"  [OK] all_play populated: {row_count:,} rows")
    return row_count


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def _resolve_db_name(args) -> str:
    """Resolve database name from --db or --context."""
    if getattr(args, "db", None):
        return sanitize_database_name(args.db)
    if not getattr(args, "context", None):
        raise SystemExit("Must provide --db or --context")
    with open(args.context) as f:
        ctx = json.load(f)
    db_name = ctx.get("db_name") or ctx.get("database_name") or ctx.get("motherduck_db_name") or ctx.get("league_name")
    if not db_name:
        raise SystemExit("No league_name found in context JSON")
    return sanitize_database_name(db_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Build normalized all_play table from matchup cross-join")
    parser.add_argument("--db", type=str, help="MotherDuck database name")
    parser.add_argument("--context", type=str, help="Path to league_context.json (legacy)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show estimated row count without writing",
    )
    args = parser.parse_args()

    db_name = _resolve_db_name(args)
    print(f"[all_play] Database: {db_name}")

    if args.dry_run:
        print("[all_play] Dry-run mode — no changes will be written")

    conn = get_pipeline_connection(db_name, qualified=True)
    try:
        row_count = build_all_play(db_name, conn, dry_run=args.dry_run)
    finally:
        conn.close()

    if not args.dry_run:
        print(f"\n[all_play] Done. {row_count:,} rows written to {ALL_PLAY_TABLE} for db_name={db_name}")


if __name__ == "__main__":
    main()
