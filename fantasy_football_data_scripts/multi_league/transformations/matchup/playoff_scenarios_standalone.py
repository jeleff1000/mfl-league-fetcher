#!/usr/bin/env python3
"""
Playoff Scenarios (Standalone)

Wraps modules/playoff_scenarios.add_playoff_scenario_columns() with MotherDuck I/O.
Reads matchup + league_settings from MotherDuck, computes scenario columns, writes back.

Usage:
    python playoff_scenarios_standalone.py --db kmffl
    python playoff_scenarios_standalone.py --db kmffl --dry-run
    python playoff_scenarios_standalone.py --context path/to/league_context.json  # legacy
"""

from __future__ import annotations

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

import pandas as pd

from multi_league.core.db_utils import get_pipeline_connection, sanitize_database_name
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.matchup.modules.playoff_scenarios import (
    add_playoff_scenario_columns,
)

# ---------------------------------------------------------------------------
# Columns to read from matchup (thin read — only what the module needs)
# ---------------------------------------------------------------------------

MATCHUP_COLUMNS = [
    "franchise_id",
    "year",
    "week",
    "manager",
    "opponent",
    "team_points",
    "opponent_points",
    "win",
    "loss",
    "tie",
    "wins_to_date",
    "losses_to_date",
    "points_scored_to_date",
    "is_playoffs",
    "is_consolation",
    "playoff_seed_to_date",
    "above_league_median",
    # Playoff odds columns (may not exist — handled below)
    "p_playoffs",
    "p_champ",
    "p_bye",
]

# Columns added by add_playoff_scenario_columns — the ones we write back
SCENARIO_COLUMNS = [
    "playoff_magic_number",
    "bye_magic_number",
    "first_seed_magic_number",
    "elimination_number",
    "clinched_playoffs",
    "clinched_bye",
    "clinched_first_seed",
    "eliminated_from_playoffs",
    "eliminated_from_bye",
    "p_playoffs_change",
    "p_playoffs_prev",
    "p_champ_change",
    "p_champ_prev",
    "p_bye_change",
    "p_bye_prev",
    "max_odds_swing",
    "is_critical_matchup",
    "is_dramatic_win",
    "is_dramatic_loss",
    "drama_score",
    "team_mu",
    "team_sigma",
    "win_probability_vs_avg",
]

# DuckDB types for each new column (used in ALTER TABLE)
COLUMN_TYPES = {
    "playoff_magic_number": "DOUBLE",
    "bye_magic_number": "DOUBLE",
    "first_seed_magic_number": "DOUBLE",
    "elimination_number": "DOUBLE",
    "clinched_playoffs": "INTEGER",
    "clinched_bye": "INTEGER",
    "clinched_first_seed": "INTEGER",
    "eliminated_from_playoffs": "INTEGER",
    "eliminated_from_bye": "INTEGER",
    "p_playoffs_change": "DOUBLE",
    "p_playoffs_prev": "DOUBLE",
    "p_champ_change": "DOUBLE",
    "p_champ_prev": "DOUBLE",
    "p_bye_change": "DOUBLE",
    "p_bye_prev": "DOUBLE",
    "max_odds_swing": "DOUBLE",
    "is_critical_matchup": "INTEGER",
    "is_dramatic_win": "INTEGER",
    "is_dramatic_loss": "INTEGER",
    "drama_score": "DOUBLE",
    "team_mu": "DOUBLE",
    "team_sigma": "DOUBLE",
    "win_probability_vs_avg": "DOUBLE",
}

CENTRAL_DB_NAME = "___leagues"
ACTIVE_TABLE_CATALOG = CENTRAL_DB_NAME


def current_catalog(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def configure_table_catalog(conn) -> None:
    global ACTIVE_TABLE_CATALOG
    catalog = current_catalog(conn)
    if catalog == "memory" and ACTIVE_TABLE_CATALOG not in {CENTRAL_DB_NAME, "memory"}:
        return
    ACTIVE_TABLE_CATALOG = catalog


def _ensure_active_catalog(conn) -> None:
    if ACTIVE_TABLE_CATALOG != current_catalog(conn):
        configure_table_catalog(conn)


def matchup_table(conn) -> str:
    _ensure_active_catalog(conn)
    return f"{ACTIVE_TABLE_CATALOG}.public.matchup"


def league_settings_table(conn) -> str:
    _ensure_active_catalog(conn)
    return f"{ACTIVE_TABLE_CATALOG}.public.league_settings"


def league_db_filter(db_name: str, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _load_settings_from_db(db_name: str, conn) -> dict[int, dict]:
    """Read per-year playoff config from league_settings.

    Returns a dict keyed by year, each value is a dict with:
        num_playoff_teams, num_bye_teams, total_regular_weeks
    """
    table_sql = league_settings_table(conn)
    try:
        rows = conn.execute(
            f"SELECT year, playoff_start_week, playoff_teams, bye_teams "
            f"FROM {table_sql} "
            f"WHERE {league_db_filter(db_name)} AND year IS NOT NULL"
        ).fetchall()
    except Exception as e:
        print(f"[WARN] playoff_scenarios_standalone: Could not read league_settings: {e}")
        return {}

    result: dict[int, dict] = {}
    for row in rows:
        year, playoff_start_week, playoff_teams, bye_teams = row

        # playoff_start_week - 1 = last regular season week
        total_regular_weeks = int(playoff_start_week) - 1 if playoff_start_week is not None else None

        num_playoff_teams = int(playoff_teams) if playoff_teams is not None else None

        # Derive bye count if missing: next_power_of_2(N) - N for standard brackets
        if bye_teams is not None:
            num_bye_teams = int(bye_teams)
        elif num_playoff_teams is not None:
            next_p2 = 1 << (num_playoff_teams - 1).bit_length()
            num_bye_teams = next_p2 - num_playoff_teams
        else:
            num_bye_teams = None

        result[int(year)] = {
            "num_playoff_teams": num_playoff_teams,
            "num_bye_teams": num_bye_teams,
            "total_regular_weeks": total_regular_weeks,
        }

    return result


def _settings_for_year(year: int, settings_by_year: dict[int, dict]) -> tuple[int, int, int]:
    """Return (num_playoff_teams, num_bye_teams, total_regular_weeks) for a year.

    Falls back gracefully:
    - num_playoff_teams: 6
    - num_bye_teams: 2
    - total_regular_weeks: 14 pre-2021, 15 from 2021 onward
    """
    cfg = settings_by_year.get(int(year), {})

    num_playoff_teams = cfg.get("num_playoff_teams") or 6

    if cfg.get("num_bye_teams") is not None:
        num_bye_teams = cfg["num_bye_teams"]
    else:
        # Derive from playoff_teams if possible
        next_p2 = 1 << (num_playoff_teams - 1).bit_length()
        num_bye_teams = next_p2 - num_playoff_teams

    total_regular_weeks = cfg.get("total_regular_weeks") or (15 if int(year) >= 2021 else 14)

    return int(num_playoff_teams), int(num_bye_teams), int(total_regular_weeks)


# ---------------------------------------------------------------------------
# Read matchup (thin)
# ---------------------------------------------------------------------------


def _read_matchup(db_name: str, conn) -> pd.DataFrame:
    """Read the thin matchup subset we need from MotherDuck."""
    # Discover which of our desired columns actually exist
    matchup_sql = matchup_table(conn)
    existing = {
        row[0]
        for row in conn.execute(
            f"SELECT column_name FROM duckdb_columns() "
            f"WHERE database_name = '{ACTIVE_TABLE_CATALOG}' AND table_name = 'matchup'"
        ).fetchall()
    }

    cols_to_read = [c for c in MATCHUP_COLUMNS if c in existing]
    col_list = ", ".join(f'"{c}"' for c in cols_to_read)

    df = conn.execute(f"SELECT {col_list} FROM {matchup_sql} WHERE {league_db_filter(db_name)}").df()

    # Cast booleans to int so the module logic works consistently
    for bool_col in ("is_playoffs", "is_consolation"):
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].fillna(False).astype(int)

    print(f"  Read {len(df):,} rows from {matchup_sql} " f"({len(cols_to_read)} columns)")
    return df


# ---------------------------------------------------------------------------
# Write back new columns
# ---------------------------------------------------------------------------


def _write_back(
    db_name: str,
    conn,
    result_df: pd.DataFrame,
    new_cols: list[str],
    dry_run: bool = False,
) -> int:
    """Write new columns from result_df back to matchup via temp table + UPDATE.

    Uses a temp table join keyed on (franchise_id, year, week) — the
    natural dedup key for a matchup row from the manager's perspective.
    franchise_id is required; rows without it will not be updated.

    Returns count of rows updated.
    """
    if not new_cols:
        print("  [WARN] No new columns to write back.")
        return 0

    join_key_cols = ["franchise_id", "year", "week"]

    print(f"  Writing back {len(new_cols)} columns via join on " f"({', '.join(join_key_cols)})")

    if dry_run:
        print(f"  [DRY RUN] Would ALTER + UPDATE columns: {new_cols}")
        sample = result_df[join_key_cols + new_cols].head(5)
        print(f"  [DRY RUN] Sample output:\n{sample.to_string(index=False)}")
        return 0

    # Build the staging DataFrame (join keys + new columns only, deduplicated)
    staging_cols = list(dict.fromkeys(join_key_cols + new_cols))
    staging_df = result_df[[c for c in staging_cols if c in result_df.columns]].copy()

    # Ensure correct types to avoid DuckDB binding surprises
    for col in new_cols:
        if col not in staging_df.columns:
            continue
        if COLUMN_TYPES.get(col, "DOUBLE") == "INTEGER":
            staging_df[col] = pd.to_numeric(staging_df[col], errors="coerce").fillna(0).astype(int)
        else:
            staging_df[col] = pd.to_numeric(staging_df[col], errors="coerce")

    # Register temp table in DuckDB in-process memory
    conn.execute("DROP TABLE IF EXISTS __scenario_staging")
    conn.register("__scenario_staging_view", staging_df)
    conn.execute("CREATE TEMP TABLE __scenario_staging AS SELECT * FROM __scenario_staging_view")
    conn.unregister("__scenario_staging_view")

    matchup_sql = matchup_table(conn)

    # Step 1: ALTER TABLE — add missing columns
    for col in new_cols:
        col_type = COLUMN_TYPES.get(col, "DOUBLE")
        conn.execute(f"ALTER TABLE {matchup_sql} " f'ADD COLUMN IF NOT EXISTS "{col}" {col_type}')

    # Step 2: UPDATE via join with staging temp table.
    # DuckDB 1.2 does not support table aliases in UPDATE (UPDATE tbl alias SET ...),
    # so we use the unaliased UPDATE ... SET col = (subquery) form instead.
    for col in new_cols:
        if col not in staging_df.columns:
            continue
        where_clauses = " AND ".join(f'{matchup_sql}."{jk}" = __scenario_staging."{jk}"' for jk in join_key_cols)
        update_sql = f"""
            UPDATE {matchup_sql}
            SET "{col}" = __scenario_staging."{col}"
            FROM __scenario_staging
            WHERE {where_clauses}
              AND {league_db_filter(db_name)}
        """
        execute_scoped(conn, update_sql, db_name, label="playoff_scenarios:update")

    # Count updated rows (proxy: rows where any key new column is non-null)
    check_col = new_cols[0]
    updated = conn.execute(
        f'SELECT COUNT(*) FROM {matchup_sql} WHERE {league_db_filter(db_name)} AND "{check_col}" IS NOT NULL'
    ).fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS __scenario_staging")

    print(f"  [OK] {updated:,} rows have {check_col} populated after write-back")
    return updated


# ---------------------------------------------------------------------------
# Core entry point
# ---------------------------------------------------------------------------


def run_playoff_scenarios(db_name: str, conn, dry_run: bool = False) -> int:
    """Read matchup from MotherDuck, compute scenario columns, write back.

    Args:
        db_name:  MotherDuck database name.
        conn:     Open DuckDB connection.
        dry_run:  If True, compute and preview without writing.

    Returns:
        Row count updated (0 for dry-run).
    """
    # 1. Load per-year settings
    print("[playoff_scenarios_standalone] Loading league settings...")
    settings_by_year = _load_settings_from_db(db_name, conn)
    if settings_by_year:
        years = sorted(settings_by_year.keys())
        print(f"  Found settings for {len(years)} year(s): {years[0]}–{years[-1]}")
    else:
        print("  [WARN] No settings found — using defaults for all years")

    # 2. Read matchup (thin)
    print("[playoff_scenarios_standalone] Reading matchup data...")
    df = _read_matchup(db_name, conn)

    if df.empty:
        print("[WARN] matchup table is empty — nothing to do.")
        return 0

    original_cols = set(df.columns)

    # 3. For each year, determine settings overrides and run the module.
    #    add_playoff_scenario_columns operates on the full df at once but
    #    internally reads _load_year_settings per (year,week) group.
    #    Since data_directory=None, it will use its own defaults.
    #    We override by calling with the first year's settings as the
    #    fallback — the module's internal loop will call _load_year_settings
    #    per year anyway, but since data_directory=None it always falls back
    #    to the function-level params.
    #
    #    To honour per-year settings from MotherDuck, we patch the module's
    #    internal cache by pre-computing params from our settings dict and
    #    passing the most common values as defaults.

    # Determine most common playoff config across years (used as function-level default)
    if settings_by_year:
        all_playoff_teams = [
            v["num_playoff_teams"] for v in settings_by_year.values() if v.get("num_playoff_teams") is not None
        ]
        all_bye_teams = [v["num_bye_teams"] for v in settings_by_year.values() if v.get("num_bye_teams") is not None]
        all_reg_weeks = [
            v["total_regular_weeks"] for v in settings_by_year.values() if v.get("total_regular_weeks") is not None
        ]
        # Use modal value as the function-level default
        modal_playoff_teams = max(set(all_playoff_teams), key=all_playoff_teams.count) if all_playoff_teams else 6
        modal_bye_teams = max(set(all_bye_teams), key=all_bye_teams.count) if all_bye_teams else 2
        modal_reg_weeks = max(set(all_reg_weeks), key=all_reg_weeks.count) if all_reg_weeks else 14
    else:
        modal_playoff_teams = 6
        modal_bye_teams = 2
        modal_reg_weeks = 14

    print(
        f"[playoff_scenarios_standalone] Modal settings: "
        f"playoff_teams={modal_playoff_teams}, bye_teams={modal_bye_teams}, "
        f"regular_weeks={modal_reg_weeks}"
    )

    # For per-year accuracy we process each year separately, merge back
    year_dfs = []
    for year, year_group in df.groupby("year", sort=True):
        n_teams, n_bye, reg_weeks = _settings_for_year(year, settings_by_year)
        print(f"  Year {year}: playoff_teams={n_teams}, bye_teams={n_bye}, " f"regular_weeks={reg_weeks}")
        enriched = add_playoff_scenario_columns(
            year_group.copy(),
            num_playoff_teams=n_teams,
            num_bye_teams=n_bye,
            total_regular_weeks=reg_weeks,
            data_directory=None,
        )
        year_dfs.append(enriched)

    result_df = pd.concat(year_dfs, ignore_index=True)

    # 4. Identify new columns
    new_cols = [c for c in SCENARIO_COLUMNS if c in result_df.columns and c not in original_cols]
    print(f"[playoff_scenarios_standalone] New columns computed: {new_cols}")

    # 5. Write back (or dry-run preview)
    updated = _write_back(db_name, conn, result_df, new_cols, dry_run=dry_run)

    return updated


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
    db_name = ctx.get("league_name") or ctx.get("db_name")
    if not db_name:
        raise SystemExit("No league_name found in context JSON")
    return sanitize_database_name(db_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Compute playoff scenario columns and write back to matchup in MotherDuck"
    )
    parser.add_argument("--db", type=str, help="MotherDuck database name")
    parser.add_argument("--context", type=str, help="Path to league_context.json (legacy fallback)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute columns and preview without writing to MotherDuck",
    )
    args = parser.parse_args()

    db_name = _resolve_db_name(args)
    print(f"[playoff_scenarios_standalone] Database: {db_name}")

    if args.dry_run:
        print("[playoff_scenarios_standalone] Dry-run mode — no changes will be written")

    data_dir = None
    if args.context:
        with open(args.context) as f:
            ctx = json.load(f)
        data_dir = ctx.get("data_directory")

    conn = get_pipeline_connection(db_name, data_dir=data_dir, qualified=True)
    try:
        updated = run_playoff_scenarios(db_name, conn, dry_run=args.dry_run)
    finally:
        conn.close()

    if not args.dry_run:
        print(f"\n[playoff_scenarios_standalone] Done. " f"{updated:,} rows updated in matchup for db_name={db_name}")
    else:
        print("\n[playoff_scenarios_standalone] Dry run complete — no data written.")


if __name__ == "__main__":
    main()
