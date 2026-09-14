#!/usr/bin/env python3
"""
Backfill Fantasy Points Columns to Super Table

One-time script to add pts_* columns and calculate for ALL existing super_table data.

This adds 28 category columns:
- pts_pass_4pt, pts_pass_5pt, pts_pass_6pt (passing with 4/5/6pt TDs, -2 INT)
- pts_pass_4pt_int1, pts_pass_5pt_int1, pts_pass_6pt_int1 (passing with -1 INT)
- pts_rush (rushing)
- pts_rec_0ppr, pts_rec_half, pts_rec_ppr, pts_rec_tep (receiving with PPR variants)
- pts_ret_yds (return yards, 1 pt per 25 yds)
- pts_misc (2pt conversions + return TDs)
- pts_k_std, pts_k_yds, pts_k_flat (kicker variants)
- pts_def_std, pts_def_ya (defense variants; pts_def_high removed 2026-04-30 — KMFFL-specific)
- pts_idp_std, pts_idp_premium, pts_idp_tackle_heavy, pts_idp_big_play (IDP variants)
- pts_pass_cmp, pts_rush_att, pts_first_downs, pts_sack_taken (optional categories)

Usage:
    python backfill_fantasy_points.py
    python backfill_fantasy_points.py --year 2024  # Single year only
    python backfill_fantasy_points.py --dry-run    # Preview without writing
"""

import argparse
import os
import sys
from datetime import datetime

import duckdb
import pandas as pd

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

from multi_league.data_fetchers.fantasy_points_calculator import calculate_all_fantasy_points, PRECALC_COLUMNS


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [BACKFILL] {msg}")


def get_connection():
    """Get ___ops database connection (MotherDuck or local for Fly.io)."""
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        # For Fly.io, this script performs heavy writes to super_table.
        # Fall through to MotherDuck which remains the write target.
        pass

    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN not set")
    return duckdb.connect(f"md:___ops?motherduck_token={token}")


def add_columns_if_missing(conn):
    """Add pts_* columns to super_table if they don't exist."""
    log("Checking for missing columns...")

    # Get existing columns
    existing = (
        conn.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'nfl_historical' AND table_name = 'nfl_player_stats_all'
    """)
        .fetchdf()["column_name"]
        .tolist()
    )

    existing_lower = {c.lower() for c in existing}

    # Add missing columns
    added = []
    for col in PRECALC_COLUMNS:
        if col.lower() not in existing_lower:
            log(f"  Adding column: {col}")
            conn.execute(f"""
                ALTER TABLE nfl_historical.nfl_player_stats_all
                ADD COLUMN IF NOT EXISTS {col} DOUBLE
            """)
            added.append(col)

    if added:
        log(f"Added {len(added)} new columns: {', '.join(added)}")
    else:
        log("All pts_* columns already exist")


def get_year_range(conn) -> list:
    """Get all years in super_table."""
    result = conn.execute("""
        SELECT DISTINCT year
        FROM nfl_historical.nfl_player_stats_all
        WHERE year IS NOT NULL
        ORDER BY year
    """).fetchdf()
    return result["year"].tolist()


def backfill_year(conn, year: int, dry_run: bool = False) -> int:
    """
    Backfill fantasy points for one year.

    Args:
        conn: DuckDB connection
        year: Year to backfill
        dry_run: If True, don't actually write

    Returns:
        Number of rows updated
    """
    log(f"Processing year {year}...")

    # Fetch all data for this year
    df = conn.execute(f"""
        SELECT *
        FROM nfl_historical.nfl_player_stats_all
        WHERE year = {year}
    """).fetchdf()

    if df.empty:
        log(f"  No data for {year}")
        return 0

    log(f"  Fetched {len(df):,} rows")

    # Calculate fantasy points
    df = calculate_all_fantasy_points(df)

    # Verify calculations
    non_zero = {col: (df[col] != 0).sum() for col in PRECALC_COLUMNS}
    log(f"  Non-zero values: {sum(non_zero.values()):,} across all columns")

    # Sample check
    for col in ["pts_pass_4pt", "pts_rush", "pts_rec_half", "pts_k_std", "pts_def_std"]:
        log(f"    {col}: {non_zero.get(col, 0):,} non-zero")

    if dry_run:
        log(f"  [DRY RUN] Would update {len(df):,} rows")
        return len(df)

    # Register the updated DataFrame
    conn.register("updated_data", df)

    # Update each pts_* column
    # We do this as a batch update using a temp table approach
    log(f"  Updating {len(df):,} rows in MotherDuck...")

    # Create temp table with just the key + pts columns
    pts_cols = ", ".join(PRECALC_COLUMNS)
    conn.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE temp_pts AS
        SELECT player_week, {pts_cols}
        FROM updated_data
        WHERE player_week IS NOT NULL
    """)

    # Build the UPDATE statement
    set_clauses = ", ".join([f"{col} = temp_pts.{col}" for col in PRECALC_COLUMNS])

    conn.execute(f"""
        UPDATE nfl_historical.nfl_player_stats_all AS main
        SET {set_clauses}
        FROM temp_pts
        WHERE main.player_week = temp_pts.player_week
    """)

    # Cleanup
    conn.execute("DROP TABLE IF EXISTS temp_pts")
    conn.unregister("updated_data")

    log(f"  Updated {len(df):,} rows for {year}")
    return len(df)


def verify_backfill(conn, sample_size: int = 10):
    """Verify backfill by checking sample data."""
    log("Verifying backfill...")

    # Check for NULL values in pts columns
    null_counts = conn.execute(f"""
        SELECT
            COUNT(*) as total_rows,
            {', '.join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as {col}_null" for col in PRECALC_COLUMNS])}
        FROM nfl_historical.nfl_player_stats_all
    """).fetchdf()

    total = null_counts["total_rows"].iloc[0]
    log(f"Total rows: {total:,}")

    for col in PRECALC_COLUMNS:
        null_count = null_counts[f"{col}_null"].iloc[0]
        if null_count > 0:
            log(f"  WARNING: {col} has {null_count:,} NULL values ({null_count/total*100:.1f}%)")

    # Sample check - verify some known players
    log("\nSample verification:")

    # QBs with passing stats
    qb_sample = conn.execute("""
        SELECT player, year, week, passing_yards, passing_tds, pts_pass_4pt, pts_pass_6pt
        FROM nfl_historical.nfl_player_stats_all
        WHERE nfl_position = 'QB' AND passing_tds > 0
        ORDER BY passing_yards DESC
        LIMIT 5
    """).fetchdf()
    log("Top QB games (by passing yards):")
    for _, row in qb_sample.iterrows():
        log(
            f"  {row['player']} ({row['year']} W{row['week']}): {row['passing_yards']:.0f}yds, {row['passing_tds']:.0f}TD -> pts_pass_4pt={row['pts_pass_4pt']:.1f}, pts_pass_6pt={row['pts_pass_6pt']:.1f}"
        )

    # Kickers
    k_sample = conn.execute("""
        SELECT player, year, week, pat_made, fg_made_40_49, fg_made_50_59, pts_k_std
        FROM nfl_historical.nfl_player_stats_all
        WHERE nfl_position = 'K' AND pat_made > 0
        ORDER BY pts_k_std DESC
        LIMIT 5
    """).fetchdf()
    log("\nTop K games (by pts_k_std):")
    for _, row in k_sample.iterrows():
        log(
            f"  {row['player']} ({row['year']} W{row['week']}): PAT={row['pat_made']:.0f}, FG40-49={row['fg_made_40_49'] if pd.notna(row['fg_made_40_49']) else 0:.0f} -> pts_k_std={row['pts_k_std']:.1f}"
        )

    # Defense
    def_sample = conn.execute("""
        SELECT player, year, week, def_sacks, def_interceptions, pts_allow_0, pts_def_std
        FROM nfl_historical.nfl_player_stats_all
        WHERE nfl_position = 'DEF'
        ORDER BY pts_def_std DESC
        LIMIT 5
    """).fetchdf()
    log("\nTop DEF games (by pts_def_std):")
    for _, row in def_sample.iterrows():
        log(
            f"  {row['player']} ({row['year']} W{row['week']}): Sacks={row['def_sacks']:.0f}, INT={row['def_interceptions']:.0f}, Shutout={row['pts_allow_0']:.0f} -> pts_def_std={row['pts_def_std']:.1f}"
        )

    # Historical player (Walter Payton)
    walter = conn.execute("""
        SELECT player, year, week, rushing_yards, rushing_tds, pts_rush
        FROM nfl_historical.nfl_player_stats_all
        WHERE player LIKE '%Walter Payton%' OR player LIKE '%W.Payton%'
        ORDER BY rushing_yards DESC
        LIMIT 3
    """).fetchdf()
    if not walter.empty:
        log("\nWalter Payton sample games:")
        for _, row in walter.iterrows():
            log(
                f"  {row['player']} ({row['year']} W{row['week']}): {row['rushing_yards']:.0f}yds, {row['rushing_tds']:.0f}TD -> pts_rush={row['pts_rush']:.1f}"
            )
    else:
        log("\nWalter Payton: No data found in super_table")


def main():
    parser = argparse.ArgumentParser(description="Backfill fantasy points columns to super table")
    parser.add_argument("--year", type=int, help="Single year to backfill (default: all years)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--verify-only", action="store_true", help="Only run verification")

    args = parser.parse_args()

    log("=" * 60)
    log("Fantasy Points Backfill")
    log("=" * 60)

    conn = get_connection()
    log("Connected to MotherDuck")

    if args.verify_only:
        verify_backfill(conn)
        conn.close()
        return 0

    # Step 1: Add columns if missing
    add_columns_if_missing(conn)

    # Step 2: Get years to process
    if args.year:
        years = [args.year]
    else:
        years = get_year_range(conn)

    log(f"Years to process: {min(years)} to {max(years)} ({len(years)} years)")

    # Step 3: Process each year
    total_rows = 0
    for year in years:
        rows = backfill_year(conn, year, args.dry_run)
        total_rows += rows

    log("=" * 60)
    log(f"Backfill complete: {total_rows:,} total rows processed")

    if not args.dry_run:
        # Step 4: Verify
        verify_backfill(conn)

    conn.close()
    log("Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
