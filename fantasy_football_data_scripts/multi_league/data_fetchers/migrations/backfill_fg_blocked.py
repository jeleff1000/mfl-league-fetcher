#!/usr/bin/env python3
"""
Backfill fg_blocked for DEF rows in the super_table.

Root cause: defense_stats.py's self-join never included fg_blocked from the
opponent's offense side. The column exists in nflverse team stats but was
never pulled into the DEF rows.

Fix: defense_stats.py now includes fg_blocked in the self-join.
This script backfills historical data by re-processing each year's DEF data
and updating the super_table.

After updating fg_blocked, also recomputes pts_def_std to include the
blocked kick contribution (fg_blocked * 2).

Usage:
    python -m multi_league.data_fetchers.migrations.backfill_fg_blocked
    python -m multi_league.data_fetchers.migrations.backfill_fg_blocked --year 2024
    python -m multi_league.data_fetchers.migrations.backfill_fg_blocked --dry-run
"""

import argparse
import os

import duckdb

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

from multi_league.data_fetchers.defense_stats import process_one_year


def log(msg: str):
    print(msg, flush=True)


def backfill_fg_blocked(year: int = None, dry_run: bool = False):
    """Backfill fg_blocked for DEF rows in super_table."""
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("Fly.io backend: fg_blocked backfill handled during super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable not set")

    # Determine year range
    if year:
        years = [year]
    else:
        # NFLverse team stats available from 1999
        from defense_stats import get_current_nfl_season_year

        current_year = get_current_nfl_season_year()
        years = list(range(1999, current_year + 1))

    log(f"Backfilling fg_blocked for {len(years)} year(s): {years[0]}-{years[-1]}")

    total_updated = 0
    total_nonzero = 0

    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        for yr in years:
            try:
                # Fetch DEF data with fg_blocked now populated
                def_df = process_one_year(yr, use_cache=False)

                if def_df.empty:
                    log(f"  [{yr}] No data")
                    continue

                if "fg_blocked" not in def_df.columns:
                    log(f"  [{yr}] No fg_blocked column in output (old code?)")
                    continue

                # Build player_week → fg_blocked mapping
                # player_week format: DEF-{franchise_id}_{year}_{week}
                if "defense_franchise_id" not in def_df.columns:
                    log(f"  [{yr}] No defense_franchise_id column, skipping")
                    continue

                mapping = def_df[["defense_franchise_id", "year", "week", "fg_blocked"]].copy()
                mapping["fg_blocked"] = mapping["fg_blocked"].fillna(0).astype(int)
                mapping["year"] = mapping["year"].astype(int)
                mapping["week"] = mapping["week"].astype(int)
                mapping["player_week"] = mapping.apply(
                    lambda r: f"DEF-{int(r['defense_franchise_id'])}_{r['year']}_{r['week']}", axis=1
                )

                nonzero = mapping[mapping["fg_blocked"] > 0]
                total_blocked = int(mapping["fg_blocked"].sum())

                if dry_run:
                    log(f"  [{yr}] {len(mapping)} DEF rows, {len(nonzero)} with blocked FGs ({total_blocked} total)")
                    if len(nonzero) > 0:
                        for _, row in nonzero.head(5).iterrows():
                            log(f"    {row['player_week']}: fg_blocked={row['fg_blocked']}")
                    continue

                # Upload mapping as temp table
                update_df = mapping[["player_week", "fg_blocked"]].copy()
                conn.execute("CREATE OR REPLACE TEMP TABLE fg_blocked_map (player_week VARCHAR, fg_blocked INTEGER)")
                conn.execute("INSERT INTO fg_blocked_map SELECT * FROM update_df")

                # UPDATE super_table: set fg_blocked for DEF rows
                result = conn.execute("""
                    UPDATE nfl_historical.nfl_player_stats_all AS s
                    SET fg_blocked = m.fg_blocked
                    FROM fg_blocked_map m
                    WHERE s.player_week = m.player_week
                """).fetchone()

                rows_updated = result[0] if result else 0

                # Recompute pts_def_std for rows where fg_blocked > 0
                # pts_def_std formula includes fg_blocked * 2
                # Since fg_blocked was previously 0/NULL, just add the delta
                recompute = conn.execute(
                    """
                    UPDATE nfl_historical.nfl_player_stats_all
                    SET pts_def_std = COALESCE(pts_def_std, 0) + (COALESCE(fg_blocked, 0) * 2)
                    WHERE nfl_position = 'DEF'
                      AND year = ?
                      AND COALESCE(fg_blocked, 0) > 0
                """,
                    [yr],
                ).fetchone()

                pts_updated = recompute[0] if recompute else 0

                conn.execute("DROP TABLE IF EXISTS fg_blocked_map")

                total_updated += rows_updated
                total_nonzero += len(nonzero)
                log(
                    f"  [{yr}] Updated {rows_updated} rows, {len(nonzero)} with blocked FGs ({total_blocked} total), {pts_updated} pts_def_std recomputed"
                )

            except ValueError as e:
                if "404" in str(e):
                    log(f"  [{yr}] No nflverse data available")
                else:
                    log(f"  [{yr}] Error: {e}")
            except Exception as e:
                log(f"  [{yr}] Error: {e}")

    finally:
        conn.close()

    log(f"\nDone. Total: {total_updated} rows updated, {total_nonzero} with blocked FGs")
    return total_updated


def main():
    parser = argparse.ArgumentParser(description="Backfill fg_blocked for DEF rows in super_table")
    parser.add_argument("--year", type=int, help="Single year to backfill")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")

    args = parser.parse_args()
    backfill_fg_blocked(year=args.year, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
