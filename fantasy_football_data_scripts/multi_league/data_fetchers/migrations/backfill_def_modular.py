#!/usr/bin/env python3
"""
Backfill modular DEF component columns in the super_table.

Adds 10 new pts_def_* columns that store raw event counts (x1) so SQL can
multiply by any league's point values. This replaces the corrections framework
for DEF scoring.

New columns:
  pts_def_sack, pts_def_int, pts_def_ff, pts_def_fr, pts_def_td,
  pts_def_safety, pts_def_block, pts_def_tfl, pts_def_3out, pts_def_4stop

Usage:
    python -m multi_league.data_fetchers.migrations.backfill_def_modular
    python -m multi_league.data_fetchers.migrations.backfill_def_modular --dry-run
"""

import argparse
import os

import duckdb


def log(msg: str):
    print(msg, flush=True)


COLUMNS = {
    "pts_def_sack": "COALESCE(TRY_CAST(def_sacks AS DOUBLE), 0)",
    "pts_def_int": "COALESCE(TRY_CAST(def_interceptions AS DOUBLE), 0)",
    "pts_def_ff": "COALESCE(TRY_CAST(def_fumbles_forced AS DOUBLE), 0)",
    "pts_def_fr": "COALESCE(TRY_CAST(fum_rec AS DOUBLE), 0)",
    "pts_def_td": "COALESCE(TRY_CAST(def_tds AS DOUBLE), 0) + COALESCE(TRY_CAST(fum_ret_td AS DOUBLE), 0)",
    "pts_def_safety": "COALESCE(TRY_CAST(def_safeties AS DOUBLE), 0)",
    "pts_def_block": "COALESCE(TRY_CAST(fg_blocked AS DOUBLE), 0)",
    "pts_def_tfl": "COALESCE(TRY_CAST(def_tackles_for_loss AS DOUBLE), 0)",
    "pts_def_3out": "COALESCE(TRY_CAST(three_out AS DOUBLE), 0)",
    "pts_def_4stop": "COALESCE(TRY_CAST(fourth_down_stop AS DOUBLE), 0)",
}


def backfill_def_modular(dry_run: bool = False):
    """Backfill modular DEF component columns in super_table."""
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        log("Fly.io backend: DEF modular backfill handled during super table rebuild")
        return

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError("MOTHERDUCK_TOKEN environment variable not set")

    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # Check how many DEF rows exist
        def_count = conn.execute(
            "SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all WHERE nfl_position = 'DEF'"
        ).fetchone()[0]
        log(f"DEF rows in super_table: {def_count:,}")

        # Add columns if they don't exist
        existing_cols = conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog = '___ops'
            AND table_schema = 'nfl_historical'
            AND table_name = 'nfl_player_stats_all'
        """).fetchall()
        existing_cols = {c[0] for c in existing_cols}

        new_cols = []
        for col in COLUMNS:
            if col not in existing_cols:
                new_cols.append(col)
                if not dry_run:
                    conn.execute(f"ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN {col} DOUBLE")
                    log(f"  Added column {col}")

        if new_cols:
            log(f"Added {len(new_cols)} new columns: {', '.join(new_cols)}")
        else:
            log("All columns already exist")

        # Build SET clause
        set_parts = [f"{col} = {expr}" for col, expr in COLUMNS.items()]
        set_clause = ",\n    ".join(set_parts)

        update_sql = f"""
            UPDATE nfl_historical.nfl_player_stats_all
            SET {set_clause}
            WHERE nfl_position = 'DEF'
        """

        if dry_run:
            log(f"\nDry run - would execute:\n{update_sql}")

            # Preview a sample
            sample = conn.execute("""
                SELECT player_week, def_sacks, def_interceptions, def_fumbles_forced,
                       fum_rec, def_tds, fum_ret_td, def_safeties, fg_blocked,
                       def_tackles_for_loss, three_out, fourth_down_stop
                FROM nfl_historical.nfl_player_stats_all
                WHERE nfl_position = 'DEF' AND year >= 2020
                LIMIT 5
            """).fetchdf()
            log(f"\nSample DEF rows (2020+):\n{sample.to_string()}")
        else:
            result = conn.execute(update_sql).fetchone()
            rows_updated = result[0] if result else 0
            log(f"\nUpdated {rows_updated:,} DEF rows with modular component columns")

            # Verify: spot-check a few rows
            verify = conn.execute("""
                SELECT player_week,
                       pts_def_sack, pts_def_int, pts_def_ff, pts_def_fr, pts_def_td,
                       pts_def_safety, pts_def_block, pts_def_tfl, pts_def_3out, pts_def_4stop,
                       pts_def_std
                FROM nfl_historical.nfl_player_stats_all
                WHERE nfl_position = 'DEF' AND year = 2024
                LIMIT 5
            """).fetchdf()
            log(f"\nVerification sample (2024):\n{verify.to_string()}")

            # Verify formula: pts_def_std should ≈ sack*1 + int*2 + ff*1 + fr*2 + td*6 + safety*2 + block*2 + PA tiers
            mismatch = conn.execute("""
                SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all
                WHERE nfl_position = 'DEF'
                AND pts_def_sack IS NOT NULL
                AND ABS(
                    COALESCE(pts_def_std, 0) - (
                        pts_def_sack * 1 + pts_def_int * 2 +
                        pts_def_fr * 2 + pts_def_td * 6 + pts_def_safety * 2 + pts_def_block * 2 +
                        COALESCE(pts_allow_0, 0) * 10 + COALESCE(pts_allow_1_6, 0) * 7 +
                        COALESCE(pts_allow_7_13, 0) * 4 + COALESCE(pts_allow_14_20, 0) * 1 +
                        COALESCE(pts_allow_21_27, 0) * 0 + COALESCE(pts_allow_28_34, 0) * -1 +
                        COALESCE(pts_allow_35_plus, 0) * -4
                    )
                ) > 0.1
            """).fetchone()[0]
            log(f"\nFormula mismatch count (pts_def_std vs components + PA): {mismatch}")

    finally:
        conn.close()

    log("\nDone.")


def main():
    parser = argparse.ArgumentParser(description="Backfill modular DEF columns in super_table")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")

    args = parser.parse_args()
    backfill_def_modular(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
