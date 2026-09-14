"""Build v24: drop HIST- shadow rows where a canonical PFR row exists
for the same (year, week, nfl_team, player).

949 player-game pairs across 29 players have dual HIST-/PFR identity in v23
(e.g. Y.A. Tittle as both HIST-56100215 and TittY.00 in the same game).
The HIST- row is a legacy artifact from before the PFR identity was resolved.
Dropping it removes the double-count that causes the 2:1 pass:recv ratio in
those team-game aggregates.

Input:  latest v23 parquet
Output: D:/league-history-data/nfl/releases/<stamp>_v24/tables/nfl_player_stats_all.parquet
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

RELEASES = Path(r"D:\league-history-data\nfl\releases")

V23_DIR = RELEASES / "nfl_local_release_pfa_ancient_upsert_20260616T225210Z_v23"
V23_PARQUET = V23_DIR / "tables" / "nfl_player_stats_all.parquet"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main(dry_run: bool = True) -> None:
    if not V23_PARQUET.exists():
        print(f"ERROR: v23 not found at {V23_PARQUET}", file=sys.stderr)
        sys.exit(1)

    db = duckdb.connect()
    db.execute("SET progress_bar_time=99999")

    print("Loading v23...")
    db.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{V23_PARQUET}')")
    total_v23 = db.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    print(f"  v23 rows: {total_v23:,}")

    # Identify HIST- rows that have a canonical (non-HIST-, non-SYN-) partner
    # for the same (year, week, nfl_team, player).
    print("\nIdentifying HIST- shadow rows...")
    shadow_rows = db.execute("""
        WITH canonical AS (
            SELECT DISTINCT year, week, nfl_team, player
            FROM st
            WHERE position != 'DEF'
              AND NFL_player_id NOT LIKE 'HIST-%'
              AND NFL_player_id NOT LIKE 'SYN-%'
              AND player IS NOT NULL
        )
        SELECT s.player_week
        FROM st s
        JOIN canonical c
          ON s.year = c.year AND s.week = c.week
         AND s.nfl_team = c.nfl_team AND s.player = c.player
        WHERE s.NFL_player_id LIKE 'HIST-%'
          AND s.position != 'DEF'
    """).fetchall()
    shadow_player_weeks = {r[0] for r in shadow_rows}
    print(f"  Shadow HIST- rows to drop: {len(shadow_player_weeks):,}")

    if len(shadow_player_weeks) == 0:
        print("Nothing to do — no dual-identity rows found.")
        return

    # Show affected players before dropping
    affected = db.execute("""
        WITH canonical AS (
            SELECT DISTINCT year, week, nfl_team, player
            FROM st
            WHERE position != 'DEF'
              AND NFL_player_id NOT LIKE 'HIST-%'
              AND NFL_player_id NOT LIKE 'SYN-%'
              AND player IS NOT NULL
        )
        SELECT s.player, COUNT(*) as shadow_games,
               MIN(s.year) as first_yr, MAX(s.year) as last_yr,
               STRING_AGG(DISTINCT s.NFL_player_id, ', ') as hist_ids
        FROM st s
        JOIN canonical c
          ON s.year = c.year AND s.week = c.week
         AND s.nfl_team = c.nfl_team AND s.player = c.player
        WHERE s.NFL_player_id LIKE 'HIST-%'
          AND s.position != 'DEF'
        GROUP BY s.player
        ORDER BY shadow_games DESC
    """).fetchall()
    print(f"\n  Affected players ({len(affected)}):")
    for row in affected:
        print(f"    {row[1]:3d} games  {row[0]}  ({int(row[2])}-{int(row[3])})  [{row[4]}]")

    if dry_run:
        print("\nDRY RUN — no files written. Pass --execute to apply.")
        return

    # Build v24: exclude shadow rows
    stamp = utc_stamp()
    out_dir = RELEASES / f"nfl_local_release_hist_dedup_{stamp}_v24"
    out_dir.mkdir(parents=True)
    (out_dir / "tables").mkdir()
    out_parquet = out_dir / "tables" / "nfl_player_stats_all.parquet"

    print(f"\nWriting v24 to {out_parquet} ...")
    db.execute(f"""
        COPY (
            SELECT * FROM st
            WHERE player_week NOT IN (
                WITH canonical AS (
                    SELECT DISTINCT year, week, nfl_team, player
                    FROM st
                    WHERE position != 'DEF'
                      AND NFL_player_id NOT LIKE 'HIST-%'
                      AND NFL_player_id NOT LIKE 'SYN-%'
                      AND player IS NOT NULL
                )
                SELECT s.player_week
                FROM st s
                JOIN canonical c
                  ON s.year = c.year AND s.week = c.week
                 AND s.nfl_team = c.nfl_team AND s.player = c.player
                WHERE s.NFL_player_id LIKE 'HIST-%'
                  AND s.position != 'DEF'
            )
        ) TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    total_v24 = db.execute(f"SELECT COUNT(*) FROM read_parquet('{out_parquet}')").fetchone()[0]
    dropped = total_v23 - total_v24
    print(f"\nv24 rows: {total_v24:,}  (dropped {dropped:,} HIST- shadow rows)")
    assert dropped == len(shadow_player_weeks), f"Row count mismatch: expected {len(shadow_player_weeks)} drops, got {dropped}"

    # Quick sanity: check 2:1 ratio team-games in v24
    ratio_games = db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT year, week, nfl_team
            FROM read_parquet('{out_parquet}')
            WHERE position != 'DEF'
            GROUP BY year, week, nfl_team
            HAVING SUM(passing_yards) > 0 AND SUM(receiving_yards) > 0
               AND ABS(SUM(passing_yards) / NULLIF(SUM(receiving_yards), 0) - 2.0) < 0.01
        )
    """).fetchone()[0]
    print(f"\n2:1 pass:recv ratio team-games in v24: {ratio_games}")
    if ratio_games == 0:
        print("  PASS — no double-count ratio games remaining.")
    else:
        print(f"  WARNING — {ratio_games} team-games still have 2:1 ratio.")

    print(f"\nv24 written: {out_dir}")


if __name__ == "__main__":
    dry = "--execute" not in sys.argv
    main(dry_run=dry)
