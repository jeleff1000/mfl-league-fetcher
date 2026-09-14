"""
fill_def_scores_from_pfr.py

Fills all NULL pts_def_team_pts / points_allowed in the v26 super table parquet
by joining against nfl_team_games_all.parquet (PFR) on (year, week, nfl_team, opponent).

Every NULL DEF score row in v26 has a corresponding PFR record — this is a pure
pipeline join failure (rows with player_week keys lacking an embedded game_id were
skipped during score population). This script closes that gap for all 1920–present rows.

Join strategy:
  1. Exact:    year + week + nfl_team + opponent_nfl_team  (handles doubleheaders)
  2. Fallback: year + week + nfl_team only, if PFR returns exactly 1 row
               (for the few cases where team codes differ slightly)

Usage:
    python -m scripts.fill_def_scores_from_pfr [--dry-run]

Outputs:
    Patches the v26 parquet in place (reads all, updates nulls, writes back).
    Prints a summary of fills applied and any unfillable rows.
"""

from __future__ import annotations
import argparse
import glob
import os
from pathlib import Path

import duckdb

V26_GLOB = r"D:\league-history-data\nfl\releases\*_v26\tables\nfl_player_stats_all.parquet"
PFR_TG   = r"D:\league-history-data\nfl\raw\pfr\boxscores\nfl_team_games_all.parquet"


def _latest_v26() -> str:
    files = sorted(glob.glob(V26_GLOB), key=lambda p: Path(p).stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No v26 parquet found at {V26_GLOB}")
    return files[0]


def build_fill_map(conn: duckdb.DuckDBPyConnection, v26: str, pfr: str) -> dict[str, tuple[float, float]]:
    """
    Returns {player_week: (pts_def_team_pts, points_allowed)} for every NULL DEF
    row that can be filled from PFR.
    """
    # Step 1: exact match on year + week + nfl_team + opponent
    exact = conn.execute(f"""
        WITH nulls AS (
            SELECT player_week,
                   year,
                   CAST(week AS INTEGER) AS week,
                   nfl_team,
                   opponent_nfl_team
            FROM '{v26}'
            WHERE position = 'DEF'
              AND pts_def_team_pts IS NULL
              AND points_allowed   IS NULL
        )
        SELECT n.player_week,
               p.team_points     AS pts,
               p.opponent_points AS pa
        FROM nulls n
        JOIN '{pfr}' p
          ON  n.year             = p.year
          AND n.week             = p.week
          AND n.nfl_team         = p.team_code
          AND n.opponent_nfl_team = p.opponent_code
    """).fetchall()

    fill_map: dict[str, tuple[float, float]] = {}
    for pw, pts, pa in exact:
        fill_map[pw] = (float(pts), float(pa))

    # Step 2: fallback for rows not resolved yet — unique team match only
    resolved = set(fill_map)
    remaining_pws = conn.execute(f"""
        SELECT player_week, year, CAST(week AS INTEGER) AS week, nfl_team, opponent_nfl_team
        FROM '{v26}'
        WHERE position = 'DEF'
          AND pts_def_team_pts IS NULL
          AND points_allowed   IS NULL
    """).fetchall()

    for pw, year, week, team, opp in remaining_pws:
        if pw in resolved:
            continue
        pfr_rows = conn.execute(f"""
            SELECT team_points, opponent_points, opponent_code
            FROM '{pfr}'
            WHERE year = {year} AND week = {week} AND team_code = '{team}'
        """).fetchall()
        if len(pfr_rows) == 1:
            fill_map[pw] = (float(pfr_rows[0][0]), float(pfr_rows[0][1]))
        elif len(pfr_rows) > 1:
            # Multiple games same week — no safe fallback without opponent match
            print(f"  AMBIGUOUS (no opp match): {pw}  {year}/w{week} {team} vs {opp}  "
                  f"pfr options: {pfr_rows}")

    return fill_map


def apply_fills(conn: duckdb.DuckDBPyConnection, v26: str, fill_map: dict,
                dry_run: bool) -> None:
    import os

    if not fill_map:
        print("No fills to apply.")
        return

    # Build a VALUES table and UPDATE via DuckDB
    values = ", ".join(
        f"('{pw}', {pts}, {pa})"
        for pw, (pts, pa) in fill_map.items()
    )

    if dry_run:
        print(f"\n[DRY RUN] Would fill {len(fill_map)} rows.")
        sample = list(fill_map.items())[:10]
        for pw, (pts, pa) in sample:
            print(f"  {pw:<50} {pts:.0f} - {pa:.0f}")
        if len(fill_map) > 10:
            print(f"  ... and {len(fill_map) - 10} more")
        return

    # Use a fresh connection so nothing holds the source file open after read
    v26_path = Path(v26)
    tmp_path = v26_path.with_name(v26_path.stem + "_patched.parquet")

    print(f"\nReading v26 parquet ...")
    fresh = duckdb.connect()
    fresh.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")

    fresh.execute(f"""
        CREATE TEMP TABLE fills (player_week VARCHAR, pts DOUBLE, pa DOUBLE)
    """)
    fresh.execute(f"INSERT INTO fills VALUES {values}")

    fresh.execute("""
        UPDATE st
        SET pts_def_team_pts = f.pts,
            points_allowed   = f.pa
        FROM fills f
        WHERE st.player_week = f.player_week
          AND st.pts_def_team_pts IS NULL
          AND st.points_allowed   IS NULL
    """)

    remaining_null = fresh.execute("""
        SELECT COUNT(*) FROM st
        WHERE position = 'DEF' AND pts_def_team_pts IS NULL AND points_allowed IS NULL
    """).fetchone()[0]
    print(f"NULL DEF rows after update (in memory): {remaining_null}")

    print(f"Writing to temp file: {tmp_path} ...")
    fresh.execute(f"COPY st TO '{tmp_path}' (FORMAT PARQUET)")
    fresh.close()

    # Now the original file is released — atomically replace it
    print(f"Replacing original parquet ...")
    os.replace(tmp_path, v26_path)
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill NULL DEF scores in v26 from PFR")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be filled without writing")
    args = parser.parse_args()

    conn = duckdb.connect()
    v26  = _latest_v26()
    pfr  = PFR_TG

    print(f"v26 : {v26}")
    print(f"PFR : {pfr}")
    print()

    # Count NULLs before
    total_null = conn.execute(f"""
        SELECT COUNT(*) FROM '{v26}'
        WHERE position = 'DEF'
          AND pts_def_team_pts IS NULL
          AND points_allowed   IS NULL
    """).fetchone()[0]
    print(f"NULL DEF score rows before: {total_null}")

    print("Building fill map ...")
    fill_map = build_fill_map(conn, v26, pfr)

    unfillable = total_null - len(fill_map)
    print(f"Fill map built: {len(fill_map)} rows fillable, {unfillable} remain unfillable")

    apply_fills(conn, v26, fill_map, dry_run=args.dry_run)

    if not args.dry_run:
        # Verify
        remaining = conn.execute(f"""
            SELECT COUNT(*) FROM '{v26}'
            WHERE position = 'DEF'
              AND pts_def_team_pts IS NULL
              AND points_allowed   IS NULL
        """).fetchone()[0]
        print(f"\nNULL DEF score rows after: {remaining}")
        print(f"Filled: {total_null - remaining}")


if __name__ == "__main__":
    main()
