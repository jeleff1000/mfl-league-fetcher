"""Apply the rushing PBP overlay atomically to the partitioned weekly plane."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb

PARTS = Path(r"D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables/weekly_repaired_parts")
OVERLAY = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/weekly_overlay_rushing_pbp_1998.parquet")


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    years = [int(r[0]) for r in con.execute(
        f"SELECT DISTINCT CAST(year AS INTEGER) FROM read_parquet('{OVERLAY.as_posix()}')"
    ).fetchall()]
    for year in sorted(years):
        part = PARTS / f"year={year}.parquet"
        tmp = PARTS / f"year={year}.rushing_pbp.tmp.parquet"
        con.execute(f"""
          COPY (SELECT t.* REPLACE (
              CASE WHEN o.NFL_player_id IS NULL THEN t.rushing_first_downs ELSE o.rushing_first_downs END AS rushing_first_downs,
              CASE WHEN o.NFL_player_id IS NULL THEN t.rushing_fumbles ELSE o.rushing_fumbles END AS rushing_fumbles,
              CASE WHEN o.NFL_player_id IS NULL THEN t.rushing_fumbles_lost ELSE o.rushing_fumbles_lost END AS rushing_fumbles_lost,
              CASE WHEN o.NFL_player_id IS NULL THEN t.fumbles ELSE o.fumbles END AS fumbles,
              CASE WHEN o.NFL_player_id IS NULL THEN t.fumbles_lost ELSE o.fumbles_lost END AS fumbles_lost,
              CASE WHEN o.NFL_player_id IS NULL THEN t.receiving_fumbles ELSE o.receiving_fumbles END AS receiving_fumbles,
              CASE WHEN o.NFL_player_id IS NULL THEN t.receiving_fumbles_lost ELSE o.receiving_fumbles_lost END AS receiving_fumbles_lost,
              CASE WHEN o.NFL_player_id IS NULL THEN t.sack_fumbles ELSE o.sack_fumbles END AS sack_fumbles,
              CASE WHEN o.NFL_player_id IS NULL THEN t.sack_fumbles_lost ELSE o.sack_fumbles_lost END AS sack_fumbles_lost)
            FROM read_parquet('{part.as_posix()}') t
            LEFT JOIN read_parquet('{OVERLAY.as_posix()}') o
              ON o.NFL_player_id=t.NFL_player_id AND o.year=t.year AND o.week=t.week
             AND o.season_type=t.season_type AND o.year={year})
          TO '{tmp.as_posix()}' (FORMAT parquet)
        """)
        os.replace(tmp, part)
    print({"parts": len(years), "overlay": str(OVERLAY)})


if __name__ == "__main__":
    main()
