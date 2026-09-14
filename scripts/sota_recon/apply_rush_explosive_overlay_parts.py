"""Apply only the 20+ explosive-rush overlay to canonical weekly parts."""
from __future__ import annotations
import os
from pathlib import Path
import duckdb

PARTS = Path(r"D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables/weekly_repaired_parts")
OVERLAY = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/weekly_overlay_rush_explosive_20.parquet")
COLUMN = "rush_explosive_10"

def main() -> None:
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    years = [int(r[0]) for r in con.execute(
        f"SELECT DISTINCT CAST(year AS INTEGER) FROM read_parquet('{OVERLAY.as_posix()}')"
    ).fetchall()]
    for p in PARTS.glob('*.rush_explosive.tmp.parquet'):
        p.unlink(missing_ok=True)
    for year in sorted(years):
        part = PARTS / f"year={year}.parquet"
        tmp = PARTS / f"year={year}.rush_explosive.tmp.parquet"
        con.execute(f"""
          COPY (SELECT t.* REPLACE (COALESCE(o.new_value, t.{COLUMN}) AS {COLUMN})
                FROM read_parquet('{part.as_posix()}') t
                LEFT JOIN read_parquet('{OVERLAY.as_posix()}') o
                  ON o.NFL_player_id=t.NFL_player_id
                 AND CAST(o.year AS INTEGER)=CAST(t.year AS INTEGER)
                 AND CAST(o.week AS INTEGER)=CAST(t.week AS INTEGER)
                 AND o.column_name='{COLUMN}'
                 AND CAST(o.year AS INTEGER)={year})
          TO '{tmp.as_posix()}' (FORMAT parquet)
        """)
        os.replace(tmp, part)
    print({'parts': len(years), 'overlay': str(OVERLAY)})

if __name__ == '__main__':
    main()
