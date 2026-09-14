#!/usr/bin/env python3
"""Build the NGS SEASON parquet from nflverse's published season-total rows (week==0),
keyed by (NFL_player_id, year). These are the exact public season NGS values -- used for the
season grain because our weekly->season weighted-average does NOT faithfully reproduce them
(validated: only avg_time_to_throw/time_to_los/rush_pct_over_expected hit ~100%). Career has
no published NGS value, so NGS is season-only.

    python scripts/build_ngs_season.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.build_ngs_weekly import PASS, RECV, RUSH  # noqa: E402

BASE = "https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats"
OUT = Path("D:/league-history-data/nfl/raw/nextgen_stats/ngs_season_2016_2025.parquet")


def _sub(con, url, mp, nm):
    sel = ", ".join(f'{s} AS {d}' for s, d in mp.items())
    con.execute(
        f'CREATE TEMP TABLE {nm} AS SELECT player_gsis_id AS gid, CAST(season AS INTEGER) AS yr, {sel} '
        f"FROM read_parquet('{url}') WHERE week = 0 AND player_gsis_id IS NOT NULL"
    )


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='800MB'")
    con.execute("PRAGMA threads=2")
    con.execute("INSTALL httpfs; LOAD httpfs")
    _sub(con, f"{BASE}/ngs_receiving.parquet", RECV, "r")
    _sub(con, f"{BASE}/ngs_rushing.parquet", RUSH, "u")
    _sub(con, f"{BASE}/ngs_passing.parquet", PASS, "p")
    rc = ", ".join(f"r.{x}" for x in RECV.values())
    uc = ", ".join(f"u.{x}" for x in RUSH.values())
    pc = ", ".join(f"p.{x}" for x in PASS.values())
    con.execute(
        f"""COPY (
            SELECT COALESCE(r.gid,u.gid,p.gid) AS NFL_player_id,
                   COALESCE(r.yr,u.yr,p.yr) AS year,
                   {rc}, {uc}, {pc}
            FROM r FULL OUTER JOIN u ON r.gid=u.gid AND r.yr=u.yr
                   FULL OUTER JOIN p ON COALESCE(r.gid,u.gid)=p.gid AND COALESCE(r.yr,u.yr)=p.yr
        ) TO '{OUT.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    n = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT NFL_player_id || '_' || year) FROM read_parquet('{OUT.as_posix()}')").fetchone()
    print(f"NGS season: {n[0]:,} rows, {n[1]:,} distinct (player,year) -> {OUT}")
    assert n[0] == n[1], "NGS season key not unique"
    return 0


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "d:/yahoo_oauth")
    raise SystemExit(main())
