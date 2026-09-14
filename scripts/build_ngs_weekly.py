#!/usr/bin/env python3
"""
Build a merged Next Gen Stats WEEKLY parquet from nflverse (2016+), keyed by
(gsis_id, season, week), for folding onto the super table.

NGS metrics are PUBLISHED per-player-week values -> direct ingest (L1 by definition, the
"we know the source" case). We keep the DISTINCT advanced metrics (separation, cushion,
YAC-over-expected, RYOE, time-to-throw, CPOE/aggressiveness, ...) and skip counting stats
we already have. Each metric is prefixed `ngs_`. Weekly only (week>=1; week=0 is season
totals). player_gsis_id == our NFL_player_id for the NGS era (verified 99.5% for 2023).

    python scripts/build_ngs_weekly.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

BASE = "https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats"
OUT = Path("D:/league-history-data/nfl/raw/nextgen_stats/ngs_weekly_2016_2025.parquet")

# source_col -> ngs_ prefixed name, per NGS table. Only the distinct advanced metrics.
RECV = {
    "avg_cushion": "ngs_avg_cushion", "avg_separation": "ngs_avg_separation",
    "avg_yac": "ngs_avg_yac", "avg_expected_yac": "ngs_avg_expected_yac",
    "avg_yac_above_expectation": "ngs_avg_yac_above_expectation",
    "percent_share_of_intended_air_yards": "ngs_pct_share_intended_air_yards",
}
RUSH = {
    "efficiency": "ngs_rush_efficiency",
    "percent_attempts_gte_eight_defenders": "ngs_pct_att_gte_8_defenders",
    "avg_time_to_los": "ngs_avg_time_to_los", "expected_rush_yards": "ngs_expected_rush_yards",
    "rush_yards_over_expected": "ngs_rush_yards_over_expected",
    "rush_pct_over_expected": "ngs_rush_pct_over_expected",
}
PASS = {
    "avg_time_to_throw": "ngs_avg_time_to_throw", "aggressiveness": "ngs_aggressiveness",
    "avg_air_yards_to_sticks": "ngs_avg_air_yards_to_sticks",
    "expected_completion_percentage": "ngs_expected_completion_pct",
    "completion_percentage_above_expectation": "ngs_completion_pct_above_expectation",
    "avg_air_yards_differential": "ngs_avg_air_yards_differential",
}
NGS_COLS = list(RECV.values()) + list(RUSH.values()) + list(PASS.values())


def _sub(con, url: str, mapping: dict[str, str], name: str) -> None:
    sel = ", ".join(f"{src} AS {dst}" for src, dst in mapping.items())
    con.execute(
        f"""CREATE TEMP TABLE {name} AS
            SELECT player_gsis_id AS gid, CAST(season AS INTEGER) AS year, CAST(week AS INTEGER) AS week, {sel}
            FROM read_parquet('{url}') WHERE week >= 1 AND player_gsis_id IS NOT NULL"""
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

    rcols = ", ".join(f"r.{c}" for c in RECV.values())
    ucols = ", ".join(f"u.{c}" for c in RUSH.values())
    pcols = ", ".join(f"p.{c}" for c in PASS.values())
    con.execute(
        f"""COPY (
            SELECT COALESCE(r.gid,u.gid,p.gid) AS NFL_player_id,
                   COALESCE(r.year,u.year,p.year) AS year,
                   COALESCE(r.week,u.week,p.week) AS week,
                   {rcols}, {ucols}, {pcols}
            FROM r FULL OUTER JOIN u ON r.gid=u.gid AND r.year=u.year AND r.week=u.week
                   FULL OUTER JOIN p ON COALESCE(r.gid,u.gid)=p.gid AND COALESCE(r.year,u.year)=p.year AND COALESCE(r.week,u.week)=p.week
        ) TO '{OUT.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    n = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT NFL_player_id||'_'||year||'_'||week) FROM read_parquet('{OUT.as_posix()}')").fetchone()
    yr = con.execute(f"SELECT MIN(year), MAX(year) FROM read_parquet('{OUT.as_posix()}')").fetchone()
    print(f"wrote {n[0]:,} player-weeks ({n[1]:,} distinct keys) {yr[0]}-{yr[1]} x {len(NGS_COLS)} NGS cols -> {OUT}")
    assert n[0] == n[1], "NGS key not unique — FULL OUTER JOIN produced dup (gid,year,week)"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
