#!/usr/bin/env python3
"""
Team-DST advanced layer: the defensive mirror of the offensive PBP atoms, computed by
grouping the merged PBP by `defteam` instead of by player. At the team level there is no
per-player attribution problem, so these are clean:

  def_epa_allowed (total/pass/rush) · def_success_allowed · def_explosive_allowed ·
  def_wpa_allowed

Baseline: team defensive EPA allowed = SUM(epa) over plays the team is on defense, which
equals -(opponent offensive EPA). This is the standard rbsdm/nflfastR team-defense EPA;
reproducible-from-public-PBP (L2), trustworthy where EPA is (1999+). Registered in
docs/advanced-stats-registry.json as team_def_epa_allowed.

    python scripts/build_team_defense_rollup.py --sanity 2023
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[0].parent
sys.path.insert(0, str(REPO))
from scripts.aggregate_merged_pbp_for_supertable_audit import (  # noqa: E402
    DEFAULT_PBP,
    boolish,
    nz,
    not_two_point,
    official_pass_attempt,
    official_play,
)


def build_team_defense_sql() -> str:
    counted = official_play()
    # NFL.com's passing-defense `att` is the pass-attempt denominator, not defensive
    # dropbacks: sacks and two-point attempts are excluded.  Keep the broader pass-play
    # expression for defensive plays/EPA (where sacks are plays), but use the canonical
    # official_pass_attempt expression for the attempt and first-down atoms beneath the
    # passing rates.  The old code used pass_play for both and inflated every 2025 team
    # denominator by its sacks.
    pass_attempt = f"({counted}) AND ({official_pass_attempt()})"
    pass_play = f"({counted}) AND ({not_two_point()}) AND ({boolish('pass_attempt')} OR {boolish('complete_pass')} OR {boolish('sack')} OR {boolish('interception')})"
    # As with passing, the source's rushing `att` is the official attempt atom. A broad
    # rush-play set is still needed for defensive play/EPA totals, because historical rows
    # can carry a touchdown or rushing-yards flag without the normalized rush_attempt bit.
    rush_attempt = f"({counted}) AND ({boolish('rush_attempt')}) AND ({not_two_point()})"
    rush_play = f"({counted}) AND ({not_two_point()}) AND ({boolish('rush_attempt')} OR {boolish('rush_touchdown')} OR {nz('rushing_yards')} <> 0)"
    off_play = f"({counted}) AND (({pass_play}) OR ({rush_play}))"
    return f"""
        SELECT
            CAST(defteam AS VARCHAR) AS nfl_team,
            CAST(season AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week,
            COALESCE(NULLIF(TRIM(CAST(season_type AS VARCHAR)), ''), 'REG') AS season_type,
            COUNT(*) FILTER (WHERE {off_play}) AS def_plays,
            SUM(CASE WHEN ({pass_attempt}) THEN 1.0 ELSE 0.0 END) AS def_attempts_allowed,
            SUM(CASE WHEN ({rush_attempt}) THEN 1.0 ELSE 0.0 END) AS def_carries_allowed,
            SUM(CASE WHEN ({pass_attempt}) AND {boolish('first_down_pass')} THEN 1.0 ELSE 0.0 END) AS passing_first_downs_allowed,
            SUM(CASE WHEN ({rush_attempt}) AND {boolish('first_down_rush')} THEN 1.0 ELSE 0.0 END) AS rushing_first_downs_allowed,
            SUM(CASE WHEN ({pass_attempt}) AND {boolish('complete_pass')} AND {boolish('first_down_pass')} THEN 1.0 ELSE 0.0 END) AS receiving_first_downs_allowed,
            SUM(CASE WHEN {off_play} THEN {nz('epa')} ELSE 0.0 END) AS def_epa_allowed,
            SUM(CASE WHEN ({pass_play}) THEN {nz('epa')} ELSE 0.0 END) AS def_pass_epa_allowed,
            SUM(CASE WHEN ({rush_play}) THEN {nz('epa')} ELSE 0.0 END) AS def_rush_epa_allowed,
            SUM(CASE WHEN {off_play} THEN {nz('wpa')} ELSE 0.0 END) AS def_wpa_allowed,
            SUM(CASE WHEN {off_play} AND {boolish('success')} THEN 1.0 ELSE 0.0 END) AS def_success_allowed,
            SUM(CASE WHEN {off_play} AND success IS NOT NULL THEN 1.0 ELSE 0.0 END) AS def_success_plays,
            SUM(CASE WHEN ({pass_play}) AND {boolish('complete_pass')} AND {nz('passing_yards')} >= 20 THEN 1.0 ELSE 0.0 END) AS def_explosive_pass_allowed,
            SUM(CASE WHEN ({rush_play}) AND {boolish('rush_attempt')} AND {nz('rushing_yards')} >= 10 THEN 1.0 ELSE 0.0 END) AS def_explosive_rush_allowed,
            SUM(CASE WHEN {off_play} THEN {nz('yards_gained')} ELSE 0.0 END) AS def_yards_allowed,
            SUM(CASE WHEN {off_play} AND {nz('down')} = 3 THEN 1.0 ELSE 0.0 END) AS def_third_down_faced,
            SUM(CASE WHEN {off_play} AND {nz('down')} = 3 AND {boolish('third_down_converted')} THEN 1.0 ELSE 0.0 END) AS def_third_down_allowed,
            SUM(CASE WHEN {off_play} AND {nz('down')} = 4 THEN 1.0 ELSE 0.0 END) AS def_fourth_down_faced,
            SUM(CASE WHEN {off_play} AND {nz('down')} = 4 AND {boolish('fourth_down_converted')} THEN 1.0 ELSE 0.0 END) AS def_fourth_down_allowed,
            SUM(CASE WHEN {off_play} AND yardline_100 IS NOT NULL AND CAST(yardline_100 AS DOUBLE) BETWEEN 1 AND 20 THEN 1.0 ELSE 0.0 END) AS def_rz_plays_faced,
            SUM(CASE WHEN {off_play} AND yardline_100 IS NOT NULL AND CAST(yardline_100 AS DOUBLE) BETWEEN 1 AND 20 AND {boolish('touchdown')} THEN 1.0 ELSE 0.0 END) AS def_rz_td_allowed
        FROM pbp_base
        WHERE defteam IS NOT NULL AND TRIM(CAST(defteam AS VARCHAR)) <> ''
        GROUP BY 1, 2, 3, 4
    """


def load_pbp_base(con: duckdb.DuckDBPyConnection, pbp: Path, year: int | None) -> None:
    cols = ["season", "week", "season_type", "defteam", "desc", "play_type", "pbp_source_system",
            "pass_attempt", "complete_pass", "sack", "interception", "rush_attempt", "rush_touchdown",
            "first_down_pass", "first_down_rush",
            "passing_yards", "rushing_yards", "yards_gained", "epa", "wpa", "success", "two_point_attempt",
            "down", "yardline_100", "third_down_converted", "fourth_down_converted", "touchdown"]
    where = "week IS NOT NULL AND season IS NOT NULL" + (f" AND season = {year}" if year else "")
    # VIEW, not a materialized table: the group-by streams straight from the parquet so peak
    # memory stays tiny on a low-RAM box (2.7M plays would otherwise be held in a temp table).
    con.execute(
        f"""CREATE OR REPLACE VIEW pbp_base AS SELECT {", ".join('"' + c + '"' for c in cols)}
            FROM read_parquet('{str(pbp)}') WHERE {where}"""
    )


def write_full(con: duckdb.DuckDBPyConnection, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"CREATE TEMP TABLE td AS {build_team_defense_sql()}")
    con.execute(
        f"""COPY (SELECT *, nfl_team || '_' || year || '_' || week AS team_week FROM td)
            TO '{str(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    return int(con.execute("SELECT COUNT(*) FROM td").fetchone()[0])


def sanity(con: duckdb.DuckDBPyConnection, year: int) -> None:
    con.execute(f"CREATE TEMP TABLE td AS {build_team_defense_sql()}")
    print(f"\n=== {year} REG team defenses by EPA allowed/play (lower = better) ===")
    q = """
        SELECT nfl_team,
               ROUND(SUM(def_epa_allowed) / NULLIF(SUM(def_plays), 0), 3) AS epa_per_play,
               ROUND(SUM(def_epa_allowed), 1) AS total_epa_allowed,
               ROUND(SUM(def_success_allowed) / NULLIF(SUM(def_success_plays), 0) * 100, 1) AS success_pct_allowed,
               ROUND(SUM(def_third_down_allowed) / NULLIF(SUM(def_third_down_faced), 0) * 100, 1) AS third_pct_allowed,
               ROUND(SUM(def_rz_td_allowed) / NULLIF(SUM(def_rz_plays_faced), 0) * 100, 1) AS rz_td_pct_allowed
        FROM td WHERE season_type = 'REG' GROUP BY nfl_team ORDER BY epa_per_play
    """
    rows = con.execute(q).fetchall()
    print("  BEST 5 (stingiest):")
    for r in rows[:5]:
        print(f"    {r[0]:<5} epa/play={r[1]:>7}  total={r[2]:>8}  success%={r[3]}  3rd%allowed={r[4]}  rz_td%={r[5]}")
    print("  WORST 5:")
    for r in rows[-5:]:
        print(f"    {r[0]:<5} epa/play={r[1]:>7}  total={r[2]:>8}  success%={r[3]}  3rd%allowed={r[4]}  rz_td%={r[5]}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sanity", type=int, default=None, help="Sanity-check one year (best/worst defenses)")
    p.add_argument("--output", type=Path, default=None, help="Write full team-defense-week parquet for ALL years")
    p.add_argument("--pbp", type=Path, default=DEFAULT_PBP)
    args = p.parse_args()
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET preserve_insertion_order=false")
    _td = Path("D:/league-history-data/nfl/tmp/duckdb"); _td.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{_td.as_posix()}'")
    if args.output:
        load_pbp_base(con, args.pbp, None)
        n = write_full(con, args.output)
        print(f"wrote {n:,} team-weeks -> {args.output}")
    else:
        year = args.sanity or 2023
        load_pbp_base(con, args.pbp, year)
        sanity(con, year)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
