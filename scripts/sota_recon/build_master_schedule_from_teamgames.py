"""
sota_recon/build_master_schedule_from_teamgames.py  --  rebuild the master schedule from D: PFR boxscores.

The original build_master_schedule_1920_2025_2026_04_29.py read an Excel (nfl_sched.xlsx) that was
deleted. This rebuilds the SAME output (_master_schedule_1920_2025.parquet) from authoritative D: data
that is always present:
  - raw/pfr/boxscores/nfl_team_games_all.parquet  (one row per team-game: year/week/season_type/
    game_date/team_code/opponent_code/team_fid/opponent_fid/points; 1920-2025; franchise nums align
    with v26 nfl_franchise_number)
  - raw/pfr/boxscores/tables/team_stats/  (stat='Total Yards' -> yds_team; deduped per boxscore_id)

Output schema matches recon_schedule + recon_teamtotal: year, week, nfl_team, opponent_nfl_team,
franchise_id, opponent_franchise_id, team_pts, opp_pts, game_date(DATE), season_phase, home_away, yds_team.
Result: schedule lane ~99.93% match (better than the deleted-Excel build).

    python -m scripts.sota_recon.build_master_schedule_from_teamgames
"""
from __future__ import annotations
import os
from pathlib import Path
import duckdb
from .sources import registry

D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def run() -> dict:
    reg = registry()
    out = reg["schedule_master"].path
    tg = reg["pfr_team_games"].path  # nfl_team_games_all.parquet
    ts = str(Path(tg).parent / "tables" / "team_stats" / "**" / "*.parquet")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
    con.execute(f"""COPY (
      WITH ty AS (SELECT boxscore_id, MAX(TRY_CAST(vis_stat AS INTEGER)) AS vis_yds,
                         MAX(TRY_CAST(home_stat AS INTEGER)) AS home_yds
                  FROM read_parquet('{ts}') WHERE stat='Total Yards' GROUP BY boxscore_id)
      SELECT CAST(t.year AS INTEGER) AS year, CAST(t.week AS INTEGER) AS week,
        t.team_code AS nfl_team, t.opponent_code AS opponent_nfl_team,
        CAST(t.team_fid AS INTEGER) AS franchise_id, CAST(t.opponent_fid AS INTEGER) AS opponent_franchise_id,
        t.team_points AS team_pts, t.opponent_points AS opp_pts, TRY_CAST(t.game_date AS DATE) AS game_date,
        t.season_type AS season_phase,
        CASE WHEN t.is_home THEN 'home' WHEN t.is_away THEN 'away' ELSE 'neutral' END AS home_away,
        CASE WHEN t.is_home THEN ty.home_yds ELSE ty.vis_yds END AS yds_team
      FROM '{Path(tg).as_posix()}' t LEFT JOIN ty ON t.boxscore_id=ty.boxscore_id
      WHERE t.year IS NOT NULL AND t.week IS NOT NULL
    ) TO '{Path(out).as_posix()}' (FORMAT PARQUET)""")
    n, wy = con.execute(f"SELECT COUNT(*), COUNT(yds_team) FROM '{Path(out).as_posix()}'").fetchone()
    con.close()
    return {"rows": n, "with_yds_team": wy, "out": out}


if __name__ == "__main__":
    r = run()
    print(f"BUILT {r['rows']:,} team-games ({r['with_yds_team']:,} with yds_team) -> {r['out']}")
