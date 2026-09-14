"""Materialize the matchup serving lattice directly from the GitHub research cache.

The job is intentionally one DuckDB process over the restored public lake.  It does
not download or assemble shard artifacts.  The output is the narrow adaptive serving
surface; the existing wide tables remain untouched on Fly.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb


def build(snapshot: Path, ops_cache: Path, output: Path, year_start: int = 1997, year_end: int = 2025,
          slug: str = "12t_flx_half_4pt", position_group: str | None = None,
          active_cache: Path | None = None, player_id: str | None = None,
          week: int | None = None, player_bucket: int | None = None,
          player_buckets: int = 1, threads: int | None = None,
          memory_limit_mb: int = 30000, reuse_player_buckets: bool = False) -> None:
    con = duckdb.connect(str(output))
    try:
        con.execute("SET preserve_insertion_order=false")
        # Keep the one-process fanout bounded.  The corrected full population
        # is larger than the old bad denominator; 16 threads exhausted the
        # runner while building hash intermediates.
        # GitHub's hosted runner is killed by the host before DuckDB can emit
        # an OOM when the allocator is allowed to approach the machine limit.
        # Keep enough headroom for the Python process, attached databases, and
        # the runner service; DuckDB can spill the remaining hash state.
        con.execute(f"SET threads={int(threads or (4 if position_group == 'IDP' else 2))}")
        con.execute(f"SET memory_limit='{int(memory_limit_mb)}MB'")
        temp_dir = output.parent / f"_research_duckdb_tmp_{output.stem}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
        con.execute("SET max_temp_directory_size='100GB'")
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops_cache.as_posix()}' AS ops (READ_ONLY)")
        if active_cache is not None:
            con.execute(f"ATTACH '{active_cache.as_posix()}' AS active_cache (READ_ONLY)")
        cached_player_columns = {
            row[0] for row in con.execute(
                "DESCRIBE lake.public.player_fantasy"
            ).fetchall()
        }
        required_cached_columns = {
            "cohort_teams", "cohort_roster", "cohort_scoring", "cohort_pass_td",
            "cohort_playoff_teams", "cohort_dynasty", "cohort_best_ball",
            "cohort_position_eligible",
        }
        missing_cached_columns = required_cached_columns - cached_player_columns
        if missing_cached_columns:
            raise RuntimeError(
                "GitHub cache is not the enriched canonical schema; missing player columns="
                f"{sorted(missing_cached_columns)}"
            )
        if position_group is not None and position_group not in {"QB", "RB", "WR", "TE", "K", "DEF", "IDP"}:
            raise ValueError(f"unsupported position group: {position_group}")
        if player_buckets < 1 or player_bucket is not None and not 0 <= player_bucket < player_buckets:
            raise ValueError("invalid player bucket")
        position_filter = ""
        if position_group is not None:
            position_filter = f"""
            AND CASE WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('QB','RB','WR','TE')
                       THEN UPPER(TRIM(CAST(p.position AS VARCHAR)))
                     WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('K','PK') THEN 'K'
                     WHEN UPPER(TRIM(CAST(p.position AS VARCHAR))) IN ('DEF','DST','D/ST') THEN 'DEF'
                     ELSE 'IDP' END = '{position_group}'
            """
        player_id_filter = (
            f"AND CAST(p.NFL_player_id AS VARCHAR)='{player_id.replace(chr(39), chr(39) * 2)}'"
            if player_id else ""
        )
        if reuse_player_buckets and player_buckets > 1:
            # The bounded builder can run every bucket in this one DuckDB
            # connection.  The placeholder is filled per bucket after the
            # population/denominator tables have been materialized once.
            player_bucket_filter = "__PLAYER_BUCKET_FILTER__"
        else:
            player_bucket_filter = (
                f"AND p.player_bucket = {int(player_bucket)}"
                if player_bucket is not None and player_buckets > 1 else ""
            )
        week_filter = f"AND CAST(p.week AS INTEGER)={int(week)}" if week is not None else ""
        base_columns = (
            "db_name", "year", "week", "cohort_teams", "cohort_roster",
            "cohort_scoring", "cohort_pass_td", "cohort_playoff_teams",
            "cohort_dynasty", "cohort_best_ball", "cohort_position_eligible",
            "position", "NFL_player_id", "manager", "is_rostered", "is_started",
            "fantasy_points", "clutch_equity", "win", "is_playoffs",
            "final_playoff_seed", "champion",
        )
        missing_base_columns = set(base_columns) - cached_player_columns
        if missing_base_columns:
            raise RuntimeError(
                "GitHub cache is missing required player base columns="
                f"{sorted(missing_base_columns)}"
            )
        quoted_base_columns = ", ".join(f'p."{column}"' for column in base_columns)
        con.execute(f"""
          CREATE TEMP TABLE position_fanout_base AS
          SELECT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                 CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                 UPPER(TRIM(CAST(COALESCE(position, nfl_position) AS VARCHAR))) AS position
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE year BETWEEN {year_start} AND {year_end}
            AND week IS NOT NULL
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(NFL_player_id AS VARCHAR), CAST(year AS INTEGER), CAST(week AS INTEGER)
            ORDER BY player_week
          ) = 1
        """)
        con.execute(f"""
          CREATE TEMP TABLE player_fantasy_base AS
          SELECT {quoted_base_columns},
                 hash(CAST(p.NFL_player_id AS VARCHAR)) % {int(player_buckets)} AS player_bucket
          FROM lake.public.player_fantasy p
          LEFT JOIN position_fanout_base f
            ON f.player_id_key=CAST(p.NFL_player_id AS VARCHAR)
           AND f.year=p.year AND f.week=p.week
          WHERE p.year BETWEEN {year_start} AND {year_end}
            AND p.week IS NOT NULL
            {position_filter.replace('p.position', 'f.position')}
          ORDER BY player_bucket
        """)
        # ``slug`` remains a CLI compatibility argument.  It must not narrow
        # the build: one restored cache produces the complete adaptive lattice.
        table_rows = con.execute("""
          SELECT table_catalog, table_schema, table_name
          FROM information_schema.tables
          WHERE table_name IN ('player_active_week','player_team_game_week')
        """).fetchall()
        table_map = {(r[2], r[0], r[1]): f'"{r[0]}"."{r[1]}"."{r[2]}"' for r in table_rows}
        active_lookup = next((v for (name, _catalog, _schema), v in table_map.items()
                              if name == 'player_active_week'), None)
        team_lookup = next((v for (name, _catalog, _schema), v in table_map.items()
                            if name == 'player_team_game_week'), None)
        if active_lookup and team_lookup:
            active_ctes = f"""
        active AS (
          SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                 CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
          FROM {active_lookup}
          WHERE year BETWEEN {year_start} AND {year_end} AND week IS NOT NULL
        ),
        team_game_week AS (
          SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                 CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
          FROM {team_lookup}
          WHERE year BETWEEN {year_start} AND {year_end} AND week IS NOT NULL
        ),
        """
        else:
            active_ctes = f"""
        active_raw AS (
          SELECT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                 CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
                 player_week, nfl_team
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE year BETWEEN {year_start} AND {year_end}
            AND week IS NOT NULL
        ),
        active AS (
          SELECT player_id_key,year,week FROM active_raw
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY player_id_key,year,week ORDER BY player_week
          ) = 1
        ),
        team_game_week AS (
          SELECT DISTINCT player_id_key,year,week FROM active_raw
        ),
        """
        common = f"""
        WITH {active_ctes}
        position_fanout AS MATERIALIZED (
          SELECT * FROM position_fanout_base
        ),
        population_inventory AS MATERIALIZED (
          SELECT DISTINCT
            p.db_name, CAST(p.year AS INTEGER) AS year, CAST(p.week AS INTEGER) AS week,
            p.cohort_teams AS teams, p.cohort_roster AS roster,
            p.cohort_scoring AS ppr, p.cohort_pass_td AS td,
            p.cohort_playoff_teams AS bracket,
            p.cohort_dynasty AS league_type,
            p.cohort_best_ball AS lineup_mode,
            CASE WHEN f.position IN ('QB','RB','WR','TE')
                      THEN f.position
                 WHEN f.position IN ('K','PK') THEN 'K'
                 WHEN f.position IN ('DEF','DST','D/ST') THEN 'DEF'
                 ELSE 'IDP' END AS pos_grp,
            CAST(p.cohort_position_eligible AS INTEGER) AS position_eligible
          FROM player_fantasy_base p
          LEFT JOIN position_fanout f
            ON f.player_id_key=CAST(p.NFL_player_id AS VARCHAR)
           AND f.year=p.year AND f.week=p.week
          WHERE p.year BETWEEN {year_start} AND {year_end}
            AND p.week IS NOT NULL
            {position_filter.replace('p.position', 'f.position')}
            {week_filter}
        ),
        position_inventory AS MATERIALIZED (
          SELECT * FROM population_inventory
          WHERE position_eligible = 1
        ),
        season_population AS MATERIALIZED (
          SELECT DISTINCT db_name,year,teams,roster,ppr,td,bracket,league_type,lineup_mode,pos_grp
          FROM position_inventory
        ),
        weekly_population AS MATERIALIZED (
          SELECT DISTINCT db_name,year,week,teams,roster,ppr,td,bracket,league_type,lineup_mode,pos_grp
          FROM position_inventory
        ),
        season_dim_counts AS MATERIALIZED (
          SELECT year,pos_grp,
            COUNT(DISTINCT db_name) FILTER (WHERE teams='10t') AS teams_10t_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE teams='12t') AS teams_12t_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='flx') AS roster_flx_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='sflx') AS roster_sflx_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='idp') AS roster_idp_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='std') AS ppr_std_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='half') AS ppr_half_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='ppr') AS ppr_ppr_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE td='4pt') AS td_4pt_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE td='6pt') AS td_6pt_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='4po') AS bracket_4po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='6po') AS bracket_6po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='8po') AS bracket_8po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE league_type='redraft') AS redraft_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE league_type='dynasty') AS dynasty_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE lineup_mode='managed') AS managed_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE lineup_mode='best_ball') AS best_ball_leagues
          FROM season_population GROUP BY 1,2
        ),
        weekly_dim_counts AS MATERIALIZED (
          SELECT year,week,pos_grp,
            COUNT(DISTINCT db_name) FILTER (WHERE teams='10t') AS teams_10t_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE teams='12t') AS teams_12t_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='flx') AS roster_flx_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='sflx') AS roster_sflx_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE roster='idp') AS roster_idp_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='std') AS ppr_std_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='half') AS ppr_half_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE ppr='ppr') AS ppr_ppr_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE td='4pt') AS td_4pt_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE td='6pt') AS td_6pt_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='4po') AS bracket_4po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='6po') AS bracket_6po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE bracket='8po') AS bracket_8po_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE league_type='redraft') AS redraft_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE league_type='dynasty') AS dynasty_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE lineup_mode='managed') AS managed_leagues,
            COUNT(DISTINCT db_name) FILTER (WHERE lineup_mode='best_ball') AS best_ball_leagues
          FROM weekly_population GROUP BY 1,2,3
        ),
        season_pooling AS MATERIALIZED (
          SELECT s.*,
            CASE WHEN (s.teams='10t' AND c.teams_10t_leagues >= 50) OR (s.teams='12t' AND c.teams_12t_leagues >= 50) THEN s.teams ELSE 'ALL' END AS effective_teams,
            CASE WHEN (s.roster='flx' AND c.roster_flx_leagues >= 50) OR (s.roster='sflx' AND c.roster_sflx_leagues >= 50) OR (s.roster='idp' AND c.roster_idp_leagues >= 50) THEN s.roster ELSE 'ALL' END AS effective_roster,
            CASE WHEN (s.ppr='std' AND c.ppr_std_leagues >= 50) OR (s.ppr='half' AND c.ppr_half_leagues >= 50) OR (s.ppr='ppr' AND c.ppr_ppr_leagues >= 50) THEN s.ppr ELSE 'ALL' END AS effective_ppr,
            CASE WHEN (s.td='4pt' AND c.td_4pt_leagues >= 50) OR (s.td='6pt' AND c.td_6pt_leagues >= 50) THEN s.td ELSE 'ALL' END AS effective_td,
            CASE WHEN (s.bracket='4po' AND c.bracket_4po_leagues >= 50) OR (s.bracket='6po' AND c.bracket_6po_leagues >= 50) OR (s.bracket='8po' AND c.bracket_8po_leagues >= 50) THEN s.bracket ELSE 'ALL' END AS effective_bracket,
            CASE WHEN (s.league_type='redraft' AND c.redraft_leagues >= 50) OR (s.league_type='dynasty' AND c.dynasty_leagues >= 50) THEN s.league_type ELSE 'ALL' END AS effective_league_type,
            CASE WHEN (s.lineup_mode='managed' AND c.managed_leagues >= 50) OR (s.lineup_mode='best_ball' AND c.best_ball_leagues >= 50) THEN s.lineup_mode ELSE 'ALL' END AS effective_lineup_mode
          FROM season_population s JOIN season_dim_counts c USING (year,pos_grp)
        ),
        weekly_pooling AS MATERIALIZED (
          SELECT w.*,
            CASE WHEN (w.teams='10t' AND c.teams_10t_leagues >= 150) OR (w.teams='12t' AND c.teams_12t_leagues >= 150) THEN w.teams ELSE 'ALL' END AS effective_teams,
            CASE WHEN (w.roster='flx' AND c.roster_flx_leagues >= 150) OR (w.roster='sflx' AND c.roster_sflx_leagues >= 150) OR (w.roster='idp' AND c.roster_idp_leagues >= 150) THEN w.roster ELSE 'ALL' END AS effective_roster,
            CASE WHEN (w.ppr='std' AND c.ppr_std_leagues >= 150) OR (w.ppr='half' AND c.ppr_half_leagues >= 150) OR (w.ppr='ppr' AND c.ppr_ppr_leagues >= 150) THEN w.ppr ELSE 'ALL' END AS effective_ppr,
            CASE WHEN (w.td='4pt' AND c.td_4pt_leagues >= 150) OR (w.td='6pt' AND c.td_6pt_leagues >= 150) THEN w.td ELSE 'ALL' END AS effective_td,
            CASE WHEN (w.bracket='4po' AND c.bracket_4po_leagues >= 150) OR (w.bracket='6po' AND c.bracket_6po_leagues >= 150) OR (w.bracket='8po' AND c.bracket_8po_leagues >= 150) THEN w.bracket ELSE 'ALL' END AS effective_bracket,
            CASE WHEN (w.league_type='redraft' AND c.redraft_leagues >= 150) OR (w.league_type='dynasty' AND c.dynasty_leagues >= 150) THEN w.league_type ELSE 'ALL' END AS effective_league_type,
            CASE WHEN (w.lineup_mode='managed' AND c.managed_leagues >= 150) OR (w.lineup_mode='best_ball' AND c.best_ball_leagues >= 150) THEN w.lineup_mode ELSE 'ALL' END AS effective_lineup_mode
          FROM weekly_population w JOIN weekly_dim_counts c USING (year,week,pos_grp)
        ),
        matchup_one AS MATERIALIZED (
          SELECT db_name, year, week, LOWER(TRIM(CAST(manager AS VARCHAR))) AS manager_key,
                 win, loss, tie, team_points, opponent_points, is_playoffs,
                 final_playoff_seed, champion,
                 ROW_NUMBER() OVER (
                   PARTITION BY db_name,year,week,LOWER(TRIM(CAST(manager AS VARCHAR)))
                   ORDER BY CASE WHEN win IS NOT NULL OR loss IS NOT NULL OR tie IS NOT NULL THEN 0 ELSE 1 END,
                            CASE WHEN team_points IS NOT NULL AND opponent_points IS NOT NULL THEN 0 ELSE 1 END
                 ) AS rn
          FROM lake.public.matchup
          WHERE year BETWEEN {year_start} AND {year_end}
        ),
        player_one AS MATERIALIZED (
          SELECT p.db_name,p.year,p.week,CAST(p.NFL_player_id AS VARCHAR) AS player_id_key,p.manager,
                 p.is_rostered,p.is_started,p.fantasy_points,p.clutch_equity,
                 p.win,p.is_playoffs,p.final_playoff_seed,p.champion,
                 f.position AS position,
                 a.player_id_key AS active_id,
                 CASE WHEN a.player_id_key IS NULL
                           AND tg.player_id_key IS NULL
                      THEN 1 ELSE 0 END AS bye_week,
                 CASE
                   WHEN f.position IN ('QB','RB','WR','TE','K','DEF')
                     THEN f.position
                   ELSE 'IDP'
                 END AS pos_grp,
                 p.cohort_teams AS teams, p.cohort_roster AS roster,
                 p.cohort_scoring AS ppr, p.cohort_pass_td AS td,
                 p.cohort_playoff_teams AS bracket,
                 p.cohort_dynasty AS league_type,
                 p.cohort_best_ball AS lineup_mode,
                 COALESCE(m.win, TRY_CAST(p.win AS INTEGER)) AS outcome_win,
                 COALESCE(m.loss, CASE WHEN p.win IS NOT NULL THEN 1-TRY_CAST(p.win AS INTEGER) END) AS outcome_loss,
                 m.tie AS outcome_tie,
                 COALESCE(m.is_playoffs,p.is_playoffs) AS playoff_signal,
                 COALESCE(m.final_playoff_seed,p.final_playoff_seed) AS playoff_seed_signal,
                 COALESCE(m.champion,p.champion) AS champion_signal,
                 ROW_NUMBER() OVER (
                   PARTITION BY p.db_name,p.year,p.week,p.NFL_player_id
                   ORDER BY CAST(p.is_started AS INTEGER) DESC, p.fantasy_points DESC NULLS LAST
                 ) AS rn
          FROM player_fantasy_base p
          LEFT JOIN active a ON a.player_id_key=CAST(p.NFL_player_id AS VARCHAR)
             AND a.year=p.year AND a.week=p.week
          LEFT JOIN position_fanout f
            ON f.player_id_key=CAST(p.NFL_player_id AS VARCHAR)
           AND f.year=p.year AND f.week=p.week
          JOIN weekly_pooling wp_filter
            ON wp_filter.db_name=p.db_name AND wp_filter.year=p.year AND wp_filter.week=p.week
           AND wp_filter.teams=p.cohort_teams AND wp_filter.roster=p.cohort_roster
           AND wp_filter.ppr=p.cohort_scoring AND wp_filter.td=p.cohort_pass_td
           AND wp_filter.bracket=p.cohort_playoff_teams
           AND wp_filter.league_type=p.cohort_dynasty AND wp_filter.lineup_mode=p.cohort_best_ball
           AND wp_filter.pos_grp=CASE
                 WHEN f.position IN ('QB','RB','WR','TE','K','DEF')
                   THEN f.position
                 ELSE 'IDP' END
          LEFT JOIN team_game_week tg ON tg.player_id_key=CAST(p.NFL_player_id AS VARCHAR)
             AND tg.year=p.year AND tg.week=p.week
          LEFT JOIN matchup_one m ON m.db_name=p.db_name AND m.year=p.year AND m.week=p.week
             AND m.manager_key=LOWER(TRIM(CAST(p.manager AS VARCHAR))) AND m.rn=1
          WHERE p.year BETWEEN {year_start} AND {year_end}
            AND p.NFL_player_id IS NOT NULL AND p.week IS NOT NULL
            {player_id_filter}
            {player_bucket_filter}
            {week_filter}
            AND (p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER)=1)
        ),
        player AS MATERIALIZED (
          SELECT * FROM player_one WHERE rn=1
        ),
        weekly_player AS MATERIALIZED (
          SELECT p.* EXCLUDE (teams,roster,ppr,td,bracket,league_type,lineup_mode),
                 wp.effective_teams AS teams, wp.effective_roster AS roster,
                 wp.effective_ppr AS ppr, wp.effective_td AS td,
                 wp.effective_bracket AS bracket,
                 wp.effective_league_type AS league_type,
                 wp.effective_lineup_mode AS lineup_mode
          FROM player p
          JOIN weekly_pooling wp
            ON wp.db_name=p.db_name AND wp.year=p.year AND wp.week=p.week
           AND wp.pos_grp=p.pos_grp AND wp.teams=p.teams AND wp.roster=p.roster
           AND wp.ppr=p.ppr AND wp.td=p.td AND wp.bracket=p.bracket
           AND wp.league_type=p.league_type AND wp.lineup_mode=p.lineup_mode
        ),
        season_player AS MATERIALIZED (
          SELECT p.* EXCLUDE (teams,roster,ppr,td,bracket,league_type,lineup_mode),
                 sp.effective_teams AS teams, sp.effective_roster AS roster,
                 sp.effective_ppr AS ppr, sp.effective_td AS td,
                 sp.effective_bracket AS bracket,
                 sp.effective_league_type AS league_type,
                 sp.effective_lineup_mode AS lineup_mode
          FROM player p
          JOIN season_pooling sp
            ON sp.db_name=p.db_name AND sp.year=p.year AND sp.pos_grp=p.pos_grp
           AND sp.teams=p.teams AND sp.roster=p.roster
           AND sp.ppr=p.ppr AND sp.td=p.td AND sp.bracket=p.bracket
           AND sp.league_type=p.league_type AND sp.lineup_mode=p.lineup_mode
        ),
        eligible AS (
          SELECT i.effective_teams AS teams,i.effective_roster AS roster,
                 i.effective_ppr AS ppr,i.effective_td AS td,
                 i.effective_league_type AS league_type,
                 i.effective_lineup_mode AS lineup_mode,
                 i.year,i.week,i.pos_grp,COUNT(*) AS n_leagues
          FROM weekly_pooling i
          GROUP BY ALL
        ),
        weekly_exact AS (
          SELECT p.teams,p.roster,p.ppr,p.td,p.bracket,p.league_type,p.lineup_mode,
            p.year,p.week,p.player_id_key,p.pos_grp,
            COUNT(*) AS rostered_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1) AS started_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL) AS healthy_started_leagues,
            SUM(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL THEN COALESCE(p.fantasy_points,0) ELSE 0 END) AS points_started,
            SUM(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL THEN COALESCE(p.clutch_equity,0) ELSE 0 END) AS clutch_sum,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.outcome_win=1) AS wins_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.outcome_loss=1) AS losses_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND (p.outcome_win=1 OR p.outcome_loss=1 OR p.outcome_tie=1)) AS valid_outcome_started_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.playoff_signal=1) AS playoff_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.champion_signal=1) AS champ_started,
            MAX(e.n_leagues) AS n_leagues,
            MAX(CASE WHEN p.active_id IS NOT NULL THEN 1 ELSE 0 END) AS active_week,
            MAX(p.bye_week) AS bye_week
          FROM weekly_player p LEFT JOIN eligible e
            ON e.teams=p.teams AND e.roster=p.roster AND e.ppr=p.ppr AND e.td=p.td
           AND e.league_type=p.league_type AND e.lineup_mode=p.lineup_mode
           AND e.year=p.year AND e.week=p.week AND e.pos_grp=p.pos_grp
          GROUP BY ALL
        ),
        weekly_bracket AS (
          SELECT teams,roster,ppr,td,bracket,
            CASE WHEN GROUPING(league_type)=1 THEN 'ALL' ELSE league_type END AS league_type,
            CASE WHEN GROUPING(lineup_mode)=1 THEN 'ALL' ELSE lineup_mode END AS lineup_mode,
            year,week,player_id_key,pos_grp,
            SUM(rostered_leagues) AS rostered_leagues,
            SUM(started_leagues) AS started_leagues,
            SUM(healthy_started_leagues) AS healthy_started_leagues,
            SUM(points_started) AS points_started, SUM(clutch_sum) AS clutch_sum,
            SUM(wins_started) AS wins_started, SUM(losses_started) AS losses_started,
            SUM(valid_outcome_started_leagues) AS valid_outcome_started_leagues,
            SUM(playoff_started) AS playoff_started, SUM(champ_started) AS champ_started,
            SUM(n_leagues) AS n_leagues, MAX(active_week) AS active_week,
            MAX(bye_week) AS bye_week
          FROM weekly_exact
          GROUP BY GROUPING SETS (
            (teams,roster,ppr,td,bracket,league_type,lineup_mode,year,week,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,league_type,year,week,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,lineup_mode,year,week,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,year,week,player_id_key,pos_grp)
          )
        ),
        -- Bracket is a playoff/championship dimension only.  Ordinary
        -- roster/start/win population counts are pooled across every bracket
        -- before the bracket-specific playoff fields are put back on each lane.
        weekly_ordinary AS (
          SELECT teams,roster,ppr,td,league_type,lineup_mode,year,week,player_id_key,pos_grp,
            SUM(rostered_leagues) AS rostered_leagues,
            SUM(started_leagues) AS started_leagues,
            SUM(healthy_started_leagues) AS healthy_started_leagues,
            SUM(points_started) AS points_started, SUM(clutch_sum) AS clutch_sum,
            SUM(wins_started) AS wins_started, SUM(losses_started) AS losses_started,
            SUM(valid_outcome_started_leagues) AS valid_outcome_started_leagues,
            SUM(n_leagues) AS n_leagues, MAX(active_week) AS active_week,
            MAX(bye_week) AS bye_week
          FROM weekly_bracket
          GROUP BY ALL
        ),
        weekly AS (
          SELECT b.teams,b.roster,b.ppr,b.td,b.bracket,b.league_type,b.lineup_mode,
            b.year,b.week,b.player_id_key,b.pos_grp,
            o.rostered_leagues,o.started_leagues,o.healthy_started_leagues,
            o.points_started,o.clutch_sum,o.wins_started,o.losses_started,
            o.valid_outcome_started_leagues,b.playoff_started,b.champ_started,
            o.n_leagues,o.active_week,o.bye_week
          FROM weekly_bracket b
          JOIN weekly_ordinary o USING
            (teams,roster,ppr,td,league_type,lineup_mode,year,week,player_id_key,pos_grp)
        ),
        season_eligible AS (
          SELECT i.effective_teams AS teams,i.effective_roster AS roster,
            i.effective_ppr AS ppr,i.effective_td AS td,
            i.effective_league_type AS league_type,
            i.effective_lineup_mode AS lineup_mode,
            i.year,i.pos_grp,COUNT(*) AS n_leagues
          FROM season_pooling i
          GROUP BY ALL
        ),
        season_weekly_exact AS (
          SELECT p.teams,p.roster,p.ppr,p.td,p.bracket,p.league_type,p.lineup_mode,
            p.year,p.week,p.player_id_key,p.pos_grp,
            COUNT(*) AS rostered_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1) AS started_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL) AS healthy_started_leagues,
            SUM(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL THEN COALESCE(p.fantasy_points,0) ELSE 0 END) AS points_started,
            SUM(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND p.active_id IS NOT NULL THEN COALESCE(p.clutch_equity,0) ELSE 0 END) AS clutch_sum,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.outcome_win=1) AS wins_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.outcome_loss=1) AS losses_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND (p.outcome_win=1 OR p.outcome_loss=1 OR p.outcome_tie=1)) AS valid_outcome_started_leagues,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.playoff_signal=1) AS playoff_started,
            COUNT(*) FILTER (WHERE CAST(p.is_started AS INTEGER)=1 AND p.champion_signal=1) AS champ_started,
            MAX(e.n_leagues) AS n_leagues,
            MAX(CASE WHEN p.active_id IS NOT NULL THEN 1 ELSE 0 END) AS active_week,
            MAX(p.bye_week) AS bye_week
          FROM season_player p LEFT JOIN season_eligible e
            ON e.teams=p.teams AND e.roster=p.roster AND e.ppr=p.ppr AND e.td=p.td
           AND e.league_type=p.league_type AND e.lineup_mode=p.lineup_mode
           AND e.year=p.year AND e.pos_grp=p.pos_grp
          GROUP BY ALL
        ),
        season_roster AS (
          SELECT p.teams,p.roster,p.ppr,p.td,p.bracket,p.league_type,p.lineup_mode,
            p.year,p.player_id_key,p.pos_grp,
            COUNT(DISTINCT p.db_name) AS rostered_leagues,
            COUNT(DISTINCT p.week) FILTER (WHERE p.active_id IS NOT NULL) AS active_weeks,
            COUNT(DISTINCT p.week) FILTER (WHERE p.active_id IS NULL AND p.bye_week=0) AS inactive_weeks,
            COUNT(DISTINCT p.week) FILTER (WHERE p.active_id IS NOT NULL AND CAST(p.is_started AS INTEGER)=1) AS started_weeks,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.playoff_signal=1) AS playoff_eligible_leagues,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.playoff_signal=1 AND CAST(p.is_started AS INTEGER)=1) AS playoff_started_leagues,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.champion_signal=1) AS champ_eligible_leagues,
            COUNT(DISTINCT p.db_name) FILTER (WHERE p.champion_signal=1 AND CAST(p.is_started AS INTEGER)=1) AS champ_started_leagues
          FROM season_player p
          GROUP BY ALL
        ),
        season_exact_bracket AS (
          SELECT w.teams,w.roster,w.ppr,w.td,w.bracket,w.league_type,w.lineup_mode,
            w.year,w.player_id_key,w.pos_grp,
            MAX(sr.rostered_leagues) AS rostered_leagues,
            SUM(w.started_leagues) FILTER (WHERE w.bye_week=0) AS started_total,
            SUM(w.n_leagues) FILTER (WHERE w.bye_week=0) AS eligible_league_weeks,
            SUM(w.n_leagues) FILTER (WHERE w.active_week=1 AND w.bye_week=0) AS healthy_eligible_league_weeks,
            SUM(w.wins_started) AS wins_total,
            SUM(w.losses_started) AS losses_total,
            SUM(w.valid_outcome_started_leagues) FILTER (WHERE w.bye_week=0) AS valid_outcome_started_total,
            SUM(w.wins_started/NULLIF(w.n_leagues,0)) FILTER (WHERE w.bye_week=0) AS expected_wins_total,
            SUM(w.losses_started/NULLIF(w.n_leagues,0)) FILTER (WHERE w.bye_week=0) AS expected_losses_total,
            SUM(w.clutch_sum/NULLIF(w.started_leagues,0)) FILTER (WHERE w.bye_week=0) AS clutch_sum,
            SUM(w.started_leagues) FILTER (WHERE w.bye_week=0) AS started_rows,
            SUM(w.healthy_started_leagues) FILTER (WHERE w.bye_week=0) AS healthy_started_rows,
            MAX(sr.active_weeks) AS active_weeks, MAX(sr.inactive_weeks) AS inactive_weeks,
            MAX(sr.started_weeks) AS started_weeks,
            MAX(e.n_leagues) AS playoff_eligible_leagues,
            MAX(sr.playoff_started_leagues) AS playoff_started_leagues,
            MAX(e.n_leagues) AS champ_eligible_leagues,
            MAX(sr.champ_started_leagues) AS champ_started_leagues,
            MAX(e.n_leagues) AS n_leagues
          FROM season_weekly_exact w
          JOIN season_roster sr USING (teams,roster,ppr,td,bracket,league_type,lineup_mode,year,player_id_key,pos_grp)
          JOIN season_eligible e
            ON e.teams=w.teams AND e.roster=w.roster AND e.ppr=w.ppr AND e.td=w.td
           AND e.league_type=w.league_type AND e.lineup_mode=w.lineup_mode
           AND e.year=w.year AND e.pos_grp=w.pos_grp
          GROUP BY ALL
        ),
        season_ordinary AS (
          SELECT teams,roster,ppr,td,league_type,lineup_mode,year,player_id_key,pos_grp,
            SUM(rostered_leagues) AS rostered_leagues,
            SUM(started_total) AS started_total, SUM(wins_total) AS wins_total,
            SUM(losses_total) AS losses_total, SUM(clutch_sum) AS clutch_sum,
            SUM(valid_outcome_started_total) AS valid_outcome_started_total,
            SUM(expected_wins_total) AS expected_wins_total,
            SUM(expected_losses_total) AS expected_losses_total,
            SUM(eligible_league_weeks) AS eligible_league_weeks,
            SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
            SUM(started_rows) AS started_rows, SUM(healthy_started_rows) AS healthy_started_rows,
            MAX(active_weeks) AS active_weeks, MAX(inactive_weeks) AS inactive_weeks,
            MAX(started_weeks) AS started_weeks, SUM(n_leagues) AS n_leagues
          FROM season_exact_bracket
          GROUP BY ALL
        ),
        season_exact AS (
          SELECT b.teams,b.roster,b.ppr,b.td,b.bracket,b.league_type,b.lineup_mode,
            b.year,b.player_id_key,b.pos_grp,
            o.rostered_leagues,o.started_total,o.eligible_league_weeks,
            o.healthy_eligible_league_weeks,o.wins_total,o.losses_total,
            o.valid_outcome_started_total,o.expected_wins_total,o.expected_losses_total,
            o.clutch_sum,o.started_rows,o.healthy_started_rows,o.active_weeks,
            o.inactive_weeks,o.started_weeks,
            b.playoff_eligible_leagues,b.playoff_started_leagues,
            b.champ_eligible_leagues,b.champ_started_leagues,o.n_leagues
          FROM season_exact_bracket b
          JOIN season_ordinary o USING
            (teams,roster,ppr,td,league_type,lineup_mode,year,player_id_key,pos_grp)
        ),
        season_lattice AS (
          SELECT teams,roster,ppr,td,bracket,
            CASE WHEN GROUPING(league_type)=1 THEN 'ALL' ELSE league_type END AS league_type,
            CASE WHEN GROUPING(lineup_mode)=1 THEN 'ALL' ELSE lineup_mode END AS lineup_mode,
            year,player_id_key,pos_grp,
            SUM(rostered_leagues) AS rostered_leagues,
            SUM(started_total) AS started_total, SUM(wins_total) AS wins_total,
            SUM(losses_total) AS losses_total, SUM(clutch_sum) AS clutch_sum,
            SUM(valid_outcome_started_total) AS valid_outcome_started_total,
            SUM(expected_wins_total) AS expected_wins_total,
            SUM(expected_losses_total) AS expected_losses_total,
            SUM(eligible_league_weeks) AS eligible_league_weeks,
            SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
            SUM(started_rows) AS started_rows, SUM(healthy_started_rows) AS healthy_started_rows,
            MAX(active_weeks) AS active_weeks,
            MAX(inactive_weeks) AS inactive_weeks, MAX(started_weeks) AS started_weeks,
            SUM(playoff_eligible_leagues) AS playoff_eligible_leagues,
            SUM(playoff_started_leagues) AS playoff_started_leagues,
            SUM(champ_eligible_leagues) AS champ_eligible_leagues,
            SUM(champ_started_leagues) AS champ_started_leagues,
            SUM(n_leagues) AS n_leagues
          FROM season_exact
          GROUP BY GROUPING SETS (
            (teams,roster,ppr,td,bracket,league_type,lineup_mode,year,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,league_type,year,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,lineup_mode,year,player_id_key,pos_grp),
            (teams,roster,ppr,td,bracket,year,player_id_key,pos_grp)
          )
        ),
        season_out AS (
          SELECT *,
            100.0*rostered_leagues/NULLIF(n_leagues,0) AS roster_rate_pct,
            (started_total/NULLIF(eligible_league_weeks,0))*active_weeks AS expected_starts,
            expected_starts*wins_total/NULLIF(valid_outcome_started_total,0) AS expected_wins,
            expected_starts*(1.0-wins_total/NULLIF(valid_outcome_started_total,0)) AS expected_losses,
            100.0*started_total/NULLIF(eligible_league_weeks,0) AS start_rate_pct,
            100.0*healthy_started_rows/NULLIF(healthy_eligible_league_weeks,0) AS healthy_start_rate_pct,
            healthy_started_rows/NULLIF(n_leagues,0) AS healthy_expected_starts,
            100.0*wins_total/NULLIF(valid_outcome_started_total,0) AS win_rate_pct,
            clutch_sum/NULLIF(started_total,0) AS avg_clutch_started,
            100.0*champ_started_leagues/NULLIF(n_leagues,0) AS champ_as_starter_pct,
            100.0*playoff_started_leagues/NULLIF(n_leagues,0) AS playoff_as_starter_pct,
            NULL::DOUBLE AS total_points_observed, NULL::DOUBLE AS ppg_when_started,
            NULL::DOUBLE AS expected_champs, NULL::DOUBLE AS expected_playoffs,
            1::BIGINT AS n_years, 0::INTEGER AS cohort_level
          FROM season_lattice
        ),
        weekly_out AS (
          SELECT *,
            100.0*rostered_leagues/NULLIF(n_leagues,0) AS roster_rate_pct,
            100.0*started_leagues/NULLIF(n_leagues,0) AS start_rate_pct,
            100.0*healthy_started_leagues/NULLIF(n_leagues,0) AS healthy_start_rate_pct,
            100.0*wins_started/NULLIF(valid_outcome_started_leagues,0) AS win_rate_pct,
            wins_started/NULLIF(n_leagues,0) * started_leagues/NULLIF(valid_outcome_started_leagues,0) AS expected_wins,
            CAST(started_leagues AS DOUBLE) AS expected_starts,
            (started_leagues/NULLIF(n_leagues,0)) * (1.0-wins_started/NULLIF(valid_outcome_started_leagues,0)) AS expected_losses,
            points_started/NULLIF(started_leagues,0) AS ppg_when_started,
            clutch_sum/NULLIF(started_leagues,0) AS avg_clutch_started,
            100.0*champ_started/NULLIF(n_leagues,0) AS champ_as_starter_pct,
            100.0*playoff_started/NULLIF(n_leagues,0) AS playoff_as_starter_pct,
            1::BIGINT AS active_weeks, 0::BIGINT AS inactive_weeks,
            CASE WHEN started_leagues>0 THEN 1 ELSE 0 END::BIGINT AS started_weeks,
            1::BIGINT AS n_years, NULL::DOUBLE AS expected_champs,
            NULL::DOUBLE AS expected_playoffs, NULL::DOUBLE AS total_points_observed,
            NULL::DOUBLE AS avg_lamar_started, 0::INTEGER AS cohort_level
          FROM weekly
        ),
        -- Execute the expensive population/player joins once.  The old builder
        -- re-ran the entire CTE graph independently for weekly, season, and
        -- career outputs; career is derived from the season table below.
        combined_out AS (
          SELECT
            0::INTEGER AS grain, teams,roster,ppr,td,bracket,league_type,lineup_mode,
            year,week,player_id_key,pos_grp,
            rostered_leagues,started_leagues,healthy_started_leagues,
            total_points_observed,points_started,clutch_sum,wins_started,losses_started,
            expected_wins,expected_losses,expected_starts,
            champ_started,playoff_started,
            NULL::BIGINT AS champ_eligible_leagues,
            NULL::BIGINT AS playoff_eligible_leagues,
            NULL::DOUBLE AS healthy_expected_starts,
            n_leagues,roster_rate_pct,start_rate_pct,healthy_start_rate_pct,win_rate_pct,
            ppg_when_started,avg_clutch_started,champ_as_starter_pct,playoff_as_starter_pct,
            expected_champs,expected_playoffs,active_weeks,inactive_weeks,started_weeks,
            active_week,bye_week,avg_lamar_started,n_years,cohort_level
          FROM weekly_out
          UNION ALL
          SELECT
            1::INTEGER AS grain, teams,roster,ppr,td,bracket,league_type,lineup_mode,
            year,NULL::INTEGER AS week,player_id_key,pos_grp,
            rostered_leagues,started_rows AS started_leagues,healthy_started_rows AS healthy_started_leagues,
            total_points_observed,NULL::DOUBLE AS points_started,clutch_sum,
            wins_total/NULLIF(n_leagues,0) AS wins_started,
            losses_total/NULLIF(n_leagues,0) AS losses_started,
            expected_wins,expected_losses,expected_starts,
            champ_started_leagues AS champ_started,playoff_started_leagues AS playoff_started,
            champ_eligible_leagues,playoff_eligible_leagues,healthy_expected_starts,
            n_leagues,roster_rate_pct,start_rate_pct,healthy_start_rate_pct,win_rate_pct,
            ppg_when_started,avg_clutch_started,champ_as_starter_pct,playoff_as_starter_pct,
            expected_champs,expected_playoffs,active_weeks,inactive_weeks,started_weeks,
            NULL::BIGINT AS active_week,NULL::BIGINT AS bye_week,
            NULL::DOUBLE AS avg_lamar_started,n_years,cohort_level
          FROM season_out
        )
        """
        if reuse_player_buckets and player_buckets > 1:
            # Materialize the expensive population, pooling, and matchup
            # graph once.  Each player bucket then only reruns the player
            # aggregation against those shared tables.
            shared_end = common.index("        player_one AS MATERIALIZED")
            shared = common[:shared_end].rstrip()
            if shared.endswith(","):
                shared = shared[:-1].rstrip()
            # Build the shared relations once.  The previous implementation
            # ran the entire shared CTE graph once per relation, which silently
            # multiplied the expensive inventory/window work by five.
            population_end = common.index("        position_inventory AS MATERIALIZED")
            population_sql = common[:population_end].rstrip()
            if population_sql.endswith(","):
                population_sql = population_sql[:-1].rstrip()
            con.execute(
                "CREATE TEMP TABLE _population_inventory_base AS\n"
                + population_sql
                + "\nSELECT * FROM population_inventory"
            )

            season_start = common.index("        season_population AS MATERIALIZED")
            weekly_start = common.index("        weekly_population AS MATERIALIZED")
            season_end = common.index("        weekly_pooling AS MATERIALIZED")
            season_sql = common[season_start:season_end].rstrip()
            if season_sql.endswith(","):
                season_sql = season_sql[:-1].rstrip()
            con.execute(
                "CREATE TEMP TABLE _season_pooling_base AS\nWITH position_inventory AS ("
                "SELECT * FROM _population_inventory_base WHERE position_eligible=1),\n"
                + season_sql.replace("FROM season_population", "FROM season_population")
                + "\nSELECT * FROM season_pooling"
            )

            weekly_end = common.index("        matchup_one AS MATERIALIZED")
            weekly_sql = common[weekly_start:weekly_end].rstrip()
            if weekly_sql.endswith(","):
                weekly_sql = weekly_sql[:-1].rstrip()
            con.execute(
                "CREATE TEMP TABLE _weekly_pooling_base AS\nWITH position_inventory AS ("
                "SELECT * FROM _population_inventory_base WHERE position_eligible=1),\n"
                + weekly_sql
                + "\nSELECT * FROM weekly_pooling"
            )

            if active_lookup and team_lookup:
                con.execute(f"""
                  CREATE TEMP TABLE _active_base AS
                  SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                         CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
                  FROM {active_lookup}
                  WHERE year BETWEEN {year_start} AND {year_end} AND week IS NOT NULL
                """)
                con.execute(f"""
                  CREATE TEMP TABLE _team_game_week_base AS
                  SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                         CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
                  FROM {team_lookup}
                  WHERE year BETWEEN {year_start} AND {year_end} AND week IS NOT NULL
                """)
            else:
                con.execute(f"""
                  CREATE TEMP TABLE _active_base AS
                  SELECT CAST(NFL_player_id AS VARCHAR) AS player_id_key,
                         CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
                  FROM ops.nfl_historical.nfl_player_stats_all
                  WHERE year BETWEEN {year_start} AND {year_end} AND week IS NOT NULL
                  QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY CAST(NFL_player_id AS VARCHAR), CAST(year AS INTEGER), CAST(week AS INTEGER)
                    ORDER BY player_week
                  ) = 1
                """)
                con.execute("""
                  CREATE TEMP TABLE _team_game_week_base AS
                  SELECT * FROM _active_base
                """)

            matchup_start = common.index("        matchup_one AS MATERIALIZED")
            matchup_end = common.index("        player_one AS MATERIALIZED")
            matchup_sql = "WITH " + common[matchup_start:matchup_end].rstrip().rstrip(",")
            con.execute(
                "CREATE TEMP TABLE _matchup_one_base AS\n"
                + matchup_sql
                + "\nSELECT * FROM matchup_one"
            )

            suffix = "WITH " + common[matchup_end:]
            suffix = suffix.replace("position_fanout f", "position_fanout_base f")
            suffix = suffix.replace("active a", "_active_base a")
            suffix = suffix.replace("team_game_week tg", "_team_game_week_base tg")
            suffix = suffix.replace("matchup_one m", "_matchup_one_base m")
            suffix = suffix.replace("weekly_pooling wp_filter", "_weekly_pooling_base wp_filter")
            suffix = suffix.replace("weekly_pooling wp", "_weekly_pooling_base wp")
            suffix = suffix.replace("season_pooling sp", "_season_pooling_base sp")
            suffix = suffix.replace("FROM weekly_pooling i", "FROM _weekly_pooling_base i")
            suffix = suffix.replace("FROM season_pooling i", "FROM _season_pooling_base i")

            for bucket in range(player_buckets):
                bucket_filter = f"AND p.player_bucket = {bucket}"
                bucket_sql = suffix.replace("__PLAYER_BUCKET_FILTER__", bucket_filter)
                con.execute("DROP TABLE IF EXISTS _combined_out")
                con.execute(
                    "CREATE TEMP TABLE _combined_out AS\n"
                    + bucket_sql
                    + "\nSELECT * FROM combined_out"
                )
                if bucket == 0:
                    con.execute("""
                      CREATE TABLE research_matchup_adaptive_weekly AS
                      SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr,
                        td AS q_td, bracket AS q_bracket,
                        league_type AS q_league_type, lineup_mode AS q_lineup_mode,
                        CAST(player_id_key AS VARCHAR) AS NFL_player_id,
                        * EXCLUDE (grain,teams,roster,ppr,td,bracket,league_type,
                                   lineup_mode,player_id_key)
                      FROM _combined_out WHERE grain=0
                    """)
                    con.execute("""
                      CREATE TABLE research_matchup_adaptive AS
                      SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr,
                        td AS q_td, bracket AS q_bracket,
                        league_type AS q_league_type, lineup_mode AS q_lineup_mode,
                        CAST(player_id_key AS VARCHAR) AS NFL_player_id, year,
                        rostered_leagues, started_leagues, healthy_started_leagues,
                        total_points_observed, clutch_sum, wins_started, losses_started,
                        expected_wins, expected_losses, expected_starts,
                        champ_started, playoff_started, champ_eligible_leagues,
                        playoff_eligible_leagues, healthy_expected_starts, n_leagues,
                        roster_rate_pct, start_rate_pct, healthy_start_rate_pct,
                        win_rate_pct, ppg_when_started, avg_clutch_started,
                        champ_as_starter_pct, playoff_as_starter_pct,
                        expected_champs, expected_playoffs, active_weeks,
                        inactive_weeks, started_weeks, n_years,
                        NULL::DOUBLE AS avg_lamar_started, cohort_level
                      FROM _combined_out WHERE grain=1
                    """)
                else:
                    con.execute("""
                      INSERT INTO research_matchup_adaptive_weekly
                      SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr,
                        td AS q_td, bracket AS q_bracket,
                        league_type AS q_league_type, lineup_mode AS q_lineup_mode,
                        CAST(player_id_key AS VARCHAR) AS NFL_player_id,
                        * EXCLUDE (grain,teams,roster,ppr,td,bracket,league_type,
                                   lineup_mode,player_id_key)
                      FROM _combined_out WHERE grain=0
                    """)
                    con.execute("""
                      INSERT INTO research_matchup_adaptive
                      SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr,
                        td AS q_td, bracket AS q_bracket,
                        league_type AS q_league_type, lineup_mode AS q_lineup_mode,
                        CAST(player_id_key AS VARCHAR) AS NFL_player_id, year,
                        rostered_leagues, started_leagues, healthy_started_leagues,
                        total_points_observed, clutch_sum, wins_started, losses_started,
                        expected_wins, expected_losses, expected_starts,
                        champ_started, playoff_started, champ_eligible_leagues,
                        playoff_eligible_leagues, healthy_expected_starts, n_leagues,
                        roster_rate_pct, start_rate_pct, healthy_start_rate_pct,
                        win_rate_pct, ppg_when_started, avg_clutch_started,
                        champ_as_starter_pct, playoff_as_starter_pct,
                        expected_champs, expected_playoffs, active_weeks,
                        inactive_weeks, started_weeks, n_years,
                        NULL::DOUBLE AS avg_lamar_started, cohort_level
                      FROM _combined_out WHERE grain=1
                    """)

            con.execute("""
              CREATE TABLE research_matchup_adaptive_career AS
              SELECT q_teams,q_roster,q_ppr,q_td,q_bracket,q_league_type,q_lineup_mode,
                NFL_player_id,
                SUM(total_points_observed) AS total_points_observed,
                SUM(rostered_leagues)*100.0/NULLIF(SUM(n_leagues),0) AS roster_rate_pct,
                SUM(expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS start_rate_pct,
                SUM(healthy_expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS healthy_start_rate_pct,
                SUM(expected_wins)/NULLIF(SUM(expected_starts),0)*100.0 AS win_rate_pct,
                SUM(expected_wins)::DOUBLE AS expected_wins,
                SUM(expected_losses)::DOUBLE AS expected_losses,
                SUM(expected_starts)::DOUBLE AS expected_starts,
                SUM(total_points_observed)/NULLIF(SUM(started_leagues),0) AS ppg_when_started,
                SUM(clutch_sum) AS avg_clutch_started,
                AVG(champ_as_starter_pct) AS champ_as_starter_pct,
                AVG(playoff_as_starter_pct) AS playoff_as_starter_pct,
                SUM(1.0*champ_started/NULLIF(champ_eligible_leagues,0)) AS expected_champs,
                SUM(1.0*playoff_started/NULLIF(playoff_eligible_leagues,0)) AS expected_playoffs,
                SUM(active_weeks)::BIGINT AS active_weeks,
                SUM(inactive_weeks)::BIGINT AS inactive_weeks,
                SUM(started_weeks)::BIGINT AS started_weeks,
                COUNT(DISTINCT year)::BIGINT AS n_years,
                NULL::DOUBLE AS avg_lamar_started,
                MAX(cohort_level) AS cohort_level,
                MAX(n_leagues)::BIGINT AS n_leagues
              FROM research_matchup_adaptive
              GROUP BY ALL
            """)
            return
        con.execute("""
          CREATE TEMP TABLE _combined_out AS
        """ + common + """
          SELECT * FROM combined_out
        """)
        con.execute("""
          CREATE TABLE research_matchup_adaptive_weekly AS
          SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr, td AS q_td,
            bracket AS q_bracket, league_type AS q_league_type, lineup_mode AS q_lineup_mode,
            CAST(player_id_key AS VARCHAR) AS NFL_player_id,
            * EXCLUDE (grain,teams,roster,ppr,td,bracket,league_type,lineup_mode,player_id_key)
          FROM _combined_out
          WHERE grain=0
        """)
        con.execute("""
          CREATE TABLE research_matchup_adaptive AS
          SELECT teams AS q_teams, roster AS q_roster, ppr AS q_ppr, td AS q_td,
            bracket AS q_bracket, league_type AS q_league_type, lineup_mode AS q_lineup_mode,
            CAST(player_id_key AS VARCHAR) AS NFL_player_id, year,
            rostered_leagues, started_leagues, healthy_started_leagues,
            total_points_observed, clutch_sum, wins_started, losses_started,
            expected_wins, expected_losses, expected_starts,
            champ_started, playoff_started, champ_eligible_leagues, playoff_eligible_leagues,
            healthy_expected_starts, n_leagues, roster_rate_pct, start_rate_pct,
            healthy_start_rate_pct, win_rate_pct, ppg_when_started, avg_clutch_started,
            champ_as_starter_pct, playoff_as_starter_pct, expected_champs, expected_playoffs,
            active_weeks, inactive_weeks, started_weeks, n_years,
            NULL::DOUBLE AS avg_lamar_started, cohort_level
          FROM _combined_out
          WHERE grain=1
        """)
        con.execute("""
          CREATE TABLE research_matchup_adaptive_career AS
          SELECT q_teams,q_roster,q_ppr,q_td,q_bracket,q_league_type,q_lineup_mode,NFL_player_id,
            SUM(total_points_observed) AS total_points_observed,
            SUM(rostered_leagues)*100.0/NULLIF(SUM(n_leagues),0) AS roster_rate_pct,
            SUM(expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS start_rate_pct,
            SUM(healthy_expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS healthy_start_rate_pct,
            SUM(expected_wins)/NULLIF(SUM(expected_starts),0)*100.0 AS win_rate_pct,
            SUM(expected_wins)::DOUBLE AS expected_wins, SUM(expected_losses)::DOUBLE AS expected_losses,
            SUM(expected_starts)::DOUBLE AS expected_starts,
            SUM(total_points_observed)/NULLIF(SUM(started_leagues),0) AS ppg_when_started,
            SUM(clutch_sum) AS avg_clutch_started,
            AVG(champ_as_starter_pct) AS champ_as_starter_pct,
            AVG(playoff_as_starter_pct) AS playoff_as_starter_pct,
            SUM(1.0*champ_started/NULLIF(champ_eligible_leagues,0)) AS expected_champs,
            SUM(1.0*playoff_started/NULLIF(playoff_eligible_leagues,0)) AS expected_playoffs,
            SUM(active_weeks)::BIGINT AS active_weeks, SUM(inactive_weeks)::BIGINT AS inactive_weeks,
            SUM(started_weeks)::BIGINT AS started_weeks, COUNT(DISTINCT year)::BIGINT AS n_years,
            NULL::DOUBLE AS avg_lamar_started, MAX(cohort_level) AS cohort_level,
            MAX(n_leagues)::BIGINT AS n_leagues
          FROM (
            SELECT q_teams,q_roster,q_ppr,q_td,q_bracket,q_league_type,q_lineup_mode,NFL_player_id,year,
              total_points_observed,rostered_leagues,started_leagues,healthy_started_leagues,
              expected_wins,expected_losses,expected_starts,healthy_expected_starts,
              champ_started,champ_eligible_leagues,playoff_started,playoff_eligible_leagues,
              champ_as_starter_pct,playoff_as_starter_pct,
              n_leagues,clutch_sum,active_weeks,inactive_weeks,started_weeks,cohort_level
            FROM research_matchup_adaptive
          ) x GROUP BY ALL
        """)
    finally:
        con.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--ops-cache", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--year-start", type=int, default=1997)
    p.add_argument("--year-end", type=int, default=2025)
    p.add_argument("--position-group", choices=["QB", "RB", "WR", "TE", "K", "DEF", "IDP"])
    p.add_argument("--active-cache", type=Path)
    p.add_argument("--player-id")
    p.add_argument("--week", type=int)
    p.add_argument("--player-bucket", type=int)
    p.add_argument("--player-buckets", type=int, default=1)
    p.add_argument("--threads", type=int)
    p.add_argument("--memory-limit-mb", type=int, default=30000)
    p.add_argument("--reuse-player-buckets", action="store_true")
    a = p.parse_args()
    build(a.snapshot, a.ops_cache, a.output, a.year_start, a.year_end,
          position_group=a.position_group, active_cache=a.active_cache,
          player_id=a.player_id, week=a.week,
          player_bucket=a.player_bucket, player_buckets=a.player_buckets,
          threads=a.threads, memory_limit_mb=a.memory_limit_mb,
          reuse_player_buckets=a.reuse_player_buckets)


if __name__ == "__main__":
    main()
