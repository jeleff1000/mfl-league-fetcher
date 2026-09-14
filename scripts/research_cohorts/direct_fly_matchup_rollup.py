"""Build the narrow, non-LAMAR source relation for matchup research rollups.

This module deliberately owns only the source contract.  LAMAR is served by the
frontend's live super-table join and is not copied into research tables.
"""
from __future__ import annotations

from pathlib import Path


MATCHUP_BUNDLE_TABLES = (
    "research_matchup",
    "research_matchup_weekly",
    "research_matchup_career",
    "research_matchup_4po",
    "research_matchup_4po_weekly",
    "research_matchup_4po_career",
    "research_matchup_8po",
    "research_matchup_8po_weekly",
    "research_matchup_8po_career",
    "research_matchup_adaptive",
    "research_matchup_adaptive_weekly",
    "research_matchup_adaptive_career",
)


def required_matchup_bundle_tables() -> tuple[str, ...]:
    return MATCHUP_BUNDLE_TABLES


def _year_filter(year_start: int, year_end: int, alias: str) -> str:
    return f"{alias}.year BETWEEN {int(year_start)} AND {int(year_end)}"


def build_narrow_weekly_activity_sql(year_start: int, year_end: int) -> str:
    """Return the only weekly super-table projection used for activity presence."""
    return f"""
        SELECT
            s.NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(s.week AS INTEGER) AS week,
            CAST(s.season_type AS VARCHAR) AS season_type,
            CAST(s.player_week AS VARCHAR) AS player_week
        FROM ___ops.nfl_historical.nfl_player_stats_all AS s
        WHERE {_year_filter(year_start, year_end, 's')}
          AND s.NFL_player_id IS NOT NULL
          AND s.week IS NOT NULL
          AND COALESCE(s.season_type, 'REG') = 'REG'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY s.NFL_player_id, s.year, s.week
            ORDER BY s.player_week
        ) = 1
    """.strip()


def build_narrow_season_table_sql(year_start: int, year_end: int) -> str:
    """Return the live season-table projection used after weekly aggregation."""
    return f"""
        SELECT
            s.NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(COALESCE(s.position, s.nfl_position) AS VARCHAR) AS position,
            CAST(s.games_played AS INTEGER) AS games_played,
            s.fpts_4pt_0ppr,
            s.fpts_4pt_half,
            s.fpts_4pt_ppr,
            s.fpts_6pt_0ppr,
            s.fpts_6pt_half,
            s.fpts_6pt_ppr
        FROM ___ops.nfl_historical.player_nfl_season AS s
        WHERE {_year_filter(year_start, year_end, 's')}
    """.strip()


def build_matchup_base_sql(year_start: int, year_end: int) -> str:
    """Return a per-league/player/week source relation for additive rollups.

    The query intentionally carries the cached outcome fields through untouched.
    It does not calculate LAMAR or join a wide super-table relation.
    """
    return f"""
        WITH weekly_player AS (
            {build_narrow_weekly_activity_sql(year_start, year_end)}
        ),
        base AS (
            SELECT
                p.db_name,
                CAST(p.year AS INTEGER) AS year,
                CAST(p.week AS INTEGER) AS week,
                p.NFL_player_id,
                p.position,
                CASE
                    WHEN p.position IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF') THEN p.position
                    WHEN p.position IN ('DL', 'LB', 'DB') THEN 'IDP'
                    ELSE 'IDP'
                END AS pos_grp,
                ls.num_teams AS teams,
                ls.roster_FLX,
                ls.roster_SUPER_FLEX,
                ls.roster_IDP,
                ls.roster_K,
                ls.roster_DEF,
                ls.is_dynasty,
                ls.sleeper_best_ball,
                ls.playoff_start_week,
                ls.playoff_teams,
                p.is_rostered,
                p.is_started,
                p.fantasy_points,
                p.team_points AS player_team_points,
                p.win,
                p.clutch_equity,
                p.is_playoffs,
                p.team_made_playoffs,
                p.final_playoff_seed,
                p.is_championship,
                (s.NFL_player_id IS NOT NULL) AS active_week,
                m.loss,
                m.tie,
                m.opponent_points,
                m.team_points AS matchup_team_points
            FROM ___leagues.public.player_fantasy AS p
            JOIN ___leagues.public.league_settings AS ls
              ON ls.db_name = p.db_name AND ls.year = p.year
            LEFT JOIN weekly_player AS s
              ON s.NFL_player_id = p.NFL_player_id
             AND s.year = p.year
             AND s.week = p.week
            LEFT JOIN ___leagues.public.matchup AS m
              ON m.db_name = p.db_name
             AND m.year = p.year
             AND m.week = p.week
             AND m.manager = p.manager
            WHERE {_year_filter(year_start, year_end, 'p')}
        )
        SELECT
            db_name, year, week, NFL_player_id, pos_grp,
            teams, roster_FLX, roster_SUPER_FLEX, roster_IDP,
            roster_K, roster_DEF, is_dynasty, sleeper_best_ball,
            playoff_start_week, playoff_teams,
            COUNT(*) AS source_rows,
            COUNT(*) FILTER (WHERE is_rostered) AS rostered_rows,
            COUNT(*) FILTER (WHERE is_started) AS started_rows,
            COUNT(*) FILTER (WHERE active_week) AS active_rows,
            SUM(COALESCE(fantasy_points, 0)) FILTER (WHERE is_started) AS points_started,
            SUM(COALESCE(clutch_equity, 0)) FILTER (WHERE is_started) AS clutch_sum,
            COUNT(*) FILTER (WHERE is_playoffs) AS playoff_rows,
            COUNT(*) FILTER (WHERE is_championship) AS championship_rows,
            COUNT(*) FILTER (
                WHERE is_playoffs OR team_made_playoffs = 1 OR final_playoff_seed IS NOT NULL
            ) AS playoff_signal_rows,
            COUNT(*) FILTER (WHERE win = 1) AS wins,
            COUNT(*) FILTER (WHERE loss = 1) AS losses,
            COUNT(*) FILTER (WHERE tie = 1) AS ties,
            SUM(COALESCE(matchup_team_points, player_team_points, 0)) AS team_points
        FROM base
        GROUP BY ALL
    """.strip()


def build_matchup_rollup_sql(year_start: int, year_end: int) -> str:
    """Return the population-wide weekly sufficient-statistics rollup.

    The only per-league relation is the deduplicated ``player_rows`` CTE.  The
    final relation groups across the entire population, so it is not a shard
    assembler and cannot inherit shard-local denominators.
    """
    return f"""
        WITH active_week AS (
            {build_narrow_weekly_activity_sql(year_start, year_end)}
        ),
        keeper_year AS (
            SELECT db_name, year,
                   BOOL_OR(COALESCE(TRY_CAST(is_keeper AS BOOLEAN), FALSE)) AS has_keeper
            FROM public.draft
            WHERE year BETWEEN {int(year_start)} AND {int(year_end)}
            GROUP BY 1, 2
        ),
        settings AS (
            SELECT
                ls.db_name,
                CAST(ls.year AS INTEGER) AS year,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN ls.num_teams <= 11 THEN '10t' ELSE '12t' END AS teams,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(ls.roster_IDP,0) + COALESCE(ls.roster_DL,0)
                        + COALESCE(ls.roster_LB,0) + COALESCE(ls.roster_DB,0)
                        + COALESCE(ls.roster_DB_LB,0) + COALESCE(ls.roster_DL_LB,0) > 0
                       THEN 'idp'
                     WHEN COALESCE(ls.roster_SUPER_FLEX,0) > 0 THEN 'sflx'
                     ELSE 'flx' END AS roster,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(ls.scoring_rec,0) = 0 THEN 'std'
                     WHEN COALESCE(ls.scoring_rec,0) < 0.75 THEN 'half'
                     ELSE 'ppr' END AS ppr,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(ls.scoring_pass_td,4) >= 5 THEN '6pt'
                     ELSE '4pt' END AS td,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN ls.playoff_teams IS NULL THEN 'ALL'
                     WHEN ls.playoff_teams <= 5 THEN '4po'
                     WHEN ls.playoff_teams <= 7 THEN '6po'
                     ELSE '8po' END AS bracket,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(ls.is_dynasty,FALSE) THEN 'dynasty'
                     ELSE 'redraft' END AS league_type,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(ls.sleeper_best_ball,FALSE) THEN 'best_ball'
                     ELSE 'managed' END AS lineup_mode,
                CASE WHEN ls.year BETWEEN 2003 AND 2010 THEN 'ALL'
                     WHEN COALESCE(k.has_keeper,FALSE) THEN 'keeper'
                     ELSE 'non_keeper' END AS keeper_mode,
                ls.roster_K, ls.roster_DEF, ls.roster_IDP,
                ls.roster_DL, ls.roster_LB, ls.roster_DB
            FROM public.league_settings ls
            LEFT JOIN keeper_year k USING (db_name, year)
            WHERE ls.year BETWEEN {int(year_start)} AND {int(year_end)}
        ),
        player_rows AS (
            SELECT
                p.db_name,
                CAST(p.year AS INTEGER) AS year,
                CAST(p.week AS INTEGER) AS week,
                p.NFL_player_id,
                CAST(MAX(p.position) AS VARCHAR) AS position,
                CASE WHEN MAX(p.position) IN ('QB','RB','WR','TE','K','DEF') THEN MAX(p.position)
                     WHEN MAX(p.position) IN ('DL','LB','DB') THEN 'IDP'
                     ELSE 'IDP' END AS pos_grp,
                s.teams, s.roster, s.ppr, s.td, s.bracket,
                s.league_type, s.lineup_mode, s.keeper_mode,
                s.roster_K, s.roster_DEF, s.roster_IDP, s.roster_DL,
                s.roster_LB, s.roster_DB,
                MAX(CAST(p.is_started AS INTEGER)) AS is_started,
                MAX(CAST(p.is_rostered AS INTEGER)) AS is_rostered,
                MAX(CAST(COALESCE(a.NFL_player_id IS NOT NULL, FALSE) AS INTEGER)) AS active_week,
                MAX(p.fantasy_points) AS fantasy_points,
                MAX(p.win) AS win, MAX(p.loss) AS loss,
                MAX(p.clutch_equity) AS clutch_equity,
                MAX(CAST(p.is_playoffs AS INTEGER)) AS is_playoffs,
                MAX(p.is_championship) AS is_championship,
                MAX(p.team_made_playoffs) AS team_made_playoffs,
                MAX(p.final_playoff_seed) AS final_playoff_seed
            FROM public.player_fantasy p
            JOIN settings s ON s.db_name = p.db_name AND s.year = p.year
            LEFT JOIN active_week a
              ON a.NFL_player_id = p.NFL_player_id
             AND a.year = p.year
             AND a.week = p.week
            WHERE p.year BETWEEN {int(year_start)} AND {int(year_end)}
              AND p.NFL_player_id IS NOT NULL
              AND p.week IS NOT NULL
              AND CAST(p.is_rostered AS INTEGER) = 1
            GROUP BY ALL
        ),
        eligible AS (
            SELECT
                r.teams, r.roster, r.ppr, r.td, r.bracket,
                r.league_type, r.lineup_mode, r.keeper_mode,
                r.year, r.week, r.pos_grp,
                COUNT(DISTINCT r.db_name) AS n_leagues
            FROM player_rows r
            WHERE r.active_week = 1
              AND (r.pos_grp NOT IN ('K','DEF') OR
                   (r.pos_grp = 'K' AND COALESCE(r.roster_K,0) > 0) OR
                   (r.pos_grp = 'DEF' AND COALESCE(r.roster_DEF,0) > 0))
            GROUP BY ALL
        ),
        numerators AS (
            SELECT
                teams, roster, ppr, td, bracket,
                league_type, lineup_mode, keeper_mode,
                year, week, NFL_player_id, pos_grp,
                COUNT(DISTINCT db_name) AS rostered_leagues,
                COUNT(DISTINCT db_name) FILTER (WHERE is_started=1) AS started_leagues,
                COUNT(DISTINCT db_name) FILTER (WHERE is_started=1 AND active_week=1) AS healthy_started_leagues,
                SUM(CASE WHEN is_started=1 AND active_week=1 THEN COALESCE(fantasy_points,0) ELSE 0 END) AS points_started,
                SUM(CASE WHEN is_started=1 AND active_week=1 THEN COALESCE(clutch_equity,0) ELSE 0 END) AS clutch_sum,
                COUNT(DISTINCT db_name) FILTER (WHERE is_started=1 AND win=1) AS wins_started,
                COUNT(DISTINCT db_name) FILTER (WHERE is_started=1 AND loss=1) AS losses_started,
                COUNT(DISTINCT db_name) FILTER (WHERE is_playoffs) AS playoff_leagues,
                COUNT(DISTINCT db_name) FILTER (WHERE is_championship) AS championship_leagues
            FROM player_rows
            GROUP BY ALL
        )
        SELECT
            n.*,
            COALESCE(e.n_leagues, 0) AS n_leagues,
            CASE WHEN COALESCE(e.n_leagues,0) > 0
                 THEN 100.0 * n.rostered_leagues / e.n_leagues END AS roster_rate_pct,
            CASE WHEN COALESCE(e.n_leagues,0) > 0
                 THEN 100.0 * n.started_leagues / e.n_leagues END AS start_rate_pct,
            CASE WHEN COALESCE(e.n_leagues,0) > 0
                 THEN 100.0 * n.healthy_started_leagues / e.n_leagues END AS healthy_start_rate_pct
        FROM numerators n
        LEFT JOIN eligible e USING (
            teams, roster, ppr, td, bracket, league_type, lineup_mode,
            keeper_mode, year, week, pos_grp
        )
    """.strip()


season_super_table_sql = build_narrow_season_table_sql


def validate_rollup_schema(path: Path) -> None:
    """Validate the produced bundle without importing the full publisher."""
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
            ).fetchall()
        }
        missing = set(MATCHUP_BUNDLE_TABLES) - tables
        if missing:
            raise ValueError(f"rollup missing matchup tables: {sorted(missing)}")
        lamar_columns = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT column_name FROM information_schema.columns "
                "WHERE lower(column_name) LIKE 'lamar%'"
            ).fetchall()
        ]
        if lamar_columns:
            raise ValueError(f"rollup must not materialize LAMAR columns: {lamar_columns[:8]}")
    finally:
        con.close()
