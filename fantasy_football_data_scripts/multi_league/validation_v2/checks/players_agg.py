"""Player aggregation checks — 10 checks verifying player_fantasy_season and
player_fantasy_career table integrity.

Checks are a mix of sql_expr (batched on player_fantasy_season/career) and
sql_full (standalone queries that need aggregation or cross-table joins).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Existence checks — sql_full
    # ------------------------------------------------------------------
    # 1. players_agg_season_exists
    #    player_fantasy_season must have data for any league with player_fantasy.
    Check(
        name="players_agg_season_exists",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description="player_fantasy_season has no data for league",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list})"
            ") pf "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy_season"
            ") pfs ON pf.db_name = pfs.db_name "
            "WHERE pfs.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_player_data"],
        fix_action="reimport",
    ),
    # 2. players_agg_career_exists
    #    player_fantasy_career must have data for multi-year leagues.
    Check(
        name="players_agg_career_exists",
        page="players",
        table="player_fantasy_career",
        severity="WARNING",
        description="player_fantasy_career has no data for multi-year league",
        feature="multi_year",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list})"
            ") pf "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy_career"
            ") pfc ON pf.db_name = pfc.db_name "
            "WHERE pfc.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_player_data", "completeness_has_career_agg"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Quality — sql_expr checks on player_fantasy_season
    # ------------------------------------------------------------------
    # 3. players_agg_season_nfl_id
    #    NFL_player_id must not be NULL in player_fantasy_season.
    Check(
        name="players_agg_season_nfl_id",
        page="players",
        table="player_fantasy_season",
        severity="ERROR",
        description="NFL_player_id NULL in player_fantasy_season",
        sql_expr="SUM(CASE WHEN NFL_player_id IS NULL THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks on player_fantasy_season
    # ------------------------------------------------------------------
    # 4. players_agg_season_managers
    #    managers (plural) column should not be empty for rostered players.
    #    Unrostered/FA rows legitimately have NULL/empty managers.
    Check(
        name="players_agg_season_managers",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description="managers column empty/NULL for rostered player in player_fantasy_season",
        sql_expr=(
            "SUM(CASE WHEN (managers IS NULL OR TRIM(managers) = '') "
            "AND fantasy_points IS NOT NULL AND fantasy_points > 0 "
            "AND LOWER(TRIM(COALESCE(managers, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 5. players_agg_season_points_range
    #    Season fantasy_points > 1000 is likely a double-counting bug.
    Check(
        name="players_agg_season_points_range",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description="fantasy_points > 1000 in player_fantasy_season (possible double-count)",
        sql_expr="SUM(CASE WHEN fantasy_points > 1000 THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 6. players_agg_season_lamar
    #    manager_lamar should be populated in player_fantasy_season.
    Check(
        name="players_agg_season_lamar",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description="manager_lamar NULL in player_fantasy_season",
        sql_expr="SUM(CASE WHEN manager_lamar IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks on player_fantasy_career
    # ------------------------------------------------------------------
    # 7. players_agg_career_managers
    #    managers column should not be NULL for rostered players.
    #    Unrostered/FA rows legitimately have NULL managers.
    Check(
        name="players_agg_career_managers",
        page="players",
        table="player_fantasy_career",
        severity="WARNING",
        description="managers NULL for rostered player in player_fantasy_career",
        sql_expr=(
            "SUM(CASE WHEN managers IS NULL "
            "AND fantasy_points IS NOT NULL AND fantasy_points > 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Cross-table consistency check — sql_full
    # ------------------------------------------------------------------
    # 8. players_agg_season_vs_weekly
    #    Season fantasy_points total should not deviate >10% from weekly sum.
    #    Must compare like-for-like: fantasy_points remains a roster-row total
    #    (started and bench), so the weekly comparison must NOT filter
    #    is_started=1. Outcome/value fields are start-based separately.
    Check(
        name="players_agg_season_vs_weekly",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description=">10% mismatch between player_fantasy_season and weekly sum from player_fantasy",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT pfs.db_name, pfs.NFL_player_id, pfs.year, "
            "    pfs.fantasy_points AS season_pts, "
            "    COALESCE(w.weekly_pts, 0) AS weekly_pts "
            "  FROM {table_prefix}player_fantasy_season pfs "
            "  LEFT JOIN ("
            "    SELECT db_name, NFL_player_id, year, SUM(COALESCE(fantasy_points, 0)) AS weekly_pts "
            "    FROM {table_prefix}player_fantasy "
            "    WHERE ((year >= 2021 AND week <= 18) OR (year < 2021 AND week <= 17)) "
            "    GROUP BY db_name, NFL_player_id, year"
            "  ) w "
            "    ON pfs.db_name = w.db_name "
            "    AND pfs.NFL_player_id = w.NFL_player_id "
            "    AND pfs.year = w.year "
            "  WHERE pfs.db_name IN ({league_list}) "
            "    AND pfs.fantasy_points IS NOT NULL "
            "    AND pfs.fantasy_points > 0.01 "
            "    AND ABS(pfs.fantasy_points - COALESCE(w.weekly_pts, 0)) / pfs.fantasy_points > 0.10"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
    ),
    # 9. players_agg_season_games_match_weekly
    #    Season games_started/games_rostered should exactly match weekly counts.
    Check(
        name="players_agg_season_games_match_weekly",
        page="players",
        table="player_fantasy_season",
        severity="WARNING",
        description="games_started or games_rostered in player_fantasy_season do not match weekly player_fantasy counts",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT pfs.db_name, pfs.NFL_player_id, pfs.year "
            "  FROM {table_prefix}player_fantasy_season pfs "
            "  LEFT JOIN ("
            "    SELECT db_name, NFL_player_id, year, "
            "      SUM(CASE WHEN COALESCE(CAST(is_started AS INTEGER), 0) = 1 THEN 1 ELSE 0 END) AS weekly_games_started, "
            "      COUNT(*) AS weekly_games_rostered "
            "    FROM {table_prefix}player_fantasy "
            "    WHERE ((year >= 2021 AND week <= 18) OR (year < 2021 AND week <= 17)) "
            "    GROUP BY db_name, NFL_player_id, year"
            "  ) w "
            "    ON pfs.db_name = w.db_name "
            "    AND pfs.NFL_player_id = w.NFL_player_id "
            "    AND pfs.year = w.year "
            "  WHERE pfs.db_name IN ({league_list}) "
            "    AND ("
            "      COALESCE(pfs.games_started, 0) != COALESCE(w.weekly_games_started, 0) "
            "      OR COALESCE(pfs.games_rostered, 0) != COALESCE(w.weekly_games_rostered, 0)"
            "    )"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
        depends_on=["completeness_has_player_data", "players_agg_season_exists"],
    ),
    # 10. players_agg_career_matches_season_totals
    #     Career rows should equal the summed season rows for the same player.
    Check(
        name="players_agg_career_matches_season_totals",
        page="players",
        table="player_fantasy_career",
        severity="WARNING",
        description="player_fantasy_career does not reconcile to summed player_fantasy_season totals",
        feature="multi_year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT COALESCE(pfc.db_name, s.db_name) AS db_name, "
            "         COALESCE(pfc.NFL_player_id, s.NFL_player_id) AS NFL_player_id "
            "  FROM {table_prefix}player_fantasy_career pfc "
            "  FULL OUTER JOIN ("
            "    SELECT db_name, NFL_player_id, "
            "      MIN(year) AS first_year, "
            "      MAX(year) AS last_year, "
            "      COUNT(DISTINCT year) AS years_active, "
            "      SUM(COALESCE(fantasy_points, 0)) AS fantasy_points, "
            "      SUM(COALESCE(player_lamar, 0)) AS player_lamar, "
            "      SUM(COALESCE(manager_lamar, 0)) AS manager_lamar, "
            "      SUM(COALESCE(clutch_equity, 0)) AS clutch_equity, "
            "      SUM(COALESCE(games_started, 0)) AS games_started, "
            "      SUM(COALESCE(games_rostered, 0)) AS games_rostered, "
            "      SUM(COALESCE(wins, 0)) AS wins, "
            "      SUM(COALESCE(losses, 0)) AS losses, "
            "      SUM(COALESCE(playoff_games, 0)) AS playoff_games, "
            "      SUM(COALESCE(playoff_wins, 0)) AS playoff_wins, "
            "      SUM(COALESCE(playoff_losses, 0)) AS playoff_losses, "
            "      SUM(COALESCE(championships, 0)) AS championships, "
            "      SUM(COALESCE(optimal_player_count, 0)) AS optimal_player_count, "
            "      SUM(COALESCE(league_wide_optimal_count, 0)) AS league_wide_optimal_count "
            "    FROM {table_prefix}player_fantasy_season "
            "    WHERE db_name IN ({league_list}) "
            "    GROUP BY db_name, NFL_player_id"
            "  ) s "
            "    ON pfc.db_name = s.db_name "
            "    AND pfc.NFL_player_id = s.NFL_player_id "
            "  WHERE COALESCE(pfc.db_name, s.db_name) IN ({league_list}) "
            "    AND ("
            "      pfc.NFL_player_id IS NULL "
            "      OR s.NFL_player_id IS NULL "
            "      OR COALESCE(pfc.first_year, 0) != COALESCE(s.first_year, 0) "
            "      OR COALESCE(pfc.last_year, 0) != COALESCE(s.last_year, 0) "
            "      OR COALESCE(pfc.years_active, 0) != COALESCE(s.years_active, 0) "
            "      OR ABS(COALESCE(pfc.fantasy_points, 0) - COALESCE(s.fantasy_points, 0)) > 0.01 "
            "      OR ABS(COALESCE(pfc.player_lamar, 0) - COALESCE(s.player_lamar, 0)) > 0.01 "
            "      OR ABS(COALESCE(pfc.manager_lamar, 0) - COALESCE(s.manager_lamar, 0)) > 0.01 "
            "      OR ABS(COALESCE(pfc.clutch_equity, 0) - COALESCE(s.clutch_equity, 0)) > 0.01 "
            "      OR COALESCE(pfc.games_started, 0) != COALESCE(s.games_started, 0) "
            "      OR COALESCE(pfc.games_rostered, 0) != COALESCE(s.games_rostered, 0) "
            "      OR COALESCE(pfc.wins, 0) != COALESCE(s.wins, 0) "
            "      OR COALESCE(pfc.losses, 0) != COALESCE(s.losses, 0) "
            "      OR COALESCE(pfc.playoff_games, 0) != COALESCE(s.playoff_games, 0) "
            "      OR COALESCE(pfc.playoff_wins, 0) != COALESCE(s.playoff_wins, 0) "
            "      OR COALESCE(pfc.playoff_losses, 0) != COALESCE(s.playoff_losses, 0) "
            "      OR COALESCE(pfc.championships, 0) != COALESCE(s.championships, 0) "
            "      OR COALESCE(pfc.optimal_player_count, 0) != COALESCE(s.optimal_player_count, 0) "
            "      OR COALESCE(pfc.league_wide_optimal_count, 0) != COALESCE(s.league_wide_optimal_count, 0)"
            "    )"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
        depends_on=["completeness_has_career_agg", "players_agg_season_exists", "players_agg_career_exists"],
    ),
]
