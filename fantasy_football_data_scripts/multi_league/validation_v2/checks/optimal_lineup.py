"""Optimal lineup checks — 12 checks verifying optimal lineup data in
player_fantasy and matchup tables.

Checks are a mix of sql_expr (batched SUM/CASE) and sql_full
(standalone queries that need self-joins or subqueries).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Existence — sql_full checks on player_fantasy
    # ------------------------------------------------------------------
    # 1. optimal_exists
    #    player_fantasy must have at least some optimal_player=1 rows.
    Check(
        name="optimal_exists",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="No optimal_player=1 rows in player_fantasy (optimal enrichment missing)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN optimal_player = 1 THEN 1 ELSE 0 END) AS opt_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND COALESCE(is_bye_week, 0) = 0 "
            "  GROUP BY db_name "
            "  HAVING opt_count = 0"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 2. optimal_nonzero_points
    #    optimal_player=1 rows with all-zero fantasy_points are suspicious.
    Check(
        name="optimal_nonzero_points",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="All optimal_player=1 rows have fantasy_points=0 (likely bad data)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN fantasy_points = 0 OR fantasy_points IS NULL THEN 1 ELSE 0 END) AS zero_count, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND optimal_player = 1 "
            "    AND COALESCE(is_bye_week, 0) = 0 "
            "  GROUP BY db_name "
            "  HAVING total > 0 AND zero_count = total"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # ERROR: Core — sql_full checks on player_fantasy
    # ------------------------------------------------------------------
    # 3. optimal_no_duplicates
    #    The same player must not be optimal twice in the same week.
    Check(
        name="optimal_no_duplicates",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="Same player flagged as optimal_player=1 twice in the same week",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, week, NFL_player_id, COUNT(*) AS cnt "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND optimal_player = 1 "
            "    AND COALESCE(is_bye_week, 0) = 0 "
            "    AND NFL_player_id IS NOT NULL "
            "  GROUP BY db_name, year, week, NFL_player_id "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_full checks on player_fantasy
    # ------------------------------------------------------------------
    # 4. optimal_not_all_one_manager
    #    If all optimal players per week come from the same manager,
    #    the optimal lineup calculation is likely wrong.
    Check(
        name="optimal_not_all_one_manager",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="All optimal players in a week from the same manager (calculation suspicious)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, week, COUNT(DISTINCT manager) AS mgr_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND optimal_player = 1 "
            "    AND COALESCE(is_bye_week, 0) = 0 "
            "    AND manager IS NOT NULL "
            "    AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  GROUP BY db_name, year, week "
            "  HAVING COUNT(DISTINCT manager) <= 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 5. optimal_per_week
    #    Every week with fantasy activity should have at least some optimal flags.
    #    NFL weeks with no started players (e.g., weeks 17-22 after fantasy season ends,
    #    or historical years with only unrostered NFL players) legitimately have no optimal.
    Check(
        name="optimal_per_week",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="Weeks with started players but zero optimal_player=1 flags",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, week "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, year, week "
            "  HAVING SUM(CASE WHEN COALESCE(is_bye_week, 0) = 0 AND is_started = 1 THEN 1 ELSE 0 END) > 0 "
            "    AND SUM(CASE WHEN COALESCE(is_bye_week, 0) = 0 AND optimal_player = 1 THEN 1 ELSE 0 END) = 0"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks on matchup
    # ------------------------------------------------------------------
    # 6. optimal_gte_team_points
    #    The optimal lineup score must be >= the sum of actually-started
    #    fantasy_points (`starter_points`, computed in player_to_matchup from
    #    the player_fantasy rows we actually have).
    #
    #    Why `starter_points`, not `team_points`:
    #    `team_points` is the platform-stored team total from Sleeper/ESPN/
    #    Yahoo's server. When the fetcher returns a complete roster, the
    #    invariant `team_points == starter_points` holds and the check is
    #    equivalent. When the fetcher is missing starter rows (ESPN API data
    #    gaps on specific player-weeks — see pigskin 2020 w5 chris where
    #    team_points=103.70 but only 7 starter rows sum to 97.10), comparing
    #    optimal against team_points penalises the optimal algorithm for
    #    upstream data missing from our player_fantasy table. The correct
    #    invariant is "the optimal lineup from the rostered players we have
    #    must be at least the starter lineup we observed for the same
    #    players" — exactly `optimal_points >= starter_points`.
    #
    #    Still scoped out of pre-2019 ESPN rows: their starter_points
    #    aggregate is ALSO unreliable (only a handful of players return per
    #    week) and populate_fantasy_points recomputes from incomplete data,
    #    so the scope mirrors system_team_points_vs_player_sum at 56cfe339.
    #    Bye rows are also excluded. Playoff byes can legitimately carry
    #    starter_points from the roster snapshot while team/opponent totals and
    #    optimal_points remain NULL/0 because no actual matchup occurred.
    Check(
        name="optimal_gte_team_points",
        page="players",
        table="matchup",
        severity="WARNING",
        description="optimal_points < starter_points (optimal cannot be less than actual starters)",
        sql_expr=(
            "SUM(CASE WHEN optimal_points IS NOT NULL "
            "AND starter_points IS NOT NULL "
            "AND optimal_points < starter_points - 0.01 "
            "AND COALESCE(CAST(is_bye_week AS INTEGER), 0) = 0 "
            "AND NOT (platform = 'espn' AND year < 2019) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. optimal_lineup_efficiency_bounds
    #    lineup_efficiency should be between 0 and 200 for standard leagues.
    #    IDP leagues allow up to 300 (IDP defenders can stack many tackle/sack
    #    points, pushing optimal well above the standard ceiling).
    Check(
        name="optimal_lineup_efficiency_bounds",
        page="players",
        table="matchup",
        severity="WARNING",
        description="lineup_efficiency < 0 or above league-appropriate ceiling (IDP: 300, standard: 200)",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.lineup_efficiency IS NOT NULL "
            "  AND (m.lineup_efficiency < -1 OR m.lineup_efficiency > CASE "
            "    WHEN EXISTS ("
            "      SELECT 1 FROM {table_prefix}league_settings ls "
            "      WHERE ls.db_name = m.db_name AND ls.year = m.year "
            "        AND (COALESCE(ls.roster_DB, 0) + COALESCE(ls.roster_DL, 0) "
            "             + COALESCE(ls.roster_LB, 0) + COALESCE(ls.roster_IDP, 0)) > 0"
            "    ) THEN 300 "
            "    ELSE 200 "
            "  END) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 8. optimal_bench_points_nonneg
    #    bench_points is the sum of fantasy_points for rostered-but-not-started
    #    players. It CAN be legitimately negative when a manager benches a
    #    DEF/DST that ends up with negative scoring (e.g., a bench DST gives
    #    up several TDs with no turnovers, -5 sack count, etc. — Sleeper/
    #    ESPN both allow negative team DEF totals, and for Sleeper most of
    #    pigskin/nyu uses pts_def_std which tolerates negatives). We only
    #    care about catching arithmetic bugs in the rollup, so tolerate
    #    small negatives and only flag egregious ones (< -20 points).
    Check(
        name="optimal_bench_points_nonneg",
        page="players",
        table="matchup",
        severity="WARNING",
        description="bench_points much less than zero (indicates rollup bug, not bench DEF)",
        sql_expr="SUM(CASE WHEN bench_points IS NOT NULL AND bench_points < -20 THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: League-wide optimal — sql_full checks on player_fantasy
    # ------------------------------------------------------------------
    # 9. optimal_league_wide_exists
    #    league_wide_optimal_player should be populated somewhere.
    Check(
        name="optimal_league_wide_exists",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="league_wide_optimal_player all NULL (league-wide optimal missing)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN league_wide_optimal_player IS NOT NULL THEN 1 ELSE 0 END) AS lwo_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name "
            "  HAVING lwo_count = 0"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks on player_fantasy
    # ------------------------------------------------------------------
    # 10. optimal_league_wide_position
    #     league_wide_optimal_position must be populated where player is league-wide optimal.
    Check(
        name="optimal_league_wide_position",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="league_wide_optimal_position NULL where league_wide_optimal_player=1",
        sql_expr=(
            "SUM(CASE WHEN league_wide_optimal_player = 1 "
            "AND league_wide_optimal_position IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # ERROR: Core — sql_expr checks on player_fantasy
    # ------------------------------------------------------------------
    # 11. optimal_position_populated
    #     optimal_position must be populated where optimal_player=1.
    Check(
        name="optimal_position_populated",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="optimal_position NULL where optimal_player=1",
        sql_expr=("SUM(CASE WHEN optimal_player = 1 AND optimal_position IS NULL " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 12. optimal_lineup_position_populated
    #     lineup_position should be populated for all started players.
    #     Pre-2019 ESPN API didn't return lineup_position — scope to
    #     league-years where the column is populated for at least some
    #     started rows (legitimate gap years excluded).
    Check(
        name="optimal_lineup_position_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="lineup_position NULL for started (is_started=1) player",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "WHERE db_name IN ({league_list}) "
            "  AND lineup_position IS NULL "
            "  AND is_started = 1 "
            "  AND (pf.db_name, pf.year) IN ("
            "    SELECT db_name, year "
            "    FROM {table_prefix}player_fantasy "
            "    WHERE lineup_position IS NOT NULL AND is_started = 1 "
            "    GROUP BY db_name, year "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
]
