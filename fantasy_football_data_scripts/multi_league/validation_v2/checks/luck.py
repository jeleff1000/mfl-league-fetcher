"""Luck checks — 17 checks verifying schedule-shuffle and luck data in the matchup table.

These validate shuffle win totals, opponent-shuffle columns, derived luck metrics,
and the presence of pre-aggregated H2H and schedule-swap tables.

Checks are a mix of sql_expr (batched SUM/CASE on matchup) and sql_full
(standalone queries that need self-joins or subqueries).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Shuffle columns — sql_expr checks (batched, quality)
    # ------------------------------------------------------------------
    # 1. luck_shuffle_avg_wins_populated
    #    shuffle_avg_wins should be populated on regular-season rows.
    Check(
        name="luck_shuffle_avg_wins_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_avg_wins NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN shuffle_avg_wins IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. luck_shuffle_avg_wins_reasonable
    #    shuffle_avg_wins must stay within league-appropriate bounds:
    #      - H2H only:      0..20 (17-week season, small safety margin)
    #      - H2H + median:  0..40 (each team earns up to 2 wins/week)
    #    Joined to league_settings per league-year so format changes
    #    mid-league are respected.
    Check(
        name="luck_shuffle_avg_wins_reasonable",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_avg_wins out of league-appropriate range",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, uses_median "
            "  FROM {table_prefix}league_settings"
            ") s ON m.db_name = s.db_name AND m.year = s.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.shuffle_avg_wins IS NOT NULL "
            "  AND ("
            "    m.shuffle_avg_wins < 0 "
            "    OR m.shuffle_avg_wins > (CASE WHEN s.uses_median = true THEN 40 ELSE 20 END)"
            "  ) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 3. luck_shuffle_win_populated
    #    shuffle_0_win (representative check) should be populated on regular-season rows.
    Check(
        name="luck_shuffle_win_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_0_win NULL on regular-season row (representative shuffle check)",
        sql_expr=(
            "SUM(CASE WHEN shuffle_0_win IS NULL " "AND is_playoffs = 0 " "AND is_bye_week = 0 " "" "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 4. luck_shuffle_win_is_probability
    #    shuffle_N_win columns are probabilities (0-100 scale). shuffle_0_win
    #    represents P(final wins = 0) across schedule permutations, NOT a win count.
    #    Must be in [0, 100].
    Check(
        name="luck_shuffle_win_is_probability",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_0_win out of [0, 100] probability range",
        sql_expr=(
            "SUM(CASE WHEN shuffle_0_win IS NOT NULL "
            "AND (shuffle_0_win < 0 OR shuffle_0_win > 100) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Opp shuffle columns — sql_expr/sql_full checks (quality)
    # ------------------------------------------------------------------
    # 6. luck_opp_shuffle_populated
    #    opp_shuffle_0_win should be populated on regular-season rows.
    Check(
        name="luck_opp_shuffle_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="opp_shuffle_0_win NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN opp_shuffle_0_win IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. luck_opp_shuffle_win_is_probability
    #    opp_shuffle_N_win columns are probabilities (0-100 scale), not win counts.
    #    Must be in [0, 100].
    Check(
        name="luck_opp_shuffle_win_is_probability",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="opp_shuffle_0_win out of [0, 100] probability range",
        sql_expr=(
            "SUM(CASE WHEN opp_shuffle_0_win IS NOT NULL "
            "AND (opp_shuffle_0_win < 0 OR opp_shuffle_0_win > 100) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 9. luck_opp_shuffle_avg_wins_populated
    #    opp_shuffle_avg_wins should be populated on regular-season rows.
    Check(
        name="luck_opp_shuffle_avg_wins_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="opp_shuffle_avg_wins NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN opp_shuffle_avg_wins IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Derived columns — sql_expr checks (quality)
    # ------------------------------------------------------------------
    # 10. luck_shuffle_avg_seed_populated
    #     shuffle_avg_seed should be populated on regular-season rows.
    Check(
        name="luck_shuffle_avg_seed_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_avg_seed NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN shuffle_avg_seed IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 11. luck_wins_vs_shuffle_populated
    #     wins_vs_shuffle_wins should be populated on regular-season rows.
    Check(
        name="luck_wins_vs_shuffle_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="wins_vs_shuffle_wins NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN wins_vs_shuffle_wins IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 12. luck_wins_vs_opp_shuffle_populated
    #     wins_vs_opp_shuffle_wins should be populated on regular-season rows.
    Check(
        name="luck_wins_vs_opp_shuffle_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="wins_vs_opp_shuffle_wins NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN wins_vs_opp_shuffle_wins IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 13. luck_seed_vs_shuffle_populated
    #     seed_vs_shuffle_seed should be populated on regular-season rows.
    Check(
        name="luck_seed_vs_shuffle_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="seed_vs_shuffle_seed NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN seed_vs_shuffle_seed IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 14. luck_opp_pts_week_pct_populated
    #     opp_pts_week_pct should be populated on regular-season rows.
    Check(
        name="luck_opp_pts_week_pct_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="opp_pts_week_pct NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN opp_pts_week_pct IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Pre-aggregated tables existence — sql_full checks (quality)
    # ------------------------------------------------------------------
    # 15. luck_h2h_season_exists
    #     h2h_season should have all-play matrix data for any league with
    #     matchup data. This is intentionally different from
    #     matchup_h2h_season, which is the actual schedule H2H record.
    Check(
        name="luck_h2h_season_exists",
        page="simulations",
        table="h2h_season",
        severity="WARNING",
        description="h2h_season has no all-play data for league (needed for Luck H2H matrix)",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}h2h_season"
            ") h ON m.db_name = h.db_name "
            "WHERE h.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
    ),
    # 16. luck_schedule_swap_exists
    #     schedule_swap_season should have data for leagues with shuffle data.
    #     Uses a graceful existence check — table may not exist in all deployments.
    Check(
        name="luck_schedule_swap_exists",
        page="simulations",
        table="schedule_swap_season",
        severity="WARNING",
        description="schedule_swap_season has no data for league (needed for schedule luck)",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND shuffle_avg_wins IS NOT NULL"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}schedule_swap_season"
            ") s ON m.db_name = s.db_name "
            "WHERE s.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
    ),
    # 17. luck_shuffle_avg_playoffs_populated
    #     shuffle_avg_playoffs should be populated where shuffle data is present.
    Check(
        name="luck_shuffle_avg_playoffs_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="shuffle_avg_playoffs NULL where shuffle_avg_wins is populated",
        sql_expr=(
            "SUM(CASE WHEN shuffle_avg_playoffs IS NULL "
            "AND shuffle_avg_wins IS NOT NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    Check(
        name="luck_h2h_season_matches_all_play_rollup",
        page="simulations",
        table="h2h_season",
        severity="WARNING",
        description="h2h_season does not match grouped all_play records",
        sql_full=(
            "WITH expected AS ("
            "  SELECT "
            "    db_name, "
            "    franchise_id, "
            "    opponent_franchise_id, "
            "    year, "
            "    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins, "
            "    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses, "
            "    SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties, "
            "    COUNT(*) AS games "
            "  FROM {table_prefix}all_play "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, franchise_id, opponent_franchise_id, year"
            "), actual AS ("
            "  SELECT "
            "    db_name, "
            "    franchise_id, "
            "    opponent_franchise_id, "
            "    year, "
            "    wins, "
            "    losses, "
            "    ties, "
            "    games "
            "  FROM {table_prefix}h2h_season "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(a.db_name, e.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.opponent_franchise_id = a.opponent_franchise_id "
            " AND e.year = a.year "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(a.wins, -1) != COALESCE(e.wins, -1) "
            "   OR COALESCE(a.losses, -1) != COALESCE(e.losses, -1) "
            "   OR COALESCE(a.ties, -1) != COALESCE(e.ties, -1) "
            "   OR COALESCE(a.games, -1) != COALESCE(e.games, -1) "
            "GROUP BY COALESCE(a.db_name, e.db_name)"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    Check(
        name="luck_schedule_swap_season_matches_rollup",
        page="simulations",
        table="schedule_swap_season",
        severity="WARNING",
        description="schedule_swap_season does not match grouped schedule_swap records",
        sql_full=(
            "WITH expected AS ("
            "  SELECT "
            "    db_name, "
            "    franchise_id, "
            "    schedule_of_franchise_id, "
            "    year, "
            "    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins, "
            "    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses, "
            "    SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties, "
            "    COUNT(*) AS games "
            "  FROM {table_prefix}schedule_swap "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, franchise_id, schedule_of_franchise_id, year"
            "), actual AS ("
            "  SELECT "
            "    db_name, "
            "    franchise_id, "
            "    schedule_of_franchise_id, "
            "    year, "
            "    wins, "
            "    losses, "
            "    ties, "
            "    games "
            "  FROM {table_prefix}schedule_swap_season "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(a.db_name, e.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.schedule_of_franchise_id = a.schedule_of_franchise_id "
            " AND e.year = a.year "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(a.wins, -1) != COALESCE(e.wins, -1) "
            "   OR COALESCE(a.losses, -1) != COALESCE(e.losses, -1) "
            "   OR COALESCE(a.ties, -1) != COALESCE(e.ties, -1) "
            "   OR COALESCE(a.games, -1) != COALESCE(e.games, -1) "
            "GROUP BY COALESCE(a.db_name, e.db_name)"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
]
