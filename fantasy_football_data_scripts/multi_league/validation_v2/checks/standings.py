"""Standings checks — 11 checks verifying matchup_season integrity and
placement/seeding data.

Checks are a mix of sql_expr (batched) and sql_full (standalone).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core — sql_full checks (need aggregation or cross-table)
    # ------------------------------------------------------------------
    # 1. standings_win_loss_tie_sum
    #    For each manager-year, wins + losses + ties must equal games (H2H)
    #    or games + above_league_median + below_league_median (H2H+Median).
    #    H2H+Median years include median outcomes in wins/losses.
    Check(
        name="standings_win_loss_tie_sum",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="wins + losses + ties != games (or games + median outcomes) for a manager-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup_season "
            "WHERE db_name IN ({league_list}) "
            "  AND games IS NOT NULL "
            "  AND wins + losses + COALESCE(ties, 0) != games "
            "  AND wins + losses + COALESCE(ties, 0) != games "
            "    + COALESCE(above_league_median, 0) "
            "    + COALESCE(below_league_median, 0) "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_season_agg"],
        fix_action="reimport",
    ),
    # 2. standings_records_consistent
    #    matchup_season internal consistency: same dual-formula as check 1.
    Check(
        name="standings_records_match_weekly",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="matchup_season wins + losses + ties inconsistent with games",
        sql_full=(
            "SELECT ms.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup_season ms "
            "WHERE ms.db_name IN ({league_list}) "
            "  AND ms.games IS NOT NULL AND ms.games > 0 "
            "  AND ms.wins + ms.losses + COALESCE(ms.ties, 0) != ms.games "
            "  AND ms.wins + ms.losses + COALESCE(ms.ties, 0) != ms.games "
            "    + COALESCE(ms.above_league_median, 0) "
            "    + COALESCE(ms.below_league_median, 0) "
            "GROUP BY ms.db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_season_agg"],
        fix_action="reimport",
    ),
    # 3. standings_final_seed_unique
    #    Each playoff seed should appear at most once per league-year.
    #    NOTE: ___leagues DDL uses playoff_seed_to_date (not final_playoff_seed).
    Check(
        name="standings_final_seed_unique",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="Duplicate playoff_seed_to_date values within a league-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, playoff_seed_to_date, COUNT(*) AS cnt "
            "  FROM {table_prefix}matchup_season "
            "  WHERE db_name IN ({league_list}) "
            "    AND playoff_seed_to_date IS NOT NULL "
            "  GROUP BY db_name, year, playoff_seed_to_date "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_season_agg"],
        fix_action="reimport",
    ),
    # 4: REMOVED — playoff_result is a descriptive string ("Lost semifinal") shared
    # by multiple managers. Uniqueness check was wrong — it's not a placement rank.
    # ------------------------------------------------------------------
    # ERROR: Core — sql_expr checks (batched on matchup_season)
    # ------------------------------------------------------------------
    # 5. standings_final_seed_populated
    #    playoff_seed_to_date must be populated for all manager-years
    #    (completed seasons have seeds).
    #    NOTE: ___leagues DDL uses playoff_seed_to_date (not final_playoff_seed).
    Check(
        name="standings_final_seed_populated",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="playoff_seed_to_date NULL for a completed season manager-year",
        sql_expr=("SUM(CASE WHEN playoff_seed_to_date IS NULL " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. standings_franchise_id_populated
    #    franchise_id must be populated on every matchup_season row.
    Check(
        name="standings_franchise_id_populated",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="franchise_id NULL in matchup_season",
        sql_expr=("SUM(CASE WHEN franchise_id IS NULL " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. standings_above_median_populated
    #    above_league_median must be populated for median leagues.
    Check(
        name="standings_above_median_populated",
        page="standings",
        table="matchup_season",
        severity="ERROR",
        description="above_league_median NULL in matchup_season (median league)",
        sql_expr=("SUM(CASE WHEN above_league_median IS NULL " "THEN 1 ELSE 0 END)"),
        feature="median",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks
    # ------------------------------------------------------------------
    # 8. standings_placement_rank_populated
    #    playoff_result should be populated for any franchise that participated in
    #    a playoff OR consolation week. Large leagues (16+ teams) often have
    #    non-playoff teams that don't enter any consolation bracket either —
    #    those legitimately have NULL playoff_result and must be excluded.
    #    NOTE: ___leagues DDL uses playoff_result on matchup_season.
    Check(
        name="standings_placement_rank_populated",
        page="standings",
        table="matchup_season",
        severity="WARNING",
        description="playoff_result NULL for franchise that participated in a bracket",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT ms.db_name, ms.year, ms.franchise_id "
            "  FROM {table_prefix}matchup_season ms "
            "  WHERE ms.db_name IN ({league_list}) "
            "    AND ms.playoff_result IS NULL "
            "    AND EXISTS ("
            "      SELECT 1 FROM {table_prefix}matchup m "
            "      WHERE m.db_name = ms.db_name "
            "        AND m.year = ms.year "
            "        AND m.franchise_id = ms.franchise_id "
            "        AND (CAST(m.is_playoffs AS INTEGER) = 1 "
            "             OR CAST(m.is_consolation AS INTEGER) = 1) "
            "    )"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 9. standings_above_below_consistent
    #    above_league_median should not exceed games played (would be impossible).
    #    NOTE: ___leagues matchup_season has above_league_median but not below_league_median.
    Check(
        name="standings_above_below_consistent",
        page="standings",
        table="matchup_season",
        severity="WARNING",
        description="above_league_median > games (impossible)",
        sql_expr=(
            "SUM(CASE WHEN above_league_median IS NOT NULL "
            "AND games IS NOT NULL "
            "AND above_league_median > games "
            "THEN 1 ELSE 0 END)"
        ),
        feature="median",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_full checks
    # ------------------------------------------------------------------
    # 10. standings_matchup_season_exists
    #     matchup_season should have data for any league with matchup data.
    Check(
        name="standings_matchup_season_exists",
        page="standings",
        table="matchup_season",
        severity="WARNING",
        description="matchup_season has no data for league",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup_season"
            ") s ON m.db_name = s.db_name "
            "WHERE s.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
    ),
    # 11. standings_matchup_season_cols
    #     matchup_season must contain the required columns.
    Check(
        name="standings_matchup_season_cols",
        page="standings",
        table="matchup_season",
        severity="WARNING",
        description="matchup_season missing required columns (wins/losses/games)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN wins IS NULL AND losses IS NULL THEN 1 ELSE 0 END) AS missing_wl, "
            "    SUM(CASE WHEN games IS NULL THEN 1 ELSE 0 END) AS missing_games "
            "  FROM {table_prefix}matchup_season "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE missing_wl = 0 AND missing_games > 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_season_agg"],
    ),
]
