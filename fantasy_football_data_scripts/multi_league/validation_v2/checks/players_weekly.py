"""Player weekly checks — 25 checks verifying player_fantasy table integrity.

Checks are a mix of sql_expr (batched SUM/CASE on player_fantasy) and sql_full
(standalone queries that need self-joins or subqueries).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check


def _def_return_yard_points_cap(alias: str) -> str:
    """Higher DEF sanity cap for leagues that award special-teams return yards."""
    return f"(100 + (COALESCE({alias}.scoring_def_st_yd, 0) * 200))"


def _def_return_yard_ppg_cap(alias: str) -> str:
    """Higher DEF PPG cap for leagues that award special-teams return yards."""
    return f"(50 + (COALESCE({alias}.scoring_def_st_yd, 0) * 40))"


CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core batch — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 1. players_player_week_not_null
    #    player_week is derived as {NFL_player_id}_{year}_{week}, so rows
    #    without an NFL_player_id legitimately have NULL player_week (HC
    #    positions, unresolved placeholder players, etc.). Only flag rows
    #    that DO have an NFL_player_id but are still missing player_week —
    #    those are real bugs.
    Check(
        name="players_player_week_not_null",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="player_week IS NULL on row with resolved NFL_player_id",
        sql_expr=(
            "SUM(CASE WHEN player_week IS NULL "
            "AND NFL_player_id IS NOT NULL "
            "AND NFL_player_id NOT LIKE 'ESPN-%' "
            "AND NFL_player_id NOT LIKE 'SLP-%' "
            "AND NFL_player_id NOT LIKE 'PICK_%' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. players_player_week_format
    #    player_week must be a non-empty string.
    Check(
        name="players_player_week_format",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="player_week is empty string (format error)",
        sql_expr="SUM(CASE WHEN player_week IS NOT NULL AND TRIM(player_week) = '' THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 3. players_nfl_id_coverage
    #    At least 90% of rostered players must have NFL_player_id populated.
    Check(
        name="players_nfl_id_coverage",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="<90% of rostered players have NFL_player_id",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN NFL_player_id IS NULL THEN 1 ELSE 0 END) AS null_ids, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND manager IS NOT NULL "
            "    AND TRIM(COALESCE(manager, '')) != '' "
            "    AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  GROUP BY db_name "
            "  HAVING total > 0 AND CAST(null_ids AS DOUBLE) / total > 0.10"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 4. players_fantasy_points_populated
    #    fantasy_points must not be NULL for started players with a real NFL_player_id.
    #    Placeholder IDs (ESPN-, SLP-, PICK-) are unresolved players where stats
    #    aren't available — these legitimately have NULL fantasy_points.
    Check(
        name="players_fantasy_points_populated",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="fantasy_points NULL for started player with resolved NFL_player_id",
        sql_expr=(
            "SUM(CASE WHEN fantasy_points IS NULL AND is_started = 1 "
            "AND NFL_player_id IS NOT NULL "
            "AND NFL_player_id NOT LIKE 'ESPN-%' "
            "AND NFL_player_id NOT LIKE 'SLP-%' "
            "AND NFL_player_id NOT LIKE 'PICK_%' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 5. players_is_started_binary
    #    is_started must be 0 or 1 (binary flag).
    Check(
        name="players_is_started_binary",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="is_started NOT IN (0, 1)",
        sql_expr="SUM(CASE WHEN is_started NOT IN (0, 1) THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. players_year_range
    #    Year must be 2000 or later and not more than 1 year in the future.
    Check(
        name="players_year_range",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="year < 2000 or > current_year + 1 (bad data)",
        sql_expr=("SUM(CASE WHEN year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. players_week_range
    #    Week must be between 1 and 22.
    Check(
        name="players_week_range",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="week < 1 or > 22 (out of valid range)",
        sql_expr="SUM(CASE WHEN week < 1 OR week > 22 THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 8. players_duplicate_player_week
    #    Duplicate (db_name, player_week, franchise_id) rows are bad joins.
    Check(
        name="players_duplicate_player_week",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="Duplicate (db_name, player_week, franchise_id) rows in player_fantasy",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, player_week, franchise_id, COUNT(*) AS cnt "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND player_week IS NOT NULL "
            "    AND franchise_id IS NOT NULL "
            "  GROUP BY db_name, player_week, franchise_id "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 9. players_ranking_starts_at_one
    #    position_week_rank = 0 means ranks are 0-indexed (pipeline bug).
    Check(
        name="players_ranking_starts_at_one",
        page="players",
        table="player_fantasy",
        severity="ERROR",
        description="position_week_rank = 0 (ranks should start at 1)",
        sql_expr=("SUM(CASE WHEN position_week_rank = 0 THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality batch — sql_expr checks
    # ------------------------------------------------------------------
    # 10. players_started_bench_consistent
    #     A player on BN or IR should not have is_started=1.
    Check(
        name="players_started_bench_consistent",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="fantasy_position IN ('BN','IR') but is_started=1",
        sql_expr=("SUM(CASE WHEN fantasy_position IN ('BN', 'IR') AND is_started = 1 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 11. players_fantasy_position_populated
    #     Rostered players should have a fantasy_position.
    #     Pre-2019 ESPN API didn't return fantasy_position for started
    #     players (only bench). Scope to league-years where the column is
    #     populated for at least some STARTED rows (not just bench) —
    #     legitimate gap years get excluded.
    Check(
        name="players_fantasy_position_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="fantasy_position NULL for rostered player",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "WHERE db_name IN ({league_list}) "
            "  AND fantasy_position IS NULL "
            "  AND COALESCE(pf.position, '') != 'STUB' "
            "  AND manager IS NOT NULL "
            "  AND TRIM(COALESCE(manager, '')) != '' "
            "  AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  AND (pf.db_name, pf.year) IN ("
            "    SELECT db_name, year "
            "    FROM {table_prefix}player_fantasy "
            "    WHERE fantasy_position IS NOT NULL "
            "      AND is_started = 1 "
            "    GROUP BY db_name, year "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 12. players_negative_points_non_def
    #     Non-DEF players with very negative fantasy_points are likely bad data.
    #     IDP leagues use -30 threshold (IDP defenders can legitimately score
    #     negative on bad games); standard leagues use -20 (allows deep-negative
    #     QB games with heavy interception/fumble/sack penalties in custom scoring).
    Check(
        name="players_negative_points_non_def",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="fantasy_points extremely negative for non-DEF player (likely bad data)",
        sql_full=(
            "SELECT pf.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "WHERE pf.db_name IN ({league_list}) "
            "  AND pf.fantasy_points IS NOT NULL "
            "  AND UPPER(COALESCE(pf.position, '')) != 'DEF' "
            "  AND pf.manager IS NOT NULL "
            "  AND TRIM(COALESCE(pf.manager, '')) != '' "
            "  AND LOWER(TRIM(pf.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  AND pf.fantasy_points < CASE "
            "    WHEN EXISTS ("
            "      SELECT 1 FROM {table_prefix}league_settings ls "
            "      WHERE ls.db_name = pf.db_name AND ls.year = pf.year "
            "        AND (COALESCE(ls.roster_DB, 0) + COALESCE(ls.roster_DL, 0) "
            "             + COALESCE(ls.roster_LB, 0) + COALESCE(ls.roster_IDP, 0)) > 0"
            "    ) THEN -30 "
            "    ELSE -15 "
            "  END "
            "GROUP BY pf.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 13. players_extreme_points
    #     Unusually high fantasy_points may signal a scoring bug.
    #     IDP leagues use 150 threshold (IDP defenders with many tackles/sacks
    #     can legitimately score high). DEF rows in leagues that explicitly
    #     score special-teams return yards need a higher ceiling as well.
    Check(
        name="players_extreme_points",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="fantasy_points unusually high (possible scoring bug)",
        sql_full=(
            "SELECT pf.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "LEFT JOIN {table_prefix}league_settings ls "
            "  ON ls.db_name = pf.db_name "
            "  AND ls.year = pf.year "
            "WHERE pf.db_name IN ({league_list}) "
            "  AND pf.fantasy_points IS NOT NULL "
            "  AND pf.fantasy_points > CASE "
            "    WHEN (COALESCE(ls.roster_DB, 0) + COALESCE(ls.roster_DL, 0) "
            "          + COALESCE(ls.roster_LB, 0) + COALESCE(ls.roster_IDP, 0)) > 0 "
            "      THEN 150 "
            f"    WHEN COALESCE(pf.position, '') = 'DEF' THEN {_def_return_yard_points_cap('ls')} "
            "    ELSE 100 "
            "  END "
            "GROUP BY pf.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 14. players_one_manager_per_week
    #     The same NFL player should not appear on two rosters in the same week.
    Check(
        name="players_one_manager_per_week",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="Same NFL_player_id on 2+ managers in same week",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, week, NFL_player_id, COUNT(DISTINCT manager) AS mgr_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND NFL_player_id IS NOT NULL "
            "    AND COALESCE(is_bye_week, 0) = 0 "
            "    AND manager IS NOT NULL "
            "    AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  GROUP BY db_name, year, week, NFL_player_id "
            "  HAVING COUNT(DISTINCT manager) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 15. players_position_variety
    #     If all started players have the same position, data is likely wrong.
    Check(
        name="players_position_variety",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="All started players have the same position (likely bad data)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, COUNT(DISTINCT position) AS pos_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_started = 1 "
            "    AND position IS NOT NULL "
            "  GROUP BY db_name "
            "  HAVING COUNT(DISTINCT position) <= 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 16. players_manager_lamar_populated
    #     manager_lamar should be populated for started players.
    #     Exclude placeholder IDs (ESPN-, SLP-, PICK_) — those are the
    #     "unresolved" bucket where the real NFL_player_id couldn't be
    #     matched; the companion backfill row (is_started=0) carries the
    #     LAMAR data via the real NFL_player_id.
    Check(
        name="players_manager_lamar_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="manager_lamar NULL for started (is_started=1) player",
        sql_expr=(
            "SUM(CASE WHEN manager_lamar IS NULL AND is_started = 1 "
            "AND NFL_player_id IS NOT NULL "
            "AND NFL_player_id NOT LIKE 'ESPN-%' "
            "AND NFL_player_id NOT LIKE 'SLP-%' "
            "AND NFL_player_id NOT LIKE 'PICK_%' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 17. players_player_lamar_nonzero
    #     In a full import, all player_lamar = 0 means LAMAR enrichment failed.
    Check(
        name="players_player_lamar_nonzero",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="All player_lamar = 0 (LAMAR enrichment likely failed)",
        feature="full_import",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN player_lamar = 0 OR player_lamar IS NULL THEN 1 ELSE 0 END) AS zero_count, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_started = 1 "
            "  GROUP BY db_name "
            "  HAVING total > 0 AND zero_count = total"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 18. players_clutch_equity_populated
    #     clutch_equity should be populated for started players.
    #     Exclude placeholder IDs (ESPN-, SLP-, PICK_) and league-years
    #     where playoff odds haven't meaningfully run (>= 50% p_champ
    #     coverage in that year — a single stray row doesn't count).
    #     Also requires NFL_player_id to be populated: the clutch calc
    #     joins on NFL_player_id as the identity key, so a row with
    #     NULL NFL_player_id is an identity-resolution failure (already
    #     tracked by `players_nfl_id_coverage`), not a clutch-calc bug.
    Check(
        name="players_clutch_equity_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="clutch_equity NULL for started (is_started=1) player with resolved NFL_player_id",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "WHERE db_name IN ({league_list}) "
            "  AND clutch_equity IS NULL "
            "  AND is_started = 1 "
            "  AND COALESCE(position, '') NOT IN ('HC') "
            "  AND NFL_player_id IS NOT NULL "
            "  AND NFL_player_id NOT LIKE 'ESPN-%' "
            "  AND NFL_player_id NOT LIKE 'SLP-%' "
            "  AND NFL_player_id NOT LIKE 'PICK_%' "
            "  AND (pf.db_name, pf.year) IN ("
            "    SELECT db_name, year "
            "    FROM {table_prefix}matchup "
            "    GROUP BY db_name, year "
            "    HAVING SUM(CASE WHEN p_champ IS NOT NULL THEN 1 ELSE 0 END) * 2 >= COUNT(*) "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 19. players_ppg_populated
    #     season_ppg should be populated for rostered players who actually scored.
    #     Players with 0 fantasy_points every week legitimately have NULL season_ppg.
    #     Requires NFL_player_id to be populated: the season_ppg calc joins
    #     on (NFL_player_id, year), so NULL NFL_player_id is an identity-
    #     resolution failure (already tracked by `players_nfl_id_coverage`),
    #     not a season_ppg-calc bug.
    Check(
        name="players_ppg_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="season_ppg NULL for rostered player with resolved NFL_player_id who scored points",
        sql_expr=(
            "SUM(CASE WHEN season_ppg IS NULL "
            "AND fantasy_points IS NOT NULL AND fantasy_points > 0 "
            "AND NFL_player_id IS NOT NULL "
            "AND manager IS NOT NULL "
            "AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "AND COALESCE(position, '') NOT IN ('HC') "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 20. players_ppg_range
    #     season_ppg must be within a sane range.
    #     DEF rows in leagues with special-teams return-yard scoring get a
    #     higher ceiling because their legitimate weekly highs are much larger.
    Check(
        name="players_ppg_range",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="season_ppg outside league-aware sane range",
        sql_full=(
            "SELECT pf.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "LEFT JOIN {table_prefix}league_settings ls "
            "  ON ls.db_name = pf.db_name "
            "  AND ls.year = pf.year "
            "WHERE pf.db_name IN ({league_list}) "
            "  AND pf.season_ppg IS NOT NULL "
            "  AND ("
            "    pf.season_ppg < -10 "
            "    OR pf.season_ppg > CASE "
            f"      WHEN COALESCE(pf.position, '') = 'DEF' THEN {_def_return_yard_ppg_cap('ls')} "
            "      ELSE 50 "
            "    END"
            "  ) "
            "GROUP BY pf.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 21. players_ranking_columns_exist
    #     position_week_rank should be populated for started players.
    Check(
        name="players_ranking_columns_exist",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="position_week_rank NULL for all started players (ranking enrichment failed)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN position_week_rank IS NULL THEN 1 ELSE 0 END) AS null_rank, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_started = 1 "
            "  GROUP BY db_name "
            "  HAVING total > 0 AND null_rank = total"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 22. players_matchup_context_populated
    #     team_points (matchup context) should be populated for started players.
    #     Bye weeks are excluded — team had no game, so team_points is legitimately NULL.
    Check(
        name="players_matchup_context_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="team_points NULL on started player (non-bye week)",
        sql_expr=(
            "SUM(CASE WHEN team_points IS NULL AND is_started = 1 " "AND opponent IS NOT NULL " "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 23. players_lamar_started_only
    #     manager_lamar should not be populated for bench (is_started=0) players.
    Check(
        name="players_lamar_started_only",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="manager_lamar IS NOT NULL for bench (is_started=0) player",
        sql_expr=("SUM(CASE WHEN manager_lamar IS NOT NULL AND is_started = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 24. players_pf_playoff_flags_match_matchup
    #     is_playoffs on player_fantasy should align with matchup table.
    Check(
        name="players_pf_playoff_flags_match_matchup",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="is_playoffs on player_fantasy inconsistent with matchup table",
        sql_full=(
            "SELECT pf.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "JOIN {table_prefix}matchup m "
            "  ON pf.db_name = m.db_name "
            "  AND pf.year = m.year "
            "  AND pf.week = m.week "
            "  AND pf.franchise_id = m.franchise_id "
            "WHERE pf.db_name IN ({league_list}) "
            "  AND pf.is_started = 1 "
            "  AND pf.is_playoffs IS NOT NULL "
            "  AND m.is_playoffs IS NOT NULL "
            "  AND pf.is_playoffs != m.is_playoffs "
            "GROUP BY pf.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 25. players_pf_median_flags_populated
    #     above_league_median should be populated for median leagues.
    Check(
        name="players_pf_median_flags_populated",
        page="players",
        table="player_fantasy",
        severity="WARNING",
        description="above_league_median NULL on player_fantasy row (median league)",
        feature="median",
        sql_expr=(
            "SUM(CASE WHEN above_league_median IS NULL "
            "AND is_started = 1 "
            "AND team_points IS NOT NULL "
            "AND opponent IS NOT NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
]
