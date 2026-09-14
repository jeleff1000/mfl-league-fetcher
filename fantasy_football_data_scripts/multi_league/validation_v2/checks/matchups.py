"""Matchup checks — 28 checks verifying matchup table integrity.

Checks are a mix of sql_expr (batched SUM/CASE on matchup) and sql_full
(standalone queries that need self-joins or subqueries).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core batch — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 1. matchups_team_points_not_null
    #    Real (non-bye, non-placeholder) rows must have team_points.
    Check(
        name="matchups_team_points_not_null",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="team_points NULL on real (non-bye, non-placeholder) rows",
        sql_expr=(
            "SUM(CASE WHEN team_points IS NULL "
            "AND COALESCE(CAST(is_bye_week AS INTEGER), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. matchups_opponent_points_not_null
    #    Opponent points must be populated on real rows.
    Check(
        name="matchups_opponent_points_not_null",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="opponent_points NULL on real (non-bye) rows",
        sql_expr=("SUM(CASE WHEN opponent_points IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 3. matchups_opponent_populated
    #    Opponent name must be populated on real rows.
    Check(
        name="matchups_opponent_populated",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="opponent NULL on real (non-bye) rows",
        sql_expr=("SUM(CASE WHEN opponent IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 4. matchups_win_loss_aligns_points
    #    A row with win=1 must not have team_points < opponent_points (unless tie).
    Check(
        name="matchups_win_loss_aligns_points",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="win=1 but team_points < opponent_points (and tie=0)",
        sql_expr=("SUM(CASE WHEN win = 1 AND team_points < opponent_points " "AND tie = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 5. matchups_margin_calculation
    #    margin should equal (team_points - opponent_points) within tolerance.
    Check(
        name="matchups_margin_calculation",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="margin != (team_points - opponent_points) by >0.01",
        sql_expr=(
            "SUM(CASE WHEN ABS(margin - (team_points - opponent_points)) > 0.01 "
            "AND team_points IS NOT NULL AND opponent_points IS NOT NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. matchups_bye_not_counted_win
    #    Bye-week rows must not be counted as wins.
    Check(
        name="matchups_bye_not_counted_win",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="is_bye_week=1 row counted as a win",
        sql_expr=("SUM(CASE WHEN is_bye_week = 1 AND win = 1 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. matchups_bye_consistency
    #    Bye rows must have a NULL / empty opponent. A "bye" with an opponent
    #    is a data-consistency contradiction.
    #
    #    Historical note: previously named `matchups_bye_not_playoff`, firing
    #    on `is_bye_week=1 AND is_playoffs=1`. That's wrong — a first-round
    #    playoff bye (the #1/#2 seed resting) is legitimately both
    #    is_bye_week=1 AND is_playoffs=1. `enforce_postseason_flags` sets
    #    both flags on purpose in the sql_byes step
    #    (sql_matchup_enrichments.py, "Flag playoff bye rows"). The old
    #    check fired on every legitimate playoff bye.
    Check(
        name="matchups_bye_consistency",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="is_bye_week=1 row has a non-NULL opponent",
        sql_expr=(
            "SUM(CASE WHEN is_bye_week = 1 "
            "AND opponent IS NOT NULL "
            "AND TRIM(COALESCE(opponent, '')) != '' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 8. matchups_no_future_years
    #    No matchup rows should have a year more than 1 year in the future.
    Check(
        name="matchups_no_future_years",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="matchup rows with year > current_year + 1 (likely bad data)",
        sql_expr=("SUM(CASE WHEN year > EXTRACT(YEAR FROM CURRENT_DATE) + 1 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Core batch — sql_full checks (need self-join or subquery)
    # ------------------------------------------------------------------
    # 9. matchups_symmetry
    #    If A vs B exists in week W, then B vs A must also exist.
    #    Asymmetry means data was written for one side only.
    #
    #    Joins on franchise_id (with opponent_franchise_id as the counterparty)
    #    rather than manager name, because the previous name-based join
    #    false-positived whenever a manager was renamed mid-season or
    #    disambiguated — the opposing row correctly exists but the name join
    #    fails to see it. Name fallback preserved for rows without
    #    franchise_id populated.
    # 9. matchups_symmetry
    #    If A vs B exists in week W, then B vs A must also exist.
    #    Uses NOT EXISTS semi-join on a compact key (franchise_id pairs).
    #    Falls back to manager/opponent names when franchise_id is NULL.
    #    NOT EXISTS short-circuits per row — much faster than the previous
    #    LEFT JOIN which materialized the full cross product.
    Check(
        name="matchups_symmetry",
        page="matchups",
        table="matchup",
        severity="ERROR",
        description="Matchup A vs B exists but B vs A missing (asymmetric)",
        sql_full=(
            "SELECT a.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup a "
            "WHERE a.db_name IN ({league_list}) "
            "  AND COALESCE(a.is_bye_week, 0) = 0 "
            "  AND a.opponent IS NOT NULL "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM {table_prefix}matchup b "
            "    WHERE b.db_name = a.db_name "
            "      AND b.year = a.year "
            "      AND b.week = a.week "
            "      AND (("
            "           a.franchise_id IS NOT NULL "
            "           AND a.opponent_franchise_id IS NOT NULL "
            "           AND b.franchise_id = a.opponent_franchise_id "
            "           AND b.opponent_franchise_id = a.franchise_id"
            "         ) OR ("
            "           (a.franchise_id IS NULL OR a.opponent_franchise_id IS NULL) "
            "           AND b.manager = a.opponent "
            "           AND b.opponent = a.manager"
            "         ))"
            "  ) "
            "GROUP BY a.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        chunked=True,
        chunk_size=5,
        fix_action="reimport",
    ),
    # 10: REMOVED — duplicate of standings_records_match_weekly (identical SQL).
    # The records-consistency check lives on the standings page.
    # ------------------------------------------------------------------
    # WARNING: Quality batch — sql_expr checks
    # ------------------------------------------------------------------
    # 11. matchups_team_name_populated
    #     team_name should always be populated.
    Check(
        name="matchups_team_name_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="team_name NULL on matchup rows",
        sql_expr=("SUM(CASE WHEN team_name IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 12. matchups_no_phantom_byes
    #     Non-bye, non-playoff, non-consolation rows must have an opponent.
    Check(
        name="matchups_no_phantom_byes",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="Non-bye regular-season row has no opponent (phantom bye)",
        sql_expr=(
            "SUM(CASE WHEN is_bye_week = 0 "
            "AND is_playoffs = 0 "
            "AND is_consolation = 0 "
            "AND opponent IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 13. matchups_points_range
    #     team_points should be in a sane range, with higher bounds for IDP/2xDEF leagues.
    Check(
        name="matchups_points_range",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="team_points outside league-appropriate bounds (IDP/2xDEF scoring)",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, "
            "    COALESCE(roster_IDP, 0) + COALESCE(roster_LB, 0) + COALESCE(roster_DL, 0) "
            "    + COALESCE(roster_DB, 0) + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0) "
            "      AS idp_slots, "
            "    COALESCE(roster_DEF, 0) AS def_slots "
            "  FROM {table_prefix}league_settings"
            ") s "
            "  ON m.db_name = s.db_name AND m.year = s.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.team_points IS NOT NULL "
            "  AND COALESCE(m.is_bye_week, 0) = 0 "
            "  AND (m.team_points < -1 OR m.team_points > ("
            "    CASE "
            "      WHEN COALESCE(s.idp_slots, 0) > 0 THEN 800 "
            "      WHEN COALESCE(s.def_slots, 0) >= 2 THEN 450 "
            "      ELSE 350 "
            "    END"
            "  )) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 14. matchups_grade_values_valid
    #     grade column should only contain valid letter grades.
    Check(
        name="matchups_grade_values_valid",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="grade column contains value outside valid set (A+–F)",
        sql_expr=(
            "SUM(CASE WHEN grade IS NOT NULL "
            "AND grade NOT IN ('A+','A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F') "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 15. matchups_power_rating_populated
    #     power_rating should be populated on real rows (excludes bye and consolation).
    Check(
        name="matchups_power_rating_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="power_rating NULL on real (non-bye, non-consolation) rows",
        sql_expr=(
            "SUM(CASE WHEN power_rating IS NULL "
            "AND is_bye_week = 0 "
            "AND COALESCE(is_consolation, 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 16. matchups_close_margin_populated
    #     close_margin should be populated on real rows.
    Check(
        name="matchups_close_margin_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="close_margin NULL on real (non-bye) rows",
        sql_expr=("SUM(CASE WHEN close_margin IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 17. matchups_teams_beat_populated
    #     teams_beat_this_week should be populated on real rows.
    Check(
        name="matchups_teams_beat_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="teams_beat_this_week NULL on real (non-bye) rows",
        sql_expr=("SUM(CASE WHEN teams_beat_this_week IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 18. matchups_league_mean_populated
    #     league_weekly_mean should be populated on real rows.
    Check(
        name="matchups_league_mean_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="league_weekly_mean NULL on real (non-bye) rows",
        sql_expr=("SUM(CASE WHEN league_weekly_mean IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 19. matchups_league_median_populated
    #     league_weekly_median should be populated in median leagues.
    Check(
        name="matchups_league_median_populated",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="league_weekly_median NULL (median league)",
        sql_expr=("SUM(CASE WHEN league_weekly_median IS NULL " "AND is_bye_week = 0 " "THEN 1 ELSE 0 END)"),
        feature="median",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 20-22: REMOVED — drama_score, is_dramatic_win/loss, and felo_score are not
    # displayed by any frontend page. These checks produced 277+ league warnings
    # that were pure noise. If these columns are added to the UI in the future,
    # re-add the checks at that time.
    # ------------------------------------------------------------------
    # WARNING: Quality batch — sql_full checks
    # ------------------------------------------------------------------
    # 23. matchups_week_continuity
    #     No gaps in regular-season week sequence within a year for a league.
    #     Scoped to regular-season weeks (is_playoffs=0 AND is_consolation=0)
    #     because multi-week playoff rounds legitimately leave the second
    #     week of each pair as a is_bye_week=1 continuation placeholder,
    #     which would otherwise be counted as a continuity gap (pigskin's
    #     2-week semifinal + 2-week championship each produced one false
    #     positive year).
    Check(
        name="matchups_week_continuity",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="Gap in regular-season week sequence within a year (missing week)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, "
            "    MAX(week) - MIN(week) + 1 AS expected_weeks, "
            "    COUNT(DISTINCT week) AS actual_weeks "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0 "
            "    AND COALESCE(CAST(is_playoffs AS INT), 0) = 0 "
            "    AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT week) < MAX(week) - MIN(week) + 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 24. matchups_no_year_gaps
    #     No gaps in year sequence for a league.
    #     Per-league allowed_gap_count whitelist: counts years that exist on
    #     the platform as league shells (status=complete) but have no
    #     matchup data on the platform side, so the pipeline correctly
    #     skips them. The "gap" is real but unrecoverable — flagging it as
    #     a fail would chase data that doesn't exist.
    #     - sanduskys_playhouse: Sleeper league 863906396951990272 (2022)
    #       has 10 rosters + a pre_draft draft + 0 matchups for all weeks
    #       + 0 transactions. The league effectively didn't play on
    #       Sleeper that year. Confirmed via Sleeper API 2026-04-29.
    Check(
        name="matchups_no_year_gaps",
        page="matchups",
        table="matchup",
        severity="WARNING",
        description="Gap in year sequence for a league (missing season)",
        sql_full=(
            "WITH allowed_gaps(db_name, allowed_gap_count) AS ("
            "  VALUES ('sanduskys_playhouse', 1)"
            "), agg AS ("
            "  SELECT db_name, "
            "    MAX(year) - MIN(year) + 1 AS expected_years, "
            "    COUNT(DISTINCT year) AS actual_years "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "SELECT a.db_name, 1 AS fail_count "
            "FROM agg a "
            "LEFT JOIN allowed_gaps g ON a.db_name = g.db_name "
            "WHERE (a.expected_years - a.actual_years) > COALESCE(g.allowed_gap_count, 0)"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 25. matchups_manager_count_stable
    #     Originally flagged any league whose per-year manager count varied
    #     across seasons, on the theory that it might indicate an import gap.
    #     In practice, long-running leagues legitimately expand or contract
    #     team counts over their history (pigskin went 9→10→12→14, degen
    #     added teams mid-history, etc.), and normalized disambiguation
    #     ("David - Team A" vs "David - Team B") changes the distinct-manager
    #     count too. The signal this check actually needs is "every team in
    #     year N played every week in year N", which is covered by
    #     matchups_week_continuity / matchups_opponent_populated etc. — so
    #     this check is a structural false positive. Downgrade to INFO-only
    #     and scope to only fire when a year has FEWER managers than
    #     league_settings.num_teams for that year (i.e. an actual gap vs
    #     an intentional expansion).
    Check(
        name="matchups_manager_count_stable",
        page="matchups",
        table="matchup",
        severity="INFO",
        description="Year has fewer managers in matchup than league_settings.num_teams",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, COUNT(DISTINCT manager) AS year_manager_count "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0 "
            "  GROUP BY db_name, year"
            ") m "
            "INNER JOIN {table_prefix}league_settings ls "
            "  ON ls.db_name = m.db_name AND ls.year = m.year "
            "WHERE ls.num_teams IS NOT NULL "
            "  AND m.year_manager_count < ls.num_teams "
            "GROUP BY m.db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
    ),
    # 26. matchups_h2h_tables_exist
    #     matchup_h2h_season should have data for any league with matchup data.
    Check(
        name="matchups_h2h_tables_exist",
        page="matchups",
        table="matchup_h2h_season",
        severity="WARNING",
        description="matchup_h2h_season has no data for league",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}league_settings "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup_h2h_season"
            ") h ON m.db_name = h.db_name "
            "WHERE h.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
    ),
    # 27. matchups_career_table_exists
    #     matchup_career should have data for any multi-year league.
    Check(
        name="matchups_career_table_exists",
        page="matchups",
        table="matchup_career",
        severity="WARNING",
        description="matchup_career has no data for multi-year league",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}league_settings "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup_career"
            ") c ON m.db_name = c.db_name "
            "WHERE c.db_name IS NULL"
        ),
        feature="multi_year",
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup", "completeness_has_career_agg"],
    ),
    # 28. matchups_is_placeholder_exists
    #     INFO: is_placeholder column does not exist in ___leagues DDL.
    #     This check is kept as a no-op placeholder for future DDL additions.
    #     The column was never added to the centralized schema.
]
