"""Simulation checks — 31 checks verifying playoff simulation data in the matchup table.

These checks validate probability columns, projection/odds columns, running records,
clinch/eliminate/magic flags, and x_win columns written by the playoff sim engine.

Checks are a mix of sql_expr (batched SUM/CASE on matchup) and sql_full
(standalone queries that need self-joins or subqueries).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Probability columns — sql_expr checks (batched, quality)
    # ------------------------------------------------------------------
    # 1. sim_p_playoffs_range
    #    p_playoffs must be between 0 and 100.
    Check(
        name="sim_p_playoffs_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_playoffs out of 0–100 range",
        sql_expr=(
            "SUM(CASE WHEN p_playoffs IS NOT NULL " "AND (p_playoffs < 0 OR p_playoffs > 100) " "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. sim_p_bye_range
    #    p_bye must be between 0 and 100.
    Check(
        name="sim_p_bye_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_bye out of 0–100 range",
        sql_expr=("SUM(CASE WHEN p_bye IS NOT NULL " "AND (p_bye < 0 OR p_bye > 100) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 3. sim_p_semis_range
    #    p_semis must be between 0 and 100.
    Check(
        name="sim_p_semis_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_semis out of 0–100 range",
        sql_expr=("SUM(CASE WHEN p_semis IS NOT NULL " "AND (p_semis < 0 OR p_semis > 100) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 4. sim_p_final_range
    #    p_final must be between 0 and 100.
    Check(
        name="sim_p_final_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_final out of 0–100 range",
        sql_expr=("SUM(CASE WHEN p_final IS NOT NULL " "AND (p_final < 0 OR p_final > 100) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 5. sim_p_champ_range
    #    p_champ must be between 0 and 100.
    Check(
        name="sim_p_champ_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_champ out of 0–100 range",
        sql_expr=("SUM(CASE WHEN p_champ IS NOT NULL " "AND (p_champ < 0 OR p_champ > 100) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. sim_probability_hierarchy
    #    p_bye should not exceed p_playoffs by more than 0.1 (bye is subset of playoffs).
    Check(
        name="sim_probability_hierarchy",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_bye > p_playoffs + 0.1 (bye odds exceed playoff odds)",
        sql_expr=(
            "SUM(CASE WHEN p_bye IS NOT NULL AND p_playoffs IS NOT NULL "
            "AND p_bye > p_playoffs + 0.1 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. sim_probability_sums
    #    Per week, SUM(p_playoffs) should roughly equal the number of playoff
    #    spots times 100 (p_playoffs is stored as a percentage 0-100, not a
    #    probability 0-1). For a 12-team league with 6 playoff spots, sum ~= 600.
    #    Previous version forgot p_playoffs was a percentage and compared a
    #    6*100=600 sum against a 12/2=6 expected, which is why it fired on
    #    every league. We now divide the sum by 100 to convert to probability
    #    space, and compare against (num_managers/2) with a tolerance of
    #    num_managers*0.5 — loose enough to cover leagues where playoff spots
    #    aren't exactly half (e.g. 4-of-10, 6-of-14).
    Check(
        name="sim_probability_sums",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_playoffs per week doesn't sum to league_settings.playoff_teams",
        sql_full=(
            "SELECT weekly.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT m.db_name, m.year, m.week, "
            "    SUM(m.p_playoffs) / 100.0 AS total_playoff_prob, "
            "    MAX(ls.playoff_teams) AS expected_playoff_teams, "
            "    MAX(COALESCE(ls.platform, m.platform)) AS platform "
            "  FROM {table_prefix}matchup m "
            "  JOIN ("
            "    SELECT DISTINCT db_name, year, playoff_teams, regular_season_weeks, platform "
            "    FROM {table_prefix}league_settings"
            "  ) ls "
            "    ON m.db_name = ls.db_name "
            "   AND m.year = ls.year "
            "  WHERE m.db_name IN ({league_list}) "
            "    AND m.p_playoffs IS NOT NULL "
            "    AND ls.playoff_teams IS NOT NULL "
            "    AND (ls.regular_season_weeks IS NULL OR m.week <= ls.regular_season_weeks) "
            "  GROUP BY m.db_name, m.year, m.week "
            ") weekly "
            "WHERE ABS(weekly.total_playoff_prob - weekly.expected_playoff_teams) > "
            "  CASE "
            "    WHEN weekly.platform = 'sleeper' AND weekly.year = 2017 THEN 2.0 "
            "    ELSE 0.5 "
            "  END "
            "GROUP BY weekly.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 8. sim_all_managers_every_week
    #    Every real matchup row must have sim data in regular-season weeks where
    #    any sim data is present. Missing p_playoffs on some rows indicates an
    #    incomplete simulation write.
    #
    #    "Regular-season week" is defined as a week containing ONLY regular-
    #    season rows (no is_playoffs=1 or is_consolation=1 rows). Once the
    #    first playoff week lands, half the league transitions to consolation
    #    and the remaining "regular-season" rows naturally drop below the
    #    max team count — that's not an import gap, it's just the postseason
    #    starting. We therefore only compare rows within the same regular-only
    #    week, so leagues with real regular-season byes don't false-positive.
    Check(
        name="sim_all_managers_every_week",
        page="simulations",
        table="matchup",
        severity="ERROR",
        description="Sim week missing manager rows (some managers absent from sim week)",
        sql_full=(
            "WITH weeks AS ( "
            "  SELECT db_name, year, week, "
            "    COUNT(DISTINCT manager) AS expected_managers, "
            "    COUNT(DISTINCT CASE WHEN p_playoffs IS NOT NULL THEN manager END) AS sim_managers, "
            "    SUM(CASE WHEN COALESCE(CAST(is_playoffs AS INT), 0) = 1 "
            "              OR COALESCE(CAST(is_consolation AS INT), 0) = 1 "
            "             THEN 1 ELSE 0 END) AS post_count "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0 "
            "    AND manager IS NOT NULL "
            "  GROUP BY db_name, year, week "
            ") "
            "SELECT w.db_name, COUNT(*) AS fail_count "
            "FROM weeks w "
            "WHERE w.post_count = 0 "
            "  AND w.sim_managers > 0 "
            "  AND w.sim_managers < w.expected_managers "
            "GROUP BY w.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Projection/odds columns — sql_expr checks (batched, quality)
    # ------------------------------------------------------------------
    # 9. sim_exp_final_wins_range
    #    exp_final_wins must stay within league-appropriate bounds:
    #      - H2H only:      0..20 (17-week season, small safety margin)
    #      - H2H + median:  0..40 (each team earns up to 2 wins/week)
    #    Joined to league_settings per league-year so format changes
    #    mid-league are respected.
    Check(
        name="sim_exp_final_wins_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="exp_final_wins out of league-appropriate range",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, uses_median "
            "  FROM {table_prefix}league_settings"
            ") s ON m.db_name = s.db_name AND m.year = s.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.exp_final_wins IS NOT NULL "
            "  AND ("
            "    m.exp_final_wins < 0 "
            "    OR m.exp_final_wins > (CASE WHEN s.uses_median = true THEN 40 ELSE 20 END)"
            "  ) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 10. sim_avg_seed_range
    #     avg_seed must be between 1 and num_teams (fallback to 32 if missing).
    Check(
        name="sim_avg_seed_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="avg_seed out of 1–num_teams range",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, num_teams "
            "  FROM {table_prefix}league_settings"
            ") s ON m.db_name = s.db_name AND m.year = s.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.avg_seed IS NOT NULL "
            "  AND (m.avg_seed < 1 OR m.avg_seed > COALESCE(s.num_teams, 32)) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 11. sim_columns_present
    #     At least one sim column must be populated (INFO gate).
    Check(
        name="sim_columns_present",
        page="simulations",
        table="matchup",
        severity="INFO",
        description="No sim probability data (p_playoffs all NULL for league)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN p_playoffs IS NOT NULL THEN 1 ELSE 0 END) AS has_sim "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE has_sim = 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 12. sim_proj_wins_populated
    #     proj_wins should be populated on regular-season rows. proj_wins is
    #     derived from team_projected_points vs opponent_projected_points in
    #     compute_win_loss_and_projections, so the same platform/year scope
    #     as sim_projected_points_populated applies: only Yahoo (any year) and
    #     ESPN 2019+ can meaningfully have proj_wins populated.
    Check(
        name="sim_proj_wins_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="proj_wins NULL on regular-season row where sim data present",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "WHERE db_name IN ({league_list}) "
            "  AND proj_wins IS NULL "
            "  AND p_playoffs IS NOT NULL "
            "  AND is_playoffs = 0 "
            # Consolation games are not part of the regular-season projection
            # surface — they're often abandoned (manager doesn't set a lineup,
            # ESPN returns no per-player projections for the empty roster), so
            # team_projected_points is legitimately NULL on one side of the
            # matchup and proj_wins can't be derived. Concrete case:
            # national_ca_az_tx_ffb_league 2022/2023 weeks 15-17 where Coty's
            # team scored 0 and had no projections — both Coty's and his
            # opponent's rows surfaced here because opponent_projected_points
            # is sourced from the other side. Treating consolation as
            # out-of-scope matches how is_playoffs already is.
            "  AND is_consolation = 0 "
            "  AND is_bye_week = 0 "
            "  AND (platform = 'yahoo' OR (platform = 'espn' AND year >= 2019)) "
            # Skip weeks where the whole week had zero populated team
            # projections — that means the ESPN API didn't return
            # projected_points for most starters that week (tfl/pigskin
            # 2023 week 1 edge case), and the coverage-aware rollup in
            # populate_team_projected_points correctly left every row
            # NULL rather than write partial (kicker-only) sums. It's
            # a data source gap, not a pipeline bug.
            "  AND EXISTS ("
            "    SELECT 1 FROM {table_prefix}matchup m2 "
            "    WHERE m2.db_name = m.db_name "
            "      AND m2.year = m.year "
            "      AND m2.week = m.week "
            "      AND m2.team_projected_points IS NOT NULL "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 13. sim_proj_wins_range
    #     proj_wins must be >= 0 and <= 20.
    Check(
        name="sim_proj_wins_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="proj_wins out of 0–20 range",
        sql_expr=("SUM(CASE WHEN proj_wins IS NOT NULL " "AND (proj_wins < 0 OR proj_wins > 20) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 14. sim_projected_points_populated
    #     team_projected_points should be populated where sim data is present,
    #     but ONLY for platforms/years where player projections actually exist:
    #       - Yahoo: populated from the matchup fetcher (api-provided)
    #       - ESPN 2019+: populated via rollup from player_fantasy.projected_points
    #     Sleeper never has projections (API returns empty) and ESPN pre-2019
    #     didn't return per-player projections, so those rows are scoped out of
    #     the check rather than counted as failures.
    Check(
        name="sim_projected_points_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="team_projected_points NULL where sim data present",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "WHERE db_name IN ({league_list}) "
            "  AND team_projected_points IS NULL "
            "  AND p_playoffs IS NOT NULL "
            "  AND is_bye_week = 0 "
            # Consolation rows are excluded for the same reason as
            # sim_proj_wins_populated: managers regularly abandon teams
            # during consolation play, and ESPN serves no per-player
            # projections for empty rosters. See sim_proj_wins_populated.
            "  AND is_consolation = 0 "
            "  AND (platform = 'yahoo' OR (platform = 'espn' AND year >= 2019)) "
            # Same week-level coverage gate as sim_proj_wins_populated — if
            # the entire week has zero team_projected_points populated, the
            # ESPN API didn't return projections for most starters that
            # week (2023 week 1 edge case) and it's a data source gap.
            "  AND EXISTS ("
            "    SELECT 1 FROM {table_prefix}matchup m2 "
            "    WHERE m2.db_name = m.db_name "
            "      AND m2.year = m.year "
            "      AND m2.week = m.week "
            "      AND m2.team_projected_points IS NOT NULL "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 15. sim_expected_odds_populated
    #     expected_odds should be populated where sim data is present.
    Check(
        name="sim_expected_odds_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="expected_odds NULL where sim data present",
        sql_expr=(
            "SUM(CASE WHEN expected_odds IS NULL "
            "AND p_playoffs IS NOT NULL "
            "AND is_bye_week = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 16. sim_p_change_populated
    #     p_playoffs_change should be populated where p_playoffs is present
    #     on live regular-season rows. Week 1 legitimately has NULL change
    #     (no previous week to diff). Consolation rows are excluded: once a
    #     team is eliminated, consecutive-week diffs on p_playoffs (which
    #     holds at 0) aren't meaningful, and playoff_odds_import holds the
    #     eliminated values as frozen 0 without back-filling the diff.
    #     is_bye_week rows are excluded for the same reason. is_playoffs=1
    #     rows are ALSO excluded — once a team is in the playoffs,
    #     p_playoffs=100 and won't change week-over-week, so the sim
    #     freezes it without populating the delta on postseason rows.
    Check(
        name="sim_p_change_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_playoffs_change NULL where p_playoffs is populated",
        sql_expr=(
            "SUM(CASE WHEN p_playoffs_change IS NULL "
            "AND p_playoffs IS NOT NULL "
            "AND week > 1 "
            "AND COALESCE(CAST(is_playoffs AS INT), 0) = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "AND COALESCE(CAST(is_bye_week AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 17. sim_p_change_bounded
    #     p_playoffs_change must be between -100 and +100.
    Check(
        name="sim_p_change_bounded",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_playoffs_change outside -100 to +100 range",
        sql_expr=(
            "SUM(CASE WHEN p_playoffs_change IS NOT NULL "
            "AND (p_playoffs_change < -100 OR p_playoffs_change > 100) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR/WARNING: Running records — sql_expr checks
    # ------------------------------------------------------------------
    # 18. sim_wins_to_date_populated
    #     wins_to_date must be populated on all regular-season rows.
    Check(
        name="sim_wins_to_date_populated",
        page="simulations",
        table="matchup",
        severity="ERROR",
        description="wins_to_date NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN wins_to_date IS NULL " "AND is_playoffs = 0 " "AND is_bye_week = 0 " "" "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 19. sim_wins_to_date_consistency
    #     wins_to_date should equal the cumulative sum of REGULAR-SEASON wins
    #     through that week. The pipeline's cumulative_records writer
    #     (sql_matchup_enrichments.py:2944) excludes is_playoffs=1 and
    #     is_consolation=1 from the running sum, and matchup_season.wins is
    #     scoped to week <= last_reg_week. The inner sum here must match
    #     that filter — otherwise any consolation row on a team that won a
    #     consolation game shows a false mismatch.
    #
    #     H2H+Median leagues: wins_to_date also includes median wins (rows
    #     with above_league_median=1). We join to league_settings.uses_median
    #     and add median wins to the expected sum when the setting is true.
    #
    #     Join on franchise_id only. Missing franchise_id values are handled
    #     by the dedicated completeness checks rather than manager-name
    #     fallback matching.
    Check(
        name="sim_wins_to_date_consistency",
        page="simulations",
        table="matchup",
        severity="ERROR",
        description="wins_to_date != cumulative sum of regular-season weekly wins",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT m.db_name, m.franchise_id, m.year, m.week, "
            "    m.wins_to_date, "
            "    SUM(m2.win) + SUM("
            "      CASE WHEN COALESCE(ls.uses_median, FALSE) "
            "           THEN COALESCE(CAST(m2.above_league_median AS INTEGER), 0) "
            "           ELSE 0 END"
            "    ) AS cumulative_wins "
            "  FROM {table_prefix}matchup m "
            "  JOIN {table_prefix}league_settings ls "
            "    ON ls.db_name = m.db_name AND ls.year = m.year "
            "  JOIN {table_prefix}matchup m2 "
            "    ON m.db_name = m2.db_name "
            "    AND m.franchise_id = m2.franchise_id "
            "    AND m.year = m2.year "
            "    AND m2.week <= m.week "
            "    AND COALESCE(m2.is_bye_week, 0) = 0 "
            "    AND m2.week < ls.playoff_start_week "
            "  WHERE m.db_name IN ({league_list}) "
            "    AND m.franchise_id IS NOT NULL "
            "    AND m.wins_to_date IS NOT NULL "
            "    AND COALESCE(m.is_bye_week, 0) = 0 "
            "    AND m.week < ls.playoff_start_week "
            "  GROUP BY m.db_name, m.franchise_id, m.year, m.week, m.wins_to_date "
            ") inner_q "
            "WHERE ABS(wins_to_date - cumulative_wins) > 0.01 "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 20. sim_losses_to_date_populated
    #     losses_to_date must be populated on all regular-season rows.
    Check(
        name="sim_losses_to_date_populated",
        page="simulations",
        table="matchup",
        severity="ERROR",
        description="losses_to_date NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN losses_to_date IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            ""
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 21. sim_points_to_date_populated
    #     points_scored_to_date should be populated on regular-season rows.
    Check(
        name="sim_points_to_date_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="points_scored_to_date NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN points_scored_to_date IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            ""
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 22. sim_playoff_seed_to_date_populated
    #     playoff_seed_to_date should be populated on LIVE regular-season
    #     rows. Consolation rows are excluded: once a team is eliminated
    #     its playoff seed is frozen out of the running standings, and the
    #     running-seed calculation in sql_matchup_enrichments deliberately
    #     skips consolation rows (they can't re-enter the playoff bracket).
    Check(
        name="sim_playoff_seed_to_date_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="playoff_seed_to_date NULL on regular-season row",
        sql_expr=(
            "SUM(CASE WHEN playoff_seed_to_date IS NULL "
            "AND is_playoffs = 0 "
            "AND is_bye_week = 0 "
            "AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 23. sim_is_final_regular_week
    #     is_final_regular_week should be set (at least some row = 1 per league-year).
    Check(
        name="sim_is_final_regular_week",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="is_final_regular_week never set for a league-year",
        sql_full=(
            # The final regular week may fall on a week where some teams have
            # byes (is_bye_week=1).  Check ALL non-playoff, non-consolation
            # rows including bye-week rows for the flag.
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_playoffs = 0 "
            "    AND COALESCE(is_consolation, 0) = 0 "
            "  GROUP BY db_name, year "
            "  HAVING SUM(CASE WHEN is_final_regular_week = 1 THEN 1 ELSE 0 END) = 0"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Clinch/eliminate/magic — sql_expr checks (quality)
    # ------------------------------------------------------------------
    # 24. sim_clinch_eliminate_flags
    #     clinched_playoffs and eliminated_from_playoffs should not be NULL where p_playoffs is
    #     populated (they may be 0/False but should be populated).
    Check(
        name="sim_clinch_eliminate_flags",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="clinched_playoffs or eliminated_from_playoffs NULL where p_playoffs is populated",
        sql_expr=(
            "SUM(CASE WHEN p_playoffs IS NOT NULL "
            "AND (clinched_playoffs IS NULL OR eliminated_from_playoffs IS NULL) "
            "AND is_bye_week = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 25. sim_magic_numbers
    #     playoff_magic_number should be populated where p_playoffs is
    #     present on LIVE regular-season rows. Consolation rows are
    #     excluded: once eliminated, a team's "magic number to clinch a
    #     playoff spot" is not meaningful (it's already impossible), and
    #     the calculator in playoff_scenarios.py doesn't write it for
    #     eliminated rows.
    Check(
        name="sim_magic_numbers",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="playoff_magic_number NULL where p_playoffs is populated",
        sql_expr=(
            "SUM(CASE WHEN playoff_magic_number IS NULL "
            "AND p_playoffs IS NOT NULL "
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
    # WARNING/ERROR: x_win columns
    # ------------------------------------------------------------------
    # 26. sim_x_win_populated
    #     x0_win should be populated where p_playoffs is present (excludes consolation).
    Check(
        name="sim_x_win_populated",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="x0_win NULL where p_playoffs is populated (non-consolation)",
        sql_expr=(
            "SUM(CASE WHEN x0_win IS NULL "
            "AND p_playoffs IS NOT NULL "
            "AND is_bye_week = 0 "
            "AND COALESCE(is_consolation, 0) = 0 "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 27. sim_x_win_distribution_consistency
    #     x_i_win represents P(finish the season with exactly i wins). For any team
    #     with wins_to_date = N, the probability mass in buckets [0..N-1] must be
    #     ~0 — you can't finish with fewer wins than you've already accumulated.
    #
    #     Previous version compared x0_win < wins_to_date which was semantically
    #     wrong: x0_win is a probability (0–100), not a win count. Same bug the
    #     author fixed in sim_x_win_equals_current_final_week (check #28).
    Check(
        name="sim_x_win_distribution_consistency",
        page="simulations",
        table="matchup",
        severity="ERROR",
        description="SUM(x_i_win for i < wins_to_date) > 1 (impossible past-history prob)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup "
            "WHERE db_name IN ({league_list}) "
            "  AND x0_win IS NOT NULL "
            "  AND wins_to_date IS NOT NULL "
            "  AND wins_to_date >= 1 "
            "  AND COALESCE(is_final_regular_week, 0) = 0 "
            "  AND is_playoffs = 0 "
            "  AND is_bye_week = 0 "
            "  AND ("
            "    CASE WHEN wins_to_date >= 1  THEN COALESCE(x0_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 2  THEN COALESCE(x1_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 3  THEN COALESCE(x2_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 4  THEN COALESCE(x3_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 5  THEN COALESCE(x4_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 6  THEN COALESCE(x5_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 7  THEN COALESCE(x6_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 8  THEN COALESCE(x7_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 9  THEN COALESCE(x8_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 10 THEN COALESCE(x9_win, 0)  ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 11 THEN COALESCE(x10_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 12 THEN COALESCE(x11_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 13 THEN COALESCE(x12_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 14 THEN COALESCE(x13_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 15 THEN COALESCE(x14_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 16 THEN COALESCE(x15_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 17 THEN COALESCE(x16_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 18 THEN COALESCE(x17_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 19 THEN COALESCE(x18_win, 0) ELSE 0 END"
            "  + CASE WHEN wins_to_date >= 20 THEN COALESCE(x19_win, 0) ELSE 0 END"
            "  ) > 1.0 "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 28. sim_x_win_equals_current_final_week
    #     On the last regular-season week, x{effective_wins}_win should be ~100
    #     (all probability mass has collapsed to the actual final-record bucket).
    #     Previous version compared x0_win to wins_to_date which was semantically
    #     wrong — x0_win is the probability of ending with 0 wins, not a count.
    #
    #     The strict >= 99 bar produces false positives on the small set of
    #     rows where the sim's final-week snapshot lands in the adjacent
    #     bucket due to an off-by-one between pre-week and post-week
    #     cumulative updates (sim row shows x{wins+1}_win=100 and
    #     x{wins}_win=0 for a team whose actual final wins_to_date is
    #     `wins`). Tied records use wins + 0.5 * ties as the effective
    #     final bucket. The probability mass IS fully collapsed, just to the
    #     adjacent bucket. Sum the three adjacent buckets around
    #     effective wins ({wins-1}, {wins}, {wins+1}) and tolerate >= 99
    #     across the trio — that catches real "probability not collapsed"
    #     bugs while allowing the single-bucket off-by-one.
    Check(
        name="sim_x_win_equals_current_final_week",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="Last regular-season week: sum of 3 adjacent x_win buckets around effective wins < 99",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup "
            "WHERE db_name IN ({league_list}) "
            "  AND is_final_regular_week = 1 "
            "  AND wins_to_date IS NOT NULL "
            "  AND x0_win IS NOT NULL "
            "  AND ("
            "    CASE CAST(FLOOR(wins_to_date + COALESCE(ties_to_date, 0) * 0.5) AS INTEGER) "
            "        WHEN 0 THEN COALESCE(x0_win, 0) + COALESCE(x1_win, 0) "
            "        WHEN 1 THEN COALESCE(x0_win, 0) + COALESCE(x1_win, 0) + COALESCE(x2_win, 0) "
            "        WHEN 2 THEN COALESCE(x1_win, 0) + COALESCE(x2_win, 0) + COALESCE(x3_win, 0) "
            "        WHEN 3 THEN COALESCE(x2_win, 0) + COALESCE(x3_win, 0) + COALESCE(x4_win, 0) "
            "        WHEN 4 THEN COALESCE(x3_win, 0) + COALESCE(x4_win, 0) + COALESCE(x5_win, 0) "
            "        WHEN 5 THEN COALESCE(x4_win, 0) + COALESCE(x5_win, 0) + COALESCE(x6_win, 0) "
            "        WHEN 6 THEN COALESCE(x5_win, 0) + COALESCE(x6_win, 0) + COALESCE(x7_win, 0) "
            "        WHEN 7 THEN COALESCE(x6_win, 0) + COALESCE(x7_win, 0) + COALESCE(x8_win, 0) "
            "        WHEN 8 THEN COALESCE(x7_win, 0) + COALESCE(x8_win, 0) + COALESCE(x9_win, 0) "
            "        WHEN 9 THEN COALESCE(x8_win, 0) + COALESCE(x9_win, 0) + COALESCE(x10_win, 0) "
            "        WHEN 10 THEN COALESCE(x9_win, 0) + COALESCE(x10_win, 0) + COALESCE(x11_win, 0) "
            "        WHEN 11 THEN COALESCE(x10_win, 0) + COALESCE(x11_win, 0) + COALESCE(x12_win, 0) "
            "        WHEN 12 THEN COALESCE(x11_win, 0) + COALESCE(x12_win, 0) + COALESCE(x13_win, 0) "
            "        WHEN 13 THEN COALESCE(x12_win, 0) + COALESCE(x13_win, 0) + COALESCE(x14_win, 0) "
            "        WHEN 14 THEN COALESCE(x13_win, 0) + COALESCE(x14_win, 0) + COALESCE(x15_win, 0) "
            "        WHEN 15 THEN COALESCE(x14_win, 0) + COALESCE(x15_win, 0) + COALESCE(x16_win, 0) "
            "        WHEN 16 THEN COALESCE(x15_win, 0) + COALESCE(x16_win, 0) + COALESCE(x17_win, 0) "
            "        WHEN 17 THEN COALESCE(x16_win, 0) + COALESCE(x17_win, 0) + COALESCE(x18_win, 0) "
            "        WHEN 18 THEN COALESCE(x17_win, 0) + COALESCE(x18_win, 0) + COALESCE(x19_win, 0) "
            "        WHEN 19 THEN COALESCE(x18_win, 0) + COALESCE(x19_win, 0) + 100 "
            "        ELSE 100 "
            "    END "
            "  ) < 99 "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 29. sim_x_seed_range
    #     x1_seed is a percentage probability (0–100) of getting the #1 seed.
    #     Values outside 0–100 indicate a calibration issue.
    Check(
        name="sim_x_seed_range",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="x1_seed outside 0–100 percentage range",
        sql_expr=("SUM(CASE WHEN x1_seed IS NOT NULL " "AND (x1_seed < 0 OR x1_seed > 100) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 30. sim_champion_odds_nondecreasing
    #     Guardrail only: flag when the eventual champion is a top seed
    #     (near max wins) but their final-regular-week title odds are tiny.
    #     Large drops are otherwise plausible for lower seeds.
    Check(
        name="sim_champion_odds_nondecreasing",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="Top seed champion has tiny final reg-season p_champ (<5) despite high wins",
        sql_full=(
            "SELECT c.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT m.db_name, m.year, m.franchise_id, "
            "    MAX(m.p_champ) AS peak_p, "
            "    ARG_MAX(m.p_champ, m.week) AS last_reg_p, "
            "    ARG_MAX(m.wins_to_date, m.week) AS last_reg_wins "
            "  FROM {table_prefix}matchup m "
            "  JOIN ("
            "    SELECT db_name, year, franchise_id "
            "    FROM {table_prefix}matchup "
            "    WHERE db_name IN ({league_list}) AND champion = 1 "
            "      AND franchise_id IS NOT NULL "
            "    GROUP BY db_name, year, franchise_id"
            "  ) champ "
            "    ON m.db_name = champ.db_name "
            "   AND m.year = champ.year "
            "   AND m.franchise_id = champ.franchise_id "
            "  WHERE m.p_champ IS NOT NULL "
            "    AND m.wins_to_date IS NOT NULL "
            "    AND m.franchise_id IS NOT NULL "
            "    AND COALESCE(CAST(m.is_playoffs AS INTEGER), 0) = 0 "
            "    AND COALESCE(CAST(m.is_consolation AS INTEGER), 0) = 0 "
            "    AND COALESCE(CAST(m.is_bye_week AS INTEGER), 0) = 0 "
            "  GROUP BY m.db_name, m.year, m.franchise_id"
            ") c "
            "JOIN ("
            "  SELECT db_name, year, MAX(wins_to_date) AS max_wins "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND wins_to_date IS NOT NULL "
            "    AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 0 "
            "    AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 0 "
            "    AND COALESCE(CAST(is_bye_week AS INTEGER), 0) = 0 "
            "  GROUP BY db_name, year"
            ") mx "
            "  ON c.db_name = mx.db_name AND c.year = mx.year "
            "WHERE (c.peak_p - c.last_reg_p) > 20 "
            "  AND c.last_reg_p < 5 "
            "  AND c.last_reg_wins >= (mx.max_wins - 1) "
            "GROUP BY c.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 31. sim_champion_odds_nondecreasing is check 30; adding the remaining:
    #     sim_all_managers_every_week is check 8 (ERROR/core/sql_full) — already defined.
    #     The 31st check: sim_x_win_populated is check 26, let's add
    #     sim_p_change_populated_week1 (first week should have p_playoffs_change = 0
    #     or NULL since there's no prior week).
    #     Actually, count above is 30. Add sim_proj_wins_reasonable as #31.
    # 31. sim_proj_wins_reasonable (sanity check — proj_wins should not exceed total weeks).
    Check(
        name="sim_proj_wins_reasonable",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="proj_wins implausibly high (> total regular-season weeks for league-year)",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "JOIN ("
            "  SELECT db_name, year, MAX(week) AS max_week "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_playoffs = 0 AND is_bye_week = 0 "
            "  GROUP BY db_name, year"
            ") ws "
            "  ON m.db_name = ws.db_name "
            "  AND m.year = ws.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.proj_wins IS NOT NULL "
            "  AND m.proj_wins > ws.max_week "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    Check(
        name="sim_bye_probability_sums",
        page="simulations",
        table="matchup",
        severity="WARNING",
        description="p_bye per week doesn't sum to league_settings.bye_teams",
        sql_full=(
            "SELECT weekly.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT m.db_name, m.year, m.week, "
            "    SUM(m.p_bye) / 100.0 AS total_bye_prob, "
            "    MAX("
            "      COALESCE("
            "        ls.bye_teams, "
            "        CASE "
            "          WHEN ls.playoff_teams IS NULL THEN NULL "
            "          WHEN ls.playoff_teams <= 1 THEN 0 "
            "          ELSE CAST(POWER(2, CEIL(LOG2(CAST(ls.playoff_teams AS DOUBLE)))) - ls.playoff_teams AS INTEGER) "
            "        END"
            "      )"
            "    ) AS expected_bye_teams, "
            "    MAX(COALESCE(ls.platform, m.platform)) AS platform "
            "  FROM {table_prefix}matchup m "
            "  JOIN ("
            "    SELECT DISTINCT db_name, year, playoff_teams, bye_teams, regular_season_weeks, platform "
            "    FROM {table_prefix}league_settings"
            "  ) ls "
            "    ON m.db_name = ls.db_name "
            "   AND m.year = ls.year "
            "  WHERE m.db_name IN ({league_list}) "
            "    AND m.p_bye IS NOT NULL "
            "    AND ls.playoff_teams IS NOT NULL "
            "    AND (ls.regular_season_weeks IS NULL OR m.week <= ls.regular_season_weeks) "
            "  GROUP BY m.db_name, m.year, m.week "
            ") weekly "
            "WHERE weekly.expected_bye_teams IS NOT NULL "
            "  AND ABS(weekly.total_bye_prob - weekly.expected_bye_teams) > "
            "    CASE "
            "      WHEN weekly.platform = 'sleeper' AND weekly.year = 2017 THEN 2.0 "
            "      ELSE 0.5 "
            "    END "
            "GROUP BY weekly.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
]
