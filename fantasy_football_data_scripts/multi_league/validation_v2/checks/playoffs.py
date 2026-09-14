"""Playoff checks — 14 checks verifying champion/sacko flags, playoff rounds,
and placement integrity in the matchup table.

Checks are a mix of sql_expr (batched) and sql_full (standalone).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core — sql_full checks (champion/sacko uniqueness)
    # ------------------------------------------------------------------
    # 1. playoffs_one_champion_per_year
    #    There must be exactly one champion FRANCHISE per league-year. The
    #    previous version summed champion rows per year and failed >1, which
    #    false-positives on leagues with multi-week championships where the
    #    pipeline legitimately sets champion=1 on every week of the final
    #    for the winning manager (e.g. pigskin_platoon 2014 has the same
    #    manager Michaela flagged champion=1 on weeks 13 AND 14 because the
    #    championship spans two weeks). Counting distinct franchise_ids
    #    with champion=1 gives the right semantic.
    Check(
        name="playoffs_one_champion_per_year",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="More than one distinct champion franchise per league-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, "
            "         COUNT(DISTINCT franchise_id) AS champ_franchises "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND champion = 1 "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT franchise_id) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 2. playoffs_champion_has_playoff_game
    #    Any champion franchise must also have at least one is_playoffs=1 row,
    #    except explicit no-playoff seasons where league_settings.playoff_teams
    #    disables the bracket and the seed 1 franchise is champion.
    Check(
        name="playoffs_champion_has_playoff_game",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="champion=1 but no is_playoffs=1 rows for that franchise-year outside no-playoff seasons",
        sql_full=(
            "SELECT c.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND champion = 1"
            ") c "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}matchup "
            "  WHERE is_playoffs = 1"
            ") p "
            "  ON c.db_name = p.db_name "
            "  AND c.year = p.year "
            "  AND c.franchise_id = p.franchise_id "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, playoff_teams "
            "  FROM {table_prefix}league_settings "
            ") ls "
            "  ON c.db_name = ls.db_name "
            "  AND c.year = ls.year "
            "WHERE p.franchise_id IS NULL "
            "  AND NOT (ls.playoff_teams IS NOT NULL AND ls.playoff_teams <= 1) "
            "GROUP BY c.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 3. playoffs_one_sacko_per_year
    #    There must be at most one sacko FRANCHISE per league-year. Same
    #    multi-week final issue as playoffs_one_champion_per_year: if the
    #    sacko game spans two weeks, the pipeline sets sacko=1 on both
    #    weeks for the same loser, which isn't a bug. Count distinct
    #    franchises, not raw rows.
    Check(
        name="playoffs_one_sacko_per_year",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="More than one distinct sacko franchise per league-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, "
            "         COUNT(DISTINCT franchise_id) AS sacko_franchises "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND sacko = 1 "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT franchise_id) > 1"
            ") "
            "GROUP BY db_name"
        ),
        feature="consolation",
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 4. playoffs_sacko_has_consolation_game
    #    Any sacko franchise that was actually seeded into the consolation
    #    bracket must also have at least one is_consolation=1 row.
    #
    #    Some leagues have a smaller consolation field than the non-playoff
    #    pool (for example, 12 teams / 7 playoff teams / 4 consolation teams).
    #    In those formats the worst seed can be excluded from consolation
    #    entirely and finish as sacko without ever playing a consolation game.
    Check(
        name="playoffs_sacko_has_consolation_game",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="sacko=1 but no is_consolation=1 rows for a sacko seed that should be in consolation",
        sql_full=(
            "SELECT s.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, franchise_id, "
            "         MAX(final_playoff_seed) AS final_playoff_seed "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND sacko = 1"
            "  GROUP BY db_name, year, franchise_id"
            ") s "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}matchup "
            "  WHERE is_consolation = 1"
            ") c "
            "  ON s.db_name = c.db_name "
            "  AND s.year = c.year "
            "  AND s.franchise_id = c.franchise_id "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, playoff_teams, num_playoff_consolation_teams, playoff_start_week "
            "  FROM {table_prefix}league_settings"
            ") ls "
            "  ON s.db_name = ls.db_name "
            "  AND s.year = ls.year "
            "LEFT JOIN ("
            "  SELECT DISTINCT m.db_name, m.year, m.franchise_id "
            "  FROM {table_prefix}matchup m "
            "  JOIN ("
            "    SELECT DISTINCT db_name, year, playoff_start_week "
            "    FROM {table_prefix}league_settings"
            "  ) ps "
            "    ON m.db_name = ps.db_name "
            "   AND m.year = ps.year "
            "  WHERE m.db_name IN ({league_list}) "
            "    AND m.week >= COALESCE(ps.playoff_start_week, 14) "
            "    AND m.opponent IS NOT NULL "
            "    AND TRIM(CAST(m.opponent AS VARCHAR)) != '' "
            "    AND (m.team_points IS NOT NULL OR m.opponent_points IS NOT NULL)"
            ") real_post "
            "  ON s.db_name = real_post.db_name "
            "  AND s.year = real_post.year "
            "  AND s.franchise_id = real_post.franchise_id "
            "WHERE c.franchise_id IS NULL "
            "  AND real_post.franchise_id IS NOT NULL "
            "  AND NOT ("
            "    ls.playoff_teams IS NOT NULL "
            "    AND ls.num_playoff_consolation_teams IS NOT NULL "
            "    AND s.final_playoff_seed IS NOT NULL "
            "    AND s.final_playoff_seed > (ls.playoff_teams + ls.num_playoff_consolation_teams)"
            "  ) "
            "GROUP BY s.db_name"
        ),
        feature="consolation",
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 5. playoffs_3rd_place_unique
    #    3rd place (placement_rank=3 or similar) must be unique per league-year.
    Check(
        name="playoffs_3rd_place_unique",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="Duplicate 3rd-place placement records per league-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, COUNT(DISTINCT franchise_id) AS cnt "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND placement_rank = 3 "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT franchise_id) > 1"
            ") "
            "GROUP BY db_name"
        ),
        feature="consolation",
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 6. playoffs_placement_game_results_unique
    #    Each placement position must be held by at most one franchise per year.
    Check(
        name="playoffs_placement_game_results_unique",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="Duplicate placement_rank per league-year (multiple franchises same rank)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, placement_rank, COUNT(DISTINCT franchise_id) AS cnt "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND placement_rank IS NOT NULL "
            "  GROUP BY db_name, year, placement_rank "
            "  HAVING COUNT(DISTINCT franchise_id) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Core — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 7. playoffs_flags_mutually_exclusive
    #    is_playoffs and is_consolation must not both be 1 on the same row.
    Check(
        name="playoffs_flags_mutually_exclusive",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="is_playoffs=1 AND is_consolation=1 on same row (mutually exclusive)",
        sql_expr=("SUM(CASE WHEN is_playoffs = 1 AND is_consolation = 1 THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 8. playoffs_seed_populated
    #    final_playoff_seed should be populated for postseason participants.
    Check(
        name="playoffs_seed_populated",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="final_playoff_seed NULL for a postseason row",
        sql_expr=("SUM(CASE WHEN final_playoff_seed IS NULL AND is_playoffs = 1 THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks
    # ------------------------------------------------------------------
    # 9. playoffs_round_populated
    #    playoff_round should be populated for all is_playoffs=1 rows.
    Check(
        name="playoffs_round_populated",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="playoff_round NULL for is_playoffs=1 row",
        sql_expr=("SUM(CASE WHEN playoff_round IS NULL AND is_playoffs = 1 THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 10. playoffs_consolation_round_populated
    #     consolation_round (NOT playoff_round) should be populated for all
    #     is_consolation=1 rows. bracket_tracer.py writes playoff_round only
    #     for championship-bracket games and consolation_round for
    #     consolation-bracket games — they're distinct columns.
    Check(
        name="playoffs_consolation_round_populated",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="consolation_round NULL for is_consolation=1 row",
        sql_expr=("SUM(CASE WHEN consolation_round IS NULL AND is_consolation = 1 THEN 1 ELSE 0 END)"),
        feature="consolation",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_full checks
    # ------------------------------------------------------------------
    # 11. playoffs_champion_every_completed_year
    #     Every year that has matchup data but no champion=1 row is suspicious.
    Check(
        name="playoffs_champion_every_completed_year",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="Year with matchup data but no champion flag (incomplete season OK)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, year "
            "  HAVING MAX(week) >= 14 "
            "    AND SUM(COALESCE(champion, 0)) = 0"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
    ),
    # 12. playoffs_sacko_every_consolation_year
    #     Every year with consolation data should have a sacko flag.
    Check(
        name="playoffs_sacko_every_consolation_year",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="Year with consolation games but no sacko flag",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, year "
            "  HAVING SUM(CASE WHEN is_consolation = 1 THEN 1 ELSE 0 END) > 0 "
            "    AND SUM(COALESCE(sacko, 0)) = 0"
            ") "
            "GROUP BY db_name"
        ),
        feature="consolation",
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
    ),
    # 13. playoffs_no_unflagged_postseason
    #     Weeks at or after playoff_start_week should have is_playoffs or
    #     is_consolation = 1. Rows without either flag during the postseason
    #     period indicate a labeling gap.
    Check(
        name="playoffs_no_unflagged_postseason",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="Rows in postseason weeks not flagged as playoff or consolation",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "JOIN ("
            "  SELECT db_name, year, MIN(playoff_start_week) AS playoff_start "
            "  FROM {table_prefix}league_settings "
            "  WHERE db_name IN ({league_list}) "
            "    AND playoff_start_week IS NOT NULL "
            "  GROUP BY db_name, year"
            ") s "
            "  ON m.db_name = s.db_name "
            "  AND m.year = s.year "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.week >= s.playoff_start "
            "  AND m.is_bye_week = 0 "
            ""
            "  AND COALESCE(m.is_playoffs, 0) = 0 "
            "  AND COALESCE(m.is_consolation, 0) = 0 "
            "GROUP BY m.db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_settings"],
    ),
    # 14. playoffs_multiweek_champion_two_weeks
    #     In multi-week championship formats, the champion should appear in
    #     exactly 2 playoff weeks in the final round. Fewer could mean a
    #     partial import; more is likely a bug.
    Check(
        name="playoffs_multiweek_champion_two_weeks",
        page="playoffs",
        table="matchup",
        severity="WARNING",
        description="Champion appears in != 2 championship weeks (multi-week format)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, manager, COUNT(*) AS champ_weeks "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_championship = 1 "
            "    AND is_playoffs = 1 "
            "  GROUP BY db_name, year, manager "
            "  HAVING COUNT(*) NOT IN (1, 2)"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
    ),
    # 15. playoffs_season_result_populated
    #     Every postseason participant (is_playoffs=1 or is_consolation=1) must
    #     have a non-empty season_result. This check would have caught the gap
    #     where simulate_playoff_brackets() + add_season_result() were never
    #     wired into the SQL pipeline. Now backed by the compute_season_result
    #     enrichment in sql_matchup_enrichments.py.
    Check(
        name="playoffs_season_result_populated",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="season_result empty for a postseason participant",
        sql_expr=(
            "SUM(CASE WHEN (CAST(is_playoffs AS INTEGER) = 1 OR CAST(is_consolation AS INTEGER) = 1) "
            "AND franchise_id IS NOT NULL "
            "AND COALESCE(season_result, '') = '' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: placement_rank coverage + consistency (new 2026-04-17)
    # ------------------------------------------------------------------
    # 16. playoffs_placement_rank_coverage
    Check(
        name="playoffs_placement_rank_coverage",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="Rostered franchise-year in completed season missing placement_rank",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND franchise_id IS NOT NULL"
            ") m "
            "JOIN ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE champion = 1"
            ") completed "
            "  ON m.db_name = completed.db_name AND m.year = completed.year "
            "LEFT JOIN ("
            "  SELECT db_name, year, franchise_id, MAX(placement_rank) AS rank "
            "  FROM {table_prefix}matchup "
            "  WHERE placement_rank IS NOT NULL AND placement_rank > 0 "
            "  GROUP BY db_name, year, franchise_id"
            ") r "
            "  ON m.db_name = r.db_name AND m.year = r.year AND m.franchise_id = r.franchise_id "
            "WHERE r.rank IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 17. playoffs_placement_rank_champion_is_1
    Check(
        name="playoffs_placement_rank_champion_is_1",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="Franchise with champion=1 must have placement_rank=1",
        sql_expr=("SUM(CASE WHEN champion = 1 AND (placement_rank IS NULL OR placement_rank != 1) THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 18. playoffs_placement_rank_sacko_is_last
    Check(
        name="playoffs_placement_rank_sacko_is_last",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="Franchise with sacko=1 must have placement_rank=num_teams",
        sql_full=(
            "SELECT s.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup s "
            "JOIN {table_prefix}league_settings ls "
            "  ON s.db_name = ls.db_name AND s.year = ls.year "
            "WHERE s.db_name IN ({league_list}) "
            "  AND s.sacko = 1 "
            "  AND (s.placement_rank IS NULL OR s.placement_rank != ls.num_teams) "
            "GROUP BY s.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_settings"],
        fix_action="reimport",
    ),
    # 19. playoffs_placement_rank_sequential
    Check(
        name="playoffs_placement_rank_sequential",
        page="playoffs",
        table="matchup",
        severity="ERROR",
        description="placement_rank for (db_name, year) completed season must be {1..num_teams}",
        sql_full=(
            "SELECT r.db_name, COUNT(*) AS fail_count FROM ("
            "  SELECT db_name, year, "
            "         COUNT(DISTINCT placement_rank) AS distinct_ranks, "
            "         MAX(placement_rank) AS max_rank, "
            "         MIN(placement_rank) AS min_rank "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND placement_rank IS NOT NULL "
            "  GROUP BY db_name, year"
            ") r "
            "JOIN ("
            "  SELECT db_name, year FROM {table_prefix}matchup WHERE champion = 1 GROUP BY db_name, year"
            ") completed "
            "  ON r.db_name = completed.db_name AND r.year = completed.year "
            "JOIN {table_prefix}league_settings ls "
            "  ON r.db_name = ls.db_name AND r.year = ls.year "
            "WHERE NOT (r.min_rank = 1 AND r.max_rank = ls.num_teams AND r.distinct_ranks = ls.num_teams) "
            "GROUP BY r.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_settings"],
        fix_action="reimport",
    ),
]
