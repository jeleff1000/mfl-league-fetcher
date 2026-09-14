"""Draft checks — 32 checks verifying draft table integrity, grades, and derived metrics.

Checks are a mix of sql_expr (batched SUM/CASE on draft) and sql_full
(standalone queries that need subqueries or cross-table joins).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

_VALID_GRADES = "('A+','A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F')"

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core — sql_full checks (need subqueries)
    # ------------------------------------------------------------------
    # 1. draft_no_duplicate_picks
    #    No duplicate (year, round, pick) tuples WITHIN A DRAFT.
    #    Sleeper dynasty leagues routinely run multiple drafts per year (startup +
    #    rookie + veteran/supplemental), each with their own round/pick numbering.
    #    draft_id distinguishes them. Yahoo/ESPN have NULL draft_id (single draft).
    Check(
        name="draft_no_duplicate_picks",
        page="draft",
        table="draft",
        severity="ERROR",
        description="Duplicate (year, round, pick) rows within the same draft",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, round, pick, COALESCE(draft_id, '') AS draft_id, COUNT(*) AS cnt "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND round IS NOT NULL AND pick IS NOT NULL "
            "  GROUP BY db_name, year, round, pick, COALESCE(draft_id, '') "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 2. draft_no_player_twice
    #    Same NFL_player_id should not appear twice in the same draft
    #    (a player can't be drafted by multiple managers in a single draft).
    #    Multi-draft Sleeper dynasties CAN have the same player in startup +
    #    rookie drafts.
    #
    #    DEF-* NFL_player_ids (DEF-14, DEF-26, etc.) are excluded: multiple
    #    real DST franchises collide on the same DEF-N slot in the super
    #    table's DEF mapping, so they produce false positives here. Fixing
    #    the collision is super_table work that's out of scope for the
    #    pipeline validator. Real-player dupes would still fire because
    #    those use numeric NFL_player_ids.
    Check(
        name="draft_no_player_twice",
        page="draft",
        table="draft",
        severity="ERROR",
        description="Same NFL_player_id drafted by 2+ managers in the same draft",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, COALESCE(draft_id, '') AS draft_id, NFL_player_id, COUNT(DISTINCT manager) AS mgr_count "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND NFL_player_id IS NOT NULL "
            "    AND NFL_player_id NOT LIKE 'DEF-%' "
            "  GROUP BY db_name, year, COALESCE(draft_id, ''), NFL_player_id "
            "  HAVING COUNT(DISTINCT manager) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 3. draft_all_managers_have_picks
    #    Every manager present in matchup for a draft year must have at least
    #    one draft pick in that year — but only for FULL drafts (>= 10 rounds).
    #    Dynasty rookie drafts have 3-5 rounds and managers can legitimately
    #    have zero picks (trades, no rookie allocation), which is not a bug.
    #    Scoping to >= 10 rounds eliminates that false-positive class.
    #
    #    Joined on franchise_id only. Missing franchise_id values are handled
    #    by the dedicated completeness checks rather than manager-name fallback.
    Check(
        name="draft_all_managers_have_picks",
        page="draft",
        table="draft",
        severity="ERROR",
        description="Franchises with 0 picks in a full (>=10 round) draft year",
        sql_full=(
            "WITH full_draft_years AS ("
            "  SELECT db_name, year "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, year "
            "  HAVING MAX(round) >= 10"
            "), "
            "expected AS ("
            "  SELECT DISTINCT m.db_name, m.year, m.franchise_id "
            "  FROM {table_prefix}matchup m "
            "  JOIN full_draft_years f "
            "    ON m.db_name = f.db_name AND m.year = f.year "
            "  WHERE COALESCE(m.is_bye_week, 0) = 0 "
            "    AND m.franchise_id IS NOT NULL"
            "), "
            "drafted AS ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}draft "
            "  WHERE franchise_id IS NOT NULL"
            ") "
            "SELECT e.db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "LEFT JOIN drafted d "
            "  ON e.db_name = d.db_name "
            "  AND e.year = d.year "
            "  AND e.franchise_id = d.franchise_id "
            "WHERE d.db_name IS NULL "
            "GROUP BY e.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_draft"],
        fix_action="reimport",
    ),
    # 4. draft_auction_costs_nonneg
    #    cost must not be negative (applies to auction drafts only; 0 is valid
    #    for snake picks that have cost = 0 by default).
    Check(
        name="draft_auction_costs_nonneg",
        page="draft",
        table="draft",
        severity="ERROR",
        description="cost < 0 on draft pick",
        sql_expr="SUM(CASE WHEN cost IS NOT NULL AND cost < 0 THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_full checks
    # ------------------------------------------------------------------
    # 5. draft_nfl_id_coverage
    #    At least 80% of draft rows should have NFL_player_id populated.
    #    Scope: only count picks for years that have matchup data, i.e.
    #    seasons that have actually been played. A current-year mock draft
    #    in the offseason will legitimately have NFL_player_ids missing for
    #    rookies who aren't in our super_table yet — that's not an
    #    identity-resolution bug we should chase, so wait until the season
    #    is real before validating.
    Check(
        name="draft_nfl_id_coverage",
        page="draft",
        table="draft",
        severity="WARNING",
        description="<80% of draft rows have NFL_player_id (ID resolution gap, played seasons only)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT d.db_name, "
            "    SUM(CASE WHEN d.NFL_player_id IS NOT NULL THEN 1 ELSE 0 END) AS has_id, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}draft d "
            "  WHERE d.db_name IN ({league_list}) "
            "    AND EXISTS ( "
            "      SELECT 1 FROM {table_prefix}matchup m "
            "      WHERE m.db_name = d.db_name AND m.year = d.year "
            "    ) "
            "  GROUP BY d.db_name "
            "  HAVING total > 0 "
            "    AND (CAST(has_id AS DOUBLE) / total) < 0.8"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 6. draft_every_year_has_draft
    #    Each year present in matchup should have at least some draft data.
    Check(
        name="draft_every_year_has_draft",
        page="draft",
        table="draft",
        severity="WARNING",
        description="League-year has matchup data but no draft picks",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}draft"
            ") d "
            "  ON m.db_name = d.db_name AND m.year = d.year "
            "WHERE d.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_draft"],
    ),
    # 7. draft_type_consistent_per_year
    #    A single draft year should not mix auction and snake picks.
    Check(
        name="draft_type_consistent_per_year",
        page="draft",
        table="draft",
        severity="WARNING",
        description="Mixed draft_type (auction + snake) within the same league-year",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, COUNT(DISTINCT norm_type) AS type_count "
            "  FROM ("
            "    SELECT db_name, year, "
            "      CASE "
            "        WHEN LOWER(draft_type) IN ('snake','linear','offline_snake') THEN 'snake' "
            "        WHEN LOWER(draft_type) = 'auction' THEN 'auction' "
            "        ELSE NULL "
            "      END AS norm_type "
            "    FROM {table_prefix}draft "
            "    WHERE db_name IN ({league_list}) "
            "      AND draft_type IS NOT NULL "
            "  ) t "
            "  WHERE norm_type IS NOT NULL "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT norm_type) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 8. draft_grade_not_all_same
    #    All picks having the identical grade is a calibration red flag.
    Check(
        name="draft_grade_not_all_same",
        page="draft",
        table="draft",
        severity="WARNING",
        description="Every draft pick has the same draft_grade (calibration issue)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, COUNT(DISTINCT draft_grade) AS distinct_grades "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND draft_grade IS NOT NULL "
            "  GROUP BY db_name, year "
            "  HAVING COUNT(DISTINCT draft_grade) = 1 "
            "    AND COUNT(*) > 5"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 9. draft_zscore_not_all_zero
    #    All pick_quality_zscore = 0 means the enrichment didn't run.
    Check(
        name="draft_zscore_not_all_zero",
        page="draft",
        table="draft",
        severity="WARNING",
        description="All pick_quality_zscore = 0 (enrichment may not have run)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN pick_quality_zscore = 0 THEN 1 ELSE 0 END) AS zero_count, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND pick_quality_zscore IS NOT NULL "
            "  GROUP BY db_name "
            "  HAVING total > 5 AND zero_count = total"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 10. draft_pick_sequence_gaps
    #     Large gaps in pick numbers within a year suggest missing rows.
    #     Scope: only main drafts (max_round >= 10) — dynasty rookie drafts
    #     legitimately have sparse rounds 6+ (forfeited / un-allocated picks
    #     that Sleeper's API doesn't return), so a "gap" of unfilled later
    #     rounds isn't a fetcher bug. Verified on wall_street_fantasy 2023
    #     (draft_id 919010413285638145, 64 of 84 picks present): the missing
    #     pick numbers are also missing on the Sleeper side.
    Check(
        name="draft_pick_sequence_gaps",
        page="draft",
        table="draft",
        severity="WARNING",
        description="Large gap in pick sequence within a main draft (missing rows)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, "
            "    MAX(pick) - MIN(pick) + 1 AS expected_range, "
            "    COUNT(DISTINCT pick) AS actual_picks "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND pick IS NOT NULL "
            "  GROUP BY db_name, year "
            "  HAVING MAX(round) >= 10 "
            "    AND COUNT(DISTINCT pick) < (MAX(pick) - MIN(pick) + 1) * 0.8"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 11. draft_agg_tables_exist
    #     Draft aggregate tables should have data for any league with draft data.
    Check(
        name="draft_agg_tables_exist",
        page="draft",
        table="draft_manager_season",
        severity="WARNING",
        description="Draft aggregate table missing data for league with draft picks",
        sql_full=(
            "WITH draft_leagues AS ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list})"
            "), presence AS ("
            "  SELECT d.db_name, "
            "    CASE WHEN dms.db_name IS NULL THEN 1 ELSE 0 END + "
            "    CASE WHEN dmc.db_name IS NULL THEN 1 ELSE 0 END + "
            "    CASE WHEN dpc.db_name IS NULL THEN 1 ELSE 0 END AS missing_count "
            "  FROM draft_leagues d "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}draft_manager_season"
            ") dms ON d.db_name = dms.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}draft_manager_career"
            ") dmc ON d.db_name = dmc.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}draft_player_career"
            ") dpc ON d.db_name = dpc.db_name "
            ") "
            "SELECT db_name, missing_count AS fail_count "
            "FROM presence "
            "WHERE missing_count > 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_draft"],
    ),
    Check(
        name="draft_manager_season_matches_draft_totals",
        page="draft",
        table="draft_manager_season",
        severity="ERROR",
        description="draft_manager_season core totals drift from grouped draft rows",
        sql_full=(
            "WITH expected AS ("
            "  SELECT db_name, franchise_id, year, COALESCE(draft_category, 'standard') AS draft_category, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) = 0 THEN 1 ELSE 0 END) AS picks, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) <> 0 THEN 1 ELSE 0 END) AS keeper_picks, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) = 0 THEN COALESCE(cost, 0) ELSE 0 END) AS total_cost "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND manager IS NOT NULL AND TRIM(manager) <> '' "
            "    AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> '' "
            "  GROUP BY db_name, franchise_id, year, COALESCE(draft_category, 'standard') "
            "  HAVING SUM(CASE WHEN COALESCE(is_keeper, 0) = 0 THEN 1 ELSE 0 END) > 0"
            "), "
            "actual AS ("
            "  SELECT db_name, franchise_id, year, COALESCE(draft_category, 'standard') AS draft_category, "
            "    picks, keeper_picks, total_cost "
            "  FROM {table_prefix}draft_manager_season "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.year = a.year "
            " AND e.draft_category = a.draft_category "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.picks, 0) <> COALESCE(a.picks, 0) "
            "   OR COALESCE(e.keeper_picks, 0) <> COALESCE(a.keeper_picks, 0) "
            "   OR ABS(COALESCE(e.total_cost, 0) - COALESCE(a.total_cost, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_draft", "draft_agg_tables_exist"],
        fix_action="reimport",
    ),
    Check(
        name="draft_manager_career_matches_season_rollup",
        page="draft",
        table="draft_manager_career",
        severity="ERROR",
        description="draft_manager_career totals drift from draft_manager_season",
        sql_full=(
            "WITH expected AS ("
            "  SELECT db_name, franchise_id, COALESCE(draft_category, 'standard') AS draft_category, "
            "    COUNT(DISTINCT year) AS years_active, "
            "    SUM(picks) AS total_picks, "
            "    SUM(keeper_picks) AS total_keeper_picks, "
            "    SUM(total_cost) AS total_cost "
            "  FROM {table_prefix}draft_manager_season "
            "  WHERE db_name IN ({league_list}) "
            "    AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> '' "
            "  GROUP BY db_name, franchise_id, COALESCE(draft_category, 'standard')"
            "), "
            "actual AS ("
            "  SELECT db_name, franchise_id, COALESCE(draft_category, 'standard') AS draft_category, "
            "    years_active, total_picks, total_keeper_picks, total_cost "
            "  FROM {table_prefix}draft_manager_career "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.draft_category = a.draft_category "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.years_active, 0) <> COALESCE(a.years_active, 0) "
            "   OR COALESCE(e.total_picks, 0) <> COALESCE(a.total_picks, 0) "
            "   OR COALESCE(e.total_keeper_picks, 0) <> COALESCE(a.total_keeper_picks, 0) "
            "   OR ABS(COALESCE(e.total_cost, 0) - COALESCE(a.total_cost, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_draft", "draft_agg_tables_exist"],
        fix_action="reimport",
    ),
    Check(
        name="draft_player_career_matches_draft_rollup",
        page="draft",
        table="draft_player_career",
        severity="ERROR",
        description="draft_player_career totals drift from grouped draft rows",
        sql_full=(
            "WITH expected AS ("
            "  SELECT db_name, player, "
            "    NULLIF(TRIM(position), '') AS position, "
            "    COALESCE(draft_category, 'standard') AS draft_category, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) = 0 THEN 1 ELSE 0 END) AS times_drafted, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) <> 0 THEN 1 ELSE 0 END) AS times_kept, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) = 0 THEN COALESCE(cost, 0) ELSE 0 END) AS total_cost, "
            "    SUM(CASE WHEN COALESCE(is_keeper, 0) <> 0 THEN COALESCE(cost, 0) ELSE 0 END) AS keeper_cost "
            "  FROM {table_prefix}draft "
            "  WHERE db_name IN ({league_list}) "
            "    AND player IS NOT NULL AND TRIM(player) <> '' "
            "    AND NULLIF(TRIM(position), '') IS NOT NULL "
            "  GROUP BY db_name, player, "
            "    NULLIF(TRIM(position), ''), "
            "    COALESCE(draft_category, 'standard')"
            "), "
            "actual AS ("
            "  SELECT db_name, player, position, COALESCE(draft_category, 'standard') AS draft_category, "
            "    times_drafted, times_kept, total_cost, keeper_cost "
            "  FROM {table_prefix}draft_player_career "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.player = a.player "
            " AND e.position = a.position "
            " AND e.draft_category = a.draft_category "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.times_drafted, 0) <> COALESCE(a.times_drafted, 0) "
            "   OR COALESCE(e.times_kept, 0) <> COALESCE(a.times_kept, 0) "
            "   OR ABS(COALESCE(e.total_cost, 0) - COALESCE(a.total_cost, 0)) > 0.01 "
            "   OR ABS(COALESCE(e.keeper_cost, 0) - COALESCE(a.keeper_cost, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_draft", "draft_agg_tables_exist"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 15. draft_position_populated
    #     Excludes 'Unknown'/'N/A' placeholder rows where the player itself
    #     wasn't identified at fetch time — there's no signal to infer a
    #     position for an unidentifiable player. backfill_draft_positions
    #     fills the rest via player_bio (NFL_player_id join + name fallback).
    Check(
        name="draft_position_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="position NULL on draft row (excluding Unknown/N/A placeholders)",
        sql_expr=(
            "SUM(CASE WHEN position IS NULL "
            "AND LOWER(TRIM(COALESCE(player, ''))) NOT IN ('unknown', 'n/a', '') "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 16. draft_grade_values_valid
    Check(
        name="draft_grade_values_valid",
        page="draft",
        table="draft",
        severity="WARNING",
        description="draft_grade value outside valid set (A+–F)",
        sql_expr=(
            f"SUM(CASE WHEN draft_grade IS NOT NULL " f"AND draft_grade NOT IN {_VALID_GRADES} " f"THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 17–24: REMOVED — total_fantasy_points, season_ppg, games_played,
    # manager_lamar, draft_grade, pick_quality_zscore are join-derived columns
    # populated at query time via player key joins, not stored in the draft table.
    # 25. draft_round_populated
    Check(
        name="draft_round_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="round NULL on draft row",
        sql_expr="SUM(CASE WHEN round IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 26. draft_report_card_columns
    #     Scope: only main drafts (max_round >= 10) for years that have
    #     matchup data — i.e. the season was actually played. Dynasty rookie
    #     drafts (typically 4-7 rounds) aren't graded because they have
    #     different semantics (rookie pick value vs main draft player value).
    #     Mock drafts for unstarted seasons can't be graded yet — there's no
    #     manager_lamar feeding the grade calc, so all picks would come back
    #     NULL legitimately. Startup drafts (25-40 rounds) for played
    #     seasons are included.
    Check(
        name="draft_report_card_columns",
        page="draft",
        table="draft",
        severity="WARNING",
        description="manager_draft_grade NULL on draft row (main draft, played seasons)",
        sql_full=(
            "SELECT db_name, SUM(null_picks) AS fail_count "
            "FROM ("
            "  SELECT d.db_name, d.year, "
            "    SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 "
            "      AND d.manager_draft_grade IS NULL THEN 1 ELSE 0 END) AS null_picks, "
            "    SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 THEN 1 ELSE 0 END) AS non_keeper_picks "
            "  FROM {table_prefix}draft d "
            "  WHERE d.db_name IN ({league_list}) "
            "    AND EXISTS ( "
            "      SELECT 1 FROM {table_prefix}matchup m "
            "      WHERE m.db_name = d.db_name AND m.year = d.year "
            "    ) "
            "  GROUP BY d.db_name, d.year "
            "  HAVING MAX(d.round) >= 10 "
            "    AND non_keeper_picks > 0"
            ") t "
            "GROUP BY db_name "
            "HAVING SUM(null_picks) > 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 27. draft_breakout_bust_populated
    #     NOTE: is_breakout and is_bust columns do not exist in ___leagues DDL.
    #     Check removed — these columns were never added to the centralized schema.
    # 28. draft_hit_rate_populated
    Check(
        name="draft_hit_rate_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="manager_hit_rate NULL on draft row",
        sql_expr="SUM(CASE WHEN manager_hit_rate IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 29. draft_roi_populated
    #     NOTE: draft_roi and lamar_per_dollar do not exist in ___leagues DDL.
    #     Check removed — these columns were never added to the centralized schema.
    # 30: REMOVED — draft_age is join-derived, populated at query time.
    # 31. draft_manager_score_populated
    #     Scope: only main drafts (max_round >= 10) for years that have
    #     matchup data. Dynasty rookie drafts (typically 4-7 rounds) and
    #     mock drafts for unstarted seasons are intentionally excluded —
    #     `manager_draft_score` derives from manager_lamar, which doesn't
    #     exist until a season has been played.
    Check(
        name="draft_manager_score_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="manager_draft_score NULL on draft row (main draft, played seasons)",
        sql_full=(
            "SELECT db_name, SUM(null_picks) AS fail_count "
            "FROM ("
            "  SELECT d.db_name, d.year, "
            "    SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 "
            "      AND d.manager_draft_score IS NULL THEN 1 ELSE 0 END) AS null_picks, "
            "    SUM(CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0 THEN 1 ELSE 0 END) AS non_keeper_picks "
            "  FROM {table_prefix}draft d "
            "  WHERE d.db_name IN ({league_list}) "
            "    AND EXISTS ( "
            "      SELECT 1 FROM {table_prefix}matchup m "
            "      WHERE m.db_name = d.db_name AND m.year = d.year "
            "    ) "
            "  GROUP BY d.db_name, d.year "
            "  HAVING MAX(d.round) >= 10 "
            "    AND non_keeper_picks > 0"
            ") t "
            "GROUP BY db_name "
            "HAVING SUM(null_picks) > 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 32. draft_alltime_percentile_populated
    Check(
        name="draft_alltime_percentile_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="manager_draft_percentile_alltime NULL on draft row",
        sql_expr="SUM(CASE WHEN manager_draft_percentile_alltime IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 33. draft_category_populated (dynasty leagues only)
    Check(
        name="draft_category_populated",
        page="draft",
        table="draft",
        severity="WARNING",
        description="draft_category NULL on draft row (dynasty league)",
        sql_expr=(
            "SUM(CASE "
            "WHEN LOWER(COALESCE(platform, '')) = 'sleeper' AND draft_category IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        feature="dynasty",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 34. draft_keeper_flag_exists (keeper leagues, INFO)
    #     Returns leagues where is_keeper is entirely NULL (flag missing).
    Check(
        name="draft_keeper_flag_exists",
        page="draft",
        table="draft",
        severity="INFO",
        description="is_keeper column all NULL (keeper flag missing for keeper league)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM {table_prefix}draft "
            "WHERE db_name IN ({league_list}) "
            "GROUP BY db_name "
            "HAVING SUM(CASE WHEN is_keeper IS NOT NULL THEN 1 ELSE 0 END) = 0"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 35: REMOVED — keeper_draft_grade is join-derived, populated at query time.
]
