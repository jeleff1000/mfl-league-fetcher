# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /settings (league settings display)
"""League settings checks — 11 checks verifying the league_settings table
integrity and required field coverage.

Mix of sql_full and sql_expr checks.
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check
from multi_league.validation_v2.scope_sql import published_league_scope_sql

_PUBLISHED_SCOPE = published_league_scope_sql()

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # BLOCKER: settings table must exist and have rows
    # ------------------------------------------------------------------
    # 1. settings_exists
    #    league_settings must have at least one row per league.
    Check(
        name="settings_exists",
        page="league_settings",
        table="league_settings",
        severity="BLOCKER",
        description="No rows in league_settings for league",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}league_settings"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Year coverage — settings must exist for every season
    # ------------------------------------------------------------------
    # 2. settings_for_all_years
    #    Every year present in the matchup table must also have a row in
    #    league_settings.  Missing settings years means enrichments and
    #    canonical scoring can't be applied for those seasons.
    Check(
        name="settings_for_all_years",
        page="league_settings",
        table="league_settings",
        severity="ERROR",
        description="league_settings missing rows for some matchup years",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}league_settings"
            ") s "
            "  ON m.db_name = s.db_name "
            "  AND m.year = s.year "
            "WHERE s.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["settings_exists", "completeness_has_matchup"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Required fields
    # ------------------------------------------------------------------
    # 3. settings_platform_defined
    #    platform must be populated — it determines which importer runs
    #    and which column helpers are selected.
    Check(
        name="settings_platform_defined",
        page="league_settings",
        table="league_settings",
        severity="ERROR",
        description="platform NULL in league_settings",
        sql_expr="SUM(CASE WHEN platform IS NULL THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 4. settings_required_fields
    #    platform, num_teams, and playoff_start_week are the three fields
    #    that every league-year row must have for the site to function.
    Check(
        name="settings_required_fields",
        page="league_settings",
        table="league_settings",
        severity="ERROR",
        description="Required fields NULL: platform, num_teams, or playoff_start_week",
        sql_expr=(
            "SUM(CASE WHEN platform IS NULL "
            "OR num_teams IS NULL "
            "OR playoff_start_week IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Scoring fields
    # ------------------------------------------------------------------
    # 5. settings_scoring_populated
    #    scoring_rec (PPR setting) should be populated for all league-years.
    Check(
        name="settings_scoring_populated",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="scoring_rec NULL in league_settings (PPR setting unknown)",
        sql_expr="SUM(CASE WHEN scoring_rec IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 6. settings_ppr_valid
    #    scoring_rec (PPR) must be non-negative. Sleeper supports arbitrary
    #    PPR values (e.g., 0.25, 2.0), so we only reject negative values.
    Check(
        name="settings_ppr_valid",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="scoring_rec is negative (invalid PPR value)",
        sql_expr=("SUM(CASE WHEN scoring_rec IS NOT NULL " "AND scoring_rec < 0 " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 7. settings_canonical_scoring
    #    scoring_pass_td should be populated (canonical scoring fields).
    #    If NULL, the enrichment pipeline couldn't derive scoring config
    #    and per-year LAMAR calculations will be inaccurate.
    Check(
        name="settings_canonical_scoring",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="scoring_pass_td NULL (canonical scoring fields not populated)",
        sql_expr="SUM(CASE WHEN scoring_pass_td IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: Structural fields
    # ------------------------------------------------------------------
    # 8. settings_num_teams_defined
    #    num_teams must be defined so the site can display league size
    #    context and validate records.
    Check(
        name="settings_num_teams_defined",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="num_teams NULL in league_settings",
        sql_expr="SUM(CASE WHEN num_teams IS NULL THEN 1 ELSE 0 END)",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 9. settings_num_teams_range
    #    num_teams should be between 4 and 32 — outside this range is
    #    almost certainly a data error.
    Check(
        name="settings_num_teams_range",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="num_teams outside valid range (4–32)",
        sql_expr=("SUM(CASE WHEN num_teams IS NOT NULL " "AND (num_teams < 4 OR num_teams > 32) " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 10. settings_playoff_start_week
    #     playoff_start_week should be a reasonable NFL week (10-18).
    #     Leagues with playoffs disabled use end_week + 1 so all played
    #     weeks remain regular season.
    Check(
        name="settings_playoff_start_week",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="playoff_start_week outside valid range (10-18, or end_week+1 for no-playoff leagues)",
        sql_expr=(
            "SUM(CASE WHEN playoff_start_week IS NOT NULL "
            "AND (playoff_start_week < 10 OR playoff_start_week > 18) "
            "AND NOT (COALESCE(playoff_teams, 0) = 0 "
            "         AND end_week IS NOT NULL "
            "         AND playoff_start_week = end_week + 1) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 11. settings_playoff_teams_range
    #     playoff_teams should be between 2 and 16, or 0 when the league has
    #     playoffs disabled.
    Check(
        name="settings_playoff_teams_range",
        page="league_settings",
        table="league_settings",
        severity="WARNING",
        description="playoff_teams outside valid range (0 for none, otherwise 2-16)",
        sql_expr=(
            "SUM(CASE WHEN playoff_teams IS NOT NULL "
            "AND playoff_teams != 0 "
            "AND (playoff_teams < 2 OR playoff_teams > 16) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
]
