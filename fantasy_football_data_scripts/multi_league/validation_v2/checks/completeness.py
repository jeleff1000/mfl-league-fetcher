"""Completeness checks — 9 checks verifying each league has data in each table.

All checks use sql_full (standalone queries returning (db_name, fail_count) rows).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check
from multi_league.validation_v2.scope_sql import published_league_scope_sql

_PUBLISHED_SCOPE = published_league_scope_sql()

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # 1. completeness_has_matchup
    #    League in inventory but no rows in matchup.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_matchup",
        page="completeness",
        table="matchup",
        severity="BLOCKER",
        description="League in inventory but no matchup data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 2. completeness_has_settings
    #    League in inventory but no rows in league_settings.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_settings",
        page="completeness",
        table="league_settings",
        severity="BLOCKER",
        description="League in inventory but no league_settings data",
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
    # 3. completeness_has_player_fantasy
    #    League in inventory but no rows in player_fantasy.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_player_fantasy",
        page="completeness",
        table="player_fantasy",
        severity="BLOCKER",
        description="League in inventory but no player_fantasy data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 4. completeness_has_draft
    #    League in inventory but no rows in draft.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_draft",
        page="completeness",
        table="draft",
        severity="WARNING",
        description="League in inventory but no draft data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}draft"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 5. completeness_has_transactions
    #    League in inventory but no rows in transactions.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_transactions",
        page="completeness",
        table="transactions",
        severity="WARNING",
        description="League in inventory but no transactions data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transactions"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 6. completeness_has_schedule
    #    League in inventory but no rows in schedule.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_schedule",
        page="completeness",
        table="schedule",
        severity="WARNING",
        description="League in inventory but no schedule data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}schedule"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 7. completeness_has_homepage_tables
    #    League in inventory but no rows in homepage_league_summary.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_homepage_tables",
        page="completeness",
        table="homepage_league_summary",
        severity="WARNING",
        description="League in inventory but no homepage_league_summary data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}homepage_league_summary"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 8. completeness_has_season_agg
    #    League in inventory but no rows in matchup_season.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_season_agg",
        page="completeness",
        table="matchup_season",
        severity="WARNING",
        description="League in inventory but no matchup_season data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup_season"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 9. completeness_has_career_agg
    #    League in inventory but no rows in matchup_career.
    #    Only applies to multi_year leagues — single-year leagues don't
    #    need career aggregation tables.
    # ------------------------------------------------------------------
    Check(
        name="completeness_has_career_agg",
        page="completeness",
        table="matchup_career",
        severity="WARNING",
        description="League in inventory but no matchup_career data",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) inv "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup_career"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        feature="multi_year",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
]
