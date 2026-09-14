# Owner: pipeline team
# Last audit: 2026-04-13
# Covers: /home (recap display)
"""Recap checks — 1 check verifying recap-specific fields.

The matchup_context and power_rating checks were removed as duplicates of
players_matchup_context_populated and matchups_power_rating_populated — they
ran identical SQL under different page tags and double-counted failures.

grade NULL remains here because it's a distinct predicate from
matchups_grade_values_valid (which checks letter-grade validity, not NULL).

All checks use sql_expr (batched).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Recap-specific quality
    # ------------------------------------------------------------------
    # 1. recaps_grade_populated
    #    Regular-season matchup rows should have a grade populated when Yahoo
    #    returned grades for that league-year so the recap engine can render
    #    performance grades without warning on historical/API-unavailable years.
    #    engine can render performance grades. `grade` is an api-source
    #    Yahoo-only column (canonical_matchup.py: `("grade", "VARCHAR", "api")`
    #    — Yahoo's XML returns A+/A/A-/.../F letter grades). Sleeper and ESPN
    #    APIs don't expose letter grades, so the check is scoped to Yahoo
    #    rather than counted as a failure on other platforms.
    Check(
        name="recaps_grade_populated",
        page="recaps",
        table="matchup",
        severity="WARNING",
        description="grade NULL on regular-season matchup rows in Yahoo league-years with grade coverage",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.grade IS NULL "
            "  AND m.is_bye_week = 0 "
            "  AND COALESCE(CAST(m.is_playoffs AS INT), 0) = 0 "
            "  AND COALESCE(CAST(m.is_consolation AS INT), 0) = 0 "
            "  AND m.platform = 'yahoo' "
            "  AND EXISTS ("
            "    SELECT 1 FROM {table_prefix}matchup g "
            "    WHERE g.db_name = m.db_name "
            "      AND g.year = m.year "
            "      AND g.grade IS NOT NULL "
            "      AND g.is_bye_week = 0 "
            "      AND COALESCE(CAST(g.is_playoffs AS INT), 0) = 0 "
            "      AND COALESCE(CAST(g.is_consolation AS INT), 0) = 0"
            "  ) "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
]
