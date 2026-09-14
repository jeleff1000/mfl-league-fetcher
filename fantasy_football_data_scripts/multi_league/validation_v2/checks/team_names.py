# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /home, /managers (team name display)
"""Team name checks — 2 checks verifying team_name integrity in the matchup
table.

All checks use sql_expr (batched).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Team name integrity
    # ------------------------------------------------------------------
    # 1. teamnames_every_franchise_has_name
    #    Any row with a franchise_id should also have a team_name so the
    #    UI can display proper team identity rather than a blank card.
    Check(
        name="teamnames_every_franchise_has_name",
        page="team_names",
        table="matchup",
        severity="WARNING",
        description="franchise_id populated but no usable display name (team_name/franchise_name/manager)",
        sql_expr=(
            "SUM(CASE WHEN franchise_id IS NOT NULL "
            "AND team_name IS NULL "
            "AND NULLIF(TRIM(COALESCE(franchise_name, '')), '') IS NULL "
            "AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 2. teamnames_name_not_empty
    #    team_name should not be an empty string after trimming whitespace.
    Check(
        name="teamnames_name_not_empty",
        page="team_names",
        table="matchup",
        severity="WARNING",
        description="team_name is blank and no franchise_name/manager fallback exists",
        sql_expr=(
            "SUM(CASE WHEN team_name IS NOT NULL "
            "AND TRIM(team_name) = '' "
            "AND NULLIF(TRIM(COALESCE(franchise_name, '')), '') IS NULL "
            "AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
]
