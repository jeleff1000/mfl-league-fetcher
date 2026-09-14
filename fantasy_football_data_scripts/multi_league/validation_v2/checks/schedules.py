# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /home (schedule display), /matchups
"""Schedule checks — 3 checks verifying the schedule table integrity.

Mix of sql_full and sql_expr checks.
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Coverage — schedule must mirror matchup week/year combos
    # ------------------------------------------------------------------
    # 1. schedules_exists_for_matchup_weeks
    #    Every (db_name, year, week) combination present in the matchup
    #    table should also have at least one row in the schedule table.
    Check(
        name="schedules_exists_for_matchup_weeks",
        page="schedules",
        table="schedule",
        severity="WARNING",
        description="schedule rows missing for matchup year/week combinations",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year, week "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, week "
            "  FROM {table_prefix}schedule"
            ") s "
            "  ON m.db_name = s.db_name "
            "  AND m.year = s.year "
            "  AND m.week = s.week "
            "WHERE s.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_schedule"],
    ),
    # ------------------------------------------------------------------
    # ERROR: Mutual exclusion — playoffs and consolation can't both be 1
    # ------------------------------------------------------------------
    # 2. schedules_playoff_consolation_exclusive
    #    is_playoffs=1 AND is_consolation=1 on the same row is impossible —
    #    a game is either a playoff game or a consolation game, never both.
    Check(
        name="schedules_playoff_consolation_exclusive",
        page="schedules",
        table="schedule",
        severity="ERROR",
        description="schedule row has is_playoffs=1 AND is_consolation=1 (mutually exclusive)",
        sql_expr=("SUM(CASE WHEN is_playoffs = 1 " "AND is_consolation = 1 " "THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Bye week flagging
    # ------------------------------------------------------------------
    # 3. schedules_bye_weeks_flagged
    #    NOTE: is_bye_week column does not exist in ___leagues schedule DDL.
    #    Check removed — schedule table uses postseason column instead.
]
