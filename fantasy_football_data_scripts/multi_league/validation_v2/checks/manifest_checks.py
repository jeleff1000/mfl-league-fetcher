"""Manifest validation checks — 6 checks that verify settings flags match actual data.

All checks use sql_full (standalone queries returning (db_name, fail_count) rows).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check
from multi_league.validation_v2.scope_sql import published_league_scope_sql

_PUBLISHED_SCOPE = published_league_scope_sql()

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # 1. manifest_settings_complete
    #    League is in inventory but has no rows in league_settings.
    # ------------------------------------------------------------------
    Check(
        name="manifest_settings_complete",
        page="manifest",
        table="league_settings",
        severity="BLOCKER",
        description="League in inventory but has no settings rows",
        sql_full=(
            # All published leagues scoped to {league_list}
            # Left-joined to settings; missing settings = failure.
            "SELECT li.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) li "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}league_settings"
            ") ls ON li.db_name = ls.db_name "
            "WHERE ls.db_name IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # 2. manifest_median_has_columns
    #    League is flagged uses_median but above_league_median is all NULL.
    # ------------------------------------------------------------------
    Check(
        name="manifest_median_has_columns",
        page="manifest",
        table="matchup",
        severity="ERROR",
        description="League flagged uses_median but above_league_median all NULL",
        sql_full=(
            # Start from leagues that are in the median scope ({league_list}),
            # then check whether any row has above_league_median NOT NULL.
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE above_league_median IS NOT NULL "
            "  GROUP BY db_name"
            ") has_data ON m.db_name = has_data.db_name "
            "WHERE has_data.db_name IS NULL"
        ),
        feature="median",
        batch_group="core",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # 3. manifest_full_import_has_lamar
    #    League is flagged full_import but player_lamar is all NULL/zero.
    # ------------------------------------------------------------------
    Check(
        name="manifest_full_import_has_lamar",
        page="manifest",
        table="player_fantasy",
        severity="ERROR",
        description="League flagged full_import but player_lamar all NULL/zero",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list})"
            ") pf "
            "LEFT JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE player_lamar IS NOT NULL AND player_lamar != 0 "
            "  GROUP BY db_name"
            ") has_lamar ON pf.db_name = has_lamar.db_name "
            "WHERE has_lamar.db_name IS NULL"
        ),
        feature="full_import",
        batch_group="core",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # 4. manifest_consolation_has_flags
    #    League is flagged consolation but is_consolation never 1.
    # ------------------------------------------------------------------
    Check(
        name="manifest_consolation_has_flags",
        page="manifest",
        table="matchup",
        severity="ERROR",
        description="League flagged consolation but is_consolation never 1",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE is_consolation = 1 "
            "  GROUP BY db_name"
            ") has_consol ON m.db_name = has_consol.db_name "
            "WHERE has_consol.db_name IS NULL"
        ),
        feature="consolation",
        batch_group="core",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # 5. manifest_keeper_has_column
    #    League is flagged keeper but is_keeper is all NULL.
    # ------------------------------------------------------------------
    Check(
        name="manifest_keeper_has_column",
        page="manifest",
        table="player_fantasy",
        severity="WARNING",
        description="League flagged keeper but is_keeper all NULL",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list})"
            ") pf "
            "LEFT JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE is_keeper IS NOT NULL "
            "  GROUP BY db_name"
            ") has_keeper ON pf.db_name = has_keeper.db_name "
            "WHERE has_keeper.db_name IS NULL"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # 6. manifest_sim_has_x_win
    #    League expected sim data but x0_win is all NULL.
    # ------------------------------------------------------------------
    Check(
        name="manifest_sim_has_x_win",
        page="manifest",
        table="matchup",
        severity="WARNING",
        description="League expected sim data but x0_win all NULL",
        sql_full=(
            "SELECT m.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE x0_win IS NOT NULL "
            "  GROUP BY db_name"
            ") has_sim ON m.db_name = has_sim.db_name "
            "WHERE has_sim.db_name IS NULL"
        ),
        feature="sim",
        batch_group="quality",
        cost_tier="cheap",
    ),
]
