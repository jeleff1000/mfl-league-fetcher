"""Manager identity checks — 4 checks verifying franchise_id integrity.

All checks use either sql_expr (batched) or sql_full (standalone).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # 1. identity_franchise_id_not_null
    #    franchise_id is the primary join key for everything; it must
    #    never be NULL on real (non-bye, non-placeholder) rows.
    # ------------------------------------------------------------------
    Check(
        name="identity_franchise_id_not_null",
        page="identity",
        table="matchup",
        severity="ERROR",
        description="franchise_id NULL (primary join key for everything)",
        sql_expr=("SUM(CASE WHEN franchise_id IS NULL AND is_bye_week = 0 THEN 1 ELSE 0 END)"),
        batch_group="core",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 2. identity_guid_one_franchise
    #    A (manager_guid, manager_name) should map to exactly one franchise_id
    #    within a league. Multiple franchise_ids for the SAME guid with DIFFERENT
    #    manager names is LEGITIMATE — the disambiguation system intentionally
    #    creates different franchise_ids when one person owns multiple teams
    #    (co-commish, multi-team dynasty). We only flag when the same (guid,
    #    manager_name) has multiple franchise_ids — that's real data corruption.
    #    Also handles cross-platform continuity (same person on Yahoo + ESPN)
    #    where platforms produce different franchise_ids for the same person.
    # ------------------------------------------------------------------
    Check(
        name="identity_guid_one_franchise",
        page="identity",
        table="matchup",
        severity="ERROR",
        description="(manager_guid, manager_name) maps to multiple franchise_ids",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  WITH base AS ("
            "    SELECT db_name, year, week, manager_guid, manager, franchise_id, team_name "
            "    FROM {table_prefix}matchup "
            "    WHERE manager_guid IS NOT NULL AND franchise_id IS NOT NULL "
            "      AND manager IS NOT NULL "
            "      AND db_name IN ({league_list})"
            "  ), same_week_multi_team AS ("
            "    SELECT DISTINCT db_name, manager_guid "
            "    FROM base "
            "    GROUP BY db_name, manager_guid, year, week "
            "    HAVING COUNT(DISTINCT franchise_id) > 1 "
            "       AND COUNT(DISTINCT COALESCE(team_name, franchise_id)) > 1"
            "  ) "
            "  SELECT b.db_name, b.manager_guid, b.manager, COUNT(DISTINCT b.franchise_id) AS fid_count "
            "  FROM base b "
            "  LEFT JOIN same_week_multi_team s "
            "    ON s.db_name = b.db_name "
            "   AND s.manager_guid = b.manager_guid "
            "  WHERE s.manager_guid IS NULL "
            "  GROUP BY b.db_name, b.manager_guid, b.manager "
            "  HAVING COUNT(DISTINCT b.franchise_id) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        fix_action="reimport",
    ),
    Check(
        name="identity_yahoo_guid_unexplained_franchise_split",
        page="identity",
        table="matchup",
        severity="ERROR",
        description="Yahoo manager_guid maps to multiple franchise_ids in the same season without same-week multi-team evidence",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  WITH base AS ("
            "    SELECT db_name, year, week, manager_guid, franchise_id, team_name "
            "    FROM {table_prefix}matchup "
            "    WHERE platform = 'yahoo' "
            "      AND manager_guid IS NOT NULL "
            "      AND LOWER(TRIM(CAST(manager_guid AS VARCHAR))) NOT IN ('', '--', '--hidden--', 'none', 'nan', '<na>') "
            "      AND franchise_id IS NOT NULL "
            "      AND LOWER(TRIM(CAST(franchise_id AS VARCHAR))) NOT IN ('', '--', '--hidden--', 'none', 'nan', '<na>') "
            "      AND db_name IN ({league_list})"
            "  ), per_guid AS ("
            "    SELECT db_name, manager_guid, COUNT(DISTINCT franchise_id) AS fid_count "
            "    FROM base "
            "    GROUP BY db_name, manager_guid "
            "    HAVING COUNT(DISTINCT franchise_id) > 1"
            "  ), same_year_multi_fid AS ("
            "    SELECT DISTINCT db_name, manager_guid "
            "    FROM base "
            "    GROUP BY db_name, manager_guid, year "
            "    HAVING COUNT(DISTINCT franchise_id) > 1"
            "  ), same_week_multi_team AS ("
            "    SELECT DISTINCT db_name, manager_guid "
            "    FROM base "
            "    GROUP BY db_name, manager_guid, year, week "
            "    HAVING COUNT(DISTINCT franchise_id) > 1 "
            "       AND COUNT(DISTINCT COALESCE(team_name, franchise_id)) > 1"
            "  ) "
            "  SELECT p.db_name, p.manager_guid "
            "  FROM per_guid p "
            "  LEFT JOIN same_week_multi_team s "
            "    ON s.db_name = p.db_name "
            "   AND s.manager_guid = p.manager_guid "
            "  JOIN same_year_multi_fid y "
            "    ON y.db_name = p.db_name "
            "   AND y.manager_guid = p.manager_guid "
            "  WHERE s.manager_guid IS NULL"
            ") "
            "GROUP BY db_name"
        ),
        feature="yahoo",
        batch_group="core",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 3. identity_franchise_year_continuity
    #    A franchise appearing in only one year is unusual and worth
    #    flagging. Could indicate an incomplete import or a mid-season
    #    manager change that wasn't resolved.
    # ------------------------------------------------------------------
    Check(
        name="identity_franchise_year_continuity",
        page="identity",
        table="matchup",
        severity="INFO",
        description="Franchise appears for <2 years (possible but flagged)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, franchise_id, COUNT(DISTINCT year) AS year_count "
            "  FROM {table_prefix}matchup "
            "  WHERE franchise_id IS NOT NULL "
            "    AND db_name IN ({league_list}) "
            "  GROUP BY db_name, franchise_id "
            "  HAVING COUNT(DISTINCT year) < 2"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # 4. identity_franchise_id_cross_table
    #    Every franchise_id in player_fantasy must also exist in matchup.
    #    Orphaned franchise_ids indicate a join-key mismatch between
    #    the weekly player data and the matchup table.
    # ------------------------------------------------------------------
    Check(
        name="identity_franchise_id_cross_table",
        page="identity",
        table="player_fantasy",
        severity="ERROR",
        description="franchise_id in player_fantasy doesn't match matchup",
        sql_full=(
            "SELECT pf.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT pf.db_name, pf.year, pf.franchise_id "
            "  FROM {table_prefix}player_fantasy pf "
            "  WHERE pf.franchise_id IS NOT NULL "
            "    AND pf.year IS NOT NULL "
            "    AND pf.db_name IN ({league_list}) "
            "    AND EXISTS ("
            "      SELECT 1 "
            "      FROM {table_prefix}matchup scope_m "
            "      WHERE scope_m.db_name = pf.db_name "
            "        AND scope_m.year = pf.year"
            "    )"
            ") pf "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, franchise_id "
            "  FROM {table_prefix}matchup "
            "  WHERE franchise_id IS NOT NULL "
            "    AND year IS NOT NULL"
            ") m ON pf.db_name = m.db_name "
            "  AND pf.year = m.year "
            "  AND pf.franchise_id = m.franchise_id "
            "WHERE m.franchise_id IS NULL "
            "GROUP BY pf.db_name"
        ),
        batch_group="core",
        depends_on=["completeness_has_matchup", "completeness_has_player_fantasy"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # 5. identity_external_synthetic_guids
    #    Synthetic external_* manager_guids are created by
    #    external_ingest.apply_mappings.synthesize_external_guid() when a
    #    user chose "create_new" (or left a manager unresolved). Their
    #    presence is not necessarily wrong, but signals that external
    #    alias mappings may be incomplete — an operator should verify.
    # ------------------------------------------------------------------
    Check(
        name="identity_external_synthetic_guids",
        page="identity",
        table="matchup",
        severity="INFO",
        description="Synthetic external_* manager_guids detected — verify external mappings are complete",
        sql_full=(
            "SELECT db_name, COUNT(DISTINCT manager_guid) AS fail_count "
            "FROM {table_prefix}matchup "
            "WHERE manager_guid LIKE 'external_%' "
            "  AND franchise_id IS NOT NULL "
            "  AND db_name IN ({league_list}) "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="cheap",
    ),
]
