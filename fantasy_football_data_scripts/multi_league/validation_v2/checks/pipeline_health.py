# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /system (pipeline health signals)
"""Pipeline health checks — 14 checks verifying that import pipeline
enrichment steps ran successfully.

Checks 1–6 use sql_full because they detect the *absence* of data
(fail when count = 0).  The executor's sql_expr pattern only supports
detecting the *presence* of bad data (fail when count > 0).

Checks 7–14 use sql_expr where failure = positive row count.

The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Enrichment ran — full import only
    # ------------------------------------------------------------------
    # 1. pipeline_nfl_expansion_ran
    #    Full imports should include unrostered/FA rows from expand_to_all_nfl.
    #    If no unrostered rows exist, the expansion step didn't run and
    #    league-wide optimal calculations will be incorrect.
    Check(
        name="pipeline_nfl_expansion_ran",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="No unrostered/FA rows in player_fantasy (NFL expansion may not have run)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN LOWER(TRIM(COALESCE(manager, ''))) IN "
            "        ('unrostered', 'fa', 'free agent', 'waivers') "
            "    THEN 1 ELSE 0 END) AS unrostered_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE unrostered_count = 0"
        ),
        feature="full_import",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 2. pipeline_lamar_calculated
    #    manager_lamar should be populated for started players.
    #    All-NULL means the LAMAR enrichment didn't run.
    Check(
        name="pipeline_lamar_calculated",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="manager_lamar all NULL for started players (LAMAR enrichment didn't run)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN manager_lamar IS NOT NULL AND is_started = 1 "
            "    THEN 1 ELSE 0 END) AS populated_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_started = 1 "
            "  GROUP BY db_name"
            ") "
            "WHERE populated_count = 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 3. pipeline_lamar_nonzero
    #    player_lamar should have non-zero values in a full import.
    #    All zeros suggest the enrichment ran but produced bad output.
    Check(
        name="pipeline_lamar_nonzero",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="player_lamar all zero (LAMAR enrichment may have produced bad output)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN ABS(COALESCE(player_lamar, 0)) > 0.001 "
            "    THEN 1 ELSE 0 END) AS nonzero_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE nonzero_count = 0"
        ),
        feature="full_import",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 4. pipeline_optimal_calculated
    #    At least some rows should have optimal_player=1 after the
    #    optimal lineup enrichment runs.
    Check(
        name="pipeline_optimal_calculated",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="No rows with optimal_player=1 (optimal lineup enrichment didn't run)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN optimal_player = 1 THEN 1 ELSE 0 END) AS opt_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE opt_count = 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 5. pipeline_rankings_populated
    #    position_week_rank should be populated after the rankings
    #    enrichment step.  All-NULL means rankings didn't run.
    Check(
        name="pipeline_rankings_populated",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="position_week_rank all NULL (rankings enrichment didn't run)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN position_week_rank IS NOT NULL THEN 1 ELSE 0 END) "
            "      AS rank_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE rank_count = 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 6. pipeline_ppg_populated
    #    season_ppg should be populated after stats enrichment.
    #    NOTE: ___leagues DDL has season_ppg but not ppg on player_fantasy.
    Check(
        name="pipeline_ppg_populated",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="season_ppg all NULL (PPG stats enrichment didn't run)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN season_ppg IS NOT NULL "
            "    THEN 1 ELSE 0 END) AS ppg_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") "
            "WHERE ppg_count = 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 7: REMOVED — duplicate of players_fantasy_points_populated (ERROR severity,
    # placeholder-aware). This WARNING version was strictly less accurate because
    # it would flag legit placeholder NFL_player_ids (ESPN-, SLP-, PICK_) as failures.
    # ------------------------------------------------------------------
    # ERROR: Franchise ID
    # ------------------------------------------------------------------
    # 8. pipeline_franchise_id_populated
    #    franchise_id must be populated for all rostered players.
    #    NULL franchise_ids will cause broken joins across all pages.
    Check(
        name="pipeline_franchise_id_populated",
        page="pipeline_health",
        table="player_fantasy",
        severity="ERROR",
        description="franchise_id NULL for rostered player rows",
        sql_expr=(
            "SUM(CASE WHEN franchise_id IS NULL "
            "AND manager IS NOT NULL "
            "AND TRIM(COALESCE(manager, '')) != '' "
            "AND LOWER(TRIM(manager)) NOT IN "
            "    ('unrostered', 'fa', 'free agent', 'waivers') "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Position coverage (full query needed)
    # ------------------------------------------------------------------
    # 9. pipeline_position_counts
    #    Each year should have at least 4 QB, 8 RB, and 8 WR rows in
    #    player_fantasy.  Lower counts suggest the expansion or roster
    #    population is incomplete.
    Check(
        name="pipeline_position_counts",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="<4 QBs or <8 RBs or <8 WRs per year (roster coverage incomplete)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, year, "
            "    SUM(CASE WHEN position = 'QB' THEN 1 ELSE 0 END) AS qb_count, "
            "    SUM(CASE WHEN position = 'RB' THEN 1 ELSE 0 END) AS rb_count, "
            "    SUM(CASE WHEN position = 'WR' THEN 1 ELSE 0 END) AS wr_count "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name, year "
            "  HAVING qb_count < 4 OR rb_count < 8 OR wr_count < 8"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: DEF coverage
    # ------------------------------------------------------------------
    # 10. pipeline_def_coverage
    #     There should be DEF/DST rows in player_fantasy for each year,
    #     but ONLY for leagues that actually roster a team DEF/DST. IDP
    #     leagues (individual defenders — DL/LB/DB slots instead of DEF)
    #     have no team DEF rows by design, so they're scoped out of the
    #     check using league_settings.roster_DEF. degen is the canonical
    #     IDP-only example: roster_DEF IS NULL, roster_DB/DL/LB/IDP > 0.
    Check(
        name="pipeline_def_coverage",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="No DEF rows in player_fantasy for some years",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT mx.db_name, mx.year "
            "  FROM {table_prefix}matchup mx "
            "  INNER JOIN {table_prefix}league_settings ls "
            "    ON ls.db_name = mx.db_name AND ls.year = mx.year "
            "  WHERE mx.db_name IN ({league_list}) "
            "    AND mx.is_bye_week = 0 "
            "    AND COALESCE(ls.roster_DEF, 0) > 0"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE position = 'DEF'"
            ") d "
            "  ON m.db_name = d.db_name "
            "  AND m.year = d.year "
            "WHERE d.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 11. pipeline_def_format
    #     DEF player_week values should follow the canonical franchise-id pattern
    #     'DEF-{franchise_id}_{year}_{week}' (e.g., 'DEF-22_2024_1'). The DEF-N
    #     format disambiguates NFL franchise relocations/renames across years
    #     (see sleeper_data_normalizer.normalize_dst_id). Team abbreviations
    #     (KC, PHI) are NOT used because they break historical continuity.
    Check(
        name="pipeline_def_format",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="DEF player_week not matching expected 'DEF-{id}_{year}_{week}' format",
        sql_expr=(
            "SUM(CASE WHEN position = 'DEF' "
            "AND player_week IS NOT NULL "
            "AND player_week NOT SIMILAR TO "
            "    'DEF-[0-9]+_[0-9]{4}_[0-9]+' "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 12. pipeline_def_points
    #     Started DEF rows must have fantasy_points populated.
    Check(
        name="pipeline_def_points",
        page="pipeline_health",
        table="player_fantasy",
        severity="WARNING",
        description="Started DEF row missing fantasy_points",
        sql_expr=("SUM(CASE WHEN position = 'DEF' AND is_started = 1 AND fantasy_points IS NULL THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # ERROR: NFL ID resolution
    # ------------------------------------------------------------------
    # 13. pipeline_nfl_id_zero_pct
    #     At least some rostered rows should have a resolved NFL_player_id.
    #     Zero resolved IDs means the ID mapping pipeline completely failed.
    #     STUB rows are intentional team/week scaffolding for private roster
    #     feeds and do not represent player rows that can resolve to NFL IDs.
    Check(
        name="pipeline_nfl_id_zero_pct",
        page="pipeline_health",
        table="player_fantasy",
        severity="ERROR",
        description="Zero NFL_player_ids resolved for league (ID mapping pipeline failed)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    COUNT(*) AS total, "
            "    SUM(CASE WHEN NFL_player_id IS NOT NULL THEN 1 ELSE 0 END) "
            "      AS resolved "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE db_name IN ({league_list}) "
            "    AND manager IS NOT NULL "
            "    AND TRIM(COALESCE(manager, '')) != '' "
            "    AND LOWER(TRIM(manager)) NOT IN "
            "        ('unrostered', 'fa', 'free agent', 'waivers') "
            "    AND COALESCE(position, '') != 'STUB' "
            "  GROUP BY db_name"
            ") "
            "WHERE total > 0 AND resolved = 0"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: is_started integrity
    # ------------------------------------------------------------------
    # 14. pipeline_is_started_binary
    #     is_started must be 0 or 1 — any other value indicates a type
    #     conversion or pipeline error.
    Check(
        name="pipeline_is_started_binary",
        page="pipeline_health",
        table="player_fantasy",
        severity="ERROR",
        description="is_started NOT IN (0, 1) — invalid value detected",
        sql_expr=("SUM(CASE WHEN is_started IS NOT NULL AND is_started NOT IN (0, 1) THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
]
