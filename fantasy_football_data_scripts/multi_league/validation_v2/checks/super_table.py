# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /system (super table health)
"""Super table checks — 12 checks verifying the integrity and coverage of
___ops.nfl_historical.nfl_player_stats_all (the NFL stats super table).

All checks use sql_full with hardcoded references to the super table since
it lives in a separate database (___ops) rather than the per-league schema.
The {league_list} placeholder is present but unused in most checks — it is
kept for executor compatibility.  {table_prefix} is not used here.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

# Convenience alias so each check SQL stays readable.
_ST = "___ops.nfl_historical.nfl_player_stats_all"

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Existence
    # ------------------------------------------------------------------
    # 1. super_table_not_empty
    #    The super table must have rows.  Empty table = everything broken.
    Check(
        name="super_table_not_empty",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="Super table (nfl_player_stats_all) has no rows",
        sql_full=(
            f"SELECT 'super_table' AS db_name, 1 AS fail_count " f"WHERE NOT EXISTS (SELECT 1 FROM {_ST} LIMIT 1)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="rebuild_super_table",
    ),
    # ------------------------------------------------------------------
    # ERROR: Primary key integrity
    # ------------------------------------------------------------------
    # 2. super_table_player_week_unique
    #    player_week must be unique — it is the join key for all
    #    per-league player_fantasy lookups.
    Check(
        name="super_table_player_week_unique",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="Duplicate player_week values in super table",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM ("
            f"  SELECT player_week, COUNT(*) AS cnt "
            f"  FROM {_ST} "
            f"  WHERE player_week IS NOT NULL "
            f"  GROUP BY player_week "
            f"  HAVING COUNT(*) > 1"
            f")"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="rebuild_super_table",
    ),
    # 3. super_table_player_week_not_null
    #    player_week must not be NULL — NULL rows can't be joined against.
    Check(
        name="super_table_player_week_not_null",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="NULL player_week values in super table",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count " f"FROM {_ST} " f"WHERE player_week IS NULL"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="rebuild_super_table",
    ),
    # ------------------------------------------------------------------
    # ERROR: Year / week range sanity
    # ------------------------------------------------------------------
    # 4. super_table_year_range
    #    Years should be between 1920 and current_year+1.  Outside this
    #    range signals a bad insert or future projection leak.
    Check(
        name="super_table_year_range",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="year < 1920 or > current_year+1 in super table",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year IS NOT NULL "
            f"  AND (year < 1920 "
            f"       OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="rebuild_super_table",
    ),
    # 5. super_table_week_range
    #    week should be between 1 and 22 (preseason/wild card through
    #    Super Bowl).  Weeks outside this range are data artifacts.
    Check(
        name="super_table_week_range",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="week < 1 or > 22 in super table",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE week IS NOT NULL "
            f"  AND (week < 1 OR week > 22)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="rebuild_super_table",
    ),
    # ------------------------------------------------------------------
    # WARNING: Position coverage
    # ------------------------------------------------------------------
    # 6. super_table_position_coverage
    #    The super table must have rows for all six key fantasy positions:
    #    QB, RB, WR, TE, DEF, K.  Missing positions mean full leagues
    #    without that position type can't be enriched.
    Check(
        name="super_table_position_coverage",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="Super table missing one or more positions: QB/RB/WR/TE/DEF/K",
        sql_full=(
            f"SELECT 'super_table' AS db_name, "
            f"  (6 - COUNT(DISTINCT pos)) AS fail_count "
            f"FROM ("
            f"  SELECT position AS pos "
            f"  FROM {_ST} "
            f"  WHERE position IN ('QB','RB','WR','TE','DEF','K') "
            f"  GROUP BY position"
            f") "
            f"HAVING COUNT(DISTINCT pos) < 6"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # ERROR: NFL player ID coverage
    # ------------------------------------------------------------------
    # 7. super_table_nfl_id_populated
    #    NFL_player_id must be populated for modern players (year >= 2000).
    #    NULL IDs mean the player can't be joined to other tables.
    Check(
        name="super_table_nfl_id_populated",
        page="system",
        table="nfl_player_stats_all",
        severity="ERROR",
        description="NFL_player_id NULL for year >= 2000 rows in super table",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year >= 2000 "
            f"  AND NFL_player_id IS NULL"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="rebuild_super_table",
    ),
    # ------------------------------------------------------------------
    # WARNING: Headshot coverage
    # ------------------------------------------------------------------
    # 8. super_table_headshot_recent
    #    headshot_url should be populated for non-DEF players in recent
    #    seasons (year >= 2020).  Missing headshots mean blank player cards.
    Check(
        name="super_table_headshot_recent",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="headshot_url NULL for non-DEF player rows with year >= 2020",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year >= 2020 "
            f"  AND position != 'DEF' "
            f"  AND headshot_url IS NULL"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: Fantasy points precomputed
    # ------------------------------------------------------------------
    # 9. super_table_fpts_populated
    #    fpts_4pt_ppr (full-PPR, 4pt pass TD) should be populated for all
    #    modern rows (year >= 2020).  Missing pre-computed fpts means the
    #    LAMAR pipeline will fall back to slower runtime calculations.
    Check(
        name="super_table_fpts_populated",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="fpts_4pt_ppr NULL for year >= 2020 rows (precomputed fpts missing)",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year >= 2020 "
            f"  AND fpts_4pt_ppr IS NULL"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: Rank columns
    # ------------------------------------------------------------------
    # 10. super_table_rank_columns
    #     rank_qb_4pt (a representative rank column) should be populated
    #     for modern rows (year >= 2000).  NULL rank columns break the
    #     optimal lineup and LAMAR calculations for affected years.
    Check(
        name="super_table_rank_columns",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="rank_qb_4pt NULL for year >= 2000 QB rows (rank columns missing)",
        sql_full=(
            f"SELECT 'super_table' AS db_name, COUNT(*) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year >= 2000 "
            f"  AND position = 'QB' "
            f"  AND rank_qb_4pt IS NULL"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # ------------------------------------------------------------------
    # WARNING: DEF coverage
    # ------------------------------------------------------------------
    # 11. super_table_def_coverage
    #     There should be at least 30 distinct DEF teams for recent years
    #     (year >= 2020) — the NFL has 32 teams.  Fewer than 30 means some
    #     defenses are missing from the super table.
    Check(
        name="super_table_def_coverage",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="<30 distinct DEF teams in super table for year >= 2020",
        sql_full=(
            f"SELECT 'super_table' AS db_name, "
            f"  (30 - COUNT(DISTINCT nfl_team)) AS fail_count "
            f"FROM {_ST} "
            f"WHERE year >= 2020 "
            f"  AND position = 'DEF' "
            f"HAVING COUNT(DISTINCT nfl_team) < 30"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # ------------------------------------------------------------------
    # WARNING: player_bio join coverage
    # ------------------------------------------------------------------
    # 12. super_table_player_bio
    #     player_bio should not be empty — it provides DOB, height, weight,
    #     and college data that is displayed on player cards.
    Check(
        name="super_table_player_bio",
        page="system",
        table="nfl_player_stats_all",
        severity="WARNING",
        description="player_bio table is empty (player card bio data missing)",
        sql_full=(
            "SELECT 'super_table' AS db_name, 1 AS fail_count "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM ___ops.nfl_historical.player_bio LIMIT 1"
            ")"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
]
