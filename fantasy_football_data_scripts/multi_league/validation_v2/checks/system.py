# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /system (cross-table invariants)
"""System checks — 7 cross-table invariant checks.

All checks use sql_full with JOINs across multiple tables.
These are the most expensive checks — cost_tier is moderate or expensive.
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Score reconciliation
    # ------------------------------------------------------------------
    # 1. system_team_points_vs_player_sum
    #    The matchup team_points should approximately equal the sum of
    #    fantasy_points for started players in player_fantasy.
    #    Threshold=3 allows small rounding differences.
    #
    #    Pre-2019 ESPN is excluded: populate_fantasy_points explicitly
    #    recomputes per-player fantasy_points for that scope (old ESPN API
    #    doesn't return reliable per-player points), while matchup.team_points
    #    remains the platform-stored value. The two are *expected* to diverge
    #    on those rows — it's not a validator failure, it's the trade-off
    #    documented in commit e97ee9c2 (populate_fantasy_points scope filter).
    #    A prior attempt to force team_points to match the recomputed sum
    #    (commit 655d9f59, reverted in 5be32c6c) cascaded into
    #    optimal_gte_team_points failures because pre-2019 roster configs
    #    differ from current settings and the optimal-lineup computation
    #    uses the current config as a fallback. The least-bad answer is to
    #    keep team_points as-stored and scope the check out of pre-2019 ESPN.
    #
    #    Modern ESPN can also include commissioner adjustments directly on the
    #    matchup team total. Those should not count as roster reconciliation
    #    failures, so compare started-player sums against (team_points -
    #    adjustment) when adjustment is present.
    Check(
        name="system_team_points_vs_player_sum",
        page="system",
        table="matchup",
        severity="WARNING",
        description="net team_points != SUM(started player fantasy_points) by >3 pts",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}matchup m "
            "JOIN ("
            "  SELECT db_name, manager, year, week, "
            "    SUM(COALESCE(fantasy_points, 0)) AS player_sum "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE is_started = 1 "
            "  GROUP BY db_name, manager, year, week"
            ") p "
            "  ON m.db_name = p.db_name "
            "  AND m.manager = p.manager "
            "  AND m.year = p.year "
            "  AND m.week = p.week "
            "WHERE m.db_name IN ({league_list}) "
            "  AND m.is_bye_week = 0 "
            "  AND COALESCE(CAST(m.is_playoffs AS INT), 0) = 0 "
            "  AND COALESCE(CAST(m.is_consolation AS INT), 0) = 0 "
            "  AND NOT (m.platform = 'espn' AND m.year < 2019) "
            "  AND m.team_points IS NOT NULL "
            "  AND CAST(m.team_points AS DOUBLE) != 0 "
            "  AND ABS((CAST(m.team_points AS DOUBLE) - COALESCE(CAST(m.adjustment AS DOUBLE), 0.0)) - p.player_sum) > 3 "
            "GROUP BY m.db_name"
        ),
        batch_group="analytics",
        cost_tier="expensive",
        threshold=3,
        depends_on=["completeness_has_matchup", "completeness_has_player_fantasy"],
    ),
    # ------------------------------------------------------------------
    # WARNING: Manager coverage
    # ------------------------------------------------------------------
    # 2. system_managers_matchup_vs_player
    #    Every manager present in the matchup table should also appear in
    #    player_fantasy.  Missing managers means roster data is incomplete.
    Check(
        name="system_managers_matchup_vs_player",
        page="system",
        table="matchup",
        severity="WARNING",
        description="Managers in matchup missing from player_fantasy",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, manager "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, manager "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE manager IS NOT NULL "
            "    AND TRIM(COALESCE(manager, '')) != '' "
            "    AND LOWER(TRIM(manager)) NOT IN "
            "        ('unrostered', 'fa', 'free agent', 'waivers')"
            ") p "
            "  ON m.db_name = p.db_name "
            "  AND m.manager = p.manager "
            "WHERE p.manager IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_player_fantasy"],
    ),
    # 3. system_year_coverage
    #    Every year present in the matchup table should also appear in
    #    player_fantasy.  Missing years mean full seasons have no roster data.
    Check(
        name="system_year_coverage",
        page="system",
        table="matchup",
        severity="WARNING",
        description="Years in matchup not represented in player_fantasy",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list})"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}player_fantasy"
            ") p "
            "  ON m.db_name = p.db_name "
            "  AND m.year = p.year "
            "WHERE p.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["completeness_has_matchup", "completeness_has_player_fantasy"],
    ),
    # ------------------------------------------------------------------
    # ERROR: Roster data coverage
    # ------------------------------------------------------------------
    # 4. system_roster_week_coverage
    #    Every (db_name, manager, year, week) combination in matchup that
    #    is not a bye or placeholder must have at least one row in
    #    player_fantasy.  Zero roster rows for a real matchup week means
    #    the lineup data is completely missing.
    #
    #    KMFFL 2013 is scoped out: that year was uploaded externally with
    #    matchup-level data only (no draft, no player stats, no transactions).
    #    The matchup parquet has all 11 managers' weekly scores but only 2
    #    appear in player_fantasy via carryover, leaving ~84 (manager, week)
    #    keys with no roster data. Mirrors the pre-2019 ESPN scope-out
    #    pattern in system_team_points_vs_player_sum.
    Check(
        name="system_roster_week_coverage",
        page="system",
        table="player_fantasy",
        severity="ERROR",
        description="Matchup weeks with zero player_fantasy rows (roster data missing)",
        sql_full=(
            "SELECT m.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name, manager, year, week "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0"
            "    AND COALESCE(team_points, 0) > 0"
            "    AND NOT (db_name = 'kmffl' AND year = 2013)"
            ") m "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, manager, year, week "
            "  FROM {table_prefix}player_fantasy"
            ") p "
            "  ON m.db_name = p.db_name "
            "  AND m.manager = p.manager "
            "  AND m.year = p.year "
            "  AND m.week = p.week "
            "WHERE p.db_name IS NULL "
            "GROUP BY m.db_name"
        ),
        batch_group="core",
        cost_tier="expensive",
        depends_on=["completeness_has_player_fantasy"],
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Draft-to-roster consistency
    # ------------------------------------------------------------------
    # 5. system_draft_players_in_fantasy
    #    Players with a resolved NFL_player_id in the main draft should appear
    #    in player_fantasy for the same league-year. Catches pipeline sync bugs
    #    where drafted players never made it into the roster tables.
    #
    #    Exclusions (known legit misses, not pipeline bugs):
    #      - NULL NFL_player_id: DEF/DST rows that didn't resolve to DEF-{id}.
    #        Can't join by key anyway; cleared by the Sleeper DEF resolver
    #        upgrade on next reimport.
    #      - round > 20: dynasty rookie / taxi stashes (college players not
    #        yet in the NFL, retired keepers held for sentiment).
    #      - Rookie-draft-only league-years (max_round < 10): dynasty rookie
    #        drafts typically have 4-7 rounds and the picked players may
    #        never make an NFL roster / fantasy_position slot for that year.
    #        Main / startup drafts (max_round >= 10) still get validated.
    #      - Match on NFL_player_id only (no player-name fallback): DST name
    #        format differences between player_fantasy and draft produce
    #        false negatives when names don't align character-for-character.
    Check(
        name="system_draft_players_in_fantasy",
        page="system",
        table="draft",
        severity="WARNING",
        description="Drafted players with NFL stats missing from player_fantasy for that league-year",
        sql_full=(
            "SELECT d.db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT DISTINCT dr.db_name, dr.year, dr.NFL_player_id::VARCHAR AS nfl_id "
            "  FROM {table_prefix}draft dr "
            "  INNER JOIN ("
            "    SELECT db_name, year "
            "    FROM {table_prefix}draft "
            "    WHERE db_name IN ({league_list}) "
            "    GROUP BY db_name, year "
            "    HAVING MAX(round) >= 10"
            "  ) main_draft ON main_draft.db_name = dr.db_name AND main_draft.year = dr.year "
            "  WHERE dr.db_name IN ({league_list}) "
            "    AND dr.player IS NOT NULL "
            "    AND dr.NFL_player_id IS NOT NULL "
            "    AND (dr.round IS NULL OR dr.round <= 20)"
            "    AND EXISTS ("
            "      SELECT 1 FROM ___ops.nfl_historical.nfl_player_stats_all s "
            "      WHERE s.NFL_player_id = dr.NFL_player_id::VARCHAR "
            "        AND s.year = dr.year"
            "    )"
            ") d "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name, year, NFL_player_id::VARCHAR AS nfl_id "
            "  FROM {table_prefix}player_fantasy "
            "  WHERE manager IS NOT NULL "
            "    AND NFL_player_id IS NOT NULL"
            ") p "
            "  ON d.db_name = p.db_name "
            "  AND d.year = p.year "
            "  AND d.nfl_id = p.nfl_id "
            "WHERE p.db_name IS NULL "
            "GROUP BY d.db_name"
        ),
        batch_group="analytics",
        cost_tier="moderate",
        depends_on=["completeness_has_player_fantasy"],
    ),
    # ------------------------------------------------------------------
    # ERROR/WARNING: Super table join rate
    # ------------------------------------------------------------------
    # 6. system_super_table_join_80
    #    At least 80% of STARTED player_weeks must join to the super_table.
    #    Bench players on bye/IR/inactive legitimately have no NFL stats row to join.
    #    The correct population to test is started players — if a player was in a
    #    starting lineup, they should have NFL stats.
    Check(
        name="system_super_table_join_80",
        page="system",
        table="player_fantasy",
        severity="ERROR",
        description="<80% of started player_weeks join to super_table (NFL ID resolution broken)",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    COUNT(*) AS total_started, "
            "    SUM(CASE WHEN s.player_week IS NOT NULL THEN 1 ELSE 0 END) "
            "      AS joined "
            "  FROM {table_prefix}player_fantasy pf "
            "  LEFT JOIN ___ops.nfl_historical.nfl_player_stats_all s "
            "    ON pf.player_week = s.player_week "
            "  WHERE pf.db_name IN ({league_list}) "
            "    AND pf.is_started = 1 "
            "    AND pf.manager IS NOT NULL "
            "    AND TRIM(COALESCE(pf.manager, '')) != '' "
            "    AND LOWER(TRIM(pf.manager)) NOT IN "
            "        ('unrostered', 'fa', 'free agent', 'waivers') "
            "  GROUP BY pf.db_name"
            ") pf "
            "WHERE total_started > 0 "
            "  AND (joined * 100.0 / total_started) < 80"
        ),
        batch_group="analytics",
        cost_tier="expensive",
        depends_on=["completeness_has_player_fantasy"],
        fix_action="reimport",
    ),
    # 7. system_super_table_join_95
    #    Advisory warning when started-player join rate falls below 95%.
    Check(
        name="system_super_table_join_95",
        page="system",
        table="player_fantasy",
        severity="WARNING",
        description="<95% of started player_weeks join to super_table (some player data missing)",
        sql_full=(
            "SELECT pf.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    COUNT(*) AS total_started, "
            "    SUM(CASE WHEN s.player_week IS NOT NULL THEN 1 ELSE 0 END) "
            "      AS joined "
            "  FROM {table_prefix}player_fantasy pf "
            "  LEFT JOIN ___ops.nfl_historical.nfl_player_stats_all s "
            "    ON pf.player_week = s.player_week "
            "  WHERE pf.db_name IN ({league_list}) "
            "    AND pf.is_started = 1 "
            "    AND pf.manager IS NOT NULL "
            "    AND TRIM(COALESCE(pf.manager, '')) != '' "
            "    AND LOWER(TRIM(pf.manager)) NOT IN "
            "        ('unrostered', 'fa', 'free agent', 'waivers') "
            "  GROUP BY pf.db_name"
            ") pf "
            "WHERE total_started > 0 "
            "  AND (joined * 100.0 / total_started) < 95"
        ),
        batch_group="analytics",
        cost_tier="expensive",
        depends_on=["completeness_has_player_fantasy"],
    ),
]
