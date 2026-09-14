# Owner: pipeline team
# Last audit: 2026-04-11
# Covers: /overview
"""Overview checks — semantic checks verifying homepage_* pre-computed tables.

All checks use sql_full.  The {table_prefix} and {league_list} placeholders
are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check
from multi_league.validation_v2.scope_sql import published_league_scope_sql

# Helper: published scope subquery
_INV_CTE = published_league_scope_sql()


def _exists_check(name, table, severity="ERROR", **kwargs):
    """Helper to build 'league in inventory but no rows in table' checks."""
    return Check(
        name=name,
        page="overview",
        table=table,
        severity=severity,
        description=f"{table} has no data for league",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_INV_CTE}) inv "
            "LEFT JOIN ("
            f"  SELECT DISTINCT db_name FROM {{table_prefix}}{table}"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group=kwargs.get("batch_group", "core"),
        cost_tier="cheap",
        fix_action="reimport",
        **{k: v for k, v in kwargs.items() if k != "batch_group"},
    )


CHECKS: list[Check] = [
    # 1-4: ERROR existence checks
    _exists_check("overview_league_summary_exists", "homepage_league_summary"),
    _exists_check("overview_manager_profiles_exists", "homepage_manager_profiles"),
    _exists_check("overview_manager_rankings_exists", "homepage_manager_rankings"),
    _exists_check("overview_current_standings_exists", "homepage_current_standings"),
    # 5: WARNING existence — multi-year leagues only.
    #    Single-year leagues legitimately can't surface a "top rivalry" yet —
    #    the homepage_top_rivalries builder requires meaningful cross-season
    #    H2H volume. weekend_warriors_ff (1 year of matchup data) is the
    #    canonical example: empty `homepage_top_rivalries` is correct, not a
    #    pipeline gap. Gate this check on leagues with 2+ matchup years.
    Check(
        name="overview_top_rivalries_exists",
        page="overview",
        table="homepage_top_rivalries",
        severity="WARNING",
        description="homepage_top_rivalries has no data for league (multi-year leagues only)",
        sql_full=(
            "SELECT inv.db_name, 1 AS fail_count "
            f"FROM ({_INV_CTE}) inv "
            "JOIN ("
            "  SELECT db_name "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name "
            "  HAVING COUNT(DISTINCT year) >= 2"
            ") multi_year ON inv.db_name = multi_year.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name FROM {table_prefix}homepage_top_rivalries"
            ") t ON inv.db_name = t.db_name "
            "WHERE t.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. standings_rank unique
    Check(
        name="overview_standings_rank_unique",
        page="overview",
        table="homepage_current_standings",
        severity="ERROR",
        description="Duplicate standings_rank values in homepage_current_standings",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, standings_rank, COUNT(*) AS cnt "
            "  FROM {table_prefix}homepage_current_standings "
            "  WHERE db_name IN ({league_list}) "
            "    AND standings_rank IS NOT NULL "
            "  GROUP BY db_name, standings_rank "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["overview_current_standings_exists"],
        fix_action="reimport",
    ),
    # 7. manager count match (rankings vs matchup)
    Check(
        name="overview_manager_count_match",
        page="overview",
        table="homepage_manager_rankings",
        severity="WARNING",
        description="Manager count in rankings doesn't match matchup manager count",
        sql_full=(
            "SELECT r.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT db_name, COUNT(*) AS ranking_count "
            "  FROM {table_prefix}homepage_manager_rankings "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") r "
            "JOIN ("
            "  SELECT db_name, COUNT(DISTINCT franchise_id) AS matchup_count "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND is_bye_week = 0 "
            "  GROUP BY db_name"
            ") m ON r.db_name = m.db_name "
            "WHERE r.ranking_count != m.matchup_count"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["overview_manager_rankings_exists", "completeness_has_matchup"],
    ),
    # 8. career wins match: rankings.wins vs matchup_career.wins + playoff wins
    #    homepage_manager_rankings intentionally includes regular-season wins
    #    plus championship-bracket playoff wins. matchup_career is built from
    #    matchup_season, whose wins are regular-season scoped, so add back
    #    non-consolation playoff wins before comparing.
    Check(
        name="overview_career_wins_match",
        page="overview",
        table="homepage_manager_rankings",
        severity="ERROR",
        description="Career wins in homepage_manager_rankings != matchup_career.wins + playoff wins",
        sql_full=(
            "WITH playoff_wins AS ("
            "  SELECT db_name, franchise_id, SUM(COALESCE(CAST(win AS INT), 0)) AS wins "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "    AND franchise_id IS NOT NULL "
            "    AND COALESCE(CAST(is_bye_week AS INT), 0) = 0 "
            "    AND COALESCE(CAST(is_playoffs AS INT), 0) = 1 "
            "    AND COALESCE(CAST(is_consolation AS INT), 0) = 0 "
            "  GROUP BY db_name, franchise_id"
            ") "
            "SELECT r.db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}homepage_manager_rankings r "
            "JOIN {table_prefix}matchup_career c "
            "  ON r.db_name = c.db_name "
            "  AND r.franchise_id = c.franchise_id "
            "LEFT JOIN playoff_wins p "
            "  ON r.db_name = p.db_name "
            "  AND r.franchise_id = p.franchise_id "
            "WHERE r.db_name IN ({league_list}) "
            "  AND r.wins IS NOT NULL "
            "  AND c.wins IS NOT NULL "
            "  AND CAST(r.wins AS INT) != CAST(c.wins AS INT) + CAST(COALESCE(p.wins, 0) AS INT) "
            "GROUP BY r.db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["overview_manager_rankings_exists", "completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 9. points_for cannot be negative (zero is valid during a live matchup)
    Check(
        name="overview_points_for_positive",
        page="overview",
        table="homepage_current_standings",
        severity="ERROR",
        description="points_for < 0 in homepage_current_standings",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}homepage_current_standings "
            "WHERE db_name IN ({league_list}) "
            "  AND points_for IS NOT NULL "
            "  AND CAST(points_for AS DOUBLE) < 0 "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["overview_current_standings_exists"],
        fix_action="reimport",
    ),
    # 10. profile career stats populated (total_wins, seasons_played)
    Check(
        name="overview_profile_career_stats",
        page="overview",
        table="homepage_manager_profiles",
        severity="ERROR",
        description="Manager profiles missing career stats (total_wins or seasons_played NULL)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}homepage_manager_profiles "
            "WHERE db_name IN ({league_list}) "
            "  AND (total_wins IS NULL OR seasons_played IS NULL) "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="cheap",
        depends_on=["overview_manager_profiles_exists"],
        fix_action="reimport",
    ),
    # 11. profile timeline populated
    Check(
        name="overview_profile_timeline",
        page="overview",
        table="homepage_manager_profiles",
        severity="WARNING",
        description="Manager profiles missing timeline data",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}homepage_manager_profiles "
            "WHERE db_name IN ({league_list}) "
            "  AND timeline_data IS NULL "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["overview_manager_profiles_exists"],
    ),
    # 12. years covered: league_summary vs matchup year range
    Check(
        name="overview_years_covered",
        page="overview",
        table="homepage_league_summary",
        severity="WARNING",
        description="Year range in homepage_league_summary doesn't match matchup year range",
        sql_full=(
            "SELECT s.db_name, 1 AS fail_count "
            "FROM {table_prefix}homepage_league_summary s "
            "JOIN ("
            "  SELECT db_name, MAX(year) AS max_year "
            "  FROM {table_prefix}matchup "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            ") m ON s.db_name = m.db_name "
            "WHERE s.db_name IN ({league_list}) "
            "  AND s.data_year IS NOT NULL "
            "  AND CAST(s.data_year AS INT) != m.max_year"
        ),
        batch_group="quality",
        cost_tier="moderate",
        depends_on=["overview_league_summary_exists", "completeness_has_matchup"],
    ),
    # 13. summary high score matches canonical matchup truth
    Check(
        name="overview_summary_high_score_match",
        page="overview",
        table="homepage_league_summary",
        severity="ERROR",
        description="homepage_league_summary highest-score fields do not match canonical matchup max",
        sql_full=(
            "WITH best_game AS ("
            "  SELECT db_name, manager, year, week, team_points "
            "  FROM ("
            "    SELECT db_name, manager, year, week, team_points, "
            "           ROW_NUMBER() OVER ("
            "             PARTITION BY db_name "
            "             ORDER BY team_points DESC, year DESC, week DESC, manager"
            "           ) AS rn "
            "    FROM {table_prefix}matchup "
            "    WHERE db_name IN ({league_list}) "
            "      AND COALESCE(is_bye_week, 0) = 0 "
            "      AND COALESCE(is_consolation, 0) = 0 "
            "      AND manager IS NOT NULL "
            "      AND opponent IS NOT NULL "
            "      AND team_points IS NOT NULL"
            "  ) ranked "
            "  WHERE rn = 1"
            ") "
            "SELECT s.db_name, 1 AS fail_count "
            "FROM {table_prefix}homepage_league_summary s "
            "JOIN best_game g ON s.db_name = g.db_name "
            "WHERE s.db_name IN ({league_list}) "
            "  AND ("
            "    ROUND(CAST(s.highest_score_points AS DOUBLE), 2) IS DISTINCT FROM ROUND(CAST(g.team_points AS DOUBLE), 2) "
            "    OR CAST(s.highest_score_year AS INTEGER) IS DISTINCT FROM CAST(g.year AS INTEGER) "
            "    OR CAST(s.highest_score_week AS INTEGER) IS DISTINCT FROM CAST(g.week AS INTEGER) "
            "    OR s.highest_score_manager IS DISTINCT FROM g.manager"
            "  )"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["overview_league_summary_exists", "completeness_has_matchup"],
        fix_action="reimport",
    ),
    # 14. current standings manager count matches latest matchup_season
    #    homepage_current_standings includes playoff weeks (total record),
    #    while matchup_season is regular-season only. We can't compare
    #    wins/points directly; just verify same set of managers exists.
    Check(
        name="overview_current_standings_match_latest_matchup_season",
        page="overview",
        table="homepage_current_standings",
        severity="ERROR",
        description="homepage_current_standings manager set != latest matchup_season managers",
        sql_full=(
            "WITH latest_year AS ("
            "  SELECT db_name, MAX(year) AS year "
            "  FROM {table_prefix}matchup_season "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name"
            "), expected AS ("
            "  SELECT ms.db_name, ms.manager "
            "  FROM {table_prefix}matchup_season ms "
            "  JOIN latest_year ly "
            "    ON ms.db_name = ly.db_name "
            "   AND ms.year = ly.year"
            "), mismatches AS ("
            "  SELECT COALESCE(h.db_name, e.db_name) AS db_name "
            "  FROM {table_prefix}homepage_current_standings h "
            "  FULL OUTER JOIN expected e "
            "    ON h.db_name = e.db_name "
            "   AND h.manager = e.manager "
            "  WHERE COALESCE(h.db_name, e.db_name) IN ({league_list}) "
            "    AND (h.manager IS NULL OR e.manager IS NULL)"
            ") "
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM mismatches "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["overview_current_standings_exists", "completeness_has_season_agg"],
        fix_action="reimport",
    ),
]
