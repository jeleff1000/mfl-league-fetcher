"""Build playoff evidence from the canonical lake without re-importing leagues."""

from __future__ import annotations

from collections.abc import Iterable

import duckdb
import pandas as pd


def _filters(db_names: Iterable[str] | None, years: Iterable[int] | None) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if db_names:
        names = list(db_names)
        clauses.append("s.db_name IN (" + ",".join("?" for _ in names) + ")")
        params.extend(names)
    if years:
        values = [int(year) for year in years]
        clauses.append("s.year IN (" + ",".join("?" for _ in values) + ")")
        params.extend(values)
    return (" AND ".join(clauses) if clauses else "TRUE", params)


def build_evidence(
    con: duckdb.DuckDBPyConnection,
    db_names: Iterable[str] | None = None,
    years: Iterable[int] | None = None,
) -> pd.DataFrame:
    """Return one evidence row per player-week in the canonical lake.

    The query uses native matchup playoff/seed fields where present and falls
    back to standings ranked by native team points or started-player points.
    A team's `is_playoffs_bf` is true only on weeks with native championship
    bracket evidence; being past the playoff start week is not sufficient.
    """

    scope, params = _filters(db_names, years)
    table_schema = _find_table_schema(con)
    q = lambda table: f'"{table_schema}"."{table}"'
    sql = f"""
    WITH scope AS (
        SELECT s.db_name, s.year, s.platform, s.playoff_start_week,
               s.playoff_teams, s.regular_season_weeks
        FROM {q('league_settings')} s
        WHERE {scope}
    ),
    weekly_player_points AS (
        SELECT p.db_name, p.year, p.week, p.manager,
               SUM(CASE WHEN COALESCE(p.is_started, 0) = 1
                        THEN COALESCE(p.fantasy_points, 0) ELSE 0 END) AS started_points
        FROM {q('player_fantasy')} p
        JOIN scope s USING (db_name, year)
        GROUP BY 1, 2, 3, 4
    ),
    team_weeks AS (
        SELECT m.db_name, m.year, m.week, m.manager,
               COALESCE(m.team_points, w.started_points) AS points,
               COALESCE(m.is_playoffs, 0) AS native_is_playoffs,
               COALESCE(m.final_playoff_seed, 0) AS native_seed,
               COALESCE(m.champion, 0) AS native_champion
        FROM {q('matchup')} m
        JOIN scope s USING (db_name, year)
        LEFT JOIN weekly_player_points w
          ON w.db_name = m.db_name AND w.year = m.year
         AND w.week = m.week AND w.manager = m.manager
    ),
    regular_totals AS (
        SELECT t.db_name, t.year, t.manager,
               SUM(CASE WHEN t.week <= s.regular_season_weeks
                        THEN COALESCE(t.points, 0) ELSE 0 END) AS regular_points
        FROM team_weeks t
        JOIN scope s USING (db_name, year)
        GROUP BY 1, 2, 3
    ),
    ranked AS (
        SELECT r.*, s.playoff_teams,
               ROW_NUMBER() OVER (
                   PARTITION BY r.db_name, r.year
                   ORDER BY r.regular_points DESC, r.manager
               ) AS points_seed
        FROM regular_totals r
        JOIN scope s USING (db_name, year)
    ),
    team_flags AS (
        SELECT t.db_name, t.year, t.manager,
               MAX(CASE WHEN t.native_seed > 0
                              AND t.native_seed <= s.playoff_teams THEN 1 ELSE 0 END) AS seed_made,
               MAX(t.native_is_playoffs) AS native_playoff,
               MAX(t.native_champion) AS native_champion,
               MAX(CASE WHEN r.points_seed <= r.playoff_teams THEN 1 ELSE 0 END) AS points_made
        FROM team_weeks t
        JOIN scope s USING (db_name, year)
        LEFT JOIN ranked r
          ON r.db_name = t.db_name AND r.year = t.year AND r.manager = t.manager
        GROUP BY 1, 2, 3
    ),
    player_manager_flags AS (
        SELECT p.db_name, p.year, p.manager,
               MAX(COALESCE(p.champion, 0)) AS player_champion
        FROM {q('player_fantasy')} p
        JOIN scope s USING (db_name, year)
        GROUP BY 1, 2, 3
    ),
    player_scope AS (
        SELECT p.db_name, p.year, p.week, p.NFL_player_id, p.manager,
               s.platform,
               GREATEST(COALESCE(f.seed_made, 0), COALESCE(f.native_playoff, 0),
                        COALESCE(f.native_champion, 0), COALESCE(f.points_made, 0),
                        COALESCE(pf.player_champion, 0)) AS made_po_bf,
               COALESCE(MAX(CASE WHEN t.native_is_playoffs = 1 THEN 1 ELSE 0 END), 0) AS is_playoffs_bf,
               COALESCE(MAX(CASE WHEN t.native_champion = 1 THEN 1 ELSE 0 END), 0) AS champion_bf,
               CASE
                 WHEN MAX(CASE WHEN t.native_is_playoffs = 1 THEN 1 ELSE 0 END) = 1
                   THEN 'championship_bracket'
                 WHEN GREATEST(COALESCE(f.seed_made, 0), COALESCE(f.native_playoff, 0),
                               COALESCE(f.native_champion, 0), COALESCE(f.points_made, 0),
                               COALESCE(pf.player_champion, 0)) = 1
                   THEN 'qualification_standings'
                 ELSE 'unresolved'
               END AS evidence_kind
        FROM {q('player_fantasy')} p
        JOIN scope s USING (db_name, year)
        LEFT JOIN team_flags f
          ON f.db_name = p.db_name AND f.year = p.year AND f.manager = p.manager
        LEFT JOIN player_manager_flags pf
          ON pf.db_name = p.db_name AND pf.year = p.year AND pf.manager = p.manager
        LEFT JOIN team_weeks t
          ON t.db_name = p.db_name AND t.year = p.year
         AND t.week = p.week AND t.manager = p.manager
        WHERE p.NFL_player_id IS NOT NULL
        GROUP BY p.db_name, p.year, p.week, p.NFL_player_id, p.manager,
                 s.platform, f.seed_made, f.native_playoff, f.native_champion, f.points_made,
                 pf.player_champion
    )
    SELECT db_name, year, week, NFL_player_id,
           CAST(made_po_bf AS TINYINT) AS made_po_bf,
           CAST(CASE WHEN made_po_bf = 1 THEN is_playoffs_bf ELSE 0 END AS TINYINT) AS is_playoffs_bf,
           CAST(champion_bf AS TINYINT) AS champion_bf,
           platform AS source_platform,
           evidence_kind,
           manager AS source_id,
           CURRENT_TIMESTAMP::VARCHAR AS generated_at
    FROM player_scope
    """
    return con.execute(sql, params).fetchdf()


def _find_table_schema(con: duckdb.DuckDBPyConnection) -> str:
    """Find the schema containing the three canonical lake tables."""
    rows = con.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name IN ('league_settings', 'matchup', 'player_fantasy')
        GROUP BY table_schema
        HAVING COUNT(DISTINCT table_name) = 3
        ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END
        LIMIT 1
        """
    ).fetchall()
    if not rows:
        raise ValueError("expected league_settings, matchup, and player_fantasy tables")
    return rows[0][0]
