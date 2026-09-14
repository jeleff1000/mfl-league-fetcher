"""Audit evidence for filling settings-derived cohort dimensions.

Read-only.  It does not create a candidate database, change the canonical
cache, or choose defaults for ambiguous records.  Every proposed fill is
reported with its source and evidence class first.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


def esc(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def tables(con: duckdb.DuckDBPyConnection, database: str, schema: str) -> set[str]:
    return {r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=?",
        [database, schema],
    ).fetchall()}


def cols(con: duckdb.DuckDBPyConnection, ref: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {ref}").fetchall()}


def count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0] or 0)


def norm_position(expr: str) -> str:
    return f"CASE WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DST','D/ST') THEN 'DEF' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('PK','PLACEKICKER') THEN 'K' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('FB','HB') THEN 'RB' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('CB','S','SAFETY') THEN 'DB' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('DE','DT','NT','DL') THEN 'DL' " \
        f"WHEN UPPER(TRIM(CAST({expr} AS VARCHAR))) IN ('MLB','OLB','ILB','LB') THEN 'LB' " \
        f"ELSE UPPER(TRIM(CAST({expr} AS VARCHAR))) END"


def first_available(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def domain_counts(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, int]:
    return {str(r[0]): int(r[1]) for r in con.execute(sql).fetchall()}


def audit(base: Path, ops: Path) -> dict[str, Any]:
    if not base.exists() or not ops.exists():
        raise SystemExit(f"missing input: base={base.exists()} ops={ops.exists()}")
    # Use an in-memory coordinator and attach both files read-only.  This
    # keeps the source files untouched and gives them stable database names.
    con = duckdb.connect()
    con.execute(f"ATTACH '{esc(base)}' AS base (READ_ONLY)")
    con.execute(f"ATTACH '{esc(ops)}' AS ops (READ_ONLY)")
    try:
        base_tables = tables(con, "base", "public")
        ops_tables = tables(con, "ops", "nfl_historical")
        if "player_fantasy" not in base_tables or "league_settings" not in base_tables:
            raise SystemExit(f"canonical tables missing: {sorted(base_tables)}")
        pcols = cols(con, "base.public.player_fantasy")
        scols = cols(con, "base.public.league_settings")
        report: dict[str, Any] = {
            "base_bytes": base.stat().st_size,
            "ops_bytes": ops.stat().st_size,
            "base_player_rows": count(con, "SELECT COUNT(*) FROM base.public.player_fantasy"),
            "base_league_player_rows": count(con, "SELECT COUNT(*) FROM base.public.player_fantasy WHERE db_name IS NOT NULL"),
            "settings_rows": count(con, "SELECT COUNT(*) FROM base.public.league_settings"),
            "base_tables": sorted(base_tables),
            "ops_tables": sorted(ops_tables),
            "player_columns": sorted(pcols),
            "settings_columns": sorted(scols),
        }

        # Position evidence: bio is the primary identity source; season stats
        # are a second source for players absent from bio.  The query never
        # treats a conflicting source as safe.
        bio_cols = set()
        stat_cols = set()
        if "player_bio" in ops_tables:
            bio_cols = cols(con, "ops.nfl_historical.player_bio")
        if "nfl_player_stats_all" in ops_tables:
            stat_cols = cols(con, "ops.nfl_historical.nfl_player_stats_all")
        report["player_bio_columns"] = sorted(bio_cols)
        report["player_bio_position_candidate_columns"] = [
            c for c in ("fantasy_position", "position_category", "nfl_position", "position") if c in bio_cols
        ]
        bio_pos_col = first_available(bio_cols, ("position", "fantasy_position", "position_category", "nfl_position"))
        # In player_bio, ``position`` is the canonical broad fantasy grouping
        # (e.g. DE->DL and CB->DB); ``nfl_position`` is the raw NFL label. The
        # player_fantasy field named fantasy_position is a lineup slot and is
        # not used here.
        stat_pos_col = first_available(stat_cols, ("position", "nfl_position", "position_category", "fantasy_position"))
        report["position_source"] = {
            "bio_columns": sorted(bio_cols),
            "stats_columns": sorted(stat_cols),
            "bio_position_column": bio_pos_col,
            "stats_position_column": stat_pos_col,
        }
        if bio_pos_col:
            bpos = norm_position(f"b.{bio_pos_col}")
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE _bio_pos AS
              SELECT DISTINCT NFL_player_id, position
              FROM (
                SELECT CAST(b.NFL_player_id AS VARCHAR) AS NFL_player_id, {bpos} AS position
                FROM ops.nfl_historical.player_bio b
                WHERE b.NFL_player_id IS NOT NULL AND {bpos} NOT IN ('', 'NULL')
                QUALIFY COUNT(DISTINCT {bpos}) OVER (PARTITION BY b.NFL_player_id)=1
              )
            """)
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE _bio_pos AS SELECT CAST(NULL AS VARCHAR) NFL_player_id, CAST(NULL AS VARCHAR) position WHERE false")
        report["position_bio_rows"] = count(con, "SELECT COUNT(*) FROM _bio_pos")
        report["position_bio_conflicting_ids"] = count(con, """
            SELECT COUNT(*) FROM (SELECT NFL_player_id FROM _bio_pos GROUP BY 1 HAVING COUNT(DISTINCT position)>1)
        """)
        report["position_target_null_rows"] = count(con, """
            SELECT COUNT(*) FROM base.public.player_fantasy
            WHERE db_name IS NOT NULL AND (position IS NULL OR TRIM(CAST(position AS VARCHAR))='')
        """) if "position" in pcols else None
        report["position_target_null_with_nfl_id"] = count(con, """
            SELECT COUNT(*) FROM base.public.player_fantasy
            WHERE db_name IS NOT NULL AND (position IS NULL OR TRIM(CAST(position AS VARCHAR))='') AND NFL_player_id IS NOT NULL
        """) if {"position", "NFL_player_id"} <= pcols else None
        report["position_bio_fillable_rows"] = count(con, """
            SELECT COUNT(*) FROM base.public.player_fantasy p JOIN _bio_pos b
              ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
            WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
        """) if {"position", "NFL_player_id"} <= pcols else None
        report["position_bio_unmatched_player_ids"] = count(con, """
            SELECT COUNT(*) FROM (SELECT DISTINCT CAST(p.NFL_player_id AS VARCHAR) id
              FROM base.public.player_fantasy p LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              WHERE p.db_name IS NOT NULL AND p.NFL_player_id IS NOT NULL
                AND b.NFL_player_id IS NULL)
        """) if "NFL_player_id" in pcols else None
        report["position_bio_domain"] = domain_counts(con, "SELECT position, COUNT(*) FROM _bio_pos GROUP BY 1 ORDER BY 1")
        if {"position", "fantasy_position"} <= pcols:
            report["position_direct_lineup_fillable_rows"] = count(con, """
              SELECT COUNT(*) FROM base.public.player_fantasy
              WHERE db_name IS NOT NULL AND (position IS NULL OR TRIM(CAST(position AS VARCHAR))='')
                AND UPPER(TRIM(CAST(fantasy_position AS VARCHAR))) IN ('QB','RB','WR','TE','K','DEF','DL','LB','DB','IDP')
            """)
        else:
            report["position_direct_lineup_fillable_rows"] = None
        if stat_pos_col and {"NFL_player_id", "year"} <= stat_cols:
            spos = norm_position(f"st.{stat_pos_col}")
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE _stats_pos AS
              SELECT CAST(st.NFL_player_id AS VARCHAR) NFL_player_id,
                     CAST(st.year AS INTEGER) AS season_year,
                     {spos} AS position
              FROM ops.nfl_historical.nfl_player_stats_all st
              WHERE st.NFL_player_id IS NOT NULL AND {spos} NOT IN ('', 'NULL')
              QUALIFY ROW_NUMBER() OVER (PARTITION BY st.NFL_player_id, st.year ORDER BY st.week NULLS LAST) = 1
            """)
            report["position_stats_rows"] = count(con, "SELECT COUNT(*) FROM _stats_pos")
            report["position_stats_conflicting_id_years"] = count(con, """
              SELECT COUNT(*) FROM (
                SELECT CAST(NFL_player_id AS VARCHAR), CAST(year AS INTEGER)
                FROM ops.nfl_historical.nfl_player_stats_all
                WHERE NFL_player_id IS NOT NULL
                GROUP BY 1,2 HAVING COUNT(DISTINCT UPPER(TRIM(CAST(position AS VARCHAR))))>1
              )
            """) if "position" in stat_cols else None
            report["position_stats_fillable_rows"] = count(con, """
              SELECT COUNT(*) FROM base.public.player_fantasy p JOIN _stats_pos st
                ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND p.NFL_player_id IS NOT NULL AND b.NFL_player_id IS NULL
            """) if {"position", "NFL_player_id", "year"} <= pcols else None
            report["position_fillable_union_rows"] = count(con, """
              SELECT COUNT(*) FROM base.public.player_fantasy p
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NOT NULL
            """) if {"position", "NFL_player_id", "year"} <= pcols else None
            report["position_unresolved_rows"] = count(con, """
              SELECT COUNT(*) FROM base.public.player_fantasy p
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NULL
            """) if {"position", "NFL_player_id", "year"} <= pcols else None
            report["position_unresolved_by_platform"] = domain_counts(con, """
              SELECT COALESCE(NULLIF(LOWER(TRIM(CAST(s.platform AS VARCHAR))),''),'NULL'), COUNT(*)
              FROM base.public.player_fantasy p
              JOIN base.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NULL
              GROUP BY 1 ORDER BY 1
            """) if {"db_name", "year"} <= pcols and {"db_name", "year", "platform"} <= scols else None
            report["position_unresolved_id_state"] = domain_counts(con, """
              SELECT CASE WHEN p.NFL_player_id IS NULL OR TRIM(CAST(p.NFL_player_id AS VARCHAR))='' THEN 'no_nfl_id' ELSE 'nfl_id_unmapped' END, COUNT(*)
              FROM base.public.player_fantasy p
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NULL
              GROUP BY 1
            """) if "NFL_player_id" in pcols else None
            report["position_unresolved_fantasy_slot_domain"] = domain_counts(con, """
              SELECT COALESCE(NULLIF(UPPER(TRIM(CAST(p.fantasy_position AS VARCHAR))),''),'NULL'), COUNT(*)
              FROM base.public.player_fantasy p
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NULL
              GROUP BY 1 ORDER BY 2 DESC LIMIT 30
            """) if "fantasy_position" in pcols else None
            report["position_unresolved_identity_sample"] = [
                dict(zip(("db_name", "platform", "year", "week", "player", "NFL_player_id",
                          "sleeper_player_id", "mfl_player_id", "fleaflicker_player_id", "espn_player_id",
                          "yahoo_player_id", "fantasy_position", "team_name", "team_key"), r))
                for r in con.execute("""
                  SELECT p.db_name, s.platform, p.year, p.week, p.player, p.NFL_player_id,
                         p.sleeper_player_id, p.mfl_player_id, p.fleaflicker_player_id, p.espn_player_id,
                         p.yahoo_player_id, p.fantasy_position, p.team_name, p.team_key
                  FROM base.public.player_fantasy p
                  JOIN base.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
                  LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
                  LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
                  WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                    AND COALESCE(b.position, st.position) IS NULL
                  ORDER BY s.platform, p.year, p.db_name, p.week
                  LIMIT 100
                """).fetchall()
            ] if {"db_name", "year", "week", "position"} <= pcols and {"db_name", "year", "platform"} <= scols else []
            report["position_unresolved_real_id_sample"] = [
                dict(zip(("db_name", "platform", "year", "week", "player", "NFL_player_id",
                          "sleeper_player_id", "mfl_player_id", "fleaflicker_player_id", "espn_player_id",
                          "yahoo_player_id", "fantasy_position", "team_name", "team_key"), r))
                for r in con.execute("""
                  SELECT p.db_name, s.platform, p.year, p.week, p.player, p.NFL_player_id,
                         p.sleeper_player_id, p.mfl_player_id, p.fleaflicker_player_id, p.espn_player_id,
                         p.yahoo_player_id, p.fantasy_position, p.team_name, p.team_key
                  FROM base.public.player_fantasy p
                  JOIN base.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
                  LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
                  LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
                  WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                    AND p.NFL_player_id IS NOT NULL AND COALESCE(b.position, st.position) IS NULL
                  ORDER BY s.platform, p.year, p.db_name, p.week
                  LIMIT 100
                """).fetchall()
            ] if {"db_name", "year", "week", "position", "NFL_player_id"} <= pcols and {"db_name", "year", "platform"} <= scols else []
            report["position_unresolved_identity_state"] = domain_counts(con, """
              SELECT CASE
                WHEN COALESCE(NULLIF(LOWER(TRIM(CAST(p.player AS VARCHAR))),''),'') IN ('', 'duplicate player')
                 AND p.sleeper_player_id IS NULL AND p.mfl_player_id IS NULL
                 AND p.fleaflicker_player_id IS NULL AND p.espn_player_id IS NULL
                 AND p.yahoo_player_id IS NULL
                 AND p.team_name IS NULL AND p.team_key IS NULL THEN 'empty_player_placeholder'
                WHEN p.NFL_player_id IS NULL OR TRIM(CAST(p.NFL_player_id AS VARCHAR))='' THEN 'real_row_without_nfl_id'
                ELSE 'real_row_with_unmapped_nfl_id'
              END, COUNT(*)
              FROM base.public.player_fantasy p
              LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
              LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
              WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                AND COALESCE(b.position, st.position) IS NULL
              GROUP BY 1 ORDER BY 1
            """) if {"position", "NFL_player_id", "player"} <= pcols else None
            report["position_unresolved_nonplaceholder_sample"] = [
                dict(zip(("db_name", "platform", "year", "week", "player", "NFL_player_id",
                          "sleeper_player_id", "mfl_player_id", "fleaflicker_player_id", "espn_player_id",
                          "yahoo_player_id", "fantasy_position", "team_name", "team_key"), r))
                for r in con.execute("""
                  SELECT p.db_name, s.platform, p.year, p.week, p.player, p.NFL_player_id,
                         p.sleeper_player_id, p.mfl_player_id, p.fleaflicker_player_id, p.espn_player_id,
                         p.yahoo_player_id, p.fantasy_position, p.team_name, p.team_key
                  FROM base.public.player_fantasy p
                  JOIN base.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
                  LEFT JOIN _bio_pos b ON CAST(p.NFL_player_id AS VARCHAR)=b.NFL_player_id
                  LEFT JOIN _stats_pos st ON CAST(p.NFL_player_id AS VARCHAR)=st.NFL_player_id AND CAST(p.year AS INTEGER)=st.season_year
                  WHERE p.db_name IS NOT NULL AND (p.position IS NULL OR TRIM(CAST(p.position AS VARCHAR))='')
                    AND COALESCE(b.position, st.position) IS NULL
                    AND NOT (COALESCE(NULLIF(LOWER(TRIM(CAST(p.player AS VARCHAR))),''),'') IN ('', 'duplicate player')
                             AND p.sleeper_player_id IS NULL AND p.mfl_player_id IS NULL
                             AND p.fleaflicker_player_id IS NULL AND p.espn_player_id IS NULL
                             AND p.yahoo_player_id IS NULL AND p.team_name IS NULL AND p.team_key IS NULL)
                    AND (p.player IS NOT NULL OR p.sleeper_player_id IS NOT NULL OR p.mfl_player_id IS NOT NULL
                         OR p.fleaflicker_player_id IS NOT NULL OR p.espn_player_id IS NOT NULL OR p.yahoo_player_id IS NOT NULL
                         OR p.team_name IS NOT NULL OR p.team_key IS NOT NULL)
                  ORDER BY s.platform, p.year, p.db_name, p.week
                  LIMIT 200
                """).fetchall()
            ] if {"db_name", "year", "week", "position", "NFL_player_id", "player"} <= pcols and {"db_name", "year", "platform"} <= scols else []
        else:
            report["position_stats_rows"] = None
            report["position_stats_conflicting_id_years"] = None
            report["position_stats_fillable_rows"] = None
            report["position_fillable_union_rows"] = report["position_bio_fillable_rows"]
            report["position_unresolved_rows"] = None

        # Exact missing-setting populations, restricted to league-years that
        # actually have player rows.  Settings-only records do not affect the
        # player cache and are reported separately by the earlier inventory.
        report["missing_setting_league_years_with_players"] = {}
        if {"db_name", "year"} <= pcols and {"db_name", "year"} <= scols:
            for name, predicate in {
                "pass_td": "s.scoring_pass_td IS NULL",
                "playoff_teams": "s.playoff_teams IS NULL",
                "roster_config": "s.roster_IDP IS NULL AND s.roster_DL IS NULL AND s.roster_LB IS NULL AND s.roster_DB IS NULL AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL AND s.roster_SUPER_FLEX IS NULL AND s.roster_FLX IS NULL",
                "best_ball": "s.sleeper_best_ball IS NULL",
            }.items():
                report["missing_setting_league_years_with_players"][name] = count(con, f"""
                  SELECT COUNT(*) FROM (
                    SELECT DISTINCT s.db_name, s.year
                    FROM base.public.league_settings s JOIN base.public.player_fantasy p
                      ON p.db_name=s.db_name AND p.year=s.year
                    WHERE {predicate}
                  )
                """)

        # Pass-TD evidence: compare missing settings years' QB fantasy points
        # against the matching 4-point and 6-point super-table calculations,
        # using the league's known reception scoring to select the ppr column.
        pass_cols = [c for c in ("fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr", "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr") if c in stat_cols]
        report["pass_td_source_columns"] = pass_cols
        stat_pos_expr = f"UPPER(TRIM(CAST(st.{stat_pos_col} AS VARCHAR)))" if stat_pos_col else "''"
        player_pos_expr = "UPPER(TRIM(CAST(p.position AS VARCHAR)))" if "position" in pcols else "''"
        if "scoring_pass_td" in scols and all(c in stat_cols for c in ("fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr", "fpts_6pt_0ppr", "fpts_6pt_half", "fpts_6pt_ppr")) and {"db_name", "year", "week", "NFL_player_id", "fantasy_points"} <= pcols and {"NFL_player_id", "year", "week"} <= stat_cols:
            four = "CASE WHEN s.scoring_rec=0 THEN st.fpts_4pt_0ppr WHEN s.scoring_rec<0.75 THEN st.fpts_4pt_half ELSE st.fpts_4pt_ppr END"
            six = "CASE WHEN s.scoring_rec=0 THEN st.fpts_6pt_0ppr WHEN s.scoring_rec<0.75 THEN st.fpts_6pt_half ELSE st.fpts_6pt_ppr END"
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE _pass_td_evidence AS
              SELECT p.db_name, CAST(p.year AS INTEGER) AS season_year,
                SUM(CASE WHEN ABS(p.fantasy_points-({four}))<=0.01 THEN 1 ELSE 0 END) matches_4,
                SUM(CASE WHEN ABS(p.fantasy_points-({six}))<=0.01 THEN 1 ELSE 0 END) matches_6,
                SUM(ABS(p.fantasy_points-({four}))) abs_error_4,
                SUM(ABS(p.fantasy_points-({six}))) abs_error_6,
                COUNT(*) compared
              FROM base.public.player_fantasy p
              JOIN base.public.league_settings s ON s.db_name=p.db_name AND s.year=p.year
              JOIN ops.nfl_historical.nfl_player_stats_all st
                ON st.NFL_player_id=p.NFL_player_id AND st.year=p.year AND st.week=p.week
              WHERE p.db_name IS NOT NULL AND s.scoring_pass_td IS NULL
                AND COALESCE({player_pos_expr}, {stat_pos_expr}, '')='QB'
              GROUP BY 1,2
            """)
            report["pass_td_evidence"] = domain_counts(con, """
              SELECT CASE WHEN compared < 10 THEN 'too_few_qb_rows'
                WHEN abs_error_4 + 0.10 < abs_error_6 THEN 'best_fit_4pt'
                WHEN abs_error_6 + 0.10 < abs_error_4 THEN 'best_fit_6pt'
                ELSE 'ambiguous' END, COUNT(*) FROM _pass_td_evidence GROUP BY 1
            """)
            report["pass_td_evidence_rows"] = [dict(zip(("db_name","year","matches_4","matches_6","abs_error_4","abs_error_6","compared"), r)) for r in con.execute("SELECT db_name, season_year, matches_4, matches_6, abs_error_4, abs_error_6, compared FROM _pass_td_evidence ORDER BY db_name, season_year").fetchall()]
        else:
            report["pass_td_evidence"] = {"SKIPPED_MISSING_SOURCE_COLUMNS": sorted({"scoring_pass_td", "NFL_player_id", "year", "week", "fantasy_points"} - (scols | pcols | stat_cols))}

        # Playoff bracket: count distinct playoff teams from the canonical
        # team qualification flag.  A bracket is safe only for exactly 4, 6,
        # or 8 teams; all other counts remain unresolved.
        team_expr = "COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)),''),NULLIF(TRIM(CAST(p.team_name AS VARCHAR)),''),NULLIF(TRIM(CAST(p.manager AS VARCHAR)),''))"
        if "playoff_teams" in scols and {"db_name", "year", "team_key", "team_name", "manager"} <= pcols:
            con.execute(f"""
              CREATE OR REPLACE TEMP TABLE _bracket_evidence AS
              SELECT p.db_name, p.year,
                COUNT(DISTINCT CASE WHEN p.made_playoffs=1 THEN {team_expr} END) made_teams,
                COUNT(DISTINCT CASE WHEN p.final_playoff_seed BETWEEN 1 AND 32 THEN {team_expr} END) seeded_teams,
                COUNT(DISTINCT CASE WHEN p.is_playoffs=1 THEN {team_expr} END) playoff_week_teams
              FROM base.public.player_fantasy p JOIN base.public.league_settings s
                ON s.db_name=p.db_name AND s.year=p.year
              WHERE p.db_name IS NOT NULL AND s.playoff_teams IS NULL
              GROUP BY 1,2
            """)
            report["bracket_evidence"] = domain_counts(con, """
              SELECT CASE
                WHEN COALESCE(NULLIF(made_teams,0),NULLIF(seeded_teams,0),NULLIF(playoff_week_teams,0)) IN (4,6,8)
                  THEN CAST(COALESCE(NULLIF(made_teams,0),NULLIF(seeded_teams,0),NULLIF(playoff_week_teams,0)) AS VARCHAR)
                ELSE 'unresolved' END, COUNT(*)
              FROM _bracket_evidence GROUP BY 1
            """)
            report["bracket_evidence_rows"] = [dict(zip(("db_name","year","made_teams","seeded_teams","playoff_week_teams"), r)) for r in con.execute("SELECT * FROM _bracket_evidence ORDER BY db_name, year").fetchall()]
        else:
            report["bracket_evidence"] = {"SKIPPED_MISSING_COLUMNS": sorted({"playoff_teams", "made_playoffs", "team_key", "team_name", "manager"} - (scols | pcols))}

        # Roster lane evidence comes only from actual lineup slots.  It is
        # deliberately separate from num_teams: IDP/SFLEX signals must be
        # visible in the weekly lineup, otherwise the result is unresolved.
        if "roster_IDP" in scols and {"db_name", "year", "fantasy_position"} <= pcols:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE _roster_evidence AS
              SELECT p.db_name, p.year,
                MAX(CASE WHEN UPPER(TRIM(fantasy_position)) IN ('DL','LB','DB','IDP') THEN 1 ELSE 0 END) idp_signal,
                MAX(CASE WHEN UPPER(TRIM(fantasy_position)) IN ('SFLEX','SFLX','SUPERFLEX','SUPER_FLEX','SUPER FLEX') THEN 1 ELSE 0 END) sflx_signal,
                COUNT(*) rows_seen
              FROM base.public.player_fantasy p JOIN base.public.league_settings s
                ON s.db_name=p.db_name AND s.year=p.year
              WHERE p.db_name IS NOT NULL
                AND s.roster_IDP IS NULL AND s.roster_DL IS NULL AND s.roster_LB IS NULL
                AND s.roster_DB IS NULL AND s.roster_DB_LB IS NULL AND s.roster_DL_LB IS NULL
                AND s.roster_SUPER_FLEX IS NULL AND s.roster_FLX IS NULL
              GROUP BY 1,2
            """)
            report["roster_evidence"] = domain_counts(con, """
              SELECT CASE WHEN idp_signal=1 THEN 'idp' WHEN sflx_signal=1 THEN 'sflx' ELSE 'flx_or_unresolved' END, COUNT(*)
              FROM _roster_evidence GROUP BY 1
            """)
        else:
            report["roster_evidence"] = {"SKIPPED_MISSING_COLUMNS": sorted({"roster_IDP", "db_name", "year", "fantasy_position"} - (scols | pcols))}

        # Best-ball evidence is reported, not guessed.  A changing set of
        # starters is observable, but it is not by itself unique to best ball;
        # the audit therefore exposes the signal and its ambiguity.
        if "sleeper_best_ball" in scols and {"db_name", "year", "week", "team_key", "is_started", "NFL_player_id"} <= pcols:
            con.execute("""
              CREATE OR REPLACE TEMP TABLE _lineup_evidence AS
              SELECT db_name, year,
                COUNT(DISTINCT week) AS week_count,
                COUNT(DISTINCT CASE WHEN is_started=1 THEN CAST(NFL_player_id AS VARCHAR) END) AS started_player_count,
                COUNT(DISTINCT CONCAT(CAST(week AS VARCHAR),'|',CAST(team_key AS VARCHAR),'|',CAST(NFL_player_id AS VARCHAR))) AS started_cell_count
              FROM base.public.player_fantasy p JOIN base.public.league_settings s USING (db_name,year)
              WHERE p.db_name IS NOT NULL AND s.sleeper_best_ball IS NULL
              GROUP BY 1,2
            """)
            report["bestball_evidence"] = {
                "league_years_examined": count(con, "SELECT COUNT(*) FROM _lineup_evidence"),
                "note": "starter-set change is reported as evidence; it is not treated as a unique best-ball proof without a source flag",
                "by_platform": domain_counts(con, """
                  SELECT COALESCE(CAST(s.platform AS VARCHAR),'NULL'), COUNT(*)
                  FROM _lineup_evidence e JOIN base.public.league_settings s USING (db_name,year)
                  GROUP BY 1 ORDER BY 1
                """),
            }
        else:
            report["bestball_evidence"] = {"SKIPPED_MISSING_COLUMNS": sorted({"sleeper_best_ball", "db_name", "year", "week", "team_key", "is_started", "NFL_player_id"} - (scols | pcols))}

        report["status"] = "audit_complete_no_promotion"
        return report
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    report = audit(args.base, args.ops)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("status", "base_bytes", "base_player_rows", "base_league_player_rows", "position_target_null_rows", "position_bio_fillable_rows")}, sort_keys=True))


if __name__ == "__main__":
    main()
