"""Read-only audit for propagating league settings onto canonical player rows.

This script never creates or replaces a research cache.  It audits whether the
existing canonical player table can be joined to one settings row at
``(db_name, year)`` and records the exact baseline needed to detect accidental
row growth later.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_SETTINGS = {
    "db_name", "year", "num_teams", "scoring_rec", "scoring_pass_td",
    "playoff_teams", "is_dynasty", "sleeper_best_ball",
    "roster_QB", "roster_RB", "roster_WR", "roster_TE", "roster_K",
    "roster_DEF", "roster_IDP", "roster_DL", "roster_LB", "roster_DB",
    "roster_DB_LB", "roster_DL_LB", "roster_SUPER_FLEX", "roster_FLX",
}
REQUIRED_PLAYER = {"db_name", "year", "position"}
EXPECTED_DIMENSIONS = (
    "league_size", "scoring", "pass_td", "playoff_teams",
    "roster_config", "format", "draft_style",
)


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM duckdb_tables() "
        "WHERE schema_name='public' AND table_name=?", [table]
    ).fetchone()[0])


def columns(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f'DESCRIBE public."{table}"').fetchall()}


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    value = con.execute(sql).fetchone()[0]
    return int(value or 0)


def json_safe(value: Any) -> Any:
    if hasattr(value, "item"):
        value = value.item()
    return value


def audit(base: Path) -> dict[str, Any]:
    if not base.exists():
        raise SystemExit(f"cache file does not exist: {base}")

    con = duckdb.connect(str(base), read_only=True)
    try:
        report: dict[str, Any] = {
            "base_path": str(base.resolve()),
            "cache_bytes": base.stat().st_size,
            "tables": [r[0] for r in con.execute(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE schema_name='public' ORDER BY table_name"
            ).fetchall()],
            "expected_dimension_columns": list(EXPECTED_DIMENSIONS),
        }
        for required_table in ("player_fantasy", "league_settings"):
            if not table_exists(con, required_table):
                raise SystemExit(f"required table missing: public.{required_table}")

        pcols = columns(con, "player_fantasy")
        scols = columns(con, "league_settings")
        report["player_schema"] = pcols
        report["settings_schema"] = scols
        report["missing_player_columns"] = sorted(REQUIRED_PLAYER - set(pcols))
        report["missing_settings_columns"] = sorted(REQUIRED_SETTINGS - set(scols))
        report["player_rows"] = scalar(con, "SELECT COUNT(*) FROM public.player_fantasy")
        report["player_league_years"] = scalar(con, """
            SELECT COUNT(*) FROM (
              SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year
              FROM public.player_fantasy
              WHERE db_name IS NOT NULL AND TRY_CAST(year AS INTEGER) IS NOT NULL
            )
        """) if {"db_name", "year"} <= set(pcols) else None
        report["settings_rows"] = scalar(con, "SELECT COUNT(*) FROM public.league_settings")
        report["settings_league_years"] = scalar(con, """
            SELECT COUNT(*) FROM (
              SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year
              FROM public.league_settings
              WHERE db_name IS NOT NULL AND TRY_CAST(year AS INTEGER) IS NOT NULL
            )
        """) if {"db_name", "year"} <= set(scols) else None
        if {"db_name", "year"} <= set(scols):
            report["settings_duplicate_groups"] = scalar(con, """
                SELECT COUNT(*) FROM (
                  SELECT db_name, TRY_CAST(year AS INTEGER) AS year
                  FROM public.league_settings
                  GROUP BY 1, 2 HAVING COUNT(*) > 1
                )
            """)
            report["settings_duplicate_rows"] = scalar(con, """
                SELECT COALESCE(SUM(n - 1), 0) FROM (
                  SELECT COUNT(*) AS n
                  FROM public.league_settings
                  GROUP BY db_name, TRY_CAST(year AS INTEGER)
                  HAVING COUNT(*) > 1
                )
            """)
        else:
            report["settings_duplicate_groups"] = None
            report["settings_duplicate_rows"] = None
        report["player_null_db_name"] = (
            scalar(con, "SELECT COUNT(*) FROM public.player_fantasy WHERE db_name IS NULL")
            if "db_name" in pcols else None
        )
        report["player_null_year"] = (
            scalar(con, "SELECT COUNT(*) FROM public.player_fantasy WHERE TRY_CAST(year AS INTEGER) IS NULL")
            if "year" in pcols else None
        )
        report["player_null_position"] = (
            scalar(con, "SELECT COUNT(*) FROM public.player_fantasy WHERE position IS NULL OR TRIM(CAST(position AS VARCHAR))=''" )
            if "position" in pcols else None
        )
        report["player_rows_with_db_name"] = (
            scalar(con, "SELECT COUNT(*) FROM public.player_fantasy WHERE db_name IS NOT NULL")
            if "db_name" in pcols else None
        )
        report["non_league_rows_without_db_name"] = report["player_null_db_name"]
        report["league_rows_null_position"] = (
            scalar(con, """
                SELECT COUNT(*) FROM public.player_fantasy
                WHERE db_name IS NOT NULL AND (position IS NULL OR TRIM(CAST(position AS VARCHAR))='')
            """) if {"db_name", "position"} <= set(pcols) else None
        )

        # These counts use a deduplicated settings relation only to measure
        # coverage.  The audit still fails on duplicates; it never silently
        # chooses one settings row.
        can_join = {"db_name", "year"} <= set(pcols) and {"db_name", "year"} <= set(scols)
        report["player_rows_without_settings"] = scalar(con, """
            SELECT COUNT(*)
            FROM public.player_fantasy p
            LEFT JOIN (
              SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year
              FROM public.league_settings
            ) s ON s.db_name=p.db_name AND s.year=TRY_CAST(p.year AS INTEGER)
            WHERE p.db_name IS NOT NULL AND s.db_name IS NULL
        """) if can_join else None
        report["player_league_years_without_settings"] = scalar(con, """
            SELECT COUNT(*) FROM (
              SELECT DISTINCT p.db_name, TRY_CAST(p.year AS INTEGER) AS year
              FROM public.player_fantasy p
              LEFT JOIN (
                SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year
                FROM public.league_settings
              ) s ON s.db_name=p.db_name AND s.year=TRY_CAST(p.year AS INTEGER)
              WHERE p.db_name IS NOT NULL AND s.db_name IS NULL
            )
        """) if can_join else None
        report["join_row_count_if_unique"] = scalar(con, """
            SELECT COUNT(*)
            FROM public.player_fantasy p
            JOIN (
              SELECT db_name, TRY_CAST(year AS INTEGER) AS year
              FROM public.league_settings
              GROUP BY 1, 2
            ) s ON s.db_name=p.db_name AND s.year=TRY_CAST(p.year AS INTEGER)
            WHERE p.db_name IS NOT NULL
        """) if can_join else None

        # Settings-domain audit.  These are the exact labels the converter is
        # expected to use; NULL/invalid values are reported, never defaulted.
        report["settings_domain_counts"] = {}
        expressions = {
            "scoring": "CASE WHEN scoring_rec IS NULL THEN 'NULL' WHEN scoring_rec=0 THEN 'standard' WHEN scoring_rec<0.75 THEN 'half_ppr' WHEN scoring_rec>=0.75 THEN 'ppr' ELSE 'INVALID' END",
            "pass_td": "CASE WHEN scoring_pass_td IS NULL THEN 'NULL' WHEN scoring_pass_td<5 THEN '4pt' WHEN scoring_pass_td>=5 THEN '6pt' ELSE 'INVALID' END",
            "playoff_teams": "CASE WHEN playoff_teams IS NULL THEN 'NULL' WHEN playoff_teams<=5 THEN '4' WHEN playoff_teams<=7 THEN '6' ELSE '8+' END",
            "roster_config": "CASE WHEN COALESCE(roster_IDP,0)+COALESCE(roster_DL,0)+COALESCE(roster_LB,0)+COALESCE(roster_DB,0)+COALESCE(roster_DB_LB,0)+COALESCE(roster_DL_LB,0)>0 THEN 'idp' WHEN COALESCE(roster_SUPER_FLEX,0)>0 THEN 'sflx' WHEN roster_FLX IS NOT NULL THEN 'flx' ELSE 'NULL' END",
            "format": "CASE WHEN sleeper_best_ball IS NULL THEN 'NULL' WHEN CAST(sleeper_best_ball AS BOOLEAN) THEN 'bestball' ELSE 'managed' END",
            "draft_style": "CASE WHEN is_dynasty IS NULL THEN 'NULL' WHEN CAST(is_dynasty AS BOOLEAN) THEN 'dynasty' ELSE 'redraft' END",
        }
        expression_requirements = {
            "scoring": {"scoring_rec"},
            "pass_td": {"scoring_pass_td"},
            "playoff_teams": {"playoff_teams"},
            "roster_config": {"roster_IDP", "roster_DL", "roster_LB", "roster_DB", "roster_DB_LB", "roster_DL_LB", "roster_SUPER_FLEX", "roster_FLX"},
            "format": {"sleeper_best_ball"},
            "draft_style": {"is_dynasty"},
        }
        for name, expr in expressions.items():
            missing = sorted(expression_requirements[name] - set(scols))
            if missing:
                report["settings_domain_counts"][name] = {"SKIPPED_MISSING_COLUMNS": missing}
                continue
            rows = con.execute(
                f"SELECT {expr} AS value, COUNT(*) AS n FROM public.league_settings GROUP BY 1 ORDER BY 1"
            ).fetchall()
            report["settings_domain_counts"][name] = {str(json_safe(r[0])): int(r[1]) for r in rows}

        # League size is intentionally audited as declared inputs only.  A
        # position-specific size formula must use roster_* plus FLX/SUPER_FLEX;
        # num_teams alone is not accepted as the position-size dimension.
        report["league_size_input_nulls"] = {}
        for pos in ("QB", "RB", "WR", "TE", "K", "DEF"):
            source = f"roster_{pos}"
            report["league_size_input_nulls"][source] = (
                scalar(con, f"SELECT COUNT(*) FROM public.league_settings WHERE {source} IS NULL")
                if source in scols else None
            )
        for source in ("roster_FLX", "roster_SUPER_FLEX"):
            report["league_size_input_nulls"][source] = (
                scalar(con, f"SELECT COUNT(*) FROM public.league_settings WHERE {source} IS NULL")
                if source in scols else None
            )

        report["status"] = "pass" if not (
            report["missing_player_columns"]
            or report["missing_settings_columns"]
            or report["settings_duplicate_groups"] is None
            or report["settings_duplicate_groups"]
            or report["player_rows_without_settings"] is None
            or report["player_rows_without_settings"]
            or report["player_null_year"]
            or report["join_row_count_if_unique"] is None
            or report["league_rows_null_position"] is None
            or report["league_rows_null_position"]
            or report["join_row_count_if_unique"] != report["player_rows_with_db_name"]
        ) else "fail"
        return report
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    report = audit(args.base)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True, default=json_safe) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("status", "cache_bytes", "player_rows", "settings_rows", "player_rows_without_settings", "settings_duplicate_groups")}, sort_keys=True))
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
