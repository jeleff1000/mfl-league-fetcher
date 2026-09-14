#!/usr/bin/env python3
"""Backfill blended draft pick scores without rerunning full imports.

This script is intentionally narrow:
- recompute draft expectation/z-score columns for one league at a time
- rebuild draft aggregate tables from those refreshed score columns
- refresh homepage draft fields that depend on pick score ordering

It does not create or alter Fly tables. Live writes are limited to scoped
UPDATE/DELETE/INSERT statements against existing columns.

Examples:
  python scripts/backfill_draft_scores.py --db demo_league --dry-run
  python scripts/backfill_draft_scores.py --db demo_league --apply
  python scripts/backfill_draft_scores.py --all --apply --limit 25
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from numbers import Integral, Real
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

LEAGUES_DB = "___leagues"
OPS_DB = "___ops"

DRAFT_SCORE_COLUMNS = [
    "expected_lamar",
    "draft_value_zscore",
    "pick_quality_zscore",
    "pick_score",
    "draft_grade",
    "manager_draft_score",
    "manager_draft_grade",
    "manager_draft_percentile_alltime",
    "keeper_draft_score",
    "keeper_draft_grade",
]

DRAFT_KEY_COLUMNS = ["db_name", "year", "round", "pick", "player", "manager"]
AGGREGATE_TABLES = ["draft_manager_season", "draft_manager_career", "draft_player_career"]
DEFAULT_GLOBAL_SOURCE_CACHE = REPO_ROOT / "tmp" / "draft_score_global_source.parquet"

LEAGUE_DRAFT_HIGHLIGHT_COLUMNS = [
    "best_pick_player",
    "best_pick_manager",
    "best_pick_position",
    "best_pick_round",
    "best_pick_year",
    "best_pick_lamar",
    "best_pick_headshot",
    "best_pick_cost",
    "best_pick_pick",
    "worst_pick_player",
    "worst_pick_manager",
    "worst_pick_position",
    "worst_pick_round",
    "worst_pick_year",
    "worst_pick_lamar",
    "worst_pick_headshot",
    "worst_pick_cost",
    "worst_pick_pick",
]

PROFILE_DRAFT_PICK_COLUMNS = [
    "best_pick_player",
    "best_pick_round",
    "best_pick_year",
    "best_pick_lamar",
    "best_pick_headshot",
    "best_pick_cost",
    "best_pick_pick",
    "worst_pick_player",
    "worst_pick_round",
    "worst_pick_year",
    "worst_pick_lamar",
    "worst_pick_headshot",
    "worst_pick_cost",
    "worst_pick_pick",
]

HOMEPAGE_PROFILE_COLUMNS = [
    *PROFILE_DRAFT_PICK_COLUMNS,
    "draft_career_grade",
    *[f"season_{column}" for column in PROFILE_DRAFT_PICK_COLUMNS],
    "season_draft_grade",
]


def load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE entries from .env without overriding the shell."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    try:
        if pd.isna(value):
            return "NULL"
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return sql_literal(value.isoformat(sep=" "))
    if isinstance(value, datetime):
        return sql_literal(value.isoformat(sep=" "))
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Integral):
        return str(int(value))
    if isinstance(value, Real):
        value_float = float(value)
        if math.isnan(value_float) or math.isinf(value_float):
            return "NULL"
        return repr(value_float)
    return sql_literal(value)


def score_to_draft_grade(avg_score: float | None) -> str | None:
    if avg_score is None:
        return None
    try:
        score = float(avg_score)
    except (TypeError, ValueError):
        return None
    z_score = (score - 100.0) / 15.0 if abs(score) > 10 else score
    if z_score >= 1.5:
        return "A+"
    if z_score >= 1.0:
        return "A"
    if z_score >= 0.7:
        return "A-"
    if z_score >= 0.45:
        return "B+"
    if z_score >= 0.15:
        return "B"
    if z_score >= -0.15:
        return "B-"
    if z_score >= -0.45:
        return "C"
    if z_score >= -0.9:
        return "D"
    return "F"


def rows_to_values(rows: list[dict[str, Any]], columns: list[str]) -> str:
    return ",\n".join("(" + ", ".join(sql_value(row.get(column)) for column in columns) + ")" for row in rows)


def insert_dataframe_values(
    writer: Any,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
    *,
    dry_run: bool,
    chunk_size: int,
) -> int:
    if df.empty:
        return 0
    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    for column in columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[columns]
    column_sql = ", ".join(sql_ident(column) for column in columns)
    inserted = 0
    for start in range(0, len(prepared), chunk_size):
        chunk = prepared.iloc[start : start + chunk_size]
        rows = chunk.to_dict("records")
        values_sql = rows_to_values(rows, columns)
        sql = f"INSERT INTO public.{table_name} ({column_sql}) VALUES {values_sql}"
        if not dry_run:
            writer.execute(sql, database=LEAGUES_DB)
        inserted += len(rows)
    return inserted


def table_columns(reader: Any, table_name: str) -> list[str]:
    rows = reader.query(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = {sql_literal(table_name)}
        ORDER BY ordinal_position
        """,
        database=LEAGUES_DB,
    )
    return [str(row["column_name"]) for row in rows]


def require_columns(reader: Any, table_name: str, required: list[str]) -> list[str]:
    columns = table_columns(reader, table_name)
    missing = [column for column in required if column not in columns]
    if missing:
        raise RuntimeError(f"public.{table_name} is missing required columns for no-DDL backfill: {', '.join(missing)}")
    return columns


def fetch_table(reader: Any, table_name: str, db_name: str) -> pd.DataFrame:
    return reader.query_df(
        f"SELECT * FROM public.{table_name} WHERE db_name = {sql_literal(db_name)}",
        database=LEAGUES_DB,
    )


def list_draft_dbs(
    reader: Any,
    *,
    limit: int | None = None,
    offset: int = 0,
    shard_index: int | None = None,
    shard_count: int | None = None,
) -> list[str]:
    limit_sql = f" LIMIT {int(limit)}" if limit is not None else ""
    offset_sql = f" OFFSET {int(offset)}" if offset else ""
    df = reader.query_df(
        f"""
        SELECT db_name, COUNT(*) AS draft_rows
        FROM public.draft
        WHERE db_name IS NOT NULL
          AND player IS NOT NULL
        GROUP BY db_name
        HAVING COUNT(*) > 0
        ORDER BY db_name
        {limit_sql}{offset_sql}
        """,
        database=LEAGUES_DB,
    )
    if df.empty:
        return []
    dbs = [str(value) for value in df["db_name"].tolist()]
    if shard_index is not None and shard_count is not None:
        if shard_count <= 0:
            raise ValueError("--shard-count must be positive")
        if shard_index < 0 or shard_index >= shard_count:
            raise ValueError("--shard-index must be between 0 and shard-count - 1")
        dbs = [db_name for idx, db_name in enumerate(dbs) if idx % shard_count == shard_index]
    return dbs


def build_global_source_cache(reader: Any, path: Path, *, refresh: bool) -> Path | None:
    """Materialize all draft rows needed for global draft-score priors.

    This is a read-only Fly query and a local cache write. It avoids rebuilding
    the same universal draft prior separately for every league in a fleet run.
    """
    if path.exists() and not refresh:
        print(f"Using existing draft global source cache: {path}", flush=True)
        return path

    path.parent.mkdir(parents=True, exist_ok=True)
    timeout = int(os.environ.get("DRAFT_SCORE_SOURCE_TIMEOUT_SECONDS", "360"))
    reader.TIMEOUT_SECONDS = max(int(getattr(reader, "TIMEOUT_SECONDS", 75)), timeout)
    reader.MAX_RETRIES = max(
        int(getattr(reader, "MAX_RETRIES", 6)), int(os.environ.get("DRAFT_SCORE_SOURCE_MAX_RETRIES", "10"))
    )
    reader.RETRY_MAX_DELAY = max(
        int(getattr(reader, "RETRY_MAX_DELAY", 15)),
        int(os.environ.get("DRAFT_SCORE_SOURCE_RETRY_MAX_DELAY", "45")),
    )
    print(f"Building draft global source cache: {path}", flush=True)
    df = reader.query_df(
        """
        WITH raw AS (
            SELECT d.db_name, d.year, TRY_CAST(d.round AS INTEGER) AS round,
                   TRY_CAST(d.pick AS INTEGER) AS pick, d.manager,
                   CASE
                     WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) IN ('D/ST', 'DST', 'D')
                       THEN 'DEF'
                     WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) = ''
                       THEN 'UNK'
                     ELSE UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1)))
                   END AS position,
                   CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN 'keeper' ELSE 'draft' END AS cohort,
                   CASE
                     WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1
                       THEN 'keeper'
                     WHEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), ''))) IN ('startup', 'rookie', 'veteran')
                       THEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), '')))
                     ELSE 'redraft'
                   END AS draft_market,
                   CASE
                     WHEN LOWER(TRIM(COALESCE(CAST(d.draft_type AS VARCHAR), ''))) = 'auction'
                       OR COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) > 0
                     THEN 'auction' ELSE 'snake'
                   END AS draft_kind,
                   COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) AS cost,
                   TRY_CAST(d.manager_lamar AS DOUBLE) AS manager_lamar
            FROM public.draft d
            WHERE d.db_name IS NOT NULL
              AND d.year IS NOT NULL
              AND d.manager_lamar IS NOT NULL
        ),
        year_stats AS (
            SELECT db_name, year, draft_market, draft_kind, cohort,
                   COUNT(*) AS picks_in_market,
                   COUNT(DISTINCT manager) AS managers_in_market,
                   MAX(COALESCE(round, 0)) AS max_round,
                   SUM(COALESCE(cost, 0)) AS total_cost
            FROM raw
            GROUP BY db_name, year, draft_market, draft_kind, cohort
        ),
        settings AS (
            SELECT db_name, year,
                   MAX(COALESCE(num_teams, 0)) AS num_teams,
                   MAX(COALESCE(draft_rounds, 0)) AS draft_rounds,
                   MAX(COALESCE(scoring_rec, 0)) AS scoring_rec,
                   MAX(COALESCE(scoring_pass_td, 4)) AS scoring_pass_td,
                   MAX(COALESCE(roster_QB, 0)) AS roster_qb,
                   MAX(COALESCE(roster_RB, 0)) AS roster_rb,
                   MAX(COALESCE(roster_WR, 0)) AS roster_wr,
                   MAX(COALESCE(roster_TE, 0)) AS roster_te,
                   MAX(COALESCE(roster_K, 0)) AS roster_k,
                   MAX(COALESCE(roster_DEF, 0)) AS roster_def,
                   MAX(COALESCE(roster_FLX, 0)) AS roster_flex,
                   MAX(COALESCE(roster_SUPER_FLEX, 0)) AS roster_super_flex,
                   MAX(COALESCE(roster_REC_FLEX, 0)) AS roster_rec_flex,
                   MAX(COALESCE(roster_BN, 0)) AS roster_bn,
                   MAX(COALESCE(roster_LB, 0) + COALESCE(roster_DL, 0) + COALESCE(roster_DB, 0)
                       + COALESCE(roster_IDP, 0) + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0)) AS idp_slots,
                   MAX(COALESCE(max_keepers, 0)) AS max_keepers,
                   MAX(CASE WHEN COALESCE(is_dynasty, false) THEN 1 ELSE 0 END) AS is_dynasty,
                   MAX(COALESCE(sleeper_taxi_slots, 0)) AS taxi_slots,
                   MAX(CASE WHEN COALESCE(sleeper_pick_trading, false) THEN 1 ELSE 0 END) AS pick_trading,
                   MAX(CASE WHEN COALESCE(uses_median, false) THEN 1 ELSE 0 END) AS uses_median
            FROM public.league_settings
            GROUP BY db_name, year
        ),
        profiled AS (
            SELECT r.*,
                   COALESCE(NULLIF(s.num_teams, 0), NULLIF(ys.managers_in_market, 0), 12) AS teams,
                   COALESCE(NULLIF(s.draft_rounds, 0), NULLIF(ys.max_round, 0), 16) AS draft_rounds,
                   COALESCE(s.scoring_rec, 0) AS scoring_rec,
                   COALESCE(s.scoring_pass_td, 4) AS pass_td_pts,
                   CASE WHEN COALESCE(s.roster_super_flex, 0) > 0 OR COALESCE(s.roster_qb, 0) >= 2 THEN 1 ELSE 0 END AS superflex,
                   CASE WHEN COALESCE(s.idp_slots, 0) > 0 THEN 1 ELSE 0 END AS idp,
                   COALESCE(s.roster_bn, 0) AS bench_count,
                   COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0) + COALESCE(s.roster_rec_flex, 0) AS flex_count,
                   COALESCE(s.roster_qb, 0) + COALESCE(s.roster_rb, 0) + COALESCE(s.roster_wr, 0)
                     + COALESCE(s.roster_te, 0) + COALESCE(s.roster_k, 0) + COALESCE(s.roster_def, 0)
                     + COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0)
                     + COALESCE(s.roster_rec_flex, 0) + COALESCE(s.idp_slots, 0) + COALESCE(s.roster_bn, 0)
                     AS total_roster_slots,
                   CASE
                     WHEN COALESCE(s.is_dynasty, 0) = 1 OR COALESCE(s.taxi_slots, 0) > 0
                       OR COALESCE(s.pick_trading, 0) = 1 OR r.draft_market IN ('startup', 'rookie', 'veteran')
                     THEN 1 ELSE 0
                   END AS dynasty_like,
                   CASE WHEN COALESCE(s.max_keepers, 0) > 0 OR r.cohort = 'keeper' THEN 1 ELSE 0 END AS keeper_like,
                   COALESCE(s.uses_median, 0) AS uses_median,
                   ys.picks_in_market, ys.managers_in_market, ys.total_cost
            FROM raw r
            JOIN year_stats ys
              ON r.db_name = ys.db_name AND r.year = ys.year
             AND r.draft_market = ys.draft_market AND r.draft_kind = ys.draft_kind AND r.cohort = ys.cohort
            LEFT JOIN settings s ON r.db_name = s.db_name AND r.year = s.year
        )
        SELECT db_name, year, draft_market, draft_kind, cohort, position, manager_lamar,
               teams, draft_rounds, scoring_rec, pass_td_pts, superflex, idp,
               bench_count, flex_count, total_roster_slots, dynasty_like,
               keeper_like, uses_median,
               CASE
                 WHEN draft_kind = 'auction' THEN
                   CASE
                     WHEN cost <= 0 THEN 'a_free'
                     WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.30 THEN 'a_30p'
                     WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.20 THEN 'a_20_29p'
                     WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.10 THEN 'a_10_19p'
                     WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.05 THEN 'a_05_09p'
                     ELSE 'a_01_04p'
                   END
                 ELSE
                   's_' || LPAD(CAST(LEAST(20, GREATEST(1,
                     CEIL(((COALESCE(pick, round, picks_in_market) - 1) * 20.0)
                     / GREATEST(picks_in_market, 1)))) AS VARCHAR), 2, '0')
               END AS capital_bucket
        FROM profiled
        """,
        database=LEAGUES_DB,
    )
    if df.empty:
        print("Draft global source cache query returned no rows; continuing without cache.", flush=True)
        return None

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    cache_conn = duckdb.connect(database=":memory:")
    cache_conn.register("_draft_global_source_upload", df)
    try:
        cache_conn.execute(
            f"COPY (SELECT * FROM _draft_global_source_upload) TO {sql_literal(tmp_path.as_posix())} (FORMAT PARQUET)"
        )
    finally:
        cache_conn.unregister("_draft_global_source_upload")
        cache_conn.close()
    tmp_path.replace(path)
    print(f"Cached {len(df):,} global draft source rows at {path}", flush=True)
    return path


def attach_global_source_cache(conn: duckdb.DuckDBPyConnection, path: Path | None) -> None:
    if not path or not path.exists():
        return
    conn.execute(f"CREATE TEMP VIEW _draft_global_source AS SELECT * FROM read_parquet({sql_literal(path.as_posix())})")


def make_local_connection(
    db_name: str,
    reader: Any,
    *,
    global_source_path: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    from multi_league.core.aggregate_ddl import ensure_aggregate_table
    from multi_league.transformations.aggregation.aggregation_utils import configure_table_catalog, get_active_catalog

    conn = duckdb.connect(database=":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")

    draft_df = fetch_table(reader, "draft", db_name)
    if draft_df.empty:
        raise RuntimeError(f"{db_name}: no draft rows found")
    conn.register("_draft_upload", draft_df)
    try:
        conn.execute("CREATE TABLE public.draft AS SELECT * FROM _draft_upload")
    finally:
        conn.unregister("_draft_upload")

    settings_df = fetch_table(reader, "league_settings", db_name)
    if settings_df.empty:
        draft_years = sorted({int(year) for year in draft_df["year"].dropna().tolist()})
        settings_df = pd.DataFrame({"db_name": [db_name] * len(draft_years), "year": draft_years})
    conn.register("_settings_upload", settings_df)
    try:
        conn.execute("CREATE TABLE public.league_settings AS SELECT * FROM _settings_upload")
    finally:
        conn.unregister("_settings_upload")
    ensure_local_league_settings_columns(conn)
    attach_global_source_cache(conn, global_source_path)

    configure_table_catalog(conn)
    for table_name in AGGREGATE_TABLES:
        ensure_aggregate_table(conn, get_active_catalog(), table_name)
    return conn


def ensure_local_league_settings_columns(conn: duckdb.DuckDBPyConnection) -> None:
    required_types = {
        "db_name": "VARCHAR",
        "year": "INTEGER",
        "num_teams": "INTEGER",
        "draft_rounds": "INTEGER",
        "scoring_rec": "DOUBLE",
        "scoring_pass_td": "INTEGER",
        "roster_QB": "INTEGER",
        "roster_RB": "INTEGER",
        "roster_WR": "INTEGER",
        "roster_TE": "INTEGER",
        "roster_K": "INTEGER",
        "roster_DEF": "INTEGER",
        "roster_FLX": "INTEGER",
        "roster_SUPER_FLEX": "INTEGER",
        "roster_REC_FLEX": "INTEGER",
        "roster_BN": "INTEGER",
        "roster_LB": "INTEGER",
        "roster_DL": "INTEGER",
        "roster_DB": "INTEGER",
        "roster_IDP": "INTEGER",
        "roster_DB_LB": "INTEGER",
        "roster_DL_LB": "INTEGER",
        "max_keepers": "INTEGER",
        "is_dynasty": "BOOLEAN",
        "sleeper_taxi_slots": "INTEGER",
        "sleeper_pick_trading": "BOOLEAN",
        "uses_median": "BOOLEAN",
    }
    existing = {row[1] for row in conn.execute("PRAGMA table_info('public.league_settings')").fetchall()}
    for column, dtype in required_types.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE public.league_settings ADD COLUMN {sql_ident(column)} {dtype}")


def run_local_scoring(conn: duckdb.DuckDBPyConnection, db_name: str) -> int:
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    engine = SQLEnrichments(db_name=db_name, conn=conn, data_dir="scratch")
    engine._ops_attached = True
    return int(engine.draft_value_zscore() or 0)


def run_local_aggregates(conn: duckdb.DuckDBPyConnection, db_name: str) -> dict[str, int]:
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career,
        aggregate_draft_manager_season,
        aggregate_draft_player_career,
    )

    return {
        "draft_manager_season": int(aggregate_draft_manager_season(conn, db_name) or 0),
        "draft_manager_career": int(aggregate_draft_manager_career(conn, db_name) or 0),
        "draft_player_career": int(aggregate_draft_player_career(conn, db_name) or 0),
    }


def upload_draft_scores(
    writer: Any,
    local_conn: duckdb.DuckDBPyConnection,
    db_name: str,
    *,
    dry_run: bool,
    chunk_size: int,
) -> int:
    columns = [*DRAFT_KEY_COLUMNS, *DRAFT_SCORE_COLUMNS]
    df = local_conn.execute(
        "SELECT " + ", ".join(sql_ident(column) for column in columns) + " FROM public.draft"
    ).fetchdf()
    updated = 0
    value_columns = [*DRAFT_KEY_COLUMNS, *DRAFT_SCORE_COLUMNS]
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : start + chunk_size]
        values_sql = rows_to_values(chunk.to_dict("records"), value_columns)
        set_sql = ",\n                ".join(
            f"{sql_ident(column)} = v.{sql_ident(column)}" for column in DRAFT_SCORE_COLUMNS
        )
        sql = f"""
            UPDATE public.draft AS d
            SET {set_sql}
            FROM (VALUES {values_sql}) AS v({", ".join(sql_ident(column) for column in value_columns)})
            WHERE d.db_name = v.db_name
              AND d.year IS NOT DISTINCT FROM v.year
              AND d.round IS NOT DISTINCT FROM v.round
              AND d.pick IS NOT DISTINCT FROM v.pick
              AND d.player IS NOT DISTINCT FROM v.player
              AND d.manager IS NOT DISTINCT FROM v.manager
        """
        if not dry_run:
            writer.execute(sql, database=LEAGUES_DB)
        updated += len(chunk)
    return updated


def upload_aggregates(
    writer: Any,
    local_conn: duckdb.DuckDBPyConnection,
    db_name: str,
    live_columns: dict[str, list[str]],
    *,
    dry_run: bool,
    chunk_size: int,
) -> dict[str, int]:
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    counts: dict[str, int] = {}
    for table_name in AGGREGATE_TABLES:
        expected_columns = list(AGGREGATE_TABLE_SPECS[table_name].column_types.keys())
        missing = [column for column in expected_columns if column not in live_columns[table_name]]
        if missing:
            raise RuntimeError(
                f"public.{table_name} is missing required columns for no-DDL backfill: {', '.join(missing)}"
            )
        df = local_conn.execute(
            f"SELECT {', '.join(sql_ident(column) for column in expected_columns)} "
            f"FROM public.{table_name} WHERE db_name = {sql_literal(db_name)}"
        ).fetchdf()
        if not dry_run:
            writer.execute(
                f"DELETE FROM public.{table_name} WHERE db_name = {sql_literal(db_name)}",
                database=LEAGUES_DB,
            )
        inserted = insert_dataframe_values(
            writer,
            table_name,
            df,
            expected_columns,
            dry_run=dry_run,
            chunk_size=chunk_size,
        )
        counts[table_name] = inserted
    return counts


def latest_homepage_year(reader: Any, db_name: str) -> int | None:
    row = reader.query(
        f"""
        SELECT COALESCE(
            (SELECT MAX(data_year) FROM public.homepage_league_summary WHERE db_name = {sql_literal(db_name)}),
            (SELECT MAX(year) FROM public.matchup WHERE db_name = {sql_literal(db_name)}),
            (SELECT MAX(year) FROM public.draft WHERE db_name = {sql_literal(db_name)})
        ) AS latest_year
        """,
        database=LEAGUES_DB,
    )
    if not row or row[0].get("latest_year") is None:
        return None
    return int(row[0]["latest_year"])


def draft_highlight_query(db_name: str, *, year: int | None = None, franchise_id: str | None = None) -> str:
    year_filter = f"AND d.year = {int(year)}" if year is not None else ""
    franchise_filter = f"AND d.franchise_id = {sql_literal(franchise_id)}" if franchise_id else ""
    return f"""
        WITH eligible AS (
            SELECT d.player, d.manager, d.franchise_id, d.position, d.year, d.round,
                   COALESCE(d.manager_lamar, 0) AS lamar,
                   d.cost, d.pick, d.NFL_player_id, d.draft_value_zscore,
                   ROW_NUMBER() OVER (
                       PARTITION BY d.franchise_id
                       ORDER BY d.draft_value_zscore DESC, COALESCE(d.manager_lamar, 0) DESC
                   ) AS best_rank,
                   ROW_NUMBER() OVER (
                       PARTITION BY d.franchise_id
                       ORDER BY d.draft_value_zscore ASC, COALESCE(d.manager_lamar, 0) ASC
                   ) AS worst_rank
            FROM public.draft d
            WHERE d.db_name = {sql_literal(db_name)}
              AND d.player IS NOT NULL
              AND d.draft_value_zscore IS NOT NULL
              AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0
              AND COALESCE(UPPER(TRIM(d.position)), '') NOT IN ('DEF', 'DST', 'D/ST', 'K')
              {year_filter}
              {franchise_filter}
        )
        SELECT * FROM eligible
        WHERE best_rank = 1 OR worst_rank = 1
    """


def fetch_league_highlight(
    reader: Any,
    db_name: str,
    *,
    year: int | None,
    descending: bool,
) -> dict[str, Any] | None:
    order_dir = "DESC" if descending else "ASC"
    row = reader.query(
        f"""
        SELECT d.player, d.manager, d.position, d.year, d.round,
               COALESCE(d.manager_lamar, 0) AS lamar,
               d.cost, d.pick, d.NFL_player_id
        FROM public.draft d
        WHERE d.db_name = {sql_literal(db_name)}
          AND d.player IS NOT NULL
          AND d.draft_value_zscore IS NOT NULL
          AND COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 0
          AND COALESCE(UPPER(TRIM(d.position)), '') NOT IN ('DEF', 'DST', 'D/ST', 'K')
          {f"AND d.year = {int(year)}" if year is not None else ""}
        ORDER BY d.draft_value_zscore {order_dir}, COALESCE(d.manager_lamar, 0) {order_dir}
        LIMIT 1
        """,
        database=LEAGUES_DB,
    )
    return dict(row[0]) if row else None


def fetch_headshots(reader: Any, rows: list[dict[str, Any]]) -> dict[str, str]:
    nfl_ids = sorted(
        {
            str(row.get("NFL_player_id")).strip()
            for row in rows
            if row.get("NFL_player_id") not in (None, "") and str(row.get("NFL_player_id")).strip()
        }
    )
    if not nfl_ids:
        return {}
    values = ", ".join(sql_literal(value) for value in nfl_ids)
    df = reader.query_df(
        f"""
        SELECT NFL_player_id, ANY_VALUE(headshot_url) AS headshot_url
        FROM nfl_historical.nfl_player_stats_all
        WHERE NFL_player_id IN ({values})
          AND headshot_url IS NOT NULL
        GROUP BY NFL_player_id
        """,
        database=OPS_DB,
    )
    if df.empty:
        return {}
    return {
        str(row["NFL_player_id"]): str(row["headshot_url"])
        for _, row in df.iterrows()
        if row.get("headshot_url") not in (None, "")
    }


def attach_headshots(reader: Any, rows: list[dict[str, Any]]) -> None:
    headshots = fetch_headshots(reader, rows)
    for row in rows:
        nfl_id = row.get("NFL_player_id")
        row["headshot_url"] = headshots.get(str(nfl_id)) if nfl_id not in (None, "") else None


def league_highlight_payload(row: dict[str, Any] | None, side: str) -> dict[str, Any]:
    prefix = f"{side}_pick_"
    if not row:
        return {
            f"{prefix}player": None,
            f"{prefix}manager": None,
            f"{prefix}position": None,
            f"{prefix}round": None,
            f"{prefix}year": None,
            f"{prefix}lamar": None,
            f"{prefix}headshot": None,
            f"{prefix}cost": None,
            f"{prefix}pick": None,
        }
    return {
        f"{prefix}player": row.get("player"),
        f"{prefix}manager": row.get("manager"),
        f"{prefix}position": row.get("position"),
        f"{prefix}round": row.get("round"),
        f"{prefix}year": row.get("year"),
        f"{prefix}lamar": round(float(row.get("lamar") or 0), 2),
        f"{prefix}headshot": row.get("headshot_url"),
        f"{prefix}cost": round(float(row["cost"]), 0) if row.get("cost") is not None else None,
        f"{prefix}pick": int(row["pick"]) if row.get("pick") is not None else None,
    }


def update_homepage_league_summary(
    reader: Any,
    writer: Any,
    db_name: str,
    *,
    dry_run: bool,
) -> int:
    latest_year = latest_homepage_year(reader, db_name)
    highlight_rows = [
        fetch_league_highlight(reader, db_name, year=None, descending=True),
        fetch_league_highlight(reader, db_name, year=None, descending=False),
    ]
    if latest_year is not None:
        highlight_rows.extend(
            [
                fetch_league_highlight(reader, db_name, year=latest_year, descending=True),
                fetch_league_highlight(reader, db_name, year=latest_year, descending=False),
            ]
        )
    compact_rows = [row for row in highlight_rows if row]
    attach_headshots(reader, compact_rows)

    payload: dict[str, Any] = {}
    payload.update({f"alltime_{k}": v for k, v in league_highlight_payload(highlight_rows[0], "best").items()})
    payload.update({f"alltime_{k}": v for k, v in league_highlight_payload(highlight_rows[1], "worst").items()})
    if latest_year is not None and len(highlight_rows) >= 4:
        payload.update({f"season_{k}": v for k, v in league_highlight_payload(highlight_rows[2], "best").items()})
        payload.update({f"season_{k}": v for k, v in league_highlight_payload(highlight_rows[3], "worst").items()})

    if not payload:
        return 0
    set_sql = ",\n                ".join(
        f"{sql_ident(column)} = {sql_value(value)}" for column, value in payload.items()
    )
    sql = f"""
        UPDATE public.homepage_league_summary
        SET {set_sql},
            last_updated = CURRENT_TIMESTAMP
        WHERE db_name = {sql_literal(db_name)}
    """
    if not dry_run:
        writer.execute(sql, database=LEAGUES_DB)
    return 1


def profile_pick_payload(row: dict[str, Any] | None, side: str, prefix: str = "") -> dict[str, Any]:
    key_prefix = f"{prefix}{side}_pick_"
    if not row:
        return {
            f"{key_prefix}player": None,
            f"{key_prefix}round": None,
            f"{key_prefix}year": None,
            f"{key_prefix}lamar": None,
            f"{key_prefix}headshot": None,
            f"{key_prefix}cost": None,
            f"{key_prefix}pick": None,
        }
    return {
        f"{key_prefix}player": row.get("player"),
        f"{key_prefix}round": row.get("round"),
        f"{key_prefix}year": row.get("year"),
        f"{key_prefix}lamar": round(float(row.get("lamar") or 0), 2),
        f"{key_prefix}headshot": row.get("headshot_url"),
        f"{key_prefix}cost": round(float(row["cost"]), 0) if row.get("cost") is not None else None,
        f"{key_prefix}pick": int(row["pick"]) if row.get("pick") is not None else None,
    }


def fetch_profile_highlights(
    reader: Any,
    db_name: str,
    *,
    year: int | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    df = reader.query_df(draft_highlight_query(db_name, year=year), database=LEAGUES_DB)
    if df.empty:
        return {}
    rows = df.to_dict("records")
    attach_headshots(reader, rows)
    by_franchise: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        franchise_id = str(row.get("franchise_id") or "")
        if not franchise_id:
            continue
        entry = by_franchise.setdefault(franchise_id, {})
        if row.get("best_rank") == 1:
            entry["best"] = row
        if row.get("worst_rank") == 1:
            entry["worst"] = row
    return by_franchise


def fetch_profile_grades(
    reader: Any,
    db_name: str,
    *,
    year: int | None,
) -> dict[str, str | None]:
    year_filter = f"AND year = {int(year)}" if year is not None else ""
    df = reader.query_df(
        f"""
        WITH manager_years AS (
            SELECT franchise_id, year,
                   COALESCE(CAST(draft_category AS VARCHAR), 'standard') AS draft_category,
                   manager_draft_score,
                   ARG_MAX(manager_draft_grade, manager_draft_score) AS manager_draft_grade
            FROM public.draft
            WHERE db_name = {sql_literal(db_name)}
              AND franchise_id IS NOT NULL
              AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
              AND manager_draft_score IS NOT NULL
              {year_filter}
            GROUP BY franchise_id, year, COALESCE(CAST(draft_category AS VARCHAR), 'standard'), manager_draft_score
        )
        SELECT franchise_id,
               AVG(manager_draft_score) AS avg_score,
               ARG_MAX(manager_draft_grade, manager_draft_score) AS manager_draft_grade
        FROM manager_years
        GROUP BY franchise_id
        """,
        database=LEAGUES_DB,
    )
    if df.empty:
        return {}
    grades: dict[str, str | None] = {}
    for _, row in df.iterrows():
        grade = row.get("manager_draft_grade")
        if year is None or grade in (None, "") or pd.isna(grade):
            grade = score_to_draft_grade(row.get("avg_score"))
        grades[str(row["franchise_id"])] = None if grade is None or pd.isna(grade) else str(grade)
    return grades


def update_homepage_manager_profiles(
    reader: Any,
    writer: Any,
    db_name: str,
    *,
    dry_run: bool,
    chunk_size: int,
) -> int:
    latest_year = latest_homepage_year(reader, db_name)
    profiles = reader.query_df(
        f"""
        SELECT franchise_id
        FROM public.homepage_manager_profiles
        WHERE db_name = {sql_literal(db_name)}
          AND franchise_id IS NOT NULL
        """,
        database=LEAGUES_DB,
    )
    if profiles.empty:
        return 0

    alltime_highlights = fetch_profile_highlights(reader, db_name, year=None)
    season_highlights = fetch_profile_highlights(reader, db_name, year=latest_year) if latest_year is not None else {}
    career_grades = fetch_profile_grades(reader, db_name, year=None)
    season_grades = fetch_profile_grades(reader, db_name, year=latest_year) if latest_year is not None else {}

    payload_rows: list[dict[str, Any]] = []
    for franchise_id in [str(value) for value in profiles["franchise_id"].tolist()]:
        row: dict[str, Any] = {"db_name": db_name, "franchise_id": franchise_id}
        highlights = alltime_highlights.get(franchise_id, {})
        row.update(profile_pick_payload(highlights.get("best"), "best"))
        row.update(profile_pick_payload(highlights.get("worst"), "worst"))
        row["draft_career_grade"] = career_grades.get(franchise_id)

        season = season_highlights.get(franchise_id, {})
        row.update(profile_pick_payload(season.get("best"), "best", "season_"))
        row.update(profile_pick_payload(season.get("worst"), "worst", "season_"))
        row["season_draft_grade"] = season_grades.get(franchise_id)
        payload_rows.append(row)

    update_columns = ["db_name", "franchise_id", *HOMEPAGE_PROFILE_COLUMNS]
    updated = 0
    for start in range(0, len(payload_rows), chunk_size):
        chunk = payload_rows[start : start + chunk_size]
        values_sql = rows_to_values(chunk, update_columns)
        set_sql = ",\n                ".join(
            f"{sql_ident(column)} = v.{sql_ident(column)}" for column in HOMEPAGE_PROFILE_COLUMNS
        )
        sql = f"""
            UPDATE public.homepage_manager_profiles AS p
            SET {set_sql}
            FROM (VALUES {values_sql}) AS v({", ".join(sql_ident(column) for column in update_columns)})
            WHERE p.db_name = v.db_name
              AND p.franchise_id = v.franchise_id
        """
        if not dry_run:
            writer.execute(sql, database=LEAGUES_DB)
        updated += len(chunk)
    return updated


def update_homepages(
    reader: Any,
    writer: Any,
    db_name: str,
    *,
    dry_run: bool,
    chunk_size: int,
) -> dict[str, int]:
    return {
        "homepage_league_summary": update_homepage_league_summary(reader, writer, db_name, dry_run=dry_run),
        "homepage_manager_profiles": update_homepage_manager_profiles(
            reader,
            writer,
            db_name,
            dry_run=dry_run,
            chunk_size=chunk_size,
        ),
    }


def draft_score_summary(reader: Any, db_name: str) -> dict[str, Any]:
    rows = reader.query(
        f"""
        SELECT COUNT(*) AS rows,
               COUNT(draft_value_zscore) AS scored,
               MIN(draft_value_zscore) AS min_score,
               MAX(draft_value_zscore) AS max_score,
               COUNT(*) FILTER (
                   WHERE pick_quality_zscore IS DISTINCT FROM draft_value_zscore
                      OR pick_score IS DISTINCT FROM CASE
                          WHEN draft_value_zscore IS NULL THEN NULL
                          ELSE ROUND(100.0 + 15.0 * CAST(draft_value_zscore AS DOUBLE), 3)
                      END
               ) AS score_drift
        FROM public.draft
        WHERE db_name = {sql_literal(db_name)}
        """,
        database=LEAGUES_DB,
    )
    return dict(rows[0]) if rows else {}


@dataclass
class LeagueResult:
    db_name: str
    status: str
    draft_rows: int = 0
    scored_rows: int = 0
    aggregate_rows: dict[str, int] = field(default_factory=dict)
    homepage_rows: dict[str, int] = field(default_factory=dict)
    error: str | None = None


def process_league(
    db_name: str,
    *,
    reader: Any,
    writer: Any,
    live_columns: dict[str, list[str]],
    dry_run: bool,
    skip_homepage: bool,
    chunk_size: int,
    global_source_path: Path | None,
) -> LeagueResult:
    before = draft_score_summary(reader, db_name)
    local_conn = make_local_connection(db_name, reader, global_source_path=global_source_path)
    try:
        run_local_scoring(local_conn, db_name)
        aggregate_counts = run_local_aggregates(local_conn, db_name)
        local_summary = local_conn.execute(
            """
            SELECT COUNT(*) AS rows, COUNT(draft_value_zscore) AS scored
            FROM public.draft
            """
        ).fetchone()
        updated_rows = upload_draft_scores(
            writer,
            local_conn,
            db_name,
            dry_run=dry_run,
            chunk_size=chunk_size,
        )
        upload_counts = upload_aggregates(
            writer,
            local_conn,
            db_name,
            live_columns,
            dry_run=dry_run,
            chunk_size=chunk_size,
        )
    finally:
        local_conn.close()

    homepage_counts: dict[str, int] = {}
    if not skip_homepage:
        if dry_run:
            # Homepage dry-run should still prove the queries can assemble.
            homepage_counts = update_homepages(
                reader,
                writer,
                db_name,
                dry_run=True,
                chunk_size=chunk_size,
            )
        else:
            homepage_counts = update_homepages(
                reader,
                writer,
                db_name,
                dry_run=False,
                chunk_size=chunk_size,
            )

    status = "dry-run" if dry_run else "updated"
    result = LeagueResult(
        db_name=db_name,
        status=status,
        draft_rows=int(local_summary[0] or updated_rows or before.get("rows") or 0),
        scored_rows=int(local_summary[1] or 0),
        aggregate_rows={**aggregate_counts, **{f"uploaded_{k}": v for k, v in upload_counts.items()}},
        homepage_rows=homepage_counts,
    )
    return result


def print_result(result: LeagueResult) -> None:
    if result.status == "failed":
        print(f"[FAIL] {result.db_name}: {result.error}", flush=True)
        return
    aggregate_text = ", ".join(f"{k}={v}" for k, v in sorted(result.aggregate_rows.items()))
    homepage_text = ", ".join(f"{k}={v}" for k, v in sorted(result.homepage_rows.items()))
    print(
        f"[{result.status.upper()}] {result.db_name}: "
        f"draft_rows={result.draft_rows:,} scored={result.scored_rows:,}"
        + (f" | aggregates: {aggregate_text}" if aggregate_text else "")
        + (f" | homepage: {homepage_text}" if homepage_text else ""),
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill blended draft scores and dependent draft homepage fields")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--db", help="Single league db_name to process")
    target.add_argument("--all", action="store_true", help="Process every league with draft rows")
    parser.add_argument("--apply", action="store_true", help="Write changes to Fly. Default is dry-run.")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode even if --apply is absent")
    parser.add_argument("--skip-homepage", action="store_true", help="Do not refresh homepage draft fields")
    parser.add_argument("--limit", type=int, help="Limit number of leagues in --all mode")
    parser.add_argument("--offset", type=int, default=0, help="Offset for --all mode")
    parser.add_argument("--shard-index", type=int, help="Zero-based shard index for fleet workers")
    parser.add_argument("--shard-count", type=int, help="Total shard count for fleet workers")
    parser.add_argument("--chunk-size", type=int, default=250, help="VALUES rows per live write chunk")
    parser.add_argument("--continue-on-error", action="store_true", help="Keep processing fleet after a league fails")
    parser.add_argument("--ledger", type=Path, help="Optional JSONL ledger path")
    parser.add_argument(
        "--global-source-cache",
        type=Path,
        default=DEFAULT_GLOBAL_SOURCE_CACHE,
        help="Parquet cache of global draft source rows for fleet scoring",
    )
    parser.add_argument(
        "--refresh-global-source-cache",
        action="store_true",
        help="Rebuild the global draft source cache before scoring",
    )
    parser.add_argument(
        "--no-global-source-cache",
        action="store_true",
        help="Disable the local global-source cache and query Fly per league instead",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    dry_run = not args.apply or args.dry_run

    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    writer = FlyWriter()

    live_columns = {
        "draft": require_columns(reader, "draft", [*DRAFT_KEY_COLUMNS, *DRAFT_SCORE_COLUMNS]),
        "homepage_league_summary": require_columns(
            reader,
            "homepage_league_summary",
            [
                "db_name",
                "last_updated",
                *[f"alltime_{column}" for column in LEAGUE_DRAFT_HIGHLIGHT_COLUMNS],
                *[f"season_{column}" for column in LEAGUE_DRAFT_HIGHLIGHT_COLUMNS],
            ],
        ),
        "homepage_manager_profiles": require_columns(
            reader,
            "homepage_manager_profiles",
            ["db_name", "franchise_id", *HOMEPAGE_PROFILE_COLUMNS],
        ),
    }
    for table_name in AGGREGATE_TABLES:
        live_columns[table_name] = table_columns(reader, table_name)

    if (args.shard_index is None) ^ (args.shard_count is None):
        raise SystemExit("--shard-index and --shard-count must be provided together")

    dbs = (
        [args.db]
        if args.db
        else list_draft_dbs(
            reader,
            limit=args.limit,
            offset=args.offset,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
        )
    )
    if not dbs:
        print("No leagues to process.", flush=True)
        return 0

    global_source_path: Path | None = None
    if not args.no_global_source_cache:
        cache_candidate = args.global_source_cache
        if args.all or args.refresh_global_source_cache or cache_candidate.exists():
            global_source_path = build_global_source_cache(
                reader,
                cache_candidate,
                refresh=args.refresh_global_source_cache or not cache_candidate.exists(),
            )

    print(
        f"Draft score backfill: {len(dbs)} league(s), mode={'dry-run' if dry_run else 'apply'}, "
        f"homepage={'skip' if args.skip_homepage else 'refresh'}, "
        f"global_source={'cache' if global_source_path else 'fly-per-league'}",
        flush=True,
    )

    results: list[LeagueResult] = []
    for db_name in dbs:
        try:
            result = process_league(
                str(db_name),
                reader=reader,
                writer=writer,
                live_columns=live_columns,
                dry_run=dry_run,
                skip_homepage=args.skip_homepage,
                chunk_size=max(1, int(args.chunk_size)),
                global_source_path=global_source_path,
            )
        except Exception as exc:  # noqa: BLE001
            result = LeagueResult(db_name=str(db_name), status="failed", error=str(exc))
            print_result(result)
            results.append(result)
            if not args.continue_on_error:
                break
            continue

        print_result(result)
        results.append(result)

    if args.ledger:
        args.ledger.parent.mkdir(parents=True, exist_ok=True)
        with args.ledger.open("a", encoding="utf-8") as fh:
            for result in results:
                fh.write(json.dumps(result.__dict__, sort_keys=True, default=str) + "\n")

    failed = [result for result in results if result.status == "failed"]
    if failed:
        print(f"Completed with {len(failed)} failure(s).", flush=True)
        return 1
    print("Draft score backfill complete.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
