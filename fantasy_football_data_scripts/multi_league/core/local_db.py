"""Local DuckDB storage for league data — replaces parquet files.

One DuckDB file per league, created with canonical DDL. Same schema as the
centralized ``___leagues`` catalog on Fly.io. Fetchers write here →
transformations run SQL here → ``upload_to_fly`` merges into ``___leagues``.

Usage:
    from multi_league.core.local_db import LocalLeagueDB

    with LocalLeagueDB(data_dir, league_name) as db:
        # Fetcher saves data
        db.save_table("matchup", df, year=2025)
        db.save_table("player_fantasy", df, year=2025)

        # Read back
        df = db.read_table("matchup")

        # Upload to Fly
        db.upload_to_fly(db_name)
"""

from __future__ import annotations

import os
import logging
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

try:
    import polars as pl
except Exception:  # pragma: no cover - optional dependency at import time
    pl = None

logger = logging.getLogger(__name__)


def _infer_fly_import_mode() -> str | None:
    explicit = os.environ.get("IMPORT_MODE") or os.environ.get("IMPORT_TYPE")
    if explicit:
        return explicit.lower().strip()

    workflow = os.environ.get("GITHUB_WORKFLOW", "").lower()
    if "full" in workflow or "paid" in workflow:
        return "full"
    if "quick" in workflow or "free" in workflow:
        return "quick"
    return None


def _infer_fly_platform() -> str | None:
    explicit = os.environ.get("PLATFORM") or os.environ.get("LEAGUE_PLATFORM")
    if explicit:
        return explicit.lower().strip()

    workflow = os.environ.get("GITHUB_WORKFLOW", "").lower()
    for platform in ("yahoo", "sleeper", "espn", "fleaflicker"):
        if platform in workflow:
            return platform
    return None


# Canonical DDL generators
from multi_league.core.canonical_matchup import (
    COLUMN_TYPES as MATCHUP_COLUMN_TYPES,
    create_matchup_table_sql,
    normalize_matchup_df,
)
from multi_league.core.canonical_player import (
    COLUMN_TYPES as PLAYER_FANTASY_COLUMN_TYPES,
    create_player_fantasy_table_sql,
)
from multi_league.core.canonical_draft import (
    COLUMN_TYPES as DRAFT_COLUMN_TYPES,
    create_draft_table_sql,
    normalize_draft_df,
)
from multi_league.core.canonical_transaction import (
    COLUMN_TYPES as TRANSACTION_COLUMN_TYPES,
    create_transaction_table_sql,
    normalize_transaction_df,
)
from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.canonical_settings import (
    _create_table_sql as _create_settings_table_sql,
    _col_type as _settings_col_type,
    get_schema_keys as _settings_schema_keys,
)
from multi_league.core.canonical_schedule import (
    COLUMN_TYPES as SCHEDULE_COLUMN_TYPES,
    create_schedule_table_sql,
    normalize_schedule_df,
)
from multi_league.core.keeper_config_schema import KEEPER_CONFIG_COLUMN_TYPES
from multi_league.core.franchise_identity_schema import (
    FRANCHISE_IDENTITY_AUDIT_COLUMN_TYPES,
    FRANCHISE_IDENTITY_REGISTRY_COLUMN_TYPES,
)
from multi_league.core.sql_utils import validate_db_name

CONFIG_TABLE_COLUMN_TYPES = {
    "franchise_identity_audit": FRANCHISE_IDENTITY_AUDIT_COLUMN_TYPES,
    "franchise_identity_registry": FRANCHISE_IDENTITY_REGISTRY_COLUMN_TYPES,
    "keeper_config": KEEPER_CONFIG_COLUMN_TYPES,
    "league_rules": {
        "db_name": "VARCHAR",
        "sacko_mode": "VARCHAR",
        "faab_budget": "DOUBLE",
        "league_format": "VARCHAR",
        "created_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    },
    "league_context": {
        "db_name": "VARCHAR",
        "platform": "VARCHAR",
        "league_id": "VARCHAR",
        "league_name": "VARCHAR",
        "league_ids_json": "VARCHAR",
        "manager_name_overrides_json": "VARCHAR",
        "franchise_merges_json": "VARCHAR",
        "keeper_rules_json": "VARCHAR",
        "league_rules_json": "VARCHAR",
        "standings_weights_json": "VARCHAR",
        "is_private": "BOOLEAN",
        "updated_at": "TIMESTAMP",
    },
    "manager_overrides": {
        "id": "INTEGER",
        "db_name": "VARCHAR",
        "operation": "VARCHAR",
        "from_name": "VARCHAR",
        "to_name": "VARCHAR",
        "applied_at": "TIMESTAMP",
    },
    "standings_config": {
        "db_name": "VARCHAR",
        "config_json": "VARCHAR",
        "updated_at": "TIMESTAMP",
    },
}


def _create_simple_table_sql(database_name: str, table_name: str, columns: dict[str, str]) -> str:
    cols_sql = ",\n            ".join(f'"{col}" {dtype}' for col, dtype in columns.items())
    return f"""
        CREATE TABLE IF NOT EXISTS "{database_name}".public.{table_name} (
            {cols_sql}
        )
    """


# Table name → DDL generator mapping
_TABLE_DDL = {
    "matchup": lambda db: create_matchup_table_sql(db),
    "player_fantasy": lambda db: create_player_fantasy_table_sql(db),
    "draft": lambda db: create_draft_table_sql(db),
    "transactions": lambda db: create_transaction_table_sql(db),
    "league_settings": lambda db: _create_settings_table_sql(db),
    "schedule": lambda db: create_schedule_table_sql(db),
}
_TABLE_DDL.update(
    {
        table: (lambda db, table=table, columns=columns: _create_simple_table_sql(db, table, columns))
        for table, columns in CONFIG_TABLE_COLUMN_TYPES.items()
    }
)

_TABLE_COLUMN_TYPES = {
    "matchup": MATCHUP_COLUMN_TYPES,
    "player_fantasy": PLAYER_FANTASY_COLUMN_TYPES,
    "draft": DRAFT_COLUMN_TYPES,
    "transactions": TRANSACTION_COLUMN_TYPES,
    "league_settings": {key: _settings_col_type(key) for key in _settings_schema_keys()},
    "schedule": SCHEDULE_COLUMN_TYPES,
}
_TABLE_COLUMN_TYPES.update(CONFIG_TABLE_COLUMN_TYPES)

# All tables now have canonical DDL
_TABLES_WITHOUT_DDL: set[str] = set()


def _expected_remote_tables() -> set[str]:
    """Return the canonical set of tables that are valid upload targets in
    ``___leagues``. Anything outside this set is a local-only scratch table
    that must not be uploaded under the strict DDL compliance policy.
    """
    # Lazy import so import ordering stays clean (aggregate_ddl transitively
    # imports canonical_matchup which imports this module).
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    return set(_TABLE_DDL) | set(AGGREGATE_TABLE_SPECS)


# Table name → normalizer mapping. Called automatically inside save_table.
# Normalizers clean types (e.g., "-" → NULL for DOUBLE), rename columns,
# and ensure only canonical columns are inserted.
_TABLE_NORMALIZERS = {
    "matchup": normalize_matchup_df,
    "player_fantasy": normalize_roster_df,
    "draft": normalize_draft_df,
    "transactions": normalize_transaction_df,
    "schedule": normalize_schedule_df,
}

_TYPE_ALIASES = {
    "INTEGER": {"INTEGER", "INT", "INT32", "SIGNED"},
    "BIGINT": {"BIGINT", "INT64", "LONG", "HUGEINT"},
    "DOUBLE": {"DOUBLE", "DOUBLE PRECISION", "FLOAT", "REAL", "DECIMAL", "NUMERIC"},
    "BOOLEAN": {"BOOLEAN", "BOOL", "LOGICAL"},
    "VARCHAR": {"VARCHAR", "TEXT", "STRING"},
}


def _canonicalize_duckdb_type(dtype: str) -> str:
    return str(dtype).upper().split("(")[0].strip()


def _types_compatible(actual: str, expected: str) -> bool:
    actual_norm = _canonicalize_duckdb_type(actual)
    expected_norm = _canonicalize_duckdb_type(expected)
    if actual_norm == expected_norm:
        return True
    return actual_norm in _TYPE_ALIASES.get(expected_norm, {expected_norm})


def _canonical_table_columns(table_name: str) -> list[str]:
    return list(_TABLE_COLUMN_TYPES[table_name].keys())


def _remote_canonical_ddl(table_name: str, database_name: str) -> str:
    ddl = _TABLE_DDL[table_name](database_name)
    return ddl.replace(f'"{database_name}".public.', "_md_target.public.")


def _cast_expression(column_name: str, target_type: str) -> str:
    quoted = f'"{column_name}"'
    text_expr = f"TRIM(CAST({quoted} AS VARCHAR))"
    nullable_text = f"NULLIF({text_expr}, '')"
    target_norm = _canonicalize_duckdb_type(target_type)

    if target_norm == "VARCHAR":
        return f"CAST({quoted} AS VARCHAR)"

    if target_norm == "BOOLEAN":
        return (
            "CASE "
            f"WHEN {quoted} IS NULL THEN NULL "
            f"WHEN LOWER({text_expr}) IN ('', 'null', 'none', 'nan') THEN NULL "
            f"WHEN LOWER({text_expr}) IN ('1', 'true', 't', 'yes', 'y') THEN TRUE "
            f"WHEN LOWER({text_expr}) IN ('0', 'false', 'f', 'no', 'n') THEN FALSE "
            f"ELSE TRY_CAST({nullable_text} AS BOOLEAN) "
            "END"
        )

    return f"TRY_CAST({nullable_text} AS {target_norm})"


def _infer_platform_from_df(df: pd.DataFrame) -> str | None:
    """Infer platform from explicit payload columns when callers omit it."""
    if _df_is_empty(df):
        return None

    if "platform" in df.columns:
        if _is_polars_df(df):
            platform_values = [
                str(v).strip().lower()
                for v in df.get_column("platform").drop_nulls().unique().to_list()
                if str(v).strip()
            ]
        else:
            platform_values = [
                str(v).strip().lower() for v in df["platform"].dropna().unique().tolist() if str(v).strip()
            ]
        if len(set(platform_values)) == 1:
            return platform_values[0]

    for id_col, platform_name in (
        ("sleeper_player_id", "sleeper"),
        ("espn_player_id", "espn"),
        ("yahoo_player_id", "yahoo"),
    ):
        if id_col not in df.columns:
            continue
        if _is_polars_df(df):
            values = [
                str(v).strip()
                for v in df.get_column(id_col).drop_nulls().to_list()
                if str(v).strip().lower() not in {"", "none", "nan"}
            ]
            if values:
                return platform_name
        else:
            values = df[id_col].astype(str).replace({"None": "", "nan": ""}).str.strip()
            if (values != "").any():
                return platform_name

    return None


def _infer_league_id_from_df(df) -> str | None:
    """Infer a stable league_id from a frame when all rows agree."""
    if _df_is_empty(df) or "league_id" not in df.columns:
        return None

    if _is_polars_df(df):
        league_values = [
            str(v).strip() for v in df.get_column("league_id").drop_nulls().unique().to_list() if str(v).strip()
        ]
    else:
        league_values = [str(v).strip() for v in df["league_id"].dropna().unique().tolist() if str(v).strip()]

    if len(set(league_values)) == 1:
        return league_values[0]
    return None


def _is_polars_df(df) -> bool:
    return pl is not None and isinstance(df, pl.DataFrame)


def _df_is_empty(df) -> bool:
    if df is None:
        return True
    if _is_polars_df(df):
        return df.is_empty()
    return df.empty


def _df_columns(df) -> list[str]:
    if df is None:
        return []
    return list(df.columns)


def _registerable_frame(df):
    """Prepare a frame for DuckDB registration.

    Handles Polars frames by casting Struct columns to string (avoids empty Struct errors).
    Handles Pandas frames by converting dict/list columns to JSON strings.
    """
    if _is_polars_df(df):
        import polars as pl

        # Cast all Struct columns to string to ensure safe ingestion into DuckDB.
        # DuckDB's Arrow interface rejects empty STRUCT types (often from {}).
        struct_cols = [c for c, t in df.schema.items() if isinstance(t, pl.Struct)]
        if struct_cols:
            df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in struct_cols])
        return df.to_arrow()

    # For pandas frames, convert any dict/list (object) columns to JSON strings
    # so DuckDB/Arrow doesn't attempt to register a nested STRUCT with no fields.
    try:
        import json
        import numpy as _np
    except Exception:
        json = None
        _np = None
    if json is not None and not _is_polars_df(df) and df is not None:
        for c in df.columns:
            # Only consider object dtype columns (potentially dict/list)
            if df[c].dtype == object:
                # If any non-null value is a dict or list, convert the whole column
                sample_has_complex = False
                for v in df[c].dropna().head(20):
                    if isinstance(v, (dict, list)):
                        sample_has_complex = True
                        break
                if sample_has_complex:
                    def _json_value_or_none(value):
                        """Serialize nested values without treating lists/dicts as numeric."""
                        if value is None or value is pd.NA:
                            return None
                        if _np is not None and isinstance(value, (float, _np.floating)) and _np.isnan(value):
                            return None
                        return json.dumps(value)

                    df[c] = df[c].apply(
                        _json_value_or_none
                    )

    return df


class LocalLeagueDB:
    """Local DuckDB file for a single league.

    Created with canonical DDL for all tables. Same types as Fly.
    No parquet, no pandas type inference, no schema drift.
    """

    def __init__(self, data_dir: str | Path, league_name: str):
        self.data_dir = Path(data_dir)
        self.league_name = league_name
        self.db_path = self.data_dir / f"{league_name}.duckdb"
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            try:
                self._conn = duckdb.connect(str(self.db_path))
            except duckdb.IOException:
                wal_path = self.db_path.with_suffix(".duckdb.wal")
                if wal_path.exists():
                    logger.warning(f"[LocalDB] Removing stale WAL: {wal_path}")
                    wal_path.unlink()
                    self._conn = duckdb.connect(str(self.db_path))
                else:
                    raise
            self._conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            logger.info(f"[LocalDB] Connected to {self.db_path}")
        return self._conn

    def _table_exists_in_public(self, table_name: str) -> bool:
        return (
            self.connect()
            .execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?",
                [table_name],
            )
            .fetchone()[0]
            > 0
        )

    def _public_column_types(self, table_name: str) -> dict[str, str]:
        return {
            str(name): str(dtype)
            for name, dtype, *_ in self.connect().execute(f"DESCRIBE public.{table_name}").fetchall()
        }

    def validate_table_schema(self, table_name: str) -> list[str]:
        """Return canonical schema mismatches for a table, if any."""
        expected_types = _TABLE_COLUMN_TYPES.get(table_name)
        if not expected_types:
            return []
        if not self._table_exists_in_public(table_name):
            return [f"public.{table_name} missing"]

        actual_types = self._public_column_types(table_name)
        mismatches: list[str] = []
        for column_name, expected_type in expected_types.items():
            actual_type = actual_types.get(column_name)
            if actual_type is None:
                mismatches.append(f"{table_name}.{column_name} missing")
            elif not _types_compatible(actual_type, expected_type):
                mismatches.append(f"{table_name}.{column_name} expected {expected_type} but found {actual_type}")
        unexpected_columns = sorted(set(actual_types) - set(expected_types))
        for column_name in unexpected_columns:
            mismatches.append(f"{table_name}.{column_name} unexpected")
        return mismatches

    def _migrate_canonical_table(self, table_name: str):
        expected_types = _TABLE_COLUMN_TYPES.get(table_name)
        if not expected_types or not self._table_exists_in_public(table_name):
            return

        conn = self.connect()
        actual_types = self._public_column_types(table_name)

        for column_name, expected_type in expected_types.items():
            if column_name in actual_types:
                continue
            conn.execute(f'ALTER TABLE public.{table_name} ADD COLUMN "{column_name}" {expected_type}')
            logger.info(
                "[LocalDB] Added missing canonical column public.%s.%s %s",
                table_name,
                column_name,
                expected_type,
            )

        actual_types = self._public_column_types(table_name)
        for column_name, expected_type in expected_types.items():
            actual_type = actual_types.get(column_name)
            if actual_type is None or _types_compatible(actual_type, expected_type):
                continue

            conn.execute(
                f"ALTER TABLE public.{table_name} "
                f'ALTER COLUMN "{column_name}" SET DATA TYPE {expected_type} '
                f"USING {_cast_expression(column_name, expected_type)}"
            )
            logger.info(
                "[LocalDB] Normalized public.%s.%s from %s -> %s",
                table_name,
                column_name,
                actual_type,
                expected_type,
            )

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def execute_sql(self, sql: str):
        """Run arbitrary SQL on local DuckDB."""
        return self.connect().execute(sql)

    def list_tables(self) -> list[str]:
        """List all tables in public schema."""
        return [
            r[0]
            for r in self.connect()
            .execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            .fetchall()
        ]

    def row_count(self, table_name: str) -> int:
        """Return number of rows in a table."""
        return self.connect().execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]

    def ensure_table(self, table_name: str):
        """Create table with canonical DDL if it doesn't exist."""
        conn = self.connect()
        if table_name in _TABLE_DDL:
            # Use canonical DDL — correct types from the start
            ddl = _TABLE_DDL[table_name]("local")
            # Replace "local".public with just public (local DuckDB has no catalog prefix)
            ddl = ddl.replace('"local".public.', "public.")
            conn.execute(ddl)
            self._migrate_canonical_table(table_name)
        # Tables without canonical DDL will be created on first save_table() call

    def _normalize_table_frame(
        self,
        table_name: str,
        df,
        *,
        platform: str | None = None,
        league_id: str | None = None,
        log_context: str = "save",
    ):
        """Normalize a frame through the table's canonical normalizer when available."""
        normalizer = _TABLE_NORMALIZERS.get(table_name)
        if normalizer is None or df is None or _df_is_empty(df):
            return df

        try:
            effective_platform = platform or _infer_platform_from_df(df)
            effective_league_id = league_id or _infer_league_id_from_df(df)

            if not effective_platform:
                effective_platform = "yahoo"
                logger.warning(
                    "[LocalDB] No explicit platform for %s %s; defaulting normalizer to yahoo",
                    table_name,
                    log_context,
                )

            if _is_polars_df(df):
                df = df.to_pandas()
            return normalizer(df, platform=effective_platform, league_id=effective_league_id)
        except Exception as e:
            logger.warning(f"[LocalDB] Normalizer for {table_name} {log_context} failed: {e} -- saving raw")
            return df

    def _insert_into_table(self, table_name: str, df) -> int:
        """Insert overlapping columns from a frame into an existing table."""
        conn = self.connect()
        table_cols = {
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
            ).fetchall()
        }
        df_cols = _df_columns(df)
        common = sorted(set(df_cols) & table_cols)

        if common:
            upload_df = df.select(common) if _is_polars_df(df) else df[common]
            conn.register("_upload_df", _registerable_frame(upload_df))
            try:
                col_list = ", ".join(f'"{c}"' for c in common)
                conn.execute(f"INSERT INTO public.{table_name} ({col_list}) SELECT {col_list} FROM _upload_df")
            finally:
                conn.unregister("_upload_df")

        return conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]

    def recanonicalize_table(
        self,
        table_name: str,
        *,
        platform: str | None = None,
        league_id: str | None = None,
    ) -> int:
        """Rewrite a canonical table in place through its canonical normalizer."""
        if table_name not in _TABLE_DDL:
            raise ValueError(f"Table {table_name} does not use canonical DDL")
        if table_name not in _TABLE_NORMALIZERS:
            raise ValueError(f"Table {table_name} does not have a canonical normalizer")

        self.ensure_table(table_name)
        if not self.table_exists(table_name):
            return 0

        df = self.read_table(table_name)
        if df.empty:
            return 0

        df = self._normalize_table_frame(
            table_name,
            df,
            platform=platform,
            league_id=league_id,
            log_context="recanonicalize",
        )
        conn = self.connect()
        conn.execute(f"DELETE FROM public.{table_name}")
        row_count = self._insert_into_table(table_name, df)
        logger.info("[LocalDB] Recanonicalized %s: %s rows", table_name, f"{row_count:,}")
        return row_count

    def save_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        year: int | None = None,
        platform: str | None = None,
        league_id: str | None = None,
    ):
        """Save DataFrame to local DuckDB table.

        Automatically normalizes data through the canonical normalizer for the table
        (cleans types, renames columns, drops non-canonical columns). Then INSERTs
        into the DDL-typed table. No bad data gets through.

        Args:
            table_name: Table name (matchup, player_fantasy, draft, etc.)
            df: DataFrame to save
            year: If provided, DELETE existing data for this year before INSERT
            platform: Platform hint for normalizer (yahoo/sleeper/espn)
            league_id: League ID for normalizer
        """
        conn = self.connect()

        # Normalize through canonical normalizer if available
        normalizer = _TABLE_NORMALIZERS.get(table_name)
        if normalizer and df is not None and not _df_is_empty(df):
            try:
                effective_platform = platform or _infer_platform_from_df(df)

                effective_league_id = league_id
                if not effective_league_id and "league_id" in df.columns:
                    if _is_polars_df(df):
                        league_values = [
                            str(v).strip()
                            for v in df.get_column("league_id").drop_nulls().unique().to_list()
                            if str(v).strip()
                        ]
                    else:
                        league_values = [
                            str(v).strip() for v in df["league_id"].dropna().unique().tolist() if str(v).strip()
                        ]
                    if len(set(league_values)) == 1:
                        effective_league_id = league_values[0]

                if not effective_platform:
                    effective_platform = "yahoo"
                    logger.warning(
                        "[LocalDB] No explicit platform for %s; defaulting normalizer to yahoo",
                        table_name,
                    )

                if _is_polars_df(df):
                    df = df.to_pandas()
                df = normalizer(df, platform=effective_platform, league_id=effective_league_id)
            except Exception as e:
                logger.warning(f"[LocalDB] Normalizer for {table_name} failed: {e} — saving raw")

        if table_name in _TABLE_DDL:
            # Canonical table — ensure exists, then INSERT
            self.ensure_table(table_name)

            if year is not None:
                conn.execute(f"DELETE FROM public.{table_name} WHERE year = {year}")

            # Fetch target table columns first to filter the source DataFrame.
            # This prevents DuckDB from attempting to convert unusable columns
            # (like nested dictionaries/structs) that aren't in the schema.
            table_cols = {
                r[0]
                for r in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
                ).fetchall()
            }

            # Canonical tables always have ``db_name VARCHAR NOT NULL`` as the
            # first column. If the caller's DataFrame doesn't carry db_name
            # (common for lightweight tables like league_settings where the
            # rows come straight from ``flatten_settings``), inject it from
            # ``self.league_name`` so the INSERT doesn't trip the NOT NULL
            # constraint.
            if "db_name" in table_cols:
                if _is_polars_df(df):
                    import polars as _pl  # noqa: PLC0415

                    if "db_name" not in df.columns:
                        df = df.with_columns(_pl.lit(self.league_name).alias("db_name"))
                    else:
                        db_name_expr = _pl.col("db_name").cast(_pl.Utf8, strict=False)
                        df = df.with_columns(
                            _pl.when(
                                db_name_expr.is_null()
                                | (db_name_expr.str.strip_chars() == "")
                            )
                            .then(_pl.lit(self.league_name))
                            .otherwise(db_name_expr)
                            .alias("db_name")
                        )
                else:
                    if "db_name" not in df.columns:
                        df = df.assign(db_name=self.league_name)
                    else:
                        df = df.copy()
                        df["db_name"] = df["db_name"].fillna(self.league_name)
                        # Empty strings are treated as missing too — the
                        # centralized schema will reject them at upload time
                        # via validate_table_schema, but we normalize here.
                        df.loc[df["db_name"].astype(str).str.strip() == "", "db_name"] = self.league_name

            df_cols = _df_columns(df)
            common = sorted(set(df_cols) & table_cols)

            if common:
                upload_df = df.select(common) if _is_polars_df(df) else df[common]
                conn.register("_upload_df", _registerable_frame(upload_df))
                col_list = ", ".join(f'"{c}"' for c in common)
                conn.execute(f"INSERT INTO public.{table_name} ({col_list}) SELECT {col_list} FROM _upload_df")
                conn.unregister("_upload_df")

        else:
            # No canonical DDL — create from DataFrame (legacy tables)
            if year is not None:
                # Try to delete year first if table exists
                try:
                    conn.execute(f"DELETE FROM public.{table_name} WHERE year = {year}")
                except Exception:
                    pass  # Table doesn't exist yet

            conn.register("_upload_df", _registerable_frame(df))
            try:
                # Check if table exists
                exists = (
                    conn.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables "
                        f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
                    ).fetchone()[0]
                    > 0
                )

                if exists:
                    # INSERT into existing
                    table_cols = {
                        r[0]
                        for r in conn.execute(
                            f"SELECT column_name FROM information_schema.columns "
                            f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
                        ).fetchall()
                    }
                    common = sorted(set(df_cols) & table_cols)
                    if common:
                        upload_df = df.select(common) if _is_polars_df(df) else df[common]
                        conn.register("_upload_df", _registerable_frame(upload_df))
                        col_list = ", ".join(f'"{c}"' for c in common)
                        conn.execute(f"INSERT INTO public.{table_name} ({col_list}) SELECT {col_list} FROM _upload_df")
                        conn.unregister("_upload_df")
                else:
                    conn.register("_upload_df", _registerable_frame(df))
                    conn.execute(f"CREATE TABLE public.{table_name} AS SELECT * FROM _upload_df")
                    conn.unregister("_upload_df")
            finally:
                pass

        row_count = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
        logger.info(f"[LocalDB] {table_name}: {row_count:,} rows (year={year})")

    def merge_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        dedup_keys: list[str],
        platform: str | None = None,
        league_id: str | None = None,
        already_normalized: bool = False,
    ):
        """Merge recovered rows into a local DuckDB table using dedup keys.

        Unlike ``save_table()``, this does not delete an entire season. It removes only
        target rows whose dedup keys match the incoming rows, then inserts the recovery
        payload. This is the write path recovery should use for surgical backfills.
        """
        if df is None or df.empty:
            return

        conn = self.connect()

        normalizer = _TABLE_NORMALIZERS.get(table_name)
        if normalizer and not already_normalized and df is not None and not df.empty:
            try:
                effective_platform = platform or _infer_platform_from_df(df)

                effective_league_id = league_id
                if not effective_league_id and "league_id" in df.columns:
                    league_values = [
                        str(v).strip() for v in df["league_id"].dropna().unique().tolist() if str(v).strip()
                    ]
                    if len(set(league_values)) == 1:
                        effective_league_id = league_values[0]

                if not effective_platform:
                    effective_platform = "yahoo"
                    logger.warning(
                        "[LocalDB] No explicit platform for %s merge; defaulting normalizer to yahoo",
                        table_name,
                    )

                df = normalizer(df, platform=effective_platform, league_id=effective_league_id)
            except Exception as e:
                logger.warning(f"[LocalDB] Normalizer for {table_name} merge failed: {e} -- saving raw")

        if table_name in _TABLE_DDL:
            self.ensure_table(table_name)

        # Inject db_name if the target table requires it (same logic as save_table)
        if "db_name" not in df.columns:
            df = df.assign(db_name=self.league_name)
        else:
            df = df.copy()
            df["db_name"] = df["db_name"].fillna(self.league_name)
            mask = df["db_name"].astype(str).str.strip() == ""
            if mask.any():
                df.loc[mask, "db_name"] = self.league_name

        conn.register("_merge_df", df)
        try:
            table_cols = {
                r[0]
                for r in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
                ).fetchall()
            }
            if not table_cols:
                raise ValueError(f"Cannot merge into missing table public.{table_name}")

            common = sorted(set(df.columns) & table_cols)
            if not common:
                logger.warning("[LocalDB] merge_table(%s) found no common columns", table_name)
                return

            valid_keys = [k for k in dedup_keys if k in common]
            if valid_keys:
                incoming = conn.execute("SELECT * FROM _merge_df").fetchdf()
                incoming = incoming.drop_duplicates(subset=valid_keys, keep="last")
                conn.unregister("_merge_df")
                conn.register("_merge_df", incoming)

                join_sql = " AND ".join(f't."{k}" IS NOT DISTINCT FROM s."{k}"' for k in valid_keys)
                conn.execute(f"DELETE FROM public.{table_name} t USING _merge_df s WHERE {join_sql}")

            col_list = ", ".join(f'"{c}"' for c in common)
            conn.execute(f"INSERT INTO public.{table_name} ({col_list}) SELECT {col_list} FROM _merge_df")
        finally:
            try:
                conn.unregister("_merge_df")
            except Exception:
                pass

        row_count = conn.execute(f"SELECT COUNT(*) FROM public.{table_name}").fetchone()[0]
        logger.info(
            "[LocalDB] %s merged: %s incoming rows on keys %s -> %s total rows",
            table_name,
            len(df),
            dedup_keys,
            f"{row_count:,}",
        )

    def read_table(self, table_name: str, year: int | None = None) -> pd.DataFrame:
        """Read table from local DuckDB."""
        conn = self.connect()
        where = f" WHERE year = {year}" if year else ""
        return conn.execute(f"SELECT * FROM public.{table_name}{where}").fetchdf()

    def table_exists(self, table_name: str) -> bool:
        return self._table_exists_in_public(table_name)

    def upload_to_fly(
        self,
        db_name: str,
        *,
        import_mode: str | None = None,
        platform: str | None = None,
        merge_source_ctx: Any | None = None,
        finalize_inventory: bool | None = None,
        finalize_merge_source: bool = True,
    ):
        """Upload this league's local data to Fly.io.

        The worker entrypoint is intentionally stable. Set
        FLY_PUBLISH_FORMAT=delta to publish a manifested Parquet delta bundle
        through /merge-league-delta. Quick imports always use the existing
        single /merge-fleet-partition lane: bounded seasons of facts with
        full-chain server rollups, never whole-league replacement or a legacy fallback.
        Configuration/identity tables initialize only where absent; saved
        user settings remain authoritative inside the fenced transaction.
        """
        import duckdb
        import json
        import shutil
        import tempfile
        import uuid

        from multi_league.core.targets.fly_target import FlyTarget

        conn = self.connect()
        try:
            conn.execute("CHECKPOINT")
        except Exception:
            pass

        def _env_bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        def finalize_inventory_and_merge_source(target: FlyTarget) -> None:
            inventory_default = publish_format != "delta"
            should_finalize_inventory = (
                _env_bool("FLY_FINALIZE_INVENTORY", inventory_default)
                if finalize_inventory is None
                else bool(finalize_inventory)
            )
            if should_finalize_inventory:
                inventory_start = time.perf_counter()
                print("[UPLOAD-FLY] finalizing league inventory")
                target.mark_league_imported(
                    db_name,
                    import_mode=import_mode or _infer_fly_import_mode(),
                    platform=platform or _infer_fly_platform(),
                )
                print(f"[UPLOAD-FLY] inventory finalized in {time.perf_counter() - inventory_start:.1f}s")
            else:
                reason = (
                    "FLY_FINALIZE_INVENTORY=0"
                    if os.environ.get("FLY_FINALIZE_INVENTORY") is not None
                    else f"default for FLY_PUBLISH_FORMAT={publish_format}"
                )
                print(f"[UPLOAD-FLY] inventory finalization skipped ({reason})")

            if finalize_merge_source:
                from multi_league.data_fetchers.shared.merge_source_copier import (
                    copy_merge_source_to_public,
                    maybe_copy_merge_source_to_public,
                )

                if merge_source_ctx is not None:
                    merge_stats = copy_merge_source_to_public(
                        merge_source_ctx,
                        db_name,
                        log_func=logger.info,
                    )
                else:
                    merge_stats = maybe_copy_merge_source_to_public(
                        data_dir=self.data_dir,
                        target_db_name=db_name,
                        log_func=logger.info,
                    )
                if merge_stats.get("status") in {"copied", "copied_multi"}:
                    logger.info("[UPLOAD-FLY] merge_source finalized: %s", merge_stats)
                    if merge_stats.get("single_year_import") != "true":
                        from multi_league.data_fetchers.shared.staging_reader import clear_staging_tables

                        clear_staging_tables(db_name=db_name, log_func=logger.info)

        def delta_fallback_allowed(exc: Exception) -> bool:
            if os.environ.get("FLY_DELTA_FALLBACK_TO_DUCKDB", "").strip().lower() not in {"1", "true", "yes", "on"}:
                return False
            message = str(exc).lower()
            safe_markers = (
                "404",
                "not found",
                "connection refused",
                "failed to establish a new connection",
                "name or service not known",
                "temporarily unavailable",
            )
            return any(marker in message for marker in safe_markers)

        resolved_mode = (import_mode or _infer_fly_import_mode() or "unknown").strip().lower()
        quick_publish = resolved_mode == "quick"
        publish_format = "delta" if quick_publish else os.environ.get("FLY_PUBLISH_FORMAT", "duckdb").strip().lower()
        if publish_format == "delta":
            from multi_league.core.delta_publish import build_delta_bundle

            snapshot_raw = os.environ.get("LEAGUE_IMPORT_BASE_GENERATION")
            if snapshot_raw is None:
                if quick_publish or _env_bool("REQUIRE_IMPORT_BASE_GENERATION", False):
                    raise RuntimeError(
                        "Delta import has no pre-fetch Fly publication generation; "
                        "refusing an unfenced league write"
                    )
                base_generation = None
            else:
                try:
                    base_generation = int(snapshot_raw)
                except ValueError as exc:
                    raise RuntimeError("Invalid pre-fetch Fly publication generation") from exc
                if base_generation < 0:
                    raise RuntimeError("Invalid pre-fetch Fly publication generation")

            bundle = None
            try:
                bundle_start = time.perf_counter()
                bundle = build_delta_bundle(
                    conn,
                    db_name=db_name,
                    import_mode=resolved_mode,
                    platform=platform or _infer_fly_platform(),
                    league_id=os.environ.get("LEAGUE_ID")
                    or os.environ.get("SLEEPER_LEAGUE_ID")
                    or os.environ.get("YAHOO_LEAGUE_ID")
                    or os.environ.get("ESPN_LEAGUE_ID"),
                    base_generation=base_generation,
                )
                manifest_path = self.data_dir / "delta_publish_manifest.json"
                manifest_path.write_text(
                    json.dumps(bundle.manifest, sort_keys=True, indent=2, default=str),
                    encoding="utf-8",
                )
                bundle_size_mb = bundle.path.stat().st_size / (1024 * 1024)
                print(
                    f"[UPLOAD-FLY] Delta bundle {bundle.bundle_id} "
                    f"({len(bundle.manifest.get('tables', []))} tables, {bundle_size_mb:.1f} MB) "
                    f"built in {time.perf_counter() - bundle_start:.1f}s"
                )

                target = FlyTarget()
                merge_start = time.perf_counter()
                if quick_publish:
                    result = target.merge_fleet_partition(
                        bundle.path, bundle_id=bundle.bundle_id, bundle_hash=bundle.bundle_hash,
                    )
                    if not isinstance(result, dict) or result.get("status") != "COMMITTED":
                        raise RuntimeError("Quick partition publication lacks a COMMITTED receipt")
                else:
                    result = target.merge_league_delta(
                        db_name, bundle.path, bundle_id=bundle.bundle_id, bundle_hash=bundle.bundle_hash,
                    )
                elapsed = time.perf_counter() - merge_start
                if isinstance(result, dict) and result.get("status") == "STALE_SKIPPED":
                    print(
                        "[UPLOAD-FLY] merge-league-delta skipped: newer committed state exists; "
                        "leaving Fly data unchanged"
                    )
                    logger.info("[UPLOAD-FLY] %s stale bundle skipped: %s", db_name, result)
                    return
                timing_bits = []
                if isinstance(result, dict):
                    if result.get("lock_wait_seconds") is not None:
                        timing_bits.append(f"server_lock_wait={float(result['lock_wait_seconds']):.1f}s")
                    if result.get("merge_seconds") is not None:
                        timing_bits.append(f"server_merge={float(result['merge_seconds']):.1f}s")
                    elif result.get("elapsed_seconds") is not None:
                        timing_bits.append(f"server_merge={float(result['elapsed_seconds']):.1f}s")
                suffix = f" ({', '.join(timing_bits)})" if timing_bits else ""
                endpoint = "merge-fleet-partition" if quick_publish else "merge-league-delta"
                print(f"[UPLOAD-FLY] {endpoint} completed in {elapsed:.1f}s{suffix}")
                finalize_inventory_and_merge_source(target)
                logger.info("[UPLOAD-FLY] %s: %s", db_name, result)
                return
            except Exception as exc:
                if not quick_publish and delta_fallback_allowed(exc):
                    logger.warning(
                        "[UPLOAD-FLY] Delta publish failed on a fallback-allowed endpoint/network error; "
                        "using legacy whole-DuckDB rollback path: %s",
                        exc,
                    )
                    print("[UPLOAD-FLY] Delta endpoint unavailable; falling back to legacy whole-DuckDB upload")
                else:
                    raise
            finally:
                if bundle is not None:
                    shutil.rmtree(bundle.path.parent, ignore_errors=True)

        expected_remote = _expected_remote_tables()

        # Get all local tables, filter to canonical set only
        all_local = [
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                "AND table_catalog = current_database()"
            ).fetchall()
        ]
        tables = [t for t in all_local if t in expected_remote]
        skipped = [t for t in all_local if t not in expected_remote]
        if skipped:
            logger.info("[UPLOAD-FLY] Skipping %d non-canonical tables: %s", len(skipped), ", ".join(sorted(skipped)))

        def qident(name: str) -> str:
            return '"' + str(name).replace('"', '""') + '"'

        def sql_literal(value: str | Path) -> str:
            return "'" + str(value).replace("'", "''") + "'"

        # This is an owned staging artifact, not a shared league-wide path.
        # A parallel same-name import must never unlink another writer's file.
        safe_db_name = "".join(
            ch if (ch.isascii() and ch.isalnum()) or ch == "_" else "_"
            for ch in str(db_name)
        )[:64] or "league"
        upload_path = Path(tempfile.gettempdir()) / f"{safe_db_name}_upload_{uuid.uuid4().hex}.duckdb"

        staging_start = time.perf_counter()
        staged_tables = 0
        staged_rows = 0
        upload_alias = "_upload"
        attached_upload = False
        try:
            conn.execute(f"ATTACH {sql_literal(upload_path)} AS {qident(upload_alias)}")
            attached_upload = True
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(upload_alias)}.public")
            for table in tables:
                table_ident = qident(table)
                try:
                    local_count = conn.execute(f"SELECT COUNT(*) FROM public.{table_ident}").fetchone()[0]
                except Exception:
                    continue
                if local_count == 0:
                    continue
                actual_cols = [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = ?
                        ORDER BY ordinal_position
                        """,
                        [table],
                    ).fetchall()
                ]
                if table in _TABLE_COLUMN_TYPES:
                    upload_cols = [col for col in _canonical_table_columns(table) if col in set(actual_cols)]
                else:
                    upload_cols = actual_cols
                if not upload_cols:
                    continue
                select_cols = ", ".join(qident(col) for col in upload_cols)
                conn.execute(
                    f"CREATE TABLE {qident(upload_alias)}.public.{table_ident} AS "
                    f"SELECT {select_cols} FROM public.{table_ident}"
                )
                staged_tables += 1
                staged_rows += local_count
                logger.info("[UPLOAD-FLY] Staged %s: %d rows", table, local_count)
        finally:
            if attached_upload:
                conn.execute(f"DETACH {qident(upload_alias)}")

        try:
            upload_conn = duckdb.connect(str(upload_path))
            try:
                upload_conn.execute("CHECKPOINT")
            finally:
                upload_conn.close()
            upload_size_mb = upload_path.stat().st_size / (1024 * 1024)
            print(
                f"[UPLOAD-FLY] Staged {staged_tables} tables / {staged_rows:,} rows "
                f"into {upload_path.name} ({upload_size_mb:.1f} MB) "
                f"in {time.perf_counter() - staging_start:.1f}s"
            )

            target = FlyTarget()
            merge_start = time.perf_counter()
            result = target.merge_league(db_name, upload_path)
            print(f"[UPLOAD-FLY] merge-league completed in {time.perf_counter() - merge_start:.1f}s")
            finalize_inventory_and_merge_source(target)
            logger.info("[UPLOAD-FLY] %s: %s", db_name, result)
        finally:
            upload_path.unlink(missing_ok=True)
