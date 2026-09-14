"""Bridge local DuckDB <-> Fly staging tables for PHASE 1.7.

PHASE 1.7 operates on a local DuckDB connection, but the staging tables live
on Fly. This module pulls Fly staging rows into local before PHASE 1.7 runs,
and pushes the resulting ``staging.conformed_*`` rows back to Fly afterward so
the existing reader/merger pipeline sees them.
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

STAGING_DB = "___leagues"

# Maps local table name (used by schema_conform) -> Fly staging/conformed suffix.
# schema_conform uses:  staging.staging_matchup, staging.staging_player_fantasy, ...
# Fly stores them as:   staging_matchup, staging_player, staging_draft, staging_transactions.
LOCAL_TO_FLY_TABLE: dict[str, str] = {
    "matchup": "matchup",
    "player_fantasy": "player",
    "draft": "draft",
    "transactions": "transactions",
}


def _quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _existing_fly_tables(reader) -> set[str]:
    try:
        rows = reader.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'staging' AND table_catalog = '{STAGING_DB}'",
            database=STAGING_DB,
        )
    except RuntimeError as e:
        # Phase 1.7 is optional — a transient Fly failure (5xx after retries,
        # network blip, etc.) on the staging probe must degrade to "no staging
        # tables" rather than abort the whole import. Real staging data, when
        # present, is gated upstream by ctx.has_external_data; if that gate is
        # set and the probe fails, we accept silently missing the merge for
        # this run rather than crashing every league import.
        logger.warning("[bridge] Fly staging probe failed (%s) — treating as no staging", e)
        return set()
    return {r["table_name"] for r in rows}


def pull_staging_to_local(
    conn: duckdb.DuckDBPyConnection,
    db_name: str,
    reader=None,
) -> dict[str, int]:
    """Copy Fly ``___leagues.staging.staging_<table>`` rows for *db_name* into local
    ``staging.staging_<local_name>``.  Returns ``{local_name: row_count}``.

    No-op for any table that doesn't exist on Fly.  Local tables are dropped
    first so the function is idempotent and can be re-run safely.
    """
    if reader is None:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()

    existing = _existing_fly_tables(reader)
    db_lit = _quote_sql(db_name)
    counts: dict[str, int] = {}

    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")

    for local_name, fly_suffix in LOCAL_TO_FLY_TABLE.items():
        fly_table = f"staging_{fly_suffix}"
        if fly_table not in existing:
            counts[local_name] = 0
            continue

        df = reader.query_df(
            f"SELECT * FROM {STAGING_DB}.staging.{fly_table} WHERE db_name = {db_lit}",
            database=STAGING_DB,
        )
        if df.empty:
            counts[local_name] = 0
            continue

        local_table = f"staging.staging_{local_name}"
        conn.execute(f"DROP TABLE IF EXISTS {local_table}")
        conn.register("_pull_view", df)
        conn.execute(f"CREATE TABLE {local_table} AS SELECT * FROM _pull_view")
        conn.unregister("_pull_view")
        counts[local_name] = len(df)
        logger.info(
            "[bridge] pulled %d rows from Fly %s -> local %s",
            len(df),
            fly_table,
            local_table,
        )

    return counts


def push_conformed_to_fly(
    conn: duckdb.DuckDBPyConnection,
    db_name: str,
    writer=None,
    reader=None,
) -> dict[str, int]:
    """Copy local ``staging.conformed_<local_name>`` rows for *db_name* to Fly
    ``___leagues.staging.conformed_<fly_suffix>``.  Returns ``{local_name: row_count}``.

    Idempotent: DELETEs existing rows for *db_name* on Fly before INSERT.
    Creates the Fly table with ``VARCHAR`` columns if it doesn't exist.
    Adds new columns via ``ALTER … ADD COLUMN IF NOT EXISTS`` for schema evolution.
    No-op for any local table that doesn't exist or has zero rows for *db_name*.
    """
    if writer is None:
        from multi_league.core.fly_writer import FlyWriter

        writer = FlyWriter()
    if reader is None:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()

    counts: dict[str, int] = {}
    db_lit = _quote_sql(db_name)

    for local_name, fly_suffix in LOCAL_TO_FLY_TABLE.items():
        local_table = f"staging.conformed_{local_name}"

        # Check whether local conformed table exists and has rows for db_name.
        try:
            n = conn.execute(
                f"SELECT count(*) FROM {local_table} WHERE db_name = ?",
                [db_name],
            ).fetchone()[0]
        except Exception:
            n = 0

        if n == 0:
            counts[local_name] = 0
            continue

        # Read the local conformed rows.
        df = conn.execute(
            f"SELECT * FROM {local_table} WHERE db_name = ?",
            [db_name],
        ).df()

        if df.empty:
            counts[local_name] = 0
            continue

        # Coerce all to string for VARCHAR upload; preserve NULL.
        df_str = df.astype(object).where(df.notna(), None)
        for col in df_str.columns:
            df_str[col] = df_str[col].apply(lambda v: str(v) if v is not None else None)

        fly_table = f"conformed_{fly_suffix}"
        fully_qualified = f"{STAGING_DB}.staging.{fly_table}"

        # Ensure Fly table exists (all VARCHAR).
        cols_ddl = ", ".join(f'"{c}" VARCHAR' for c in df_str.columns)
        writer.execute(
            f"CREATE TABLE IF NOT EXISTS {fully_qualified} ({cols_ddl})",
            database=STAGING_DB,
        )

        # ALTER ADD COLUMN IF NOT EXISTS for any new columns (schema evolution).
        for c in df_str.columns:
            try:
                writer.execute(
                    f'ALTER TABLE {fully_qualified} ADD COLUMN IF NOT EXISTS "{c}" VARCHAR',
                    database=STAGING_DB,
                )
            except Exception as e:
                logger.warning("[bridge] ALTER ADD COLUMN failed for %s.%s: %s", fly_table, c, e)

        # Idempotent: clear this db_name's existing rows.
        writer.execute(
            f"DELETE FROM {fully_qualified} WHERE db_name = {db_lit}",
            database=STAGING_DB,
        )

        # Batch INSERT — keep batches <= 200 rows to stay under Fly POST body limit.
        BATCH = 200
        cols_quoted = ", ".join(f'"{c}"' for c in df_str.columns)
        for start in range(0, len(df_str), BATCH):
            batch = df_str.iloc[start : start + BATCH]
            value_rows = []
            for _, row in batch.iterrows():
                vals = []
                for v in row:
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        vals.append("NULL")
                    else:
                        s = str(v).replace("'", "''")
                        # Strip control characters (keep tab / newline as spaces).
                        s = "".join(ch if ord(ch) >= 32 or ch in "\t\n" else " " for ch in s)
                        vals.append(f"'{s}'")
                value_rows.append(f"({', '.join(vals)})")
            writer.execute(
                f"INSERT INTO {fully_qualified} ({cols_quoted}) VALUES {', '.join(value_rows)}",
                database=STAGING_DB,
            )

        counts[local_name] = len(df_str)
        logger.info(
            "[bridge] pushed %d rows from local %s -> Fly %s",
            len(df_str),
            local_table,
            fly_table,
        )

    return counts
