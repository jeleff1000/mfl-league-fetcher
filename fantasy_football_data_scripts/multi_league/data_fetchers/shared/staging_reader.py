"""Staging I/O against Fly-backed DuckDB.

Replaces the deleted streamlit_helpers.database module. Fly only allows
writes to ___leagues and ___ops, so all staging tables live in a single
shared schema at ``___leagues.staging.staging_*``, partitioned by a
``db_name`` column (same pattern as ``___leagues.public.*`` rows are
partitioned by ``league_id``). The frontend upload-staging route writes
to the same location.

Public surface:
    - read_staging_data(db_name, table_name=None, year=None)
    - write_staging_settings_to_files(db_name, settings_dir)
    - pre_fetch_staging_settings(ctx, log_func=print)
    - clear_staging_tables(db_name, log_func=print)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from multi_league.core.db_utils import sanitize_database_name

logger = logging.getLogger(__name__)

#: All staging tables live in this shared Fly database.
STAGING_DB = "___leagues"

STAGING_TABLES: tuple[str, ...] = (
    "staging_matchup",
    "staging_player",
    "staging_draft",
    "staging_transactions",
    "staging_schedule",
    "staging_settings",
)

TABLE_TO_TYPE: dict[str, str] = {
    "staging_matchup": "matchup",
    "staging_player": "player",
    "staging_draft": "draft",
    "staging_transactions": "transactions",
    "staging_schedule": "schedule",
}

TABLE_NAME_ALIASES: dict[str, str] = {
    "matchup": "staging_matchup",
    "matchups": "staging_matchup",
    "matchup_data": "staging_matchup",
    "player": "staging_player",
    "players": "staging_player",
    "player_data": "staging_player",
    "draft": "staging_draft",
    "draft_data": "staging_draft",
    "transactions": "staging_transactions",
    "transaction_data": "staging_transactions",
    "schedule": "staging_schedule",
    "schedule_data": "staging_schedule",
}

BUSY_RETRY_MARKERS: tuple[str, ...] = (
    "query service is busy",
    "ops_writing",
    "query failed (502)",
    "query failed (503)",
    "service unavailable",
    "empty response body",
    "starting",
)
BUSY_RETRY_ATTEMPTS = 8
BUSY_RETRY_BASE_SECONDS = 2.0
BUSY_RETRY_MAX_SECONDS = 30.0


def _get_reader():
    """Indirection for test patching."""
    from multi_league.core.db_reader import get_reader

    return get_reader()


def _get_writer():
    """Indirection for test patching."""
    from multi_league.core.fly_writer import FlyWriter

    return FlyWriter()


def _is_transient_busy_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in BUSY_RETRY_MARKERS)


def _with_busy_retry(description: str, func, *args, **kwargs):
    for attempt in range(1, BUSY_RETRY_ATTEMPTS + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if not _is_transient_busy_error(exc) or attempt == BUSY_RETRY_ATTEMPTS:
                raise
            sleep_seconds = min(
                BUSY_RETRY_MAX_SECONDS,
                BUSY_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
            )
            logger.warning(
                "Fly staging %s busy on attempt %s/%s; retrying in %.1fs: %s",
                description,
                attempt,
                BUSY_RETRY_ATTEMPTS,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def _reader_query(reader, sql: str, *, database: str):
    return _with_busy_retry("query", reader.query, sql, database=database)


def _reader_query_df(reader, sql: str, *, database: str):
    return _with_busy_retry("query_df", reader.query_df, sql, database=database)


def _writer_execute(writer, sql: str, *, database: str):
    return _with_busy_retry("write", writer.execute, sql, database=database)


def _existing_staging_tables(reader) -> set[str]:
    rows = _reader_query(
        reader,
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = 'staging' AND table_catalog = '{STAGING_DB}'",
        database=STAGING_DB,
    )
    return {row["table_name"] for row in rows}


def _pick_best_table(
    reader,
    db_lit: str,
    existing: set[str],
    raw_name: str,
) -> str | None:
    """Prefer ``conformed_<suffix>`` over ``staging_<suffix>`` when PHASE 1.7
    has already pushed conformed rows for this db_name.  Returns the Fly table
    name to read, or ``None`` if neither variant exists for this db_name.

    This is intentionally cheap: a single COUNT query against the conformed
    table, only when the conformed table is present in *existing*.
    """
    if raw_name.startswith("staging_"):
        suffix = raw_name[len("staging_") :]
        conformed = f"conformed_{suffix}"
        if conformed in existing:
            try:
                rows = _reader_query(
                    reader,
                    f"SELECT count(*) AS n FROM {STAGING_DB}.staging.{conformed} " f"WHERE db_name = {db_lit}",
                    database=STAGING_DB,
                )
                if rows and int(rows[0].get("n", 0)) > 0:
                    return conformed
            except Exception:
                pass  # fall through to raw table
    return raw_name if raw_name in existing else None


def _quote_sql(value: str) -> str:
    """Minimal single-quote escape for string literals in SQL."""
    return "'" + value.replace("'", "''") + "'"


def _parse_settings_json(raw: Any) -> Any:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw
        if isinstance(parsed, str):
            try:
                return json.loads(parsed)
            except json.JSONDecodeError:
                return parsed
        return parsed
    return raw


def read_staging_data(
    db_name: str,
    table_name: str | None = None,
    year: int | None = None,
) -> dict | pd.DataFrame | None:
    """Read staging tables from Fly.

    Args:
        db_name: Raw league name (will be sanitized).
        table_name: If given, return just that table's DataFrame (or None if missing).
                    Accepts canonical ("matchup") or legacy ("matchup_data") names.
        year: Optional year filter (requires table_name).

    Returns:
        - If table_name given: DataFrame, or None if table absent.
        - Otherwise: dict with 'settings' (list) and per-type DataFrames.
    """
    db = sanitize_database_name(db_name)
    reader = _get_reader()
    existing = _existing_staging_tables(reader)
    db_lit = _quote_sql(db)

    if table_name:
        staging_name = TABLE_NAME_ALIASES.get(table_name, f"staging_{table_name}")
        # Prefer conformed_* over staging_* when PHASE 1.7 has already run.
        chosen = _pick_best_table(reader, db_lit, existing, staging_name)
        if chosen is None:
            return None
        sql = f"SELECT * FROM {STAGING_DB}.staging.{chosen} WHERE db_name = {db_lit}"
        if year is not None:
            sql += f" AND year = {int(year)}"
        df = _reader_query_df(reader, sql, database=STAGING_DB)
        return df if len(df) > 0 else None

    result: dict[str, Any] = {}

    if "staging_settings" in existing:
        settings_df = _reader_query_df(
            reader,
            f"SELECT * FROM {STAGING_DB}.staging.staging_settings WHERE db_name = {db_lit}",
            database=STAGING_DB,
        )
        if not settings_df.empty:
            json_col = "raw_settings_json" if "raw_settings_json" in settings_df.columns else "settings_json"
            settings_list = []
            for _, row in settings_df.iterrows():
                raw = row.get(json_col)
                settings_list.append(
                    {
                        "year": int(row["year"]) if pd.notna(row["year"]) else None,
                        "settings_json": _parse_settings_json(raw),
                        "filename": row.get("filename", "") or "",
                    }
                )
            result["settings"] = settings_list

    for staging_name, data_type in TABLE_TO_TYPE.items():
        # Prefer conformed_* over staging_* when PHASE 1.7 has already run.
        chosen = _pick_best_table(reader, db_lit, existing, staging_name)
        if chosen is None:
            continue
        df = _reader_query_df(
            reader,
            f"SELECT * FROM {STAGING_DB}.staging.{chosen} WHERE db_name = {db_lit}",
            database=STAGING_DB,
        )
        if not df.empty:
            result[data_type] = df

    return result


def write_staging_settings_to_files(db_name: str, settings_dir: str) -> list[int]:
    """Read settings rows from staging and materialize them as JSON files.

    Filename format mirrors Yahoo's: league_settings_{year}_{league_key}.json.
    Adds `fetched_at` and ensures `year` is present in the JSON.
    """
    staging = read_staging_data(db_name)
    if not isinstance(staging, dict):
        return []
    settings_list = staging.get("settings", [])
    if not settings_list:
        return []

    out_dir = Path(settings_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[int] = []
    for entry in settings_list:
        year = entry.get("year")
        payload = entry.get("settings_json", {})
        if not year:
            continue

        if isinstance(payload, list):
            payload = payload[0] if payload and isinstance(payload[0], dict) else None
        if not isinstance(payload, dict):
            logger.warning(
                "Skipping settings for year %s: payload is %s",
                year,
                type(payload).__name__,
            )
            continue

        league_key = str(payload.get("league_key", "external")).replace(".", "_")
        filename = f"league_settings_{year}_{league_key}.json"
        filepath = out_dir / filename

        payload.setdefault("fetched_at", datetime.now().isoformat())
        payload["year"] = year

        filepath.write_text(json.dumps(payload, indent=2))
        written.append(int(year))

    return written


def pre_fetch_staging_settings(ctx, log_func=print) -> set[int]:
    """Phase-0.2 orchestrator helper: write staged settings to disk before platform fetch.

    Returns the set of years with staged settings (empty set if none).
    """
    from multi_league.core.runtime_mode import is_corpus_mode

    if is_corpus_mode():
        return set()

    settings_dir = Path(ctx.data_directory) / "league_settings"
    try:
        written = write_staging_settings_to_files(ctx.league_name, str(settings_dir))
    except Exception as exc:
        if _is_transient_busy_error(exc):
            log_func(f"[STAGING] Fly busy while reading staged settings; continuing without them: {exc}")
            return set()
        raise
    if written:
        log_func(
            f"[STAGING] Wrote {len(written)} staged settings file(s) for year(s) "
            f"{sorted(written)} to {settings_dir}"
        )
    return set(written)


def clear_staging_tables(db_name: str, log_func=print) -> None:
    """Delete this league's rows from the shared staging tables. Called after
    a full import merges the staging data. Leaves the tables in place for
    other leagues' concurrent imports. Skips any staging tables that don't
    yet exist (a league may have uploaded only a subset of types).

    Also clears any ``conformed_*`` tables written by PHASE 1.7 so stale
    conformed rows don't shadow fresh staging data on the next re-import.
    """
    db = sanitize_database_name(db_name)
    db_lit = _quote_sql(db)
    reader = _get_reader()
    existing = _existing_staging_tables(reader)
    writer = _get_writer()
    cleared = 0
    for table in STAGING_TABLES:
        if table not in existing:
            continue
        _writer_execute(
            writer,
            f"DELETE FROM {STAGING_DB}.staging.{table} WHERE db_name = {db_lit}",
            database=STAGING_DB,
        )
        cleared += 1

    # Also clear any conformed_* tables so stale PHASE 1.7 output doesn't
    # route the merger to old data on the next re-import.
    conformed_tables = [t for t in existing if t.startswith("conformed_")]
    for table in conformed_tables:
        try:
            _writer_execute(
                writer,
                f"DELETE FROM {STAGING_DB}.staging.{table} WHERE db_name = {db_lit}",
                database=STAGING_DB,
            )
            cleared += 1
            log_func(f"[STAGING] Cleared {table} for db_name={db}")
        except Exception as e:
            log_func(f"[STAGING] Failed to clear {table}: {e}")

    log_func(f"[STAGING] clear_staging_tables: cleared {cleared} table(s) for db_name={db}")
