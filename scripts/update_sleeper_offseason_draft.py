#!/usr/bin/env python3
"""
Update a Sleeper offseason draft without re-fetching season tables.

This is for dynasty/keeper leagues that complete the next season's rookie or
annual draft before matchups, transactions, and weekly player data exist for
that season. The script fetches only the Sleeper draft endpoints needed for
that year, replaces that league/year in ___leagues.public.draft, then rebuilds
draft aggregates and homepage aggregates from existing Fly data.

Example:
  python scripts/update_sleeper_offseason_draft.py --db the_infirmary --dry-run
  python scripts/update_sleeper_offseason_draft.py --db the_infirmary
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from numbers import Integral, Real
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))


LEAGUES_DB = "___leagues"
SAFE_DRAFT_SQL_STEPS = [
    "backfill_draft_managers",
    "player_to_draft",
    "draft_manager_aggregates",
    "draft_cost_buckets",
    "draft_value_zscore",
    "draft_bench_insurance",
    "draft_starter_designation",
    "draft_failure_rates",
    "draft_bench_value_by_rank",
    "draft_pick_conveyances",
]
OPS_DRAFT_SQL_STEPS = [
    "backfill_draft_positions",
    "draft_age_zscore",
]
HOMEPAGE_TABLES = [
    "homepage_league_summary",
    "homepage_manager_rankings",
    "homepage_current_standings",
    "homepage_top_rivalries",
    "homepage_manager_profiles",
]
LOCAL_OFFSEASON_SOURCE_TABLES = [
    "draft",
    "player_fantasy",
    "matchup",
    "transactions",
    "league_settings",
    "league_context",
    "matchup_season",
]
OFFSEASON_PUBLISH_TABLES = [
    "draft",
    "draft_manager_season",
    "draft_manager_career",
    "draft_player_career",
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


def insert_dataframe_values(
    writer: Any,
    table_name: str,
    df: pd.DataFrame,
    columns: list[str],
    *,
    chunk_size: int,
) -> int:
    """Insert a canonical aggregate frame through Fly's write connection.

    The targeted updater used this helper without defining it, which made a
    successful draft fetch fail after the expensive homepage calculation.
    Local staged updates use the aggregate utility's registered-frame path;
    this keeps the direct Fly fallback complete for the standalone CLI.
    """
    if df.empty:
        return 0
    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    for column in columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[columns]
    column_sql = ", ".join(f'"{column.replace(chr(34), chr(34) * 2)}"' for column in columns)
    inserted = 0
    for start in range(0, len(prepared), chunk_size):
        rows = prepared.iloc[start : start + chunk_size].to_dict("records")
        values = ", ".join(
            "(" + ", ".join(sql_value(row.get(column)) for column in columns) + ")" for row in rows
        )
        writer.execute(f"INSERT INTO public.{table_name} ({column_sql}) VALUES {values}")
        inserted += len(rows)
    return inserted


def sleeper_api_json(path: str) -> Any:
    with urlopen(f"https://api.sleeper.app/v1/{path.lstrip('/')}", timeout=25) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_json_field(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


class FlyQueryResult:
    """Small DB-API-like wrapper around Fly's list-of-dicts query response."""

    def __init__(self, rows: list[dict[str, Any]] | None, *, write: bool = False):
        self._dict_rows = rows or []
        self._columns = list(self._dict_rows[0].keys()) if self._dict_rows else []
        if write and not self._dict_rows:
            self._columns = ["rows_affected"]
            self._tuples = [(0,)]
        else:
            self._tuples = [tuple(row.get(column) for column in self._columns) for row in self._dict_rows]
        self.description = [(column,) for column in self._columns]
        self._index = 0

    def fetchone(self):
        if self._index >= len(self._tuples):
            return None
        row = self._tuples[self._index]
        self._index += 1
        return row

    def fetchall(self):
        rows = self._tuples[self._index :]
        self._index = len(self._tuples)
        return rows

    def fetchdf(self) -> pd.DataFrame:
        return pd.DataFrame(self._dict_rows)


class FlyDuckDBConnection:
    """DuckDB-ish connection backed by Fly's /query and /query-rw endpoints."""

    _WRITE_RE = re.compile(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE|COPY|ATTACH|DETACH)\b", re.I)

    def __init__(self, database: str = LEAGUES_DB):
        from multi_league.core.fly_writer import FlyWriter
        from multi_league.core.readers.fly_reader import FlyReader

        self.database = database
        self.reader = FlyReader()
        self.writer = FlyWriter()

    def execute(self, sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> FlyQueryResult:
        statement = self._bind_params(sql, params)
        write = self._is_write(statement)
        if write:
            rows = self.writer.execute(statement, database=self.database)
            return FlyQueryResult(rows, write=True)
        rows = self.reader.query(statement, database=self.database)
        return FlyQueryResult(rows)

    def close(self) -> None:
        return None

    @classmethod
    def _is_write(cls, sql: str) -> bool:
        stripped = sql.strip().lstrip("(")
        if not stripped:
            return False
        return bool(cls._WRITE_RE.search(stripped))

    @staticmethod
    def _bind_params(sql: str, params: list[Any] | tuple[Any, ...] | None) -> str:
        if not params:
            return sql
        statement = sql
        for param in params:
            statement = statement.replace("?", sql_value(param), 1)
        return statement


@dataclass(frozen=True)
class OffseasonDraftPlan:
    db_name: str
    league_name: str
    sleeper_league_id: str
    draft_year: int
    completed_season: int | None
    manager_name_overrides: dict[str, str]


@dataclass(frozen=True)
class SleeperDraftSelection:
    draft_ids: tuple[str, ...]
    statuses: tuple[str, ...]
    pick_count: int


def fly_rows(conn: FlyDuckDBConnection, sql: str) -> list[dict[str, Any]]:
    return conn.execute(sql).fetchdf().to_dict("records")


def fly_scalar(conn: FlyDuckDBConnection, sql: str, default: Any = None) -> Any:
    row = conn.execute(sql).fetchone()
    if not row:
        return default
    return row[0]


def resolve_plan(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    draft_year: int | None = None,
    sleeper_league_id: str | None = None,
) -> OffseasonDraftPlan:
    safe_db = sql_literal(db_name)
    rows = fly_rows(
        conn,
        "SELECT db_name, platform, league_id, league_name, manager_name_overrides_json "
        "FROM public.league_context "
        f"WHERE db_name = {safe_db} "
        "LIMIT 1",
    )
    if not rows:
        raise SystemExit(f"No league_context row found for {db_name!r}")

    context = rows[0]
    platform = str(context.get("platform") or "").lower()
    if platform != "sleeper":
        raise SystemExit(f"{db_name!r} is not a Sleeper league (platform={platform!r})")

    completed_season = fly_scalar(
        conn,
        "SELECT MAX(TRY_CAST(year AS INT)) AS season " "FROM public.matchup " f"WHERE db_name = {safe_db}",
    )
    completed_int = int(completed_season) if completed_season is not None else None
    resolved_draft_year = int(draft_year or ((completed_int + 1) if completed_int else datetime.now(UTC).year))
    resolved_league_id = str(sleeper_league_id or context.get("league_id") or "").strip()
    if not resolved_league_id:
        raise SystemExit(f"No Sleeper league_id found for {db_name!r}; pass --league-id")

    manager_name_overrides = parse_json_field(context.get("manager_name_overrides_json"), {})
    if not isinstance(manager_name_overrides, dict):
        manager_name_overrides = {}

    return OffseasonDraftPlan(
        db_name=db_name,
        league_name=str(context.get("league_name") or db_name),
        sleeper_league_id=resolved_league_id,
        draft_year=resolved_draft_year,
        completed_season=completed_int,
        manager_name_overrides={str(k): str(v) for k, v in manager_name_overrides.items()},
    )


def select_sleeper_drafts(plan: OffseasonDraftPlan, *, require_complete: bool) -> SleeperDraftSelection:
    try:
        drafts = sleeper_api_json(f"league/{plan.sleeper_league_id}/drafts")
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Could not read Sleeper drafts for {plan.sleeper_league_id}: {exc}") from exc

    matches = [draft for draft in drafts if str(draft.get("season")) == str(plan.draft_year)]
    if require_complete:
        selected = [draft for draft in matches if str(draft.get("status") or "").lower() == "complete"]
    else:
        selected = matches

    if not selected:
        statuses = sorted({str(draft.get("status") or "unknown") for draft in matches}) if matches else []
        detail = f" statuses={statuses}" if statuses else ""
        raise SystemExit(
            f"No {'complete ' if require_complete else ''}Sleeper draft found for "
            f"{plan.league_name} season {plan.draft_year} on league_id {plan.sleeper_league_id}.{detail}"
        )

    draft_ids: list[str] = []
    statuses: list[str] = []
    pick_count = 0
    for draft in selected:
        draft_id = str(draft.get("draft_id") or "").strip()
        if not draft_id:
            continue
        picks = sleeper_api_json(f"draft/{draft_id}/picks")
        draft_ids.append(draft_id)
        statuses.append(str(draft.get("status") or "unknown"))
        pick_count += len(picks) if isinstance(picks, list) else 0

    return SleeperDraftSelection(tuple(draft_ids), tuple(statuses), pick_count)


def fetch_sleeper_draft_dataframe(plan: OffseasonDraftPlan, selection: SleeperDraftSelection) -> pd.DataFrame:
    from multi_league.core.canonical_draft import normalize_draft_df
    from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext
    from multi_league.data_fetchers.sleeper.sleeper_draft import SleeperDraftFetcher

    with tempfile.TemporaryDirectory(prefix=f"{plan.db_name}_offseason_draft_") as tmp:
        ctx = SleeperContext(
            league_id=plan.sleeper_league_id,
            league_name=plan.league_name,
            username="",
            start_year=plan.draft_year,
            end_year=plan.draft_year,
            data_directory=Path(tmp) / plan.db_name,
            league_ids={str(plan.draft_year): plan.sleeper_league_id},
            manager_name_overrides=plan.manager_name_overrides,
            database_name=plan.db_name,
            import_mode="offseason_draft",
        )
        raw = SleeperDraftFetcher(ctx).fetch_draft_for_year(plan.draft_year)

    if raw.empty:
        return raw

    raw = raw[raw["draft_id"].astype(str).isin(selection.draft_ids)].copy()
    raw["db_name"] = plan.db_name
    normalized = normalize_draft_df(raw, platform="sleeper", league_id=plan.sleeper_league_id)
    normalized["db_name"] = plan.db_name
    return normalized


def league_publish_generation(conn: FlyDuckDBConnection, db_name: str) -> int:
    """Read the no-rewind generation required by Fly's scoped publisher."""
    row = conn.execute(
        "SELECT COALESCE(MAX(generation), 0) "
        "FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {sql_literal(db_name)}"
    ).fetchone()
    return int(row[0] or 0) if row else 0


def replace_draft_year(
    conn: FlyDuckDBConnection,
    plan: OffseasonDraftPlan,
    draft_df: pd.DataFrame,
    *,
    chunk_size: int,
    dry_run: bool,
    base_generation: int | None = None,
) -> int:
    if draft_df.empty:
        raise SystemExit("Fetched draft dataframe is empty; refusing to delete existing draft rows")

    prepared = draft_df.copy()
    prepared["db_name"] = plan.db_name
    if "year" not in prepared.columns or not prepared["year"].eq(plan.draft_year).all():
        raise ValueError(f"Draft replacement must contain only {plan.draft_year} rows for {plan.db_name!r}")

    if dry_run:
        print(
            f"[dry-run] Would replace {LEAGUES_DB}.public.draft rows for "
            f"db_name={plan.db_name!r}, year={plan.draft_year}: {len(prepared)} rows"
        )
        return len(prepared)

    # The fleet-partition publisher is Fly's hardened active-season transport:
    # it deletes only (db_name, year) represented by this bundle, then inserts
    # the replacement rows transactionally. Do not issue raw /query-rw DML here.
    import duckdb

    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.targets.fly_target import FlyTarget

    _ = chunk_size  # Parquet bundle publishing has no client-side SQL chunks.
    generation = (base_generation if base_generation is not None
                  else league_publish_generation(conn, plan.db_name))
    with tempfile.TemporaryDirectory(prefix="offseason_draft_publish_") as temp_dir:
        stage = duckdb.connect(":memory:")
        try:
            stage.execute("CREATE SCHEMA public")
            stage.register("_offseason_draft", prepared)
            stage.execute("CREATE TABLE public.draft AS SELECT * FROM _offseason_draft")
            stage.unregister("_offseason_draft")
            bundle = build_fleet_partition_bundle(
                stage,
                active_year=plan.draft_year,
                league_generations={plan.db_name: generation},
                tables=["draft"],
                output_dir=temp_dir,
            )
        finally:
            stage.close()
        result = FlyTarget().merge_fleet_partition(
            bundle.path,
            bundle_id=bundle.bundle_id,
            bundle_hash=bundle.bundle_hash,
        )
        if str(result.get("status") or "").upper() != "COMMITTED":
            raise RuntimeError(f"Scoped draft publish did not commit for {plan.db_name!r}: {result}")
    return len(prepared)


def run_draft_sql_enrichments(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    include_ops_enrichments: bool,
    strict: bool,
    data_dir: str | None = None,
) -> dict[str, Any]:
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    results: dict[str, Any] = {}
    enricher = SQLEnrichments(db_name, conn=conn, data_dir=data_dir, quick=True)
    # A direct Fly connection already exposes ___ops. The complete update path
    # stages in a stateful local DuckDB and must attach the cached ops database
    # itself so the value-score model can retain its TEMP tables.
    if not data_dir:
        enricher._ops_attached = True
    roster_by_year, scoring_params = enricher.load_settings_from_db()
    if roster_by_year:
        enricher.roster_by_year = roster_by_year
        enricher._update_scoring_params(scoring_params)

    steps = list(SAFE_DRAFT_SQL_STEPS)
    if include_ops_enrichments:
        steps = [OPS_DRAFT_SQL_STEPS[0], *steps, OPS_DRAFT_SQL_STEPS[1]]

    for step in steps:
        func = getattr(enricher, step)
        logging.info("[draft-sql] %s", step)
        try:
            results[step] = func()
        except Exception as exc:
            if strict:
                raise
            logging.warning("[draft-sql] %s skipped after error: %s", step, exc)
            results[step] = f"skipped: {exc}"
    return results


def _create_local_table(local_conn: Any, table_name: str, df: pd.DataFrame) -> None:
    """Materialize one scoped Fly dataframe in the local public schema."""
    register_name = f"_offseason_{table_name}_source"
    local_conn.register(register_name, df)
    try:
        local_conn.execute(f'CREATE TABLE public."{table_name}" AS SELECT * FROM {register_name}')
    finally:
        local_conn.unregister(register_name)


def _restore_canonical_dtypes(df: pd.DataFrame, column_types: dict[str, str]) -> pd.DataFrame:
    """Keep all-null canonical columns typed when staging through pandas.

    Fly serializes a nullable VARCHAR column whose selected rows are all NULL
    as a pandas object column.  DuckDB otherwise infers that column as
    INTEGER when it is registered locally, which breaks the regular draft SQL
    that quite reasonably applies string functions to optional text fields.
    Restore the canonical schema before creating a stateful local table.
    """
    typed = df.copy()
    for column, duckdb_type in column_types.items():
        if column not in typed.columns:
            continue
        if duckdb_type == "VARCHAR":
            typed[column] = typed[column].astype("string")
        elif duckdb_type in {"INTEGER", "BIGINT"}:
            typed[column] = pd.to_numeric(typed[column], errors="coerce").astype("Int64")
        elif duckdb_type == "DOUBLE":
            typed[column] = pd.to_numeric(typed[column], errors="coerce").astype("Float64")
        elif duckdb_type == "BOOLEAN":
            values = typed[column]
            typed[column] = values.map(
                lambda value: (
                    pd.NA
                    if pd.isna(value)
                    else value
                    if isinstance(value, bool)
                    else str(value).strip().lower() in {"1", "true", "yes"}
                )
            ).astype("boolean")
    return typed


def _restore_canonical_draft_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    from multi_league.core.canonical_draft import COLUMN_TYPES

    return _restore_canonical_dtypes(df, COLUMN_TYPES)


def _restore_canonical_transaction_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    from multi_league.core.canonical_transaction import COLUMN_TYPES

    return _restore_canonical_dtypes(df, COLUMN_TYPES)


def _restore_canonical_player_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    from multi_league.core.canonical_player import COLUMN_TYPES

    typed = df.copy()
    # The Fly HTTP dataframe transport represents a zero-row result as a
    # zero-column frame.  A completed future-season draft still needs the
    # canonical player table so the regular draft SQL can distinguish "no
    # player weeks yet" from a missing input table.
    if typed.empty and len(typed.columns) == 0:
        typed = pd.DataFrame({column: pd.Series(dtype="object") for column in COLUMN_TYPES})
    return _restore_canonical_dtypes(typed, COLUMN_TYPES)


def _identity_token(value: Any) -> str | None:
    """Return a comparable visible identity token, or None for placeholders."""
    from multi_league.core.manager_identity import is_hidden_manager_guid

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    token = str(value).strip()
    return token if token and not is_hidden_manager_guid(token) else None


def _restore_historical_draft_franchise_ids(
    historical: pd.DataFrame,
    incoming: pd.DataFrame,
    manager_name_overrides: dict[str, str],
) -> pd.DataFrame:
    """Carry a known franchise through a masked-current-season platform ID.

    Yahoo can deliberately mask manager GUIDs in a current draft while the
    league already has a stable franchise registry.  A user-selected display
    alias is applied first, then we carry a franchise ID only when the prior
    history maps that alias to exactly one franchise.  Ambiguous identities are
    intentionally left unset so the publish guard rejects them rather than
    silently merging two people.
    """
    if incoming.empty or "manager" not in incoming.columns:
        return incoming

    result = incoming.copy()
    if "franchise_id" not in result.columns:
        result["franchise_id"] = None

    overrides = {
        key.casefold(): str(value).strip()
        for key, value in manager_name_overrides.items()
        if _identity_token(key) and _identity_token(value)
    }
    if overrides:
        result["manager"] = result["manager"].map(
            lambda value: overrides.get(str(value).strip().casefold(), value)
            if _identity_token(value)
            else value
        )

    if not {"manager", "franchise_id"}.issubset(historical.columns):
        return result

    manager_to_franchises: dict[str, set[str]] = {}
    for manager, franchise_id in historical[["manager", "franchise_id"]].itertuples(index=False, name=None):
        manager_token = _identity_token(manager)
        franchise_token = _identity_token(franchise_id)
        if manager_token and franchise_token:
            manager_to_franchises.setdefault(manager_token.casefold(), set()).add(franchise_token)

    unambiguous = {
        manager: next(iter(franchise_ids))
        for manager, franchise_ids in manager_to_franchises.items()
        if len(franchise_ids) == 1
    }
    restored = 0
    for index, manager in result["manager"].items():
        if _identity_token(result.at[index, "franchise_id"]):
            continue
        manager_token = _identity_token(manager)
        franchise_id = unambiguous.get(manager_token.casefold()) if manager_token else None
        if franchise_id:
            result.at[index, "franchise_id"] = franchise_id
            restored += 1
    if restored:
        logging.info("[offseason-draft] restored %s draft rows to established franchise identities", restored)
    return result


def _hydrate_local_offseason_draft_db(
    conn: FlyDuckDBConnection,
    plan: OffseasonDraftPlan,
    draft_df: pd.DataFrame,
) -> Any:
    """Build a stateful, one-league DuckDB stage without mutating Fly.

    The standard import path calculates draft grades locally because the score
    model creates TEMP tables.  Fly's HTTP query interface deliberately uses
    one connection per statement, so the same complete calculation cannot be
    correct there.  This stage reads only this league's existing source rows,
    replaces the selected draft year locally, and is published only after all
    enrichments and rollups have completed.
    """
    import duckdb

    local = duckdb.connect(":memory:")
    local.execute("CREATE SCHEMA public")
    source_frames: dict[str, pd.DataFrame] = {}
    for table_name in LOCAL_OFFSEASON_SOURCE_TABLES:
        where_clause = f"db_name = {sql_literal(plan.db_name)}"
        # The player-to-draft calculation is year-local.  Loading only the
        # target season keeps the offseason path as fast as a quick import,
        # while retaining the normal player-aware source shape (including an
        # empty future-season table before Week 1).
        if table_name == "player_fantasy":
            where_clause += f" AND year = {int(plan.draft_year)}"
        frame = conn.execute(
            f"SELECT * FROM public.{table_name} WHERE {where_clause}"
        ).fetchdf()
        if table_name == "player_fantasy":
            frame = _restore_canonical_player_dtypes(frame)
        source_frames[table_name] = frame

    existing_draft = source_frames["draft"]
    if existing_draft.empty:
        local.close()
        raise RuntimeError(
            f"Refusing a scoped draft update for {plan.db_name!r}: Fly has no existing draft schema/history to preserve"
        )

    incoming = draft_df.copy()
    incoming.columns = [str(column) for column in incoming.columns]
    incoming["db_name"] = plan.db_name
    incoming["year"] = int(plan.draft_year)
    all_draft_columns = list(dict.fromkeys([*existing_draft.columns.tolist(), *incoming.columns.tolist()]))
    existing_draft = existing_draft.reindex(columns=all_draft_columns)
    incoming = incoming.reindex(columns=all_draft_columns)
    preserved_draft = existing_draft[
        pd.to_numeric(existing_draft["year"], errors="coerce") != int(plan.draft_year)
    ].copy()
    incoming = _restore_historical_draft_franchise_ids(
        preserved_draft,
        incoming,
        plan.manager_name_overrides,
    )
    source_frames["draft"] = _restore_canonical_draft_dtypes(
        pd.concat([preserved_draft, incoming], ignore_index=True)
    )
    source_frames["transactions"] = _restore_canonical_transaction_dtypes(source_frames["transactions"])

    for table_name, frame in source_frames.items():
        if frame.empty:
            if table_name == "player_fantasy":
                # Normal imports create this canonical table even before the
                # first player week exists.  Do the same here so every draft
                # enrichment sees the real pipeline shape.
                _create_local_table(local, table_name, frame)
                continue
            if table_name in {"draft", "matchup", "league_settings", "league_context"}:
                local.close()
                raise RuntimeError(f"Required local offseason source table is empty: {table_name}")
            continue
        _create_local_table(local, table_name, frame)
    return local


def _require_local_publish_rows(local_conn: Any, table_name: str, db_name: str, draft_year: int) -> None:
    where_clause = f"db_name = {sql_literal(db_name)}"
    if table_name in {"draft", "draft_manager_season"}:
        where_clause += f" AND TRY_CAST(year AS INTEGER) = {int(draft_year)}"
    row_count = int(local_conn.execute(f"SELECT COUNT(*) FROM public.{table_name} WHERE {where_clause}").fetchone()[0] or 0)
    if row_count == 0:
        raise RuntimeError(
            f"Complete offseason update produced no {table_name} rows for {db_name!r}; refusing to replace a Fly scope"
        )


def publish_local_offseason_outputs(
    source_conn: FlyDuckDBConnection,
    local_conn: Any,
    plan: OffseasonDraftPlan,
    *,
    base_generation: int,
) -> dict[str, Any]:
    """Atomically publish only the selected year plus refreshed league rollups."""
    import duckdb

    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.targets.fly_target import FlyTarget

    for table_name in OFFSEASON_PUBLISH_TABLES:
        _require_local_publish_rows(local_conn, table_name, plan.db_name, plan.draft_year)

    with tempfile.TemporaryDirectory(prefix="offseason_draft_full_publish_") as temp_dir:
        stage = duckdb.connect(":memory:")
        try:
            stage.execute("CREATE SCHEMA public")
            for table_name in OFFSEASON_PUBLISH_TABLES:
                where_clause = f"db_name = {sql_literal(plan.db_name)}"
                if table_name in {"draft", "draft_manager_season"}:
                    where_clause += f" AND TRY_CAST(year AS INTEGER) = {int(plan.draft_year)}"
                frame = local_conn.execute(
                    f'SELECT * FROM public."{table_name}" WHERE {where_clause}'
                ).fetchdf()
                register_name = f"_publish_{table_name}"
                stage.register(register_name, frame)
                try:
                    stage.execute(f'CREATE TABLE public."{table_name}" AS SELECT * FROM {register_name}')
                finally:
                    stage.unregister(register_name)
            bundle = build_fleet_partition_bundle(
                stage,
                active_year=plan.draft_year,
                league_generations={plan.db_name: base_generation},
                tables=OFFSEASON_PUBLISH_TABLES,
                output_dir=temp_dir,
            )
        finally:
            stage.close()
        result = FlyTarget().merge_fleet_partition(
            bundle.path,
            bundle_id=bundle.bundle_id,
            bundle_hash=bundle.bundle_hash,
        )
    if str(result.get("status") or "").upper() != "COMMITTED":
        raise RuntimeError(f"Complete offseason publish did not commit for {plan.db_name!r}: {result}")
    return result


def run_complete_local_offseason_update(
    conn: FlyDuckDBConnection,
    plan: OffseasonDraftPlan,
    draft_df: pd.DataFrame,
    *,
    include_ops_enrichments: bool,
    strict_sql_enrichments: bool,
    skip_sql_enrichments: bool,
    skip_aggregates: bool,
    skip_homepage: bool,
    chunk_size: int,
    base_generation: int | None = None,
) -> None:
    """Run the same complete draft enrichment/rollup sequence as imports locally."""
    # The local stage copies live league inputs. Bind its publication version
    # before hydration so a newer weekly writer cannot be overwritten later.
    if base_generation is None:
        base_generation = league_publish_generation(conn, plan.db_name)
    local = _hydrate_local_offseason_draft_db(conn, plan, draft_df)
    try:
        if not skip_sql_enrichments:
            run_draft_sql_enrichments(
                local,
                plan.db_name,
                include_ops_enrichments=include_ops_enrichments,
                strict=strict_sql_enrichments,
                data_dir="offseason-local",
            )
        if not skip_aggregates:
            rebuild_draft_aggregates(local, plan.db_name)
        if not skip_homepage:
            logging.info(
                "[offseason-draft] Preserving homepage tables: this scoped refresh changes draft rows only; "
                "player, matchup, and transaction sources are unchanged."
            )

        # Fly sees one scoped fleet bundle only after every local computation
        # succeeds: no raw interim draft can replace the live year.
        publish_local_offseason_outputs(conn, local, plan, base_generation=base_generation)
    finally:
        local.close()


def rebuild_draft_aggregates(conn: FlyDuckDBConnection, db_name: str) -> dict[str, int]:
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career,
        aggregate_draft_manager_season,
        aggregate_draft_player_career,
        create_draft_manager_career_table,
        create_draft_manager_season_table,
        create_draft_player_career_table,
    )

    create_draft_manager_season_table(conn, db_name)
    create_draft_manager_career_table(conn, db_name)
    create_draft_player_career_table(conn, db_name)
    return {
        "draft_manager_season": aggregate_draft_manager_season(conn, db_name),
        "draft_manager_career": aggregate_draft_manager_career(conn, db_name),
        "draft_player_career": aggregate_draft_player_career(conn, db_name),
    }


def detect_league_platform(conn: FlyDuckDBConnection, db_name: str) -> str:
    value = fly_scalar(
        conn,
        "SELECT platform FROM public.league_settings "
        f"WHERE db_name = {sql_literal(db_name)} AND platform IS NOT NULL "
        "LIMIT 1",
    )
    platform = str(value or "").lower()
    return platform if platform in {"yahoo", "sleeper", "espn"} else "sleeper"


def replace_scoped_aggregate_dataframe(
    conn: FlyDuckDBConnection,
    db_name: str,
    table_name: str,
    df: pd.DataFrame,
    *,
    chunk_size: int,
) -> int:
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, ensure_aggregate_table

    from multi_league.transformations.aggregation.aggregation_utils import (
        get_active_catalog,
        replace_scoped_aggregate_table_from_dataframe,
    )

    ensure_aggregate_table(conn, get_active_catalog(), table_name)
    expected_columns = list(AGGREGATE_TABLE_SPECS[table_name].column_types.keys())
    data_columns = [column for column in expected_columns if column != "db_name"]

    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    if "db_name" in prepared.columns:
        prepared = prepared.drop(columns=["db_name"])
    extra = sorted(set(prepared.columns) - set(data_columns))
    if extra:
        raise ValueError(f"{table_name} aggregate dataframe has non-canonical columns: {extra}")
    for column in data_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared.insert(0, "db_name", db_name)
    prepared = prepared[expected_columns]

    if hasattr(conn, "register") and hasattr(conn, "unregister"):
        replace_scoped_aggregate_table_from_dataframe(conn, db_name, table_name, prepared)
        return len(prepared)

    conn.execute(f"DELETE FROM {LEAGUES_DB}.public.{table_name} WHERE db_name = {sql_literal(db_name)}")
    return insert_dataframe_values(conn, table_name, prepared, expected_columns, chunk_size=chunk_size)


def rebuild_homepage_tables(conn: FlyDuckDBConnection, db_name: str, *, chunk_size: int) -> dict[str, int]:
    from multi_league.transformations.aggregation.homepage_summary import compute_homepage_frames

    tables = compute_homepage_frames(conn, db_name, platform=detect_league_platform(conn, db_name))

    counts: dict[str, int] = {}
    for table_name in HOMEPAGE_TABLES:
        counts[table_name] = replace_scoped_aggregate_dataframe(
            conn,
            db_name,
            table_name,
            tables[table_name],
            chunk_size=chunk_size,
        )
    return counts


def print_draft_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("No draft rows fetched.")
        return
    summary = (
        df.groupby(["year", "draft_id", "draft_category"], dropna=False)
        .agg(
            picks=("pick", "count"),
            managers=("manager", "nunique"),
            first_pick=("player", "first"),
        )
        .reset_index()
    )
    print(summary.to_string(index=False))


def verify_draft_year(conn: FlyDuckDBConnection, plan: OffseasonDraftPlan) -> None:
    df = conn.execute(
        "SELECT year, draft_category, COUNT(*) AS picks, "
        "COUNT(DISTINCT manager) AS managers, COUNT(DISTINCT draft_id) AS drafts "
        "FROM public.draft "
        f"WHERE db_name = {sql_literal(plan.db_name)} AND year = {int(plan.draft_year)} "
        "GROUP BY year, draft_category "
        "ORDER BY year, draft_category"
    ).fetchdf()
    print("\nLive draft rows:")
    print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Update only a Sleeper offseason draft year in Fly.")
    parser.add_argument("--db", required=True, help="League db_name, e.g. the_infirmary")
    parser.add_argument("--draft-year", type=int, help="Draft season to fetch. Defaults to max matchup year + 1.")
    parser.add_argument("--league-id", help="Override Sleeper league_id/current season shell.")
    parser.add_argument("--allow-incomplete-draft", action="store_true", help="Include drafts that are not complete.")
    parser.add_argument(
        "--skip-sql-enrichments", action="store_true", help="Only replace draft rows; skip SQL enrichments."
    )
    parser.add_argument(
        "--include-ops-enrichments",
        action="store_true",
        help="Also run ops-backed draft enrichments such as position/age backfills.",
    )
    parser.add_argument(
        "--strict-sql-enrichments",
        action="store_true",
        help="Fail the update if any optional draft SQL enrichment errors.",
    )
    parser.add_argument("--skip-aggregates", action="store_true", help="Skip draft aggregate table rebuilds.")
    parser.add_argument("--skip-homepage", action="store_true", help="Skip homepage aggregate rebuilds.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and summarize, but do not write to Fly.")
    parser.add_argument("--chunk-size", type=int, default=50, help="Rows per Fly INSERT statement.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    load_dotenv(REPO_ROOT / ".env")
    conn = FlyDuckDBConnection()

    plan = resolve_plan(conn, args.db, draft_year=args.draft_year, sleeper_league_id=args.league_id)
    selection = select_sleeper_drafts(plan, require_complete=not args.allow_incomplete_draft)
    print(
        f"{plan.league_name} ({plan.db_name}): completed_season={plan.completed_season}, "
        f"draft_year={plan.draft_year}, league_id={plan.sleeper_league_id}, "
        f"draft_ids={list(selection.draft_ids)}, statuses={list(selection.statuses)}, "
        f"picks={selection.pick_count}"
    )

    draft_df = fetch_sleeper_draft_dataframe(plan, selection)
    if draft_df.empty:
        raise SystemExit("Sleeper returned no picks for the selected draft(s).")
    print("\nFetched draft rows:")
    print_draft_summary(draft_df)

    replaced = replace_draft_year(conn, plan, draft_df, chunk_size=args.chunk_size, dry_run=args.dry_run)
    if args.dry_run:
        print("[dry-run] Skipping SQL enrichments, aggregates, and homepage writes.")
        return

    print(f"\nReplaced draft rows: {replaced}")

    if not args.skip_sql_enrichments:
        results = run_draft_sql_enrichments(
            conn,
            plan.db_name,
            include_ops_enrichments=args.include_ops_enrichments,
            strict=args.strict_sql_enrichments,
        )
        print("\nDraft SQL enrichments:")
        for name, result in results.items():
            print(f"  {name}: {result}")

    if not args.skip_aggregates:
        aggregate_counts = rebuild_draft_aggregates(conn, plan.db_name)
        print("\nDraft aggregates:")
        for table_name, count in aggregate_counts.items():
            print(f"  {table_name}: {count}")

    if not args.skip_homepage:
        homepage_counts = rebuild_homepage_tables(conn, plan.db_name, chunk_size=args.chunk_size)
        print("\nHomepage aggregates:")
        for table_name, count in homepage_counts.items():
            print(f"  {table_name}: {count}")

    verify_draft_year(conn, plan)
    print("\nOffseason draft update complete.")


if __name__ == "__main__":
    main()
