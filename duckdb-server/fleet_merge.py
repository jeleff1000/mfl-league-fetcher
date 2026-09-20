"""Fleet partition merge: scoped weekly publishes into shared ___leagues tables.

FastAPI-free on purpose — main.py wraps these functions with auth, locking,
publish-state bookkeeping, and hard-exit timers, while tests and the offline
smoke harness drive them directly against local DuckDB connections.

Contract (see docs/runbooks/weekly-update-system-plan.md):
- Weekly tables merge with ``merge_mode="replace_scope"``.
- ``active_season`` tables delete only
  ``year = <scope year> AND db_name IN (SELECT DISTINCT db_name FROM parquet)``.
- ``league_rollup`` tables delete only
  ``db_name IN (SELECT DISTINCT db_name FROM parquet)``.
- The db_name set always comes from the uploaded parquet itself, never from
  the manifest, so leagues absent from the bundle can never be deleted.
- Weekly bundles may never carry ``replace_league`` — that mode stays on the
  per-league delta endpoint (the repair lane).
- Quick v3 imports declare one/two explicit fact years and initialize config
  only where that league has no saved rows; both use the same transaction.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from collections.abc import Callable

import duckdb

FLEET_SCHEMA_VERSION = "fleet-partition-v1"
FLEET_CAREER_SCHEMA_VERSION = "fleet-partition-v2"
FLEET_HOMEPAGE_SCHEMA_VERSION = "fleet-partition-v3"
FLEET_MANIFEST_VERSION = 1
FLEET_DB_SENTINEL = "___fleet"

CADENCE_ACTIVE_SEASON = "active_season"
CADENCE_LEAGUE_ROLLUP = "league_rollup"
ALLOWED_CADENCE_CLASSES = {CADENCE_ACTIVE_SEASON, CADENCE_LEAGUE_ROLLUP}
EMPTY_ACTIVE_PARTITION_TABLES = {"draft"}

# EVERY canonical table's required weekly cadence. A bundle that labels a
# year-bearing table league_rollup would widen its delete scope from one
# season to whole league histories, so unknown tables and mismatched classes
# are both rejected. Kept in exact two-way sync with the worker registry by
# tests/test_fleet_partition.py::test_expected_cadence_matches_worker_registry.
EXPECTED_CADENCE = {
    "all_play": CADENCE_ACTIVE_SEASON,
    "draft": CADENCE_ACTIVE_SEASON,
    "draft_manager_career": CADENCE_LEAGUE_ROLLUP,
    "draft_manager_season": CADENCE_ACTIVE_SEASON,
    "draft_player_career": CADENCE_LEAGUE_ROLLUP,
    "franchise_identity_audit": CADENCE_LEAGUE_ROLLUP,
    "franchise_identity_registry": CADENCE_LEAGUE_ROLLUP,
    "h2h_season": CADENCE_ACTIVE_SEASON,
    "homepage_current_standings": CADENCE_LEAGUE_ROLLUP,
    "homepage_league_summary": CADENCE_LEAGUE_ROLLUP,
    "homepage_manager_profiles": CADENCE_LEAGUE_ROLLUP,
    "homepage_manager_rankings": CADENCE_LEAGUE_ROLLUP,
    "homepage_top_rivalries": CADENCE_LEAGUE_ROLLUP,
    "keeper_config": CADENCE_ACTIVE_SEASON,
    "league_context": CADENCE_LEAGUE_ROLLUP,
    "league_rules": CADENCE_LEAGUE_ROLLUP,
    "league_settings": CADENCE_ACTIVE_SEASON,
    "manager_overrides": CADENCE_LEAGUE_ROLLUP,
    "matchup": CADENCE_ACTIVE_SEASON,
    "matchup_career": CADENCE_LEAGUE_ROLLUP,
    "matchup_h2h_career": CADENCE_LEAGUE_ROLLUP,
    "matchup_h2h_season": CADENCE_ACTIVE_SEASON,
    "matchup_season": CADENCE_ACTIVE_SEASON,
    "player_fantasy": CADENCE_ACTIVE_SEASON,
    "player_fantasy_career": CADENCE_LEAGUE_ROLLUP,
    "player_fantasy_career_all": CADENCE_LEAGUE_ROLLUP,
    "player_fantasy_season": CADENCE_ACTIVE_SEASON,
    "player_fantasy_season_all": CADENCE_ACTIVE_SEASON,
    "schedule": CADENCE_ACTIVE_SEASON,
    "schedule_swap": CADENCE_ACTIVE_SEASON,
    "schedule_swap_season": CADENCE_ACTIVE_SEASON,
    "standings_by_year": CADENCE_ACTIVE_SEASON,
    "standings_config": CADENCE_LEAGUE_ROLLUP,
    "transaction_manager_career": CADENCE_LEAGUE_ROLLUP,
    "transaction_manager_season": CADENCE_ACTIVE_SEASON,
    "transaction_player_career": CADENCE_LEAGUE_ROLLUP,
    "transaction_report_card": CADENCE_ACTIVE_SEASON,
    "transactions": CADENCE_ACTIVE_SEASON,
}


class FleetValidationError(Exception):
    pass


class FleetGenerationConflict(Exception):
    """A batched league was republished (repair lane) after this bundle was built."""


def _qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def validate_fleet_manifest_shape(
    manifest: dict,
    *,
    allowed_tables: set[str],
    identity_keys: dict[str, tuple[str, ...]],
    expected_bundle_id: str | None = None,
    expected_bundle_hash: str | None = None,
) -> tuple[set[str], dict[str, dict]]:
    """Structural manifest validation. Returns (allowed archive paths, table entries)."""
    if not isinstance(manifest, dict):
        raise FleetValidationError("Manifest must be an object")
    if manifest.get("manifest_version") != FLEET_MANIFEST_VERSION:
        raise FleetValidationError(f"Unsupported manifest_version: {manifest.get('manifest_version')!r}")
    if manifest.get("schema_version") not in {FLEET_SCHEMA_VERSION, FLEET_CAREER_SCHEMA_VERSION, FLEET_HOMEPAGE_SCHEMA_VERSION}:
        raise FleetValidationError(f"Unsupported schema_version: {manifest.get('schema_version')!r}")
    if manifest.get("db_name") != FLEET_DB_SENTINEL:
        raise FleetValidationError(f"Fleet manifest db_name must be {FLEET_DB_SENTINEL!r}")
    quick = manifest.get("mode") == "quick"
    if manifest.get("mode") not in {"weekly", "quick"}:
        raise FleetValidationError(f"Unsupported fleet mode: {manifest.get('mode')!r}")
    active_year = manifest.get("active_year")
    if not isinstance(active_year, int):
        raise FleetValidationError("Fleet manifest missing integer active_year")
    quick_years = manifest.get("quick_years")
    repair_missing_season_rollups = manifest.get("repair_missing_season_rollups", False)
    if not isinstance(repair_missing_season_rollups, bool):
        raise FleetValidationError("repair_missing_season_rollups must be boolean")
    if repair_missing_season_rollups and (
        manifest.get("schema_version") != FLEET_CAREER_SCHEMA_VERSION or quick
    ):
        raise FleetValidationError(
            "Missing season repair requires the weekly v2 career publication contract"
        )
    if quick:
        if (manifest.get("schema_version") != FLEET_HOMEPAGE_SCHEMA_VERSION
                or not isinstance(quick_years, list) or not 1 <= len(quick_years) <= 2
                or any(type(year) is not int or not 1900 <= year <= 2200 for year in quick_years)
                or quick_years != sorted(set(quick_years)) or active_year != max(quick_years)):
            raise FleetValidationError("Quick publication requires v3 and one or two explicit integer years")
    elif quick_years is not None:
        raise FleetValidationError("Weekly publication cannot declare quick_years")
    if expected_bundle_id and manifest.get("bundle_id") != expected_bundle_id:
        raise FleetValidationError("bundle_id header does not match manifest")
    if expected_bundle_hash and manifest.get("bundle_hash") != expected_bundle_hash:
        raise FleetValidationError("bundle_hash header does not match manifest")
    if not manifest.get("bundle_id") or not manifest.get("bundle_hash"):
        raise FleetValidationError("Fleet manifest missing bundle_id or bundle_hash")

    db_names = manifest.get("db_names")
    if not isinstance(db_names, list) or not db_names:
        raise FleetValidationError("Fleet manifest missing db_names list")
    if quick and len(db_names) != 1:
        raise FleetValidationError("Quick publication requires exactly one league")
    generations = manifest.get("league_generations")
    if not isinstance(generations, dict):
        raise FleetValidationError(
            "Fleet manifest missing league_generations (G14 no-rewind requires "
            "the generation each league was built from)"
        )
    if set(generations) != set(db_names):
        raise FleetValidationError("league_generations keys must exactly match db_names")
    for name, gen in generations.items():
        if not isinstance(gen, int) or gen < 0:
            raise FleetValidationError(f"Invalid generation for {name}: {gen!r}")

    tables = manifest.get("tables")
    if not isinstance(tables, list) or not tables:
        raise FleetValidationError("Fleet manifest must include at least one table")
    if not isinstance(manifest.get("omitted_tables"), list):
        raise FleetValidationError("Fleet manifest must include omitted_tables")

    allowed_paths: set[str] = {"manifest.json"}
    table_entries: dict[str, dict] = {}
    seen_tables: set[str] = set()

    for entry in tables:
        if not isinstance(entry, dict):
            raise FleetValidationError("Each table entry must be an object")
        table = str(entry.get("table") or "")
        if table not in allowed_tables:
            raise FleetValidationError(f"Unsupported canonical table: {table!r}")
        if table in seen_tables:
            raise FleetValidationError(f"Duplicate table entry: {table}")
        seen_tables.add(table)
        if manifest.get("schema_version") in {FLEET_CAREER_SCHEMA_VERSION, FLEET_HOMEPAGE_SCHEMA_VERSION}:
            from multi_league.transformations.aggregation.aggregation_utils import CAREER_ROLLUP_TABLES

            if table in CAREER_ROLLUP_TABLES:
                raise FleetValidationError(f"{table} must be recomputed on the full persisted chain, not uploaded")
        if manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION:
            from multi_league.transformations.aggregation.aggregation_utils import HOMEPAGE_ROLLUP_TABLES

            if table in HOMEPAGE_ROLLUP_TABLES:
                raise FleetValidationError(f"{table} must be recomputed on the full persisted chain, not uploaded")

        expected_path = f"tables/{table}.parquet"
        if entry.get("path") != expected_path:
            raise FleetValidationError(f"Invalid path for {table}: {entry.get('path')!r}")
        if entry.get("format") != "parquet":
            raise FleetValidationError(f"Unsupported table format for {table}: {entry.get('format')!r}")
        from multi_league.core.local_db import CONFIG_TABLE_COLUMN_TYPES

        initialize_config = quick and table in CONFIG_TABLE_COLUMN_TYPES
        required_mode = "initialize_missing" if initialize_config else "replace_scope"
        if entry.get("merge_mode") != required_mode:
            raise FleetValidationError(
                f"Unsupported merge_mode for {table}: {entry.get('merge_mode')!r} "
                "(weekly fleet bundles must use replace_scope)"
            )

        cadence = entry.get("cadence_class")
        if cadence not in ALLOWED_CADENCE_CLASSES:
            raise FleetValidationError(f"Unsupported cadence_class for {table}: {cadence!r}")
        expected_cadence = EXPECTED_CADENCE.get(table)
        if expected_cadence is None:
            raise FleetValidationError(
                f"{table} has no server-side cadence policy; update fleet_merge.EXPECTED_CADENCE"
            )
        if cadence != expected_cadence:
            raise FleetValidationError(
                f"{table} must publish as {expected_cadence}, manifest declares {cadence}"
            )

        if quick and not initialize_config and cadence != CADENCE_ACTIVE_SEASON:
            raise FleetValidationError(f"Quick publication cannot replace whole-league table {table}")
        scope = entry.get("scope")
        if not isinstance(scope, dict):
            raise FleetValidationError(f"Missing scope for {table}")
        if initialize_config:
            if scope:
                raise FleetValidationError(f"{table} initialization must declare empty scope")
        elif cadence == CADENCE_ACTIVE_SEASON:
            if quick:
                years = scope.get("years")
                if (set(scope) != {"years"} or not isinstance(years, list) or not years
                        or any(type(year) is not int for year in years)
                        or years != sorted(set(years)) or not set(years).issubset(quick_years)):
                    raise FleetValidationError(f"{table} has invalid quick year scope")
            elif scope != {"year": active_year}:
                raise FleetValidationError(
                    f"{table} scope year {scope.get('year')!r} does not match manifest active_year {active_year}"
                )
        elif scope:
            raise FleetValidationError(f"{table} is {cadence} and must declare an empty scope")

        empty_reason = entry.get("empty_reason")
        is_explicit_empty = empty_reason == "explicit_empty_active_partition"
        if empty_reason not in {None, "explicit_empty_active_partition"}:
            raise FleetValidationError(f"Unsupported empty_reason for {table}: {empty_reason!r}")
        if is_explicit_empty:
            if (table not in EMPTY_ACTIVE_PARTITION_TABLES
                    or quick or len(db_names) != 1 or cadence != CADENCE_ACTIVE_SEASON
                    or scope != {"year": active_year}):
                raise FleetValidationError(
                    f"Explicit empty {table} must be one weekly league's exact active-year partition"
                )
            if entry.get("row_count") != 0 or entry.get("db_name_count") != 0:
                raise FleetValidationError(f"Explicit empty {table} must declare zero rows and db_names")
        else:
            if not isinstance(entry.get("row_count"), int) or entry["row_count"] <= 0:
                raise FleetValidationError(f"Invalid row_count for {table} (empty tables must be omitted)")
            if not isinstance(entry.get("db_name_count"), int) or entry["db_name_count"] <= 0:
                raise FleetValidationError(f"Invalid db_name_count for {table}")
        if not entry.get("db_names_hash"):
            raise FleetValidationError(f"Missing db_names_hash for {table}")

        declared_columns = entry.get("columns")
        if not isinstance(declared_columns, list) or not declared_columns:
            raise FleetValidationError(f"Missing declared columns for {table}")
        declared_names = [str(col.get("name") or "") for col in declared_columns if isinstance(col, dict)]
        if len(declared_names) != len(declared_columns) or len(set(declared_names)) != len(declared_names):
            raise FleetValidationError(f"Invalid declared columns for {table}")
        if "db_name" not in declared_names:
            raise FleetValidationError(f"{table} is missing required db_name partition column")
        server_generated_columns = entry.get("server_generated_columns") or []
        if not isinstance(server_generated_columns, list) or len(set(server_generated_columns)) != len(server_generated_columns):
            raise FleetValidationError(f"Invalid server_generated_columns for {table}")
        declared_types = {str(col["name"]): str(col["type"]).upper() for col in declared_columns}
        for column in server_generated_columns:
            if column != "last_updated" or declared_types.get(column) != "TIMESTAMP":
                raise FleetValidationError(
                    f"Unsupported server-generated column {table}.{column}; only TIMESTAMP last_updated is allowed"
                )
        if cadence == CADENCE_ACTIVE_SEASON and "year" not in declared_names:
            raise FleetValidationError(f"{table} is active_season but declares no year column")

        keys = list(entry.get("primary_keys") or entry.get("identity_keys") or ())
        if not keys:
            keys = [key for key in identity_keys.get(table, ()) if key in declared_names]
        if not keys:
            # Every canonical table has identity keys; without them the
            # null/duplicate defenses downstream would silently no-op.
            raise FleetValidationError(f"{table} declares no identity keys")
        entry["_identity_keys"] = keys

        allowed_paths.add(expected_path)
        table_entries[table] = entry

    if quick:
        payload_years = {year for entry in table_entries.values()
                         for year in entry["scope"].get("years", [])}
        if payload_years != set(quick_years):
            raise FleetValidationError("Quick years must exactly match the payload year scopes")
    return allowed_paths, table_entries


def validate_fleet_parquet_tables(
    manifest: dict,
    table_entries: dict[str, dict],
    extract_dir: Path,
    *,
    sha256_file: Callable[[Path], str],
) -> None:
    """Content validation: hashes, schema, row counts, identity, scope bounds."""
    active_year = int(manifest["active_year"])
    conn = duckdb.connect(":memory:")
    try:
        for table, entry in sorted(table_entries.items()):
            parquet_path = extract_dir / entry["path"]
            if not parquet_path.exists():
                raise FleetValidationError(f"Missing Parquet file for {table}")
            if sha256_file(parquet_path) != entry.get("sha256"):
                raise FleetValidationError(f"Parquet sha256 mismatch for {table}")

            parquet_ref = f"read_parquet({_sql_literal(parquet_path)})"
            schema_rows = conn.execute(f"DESCRIBE SELECT * FROM {parquet_ref}").fetchall()
            actual_cols = [row[0] for row in schema_rows]
            declared_cols = [col["name"] for col in entry["columns"]]
            if actual_cols != declared_cols:
                raise FleetValidationError(
                    f"Parquet schema mismatch for {table}: actual {actual_cols!r}, manifest {declared_cols!r}"
                )

            keys = entry.get("_identity_keys") or []
            missing_identity = [key for key in keys if key not in actual_cols]
            if missing_identity:
                raise FleetValidationError(f"{table} is missing identity columns: {', '.join(missing_identity)}")

            years = entry["scope"].get("years", [active_year])
            active_scoped = (
                entry["cadence_class"] == CADENCE_ACTIVE_SEASON
                and entry["merge_mode"] != "initialize_missing"
            )
            null_exprs = [
                f"SUM(CASE WHEN {_qident(key)} IS NULL THEN 1 ELSE 0 END)::BIGINT "
                f"AS {_qident(f'null_{index}')}"
                for index, key in enumerate(keys)
            ]
            out_of_scope_expr = (
                "SUM(CASE WHEN year IS NULL OR year NOT IN "
                f"({','.join(str(int(year)) for year in years)}) THEN 1 ELSE 0 END)::BIGINT"
                if active_scoped else "0::BIGINT"
            )
            stats_sql = ", ".join([
                "COUNT(*)::BIGINT AS row_count",
                "SUM(CASE WHEN db_name IS NULL OR TRIM(CAST(db_name AS VARCHAR)) = '' "
                "THEN 1 ELSE 0 END)::BIGINT AS blank_db",
                f"{out_of_scope_expr} AS out_of_scope",
                *null_exprs,
            ])
            year_rows_sql = (
                "SELECT DISTINCT year FROM rows"
                if "year" in actual_cols else "SELECT NULL::INTEGER AS year WHERE FALSE"
            )
            result = conn.execute(f"""
                WITH rows AS MATERIALIZED (SELECT * FROM {parquet_ref}),
                dbs AS MATERIALIZED (
                    SELECT DISTINCT CAST(db_name AS VARCHAR) AS db FROM rows
                ),
                years AS MATERIALIZED (
                    {year_rows_sql}
                ),
                stats AS (SELECT {stats_sql} FROM rows)
                SELECT stats.*,
                       (SELECT COUNT(*)::BIGINT FROM dbs) AS db_count,
                       (SELECT COALESCE(sha256(string_agg(db, chr(31) ORDER BY db)), sha256('empty'))
                          FROM dbs) AS db_hash,
                       (SELECT list(db ORDER BY db) FROM dbs) AS db_values,
                       (SELECT list(year ORDER BY year) FROM years) AS year_values
                FROM stats
            """)
            summary = result.fetchone()
            content = dict(zip((item[0] for item in result.description), summary))

            actual_count = int(content["row_count"] or 0)
            if actual_count != entry["row_count"]:
                raise FleetValidationError(
                    f"Parquet row_count mismatch for {table}: actual {actual_count}, manifest {entry['row_count']}"
                )

            blank_db = int(content["blank_db"] or 0)
            if blank_db:
                raise FleetValidationError(f"{table} contains {blank_db} rows with blank db_name")

            actual_db_count = int(content["db_count"] or 0)
            actual_db_hash = str(content["db_hash"])
            if actual_db_count != entry["db_name_count"]:
                raise FleetValidationError(
                    f"db_name_count mismatch for {table}: actual {actual_db_count}, "
                    f"manifest {entry['db_name_count']}"
                )
            if actual_db_hash != entry["db_names_hash"]:
                raise FleetValidationError(f"db_names_hash mismatch for {table}")

            if manifest.get("mode") == "quick":
                actual_dbs = {str(value) for value in (content["db_values"] or [])}
                if actual_dbs != set(manifest["db_names"]):
                    raise FleetValidationError(f"{table} does not match the quick league scope")

            if active_scoped:
                out_of_scope = int(content["out_of_scope"] or 0)
                if out_of_scope:
                    raise FleetValidationError(
                        f"{table} contains {out_of_scope} rows outside active year {active_year}; "
                        "weekly publishes may not rewrite prior seasons"
                    )

                if manifest.get("mode") == "quick":
                    actual_years = set(content["year_values"] or [])
                    if actual_years != set(years):
                        raise FleetValidationError(f"{table} declared years must exactly match its rows")

            if keys:
                bad_nulls = {
                    key: int(content[f"null_{index}"] or 0)
                    for index, key in enumerate(keys)
                    if int(content[f"null_{index}"] or 0)
                }
                if bad_nulls:
                    raise FleetValidationError(f"{table} contains null identity keys: {bad_nulls}")
                key_expr = ", ".join(_qident(key) for key in keys)
                duplicates = int(
                    conn.execute(
                        f"""
                        SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT
                        FROM (
                            SELECT {key_expr}, COUNT(*) AS cnt
                            FROM {parquet_ref}
                            GROUP BY {key_expr}
                            HAVING COUNT(*) > 1
                        )
                        """
                    ).fetchone()[0]
                    or 0
                )
                if duplicates:
                    raise FleetValidationError(f"{table} contains {duplicates} duplicate identity rows")
    finally:
        conn.close()


def scope_predicate(entry: dict, db_names_ref: str) -> tuple[str, list]:
    """WHERE clause + params bounding a table's delete/verify scope.

    ``db_names_ref`` must be a relation holding the distinct db_names derived
    from the uploaded parquet (never manifest metadata). Every cadence class
    needs an explicit branch here — a fall-through default could widen a
    delete to whole league histories, so unknown classes raise instead.
    """
    db_bound = f"db_name IN (SELECT db_name FROM {db_names_ref})"
    cadence = entry["cadence_class"]
    if entry.get("merge_mode") == "initialize_missing":
        return db_bound, []
    if cadence == CADENCE_ACTIVE_SEASON:
        if "years" in entry["scope"]:
            years = entry["scope"]["years"]
            return f"year IN ({','.join('?' for _ in years)}) AND {db_bound}", years
        return f"year = ? AND {db_bound}", [int(entry["scope"]["year"])]
    if cadence == CADENCE_LEAGUE_ROLLUP:
        return db_bound, []
    raise FleetValidationError(f"No delete scope defined for cadence_class {cadence!r}")


def _default_execute(conn, sql: str, params=None, *, step: str = ""):
    if params is None:
        return conn.execute(sql)
    return conn.execute(sql, params)


class _AggregationConnection:
    """Keep shared aggregation SQL on the publication's timed connection."""

    def __init__(self, conn, execute):
        self._conn = conn
        self._execute = execute
        # The publication owns one serialized transaction. Validate each
        # canonical aggregate schema once, then reuse that result across the
        # season, career, and homepage builders in this same request only.
        self._aggregate_schema_validation_cache: set[tuple[str, str]] = set()

    def execute(self, sql, params=None):
        return self._execute(self._conn, sql, params, step="fleet career aggregation")

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _repair_legacy_null_trade_pick_mirrors(conn, db_name: str, run: Callable) -> None:
    """Fill only legacy NULL sent values from an exact received pick mirror."""
    try:
        columns = {str(row[0]).lower() for row in conn.execute("DESCRIBE public.transactions").fetchall()}
    except Exception:
        return
    required = {
        "db_name", "transaction_id", "year", "transaction_type", "trade_direction",
        "franchise_id", "source_franchise_id", "sleeper_player_id", "trade_asset_lamar",
    }
    if not required.issubset(columns):
        return

    run(
        conn,
        """
        WITH received AS (
            SELECT transaction_id, year,
                   franchise_id AS receiving_franchise_id,
                   source_franchise_id AS sending_franchise_id,
                   sleeper_player_id,
                   MAX(trade_asset_lamar) AS asset_lamar
            FROM public.transactions
            WHERE db_name = ?
              AND transaction_type = 'trade_pick'
              AND trade_direction = 'received'
              AND sleeper_player_id IS NOT NULL
              AND trade_asset_lamar IS NOT NULL
            GROUP BY transaction_id, year, franchise_id, source_franchise_id, sleeper_player_id
        )
        UPDATE public.transactions AS sent
        SET trade_asset_lamar = received.asset_lamar
        FROM received
        WHERE sent.db_name = ?
          AND sent.transaction_type = 'trade_pick'
          AND sent.trade_direction = 'sent'
          AND sent.trade_asset_lamar IS NULL
          AND sent.transaction_id = received.transaction_id
          AND sent.year = received.year
          AND sent.franchise_id = received.sending_franchise_id
          AND COALESCE(sent.source_franchise_id, '') = COALESCE(received.receiving_franchise_id, '')
          AND sent.sleeper_player_id = received.sleeper_player_id
        """,
        [db_name, db_name],
        step="repair legacy null trade-pick mirror",
    )


def _assert_quick_identity_context_compatible(conn, manifest: dict, extract_dir: Path) -> None:
    """Reject already-transformed stale identities before touching league rows.

    All worker identity settings must match saved settings, including empty
    values: not every uploaded derived table is rebuilt by the canonical
    identity helper. Omitted context is treated as empty, never as a bypass.
    """
    if manifest.get("mode") != "quick":
        return
    entry = next((item for item in manifest["tables"] if item["table"] == "league_context"), None)
    exists = conn.execute("SELECT 1 FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='public' AND table_name='league_context'").fetchone()
    if not exists:
        return
    fields = ("manager_name_overrides_json", "franchise_merges_json")
    saved = conn.execute(
        f"SELECT {', '.join(fields)} FROM public.league_context WHERE db_name = ?",
        [manifest["db_names"][0]],
    ).fetchall()
    if not saved:
        return  # First import initializes its own settings in this transaction.
    if len(saved) != 1:
        raise FleetValidationError("Ambiguous saved identity configuration")
    incoming = [(None, None)]
    if entry is not None:
        columns = {column["name"] for column in entry["columns"]}
        select = ', '.join(field if field in columns else 'NULL' for field in fields)
        incoming = conn.execute(f"SELECT {select} FROM read_parquet({_sql_literal(extract_dir / entry['path'])})").fetchall()
    for row in incoming:
        for field, raw, persisted, kind in zip(fields, row, saved[0], (dict, list), strict=True):
            try:
                proposed = json.loads(raw) if raw else kind()
                current = json.loads(persisted) if persisted else kind()
            except (TypeError, ValueError) as error:
                raise FleetValidationError(f"Invalid identity configuration: {field}") from error
            if not isinstance(proposed, kind) or not isinstance(current, kind):
                raise FleetValidationError(f"Invalid identity configuration: {field}")
            if proposed != current:
                raise FleetValidationError(f"Quick identity configuration differs from saved {field}; rebuild with saved settings")


def apply_fleet_merge(
    conn: duckdb.DuckDBPyConnection,
    manifest: dict,
    extract_dir: Path,
    *,
    execute: Callable | None = None,
    manage_transaction: bool = True,
) -> dict[str, Any]:
    """Execute the scoped DELETE + INSERT for every table in the bundle.

    With ``manage_transaction=True`` this runs standalone (tests, smoke).
    main.py passes ``False`` and owns BEGIN/COMMIT so publish-state rows join
    the same transaction.
    """
    run = execute or _default_execute
    merged: dict[str, int] = {}
    season_rollups: dict[str, dict[str, int]] = {}
    season_seconds: dict[str, float] = {}
    season_stage_seconds: dict[str, dict[str, float]] = {}
    season_rollup_years: dict[str, list[int]] = {}
    career_rollups: dict[str, dict[str, int]] = {}
    career_seconds: dict[str, float] = {}
    homepage_rollups: dict[str, dict[str, int]] = {}
    homepage_seconds: dict[str, float] = {}
    merged_db_names: set[str] = set()
    timings: dict[str, dict[str, float]] = {}
    total_start = time.perf_counter()
    in_transaction = False
    ensure_generation_tables(conn)
    try:
        if manage_transaction:
            run(conn, "BEGIN TRANSACTION", step="begin fleet merge")
            in_transaction = True

        # G14 no-rewind: reject the whole bundle if any batched league was
        # republished after this bundle was built. Checked INSIDE the
        # transaction so the read and the eventual bump are atomic.
        expected_generations = manifest.get("league_generations") or {}
        if expected_generations:
            conflicts = check_fleet_generations(conn, expected_generations)
            if conflicts:
                raise FleetGenerationConflict(
                    f"{len(conflicts)} league(s) republished since bundle build: "
                    f"{dict(sorted(conflicts.items())[:5])} — rebuild the bundle"
                )

        _assert_quick_identity_context_compatible(conn, manifest, extract_dir)

        for entry in manifest["tables"]:
            table = entry["table"]
            table_start = time.perf_counter()
            parquet_path = extract_dir / entry["path"]
            parquet_ref = f"read_parquet({_sql_literal(parquet_path)})"
            target = f"public.{_qident(table)}"

            incoming_schema = conn.execute(f"DESCRIBE SELECT * FROM {parquet_ref}").fetchall()
            incoming_col_types = {row[0]: row[1] for row in incoming_schema}
            incoming_cols = list(incoming_col_types.keys())

            try:
                conn.execute(f"DESCRIBE {target}")
            except Exception:
                select_cols = ", ".join(_qident(col) for col in incoming_cols)
                run(
                    conn,
                    f"CREATE TABLE {target} AS SELECT {select_cols} FROM {parquet_ref} WHERE FALSE",
                    step=f"fleet create {table}",
                )

            target_col_types = {row[0]: row[1] for row in conn.execute(f"DESCRIBE {target}").fetchall()}
            missing_cols = [col for col in incoming_cols if col not in target_col_types]
            for col in missing_cols:
                run(
                    conn,
                    f"ALTER TABLE {target} ADD COLUMN {_qident(col)} {incoming_col_types[col]}",
                    step=f"fleet add column {table}.{col}",
                )
            if missing_cols:
                target_col_types = {row[0]: row[1] for row in conn.execute(f"DESCRIBE {target}").fetchall()}

            expected_count = int(entry["row_count"])
            initialize_config = entry.get("merge_mode") == "initialize_missing"
            if initialize_config:
                # Existing league configuration is authoritative, even when
                # the worker carries additional defaults. Filter atomically.
                merged_db_names.update(row[0] for row in conn.execute(f"SELECT DISTINCT db_name FROM {parquet_ref}").fetchall())
                run(conn, f"CREATE OR REPLACE TEMP TABLE _fleet_config_rows AS "
                    f"SELECT incoming.* FROM {parquet_ref} incoming WHERE NOT EXISTS "
                    f"(SELECT 1 FROM {target} saved WHERE saved.db_name = incoming.db_name)",
                    step=f"fleet initialize scope {table}")
                parquet_ref = "_fleet_config_rows"
                expected_count = conn.execute("SELECT COUNT(*) FROM _fleet_config_rows").fetchone()[0]
                if not expected_count:
                    merged[table] = 0
                    continue

            common_cols = [col for col in incoming_cols if col in target_col_types]
            select_exprs = []
            cast_cols: list[str] = []
            for col in common_cols:
                incoming_type = str(incoming_col_types.get(col) or "").upper()
                target_type = str(target_col_types[col]).upper()
                if col in (entry.get("server_generated_columns") or []):
                    if not incoming_type.startswith("TIMESTAMP") or not target_type.startswith("TIMESTAMP"):
                        raise FleetValidationError(
                            f"Server-generated {table}.{col} requires TIMESTAMP source and target"
                        )
                    select_exprs.append(f"CAST(current_timestamp AS TIMESTAMP) AS {_qident(col)}")
                    continue
                if incoming_type == target_type:
                    select_exprs.append(_qident(col))
                else:
                    cast_cols.append(col)
                    select_exprs.append(f"TRY_CAST({_qident(col)} AS {target_col_types[col]}) AS {_qident(col)}")

            # Fail-closed type-drift guard: TRY_CAST silently NULLs values it
            # cannot convert, which would corrupt every batched league in one
            # commit. Count cast failures on the parquet BEFORE any delete
            # runs and abort the publish if a non-null value would be lost.
            if cast_cols:
                failure_exprs = ", ".join(
                    f"SUM(CASE WHEN {_qident(col)} IS NOT NULL AND "
                    f"TRY_CAST({_qident(col)} AS {target_col_types[col]}) IS NULL "
                    f"THEN 1 ELSE 0 END)::BIGINT"
                    for col in cast_cols
                )
                failure_row = conn.execute(f"SELECT {failure_exprs} FROM {parquet_ref}").fetchone()
                cast_failures = {
                    col: int(failure_row[idx] or 0)
                    for idx, col in enumerate(cast_cols)
                    if int(failure_row[idx] or 0)
                }
                if cast_failures:
                    raise RuntimeError(
                        f"Type drift in {table}: non-null values would be NULLed by cast: {cast_failures} "
                        f"(incoming vs target types: "
                        f"{ {c: (incoming_col_types[c], target_col_types[c]) for c in cast_failures} })"
                    )

            # Materialize the parquet-derived db_name set once; the DELETE and
            # verify predicates reuse it instead of re-scanning a potentially
            # 10M-row parquet twice more inside the write transaction.
            if entry.get("empty_reason") == "explicit_empty_active_partition":
                run(
                    conn,
                    "CREATE OR REPLACE TEMP TABLE _fleet_scope_dbs AS "
                    "SELECT CAST(? AS VARCHAR) AS db_name",
                    [manifest["db_names"][0]],
                    step=f"fleet empty scope db {table}",
                )
            else:
                run(
                    conn,
                    f"CREATE OR REPLACE TEMP TABLE _fleet_scope_dbs AS "
                    f"SELECT DISTINCT db_name FROM {parquet_ref}",
                    step=f"fleet scope dbs {table}",
                )
            predicate, params = scope_predicate(entry, "_fleet_scope_dbs")
            merged_db_names.update(row[0] for row in conn.execute("SELECT db_name FROM _fleet_scope_dbs").fetchall())

            delete_start = time.perf_counter()
            if not initialize_config:
                run(conn, f"DELETE FROM {target} WHERE {predicate}", params, step=f"fleet delete {table}")

            insert_start = time.perf_counter()
            run(
                conn,
                f"INSERT INTO {target} ({', '.join(_qident(col) for col in common_cols)}) "
                f"SELECT {', '.join(select_exprs)} FROM {parquet_ref}",
                step=f"fleet insert {table}",
            )

            verify_start = time.perf_counter()
            stored_count = int(
                conn.execute(f"SELECT COUNT(*) FROM {target} WHERE {predicate}", params).fetchone()[0] or 0
            )
            if stored_count != expected_count:
                raise RuntimeError(
                    f"Post-insert scoped row count mismatch for {table}: "
                    f"expected {expected_count}, stored {stored_count}"
                )
            merged[table] = expected_count
            timings[table] = {
                "delete_seconds": round(insert_start - delete_start, 4),
                "insert_seconds": round(verify_start - insert_start, 4),
                "verify_seconds": round(time.perf_counter() - verify_start, 4),
                "total_seconds": round(time.perf_counter() - table_start, 4),
                # Publish-summary visibility: which columns were type-coerced
                # (all verified lossless by the pre-delete cast guard above).
                "coerced_columns": {
                    col: f"{incoming_col_types[col]} -> {target_col_types[col]}" for col in cast_cols
                },
            }

        if merged_db_names != set(manifest.get("db_names") or []):
            raise FleetValidationError("Published row scope does not match the generation-protected league scope")

        if manifest.get("mode") == "quick":
            for db_name in merged_db_names:
                # First publication must leave usable provider context for
                # future updates; existing context is never replaced.
                rows = conn.execute("SELECT platform, league_id, league_ids_json FROM public.league_context WHERE db_name = ?", [db_name]).fetchall()
                if len(rows) != 1:
                    raise FleetValidationError(f"Quick publication requires one league_context for {db_name}")
                platform, league_id, ids_json = rows[0]
                try:
                    ids = json.loads(ids_json) if ids_json else {}
                except (TypeError, ValueError) as error:
                    raise FleetValidationError(f"Invalid league context for {db_name}") from error
                if (platform not in {"yahoo", "sleeper", "espn"} or not isinstance(ids, dict)
                        or not (str(league_id or '').strip() or any(str(value or '').strip() for value in ids.values()))):
                    raise FleetValidationError(f"Quick publication requires usable provider context for {db_name}")

        if manifest.get("schema_version") in {FLEET_CAREER_SCHEMA_VERSION, FLEET_HOMEPAGE_SCHEMA_VERSION}:
            from multi_league.transformations.aggregation.aggregation_utils import (
                aggregate_career_rollups,
                aggregate_complete_chain_season_rollups,
                assert_retained_season_rollup_coverage,
                find_missing_retained_season_rollup_years,
                HomepageValidationError,
            )

            # The source and season partitions are now merged, but still
            # uncommitted. Reuse normal career SQL against this same full
            # history connection. Any error rolls the entire publication back.
            aggregation_conn = _AggregationConnection(conn, run)
            for db_name in sorted(merged_db_names):
                prepared_nfl_lookups = False
                repair_requested = bool(manifest.get("repair_missing_season_rollups"))
                if (
                    manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION
                    or repair_requested
                ):
                    season_start = time.perf_counter()
                    # Validated source partitions have exactly this season.
                    # Unchanged historical seasons remain materialized; careers
                    # and homepage outputs still read the complete live chain.
                    changed_years = set(manifest.get("quick_years") or [manifest["active_year"]])
                    try:
                        gap_scan_start = time.perf_counter()
                        missing_by_table = find_missing_retained_season_rollup_years(
                            aggregation_conn, db_name, season_years=changed_years
                        )
                        gap_scan_seconds = time.perf_counter() - gap_scan_start
                        repair_years = set().union(*missing_by_table.values()) if missing_by_table else set()
                        changed_rollup_years = (
                            changed_years
                            if manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION
                            else set()
                        )
                        rollup_years = changed_rollup_years | repair_years
                        rollup_start = time.perf_counter()
                        if rollup_years:
                            season_rollups[db_name] = aggregate_complete_chain_season_rollups(
                                aggregation_conn,
                                db_name,
                                season_years=changed_rollup_years,
                                repair_years_by_table=missing_by_table,
                                prepare_shared_nfl_lookups=True,
                            )
                            prepared_nfl_lookups = True
                        else:
                            season_rollups[db_name] = {}
                        rollup_seconds = time.perf_counter() - rollup_start
                        validation_start = time.perf_counter()
                        assert_retained_season_rollup_coverage(
                            aggregation_conn, db_name, season_years=changed_years
                        )
                        validation_seconds = time.perf_counter() - validation_start
                    except HomepageValidationError as exc:
                        raise FleetValidationError(str(exc)) from exc
                    season_stage_seconds[db_name] = {
                        "historical_gap_scan": round(gap_scan_seconds, 4),
                        "rollup_build": round(rollup_seconds, 4),
                        "retained_validation": round(validation_seconds, 4),
                    }
                    season_rollup_years[db_name] = sorted(rollup_years)
                    season_seconds[db_name] = round(time.perf_counter() - season_start, 4)
                career_start = time.perf_counter()
                try:
                    career_rollups[db_name] = aggregate_career_rollups(
                        aggregation_conn,
                        db_name,
                        refresh_game_ranks=not prepared_nfl_lookups,
                        prepared_nfl_lookups=prepared_nfl_lookups,
                    )
                finally:
                    if prepared_nfl_lookups:
                        from multi_league.transformations.aggregation.aggregate_fantasy_context import (
                            _drop_scoped_nfl_lookup_tables,
                        )

                        _drop_scoped_nfl_lookup_tables(aggregation_conn)
                career_seconds[db_name] = round(time.perf_counter() - career_start, 4)
                if manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION:
                    from multi_league.transformations.aggregation.aggregation_utils import (
                        HomepageValidationError, aggregate_homepage_rollups,
                    )

                    homepage_start = time.perf_counter()
                    try:
                        _repair_legacy_null_trade_pick_mirrors(conn, db_name, run)
                        changed_year_sql = ", ".join(str(year) for year in sorted(changed_years))
                        active_profile_ids = {
                            str(row[0])
                            for row in aggregation_conn.execute(
                                "SELECT DISTINCT franchise_id FROM public.matchup "
                                f"WHERE db_name = ? AND year IN ({changed_year_sql}) "
                                "AND franchise_id IS NOT NULL",
                                [db_name],
                            ).fetchall()
                        }
                        homepage_rollups[db_name] = aggregate_homepage_rollups(
                            aggregation_conn,
                            db_name,
                            manager_profile_franchise_ids=active_profile_ids,
                            changed_years=changed_years,
                        )
                    except HomepageValidationError as exc:
                        raise FleetValidationError(str(exc)) from exc
                    homepage_seconds[db_name] = round(time.perf_counter() - homepage_start, 4)

        bump_generations(
            conn,
            list(manifest.get("db_names") or []),
            lane="fleet",
            run_id=str(manifest.get("import_run_id") or ""),
        )

        if manage_transaction:
            run(conn, "COMMIT", step="commit fleet merge")
            in_transaction = False
    except Exception:
        if in_transaction:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
        raise

    return {
        "status": "COMMITTED",
        "db_name": FLEET_DB_SENTINEL,
        "bundle_id": manifest["bundle_id"],
        "bundle_hash": manifest["bundle_hash"],
        "active_year": manifest["active_year"],
        "tables": merged,
        "season_rollups": season_rollups,
        "season_seconds": season_seconds,
        "season_stage_seconds": season_stage_seconds,
        "season_rollup_years": season_rollup_years,
        "career_rollups": career_rollups,
        "career_seconds": career_seconds,
        "homepage_rollups": homepage_rollups,
        "homepage_seconds": homepage_seconds,
        "table_count": len(merged),
        "row_count": sum(merged.values()),
        "timings": timings,
        "elapsed_seconds": round(time.perf_counter() - total_start, 4),
    }


# ---------------------------------------------------------------------------
# G14: per-league monotonic publish generations (no-rewind across lanes).
# A fleet bundle records each included league's generation at BUILD time;
# the merge rejects the bundle if any league's current generation differs
# (a repair-lane commit landed after the bundle was built). Both lanes
# increment on commit. No run-id or wall-clock comparisons.
# ---------------------------------------------------------------------------

GENERATIONS_SCHEMA = "merge_admin"
GENERATIONS_TABLE = "league_publish_generations"
FLEET_PUBLISH_LOCK_TABLE = "publish_locks"
FLEET_PUBLISH_LOCK_NAME = "fleet_publish_window"


def _generations_ref() -> str:
    return f"{_qident(GENERATIONS_SCHEMA)}.{_qident(GENERATIONS_TABLE)}"


def _lock_ref() -> str:
    return f"{_qident(GENERATIONS_SCHEMA)}.{_qident(FLEET_PUBLISH_LOCK_TABLE)}"


def ensure_generation_tables(conn) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(GENERATIONS_SCHEMA)}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_generations_ref()} (
            db_name VARCHAR PRIMARY KEY,
            generation BIGINT NOT NULL,
            lane VARCHAR,
            run_id VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_lock_ref()} (
            lock_name VARCHAR PRIMARY KEY,
            locked BOOLEAN NOT NULL,
            run_id VARCHAR,
            locked_at TIMESTAMP
        )
        """
    )


def current_generations(conn, db_names: list[str]) -> dict[str, int]:
    """Current generation per league (missing row = 0)."""
    if not db_names:
        return {}
    placeholders = ", ".join("?" for _ in db_names)
    rows = conn.execute(
        f"SELECT db_name, generation FROM {_generations_ref()} WHERE db_name IN ({placeholders})",
        list(db_names),
    ).fetchall()
    found = {str(r[0]): int(r[1]) for r in rows}
    return {name: found.get(name, 0) for name in db_names}


def check_fleet_generations(conn, expected: dict[str, int]) -> dict[str, dict[str, int]]:
    """Return leagues whose current generation differs from the bundle's."""
    current = current_generations(conn, sorted(expected))
    return {
        name: {"bundle": int(expected[name]), "current": current[name]}
        for name in expected
        if current[name] != int(expected[name])
    }


def bump_generations(conn, db_names: list[str], *, lane: str, run_id: str) -> None:
    """Increment each league's generation (call INSIDE the commit transaction)."""
    for name in sorted(set(db_names)):
        conn.execute(
            f"""
            INSERT INTO {_generations_ref()} (db_name, generation, lane, run_id, updated_at)
            VALUES (?, 1, ?, ?, current_timestamp)
            ON CONFLICT (db_name) DO UPDATE SET
                generation = {_generations_ref()}.generation + 1,
                lane = excluded.lane,
                run_id = excluded.run_id,
                updated_at = now()
            """,
            [name, lane, run_id],
        )


def set_fleet_publish_lock(conn, locked: bool, *, run_id: str) -> None:
    ensure_generation_tables(conn)
    conn.execute(
        f"""
        INSERT INTO {_lock_ref()} (lock_name, locked, run_id, locked_at)
        VALUES (?, ?, ?, current_timestamp)
        ON CONFLICT (lock_name) DO UPDATE SET
            locked = excluded.locked,
            run_id = excluded.run_id,
            locked_at = now()
        """,
        [FLEET_PUBLISH_LOCK_NAME, locked, run_id],
    )


def fleet_publish_locked(conn) -> bool:
    try:
        row = conn.execute(
            f"SELECT locked FROM {_lock_ref()} WHERE lock_name = ?",
            [FLEET_PUBLISH_LOCK_NAME],
        ).fetchone()
    except Exception:
        return False  # table absent = no lock ever set
    return bool(row and row[0])
