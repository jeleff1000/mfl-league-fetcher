"""Fleet partition merge: scoped weekly publishes into shared ___leagues tables.

FastAPI-free on purpose — main.py wraps these functions with auth, locking,
publish-state bookkeeping, and hard-exit timers, while tests and the offline
smoke harness drive them directly against local DuckDB connections.

Contract (see docs/runbooks/weekly-update-system-plan.md):
- Every table merges with ``merge_mode="replace_scope"``.
- ``active_season`` tables delete only
  ``year = <scope year> AND db_name IN (SELECT DISTINCT db_name FROM parquet)``.
- ``league_rollup`` tables delete only
  ``db_name IN (SELECT DISTINCT db_name FROM parquet)``.
- The db_name set always comes from the uploaded parquet itself, never from
  the manifest, so leagues absent from the bundle can never be deleted.
- Weekly bundles may never carry ``replace_league`` — that mode stays on the
  per-league delta endpoint (the repair lane).
"""

from __future__ import annotations

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
    if manifest.get("mode") != "weekly":
        raise FleetValidationError(f"Unsupported fleet mode: {manifest.get('mode')!r}")
    active_year = manifest.get("active_year")
    if not isinstance(active_year, int):
        raise FleetValidationError("Fleet manifest missing integer active_year")
    if expected_bundle_id and manifest.get("bundle_id") != expected_bundle_id:
        raise FleetValidationError("bundle_id header does not match manifest")
    if expected_bundle_hash and manifest.get("bundle_hash") != expected_bundle_hash:
        raise FleetValidationError("bundle_hash header does not match manifest")
    if not manifest.get("bundle_id") or not manifest.get("bundle_hash"):
        raise FleetValidationError("Fleet manifest missing bundle_id or bundle_hash")

    db_names = manifest.get("db_names")
    if not isinstance(db_names, list) or not db_names:
        raise FleetValidationError("Fleet manifest missing db_names list")
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
        if entry.get("merge_mode") != "replace_scope":
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

        scope = entry.get("scope")
        if not isinstance(scope, dict):
            raise FleetValidationError(f"Missing scope for {table}")
        if cadence == CADENCE_ACTIVE_SEASON:
            if scope.get("year") != active_year:
                raise FleetValidationError(
                    f"{table} scope year {scope.get('year')!r} does not match manifest active_year {active_year}"
                )
        elif scope:
            raise FleetValidationError(f"{table} is {cadence} and must declare an empty scope")

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

            actual_count = int(conn.execute(f"SELECT COUNT(*) FROM {parquet_ref}").fetchone()[0] or 0)
            if actual_count != entry["row_count"]:
                raise FleetValidationError(
                    f"Parquet row_count mismatch for {table}: actual {actual_count}, manifest {entry['row_count']}"
                )

            blank_db = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {parquet_ref} "
                    "WHERE db_name IS NULL OR TRIM(CAST(db_name AS VARCHAR)) = ''"
                ).fetchone()[0]
                or 0
            )
            if blank_db:
                raise FleetValidationError(f"{table} contains {blank_db} rows with blank db_name")

            db_row = conn.execute(
                f"""
                SELECT
                    COUNT(*),
                    COALESCE(sha256(string_agg(db, chr(31) ORDER BY db)), sha256('empty'))
                FROM (SELECT DISTINCT CAST(db_name AS VARCHAR) AS db FROM {parquet_ref})
                """
            ).fetchone()
            actual_db_count, actual_db_hash = int(db_row[0] or 0), str(db_row[1])
            if actual_db_count != entry["db_name_count"]:
                raise FleetValidationError(
                    f"db_name_count mismatch for {table}: actual {actual_db_count}, "
                    f"manifest {entry['db_name_count']}"
                )
            if actual_db_hash != entry["db_names_hash"]:
                raise FleetValidationError(f"db_names_hash mismatch for {table}")

            if entry["cadence_class"] == CADENCE_ACTIVE_SEASON:
                out_of_scope = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {parquet_ref} WHERE year IS DISTINCT FROM ?",
                        [active_year],
                    ).fetchone()[0]
                    or 0
                )
                if out_of_scope:
                    raise FleetValidationError(
                        f"{table} contains {out_of_scope} rows outside active year {active_year}; "
                        "weekly publishes may not rewrite prior seasons"
                    )

            keys = entry.get("_identity_keys") or []
            missing_identity = [key for key in keys if key not in actual_cols]
            if missing_identity:
                raise FleetValidationError(f"{table} is missing identity columns: {', '.join(missing_identity)}")
            if keys:
                null_checks = ", ".join(
                    f"SUM(CASE WHEN {_qident(key)} IS NULL THEN 1 ELSE 0 END)::BIGINT" for key in keys
                )
                null_row = conn.execute(f"SELECT {null_checks} FROM {parquet_ref}").fetchone()
                bad_nulls = {key: int(null_row[idx] or 0) for idx, key in enumerate(keys) if int(null_row[idx] or 0)}
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
    if cadence == CADENCE_ACTIVE_SEASON:
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

    def execute(self, sql, params=None):
        return self._execute(self._conn, sql, params, step="fleet career aggregation")

    def __getattr__(self, name):
        return getattr(self._conn, name)


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
            run(
                conn,
                f"CREATE OR REPLACE TEMP TABLE _fleet_scope_dbs AS "
                f"SELECT DISTINCT db_name FROM {parquet_ref}",
                step=f"fleet scope dbs {table}",
            )
            predicate, params = scope_predicate(entry, "_fleet_scope_dbs")
            merged_db_names.update(row[0] for row in conn.execute("SELECT db_name FROM _fleet_scope_dbs").fetchall())

            delete_start = time.perf_counter()
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
            expected_count = int(entry["row_count"])
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

        if manifest.get("schema_version") in {FLEET_CAREER_SCHEMA_VERSION, FLEET_HOMEPAGE_SCHEMA_VERSION}:
            from multi_league.transformations.aggregation.aggregation_utils import (
                aggregate_career_rollups,
                aggregate_complete_chain_season_rollups,
            )

            # The source and season partitions are now merged, but still
            # uncommitted. Reuse normal career SQL against this same full
            # history connection. Any error rolls the entire publication back.
            aggregation_conn = _AggregationConnection(conn, run)
            for db_name in sorted(merged_db_names):
                if manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION:
                    season_start = time.perf_counter()
                    season_rollups[db_name] = aggregate_complete_chain_season_rollups(
                        aggregation_conn, db_name
                    )
                    season_seconds[db_name] = round(time.perf_counter() - season_start, 4)
                career_start = time.perf_counter()
                career_rollups[db_name] = aggregate_career_rollups(aggregation_conn, db_name)
                career_seconds[db_name] = round(time.perf_counter() - career_start, 4)
                if manifest.get("schema_version") == FLEET_HOMEPAGE_SCHEMA_VERSION:
                    from multi_league.transformations.aggregation.aggregation_utils import (
                        HomepageValidationError, aggregate_homepage_rollups,
                    )

                    homepage_start = time.perf_counter()
                    try:
                        homepage_rollups[db_name] = aggregate_homepage_rollups(aggregation_conn, db_name)
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
