"""Fleet partition bundle creation for weekly (Tuesday) Fly uploads.

A fleet partition bundle carries the changed scope for MANY leagues at once —
the normal Tuesday path for shared ``___leagues`` tables. It reuses the delta
bundle layout (``manifest.json`` + ``tables/<table>.parquet``) but each table
merges with ``replace_scope`` instead of ``replace_league``:

- ``active_season`` tables: the server deletes only
  ``(db_name IN <parquet db_names>) AND year = <active_year>`` before insert.
- ``league_rollup`` tables: the server deletes only
  ``db_name IN <parquet db_names>``.

The server derives the db_name set from the uploaded parquet itself, so
leagues that failed fetch (and therefore have no rows in the bundle) can
never be wiped. Per-league full imports keep using
``delta_publish.build_delta_bundle`` (the repair lane).

See docs/runbooks/weekly-update-system-plan.md ("The publish contract").
"""

from __future__ import annotations

import json
import os
import tarfile
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import duckdb

from multi_league.core.delta_publish import (
    CADENCE_ACTIVE_SEASON,
    MANIFEST_VERSION,
    _actual_columns,
    _content_hash,
    _duplicate_primary_key_count,
    _export_table,
    _key_null_counts,
    _min_max,
    _primary_key_hash,
    _sha256_json,
    canonical_table_registry,
    qident,
)

FLEET_SCHEMA_VERSION = "fleet-partition-v1"
FLEET_PRODUCER = "league-history-fleet-builder"

# Sentinel db_name used for publish-state bookkeeping on the server. It shares
# the league delta state table, so it must satisfy the server's db-name
# pattern ([A-Za-z0-9_]+) and never collide with a real league database name.
FLEET_DB_SENTINEL = "___fleet"


class FleetScopeError(RuntimeError):
    """Raised when staged rows violate the declared weekly publish scope."""


def quarantine_incomplete_leagues(
    conn: duckdb.DuckDBPyConnection,
    *,
    active_year: int,
    prior_baseline: dict[str, Any],
    core_tables: tuple[str, ...] = ("matchup", "player_fantasy", "schedule"),
    min_row_ratio: float = 0.9,
) -> dict[str, Any]:
    """Per-league completeness gate: fetch success is NOT enough to publish.

    A league whose fetch half-succeeded (rate limit, missing payload family,
    truncated response) would otherwise look "present" and get its active
    season overwritten with incomplete rows. This gate compares each staged
    league's active-season row counts against the pre-publish baseline
    (``publish_baseline.py capture`` JSON) and QUARANTINES leagues that:

    - are missing a core table's rows entirely while the baseline had them, or
    - staged fewer than ``min_row_ratio`` x baseline rows for a core table
      (the active season only grows week over week, so a collapse means a
      partial fetch, not a correction).

    Quarantined leagues' rows are deleted from EVERY staged public table so
    the bundle cannot carry them (they fall back to last week's data and the
    repair lane). Leagues absent from the baseline (new imports) pass.
    Returns a report: {"quarantined": {db_name: [reasons]}, "checked": N,
    "passed": N}.
    """
    baseline_tables = prior_baseline.get("tables") or {}

    def baseline_rows(table: str, db_name: str) -> int | None:
        entry = baseline_tables.get(table)
        if not entry:
            return None
        scopes = entry.get("scopes") or {}
        keys = entry.get("scope_keys") or []
        if keys == ["db_name", "year"]:
            scope = scopes.get(f"{db_name}{active_year}")
        elif keys == ["db_name"]:
            scope = scopes.get(db_name)
        else:
            return None
        return int(scope["rows"]) if scope else None

    staged_tables = {
        row[0]
        for row in conn.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog = current_database()
              AND table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }

    staged_leagues: set[str] = set()
    staged_counts: dict[str, dict[str, int]] = {}
    for table in core_tables:
        if table not in staged_tables:
            continue
        rows = conn.execute(
            f"SELECT CAST(db_name AS VARCHAR), COUNT(*) FROM public.{qident(table)} GROUP BY 1"
        ).fetchall()
        staged_counts[table] = {str(db): int(n) for db, n in rows}
        staged_leagues.update(staged_counts[table])

    quarantined: dict[str, list[str]] = {}
    for db_name in sorted(staged_leagues):
        reasons: list[str] = []
        for table in core_tables:
            expected = baseline_rows(table, db_name)
            if expected is None or expected == 0:
                continue  # new league or table not previously populated
            staged = staged_counts.get(table, {}).get(db_name, 0)
            if staged == 0:
                reasons.append(f"{table}: baseline had {expected} active-season rows, staged has none")
            elif staged < expected * min_row_ratio:
                reasons.append(
                    f"{table}: staged {staged} rows < {min_row_ratio:.0%} of baseline {expected} "
                    "(row-count collapse; likely partial fetch)"
                )
        if reasons:
            quarantined[db_name] = reasons

    if quarantined:
        placeholders = ", ".join("?" for _ in quarantined)
        for table in sorted(staged_tables):
            if "db_name" not in _actual_columns(conn, table):
                continue
            conn.execute(
                f"DELETE FROM public.{qident(table)} WHERE CAST(db_name AS VARCHAR) IN ({placeholders})",
                list(quarantined),
            )

    return {
        "checked": len(staged_leagues),
        "passed": len(staged_leagues) - len(quarantined),
        "quarantined": quarantined,
    }


@dataclass(frozen=True)
class FleetBundle:
    path: Path
    manifest: dict[str, Any]

    @property
    def bundle_id(self) -> str:
        return str(self.manifest["bundle_id"])

    @property
    def bundle_hash(self) -> str:
        return str(self.manifest["bundle_hash"])


def _db_names_summary(conn: duckdb.DuckDBPyConnection, table: str) -> tuple[int, str]:
    """Distinct db_name count + order-independent hash for cross-checks."""
    row = conn.execute(
        f"""
        SELECT
            COUNT(*),
            COALESCE(sha256(string_agg(db, chr(31) ORDER BY db)), sha256('empty'))
        FROM (
            SELECT DISTINCT CAST(db_name AS VARCHAR) AS db
            FROM public.{qident(table)}
        )
        """
    ).fetchone()
    return int(row[0] or 0), str(row[1])


def _blank_db_name_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM public.{qident(table)}
            WHERE db_name IS NULL OR TRIM(CAST(db_name AS VARCHAR)) = ''
            """
        ).fetchone()[0]
        or 0
    )


def _out_of_scope_year_count(conn: duckdb.DuckDBPyConnection, table: str, active_year: int) -> int:
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM public.{qident(table)}
            WHERE year IS DISTINCT FROM ?
            """,
            [active_year],
        ).fetchone()[0]
        or 0
    )


def build_fleet_partition_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    active_year: int,
    league_generations: dict[str, int],
    tables: list[str] | None = None,
    output_dir: str | Path | None = None,
    import_run_id: str | None = None,
    publish_sequence: int | None = None,
    producer_version: str | None = None,
) -> FleetBundle:
    """Build a fleet partition bundle from a staged fleet DuckDB.

    ``conn`` must hold, in its ``public`` schema, exactly the rows to publish:
    for ``active_season`` tables only ``year == active_year`` rows, for
    ``league_rollup`` tables the full refreshed slice per included league.
    Scope violations, blank db_names, and duplicate/null identity keys fail
    the build — nothing is uploaded.

    ``league_generations`` (G14 no-rewind) is the generation each batched
    league was at when this bundle's data was built — read from the server's
    ``merge_admin.league_publish_generations`` table (missing league = 0).
    The merge rejects the bundle if any league was republished since.
    """
    registry = canonical_table_registry()
    requested = list(tables) if tables is not None else sorted(registry)
    unknown = sorted(set(requested) - set(registry))
    if unknown:
        raise ValueError(f"Unknown canonical tables requested: {', '.join(unknown)}")

    run_id = str(
        import_run_id
        or os.environ.get("GITHUB_RUN_ID")
        or os.environ.get("IMPORT_RUN_ID")
        or f"local-{os.getpid()}-{int(time.time())}"
    )
    sequence = int(
        publish_sequence
        if publish_sequence is not None
        else (os.environ.get("GITHUB_RUN_ATTEMPT") or os.environ.get("PUBLISH_SEQUENCE") or "1")
    )
    version = producer_version or os.environ.get("GITHUB_SHA") or os.environ.get("PRODUCER_VERSION") or "local"

    base_dir = Path(output_dir) if output_dir else Path(tempfile.mkdtemp(prefix="fleet_partition_"))
    tables_dir = base_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    local_tables = {
        row[0]
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = current_database()
              AND table_schema = 'public'
              AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }

    table_entries: list[dict[str, Any]] = []
    omitted_tables: list[dict[str, Any]] = []
    union_db_names: set[str] = set()

    for table in sorted(requested):
        spec = registry[table]
        if table not in local_tables:
            omitted_tables.append({"table": table, "reason": "table_not_present_locally"})
            continue

        actual_cols = set(_actual_columns(conn, table))
        allowed_cols = list(spec["columns"].keys())
        upload_cols = [c for c in allowed_cols if c in actual_cols]
        if "db_name" not in upload_cols:
            omitted_tables.append({"table": table, "reason": "missing_db_name_partition"})
            continue

        row_count = int(conn.execute(f"SELECT COUNT(*) FROM public.{qident(table)}").fetchone()[0] or 0)
        if row_count == 0:
            omitted_tables.append({"table": table, "reason": "no_rows_in_scope"})
            continue

        blank_db = _blank_db_name_count(conn, table)
        if blank_db:
            raise FleetScopeError(f"{table} has {blank_db} rows with blank db_name")

        cadence = str(spec["cadence_class"])
        scope: dict[str, Any] = {}
        if cadence == CADENCE_ACTIVE_SEASON:
            if "year" not in upload_cols:
                raise FleetScopeError(f"{table} is active_season but staged data has no year column")
            out_of_scope = _out_of_scope_year_count(conn, table, active_year)
            if out_of_scope:
                raise FleetScopeError(
                    f"{table} has {out_of_scope} rows outside active year {active_year}; "
                    "weekly publishes may not rewrite prior seasons"
                )
            scope = {"year": int(active_year)}

        # Weekly fleet publishes must carry full identity — silently narrowing
        # the key would let duplicate rows through (or reject valid ones).
        missing_identity = [c for c in spec["primary_keys"] if c not in upload_cols]
        if missing_identity:
            raise FleetScopeError(
                f"{table} staged data is missing identity columns: {', '.join(missing_identity)}"
            )
        primary_keys = list(spec["primary_keys"])
        if primary_keys:
            null_counts = _key_null_counts(conn, table, primary_keys)
            bad_nulls = {key: count for key, count in null_counts.items() if count}
            if bad_nulls:
                raise FleetScopeError(f"{table} has null identity keys: {bad_nulls}")
            duplicates = _duplicate_primary_key_count(conn, table, primary_keys)
            if duplicates:
                raise FleetScopeError(f"{table} has {duplicates} duplicate identity rows")

        parquet_path = tables_dir / f"{table}.parquet"
        compression, file_hash = _export_table(conn, table, upload_cols, parquet_path)
        db_name_count, db_names_hash = _db_names_summary(conn, table)
        union_db_names.update(
            str(row[0]) for row in conn.execute(f'SELECT DISTINCT db_name FROM public.{qident(table)}').fetchall()
        )

        fingerprints = {
            "row_count": row_count,
            "content_hash": _content_hash(conn, table, upload_cols),
            "primary_key_hash": _primary_key_hash(conn, table, primary_keys),
            "duplicate_primary_keys": 0,
            "key_null_counts": {key: 0 for key in primary_keys},
            **_min_max(conn, table, set(upload_cols)),
        }

        table_entries.append(
            {
                "table": table,
                "included": True,
                "path": f"tables/{table}.parquet",
                "format": "parquet",
                "compression": compression,
                "sha256": file_hash,
                "row_count": row_count,
                "columns": [{"name": c, "type": spec["columns"][c]} for c in upload_cols],
                "partition_keys": [c for c in spec["partition_keys"] if c in upload_cols],
                "identity_keys": primary_keys,
                "primary_keys": primary_keys,
                "merge_mode": "replace_scope",
                "cadence_class": cadence,
                "scope": scope,
                "db_name_count": db_name_count,
                "db_names_hash": db_names_hash,
                "kind": spec["kind"],
                "fingerprints": fingerprints,
            }
        )

    if not table_entries:
        raise FleetScopeError("Fleet partition bundle contains no publishable tables")

    logical_payload = {
        "manifest_version": MANIFEST_VERSION,
        "schema_version": FLEET_SCHEMA_VERSION,
        "db_name": FLEET_DB_SENTINEL,
        "mode": "weekly",
        "active_year": int(active_year),
        "import_run_id": run_id,
        "publish_sequence": sequence,
        "db_name_count": len(union_db_names),
        "db_names": sorted(union_db_names),
        "league_generations": {
            name: int(league_generations.get(name, 0)) for name in sorted(union_db_names)
        },
        "tables": [
            {
                "table": t["table"],
                "row_count": t["row_count"],
                "columns": t["columns"],
                "primary_keys": t["primary_keys"],
                "merge_mode": t["merge_mode"],
                "cadence_class": t["cadence_class"],
                "scope": t["scope"],
                "db_name_count": t["db_name_count"],
                "db_names_hash": t["db_names_hash"],
                "fingerprints": t["fingerprints"],
            }
            for t in table_entries
        ],
        "omitted_tables": omitted_tables,
    }
    bundle_hash = _sha256_json(logical_payload)
    bundle_id = f"fleet-{run_id}-{sequence}-{bundle_hash[:12]}-{uuid.uuid4().hex[:8]}"

    manifest = {
        **logical_payload,
        "tables": table_entries,
        "producer": FLEET_PRODUCER,
        "producer_version": version,
        "created_at": datetime.now(UTC).isoformat(),
        "bundle_id": bundle_id,
        "bundle_hash": bundle_hash,
    }

    manifest_path = base_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2, default=str), encoding="utf-8")

    archive_path = base_dir / f"{bundle_id}.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(manifest_path, arcname="manifest.json")
        for entry in table_entries:
            tar.add(base_dir / entry["path"], arcname=entry["path"])

    return FleetBundle(path=archive_path, manifest=manifest)
