"""Delta publish bundle creation for Fly league uploads.

This module creates a small, versioned archive from a local league DuckDB:

    manifest.json
    tables/<canonical_table>.parquet

The manifest uses logical table fingerprints for idempotency. Parquet file
hashes are recorded only for transfer integrity because Parquet bytes are not
guaranteed to be deterministic across DuckDB/compression versions.
"""

from __future__ import annotations

import hashlib
import json
import os
import tarfile
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, UTC
from pathlib import Path
from typing import Any

import duckdb

from multi_league.core.player_week_identity import (
    matchup_publish_dedup_statements,
    player_week_publish_dedup_statements,
    player_week_repair_statements,
)

MANIFEST_VERSION = 1
SCHEMA_VERSION = "league-delta-v1"
PRODUCER = "league-history-worker"

CORE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "matchup": ("db_name", "manager_week"),
    # player_week is the canonical per-player/week identity. Full imports add
    # unrostered NFL rows with no franchise_id, so franchise_id cannot be part
    # of the non-null publish identity for this table.
    "player_fantasy": ("db_name", "player_week"),
    "draft": ("db_name", "year", "draft_id", "round", "pick"),
    "transactions": ("db_name", "transaction_id", "transaction_sequence"),
    # A schedule row is the per-franchise weekly slot. Opponent identity can be
    # absent for byes or still-unresolved schedules, so it is not part of the
    # publish identity.
    "schedule": ("db_name", "manager_week"),
    "league_settings": ("db_name", "year"),
    "franchise_identity_audit": ("db_name", "assignment_key"),
    "franchise_identity_registry": ("db_name", "resolved_franchise_id"),
    "keeper_config": ("db_name", "year"),
    "league_context": ("db_name",),
    "league_rules": ("db_name",),
    "manager_overrides": ("db_name", "id"),
    "standings_config": ("db_name",),
}

FULL_REQUIRED_TABLES = {"matchup", "player_fantasy", "league_settings"}

# Cadence classes for the weekly (Tuesday) publish contract.
# See docs/runbooks/weekly-update-system-plan.md ("The publish contract").
CADENCE_APPEND_WEEK = "append_week"  # reserved for v2 sidecar-migrated tables
CADENCE_ACTIVE_SEASON = "active_season"
CADENCE_LEAGUE_ROLLUP = "league_rollup"

CADENCE_CLASSES = {CADENCE_APPEND_WEEK, CADENCE_ACTIVE_SEASON, CADENCE_LEAGUE_ROLLUP}

_CADENCE_OVERRIDES: dict[str, str] = {
    # Identity-resolution history is whole-league data; its year/week columns
    # describe when assignments happened, not a weekly publish grain.
    "franchise_identity_audit": CADENCE_LEAGUE_ROLLUP,
}


def cadence_class_for_table(table: str, columns: dict[str, str] | set[str]) -> str:
    """Weekly publish cadence for a canonical table.

    Tables carrying a year column are replaced at (db_name-set, active year)
    scope on a normal Tuesday; everything else is a small whole-league slice.
    Prior seasons are frozen either way — weekly publishes may never rewrite
    them (rewrite-history policy).
    """
    override = _CADENCE_OVERRIDES.get(table)
    if override:
        return override
    return CADENCE_ACTIVE_SEASON if "year" in columns else CADENCE_LEAGUE_ROLLUP


def weekly_scope_keys_for_cadence(cadence: str) -> list[str]:
    if cadence == CADENCE_ACTIVE_SEASON:
        return ["db_name", "year"]
    if cadence == CADENCE_APPEND_WEEK:
        return ["db_name", "year", "week"]
    return ["db_name"]


@dataclass(frozen=True)
class DeltaBundle:
    path: Path
    manifest: dict[str, Any]

    @property
    def bundle_id(self) -> str:
        return str(self.manifest["bundle_id"])

    @property
    def bundle_hash(self) -> str:
        return str(self.manifest["bundle_hash"])


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def _sha256_json(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _column_value_expr(column: str) -> str:
    quoted = qident(column)
    return f"COALESCE(CAST({quoted} AS VARCHAR), '<NULL>')"


def _row_hash_expr(columns: list[str]) -> str:
    if not columns:
        return "sha256('')"
    return "sha256(concat_ws(chr(31), " + ", ".join(_column_value_expr(c) for c in columns) + "))"


def _content_hash(conn: duckdb.DuckDBPyConnection, table: str, columns: list[str]) -> str:
    table_ref = f"public.{qident(table)}"
    row_hash = _row_hash_expr(columns)
    sql = f"""
    SELECT COALESCE(sha256(string_agg(row_hash, '' ORDER BY row_hash)), sha256('empty')) AS content_hash
    FROM (
        SELECT {row_hash} AS row_hash
        FROM {table_ref}
    )
    """
    return str(conn.execute(sql).fetchone()[0])


def _primary_key_hash(conn: duckdb.DuckDBPyConnection, table: str, primary_keys: list[str]) -> str | None:
    if not primary_keys:
        return None
    table_ref = f"public.{qident(table)}"
    row_hash = _row_hash_expr(primary_keys)
    sql = f"""
    SELECT COALESCE(sha256(string_agg(row_hash, '' ORDER BY row_hash)), sha256('empty')) AS primary_key_hash
    FROM (
        SELECT {row_hash} AS row_hash
        FROM {table_ref}
    )
    """
    return str(conn.execute(sql).fetchone()[0])


def _duplicate_primary_key_count(conn: duckdb.DuckDBPyConnection, table: str, primary_keys: list[str]) -> int:
    if not primary_keys:
        return 0
    table_ref = f"public.{qident(table)}"
    cols = ", ".join(qident(c) for c in primary_keys)
    sql = f"""
    SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT AS duplicate_count
    FROM (
        SELECT {cols}, COUNT(*) AS cnt
        FROM {table_ref}
        GROUP BY {cols}
        HAVING COUNT(*) > 1
    )
    """
    return int(conn.execute(sql).fetchone()[0] or 0)


def _key_null_counts(conn: duckdb.DuckDBPyConnection, table: str, keys: list[str]) -> dict[str, int]:
    if not keys:
        return {}
    table_ref = f"public.{qident(table)}"
    selects = ", ".join(f"SUM(CASE WHEN {qident(c)} IS NULL THEN 1 ELSE 0 END)::BIGINT AS {qident(c)}" for c in keys)
    row = conn.execute(f"SELECT {selects} FROM {table_ref}").fetchone()
    return {key: int(row[i] or 0) for i, key in enumerate(keys)}


def _min_max(conn: duckdb.DuckDBPyConnection, table: str, columns: set[str]) -> dict[str, Any]:
    table_ref = f"public.{qident(table)}"
    metrics: dict[str, Any] = {}
    select_parts: list[str] = []
    for col in ("year", "week"):
        if col in columns:
            select_parts.extend([f"MIN({qident(col)})", f"MAX({qident(col)})"])
    if not select_parts:
        return metrics
    row = conn.execute(f"SELECT {', '.join(select_parts)} FROM {table_ref}").fetchone()
    idx = 0
    for col in ("year", "week"):
        if col in columns:
            metrics[f"min_{col}"] = row[idx]
            metrics[f"max_{col}"] = row[idx + 1]
            idx += 2
    return metrics


def canonical_table_registry() -> dict[str, dict[str, Any]]:
    """Return upload table policy generated from the repo canonical specs."""
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS
    from multi_league.core.local_db import _TABLE_COLUMN_TYPES, _expected_remote_tables

    registry: dict[str, dict[str, Any]] = {}
    for table in sorted(_expected_remote_tables()):
        if table in _TABLE_COLUMN_TYPES:
            columns = dict(_TABLE_COLUMN_TYPES[table])
            kind = "core"
            source_tables: list[str] = []
            primary_keys = [c for c in CORE_PRIMARY_KEYS.get(table, ()) if c in columns]
        elif table in AGGREGATE_TABLE_SPECS:
            spec = AGGREGATE_TABLE_SPECS[table]
            columns = dict(spec.column_types)
            kind = "aggregate"
            source_tables = sorted(spec.source_tables)
            primary_keys = [c for c in spec.primary_key if c in columns]
            # This is one materialized summary row per league.  The aggregate
            # builder has no intrinsic row key, but a Fleet replace-scope
            # publish needs an explicit identity to atomically replace it.
            if table == "homepage_league_summary" and "db_name" in columns:
                primary_keys = ["db_name"]
        else:
            continue
        cadence = cadence_class_for_table(table, columns)
        registry[table] = {
            "table": table,
            "kind": kind,
            "columns": columns,
            "partition_keys": ["db_name"] if "db_name" in columns else [],
            "identity_keys": primary_keys,
            "primary_keys": primary_keys,
            "valid_import_modes": ["quick", "full", "fleet", "all_years", "unknown"],
            "merge_mode": "replace_league",
            "cadence_class": cadence,
            "weekly_merge_mode": "replace_scope",
            "weekly_scope_keys": weekly_scope_keys_for_cadence(cadence),
            "source_tables": source_tables,
            "required_for_full": table in FULL_REQUIRED_TABLES,
        }
    return registry


def _actual_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_catalog = current_database()
              AND table_schema = 'public'
              AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table],
        ).fetchall()
    ]


def _repair_player_fantasy_publish_identity(conn: duckdb.DuckDBPyConnection) -> None:
    """Fill any remaining blank player_week keys before manifesting a bundle."""
    table_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_schema = 'public'
          AND table_name = 'player_fantasy'
          AND table_type = 'BASE TABLE'
        """
    ).fetchone()[0]
    if not table_exists:
        return

    columns = set(_actual_columns(conn, "player_fantasy"))
    if "player_week" not in columns:
        return

    table_ref = f"public.{qident('player_fantasy')}"
    before = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_ref}
            WHERE NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
            """
        ).fetchone()[0]
        or 0
    )

    for _, sql in player_week_repair_statements(table_ref, columns):
        conn.execute(sql)

    after = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_ref}
            WHERE NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
            """
        ).fetchone()[0]
        or 0
    )
    if before and after == 0:
        print(f"[UPLOAD-FLY] repaired {before:,} blank player_week identities before delta bundle")
    if after:
        sample_columns = [
            c
            for c in ("db_name", "year", "week", "NFL_player_id", "player", "manager", "fantasy_position")
            if c in columns
        ]
        sample_select = ", ".join(qident(c) for c in sample_columns) if sample_columns else "COUNT(*) AS rows"
        samples = conn.execute(
            f"""
            SELECT {sample_select}
            FROM {table_ref}
            WHERE NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
            LIMIT 5
            """
        ).fetchall()
        raise RuntimeError(
            f"player_fantasy still has {after:,} blank player_week identities after repair; samples={samples}"
        )

    statements = player_week_publish_dedup_statements(table_ref, columns)
    if not statements:
        return
    temp_table = "_player_fantasy_publish_dedup"
    conn.execute(statements[0][1])
    duplicate_count = int(conn.execute(f'SELECT COUNT(*) FROM "{temp_table}"').fetchone()[0] or 0)
    if duplicate_count:
        conn.execute(statements[1][1])
        print(f"[UPLOAD-FLY] removed {duplicate_count:,} duplicate player_week identities before delta bundle")
    conn.execute(statements[2][1])


def _dedupe_matchup_publish_identity(conn: duckdb.DuckDBPyConnection) -> None:
    """Remove any remaining duplicate matchup manager_week keys before export."""
    table_exists = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = current_database()
          AND table_schema = 'public'
          AND table_name = 'matchup'
          AND table_type = 'BASE TABLE'
        """
    ).fetchone()[0]
    if not table_exists:
        return

    columns = set(_actual_columns(conn, "matchup"))
    if "manager_week" not in columns:
        return

    table_ref = f"public.{qident('matchup')}"
    statements = matchup_publish_dedup_statements(table_ref, columns)
    if not statements:
        return

    temp_table = "_matchup_publish_dedup"
    try:
        conn.execute(statements[0][1])
        duplicate_count = int(conn.execute(f'SELECT COUNT(*) FROM "{temp_table}"').fetchone()[0] or 0)
        if duplicate_count:
            conn.execute(statements[1][1])
            print(f"[UPLOAD-FLY] removed {duplicate_count:,} duplicate manager_week identities before delta bundle")
    finally:
        conn.execute(statements[2][1])


def _export_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    out_path: Path,
) -> tuple[str, str]:
    select_cols = ", ".join(qident(c) for c in columns)
    sql = (
        f"COPY (SELECT {select_cols} FROM public.{qident(table)}) "
        f"TO {sql_literal(out_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    try:
        conn.execute(sql)
        return "zstd", _sha256_file(out_path)
    except Exception:
        if out_path.exists():
            out_path.unlink()
        conn.execute(
            f"COPY (SELECT {select_cols} FROM public.{qident(table)}) TO {sql_literal(out_path)} (FORMAT PARQUET)"
        )
        return "default", _sha256_file(out_path)


def build_delta_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    db_name: str,
    import_mode: str | None = None,
    platform: str | None = None,
    league_id: str | None = None,
    output_dir: str | Path | None = None,
) -> DeltaBundle:
    _repair_player_fantasy_publish_identity(conn)
    _dedupe_matchup_publish_identity(conn)

    registry = canonical_table_registry()
    normalized_mode = (import_mode or os.environ.get("IMPORT_MODE") or "unknown").lower().strip()
    normalized_platform = (
        (platform or os.environ.get("PLATFORM") or os.environ.get("LEAGUE_PLATFORM") or "unknown").lower().strip()
    )
    import_run_id = (
        os.environ.get("GITHUB_RUN_ID") or os.environ.get("IMPORT_RUN_ID") or f"local-{os.getpid()}-{int(time.time())}"
    )
    publish_sequence = int(os.environ.get("GITHUB_RUN_ATTEMPT") or os.environ.get("PUBLISH_SEQUENCE") or "1")
    producer_version = os.environ.get("GITHUB_SHA") or os.environ.get("PRODUCER_VERSION") or "local"

    base_dir = Path(output_dir) if output_dir else Path(tempfile.mkdtemp(prefix=f"{db_name}_delta_"))
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

    for table, spec in sorted(registry.items()):
        if table not in local_tables:
            omitted_tables.append(
                {
                    "table": table,
                    "reason": "table_not_present_locally",
                    "required_for_full": bool(spec["required_for_full"]),
                }
            )
            continue

        actual_cols = _actual_columns(conn, table)
        allowed_cols = list(spec["columns"].keys())
        upload_cols = [c for c in allowed_cols if c in set(actual_cols)]
        if "db_name" in allowed_cols and "db_name" not in upload_cols:
            omitted_tables.append({"table": table, "reason": "missing_db_name_partition"})
            continue
        if not upload_cols:
            omitted_tables.append({"table": table, "reason": "no_canonical_columns"})
            continue

        row_count = int(conn.execute(f"SELECT COUNT(*) FROM public.{qident(table)}").fetchone()[0] or 0)
        parquet_path = tables_dir / f"{table}.parquet"
        compression, file_hash = _export_table(conn, table, upload_cols, parquet_path)

        primary_keys = [c for c in spec["primary_keys"] if c in upload_cols]
        fingerprints = {
            "row_count": row_count,
            "content_hash": _content_hash(conn, table, upload_cols),
            "primary_key_hash": _primary_key_hash(conn, table, primary_keys),
            "duplicate_primary_keys": _duplicate_primary_key_count(conn, table, primary_keys),
            "key_null_counts": _key_null_counts(conn, table, primary_keys),
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
                "identity_keys": [c for c in spec["identity_keys"] if c in upload_cols],
                "primary_keys": primary_keys,
                "merge_mode": spec["merge_mode"],
                "scope": {"db_name": db_name},
                "kind": spec["kind"],
                "source_tables": spec["source_tables"],
                "empty_reason": "no_rows_in_scope" if row_count == 0 else None,
                "fingerprints": fingerprints,
            }
        )

    logical_payload = {
        "manifest_version": MANIFEST_VERSION,
        "schema_version": SCHEMA_VERSION,
        "db_name": db_name,
        "league_id": league_id,
        "platform": normalized_platform,
        "import_mode": normalized_mode,
        "import_run_id": str(import_run_id),
        "publish_sequence": publish_sequence,
        "tables": [
            {
                "table": t["table"],
                "row_count": t["row_count"],
                "columns": t["columns"],
                "partition_keys": t["partition_keys"],
                "primary_keys": t["primary_keys"],
                "merge_mode": t["merge_mode"],
                "scope": t["scope"],
                "fingerprints": t["fingerprints"],
            }
            for t in table_entries
        ],
        "omitted_tables": omitted_tables,
    }
    bundle_hash = _sha256_json(logical_payload)
    bundle_id = f"{db_name}-{import_run_id}-{publish_sequence}-{bundle_hash[:12]}-{uuid.uuid4().hex[:8]}"

    manifest = {
        **logical_payload,
        "tables": table_entries,
        "producer": PRODUCER,
        "producer_version": producer_version,
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

    return DeltaBundle(path=archive_path, manifest=manifest)
