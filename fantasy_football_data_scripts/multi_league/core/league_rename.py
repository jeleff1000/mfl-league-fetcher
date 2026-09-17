"""Canonical, idempotent league identity rename primitives.

This module deliberately operates on the canonical table registry supplied by
the caller. It never discovers arbitrary ``db_name`` tables.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any


_DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_names(conn) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }


def _columns(conn, table: str) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table],
        ).fetchall()
    ]


def _count(conn, table: str, db_name: str) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM public.{_qident(table)} WHERE db_name = ?",
            [db_name],
        ).fetchone()[0]
        or 0
    )


def _ensure_receipts(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS merge_admin")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS merge_admin.league_rename_operations (
            operation_id VARCHAR PRIMARY KEY,
            source_db VARCHAR NOT NULL,
            target_db VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            result_json VARCHAR,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _years(conn, db_name: str, existing: set[str]) -> list[int]:
    if "matchup" not in existing or "year" not in _columns(conn, "matchup"):
        return []
    return [
        int(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT TRY_CAST(year AS INTEGER)
            FROM public.matchup
            WHERE db_name = ? AND TRY_CAST(year AS INTEGER) IS NOT NULL
            ORDER BY 1
            """,
            [db_name],
        ).fetchall()
    ]


def consolidate_canonical_league(
    conn,
    *,
    source_db: str,
    target_db: str,
    display_name: str,
    operation_id: str,
    registry: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Move one canonical league identity to ``target_db``.

    Existing target facts win identity conflicts. Aggregate rows are retargeted
    with source rows winning only matching identities, preserving target-only
    current-season rows. A durable receipt makes an identical retry a no-op.
    """
    if not _DB_NAME_RE.fullmatch(source_db) or not _DB_NAME_RE.fullmatch(target_db):
        raise ValueError("invalid league database name")
    if source_db == target_db:
        raise ValueError("source and target league database names must differ")
    if not operation_id or len(operation_id) > 200:
        raise ValueError("operation_id is required and must be at most 200 characters")
    normalized_name = str(display_name or "").strip()
    if not normalized_name or len(normalized_name) > 100:
        raise ValueError("display_name is required and must be at most 100 characters")

    _ensure_receipts(conn)
    prior = conn.execute(
        "SELECT source_db, target_db, status, result_json "
        "FROM merge_admin.league_rename_operations WHERE operation_id = ?",
        [operation_id],
    ).fetchone()
    if prior:
        if str(prior[0]) != source_db or str(prior[1]) != target_db:
            raise ValueError("operation_id already belongs to a different rename")
        if str(prior[2]) == "CONSOLIDATED":
            result = json.loads(str(prior[3] or "{}"))
            result["status"] = "ALREADY_CONSOLIDATED"
            return result

    existing = _table_names(conn)
    canonical = [table for table in sorted(registry) if table in existing]
    source_years = _years(conn, source_db, existing)
    target_years_before = _years(conn, target_db, existing)
    aggregates_retargeted: list[str] = []
    moved_rows: dict[str, int] = {}

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            """
            INSERT INTO merge_admin.league_rename_operations
                (operation_id, source_db, target_db, display_name, status, result_json, updated_at)
            VALUES (?, ?, ?, ?, 'RUNNING', NULL, CURRENT_TIMESTAMP)
            ON CONFLICT (operation_id) DO UPDATE SET
                display_name = excluded.display_name,
                status = 'RUNNING',
                updated_at = now()
            """,
            [operation_id, source_db, target_db, normalized_name],
        )

        if _qualified_table_exists(conn, "merge_admin", "league_publish_generations"):
            source_generation = conn.execute(
                "SELECT generation, lane, run_id, updated_at "
                "FROM merge_admin.league_publish_generations WHERE db_name = ? LIMIT 1",
                [source_db],
            ).fetchone()
            target_generation = conn.execute(
                "SELECT generation, lane, run_id, updated_at "
                "FROM merge_admin.league_publish_generations WHERE db_name = ? LIMIT 1",
                [target_db],
            ).fetchone()
            if source_generation or target_generation:
                winner = max(
                    (row for row in (source_generation, target_generation) if row),
                    key=lambda row: int(row[0] or 0),
                )
                conn.execute(
                    "DELETE FROM merge_admin.league_publish_generations WHERE db_name IN (?, ?)",
                    [source_db, target_db],
                )
                conn.execute(
                    "INSERT INTO merge_admin.league_publish_generations "
                    "(db_name, generation, lane, run_id, updated_at) VALUES (?, ?, ?, ?, ?)",
                    [target_db, *winner],
                )
        for receipt_table in ("league_delta_merge_state", "league_merge_state"):
            if _qualified_table_exists(conn, "merge_admin", receipt_table):
                conn.execute(
                    f"UPDATE merge_admin.{_qident(receipt_table)} SET db_name = ? WHERE db_name = ?",
                    [target_db, source_db],
                )

        for table in canonical:
            columns = _columns(conn, table)
            if "db_name" not in columns:
                continue
            table_ref = f"public.{_qident(table)}"
            if str(registry[table].get("kind")) == "aggregate":
                source_count = _count(conn, table, source_db)
                if source_count <= 0:
                    continue
                identity_keys = [
                    str(key)
                    for key in registry[table].get("identity_keys", [])
                    if str(key) != "db_name" and str(key) in columns
                ]
                if identity_keys:
                    identity_match = " AND ".join(
                        f"t.{_qident(key)} IS NOT DISTINCT FROM s.{_qident(key)}"
                        for key in identity_keys
                    )
                    conn.execute(
                        f"""
                        DELETE FROM {table_ref} AS t
                        WHERE t.db_name = ?
                          AND EXISTS (
                            SELECT 1 FROM {table_ref} AS s
                            WHERE s.db_name = ? AND {identity_match}
                          )
                        """,
                        [target_db, source_db],
                    )
                else:
                    conn.execute(f"DELETE FROM {table_ref} WHERE db_name = ?", [target_db])
                conn.execute(
                    f"UPDATE {table_ref} SET db_name = ? WHERE db_name = ?",
                    [target_db, source_db],
                )
                moved_rows[table] = source_count
                aggregates_retargeted.append(table)
                continue

            source_count = _count(conn, table, source_db)
            if source_count <= 0:
                continue
            target_count = _count(conn, table, target_db)
            identity_keys = [
                str(key)
                for key in registry[table].get("identity_keys", [])
                if str(key) != "db_name" and str(key) in columns
            ]
            if target_count <= 0:
                conn.execute(
                    f"UPDATE {table_ref} SET db_name = ? WHERE db_name = ?",
                    [target_db, source_db],
                )
                moved_rows[table] = source_count
                continue

            if identity_keys:
                insert_columns = ", ".join(_qident(column) for column in columns)
                select_columns = ", ".join(
                    "?" if column == "db_name" else f"s.{_qident(column)}"
                    for column in columns
                )
                identity_match = " AND ".join(
                    f"t.{_qident(key)} IS NOT DISTINCT FROM s.{_qident(key)}"
                    for key in identity_keys
                )
                before = _count(conn, table, target_db)
                conn.execute(
                    f"""
                    INSERT INTO {table_ref} ({insert_columns})
                    SELECT {select_columns}
                    FROM {table_ref} AS s
                    WHERE s.db_name = ?
                      AND NOT EXISTS (
                        SELECT 1 FROM {table_ref} AS t
                        WHERE t.db_name = ? AND {identity_match}
                      )
                    """,
                    [target_db, source_db, target_db],
                )
                moved_rows[table] = _count(conn, table, target_db) - before
            else:
                moved_rows[table] = 0
            conn.execute(f"DELETE FROM {table_ref} WHERE db_name = ?", [source_db])

        if "league_context" in canonical and "league_name" in _columns(conn, "league_context"):
            conn.execute(
                "UPDATE public.league_context SET league_name = ? WHERE db_name = ?",
                [normalized_name, target_db],
            )

        source_rows_remaining = sum(_count(conn, table, source_db) for table in canonical)
        if source_rows_remaining:
            raise RuntimeError(
                f"canonical source rows remain after rename: {source_rows_remaining}"
            )
        target_years = _years(conn, target_db, existing)
        expected_years = sorted(set(source_years) | set(target_years_before))
        if expected_years and target_years != expected_years:
            raise RuntimeError(
                f"target matchup history is incomplete: expected={expected_years}, actual={target_years}"
            )
        result: dict[str, Any] = {
            "status": "CONSOLIDATED",
            "operation_id": operation_id,
            "source_db": source_db,
            "target_db": target_db,
            "source_rows_remaining": source_rows_remaining,
            "target_years": target_years,
            "aggregate_tables_retargeted": aggregates_retargeted,
            "moved_rows": moved_rows,
        }
        result_json = json.dumps(result, sort_keys=True, separators=(",", ":"))
        conn.execute(
            """
            UPDATE merge_admin.league_rename_operations
            SET status = 'CONSOLIDATED', result_json = ?, updated_at = now()
            WHERE operation_id = ?
            """,
            [result_json, operation_id],
        )
        conn.execute("COMMIT")
        return result
    except Exception:
        conn.execute("ROLLBACK")
        raise


_CONTROL_PLANE_TABLES: tuple[tuple[str, str, str], ...] = (
    ("main", "league_credentials", "database_name"),
    ("main", "sleeper_leagues", "database_name"),
    ("main", "espn_leagues", "database_name"),
    ("main", "import_jobs", "database_name"),
    ("accounts", "pending_paid_imports", "database_name"),
    ("accounts", "league_inventory_v1", "league_db"),
    ("accounts", "league_update_manifests", "database_name"),
    ("accounts", "league_update_dispatches", "database_name"),
    ("accounts", "offseason_draft_update_dispatches", "database_name"),
    ("fleet_health", "validation_results", "db_name"),
)
_SINGLETON_CONTROL_TABLES = {
    ("accounts", "league_inventory_v1"),
    ("accounts", "league_update_manifests"),
    ("accounts", "league_update_dispatches"),
    ("accounts", "offseason_draft_update_dispatches"),
}


def _qualified_table_exists(conn, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'
            """,
            [schema, table],
        ).fetchone()[0]
    )


def _qualified_columns(conn, schema: str, table: str) -> list[str]:
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
    ]


def _inventory_row(conn, db_name: str) -> dict[str, Any] | None:
    if not _qualified_table_exists(conn, "accounts", "league_inventory"):
        return None
    columns = _qualified_columns(conn, "accounts", "league_inventory")
    row = conn.execute(
        "SELECT * FROM accounts.league_inventory WHERE database_name = ? LIMIT 1",
        [db_name],
    ).fetchone()
    return dict(zip(columns, row, strict=True)) if row else None


def _same_logical_inventory(source: Mapping[str, Any], target: Mapping[str, Any], target_db: str) -> bool:
    source_pointer = str(source.get("league_db") or "").strip()
    target_pointer = str(target.get("league_db") or "").strip()
    if source_pointer == target_db and target_pointer == target_db:
        return True
    source_platform = str(source.get("platform") or "").strip().lower()
    target_platform = str(target.get("platform") or "").strip().lower()
    source_id = str(source.get("league_id") or "").strip()
    target_id = str(target.get("league_id") or "").strip()
    return bool(
        source_platform
        and source_platform == target_platform
        and source_id
        and source_id == target_id
    )


def _preferred_inventory_value(column: str, source: Any, target: Any, display_name: str, target_db: str) -> Any:
    if column in {"database_name", "league_db"}:
        return target_db
    if column == "league_name":
        return display_name
    if column in {"has_credentials", "in_centralized", "paid", "grandfathered"}:
        return bool(source) or bool(target)
    if column == "tier":
        priority = {"free": 0, "grandfathered": 1, "paid": 2}
        choices = [value for value in (source, target) if value is not None]
        return max(choices, key=lambda value: priority.get(str(value).lower(), 0)) if choices else None
    if column == "entitled_mode":
        priority = {"quick": 0, "full": 1}
        choices = [value for value in (source, target) if value is not None]
        return max(choices, key=lambda value: priority.get(str(value).lower(), 0)) if choices else None
    if column.endswith("_at"):
        choices = [value for value in (source, target) if value is not None]
        return max(choices) if choices else None
    return target if target is not None else source


def _merge_inventory(conn, *, source_db: str, target_db: str, display_name: str) -> None:
    if not _qualified_table_exists(conn, "accounts", "league_inventory"):
        return
    source = _inventory_row(conn, source_db)
    target = _inventory_row(conn, target_db)
    if source and target and not _same_logical_inventory(source, target, target_db):
        raise ValueError("rename target belongs to an unrelated league")
    if not source and not target:
        return
    columns = _qualified_columns(conn, "accounts", "league_inventory")
    if source and target:
        merged = [
            _preferred_inventory_value(column, source.get(column), target.get(column), display_name, target_db)
            for column in columns
        ]
        conn.execute(
            "DELETE FROM accounts.league_inventory WHERE database_name IN (?, ?)",
            [source_db, target_db],
        )
        conn.execute(
            f"INSERT INTO accounts.league_inventory ({', '.join(_qident(column) for column in columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            merged,
        )
    elif source:
        assignments = ["database_name = ?"]
        params: list[Any] = [target_db]
        if "league_db" in columns:
            assignments.append("league_db = ?")
            params.append(target_db)
        if "league_name" in columns:
            assignments.append("league_name = ?")
            params.append(display_name)
        params.append(source_db)
        conn.execute(
            f"UPDATE accounts.league_inventory SET {', '.join(assignments)} WHERE database_name = ?",
            params,
        )
    else:
        assignments = []
        params = []
        if "league_db" in columns:
            assignments.append("league_db = ?")
            params.append(target_db)
        if "league_name" in columns:
            assignments.append("league_name = ?")
            params.append(display_name)
        if assignments:
            params.append(target_db)
            conn.execute(
                f"UPDATE accounts.league_inventory SET {', '.join(assignments)} WHERE database_name = ?",
                params,
            )


def validate_control_plane_rename(conn, *, source_db: str, target_db: str) -> None:
    """Reject an occupied target unless both rows already name one logical league."""
    source_inventory = _inventory_row(conn, source_db)
    target_inventory = _inventory_row(conn, target_db)
    if source_inventory and target_inventory and not _same_logical_inventory(
        source_inventory, target_inventory, target_db
    ):
        raise ValueError("rename target belongs to an unrelated league")


def _reconcile_inventory_credentials(conn, target_db: str) -> None:
    """Mark the inventory credential flag when an encrypted credential row exists."""
    if not _qualified_table_exists(conn, "accounts", "league_inventory"):
        return
    if "has_credentials" not in _qualified_columns(conn, "accounts", "league_inventory"):
        return
    if not _qualified_table_exists(conn, "main", "league_credentials"):
        return
    if not conn.execute(
        "SELECT 1 FROM main.league_credentials WHERE database_name = ? LIMIT 1",
        [target_db],
    ).fetchone():
        return
    conn.execute(
        "UPDATE accounts.league_inventory SET has_credentials = TRUE WHERE database_name = ?",
        [target_db],
    )


def retarget_league_control_plane(
    conn,
    *,
    source_db: str,
    target_db: str,
    display_name: str,
    operation_id: str,
) -> dict[str, Any]:
    """Retarget allowlisted league registries without exposing credentials."""
    if not _DB_NAME_RE.fullmatch(source_db) or not _DB_NAME_RE.fullmatch(target_db):
        raise ValueError("invalid league database name")
    normalized_name = str(display_name or "").strip()
    if not normalized_name:
        raise ValueError("display_name is required")
    conn.execute("CREATE SCHEMA IF NOT EXISTS accounts")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts.league_rename_operations (
            operation_id VARCHAR PRIMARY KEY,
            source_db VARCHAR NOT NULL,
            target_db VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts.league_url_aliases (
            old_db_name VARCHAR PRIMARY KEY,
            new_db_name VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
        """
    )
    prior = conn.execute(
        "SELECT source_db, target_db, status FROM accounts.league_rename_operations WHERE operation_id = ?",
        [operation_id],
    ).fetchone()
    if prior:
        if str(prior[0]) != source_db or str(prior[1]) != target_db:
            raise ValueError("operation_id already belongs to a different rename")
        if str(prior[2]) == "COMMITTED":
            _reconcile_inventory_credentials(conn, target_db)
            return {
                "status": "ALREADY_COMMITTED",
                "operation_id": operation_id,
                "source_db": source_db,
                "target_db": target_db,
            }

    validate_control_plane_rename(conn, source_db=source_db, target_db=target_db)

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            """
            INSERT INTO accounts.league_rename_operations
                (operation_id, source_db, target_db, status, updated_at)
            VALUES (?, ?, ?, 'RUNNING', now())
            ON CONFLICT (operation_id) DO UPDATE SET status = 'RUNNING', updated_at = now()
            """,
            [operation_id, source_db, target_db],
        )
        _merge_inventory(
            conn,
            source_db=source_db,
            target_db=target_db,
            display_name=normalized_name,
        )
        updated_tables: list[str] = []
        for schema, table, column in _CONTROL_PLANE_TABLES:
            if not _qualified_table_exists(conn, schema, table):
                continue
            columns = _qualified_columns(conn, schema, table)
            if column not in columns:
                continue
            table_ref = f"{_qident(schema)}.{_qident(table)}"
            if (schema, table) in _SINGLETON_CONTROL_TABLES:
                target_exists = bool(
                    conn.execute(
                        f"SELECT 1 FROM {table_ref} WHERE {_qident(column)} = ? LIMIT 1",
                        [target_db],
                    ).fetchone()
                )
                if target_exists:
                    conn.execute(
                        f"DELETE FROM {table_ref} WHERE {_qident(column)} = ?",
                        [source_db],
                    )
                    updated_tables.append(f"{schema}.{table}")
                    continue
            assignments = [f"{_qident(column)} = ?"]
            params: list[Any] = [target_db]
            if "league_name" in columns:
                assignments.append("league_name = ?")
                params.append(normalized_name)
            if "import_payload_json" in columns:
                assignments.append(
                    "import_payload_json = CASE WHEN import_payload_json IS NULL "
                    "THEN NULL ELSE replace(import_payload_json, ?, ?) END"
                )
                params.extend([source_db, target_db])
            params.append(source_db)
            conn.execute(
                f"UPDATE {table_ref} "
                f"SET {', '.join(assignments)} WHERE {_qident(column)} = ?",
                params,
            )
            updated_tables.append(f"{schema}.{table}")

        _reconcile_inventory_credentials(conn, target_db)

        conn.execute(
            """
            INSERT INTO accounts.league_url_aliases(old_db_name, new_db_name, updated_at)
            VALUES (?, ?, now())
            ON CONFLICT (old_db_name) DO UPDATE SET
                new_db_name = excluded.new_db_name,
                updated_at = now()
            """,
            [source_db, target_db],
        )
        conn.execute(
            """
            UPDATE accounts.league_rename_operations
            SET status = 'COMMITTED', updated_at = now()
            WHERE operation_id = ?
            """,
            [operation_id],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "status": "COMMITTED",
        "operation_id": operation_id,
        "source_db": source_db,
        "target_db": target_db,
        "updated_tables": updated_tables,
    }
