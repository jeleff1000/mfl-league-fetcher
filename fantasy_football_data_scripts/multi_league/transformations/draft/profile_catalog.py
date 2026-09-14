#!/usr/bin/env python3
"""Fleet-level draft profile catalog.

Discovery miners should be free to surface thousands of candidate signals.
This module turns validated state candidates into a reusable catalog of manager
and league profiles, then assigns those catalog profiles back to managers and
leagues with cheap deterministic matching.

The catalog is intentionally downstream of validation. It should not invent
Hero RB, Zero RB, team affinity, or any other product label first. It learns
which validated state signatures recur across the fleet, keeps only signatures
with enough support, and leaves display naming to the frontend or a later
copy-generation layer.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
import re
import statistics
import sys
from typing import Any
from collections.abc import Iterable

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path

setup_module_path()

from multi_league.core.db_utils import get_pipeline_connection
from multi_league.core.sql_utils import execute_scoped
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    make_logger,
)

log = make_logger("DRAFT-PROFILE-CATALOG")

MODEL_VERSION = "draft-profile-catalog-v0.1"
FLEET_CATALOG_DB_NAME = "fleet_catalog"

PROMOTABLE_LEVELS = {"state_ready", "briefing_watch"}
READY_LEVEL = "state_ready"

DRAFT_PROFILE_CATALOG_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "profile_id": "VARCHAR",
    "profile_kind": "VARCHAR",
    "scope_type": "VARCHAR",
    "state_family": "VARCHAR",
    "feature_type": "VARCHAR",
    "feature_value": "VARCHAR",
    "signal_metric": "VARCHAR",
    "signal_direction": "VARCHAR",
    "profile_status": "VARCHAR",
    "scope_count": "INTEGER",
    "league_count": "INTEGER",
    "ready_count": "INTEGER",
    "watch_count": "INTEGER",
    "total_picks": "INTEGER",
    "max_years_seen": "INTEGER",
    "median_validation_score": "DOUBLE",
    "median_abs_shrunk_z": "DOUBLE",
    "median_lift": "DOUBLE",
    "median_value_delta": "DOUBLE",
    "min_q_value": "DOUBLE",
    "model_version": "VARCHAR",
}

DRAFT_PROFILE_ASSIGNMENT_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "scope_type": "VARCHAR",
    "scope_key": "VARCHAR",
    "scope_label": "VARCHAR",
    "profile_id": "VARCHAR",
    "profile_kind": "VARCHAR",
    "profile_status": "VARCHAR",
    "assignment_rank": "INTEGER",
    "assignment_score": "DOUBLE",
    "evidence_state_key": "VARCHAR",
    "state_family": "VARCHAR",
    "feature_type": "VARCHAR",
    "feature_value": "VARCHAR",
    "signal_metric": "VARCHAR",
    "signal_direction": "VARCHAR",
    "promotion_level": "VARCHAR",
    "validation_score": "DOUBLE",
    "shrunk_z_score": "DOUBLE",
    "value_delta": "DOUBLE",
    "picks": "INTEGER",
    "years_seen": "INTEGER",
    "model_version": "VARCHAR",
}


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _slug(value: Any, *, max_len: int = 36) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return (text or "unknown")[:max_len].strip("_") or "unknown"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _median(values: Iterable[float]) -> float:
    clean = [float(value) for value in values if value is not None and not math.isnan(float(value))]
    return float(statistics.median(clean)) if clean else 0.0


def profile_kind_for_scope(scope_type: str) -> str:
    """Return the catalog bucket for a state-candidate scope."""
    scope = str(scope_type or "").lower()
    if scope == "league_inefficiency":
        return "league"
    if scope == "manager":
        return "manager"
    return scope or "unknown"


def profile_signature(row: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    """Canonical signature for cataloging a validated state candidate."""
    scope_type = str(row.get("scope_type") or "unknown")
    return (
        profile_kind_for_scope(scope_type),
        scope_type,
        str(row.get("state_family") or "unknown"),
        str(row.get("feature_type") or "unknown"),
        str(row.get("feature_value") or "unknown"),
        str(row.get("signal_metric") or "unknown"),
        str(row.get("signal_direction") or "unknown"),
    )


def profile_id_for_signature(signature: tuple[str, str, str, str, str, str, str]) -> str:
    """Build a stable compact profile id from a signature."""
    raw = "|".join(signature)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    prefix = _slug(signature[0], max_len=8)
    family = _slug(signature[2], max_len=18)
    value = _slug(signature[4], max_len=24)
    direction = _slug(signature[6], max_len=12)
    return f"{prefix}_{family}_{value}_{direction}_{digest}"


def _state_rows(rows: Iterable[dict[str, Any]], *, include_watch: bool) -> list[dict[str, Any]]:
    allowed = PROMOTABLE_LEVELS if include_watch else {READY_LEVEL}
    return [
        row
        for row in rows
        if str(row.get("promotion_level") or "") in allowed
        and str(row.get("scope_type") or "") in {"manager", "league_inefficiency"}
    ]


def learn_profile_catalog(
    state_rows: Iterable[dict[str, Any]],
    *,
    include_watch: bool = True,
    min_manager_scopes: int = 4,
    min_league_scopes: int = 4,
    min_ready_share: float = 0.35,
    manager_profile_limit: int = 200,
    league_profile_limit: int = 50,
) -> list[dict[str, Any]]:
    """Learn reusable profile definitions from validated state candidates.

    The unit of learning is a recurring validated state signature. A signature
    must appear across enough distinct managers or leagues before it enters the
    catalog. This keeps one-league anecdotes out of the profile vocabulary.
    """
    grouped: dict[tuple[str, str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _state_rows(state_rows, include_watch=include_watch):
        grouped[profile_signature(row)].append(row)

    catalog: list[dict[str, Any]] = []
    for signature, rows in grouped.items():
        profile_kind, scope_type, state_family, feature_type, feature_value, signal_metric, signal_direction = signature
        scope_ids = {
            (
                str(row.get("db_name") or ""),
                str(row.get("scope_type") or ""),
                str(row.get("scope_key") or ""),
            )
            for row in rows
        }
        leagues = {str(row.get("db_name") or "") for row in rows if row.get("db_name")}
        scope_count = len(scope_ids)
        league_count = len(leagues)
        ready_count = sum(1 for row in rows if row.get("promotion_level") == READY_LEVEL)
        watch_count = sum(1 for row in rows if row.get("promotion_level") == "briefing_watch")
        min_scopes = min_league_scopes if profile_kind == "league" else min_manager_scopes
        if scope_count < min_scopes:
            continue

        ready_share = ready_count / max(scope_count, 1)
        profile_status = "catalog_ready" if ready_share >= min_ready_share else "catalog_watch"
        if profile_status == "catalog_watch" and not include_watch:
            continue

        catalog.append(
            {
                "db_name": FLEET_CATALOG_DB_NAME,
                "profile_id": profile_id_for_signature(signature),
                "profile_kind": profile_kind,
                "scope_type": scope_type,
                "state_family": state_family,
                "feature_type": feature_type,
                "feature_value": feature_value,
                "signal_metric": signal_metric,
                "signal_direction": signal_direction,
                "profile_status": profile_status,
                "scope_count": scope_count,
                "league_count": league_count,
                "ready_count": ready_count,
                "watch_count": watch_count,
                "total_picks": sum(_int(row.get("picks")) for row in rows),
                "max_years_seen": max((_int(row.get("years_seen")) for row in rows), default=0),
                "median_validation_score": round(_median(_float(row.get("validation_score")) for row in rows), 3),
                "median_abs_shrunk_z": round(_median(abs(_float(row.get("shrunk_z_score"))) for row in rows), 3),
                "median_lift": round(_median(_float(row.get("shrunk_lift"), 1.0) for row in rows), 4),
                "median_value_delta": round(_median(_float(row.get("value_delta")) for row in rows), 4),
                "min_q_value": round(min((_float(row.get("q_value"), 1.0) for row in rows), default=1.0), 8),
                "model_version": MODEL_VERSION,
            }
        )

    def sort_key(row: dict[str, Any]) -> tuple[int, int, float, float, int]:
        return (
            1 if row["profile_status"] == "catalog_ready" else 0,
            _int(row.get("scope_count")),
            _float(row.get("median_validation_score")),
            _float(row.get("median_abs_shrunk_z")),
            _int(row.get("total_picks")),
        )

    manager_rows = [row for row in catalog if row["profile_kind"] == "manager"]
    league_rows = [row for row in catalog if row["profile_kind"] == "league"]
    other_rows = [row for row in catalog if row["profile_kind"] not in {"manager", "league"}]
    manager_rows = sorted(manager_rows, key=sort_key, reverse=True)[:manager_profile_limit]
    league_rows = sorted(league_rows, key=sort_key, reverse=True)[:league_profile_limit]
    return [*manager_rows, *league_rows, *sorted(other_rows, key=sort_key, reverse=True)]


def assign_catalog_profiles(
    state_rows: Iterable[dict[str, Any]],
    catalog_rows: Iterable[dict[str, Any]],
    *,
    include_watch: bool = True,
    max_assignments_per_scope: int = 6,
) -> list[dict[str, Any]]:
    """Assign learned catalog profiles back to manager or league scopes."""
    catalog_by_signature = {
        (
            row["profile_kind"],
            row["scope_type"],
            row["state_family"],
            row["feature_type"],
            row["feature_value"],
            row["signal_metric"],
            row["signal_direction"],
        ): row
        for row in catalog_rows
    }

    assignments: list[dict[str, Any]] = []
    for row in _state_rows(state_rows, include_watch=include_watch):
        catalog_row = catalog_by_signature.get(profile_signature(row))
        if not catalog_row:
            continue
        catalog_strength = _float(catalog_row.get("median_validation_score"))
        row_strength = _float(row.get("validation_score"))
        assignment_score = (0.72 * row_strength) + (0.28 * catalog_strength)
        assignments.append(
            {
                "db_name": row.get("db_name"),
                "scope_type": row.get("scope_type"),
                "scope_key": row.get("scope_key"),
                "scope_label": row.get("scope_label"),
                "profile_id": catalog_row["profile_id"],
                "profile_kind": catalog_row["profile_kind"],
                "profile_status": catalog_row["profile_status"],
                "assignment_rank": None,
                "assignment_score": round(assignment_score, 3),
                "evidence_state_key": row.get("state_key"),
                "state_family": row.get("state_family"),
                "feature_type": row.get("feature_type"),
                "feature_value": row.get("feature_value"),
                "signal_metric": row.get("signal_metric"),
                "signal_direction": row.get("signal_direction"),
                "promotion_level": row.get("promotion_level"),
                "validation_score": _float(row.get("validation_score")),
                "shrunk_z_score": _float(row.get("shrunk_z_score")),
                "value_delta": _float(row.get("value_delta")),
                "picks": _int(row.get("picks")),
                "years_seen": _int(row.get("years_seen")),
                "model_version": MODEL_VERSION,
            }
        )

    assignments.sort(
        key=lambda item: (
            str(item.get("db_name") or ""),
            str(item.get("scope_type") or ""),
            str(item.get("scope_key") or ""),
            -_float(item.get("assignment_score")),
        )
    )
    capped: list[dict[str, Any]] = []
    per_scope_count: dict[tuple[str, str, str], int] = defaultdict(int)
    for assignment in assignments:
        scope = (
            str(assignment.get("db_name") or ""),
            str(assignment.get("scope_type") or ""),
            str(assignment.get("scope_key") or ""),
        )
        if per_scope_count[scope] >= max_assignments_per_scope:
            continue
        per_scope_count[scope] += 1
        assignment["assignment_rank"] = per_scope_count[scope]
        capped.append(assignment)
    return capped


def create_draft_profile_tables(conn) -> None:
    """Create or migrate profile catalog and assignment tables."""
    configure_table_catalog(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    for table_name, columns in {
        "draft_profile_catalog": DRAFT_PROFILE_CATALOG_COLUMNS,
        "draft_profile_assignment": DRAFT_PROFILE_ASSIGNMENT_COLUMNS,
    }.items():
        columns_sql = ",\n        ".join(f"{name} {dtype}" for name, dtype in columns.items())
        conn.execute(f"CREATE TABLE IF NOT EXISTS {central_table(table_name)} ({columns_sql})")
        for name, dtype in columns.items():
            conn.execute(f"ALTER TABLE {central_table(table_name)} ADD COLUMN IF NOT EXISTS {name} {dtype}")


def _insert_scoped_rows(
    conn,
    *,
    table_name: str,
    temp_table: str,
    columns: dict[str, str],
    rows: list[dict[str, Any]],
    db_names: Iterable[str],
) -> None:
    """Replace scoped central-table rows using a temp table and db_name guards."""
    columns_sql = ", ".join(f"{name} {dtype}" for name, dtype in columns.items())
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    conn.execute(f"CREATE TEMP TABLE {temp_table} ({columns_sql})")
    if rows:
        conn.executemany(
            f"INSERT INTO {temp_table} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
            [[row.get(col) for col in columns] for row in rows],
        )

    for target_db in db_names:
        scoped = str(target_db)
        execute_scoped(
            conn,
            f"DELETE FROM {central_table(table_name)} WHERE db_name = {_q(scoped)}",
            scoped,
            label=f"{table_name}:delete",
        )
        if rows:
            execute_scoped(
                conn,
                f"""
                INSERT INTO {central_table(table_name)} ({', '.join(columns)})
                SELECT {', '.join(columns)}
                FROM {temp_table}
                WHERE db_name = {_q(scoped)}
                """,
                scoped,
                label=f"{table_name}:insert",
            )


def _fetch_state_rows(conn, *, db_name: str | None = None) -> list[dict[str, Any]]:
    where = f"WHERE db_name = {_q(db_name)}" if db_name else ""
    rows = conn.execute(f"SELECT * FROM {central_table('draft_intelligence_state_candidate')} {where}").fetchall()
    cols = [desc[0] for desc in conn.description]
    return [dict(zip(cols, row)) for row in rows]


def _existing_assignment_db_names(conn) -> set[str]:
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT db_name
            FROM {central_table('draft_profile_assignment')}
            WHERE db_name IS NOT NULL AND TRIM(db_name) != ''
            """
        ).fetchall()
    except Exception:
        return set()
    return {str(row[0]) for row in rows if row and row[0]}


def replace_profile_catalog_and_assignments(
    conn,
    *,
    db_name: str | None = None,
    include_watch: bool = True,
    min_manager_scopes: int = 4,
    min_league_scopes: int = 4,
    manager_profile_limit: int = 200,
    league_profile_limit: int = 50,
) -> dict[str, int]:
    """Rebuild the fleet catalog and scoped assignments from validated states."""
    create_draft_profile_tables(conn)
    state_rows = _fetch_state_rows(conn)
    assignment_state_rows = [
        row for row in state_rows if db_name is None or str(row.get("db_name") or "") == str(db_name)
    ]
    catalog = learn_profile_catalog(
        state_rows,
        include_watch=include_watch,
        min_manager_scopes=min_manager_scopes,
        min_league_scopes=min_league_scopes,
        manager_profile_limit=manager_profile_limit,
        league_profile_limit=league_profile_limit,
    )
    assignments = assign_catalog_profiles(assignment_state_rows, catalog, include_watch=include_watch)

    _insert_scoped_rows(
        conn,
        table_name="draft_profile_catalog",
        temp_table="tmp_draft_profile_catalog",
        columns=DRAFT_PROFILE_CATALOG_COLUMNS,
        rows=catalog,
        db_names=[FLEET_CATALOG_DB_NAME],
    )

    if db_name is not None:
        assignment_db_names = {str(db_name)}
    else:
        assignment_db_names = {
            str(row.get("db_name"))
            for row in assignment_state_rows
            if row.get("db_name") is not None and str(row.get("db_name")).strip()
        } | _existing_assignment_db_names(conn)

    _insert_scoped_rows(
        conn,
        table_name="draft_profile_assignment",
        temp_table="tmp_draft_profile_assignment",
        columns=DRAFT_PROFILE_ASSIGNMENT_COLUMNS,
        rows=assignments,
        db_names=sorted(assignment_db_names),
    )
    return {"catalog": len(catalog), "assignments": len(assignments)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build draft profile catalog from validated states.")
    parser.add_argument("--db", help="Optional single db_name for assignment-only/local checks")
    parser.add_argument("--catalog-db", default="___leagues", help="Local DuckDB file/catalog that holds fleet tables")
    parser.add_argument("--data-dir", help="Use local DuckDB instead of Fly read API")
    parser.add_argument("--min-manager-scopes", type=int, default=4)
    parser.add_argument("--min-league-scopes", type=int, default=4)
    parser.add_argument("--manager-profile-limit", type=int, default=200)
    parser.add_argument("--league-profile-limit", type=int, default=50)
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.data_dir:
        raise RuntimeError("Profile catalog writes require a local/worker DuckDB connection")

    connection_db = args.db or args.catalog_db
    conn = get_pipeline_connection(connection_db, data_dir=args.data_dir, qualified=True)
    try:
        result = replace_profile_catalog_and_assignments(
            conn,
            db_name=args.db,
            include_watch=not args.ready_only,
            min_manager_scopes=args.min_manager_scopes,
            min_league_scopes=args.min_league_scopes,
            manager_profile_limit=args.manager_profile_limit,
            league_profile_limit=args.league_profile_limit,
        )
    finally:
        conn.close()

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"catalog={result['catalog']} assignments={result['assignments']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
