#!/usr/bin/env python3
"""Fleet-level draft profile archetypes.

The profile catalog stores one validated signal at a time. This module learns
recurring combinations of those signals so the product can say, "this manager
belongs to this draft archetype" without doing expensive frontend work.

Archetypes are deliberately downstream of discovery and validation:

    state candidates -> profile catalog -> profile assignments -> archetypes

That keeps names and blurbs from guiding the miner. Labels here are generated
from the evidence after a combination earns enough support across the fleet.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from itertools import combinations
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
from multi_league.transformations.draft.profile_catalog import FLEET_CATALOG_DB_NAME

log = make_logger("DRAFT-PROFILE-ARCHETYPES")

MODEL_VERSION = "draft-profile-archetypes-v0.1"

PROFILE_STATUSES = {"catalog_ready", "catalog_watch"}
READY_STATUS = "catalog_ready"

DRAFT_PROFILE_ARCHETYPE_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "archetype_id": "VARCHAR",
    "archetype_kind": "VARCHAR",
    "archetype_status": "VARCHAR",
    "profile_ids_json": "VARCHAR",
    "primary_profile_id": "VARCHAR",
    "profile_count": "INTEGER",
    "scope_count": "INTEGER",
    "league_count": "INTEGER",
    "ready_signal_count": "INTEGER",
    "watch_signal_count": "INTEGER",
    "total_picks": "INTEGER",
    "max_years_seen": "INTEGER",
    "median_assignment_score": "DOUBLE",
    "median_validation_score": "DOUBLE",
    "median_abs_shrunk_z": "DOUBLE",
    "median_value_delta": "DOUBLE",
    "archetype_label": "VARCHAR",
    "archetype_summary": "VARCHAR",
    "evidence_json": "VARCHAR",
    "model_version": "VARCHAR",
}

DRAFT_PROFILE_ARCHETYPE_ASSIGNMENT_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "scope_type": "VARCHAR",
    "scope_key": "VARCHAR",
    "scope_label": "VARCHAR",
    "archetype_id": "VARCHAR",
    "archetype_kind": "VARCHAR",
    "archetype_status": "VARCHAR",
    "archetype_rank": "INTEGER",
    "archetype_score": "DOUBLE",
    "archetype_label": "VARCHAR",
    "archetype_summary": "VARCHAR",
    "matched_profile_ids_json": "VARCHAR",
    "supporting_profile_count": "INTEGER",
    "primary_profile_id": "VARCHAR",
    "primary_feature_type": "VARCHAR",
    "primary_feature_value": "VARCHAR",
    "primary_signal_direction": "VARCHAR",
    "evidence_json": "VARCHAR",
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
    clean = []
    for value in values:
        if value is None:
            continue
        number = _float(value, default=float("nan"))
        if not math.isnan(number):
            clean.append(number)
    return float(statistics.median(clean)) if clean else 0.0


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _scope_id(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("db_name") or ""),
        str(row.get("scope_type") or ""),
        str(row.get("scope_key") or ""),
    )


def _assignment_rows(rows: Iterable[dict[str, Any]], *, include_watch: bool) -> list[dict[str, Any]]:
    allowed = PROFILE_STATUSES if include_watch else {READY_STATUS}
    return [
        row
        for row in rows
        if str(row.get("profile_status") or "") in allowed
        and str(row.get("profile_kind") or "") in {"manager", "league"}
        and str(row.get("profile_id") or "")
    ]


def _profile_signature(profile_ids: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({str(profile_id) for profile_id in profile_ids if str(profile_id or "").strip()}))


def _capital_tier_from_value(value: str) -> str:
    normalized = value.lower()
    if (
        "premium" in normalized
        or "snake_r1_2" in normalized
        or "auction_50_plus" in normalized
        or "auction_30_49" in normalized
    ):
        return "premium"
    if "core" in normalized or "snake_r3_5" in normalized or "auction_16_29" in normalized:
        return "core"
    if "depth" in normalized or "snake_r6_9" in normalized or "auction_6_15" in normalized:
        return "depth"
    if "flyer" in normalized or "snake_r10_plus" in normalized or "auction_1_5" in normalized:
        return "flyer"
    return _slug(value, max_len=24)


def _slot_root(value: str) -> str:
    normalized = str(value or "").upper()
    match = re.match(r"^([A-Z]+)(\d+)", normalized)
    if match:
        pos, raw_slot = match.groups()
        slot = int(raw_slot)
        if slot == 1:
            return f"{pos}_anchor"
        if slot == 2:
            return f"{pos}_second"
        if slot <= 4:
            return f"{pos}_depth"
        return f"{pos}_deep_depth"

    match = re.match(r"^([A-Z]+)_(ANCHOR|SECOND|DEPTH|DEEP_DEPTH)", normalized)
    if match:
        return f"{match.group(1)}_{match.group(2).lower()}"
    return _slug(value, max_len=36)


def _archetype_dimension(row: dict[str, Any]) -> tuple[str, str]:
    feature_type = str(row.get("feature_type") or "")
    feature_value = str(row.get("feature_value") or "")

    if feature_type.startswith("position_slot"):
        root = _slot_root(feature_value)
        if "capital" in feature_type:
            return ("position_slot_capital", f"{root}_{_capital_tier_from_value(feature_value)}")
        return ("position_slot", root)

    if feature_type.startswith("position_capital"):
        pos = feature_value.split("_", 1)[0].upper()
        return ("position_capital", f"{pos}_{_capital_tier_from_value(feature_value)}")

    if feature_type == "format_capital_tier":
        return ("format_capital", _capital_tier_from_value(feature_value))

    return (feature_type, feature_value)


def _semantic_profile_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    """Collapse duplicate lenses for archetype naming.

    A manager can have both a count signal and a capital signal for the same
    player trait. Those are useful as separate evidence rows on the dossier, but
    combining both into one archetype produces labels like "targets rookies +
    targets rookies." The archetype layer keeps the strongest lens per trait.
    """
    feature_type, feature_value = _archetype_dimension(row)
    return (str(row.get("profile_kind") or ""), feature_type, feature_value, str(row.get("signal_direction") or ""))


def _dedupe_semantic_profiles(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = _semantic_profile_key(row)
        current = best_by_key.get(key)
        if current is None:
            best_by_key[key] = row
            continue
        row_score = (_float(row.get("assignment_score")), _float(row.get("validation_score")))
        current_score = (_float(current.get("assignment_score")), _float(current.get("validation_score")))
        if row_score > current_score:
            best_by_key[key] = row
    return list(best_by_key.values())


def archetype_id_for_signature(archetype_kind: str, signature: tuple[str, ...]) -> str:
    raw = "|".join(signature)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{_slug(archetype_kind, max_len=8)}_{len(signature)}_{digest}"


def _display_value(value: Any) -> str:
    raw = str(value or "unknown").strip()
    text = re.sub(r"[_|]+", " ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = []
    keep_upper = {
        "qb",
        "rb",
        "wr",
        "te",
        "k",
        "def",
        "dst",
        "idp",
        "nfl",
        "ras",
        "ppr",
        "adp",
    }
    for token in text.split(" "):
        lower = token.lower()
        if lower in keep_upper:
            tokens.append(lower.upper())
        elif re.fullmatch(r"[a-z]{1,3}\d+", lower):
            tokens.append(lower.upper())
        else:
            tokens.append(lower)
    return " ".join(tokens)


def describe_profile_signal(row: dict[str, Any]) -> str:
    """Return a compact deterministic phrase for a profile signal."""
    value = _display_value(row.get("feature_value"))
    direction = str(row.get("signal_direction") or "").lower()
    metric = str(row.get("signal_metric") or "").lower()
    kind = str(row.get("profile_kind") or row.get("archetype_kind") or "").lower()

    if kind == "league":
        if direction == "underweight":
            return f"market underweights {value}"
        if direction == "overweight":
            return f"market overweights {value}"
        if "inefficiency" in metric:
            return f"market signal: {value}"
        return f"league signal: {value}"

    if direction == "overweight":
        return f"targets {value}"
    if direction == "underweight":
        return f"fades {value}"
    if direction in {"positive", "better", "high"}:
        return f"gets value from {value}"
    if direction in {"negative", "worse", "low"}:
        return f"loses value on {value}"
    return f"signal: {value}"


def _ordered_evidence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_profile: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_profile[str(row.get("profile_id") or "")].append(row)

    evidence = []
    for profile_id, profile_rows in by_profile.items():
        sample = sorted(
            profile_rows,
            key=lambda row: (_float(row.get("assignment_score")), _float(row.get("validation_score"))),
            reverse=True,
        )[0]
        evidence.append(
            {
                "profile_id": profile_id,
                "feature_type": sample.get("feature_type"),
                "feature_value": sample.get("feature_value"),
                "signal_metric": sample.get("signal_metric"),
                "signal_direction": sample.get("signal_direction"),
                "label": describe_profile_signal(sample),
                "scope_count": len({_scope_id(row) for row in profile_rows}),
                "median_assignment_score": round(
                    _median(_float(row.get("assignment_score")) for row in profile_rows), 3
                ),
                "median_validation_score": round(
                    _median(_float(row.get("validation_score")) for row in profile_rows), 3
                ),
            }
        )

    evidence.sort(
        key=lambda row: (
            _int(row.get("scope_count")),
            _float(row.get("median_assignment_score")),
            _float(row.get("median_validation_score")),
        ),
        reverse=True,
    )
    return evidence


def _archetype_label(archetype_kind: str, evidence: list[dict[str, Any]]) -> str:
    labels = [str(row.get("label") or "signal") for row in evidence]
    if not labels:
        return "Discovered profile"
    first = labels[0]
    if len(labels) == 1:
        label = first
    elif len(labels) == 2:
        label = f"{first} + {labels[1]}"
    else:
        label = f"{first} + {labels[1]} + {len(labels) - 2} more"
    prefix = "League: " if archetype_kind == "league" else "Manager: "
    return prefix + label[:140]


def _archetype_summary(archetype_kind: str, scope_count: int, league_count: int, evidence: list[dict[str, Any]]) -> str:
    unit = "leagues" if archetype_kind == "league" else "managers"
    place = "across the fleet" if archetype_kind == "league" else f"across {league_count} leagues"
    top = "; ".join(str(row.get("label")) for row in evidence[:3] if row.get("label"))
    if not top:
        top = "validated draft signals"
    return f"{scope_count} {unit} {place} share this pattern. Evidence: {top}."


def learn_profile_archetypes(
    assignment_rows: Iterable[dict[str, Any]],
    *,
    include_watch: bool = True,
    min_manager_scopes: int = 4,
    min_league_scopes: int = 4,
    min_ready_share: float = 0.40,
    max_profiles_per_scope: int = 5,
    max_profile_combo_size: int = 3,
    manager_archetype_limit: int = 200,
    league_archetype_limit: int = 50,
) -> list[dict[str, Any]]:
    """Learn recurring multi-profile archetypes from profile assignments."""
    scoped_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _assignment_rows(assignment_rows, include_watch=include_watch):
        scoped_rows[_scope_id(row)].append(row)

    grouped: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = defaultdict(list)
    for rows in scoped_rows.values():
        ordered = sorted(
            _dedupe_semantic_profiles(rows),
            key=lambda row: (
                _int(row.get("assignment_rank"), 999),
                -_float(row.get("assignment_score")),
                str(row.get("profile_id") or ""),
            ),
        )[:max_profiles_per_scope]
        by_id = {str(row.get("profile_id")): row for row in ordered}
        ids = list(by_id)
        max_size = min(max_profile_combo_size, len(ids))
        for size in range(1, max_size + 1):
            for combo in combinations(ids, size):
                signature = _profile_signature(combo)
                if not signature:
                    continue
                archetype_kind = str(by_id[signature[0]].get("profile_kind") or "unknown")
                grouped[(archetype_kind, signature)].extend(by_id[profile_id] for profile_id in signature)

    archetypes: list[dict[str, Any]] = []
    for (archetype_kind, signature), rows in grouped.items():
        scope_ids = {_scope_id(row) for row in rows}
        leagues = {scope[0] for scope in scope_ids if scope[0]}
        scope_count = len(scope_ids)
        min_scopes = min_league_scopes if archetype_kind == "league" else min_manager_scopes
        if scope_count < min_scopes:
            continue

        ready_signal_count = sum(1 for row in rows if row.get("profile_status") == READY_STATUS)
        watch_signal_count = sum(1 for row in rows if row.get("profile_status") == "catalog_watch")
        ready_share = ready_signal_count / max(len(rows), 1)
        archetype_status = "archetype_ready" if ready_share >= min_ready_share else "archetype_watch"
        if archetype_status == "archetype_watch" and not include_watch:
            continue

        evidence = _ordered_evidence(rows)
        primary_profile_id = str(evidence[0]["profile_id"]) if evidence else signature[0]
        archetypes.append(
            {
                "db_name": FLEET_CATALOG_DB_NAME,
                "archetype_id": archetype_id_for_signature(archetype_kind, signature),
                "archetype_kind": archetype_kind,
                "archetype_status": archetype_status,
                "profile_ids_json": _json(list(signature)),
                "primary_profile_id": primary_profile_id,
                "profile_count": len(signature),
                "scope_count": scope_count,
                "league_count": len(leagues),
                "ready_signal_count": ready_signal_count,
                "watch_signal_count": watch_signal_count,
                "total_picks": sum(_int(row.get("picks")) for row in rows),
                "max_years_seen": max((_int(row.get("years_seen")) for row in rows), default=0),
                "median_assignment_score": round(_median(_float(row.get("assignment_score")) for row in rows), 3),
                "median_validation_score": round(_median(_float(row.get("validation_score")) for row in rows), 3),
                "median_abs_shrunk_z": round(_median(abs(_float(row.get("shrunk_z_score"))) for row in rows), 3),
                "median_value_delta": round(_median(_float(row.get("value_delta")) for row in rows), 4),
                "archetype_label": _archetype_label(archetype_kind, evidence),
                "archetype_summary": _archetype_summary(archetype_kind, scope_count, len(leagues), evidence),
                "evidence_json": _json(evidence),
                "model_version": MODEL_VERSION,
            }
        )

    def sort_key(row: dict[str, Any]) -> tuple[int, int, int, float, float]:
        return (
            1 if row["archetype_status"] == "archetype_ready" else 0,
            _int(row.get("profile_count")),
            _int(row.get("scope_count")),
            _float(row.get("median_assignment_score")),
            _float(row.get("median_validation_score")),
        )

    manager_rows = [row for row in archetypes if row["archetype_kind"] == "manager"]
    league_rows = [row for row in archetypes if row["archetype_kind"] == "league"]
    other_rows = [row for row in archetypes if row["archetype_kind"] not in {"manager", "league"}]
    manager_rows = sorted(manager_rows, key=sort_key, reverse=True)[:manager_archetype_limit]
    league_rows = sorted(league_rows, key=sort_key, reverse=True)[:league_archetype_limit]
    return [*manager_rows, *league_rows, *sorted(other_rows, key=sort_key, reverse=True)]


def _load_profile_ids(profile_ids_json: Any) -> set[str]:
    if isinstance(profile_ids_json, list | tuple | set):
        return {str(value) for value in profile_ids_json}
    try:
        value = json.loads(str(profile_ids_json or "[]"))
    except json.JSONDecodeError:
        value = []
    return {str(item) for item in value if str(item or "").strip()}


def assign_profile_archetypes(
    assignment_rows: Iterable[dict[str, Any]],
    archetype_rows: Iterable[dict[str, Any]],
    *,
    include_watch: bool = True,
    max_archetypes_per_scope: int = 3,
) -> list[dict[str, Any]]:
    """Assign learned archetypes back to each manager or league scope."""
    scoped_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _assignment_rows(assignment_rows, include_watch=include_watch):
        scoped_rows[_scope_id(row)].append(row)

    archetypes = list(archetype_rows)
    archetype_profiles = {
        row["archetype_id"]: _load_profile_ids(row.get("profile_ids_json"))
        for row in archetypes
        if row.get("archetype_id")
    }

    assignments: list[dict[str, Any]] = []
    for scope, rows in scoped_rows.items():
        rows_by_profile = {str(row.get("profile_id")): row for row in rows if row.get("profile_id")}
        scope_profiles = set(rows_by_profile)
        candidates = []
        for archetype in archetypes:
            profile_ids = archetype_profiles.get(str(archetype.get("archetype_id")), set())
            if not profile_ids or not profile_ids.issubset(scope_profiles):
                continue
            matched_rows = [rows_by_profile[profile_id] for profile_id in profile_ids]
            row_score = _median(_float(row.get("assignment_score")) for row in matched_rows)
            catalog_score = _float(archetype.get("median_assignment_score"))
            combo_bonus = min(len(profile_ids), 3) * 4.0
            score = (0.64 * row_score) + (0.28 * catalog_score) + combo_bonus
            candidates.append((profile_ids, matched_rows, archetype, score))

        candidates.sort(
            key=lambda item: (
                len(item[0]),
                item[3],
                _int(item[2].get("scope_count")),
            ),
            reverse=True,
        )

        selected_sets: list[set[str]] = []
        rank = 0
        for profile_ids, matched_rows, archetype, score in candidates:
            if any(profile_ids.issubset(existing) for existing in selected_sets):
                continue
            rank += 1
            selected_sets.append(set(profile_ids))
            evidence = _ordered_evidence(matched_rows)
            primary = sorted(
                matched_rows,
                key=lambda row: (_float(row.get("assignment_score")), _float(row.get("validation_score"))),
                reverse=True,
            )[0]
            assignments.append(
                {
                    "db_name": scope[0],
                    "scope_type": scope[1],
                    "scope_key": scope[2],
                    "scope_label": primary.get("scope_label"),
                    "archetype_id": archetype.get("archetype_id"),
                    "archetype_kind": archetype.get("archetype_kind"),
                    "archetype_status": archetype.get("archetype_status"),
                    "archetype_rank": rank,
                    "archetype_score": round(score, 3),
                    "archetype_label": archetype.get("archetype_label"),
                    "archetype_summary": archetype.get("archetype_summary"),
                    "matched_profile_ids_json": _json(sorted(profile_ids)),
                    "supporting_profile_count": len(profile_ids),
                    "primary_profile_id": primary.get("profile_id"),
                    "primary_feature_type": primary.get("feature_type"),
                    "primary_feature_value": primary.get("feature_value"),
                    "primary_signal_direction": primary.get("signal_direction"),
                    "evidence_json": _json(evidence),
                    "model_version": MODEL_VERSION,
                }
            )
            if rank >= max_archetypes_per_scope:
                break
    return assignments


def create_draft_profile_archetype_tables(conn) -> None:
    """Create or migrate profile archetype catalog and assignment tables."""
    configure_table_catalog(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    for table_name, columns in {
        "draft_profile_archetype_catalog": DRAFT_PROFILE_ARCHETYPE_COLUMNS,
        "draft_profile_archetype_assignment": DRAFT_PROFILE_ARCHETYPE_ASSIGNMENT_COLUMNS,
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


def _fetch_rows(conn, table_name: str, *, db_name: str | None = None) -> list[dict[str, Any]]:
    where = f"WHERE db_name = {_q(db_name)}" if db_name else ""
    rows = conn.execute(f"SELECT * FROM {central_table(table_name)} {where}").fetchall()
    cols = [desc[0] for desc in conn.description]
    return [dict(zip(cols, row)) for row in rows]


def _existing_archetype_assignment_db_names(conn) -> set[str]:
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT db_name
            FROM {central_table('draft_profile_archetype_assignment')}
            WHERE db_name IS NOT NULL AND TRIM(db_name) != ''
            """
        ).fetchall()
    except Exception:
        return set()
    return {str(row[0]) for row in rows if row and row[0]}


def replace_profile_archetypes_and_assignments(
    conn,
    *,
    db_name: str | None = None,
    include_watch: bool = True,
    min_manager_scopes: int = 4,
    min_league_scopes: int = 4,
    manager_archetype_limit: int = 200,
    league_archetype_limit: int = 50,
) -> dict[str, int]:
    """Rebuild archetype catalog and scoped archetype assignments."""
    create_draft_profile_archetype_tables(conn)
    all_profile_assignments = _fetch_rows(conn, "draft_profile_assignment")
    scoped_profile_assignments = [
        row for row in all_profile_assignments if db_name is None or str(row.get("db_name") or "") == str(db_name)
    ]
    archetypes = learn_profile_archetypes(
        all_profile_assignments,
        include_watch=include_watch,
        min_manager_scopes=min_manager_scopes,
        min_league_scopes=min_league_scopes,
        manager_archetype_limit=manager_archetype_limit,
        league_archetype_limit=league_archetype_limit,
    )
    assignments = assign_profile_archetypes(scoped_profile_assignments, archetypes, include_watch=include_watch)

    _insert_scoped_rows(
        conn,
        table_name="draft_profile_archetype_catalog",
        temp_table="tmp_draft_profile_archetype_catalog",
        columns=DRAFT_PROFILE_ARCHETYPE_COLUMNS,
        rows=archetypes,
        db_names=[FLEET_CATALOG_DB_NAME],
    )

    if db_name is not None:
        assignment_db_names = {str(db_name)}
    else:
        assignment_db_names = {
            str(row.get("db_name"))
            for row in scoped_profile_assignments
            if row.get("db_name") is not None and str(row.get("db_name")).strip()
        } | _existing_archetype_assignment_db_names(conn)

    _insert_scoped_rows(
        conn,
        table_name="draft_profile_archetype_assignment",
        temp_table="tmp_draft_profile_archetype_assignment",
        columns=DRAFT_PROFILE_ARCHETYPE_ASSIGNMENT_COLUMNS,
        rows=assignments,
        db_names=sorted(assignment_db_names),
    )
    return {"archetypes": len(archetypes), "assignments": len(assignments)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build draft profile archetypes from profile assignments.")
    parser.add_argument("--db", help="Optional single db_name for assignment-only/local checks")
    parser.add_argument("--catalog-db", default="___leagues", help="Local DuckDB file/catalog that holds fleet tables")
    parser.add_argument("--data-dir", help="Use local DuckDB instead of Fly read API")
    parser.add_argument("--min-manager-scopes", type=int, default=4)
    parser.add_argument("--min-league-scopes", type=int, default=4)
    parser.add_argument("--manager-archetype-limit", type=int, default=200)
    parser.add_argument("--league-archetype-limit", type=int, default=50)
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.data_dir:
        raise RuntimeError("Profile archetype writes require a local/worker DuckDB connection")

    connection_db = args.db or args.catalog_db
    conn = get_pipeline_connection(connection_db, data_dir=args.data_dir, qualified=True)
    try:
        result = replace_profile_archetypes_and_assignments(
            conn,
            db_name=args.db,
            include_watch=not args.ready_only,
            min_manager_scopes=args.min_manager_scopes,
            min_league_scopes=args.min_league_scopes,
            manager_archetype_limit=args.manager_archetype_limit,
            league_archetype_limit=args.league_archetype_limit,
        )
    finally:
        conn.close()

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        log(f"Built {result['archetypes']} archetypes and {result['assignments']} assignments")
    return 0


if __name__ == "__main__":
    sys.exit(main())
