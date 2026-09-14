#!/usr/bin/env python3
"""Validate mined draft tendencies for state-machine promotion.

This is the bridge between discovery and product behavior. The miner may find
hundreds of statistically interesting candidates; this gate applies shrinkage,
repeatability, and feature-family priors before anything can become a promoted
draft intelligence state.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
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
    resolve_db_name,
)
from multi_league.transformations.draft.state_registry import classify_feature
from multi_league.transformations.draft.tendency_miner import (
    DRAFT_TENDENCY_COLUMNS,
    create_draft_tendency_table,
)
from multi_league.transformations.draft.league_inefficiency_miner import (
    DRAFT_INEFFICIENCY_COLUMNS,
    create_draft_inefficiency_table,
)

log = make_logger("DRAFT-STATE")

MODEL_VERSION = "draft-state-validation-v0.2"
STATE_READY_Q_MAX = 0.05
BRIEFING_WATCH_Q_MAX = 0.20
STATE_READY_CAP_BY_SCOPE = {
    "manager": 4,
    "league_inefficiency": 2,
}
BRIEFING_WATCH_CAP_BY_SCOPE = {
    "manager": 8,
    "league_inefficiency": 6,
}

DRAFT_STATE_COLUMNS: dict[str, str] = {
    "db_name": "VARCHAR",
    "scope_type": "VARCHAR",
    "scope_key": "VARCHAR",
    "scope_label": "VARCHAR",
    "state_key": "VARCHAR",
    "state_family": "VARCHAR",
    "feature_type": "VARCHAR",
    "feature_value": "VARCHAR",
    "signal_metric": "VARCHAR",
    "signal_direction": "VARCHAR",
    "promotion_level": "VARCHAR",
    "promotion_rank": "INTEGER",
    "validation_status": "VARCHAR",
    "validation_score": "DOUBLE",
    "p_value": "DOUBLE",
    "q_value": "DOUBLE",
    "repeatability": "DOUBLE",
    "sample_reliability": "DOUBLE",
    "shrinkage_alpha": "DOUBLE",
    "shrunk_z_score": "DOUBLE",
    "shrunk_lift": "DOUBLE",
    "shrunk_excess_capital": "DOUBLE",
    "observed_value": "DOUBLE",
    "expected_value": "DOUBLE",
    "value_delta": "DOUBLE",
    "pick_score_delta": "DOUBLE",
    "picks": "INTEGER",
    "years_seen": "INTEGER",
    "earliest_year": "INTEGER",
    "latest_year": "INTEGER",
    "recency_weight": "DOUBLE",
    "observed_capital": "DOUBLE",
    "expected_capital": "DOUBLE",
    "excess_capital": "DOUBLE",
    "observed_share": "DOUBLE",
    "expected_share": "DOUBLE",
    "lift": "DOUBLE",
    "capital_z_score": "DOUBLE",
    "confidence": "VARCHAR",
    "evidence_level": "VARCHAR",
    "source_model_version": "VARCHAR",
    "registry_version": "VARCHAR",
    "model_version": "VARCHAR",
}


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
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


def _two_sided_normal_p(abs_z: float) -> float:
    """Return two-sided normal-tail p-value using stdlib math only."""
    return max(0.0, min(1.0, math.erfc(abs_z / math.sqrt(2.0))))


def _benjamini_hochberg(p_values: list[float]) -> list[float]:
    """Return Benjamini-Hochberg q-values in input order."""
    if not p_values:
        return []

    indexed = sorted(enumerate(p_values), key=lambda item: item[1])
    m = float(len(indexed))
    q_values = [1.0] * len(indexed)
    running_min = 1.0
    for rank, (original_index, p_value) in reversed(list(enumerate(indexed, start=1))):
        adjusted = min(1.0, p_value * m / rank)
        running_min = min(running_min, adjusted)
        q_values[original_index] = running_min
    return q_values


def create_draft_state_table(conn) -> None:
    """Create or migrate the validated state candidate table."""
    configure_table_catalog(conn)
    columns_sql = ",\n        ".join(f"{name} {dtype}" for name, dtype in DRAFT_STATE_COLUMNS.items())
    conn.execute(f"""
        CREATE SCHEMA IF NOT EXISTS public;
        CREATE TABLE IF NOT EXISTS {central_table("draft_intelligence_state_candidate")} (
            {columns_sql}
        )
    """)
    for name, dtype in DRAFT_STATE_COLUMNS.items():
        conn.execute(
            f"ALTER TABLE {central_table('draft_intelligence_state_candidate')} "
            f"ADD COLUMN IF NOT EXISTS {name} {dtype}"
        )


def _repeatability(row: dict[str, Any], direction: str, years_seen: int) -> float:
    explicit = row.get("repeatability")
    if explicit is not None:
        return max(0.0, min(1.0, _float(explicit)))
    if years_seen <= 0:
        return 0.0

    positive_years = _int(row.get("positive_years"))
    negative_years = _int(row.get("negative_years"))
    if positive_years or negative_years:
        matching_years = positive_years if direction == "overweight" else negative_years
        return max(0.0, min(1.0, matching_years / years_seen))

    return 0.25 if years_seen == 1 else 0.50


def _recency_weight(row: dict[str, Any]) -> float:
    """Return candidate recency weight, neutral when older miners lack it."""
    explicit = row.get("recency_weight")
    if explicit is None:
        return 1.0
    return max(0.0, min(1.0, _float(explicit, 1.0)))


def _recency_reliability(row: dict[str, Any], recency_weight: float) -> float:
    """Return the recency multiplier used for shrinkage.

    Manager behavior can change abruptly, so stale manager evidence should be
    discounted hard. League inefficiencies are room/format effects; if they
    repeat for many seasons, old evidence should still count, just with a
    dampener so recent markets float to the top.
    """
    if str(row.get("scope_type") or "") == "league_inefficiency":
        return 0.55 + (0.45 * recency_weight)
    return recency_weight


def _validate_candidate(
    row: dict[str, Any],
    *,
    signal_metric: str,
    z_field: str,
    observed_value_field: str,
    expected_value_field: str,
    delta_field: str,
) -> dict[str, Any]:
    """Apply deterministic promotion gates to one normalized discovery row."""
    feature_type = str(row.get("feature_type") or "unknown")
    feature_value = str(row.get("feature_value") or "unknown")
    metadata = classify_feature(feature_type, feature_value)

    z_score = _float(row.get(z_field))
    lift = _float(row.get("lift"), 1.0)
    excess_capital = _float(row.get("excess_capital"))
    observed_capital = max(0.0, _float(row.get("observed_capital")))
    expected_capital = max(0.0, _float(row.get("expected_capital")))
    if expected_capital == 0:
        evidence_weight = observed_capital
    else:
        evidence_weight = observed_capital + expected_capital
    picks = _int(row.get("picks"))
    years_seen = _int(row.get("years_seen"))
    direction = "overweight" if z_score >= 0 else "underweight"
    repeatability = _repeatability(row, direction, years_seen)
    recency_weight = _recency_weight(row)
    recency_reliability = _recency_reliability(row, recency_weight)

    prior_weight = _float(metadata["prior_weight"], 70.0)
    capital_reliability = evidence_weight / (evidence_weight + prior_weight) if evidence_weight > 0 else 0.0
    year_reliability = min(1.0, years_seen / max(_int(metadata["ready_years"], 3), 1))
    pick_reliability = min(1.0, picks / max(_int(metadata["ready_picks"], 8), 1))
    shrinkage_alpha = max(
        0.0,
        min(1.0, capital_reliability * year_reliability * pick_reliability * recency_reliability),
    )

    shrunk_z = z_score * shrinkage_alpha
    shrunk_lift = 1.0 + ((lift - 1.0) * shrinkage_alpha)
    shrunk_excess = excess_capital * shrinkage_alpha
    abs_shrunk_z = abs(shrunk_z)
    p_value = _two_sided_normal_p(abs(z_score))

    ready = (
        years_seen >= _int(metadata["ready_years"])
        and picks >= _int(metadata["ready_picks"])
        and repeatability >= _float(metadata["ready_repeatability"])
        and abs_shrunk_z >= _float(metadata["ready_abs_z"])
    )
    watch = (
        years_seen >= _int(metadata["watch_years"])
        and picks >= _int(metadata["watch_picks"])
        and repeatability >= _float(metadata["watch_repeatability"])
        and abs_shrunk_z >= _float(metadata["watch_abs_z"])
    )

    if ready:
        promotion_level = "state_ready"
        validation_status = "validated"
    elif watch:
        promotion_level = "briefing_watch"
        validation_status = "watch"
    else:
        promotion_level = "explore_only"
        validation_status = "needs_more_evidence"

    z_component = min(1.0, abs_shrunk_z / max(_float(metadata["ready_abs_z"]), 0.01))
    repeat_component = min(1.0, repeatability / max(_float(metadata["ready_repeatability"]), 0.01))
    years_component = min(1.0, years_seen / max(_int(metadata["ready_years"]), 1))
    picks_component = min(1.0, picks / max(_int(metadata["ready_picks"]), 1))
    recency_component = recency_weight
    validation_score = 100.0 * (
        0.38 * z_component
        + 0.22 * repeat_component
        + 0.13 * years_component
        + 0.13 * picks_component
        + 0.14 * recency_component
    )

    return {
        "db_name": row.get("db_name"),
        "scope_type": row.get("scope_type"),
        "scope_key": row.get("scope_key"),
        "scope_label": row.get("scope_label"),
        "state_key": f"{signal_metric}:{metadata['state_key']}",
        "state_family": metadata["state_family"],
        "feature_type": feature_type,
        "feature_value": feature_value,
        "signal_metric": signal_metric,
        "signal_direction": direction,
        "promotion_level": promotion_level,
        "promotion_rank": None,
        "validation_status": validation_status,
        "validation_score": round(validation_score, 3),
        "p_value": round(p_value, 8),
        "q_value": round(p_value, 8),
        "repeatability": round(repeatability, 4),
        "sample_reliability": round(shrinkage_alpha, 4),
        "shrinkage_alpha": round(shrinkage_alpha, 4),
        "shrunk_z_score": round(shrunk_z, 3),
        "shrunk_lift": round(shrunk_lift, 4),
        "shrunk_excess_capital": round(shrunk_excess, 3),
        "observed_value": _float(row.get(observed_value_field)),
        "expected_value": _float(row.get(expected_value_field)),
        "value_delta": _float(row.get(delta_field)),
        "pick_score_delta": _float(row.get("pick_score_delta")),
        "picks": picks,
        "years_seen": years_seen,
        "earliest_year": _int(row.get("earliest_year")) or None,
        "latest_year": _int(row.get("latest_year")) or None,
        "recency_weight": round(recency_weight, 4),
        "observed_capital": round(observed_capital, 3),
        "expected_capital": round(expected_capital, 3),
        "excess_capital": round(excess_capital, 3),
        "observed_share": _float(row.get("observed_share")),
        "expected_share": _float(row.get("expected_share")),
        "lift": lift,
        "capital_z_score": z_score,
        "confidence": row.get("confidence"),
        "evidence_level": row.get("evidence_level"),
        "source_model_version": row.get("model_version"),
        "registry_version": metadata["registry_version"],
        "model_version": MODEL_VERSION,
    }


def validate_tendency_candidate(row: dict[str, Any]) -> dict[str, Any]:
    """Apply promotion gates to one manager affinity candidate."""
    return _validate_candidate(
        row,
        signal_metric="capital_affinity",
        z_field="capital_z_score",
        observed_value_field="observed_share",
        expected_value_field="expected_share",
        delta_field="excess_capital",
    )


def validate_inefficiency_candidate(row: dict[str, Any]) -> dict[str, Any]:
    """Apply promotion gates to one league market inefficiency candidate."""
    return _validate_candidate(
        row,
        signal_metric="value_inefficiency",
        z_field="value_z_score",
        observed_value_field="observed_residual",
        expected_value_field="expected_residual",
        delta_field="excess_residual",
    )


def validate_tendency_candidates(
    rows: Iterable[dict[str, Any]],
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Validate mined tendency rows and return state-machine candidates."""
    validated = [validate_tendency_candidate(row) for row in rows]
    _apply_fdr_gate(validated)
    if include_explore:
        return validated
    return [row for row in validated if row["promotion_level"] != "explore_only"]


def validate_inefficiency_candidates(
    rows: Iterable[dict[str, Any]],
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Validate league inefficiency rows and return state-machine candidates."""
    validated = [validate_inefficiency_candidate(row) for row in rows]
    _apply_fdr_gate(validated)
    if include_explore:
        return validated
    return [row for row in validated if row["promotion_level"] != "explore_only"]


def _apply_fdr_gate(rows: list[dict[str, Any]]) -> None:
    """Apply multiple-comparison control after row-level validation."""
    if not rows:
        return

    q_values = _benjamini_hochberg([_float(row.get("p_value"), 1.0) for row in rows])
    for row, q_value in zip(rows, q_values):
        row["q_value"] = round(q_value, 8)
        level = row["promotion_level"]
        if level == "state_ready" and q_value > STATE_READY_Q_MAX:
            if q_value <= BRIEFING_WATCH_Q_MAX:
                row["promotion_level"] = "briefing_watch"
                row["validation_status"] = "watch_fdr_demoted"
            else:
                row["promotion_level"] = "explore_only"
                row["validation_status"] = "fdr_rejected"
        elif level == "briefing_watch" and q_value > BRIEFING_WATCH_Q_MAX:
            row["promotion_level"] = "explore_only"
            row["validation_status"] = "fdr_rejected"
    _apply_capacity_gate(rows)


def _promotion_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    return (
        _float(row.get("validation_score")),
        abs(_float(row.get("shrunk_z_score"))),
        _float(row.get("repeatability")),
        _float(row.get("recency_weight"), 1.0),
        -_float(row.get("q_value"), 1.0),
    )


def _apply_capacity_gate(rows: list[dict[str, Any]]) -> None:
    """Keep each state-machine scope compact enough to be useful."""
    grouped: dict[tuple[Any, Any, Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("db_name"), row.get("scope_type"), row.get("scope_key"), row.get("signal_metric"))
        grouped.setdefault(key, []).append(row)

    for (_, scope_type, _, _), scope_rows in grouped.items():
        ready_cap = STATE_READY_CAP_BY_SCOPE.get(str(scope_type), 4)
        watch_cap = BRIEFING_WATCH_CAP_BY_SCOPE.get(str(scope_type), 8)
        ready_seen = 0
        watch_seen = 0
        rank = 0

        for row in sorted(scope_rows, key=_promotion_sort_key, reverse=True):
            level = row["promotion_level"]
            if level == "state_ready":
                if ready_seen < ready_cap:
                    ready_seen += 1
                    rank += 1
                    row["promotion_rank"] = rank
                elif watch_seen < watch_cap:
                    watch_seen += 1
                    rank += 1
                    row["promotion_level"] = "briefing_watch"
                    row["promotion_rank"] = rank
                    row["validation_status"] = "watch_capacity_demoted"
                else:
                    row["promotion_level"] = "explore_only"
                    row["promotion_rank"] = None
                    row["validation_status"] = "capacity_suppressed"
            elif level == "briefing_watch":
                if watch_seen < watch_cap:
                    watch_seen += 1
                    rank += 1
                    row["promotion_rank"] = rank
                else:
                    row["promotion_level"] = "explore_only"
                    row["promotion_rank"] = None
                    row["validation_status"] = "capacity_suppressed"
            else:
                row["promotion_rank"] = None


def run_tendency_validation(
    conn,
    db_name: str,
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Read one league's mined candidates and validate them."""
    configure_table_catalog(conn)
    create_draft_tendency_table(conn)
    columns = ", ".join(DRAFT_TENDENCY_COLUMNS)
    result = conn.execute(
        f"""
        SELECT {columns}
        FROM {central_table('draft_tendency_candidate')}
        WHERE db_name = {_q(db_name)}
        """
    )
    cols = [desc[0] for desc in result.description]
    rows = [dict(zip(cols, row)) for row in result.fetchall()]
    return validate_tendency_candidates(rows, include_explore=include_explore)


def run_tendency_validation_fly(
    db_name: str,
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Validate candidates from the read-only Fly API."""
    from multi_league.core.readers.fly_reader import FlyReader

    columns = ", ".join(DRAFT_TENDENCY_COLUMNS)
    rows = FlyReader().query(
        f"""
        SELECT {columns}
        FROM ___leagues.public.draft_tendency_candidate
        WHERE db_name = {_q(db_name)}
        """,
        database="___leagues",
    )
    return validate_tendency_candidates(rows, include_explore=include_explore)


def run_inefficiency_validation(
    conn,
    db_name: str,
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Read one league's inefficiency candidates and validate them."""
    configure_table_catalog(conn)
    create_draft_inefficiency_table(conn)
    columns = ", ".join(DRAFT_INEFFICIENCY_COLUMNS)
    result = conn.execute(
        f"""
        SELECT {columns}
        FROM {central_table('draft_inefficiency_candidate')}
        WHERE db_name = {_q(db_name)}
        """
    )
    cols = [desc[0] for desc in result.description]
    rows = [dict(zip(cols, row)) for row in result.fetchall()]
    return validate_inefficiency_candidates(rows, include_explore=include_explore)


def run_inefficiency_validation_fly(
    db_name: str,
    *,
    include_explore: bool = True,
) -> list[dict[str, Any]]:
    """Validate league inefficiency candidates from the read-only Fly API."""
    from multi_league.core.readers.fly_reader import FlyReader

    columns = ", ".join(DRAFT_INEFFICIENCY_COLUMNS)
    rows = FlyReader().query(
        f"""
        SELECT {columns}
        FROM ___leagues.public.draft_inefficiency_candidate
        WHERE db_name = {_q(db_name)}
        """,
        database="___leagues",
    )
    return validate_inefficiency_candidates(rows, include_explore=include_explore)


def replace_validated_tendency_states(
    conn,
    db_name: str,
    *,
    include_explore: bool = True,
) -> int:
    """Replace one league's validated state candidates."""
    configure_table_catalog(conn)
    create_draft_state_table(conn)
    rows = run_tendency_validation(conn, db_name, include_explore=include_explore)

    temp_table = "tmp_draft_intelligence_state_candidate"
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    columns_sql = ", ".join(f"{name} {dtype}" for name, dtype in DRAFT_STATE_COLUMNS.items())
    conn.execute(f"CREATE TEMP TABLE {temp_table} ({columns_sql})")
    if rows:
        insert_sql = f"INSERT INTO {temp_table} ({', '.join(DRAFT_STATE_COLUMNS)}) VALUES ({', '.join(['?'] * len(DRAFT_STATE_COLUMNS))})"
        conn.executemany(insert_sql, [[row.get(col) for col in DRAFT_STATE_COLUMNS] for row in rows])

    execute_scoped(
        conn,
        f"DELETE FROM {central_table('draft_intelligence_state_candidate')} WHERE db_name = {_q(db_name)}",
        db_name,
        label="draft_intelligence_state_candidate:delete",
    )
    if rows:
        execute_scoped(
            conn,
            f"""
            INSERT INTO {central_table('draft_intelligence_state_candidate')} ({', '.join(DRAFT_STATE_COLUMNS)})
            SELECT {', '.join(DRAFT_STATE_COLUMNS)}
            FROM {temp_table}
            WHERE db_name = {_q(db_name)}
            """,
            db_name,
            label="draft_intelligence_state_candidate:insert",
        )
    return conn.execute(
        f"SELECT COUNT(*) FROM {central_table('draft_intelligence_state_candidate')} WHERE db_name = {_q(db_name)}"
    ).fetchone()[0]


def replace_validated_inefficiency_states(
    conn,
    db_name: str,
    *,
    include_explore: bool = True,
) -> int:
    """Replace one league's validated league-inefficiency state candidates."""
    configure_table_catalog(conn)
    create_draft_state_table(conn)
    rows = run_inefficiency_validation(conn, db_name, include_explore=include_explore)

    temp_table = "tmp_draft_inefficiency_state_candidate"
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    columns_sql = ", ".join(f"{name} {dtype}" for name, dtype in DRAFT_STATE_COLUMNS.items())
    conn.execute(f"CREATE TEMP TABLE {temp_table} ({columns_sql})")
    if rows:
        insert_sql = f"INSERT INTO {temp_table} ({', '.join(DRAFT_STATE_COLUMNS)}) VALUES ({', '.join(['?'] * len(DRAFT_STATE_COLUMNS))})"
        conn.executemany(insert_sql, [[row.get(col) for col in DRAFT_STATE_COLUMNS] for row in rows])

    execute_scoped(
        conn,
        f"""
        DELETE FROM {central_table('draft_intelligence_state_candidate')}
        WHERE db_name = {_q(db_name)} AND signal_metric = 'value_inefficiency'
        """,
        db_name,
        label="draft_intelligence_state_candidate:delete-inefficiency",
    )
    if rows:
        execute_scoped(
            conn,
            f"""
            INSERT INTO {central_table('draft_intelligence_state_candidate')} ({', '.join(DRAFT_STATE_COLUMNS)})
            SELECT {', '.join(DRAFT_STATE_COLUMNS)}
            FROM {temp_table}
            WHERE db_name = {_q(db_name)}
            """,
            db_name,
            label="draft_intelligence_state_candidate:insert-inefficiency",
        )
    return conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {central_table('draft_intelligence_state_candidate')}
        WHERE db_name = {_q(db_name)} AND signal_metric = 'value_inefficiency'
        """
    ).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate mined draft tendencies for state-machine promotion.")
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--db", help="League db_name")
    parser.add_argument("--data-dir", default=None, help="Local data dir for DuckDB-backed runs")
    parser.add_argument(
        "--write", action="store_true", help="Write validated states to draft_intelligence_state_candidate"
    )
    parser.add_argument(
        "--source",
        choices=["tendency", "inefficiency", "all"],
        default="tendency",
        help="Which discovery candidate table to validate",
    )
    parser.add_argument(
        "--promoted-only", action="store_true", help="Exclude explore-only candidates from output/write"
    )
    parser.add_argument("--json", action="store_true", help="Print rows as JSON")
    args = parser.parse_args()

    db_name, _ = resolve_db_name(args)
    include_explore = not args.promoted_only

    conn = None
    try:
        if args.write or args.data_dir:
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            if args.write:
                counts = {}
                if args.source in {"tendency", "all"}:
                    counts["tendency"] = replace_validated_tendency_states(
                        conn, db_name, include_explore=include_explore
                    )
                if args.source in {"inefficiency", "all"}:
                    counts["inefficiency"] = replace_validated_inefficiency_states(
                        conn, db_name, include_explore=include_explore
                    )
                count = sum(counts.values())
                log(f"wrote {count} validated draft intelligence states for {db_name}: {counts}")
                return 0
            rows = []
            if args.source in {"tendency", "all"}:
                rows.extend(run_tendency_validation(conn, db_name, include_explore=include_explore))
            if args.source in {"inefficiency", "all"}:
                rows.extend(run_inefficiency_validation(conn, db_name, include_explore=include_explore))
        else:
            rows = []
            if args.source in {"tendency", "all"}:
                rows.extend(run_tendency_validation_fly(db_name, include_explore=include_explore))
            if args.source in {"inefficiency", "all"}:
                rows.extend(run_inefficiency_validation_fly(db_name, include_explore=include_explore))

        if args.json:
            print(json.dumps(rows, indent=2, default=str))
        else:
            for row in rows:
                print(
                    f"{row['scope_label']}: {row['promotion_level']} {row['state_key']} "
                    f"dir={row['signal_direction']} shrunk_z={row['shrunk_z_score']} "
                    f"repeat={row['repeatability']} score={row['validation_score']}"
                )
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
