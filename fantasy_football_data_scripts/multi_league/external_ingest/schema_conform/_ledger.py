"""DDL + writers for staging.schema_decisions, staging.external_identity_overrides,
and staging.import_run_summary."""

from __future__ import annotations
import json
from typing import Any

import duckdb


SCHEMA_DECISIONS_DDL = """
CREATE TABLE IF NOT EXISTS staging.schema_decisions (
    db_name              VARCHAR NOT NULL,
    run_id               VARCHAR NOT NULL,
    run_timestamp        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    table_name           VARCHAR NOT NULL,
    source_col           VARCHAR,
    ddl_slot             VARCHAR,
    score                DOUBLE,
    score_components     JSON,
    second_best_score    DOUBLE,
    second_best_slot     VARCHAR,
    pass_number          INTEGER,
    was_derived          BOOLEAN,
    derivation_rule      VARCHAR,
    was_coalesced        BOOLEAN,
    coalesce_inputs      VARCHAR[],
    status               VARCHAR NOT NULL,
    invariant_failures   VARCHAR[],
    sample_values        VARCHAR[]
);
"""

EXTERNAL_IDENTITY_OVERRIDES_DDL = """
CREATE TABLE IF NOT EXISTS staging.external_identity_overrides (
    db_name                VARCHAR NOT NULL,
    source_manager_norm    VARCHAR NOT NULL,
    decision               VARCHAR NOT NULL,   -- 'new_manager' | 'merge_to_franchise'
    target_franchise_id    VARCHAR,
    external_hash          VARCHAR,
    confirmed_by           VARCHAR,
    confirmed_at           TIMESTAMP,
    PRIMARY KEY (db_name, source_manager_norm)
);
"""


def create_schema_decisions_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(SCHEMA_DECISIONS_DDL)


def create_external_identity_overrides_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(EXTERNAL_IDENTITY_OVERRIDES_DDL)


IMPORT_RUN_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS staging.import_run_summary (
    db_name        VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    run_timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    phase          VARCHAR NOT NULL,
    status         VARCHAR NOT NULL,
    failures_json  JSON,
    detail         VARCHAR
);
"""


def create_import_run_summary_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(IMPORT_RUN_SUMMARY_DDL)


def log_run_summary(
    conn: duckdb.DuckDBPyConnection,
    db_name: str,
    run_id: str,
    phase: str,
    status: str,
    failures_json: list | None = None,
    detail: str | None = None,
) -> None:
    """Append a single row to staging.import_run_summary."""
    conn.execute(
        """INSERT INTO staging.import_run_summary
           (db_name, run_id, phase, status, failures_json, detail)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            db_name,
            run_id,
            phase,
            status,
            json.dumps(failures_json) if failures_json is not None else None,
            detail,
        ],
    )


def log_decision(conn: duckdb.DuckDBPyConnection, decision: dict[str, Any]) -> None:
    """Insert a single decision row. score_components is dict; serialized to JSON.
    coalesce_inputs / invariant_failures / sample_values are lists.
    """
    components_json = json.dumps(decision.get("score_components") or {})
    conn.execute(
        """
        INSERT INTO staging.schema_decisions
            (db_name, run_id, table_name, source_col, ddl_slot,
             score, score_components, second_best_score, second_best_slot,
             pass_number, was_derived, derivation_rule,
             was_coalesced, coalesce_inputs,
             status, invariant_failures, sample_values)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            decision["db_name"],
            decision["run_id"],
            decision["table_name"],
            decision.get("source_col"),
            decision.get("ddl_slot"),
            decision.get("score"),
            components_json,
            decision.get("second_best_score"),
            decision.get("second_best_slot"),
            decision.get("pass_number"),
            decision.get("was_derived", False),
            decision.get("derivation_rule"),
            decision.get("was_coalesced", False),
            decision.get("coalesce_inputs") or [],
            decision["status"],
            decision.get("invariant_failures") or [],
            decision.get("sample_values") or [],
        ],
    )


def query_decisions_for_run(conn, db_name: str, run_id: str) -> list[dict]:
    rows = (
        conn.execute(
            "SELECT * FROM staging.schema_decisions WHERE db_name=? AND run_id=?",
            [db_name, run_id],
        )
        .df()
        .to_dict(orient="records")
    )
    for r in rows:
        if r.get("score_components"):
            # Handle JSON deserialization - DuckDB may return string or dict
            components = r["score_components"]
            if isinstance(components, str):
                r["score_components"] = json.loads(components)
            elif not isinstance(components, dict):
                # Fallback for other types
                r["score_components"] = json.loads(json.dumps(components))
    return rows


def read_overrides_for_db(conn, db_name: str) -> dict[str, dict]:
    """Returns {source_manager_norm: {decision, target_franchise_id, external_hash}}."""
    rows = (
        conn.execute(
            "SELECT * FROM staging.external_identity_overrides WHERE db_name=?",
            [db_name],
        )
        .df()
        .to_dict(orient="records")
    )
    return {r["source_manager_norm"]: r for r in rows}
