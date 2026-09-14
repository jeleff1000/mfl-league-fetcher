"""Durable lifecycle and freshness receipts for active-season league updates."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


VALID_STATUSES = {"running", "succeeded", "failed"}
DEFAULT_GRANDFATHERED_LEAGUES = {"kmffl", "tfl_of_extraordinary_gentleman"}
REQUIRED_SUCCESS_TABLES = (
    "player_fantasy",
    "league_settings",
    "homepage_league_summary",
)


def _literal(value: object | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def assert_league_update_entitled(reader: Any, *, database_name: str) -> None:
    configured = {
        value.strip().lower()
        for value in os.environ.get("LEAGUE_UPDATE_GRANDFATHERED_DBS", "").split(",")
        if value.strip()
    }
    if database_name.lower() in DEFAULT_GRANDFATHERED_LEAGUES | configured:
        return
    eligible = reader.query_scalar(
        "SELECT COUNT(*) FROM accounts.league_inventory "
        f"WHERE database_name = {_literal(database_name)} "
        "AND LOWER(COALESCE(entitled_mode, '')) = 'full' "
        "AND LOWER(COALESCE(tier, '')) = 'paid' AND expires_at > NOW()",
        database="___ops",
    )
    if int(eligible or 0) < 1:
        raise PermissionError(f"League update is not entitled: {database_name}")


def record_league_update_status(
    writer: Any,
    *,
    database_name: str,
    platform: str,
    status: str,
    dispatch_token: str,
    workflow_run_id: int | str | None = None,
    receipt: Mapping[str, Any] | None = None,
    cache_verified: bool = False,
    error: str | None = None,
) -> bool:
    normalized = str(status).strip().lower()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported league update status: {status!r}")
    receipt = dict(receipt or {})
    if normalized == "succeeded":
        if str(receipt.get("status") or "").upper() != "COMMITTED":
            raise ValueError("A succeeded league update requires a COMMITTED refresh receipt")
        if not cache_verified:
            raise ValueError("A succeeded league update requires cache verification")
        if not receipt.get("source_fingerprint"):
            raise ValueError("A succeeded league update requires a source fingerprint")
        post_publish_counts = receipt.get("post_publish_counts")
        missing_rows = [
            table
            for table in REQUIRED_SUCCESS_TABLES
            if not isinstance(post_publish_counts, Mapping)
            or int(post_publish_counts.get(table) or 0) < 1
        ]
        if missing_rows:
            raise ValueError(
                "A succeeded league update is missing required post-publish rows for: "
                + ", ".join(missing_rows)
            )

    run_id = int(workflow_run_id) if workflow_run_id not in (None, "") else None
    is_success = normalized == "succeeded"
    is_terminal = normalized in {"succeeded", "failed"}
    source_year = int(receipt["source_year"]) if is_success else None
    source_week = int(receipt["source_week"]) if is_success else None
    source_fingerprint = receipt.get("source_fingerprint") if is_success else None
    generation = receipt.get("bundle_id") if is_success else None
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS accounts;
    CREATE TABLE IF NOT EXISTS accounts.league_update_dispatches (
      database_name VARCHAR PRIMARY KEY, platform VARCHAR NOT NULL, status VARCHAR NOT NULL,
      workflow_file VARCHAR, workflow_run_id BIGINT, dispatch_token VARCHAR,
      source_year INTEGER, source_week INTEGER, source_fingerprint VARCHAR,
      publish_generation VARCHAR, healthy BOOLEAN DEFAULT FALSE,
      dispatched_at TIMESTAMP, started_at TIMESTAMP, completed_at TIMESTAMP,
      lease_expires_at TIMESTAMP, updated_at TIMESTAMP DEFAULT NOW(), error VARCHAR
    );
    INSERT INTO accounts.league_update_dispatches
      (database_name, platform, status, workflow_run_id, dispatch_token,
       source_year, source_week, source_fingerprint, publish_generation, healthy,
       started_at, completed_at, lease_expires_at, updated_at, error)
    VALUES ({_literal(database_name)}, {_literal(platform)}, {_literal(normalized)},
      {run_id if run_id is not None else 'NULL'}, {_literal(dispatch_token)},
      {source_year if source_year is not None else 'NULL'},
      {source_week if source_week is not None else 'NULL'}, {_literal(source_fingerprint)},
      {_literal(generation)}, {'TRUE' if is_success else 'FALSE'},
      {'NOW()' if normalized == 'running' else 'NULL'},
      {'NOW()' if is_terminal else 'NULL'},
      {"NOW() + INTERVAL '20 minutes'" if normalized == 'running' else 'NULL'},
      NOW(), {_literal((error or '')[:2000] or None)})
    ON CONFLICT (database_name) DO UPDATE SET
      platform = excluded.platform, status = excluded.status,
      workflow_run_id = COALESCE(excluded.workflow_run_id, accounts.league_update_dispatches.workflow_run_id),
      source_year = CASE WHEN excluded.status = 'succeeded' THEN excluded.source_year ELSE accounts.league_update_dispatches.source_year END,
      source_week = CASE WHEN excluded.status = 'succeeded' THEN excluded.source_week ELSE accounts.league_update_dispatches.source_week END,
      source_fingerprint = CASE WHEN excluded.status = 'succeeded' THEN excluded.source_fingerprint ELSE accounts.league_update_dispatches.source_fingerprint END,
      publish_generation = CASE WHEN excluded.status = 'succeeded' THEN excluded.publish_generation ELSE accounts.league_update_dispatches.publish_generation END,
      healthy = excluded.healthy, started_at = COALESCE(excluded.started_at, accounts.league_update_dispatches.started_at),
      completed_at = excluded.completed_at, lease_expires_at = excluded.lease_expires_at,
      updated_at = NOW(), error = excluded.error
    WHERE accounts.league_update_dispatches.dispatch_token = {_literal(dispatch_token)}
    RETURNING database_name;
    """
    response = writer.execute(sql, database="___ops")
    if isinstance(response, list):
        return bool(response)
    fetchone = getattr(response, "fetchone", None)
    return bool(fetchone and fetchone())
