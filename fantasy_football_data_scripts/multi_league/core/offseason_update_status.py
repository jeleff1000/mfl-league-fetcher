"""Durable Fly status records for user-triggered offseason draft updates."""

from __future__ import annotations

from typing import Any


VALID_STATUSES = {"dispatching", "dispatched", "running", "succeeded", "no_change", "failed"}
TERMINAL_STATUSES = {"succeeded", "no_change", "failed"}


def _sql_literal(value: object | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def classify_offseason_update_result(result: Any) -> str:
    """Translate a checker result into a truthful user-facing terminal state."""
    if bool(getattr(result, "updated", False)):
        return "succeeded"
    if str(getattr(result, "status", "")) == "up_to_date":
        return "no_change"
    return "failed"


def record_offseason_update_status(
    writer: Any,
    *,
    database_name: str,
    draft_year: int,
    platform: str,
    status: str,
    workflow_run_id: int | str | None = None,
    dispatch_token: str | None = None,
    error: str | None = None,
) -> bool:
    """Upsert one worker lifecycle state into the shared Fly dispatch ledger."""
    normalized_status = str(status).strip().lower()
    if normalized_status not in VALID_STATUSES:
        raise ValueError(f"Unsupported offseason update status: {status!r}")
    year = int(draft_year)
    if year < 2020 or year > 2100:
        raise ValueError(f"Invalid offseason draft year: {draft_year!r}")
    run_id = int(workflow_run_id) if workflow_run_id not in (None, "") else None
    terminal = normalized_status in TERMINAL_STATUSES
    started_at = "NOW()" if normalized_status == "running" else "NULL"
    completed_at = "NOW()" if terminal else "NULL"
    lease_expires_at = (
        "NOW() + INTERVAL '135 minutes'"
        if normalized_status == "running"
        else "NULL"
    )
    error_value = _sql_literal((error or "")[:2000] or None)

    sql = f"""
    INSERT INTO accounts.offseason_draft_update_dispatches
        (database_name, draft_year, platform, status, workflow_run_id, dispatch_token,
         started_at, completed_at, lease_expires_at, updated_at, error)
    VALUES
        ({_sql_literal(database_name)}, {year}, {_sql_literal(platform)},
         {_sql_literal(normalized_status)}, {run_id if run_id is not None else 'NULL'}, {_sql_literal(dispatch_token)},
         {started_at}, {completed_at}, {lease_expires_at}, NOW(), {error_value})
    ON CONFLICT (database_name, draft_year) DO UPDATE SET
        platform = excluded.platform,
        status = excluded.status,
        workflow_run_id = COALESCE(excluded.workflow_run_id, accounts.offseason_draft_update_dispatches.workflow_run_id),
        started_at = COALESCE(excluded.started_at, accounts.offseason_draft_update_dispatches.started_at),
        completed_at = excluded.completed_at,
        lease_expires_at = excluded.lease_expires_at,
        updated_at = NOW(),
        error = excluded.error
    WHERE accounts.offseason_draft_update_dispatches.dispatch_token
        IS NOT DISTINCT FROM excluded.dispatch_token
    RETURNING database_name;
    """
    response = writer.execute(sql, database="___ops")
    if isinstance(response, list):
        return bool(response)
    fetchone = getattr(response, "fetchone", None)
    return bool(fetchone and fetchone())
