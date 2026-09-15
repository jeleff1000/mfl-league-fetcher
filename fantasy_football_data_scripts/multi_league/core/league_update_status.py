"""Durable lifecycle and freshness receipts for active-season league updates."""

from __future__ import annotations

import os
import json
from collections.abc import Mapping
from typing import Any

from multi_league.core.league_update_manifest import (
    canonical_manifest_json,
    manifest_digest,
    source_manifest_from_mapping,
)


VALID_STATUSES = {
    "dispatching",
    "dispatched",
    "running",
    "committed",
    "cache_verified",
    "succeeded",
    "failed",
    "cancelled",
    "stale",
    "credential_required",
    "incomplete_source",
    "validation_failed",
    "committed_cache_pending",
}
TERMINAL_STATUSES = {
    "succeeded",
    "failed",
    "cancelled",
    "stale",
    "credential_required",
    "incomplete_source",
    "validation_failed",
}
PUBLICATION_STATUSES = {
    "committed",
    "cache_verified",
    "committed_cache_pending",
    "succeeded",
}
ALLOWED_PRIOR_STATUSES = {
    "dispatching": {"dispatching"},
    "dispatched": {"dispatching", "dispatched"},
    "running": {"dispatching", "dispatched", "running"},
    "committed": {"running", "committed"},
    "cache_verified": {"committed", "committed_cache_pending", "cache_verified"},
    "committed_cache_pending": {
        "running",
        "committed",
        "cache_verified",
        "committed_cache_pending",
    },
    # Direct running -> succeeded remains temporarily valid for deployed workers;
    # the explicit committed/cache states are additive and become mandatory when
    # all callers are migrated together.
    "succeeded": {
        "running",
        "committed",
        "cache_verified",
        "committed_cache_pending",
        "succeeded",
    },
    "failed": {"dispatching", "dispatched", "running", "failed"},
    "cancelled": {"dispatching", "dispatched", "running", "cancelled"},
    "stale": {"dispatching", "dispatched", "running", "stale"},
    "credential_required": {"running", "credential_required"},
    "incomplete_source": {"running", "incomplete_source"},
    "validation_failed": {"running", "validation_failed"},
}
DEFAULT_GRANDFATHERED_LEAGUES = {"kmffl", "tfl_of_extraordinary_gentleman"}


def build_cache_recovery_receipt(
    row: Mapping[str, Any], *, current_generation: int
) -> dict[str, Any]:
    """Rebuild only the durable committed receipt; never fetch or republish data."""
    if str(row.get("status") or "") != "committed_cache_pending":
        raise ValueError("League is not eligible for cache recovery")
    base = row.get("base_generation")
    if base in (None, "") or int(current_generation) != int(base) + 1:
        raise ValueError("A newer publication superseded this cache recovery")
    try:
        receipt = json.loads(str(row.get("publication_receipt_json") or ""))
    except (TypeError, ValueError) as exc:
        raise ValueError("Committed publication receipt is missing") from exc
    if not isinstance(receipt, dict) or receipt.get("status") != "COMMITTED":
        raise ValueError("Committed publication receipt is invalid")
    if receipt.get("executed") is not True:
        raise ValueError("Committed publication was not executed")
    digest = str(row.get("source_fingerprint") or "")
    if not digest or digest != str(receipt.get("source_manifest_digest") or ""):
        raise ValueError("Committed publication manifest identity changed")
    if receipt.get("source_manifest_complete") is True \
       and digest != str(row.get("published_manifest_digest") or ""):
        raise ValueError("published manifest does not match the committed receipt")
    if not receipt.get("source_manifest_json"):
        raise ValueError("Committed publication manifest is missing")
    if str(receipt.get("bundle_id") or "") != str(row.get("bundle_id") or "") \
       or int(receipt.get("source_year") or 0) != int(row.get("source_year") or 0) \
       or int(receipt.get("source_week") or 0) != int(row.get("source_week") or 0) \
       or int(receipt.get("base_generation") if receipt.get("base_generation") is not None else -1) != int(base):
        raise ValueError("Committed publication identity changed")
    return receipt


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
        "SELECT CASE WHEN LOWER(COALESCE(entitled_mode, '')) = 'full' "
        "AND ((LOWER(COALESCE(tier, '')) = 'paid' AND expires_at > NOW()) "
        "OR LOWER(COALESCE(tier, '')) = 'grandfathered') "
        "THEN 1 ELSE 0 END FROM ("
        "SELECT tier, entitled_mode, expires_at FROM accounts.league_inventory "
        f"WHERE database_name = {_literal(database_name)} "
        "ORDER BY updated_at DESC NULLS LAST LIMIT 1) latest",
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
    attempt_id: str | None = None,
    claim_version: int = 1,
    receipt: Mapping[str, Any] | None = None,
    cache_verified: bool = False,
    error: str | None = None,
) -> bool:
    normalized = str(status).strip().lower()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported league update status: {status!r}")
    receipt = dict(receipt or {})
    if normalized in PUBLICATION_STATUSES:
        if str(receipt.get("status") or "").upper() != "COMMITTED":
            raise ValueError(f"A {normalized} league update requires a COMMITTED refresh receipt")
        if normalized in {"cache_verified", "succeeded"} and not cache_verified:
            raise ValueError("A succeeded league update requires cache verification")
        if not (receipt.get("source_manifest_digest") or receipt.get("source_fingerprint")):
            raise ValueError("A published league update requires a source manifest digest")

    run_id = int(workflow_run_id) if workflow_run_id not in (None, "") else None
    normalized_attempt_id = str(attempt_id or dispatch_token)
    normalized_claim_version = int(claim_version)
    if normalized_claim_version < 1:
        raise ValueError("claim_version must be positive")
    is_success = normalized == "succeeded"
    is_terminal = normalized in TERMINAL_STATUSES
    has_publication = normalized in PUBLICATION_STATUSES
    manifest_aware_publication = bool(
        has_publication and receipt.get("source_manifest_digest")
    )
    can_promote_manifest = (
        manifest_aware_publication and receipt.get("source_manifest_complete") is True
    )
    source_year = int(receipt["source_year"]) if has_publication else None
    source_week = int(receipt["source_week"]) if has_publication else None
    source_fingerprint = (
        receipt.get("source_manifest_digest") or receipt.get("source_fingerprint")
        if has_publication
        else None
    )
    captured_manifest_json: str | None = None
    raw_manifest_json = receipt.get("source_manifest_json") if has_publication else None
    if raw_manifest_json:
        try:
            parsed_manifest = source_manifest_from_mapping(json.loads(str(raw_manifest_json)))
        except (TypeError, ValueError) as exc:
            raise ValueError("Published source manifest JSON is invalid") from exc
        if manifest_digest(parsed_manifest) != str(source_fingerprint):
            raise ValueError("Published source manifest digest does not match its payload")
        if parsed_manifest.database_name != database_name:
            raise ValueError("Published source manifest belongs to a different league")
        if parsed_manifest.active_season != source_year:
            raise ValueError("Published source manifest belongs to a different season")
        captured_manifest_json = canonical_manifest_json(parsed_manifest)
    generation = receipt.get("bundle_id") if has_publication else None
    base_generation = receipt.get("base_generation") if has_publication else None
    durable_receipt_json = json.dumps({
        "status": "COMMITTED",
        "executed": True,
        "source_year": source_year,
        "source_week": source_week,
        "source_manifest_digest": source_fingerprint,
        "source_manifest_json": receipt.get("source_manifest_json"),
        "source_manifest_complete": receipt.get("source_manifest_complete") is True,
        "bundle_id": generation,
        "base_generation": base_generation,
    }, sort_keys=True, separators=(",", ":")) if has_publication else None
    allowed_prior = ", ".join(_literal(value) for value in sorted(ALLOWED_PRIOR_STATUSES[normalized]))
    run_owner_guard = (
        "workflow_run_id IS NULL"
        if run_id is None
        else f"(workflow_run_id IS NULL OR workflow_run_id = {run_id})"
    )
    manifest_guard = (
        "EXISTS (SELECT 1 FROM accounts.league_update_manifests m "
        f"WHERE m.database_name = {_literal(database_name)} "
        f"AND m.observed_manifest_digest = {_literal(source_fingerprint)} "
        "AND m.observed_manifest_json IS NOT NULL)"
        if manifest_aware_publication and captured_manifest_json is None
        else "TRUE"
    )
    published_guard = (
        "AND EXISTS (SELECT 1 FROM accounts.league_update_manifests m "
        "WHERE m.database_name = accounts.league_update_dispatches.database_name "
        f"AND m.published_manifest_digest = {_literal(source_fingerprint)})"
        if can_promote_manifest
        else ""
    )
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS accounts;
    CREATE TABLE IF NOT EXISTS accounts.league_update_manifests (
      database_name VARCHAR PRIMARY KEY,
      platform VARCHAR, active_season INTEGER, through_week INTEGER,
      observed_manifest_json VARCHAR, observed_manifest_digest VARCHAR,
      published_manifest_json VARCHAR, published_manifest_digest VARCHAR,
      published_at TIMESTAMP,
      probe_status VARCHAR NOT NULL DEFAULT 'unknown', probe_error_code VARCHAR,
      last_attempt_at TIMESTAMP, last_success_at TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    ALTER TABLE accounts.league_update_manifests ADD COLUMN IF NOT EXISTS published_manifest_json VARCHAR;
    ALTER TABLE accounts.league_update_manifests ADD COLUMN IF NOT EXISTS published_manifest_digest VARCHAR;
    ALTER TABLE accounts.league_update_manifests ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;
    CREATE TABLE IF NOT EXISTS accounts.league_update_dispatches (
      database_name VARCHAR PRIMARY KEY, platform VARCHAR NOT NULL, status VARCHAR NOT NULL,
      workflow_file VARCHAR, workflow_run_id BIGINT, dispatch_token VARCHAR,
      source_year INTEGER, source_week INTEGER, source_fingerprint VARCHAR,
      publish_generation VARCHAR, healthy BOOLEAN DEFAULT FALSE,
      dispatched_at TIMESTAMP, started_at TIMESTAMP, completed_at TIMESTAMP,
      lease_expires_at TIMESTAMP, updated_at TIMESTAMP DEFAULT NOW(), error VARCHAR
    );
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS attempt_id VARCHAR;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS claim_version BIGINT DEFAULT 0;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMP;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS observed_manifest_digest VARCHAR;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS base_generation VARCHAR;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS bundle_id VARCHAR;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS cache_state VARCHAR;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS committed_at TIMESTAMP;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS cache_verified_at TIMESTAMP;
    ALTER TABLE accounts.league_update_dispatches ADD COLUMN IF NOT EXISTS publication_receipt_json VARCHAR;
    INSERT INTO accounts.league_update_dispatches
      (database_name, platform, status, workflow_run_id, dispatch_token,
       source_year, source_week, source_fingerprint, publish_generation, healthy,
       started_at, completed_at, lease_expires_at, updated_at, error,
       attempt_id, claim_version, heartbeat_at, observed_manifest_digest,
       base_generation, bundle_id, cache_state, committed_at, cache_verified_at,
       publication_receipt_json)
    SELECT {_literal(database_name)}, {_literal(platform)}, {_literal(normalized)},
      {run_id if run_id is not None else 'NULL'}, {_literal(dispatch_token)},
      {source_year if source_year is not None else 'NULL'},
      {source_week if source_week is not None else 'NULL'}, {_literal(source_fingerprint)},
      {_literal(generation)}, {'TRUE' if is_success else 'FALSE'},
      {'NOW()' if normalized == 'running' else 'NULL'},
      {'NOW()' if is_terminal else 'NULL'},
      {"NOW() + INTERVAL '20 minutes'" if normalized == 'running' else 'NULL'},
      NOW(), {_literal((error or '')[:2000] or None)},
      {_literal(normalized_attempt_id)}, {normalized_claim_version},
      {'NOW()' if normalized == 'running' else 'NULL'}, {_literal(source_fingerprint)},
      {_literal(base_generation)}, {_literal(generation)}, {_literal(normalized)},
      {'NOW()' if normalized in PUBLICATION_STATUSES else 'NULL'},
      {'NOW()' if normalized in {'cache_verified', 'succeeded'} else 'NULL'},
      {_literal(durable_receipt_json)}
    WHERE {manifest_guard}
    ON CONFLICT (database_name) DO NOTHING;
    UPDATE accounts.league_update_dispatches SET
      platform = {_literal(platform)}, status = {_literal(normalized)},
      workflow_run_id = COALESCE(workflow_run_id, {run_id if run_id is not None else 'NULL'}),
      attempt_id = COALESCE(attempt_id, {_literal(normalized_attempt_id)}),
      claim_version = CASE WHEN COALESCE(claim_version, 0) = 0 THEN {normalized_claim_version} ELSE claim_version END,
      source_year = CASE WHEN {str(has_publication).upper()} THEN {source_year if source_year is not None else 'NULL'} ELSE source_year END,
      source_week = CASE WHEN {str(has_publication).upper()} THEN {source_week if source_week is not None else 'NULL'} ELSE source_week END,
      source_fingerprint = CASE WHEN {str(has_publication).upper()} THEN {_literal(source_fingerprint)} ELSE source_fingerprint END,
      observed_manifest_digest = CASE WHEN {str(has_publication).upper()} THEN {_literal(source_fingerprint)} ELSE observed_manifest_digest END,
      publish_generation = CASE WHEN {str(has_publication).upper()} THEN {_literal(generation)} ELSE publish_generation END,
      base_generation = CASE WHEN {str(has_publication).upper()} THEN {_literal(base_generation)} ELSE base_generation END,
      bundle_id = CASE WHEN {str(has_publication).upper()} THEN {_literal(generation)} ELSE bundle_id END,
      publication_receipt_json = CASE WHEN {str(has_publication).upper()} THEN {_literal(durable_receipt_json)} ELSE publication_receipt_json END,
      cache_state = {_literal(normalized)}, healthy = {'TRUE' if is_success else 'FALSE'},
      started_at = CASE WHEN {_literal(normalized)} = 'running' THEN COALESCE(started_at, NOW()) ELSE started_at END,
      heartbeat_at = CASE WHEN {_literal(normalized)} = 'running' THEN NOW() ELSE heartbeat_at END,
      committed_at = CASE WHEN {str(normalized in PUBLICATION_STATUSES).upper()} THEN COALESCE(committed_at, NOW()) ELSE committed_at END,
      cache_verified_at = CASE WHEN {str(normalized in {'cache_verified', 'succeeded'}).upper()} THEN COALESCE(cache_verified_at, NOW()) ELSE cache_verified_at END,
      completed_at = CASE WHEN {str(is_terminal).upper()} THEN COALESCE(completed_at, NOW()) ELSE completed_at END,
      lease_expires_at = CASE
        WHEN {_literal(normalized)} = 'running' THEN NOW() + INTERVAL '20 minutes'
        WHEN {str(is_terminal).upper()} THEN NULL
        ELSE lease_expires_at END,
      updated_at = NOW(), error = {_literal((error or '')[:2000] or None)}
    WHERE database_name = {_literal(database_name)}
      AND dispatch_token = {_literal(dispatch_token)}
      AND COALESCE(attempt_id, {_literal(normalized_attempt_id)}) = {_literal(normalized_attempt_id)}
      AND COALESCE(claim_version, 0) IN (0, {normalized_claim_version})
      AND {run_owner_guard}
      AND {manifest_guard}
      AND status IN ({allowed_prior})
      AND NOT ({str(is_terminal).upper()} AND status = {_literal(normalized)})
    RETURNING database_name;
    UPDATE accounts.league_update_manifests SET
      published_manifest_json = {(_literal(captured_manifest_json) if captured_manifest_json is not None else 'observed_manifest_json')},
      published_manifest_digest = {_literal(source_fingerprint)},
      published_at = NOW(), updated_at = NOW()
    WHERE {str(can_promote_manifest).upper()}
      AND database_name = {_literal(database_name)}
      AND ({str(captured_manifest_json is not None).upper()}
           OR observed_manifest_digest = {_literal(source_fingerprint)})
      AND EXISTS (
        SELECT 1 FROM accounts.league_update_dispatches d
        WHERE d.database_name = {_literal(database_name)}
          AND d.dispatch_token = {_literal(dispatch_token)}
          AND d.status = {_literal(normalized)}
      );
    SELECT database_name
    FROM accounts.league_update_dispatches
    WHERE database_name = {_literal(database_name)}
      AND dispatch_token = {_literal(dispatch_token)}
      AND COALESCE(attempt_id, {_literal(normalized_attempt_id)}) = {_literal(normalized_attempt_id)}
      AND COALESCE(claim_version, 0) IN (0, {normalized_claim_version})
      AND {run_owner_guard}
      AND status = {_literal(normalized)}
      {published_guard}
    LIMIT 1;
    """
    response = writer.execute(sql, database="___ops")
    if isinstance(response, list):
        return bool(response)
    fetchone = getattr(response, "fetchone", None)
    return bool(fetchone and fetchone())


transition_league_update_attempt = record_league_update_status
