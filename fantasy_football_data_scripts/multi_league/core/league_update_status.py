"""Durable lifecycle and freshness receipts for active-season league updates."""

from __future__ import annotations

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
    "no_change",
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
    "no_change",
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
    "no_change": {"running", "no_change"},
    "validation_failed": {"running", "validation_failed"},
}
def build_cache_recovery_receipt(
    row: Mapping[str, Any], *, current_generation: int
) -> dict[str, Any]:
    """Rebuild only the durable committed receipt; never fetch or republish data."""
    if str(row.get("status") or "") not in {"committed", "committed_cache_pending"}:
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
    manifest_scope_admitted = (
        receipt.get("source_manifest_complete") is True
        or receipt.get("source_manifest_scope_complete") is True
    )
    if manifest_scope_admitted \
       and digest != str(row.get("published_manifest_digest") or ""):
        raise ValueError("published manifest does not match the committed receipt")
    if manifest_scope_admitted \
       and not receipt.get("source_manifest_json"):
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
    eligible = reader.query_scalar(
        "SELECT CASE WHEN LOWER(COALESCE(entitled_mode, '')) = 'full' "
        "AND LOWER(COALESCE(tier, '')) = 'paid' AND expires_at > NOW() "
        "THEN 1 ELSE 0 END FROM ("
        "SELECT tier, entitled_mode, expires_at FROM accounts.league_inventory "
        f"WHERE database_name = {_literal(database_name)} "
        "ORDER BY updated_at DESC NULLS LAST LIMIT 1) latest",
        database="___ops",
    )
    if int(eligible or 0) < 1:
        raise PermissionError(f"League update is not entitled: {database_name}")


def start_league_update_execution(
    reader: Any,
    writer: Any,
    *,
    database_name: str,
    platform: str,
    dispatch_token: str | None,
    attempt_id: str | None = None,
    claim_version: int = 1,
    workflow_run_id: int | str | None = None,
) -> bool:
    """Authorize an update and claim its running state in the worker process.

    UI dispatch already creates the durable claim.  Starting a second Python
    process solely to move that claim from ``dispatched`` to ``running`` added
    material latency before every provider fetch.  Keeping the same guarded
    transition in the refresh preflight preserves ownership while allowing it
    to overlap the other independent Fly reads.

    Direct/local execute calls may not have a dispatch token.  They still pass
    the paid entitlement gate but do not create lifecycle state.
    """
    assert_league_update_entitled(reader, database_name=database_name)
    normalized_token = str(dispatch_token or "").strip()
    if not normalized_token:
        return True
    if writer is None:
        raise RuntimeError("A claimed league update requires a Fly writer")
    claimed = record_league_update_status(
        writer,
        database_name=database_name,
        platform=platform,
        status="running",
        dispatch_token=normalized_token,
        attempt_id=attempt_id,
        claim_version=claim_version,
        workflow_run_id=workflow_run_id,
    )
    if not claimed:
        raise RuntimeError("Worker no longer owns this league update claim")
    return True


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
    # A proven no-op leaves the last successful publication fully current.
    # Treat it as healthy while retaining the prior publication metadata.
    is_success = normalized in {"succeeded", "no_change"}
    is_terminal = normalized in TERMINAL_STATUSES
    has_publication = normalized in PUBLICATION_STATUSES
    manifest_aware_publication = bool(
        has_publication and receipt.get("source_manifest_digest")
    )
    # ``source_manifest_complete`` records whether every game in the captured
    # week is final. ``source_manifest_scope_complete`` separately proves that
    # every changed manifest partition was admitted, even when games remain
    # live. A committed, scope-complete partial snapshot is the next delta
    # baseline; a historical correction outside the fetched scope is not.
    manifest_scope_admitted = (
        receipt.get("source_manifest_complete") is True
        or receipt.get("source_manifest_scope_complete") is True
    )
    can_promote_manifest = (
        manifest_aware_publication
        and manifest_scope_admitted
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
    require_manifest_promotion = bool(
        can_promote_manifest
        and (
            manifest_scope_admitted
            or captured_manifest_json is not None
        )
    )
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
        "source_manifest_scope_complete": receipt.get("source_manifest_scope_complete") is True,
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
        if require_manifest_promotion and captured_manifest_json is None
        else "TRUE"
    )
    published_guard = (
        "AND EXISTS (SELECT 1 FROM accounts.league_update_manifests m "
        "WHERE m.database_name = accounts.league_update_dispatches.database_name "
        f"AND m.published_manifest_digest = {_literal(source_fingerprint)})"
        if require_manifest_promotion
        else ""
    )
    sql = f"""
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
      {"NOW() + INTERVAL '3 minutes'" if normalized == 'running' else 'NULL'},
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
        WHEN {_literal(normalized)} = 'running' THEN NOW() + INTERVAL '3 minutes'
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
