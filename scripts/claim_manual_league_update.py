#!/usr/bin/env python3
"""Adopt a paid manual execute run into the existing Update League lifecycle."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from collections.abc import Callable
from typing import Any, MutableMapping


PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "fantasy_football_data_scripts"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from multi_league.core.league_update_status import (  # noqa: E402
    assert_league_update_entitled,
    record_league_update_status,
)


DB_NAME_RE = re.compile(r"[a-z0-9_]{1,63}\Z")
MANUAL_PLATFORMS = {"yahoo", "espn", "sleeper"}


def _append_execution_outputs(path: Path | None, values: dict[str, Any]) -> None:
    if path is None:
        return
    with path.open("a", encoding="utf-8") as output:
        output.write(f"observed_manifest_digest={values['observed_manifest_digest']}\n")
        output.write(f"token={values['dispatch_token']}\n")
        output.write(f"attempt_id={values['attempt_id']}\n")
        output.write(f"claim_version={values['claim_version']}\n")


def prepare_update_execution(
    reader: Any,
    writer: Any,
    *,
    db_name: str,
    platform: str,
    execute: bool,
    observed_manifest_digest: str | None,
    dispatch_token: str | None,
    attempt_id: str | None,
    claim_version: int,
    run_id: int,
    run_attempt: int,
    scheduled_demo: bool = False,
    output_path: Path | None = None,
    environment: MutableMapping[str, str] | None = None,
    probe: Callable[[str], str] | None = None,
    claim: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Prepare one refresh using its already-open Fly reader and writer.

    UI-dispatched runs retain their exact manifest and claim. Direct manual
    runs capture the manifest and paid claim here, avoiding two extra Python
    processes and two extra Fly client startups.
    """
    values: dict[str, Any] = {
        "observed_manifest_digest": str(observed_manifest_digest or ""),
        "dispatch_token": str(dispatch_token or ""),
        "attempt_id": str(attempt_id or ""),
        "claim_version": int(claim_version or 1),
    }
    if not execute:
        _append_execution_outputs(output_path, values)
        return values

    if not values["observed_manifest_digest"]:
        if probe is None:
            from scripts.probe_league_update_freshness import probe_freshness

            probe = probe_freshness
        values["observed_manifest_digest"] = probe(db_name)

    if not scheduled_demo and not values["dispatch_token"]:
        claim = claim or claim_manual_attempt
        owned = claim(
            reader,
            writer,
            db_name=db_name,
            platform=platform,
            run_id=run_id,
            run_attempt=run_attempt,
        )
        values.update(owned)

    target_environment = os.environ if environment is None else environment
    target_environment["LEAGUE_UPDATE_TOKEN"] = values["dispatch_token"]
    target_environment["LEAGUE_UPDATE_ATTEMPT_ID"] = values["attempt_id"]
    target_environment["LEAGUE_UPDATE_CLAIM_VERSION"] = str(values["claim_version"])
    _append_execution_outputs(output_path, values)
    return values


def _owned_claim(row: dict[str, Any] | None, *, db_name: str, token: str, run_id: int) -> dict[str, Any]:
    if not row or str(row.get("database_name")) != db_name \
        or str(row.get("status")) != "dispatching" \
        or str(row.get("dispatch_token")) != token \
        or str(row.get("attempt_id")) != token \
        or int(row.get("workflow_run_id") or 0) != run_id \
        or int(row.get("claim_version") or 0) < 1:
        raise RuntimeError("Manual league update no longer owns its dispatch claim")
    return {
        "dispatch_token": token,
        "attempt_id": token,
        "claim_version": int(row["claim_version"]),
    }


def claim_manual_attempt(
    reader: Any,
    writer: Any,
    *,
    db_name: str,
    platform: str,
    run_id: int,
    run_attempt: int,
) -> dict[str, Any]:
    if not DB_NAME_RE.fullmatch(db_name) or platform not in MANUAL_PLATFORMS:
        raise ValueError("Invalid manual league update target")
    if run_id < 1 or run_attempt < 1:
        raise ValueError("Invalid GitHub run identity")
    # Manual dispatch is not an entitlement bypass. Check before either a
    # lifecycle insert or a terminal-attempt CAS.
    assert_league_update_entitled(reader, database_name=db_name)
    token = f"manual-{run_id}-{run_attempt}"
    inserted_or_owned = record_league_update_status(
        writer, database_name=db_name, platform=platform, status="dispatching",
        dispatch_token=token, attempt_id=token, claim_version=1,
        workflow_run_id=run_id,
    )
    if inserted_or_owned:
        rows = reader.query(
            "SELECT database_name, status, dispatch_token, attempt_id, "
            "claim_version, workflow_run_id FROM accounts.league_update_dispatches "
            f"WHERE database_name = '{db_name}' LIMIT 1",
            database="___ops",
        )
        claim = _owned_claim(rows[0] if rows else None, db_name=db_name, token=token, run_id=run_id)
        renewed = writer.execute(
            "UPDATE accounts.league_update_dispatches SET "
            "lease_expires_at = NOW() + INTERVAL '20 minutes', updated_at = NOW() "
            f"WHERE database_name = '{db_name}' AND status = 'dispatching' "
            f"AND dispatch_token = '{token}' AND attempt_id = '{token}' "
            f"AND claim_version = {claim['claim_version']} AND workflow_run_id = {run_id} "
            "RETURNING database_name",
            database="___ops",
        )
        if not renewed:
            raise RuntimeError("Manual league update no longer owns its dispatch claim")
        return claim

    # A prior terminal row cannot be rewritten with the old claim. A new
    # manual GH run takes ownership only through the same lease/terminal CAS
    # boundary as the UI, never over a committed/cache-pending publication.
    rows = writer.execute(
        "UPDATE accounts.league_update_dispatches SET "
        f"platform = '{platform}', status = 'dispatching', "
        f"dispatch_token = '{token}', attempt_id = '{token}', "
        "claim_version = COALESCE(claim_version, 0) + 1, "
        f"workflow_run_id = {run_id}, workflow_file = NULL, "
        "heartbeat_at = NULL, cache_state = 'dispatching', "
        "dispatched_at = NULL, started_at = NULL, completed_at = NULL, "
        "lease_expires_at = NOW() + INTERVAL '20 minutes', updated_at = NOW(), error = NULL "
        f"WHERE database_name = '{db_name}' "
        f"AND (dispatch_token IS NULL OR dispatch_token <> '{token}') "
        "AND (status IN ('succeeded', 'failed', 'cancelled', 'stale', "
        "'credential_required', 'incomplete_source', 'no_change', 'validation_failed') "
        "OR (status IN ('dispatching', 'dispatched', 'running') AND "
        "(lease_expires_at < NOW() OR "
        "(lease_expires_at IS NULL AND updated_at < NOW() - INTERVAL '20 minutes')) "
        "AND (heartbeat_at IS NULL OR heartbeat_at < NOW() - INTERVAL '20 minutes'))) "
        "RETURNING database_name, status, dispatch_token, attempt_id, "
        "claim_version, workflow_run_id",
        database="___ops",
    )
    return _owned_claim(rows[0] if rows else None, db_name=db_name, token=token, run_id=run_id)


def fail_partial_manual_attempt(
    reader: Any,
    writer: Any,
    *,
    db_name: str,
    platform: str,
    run_id: int,
    run_attempt: int,
) -> bool:
    """Settle only a claim owned before GitHub could emit its step outputs."""
    if not DB_NAME_RE.fullmatch(db_name) or platform not in MANUAL_PLATFORMS \
       or run_id < 1 or run_attempt < 1:
        raise ValueError("Invalid partial manual claim identity")
    token = f"manual-{run_id}-{run_attempt}"
    rows = reader.query(
        "SELECT database_name, platform, status, dispatch_token, attempt_id, "
        "workflow_run_id, claim_version FROM accounts.league_update_dispatches "
        f"WHERE database_name = '{db_name}' LIMIT 1",
        database="___ops",
    )
    row = rows[0] if rows else None
    if not row or str(row.get("platform")) != platform:
        return False
    try:
        claim = _owned_claim(row, db_name=db_name, token=token, run_id=run_id)
    except RuntimeError:
        return False
    settled = writer.execute(
        "UPDATE accounts.league_update_dispatches SET status = 'failed', "
        "cache_state = 'failed', lease_expires_at = NULL, completed_at = NOW(), "
        "updated_at = NOW(), error = 'Manual update claim failed before start' "
        f"WHERE database_name = '{db_name}' AND platform = '{platform}' "
        "AND status = 'dispatching' "
        f"AND dispatch_token = '{token}' AND attempt_id = '{token}' "
        f"AND claim_version = {claim['claim_version']} AND workflow_run_id = {run_id} "
        "RETURNING database_name",
        database="___ops",
    )
    return bool(settled)



def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--platform", required=True, choices=sorted(MANUAL_PLATFORMS))
    parser.add_argument("--run-id", type=int, default=int(os.environ.get("GITHUB_RUN_ID") or 0))
    parser.add_argument("--run-attempt", type=int, default=int(os.environ.get("GITHUB_RUN_ATTEMPT") or 0))
    parser.add_argument("--fail-owned-claim", action="store_true")
    args = parser.parse_args(argv)

    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.readers.fly_reader import FlyReader

    reader, writer = FlyReader(), FlyWriter()
    if args.fail_owned_claim:
        fail_partial_manual_attempt(
            reader, writer, db_name=args.db, platform=args.platform,
            run_id=args.run_id, run_attempt=args.run_attempt,
        )
        return 0
    claim = claim_manual_attempt(
        reader, writer, db_name=args.db, platform=args.platform,
        run_id=args.run_id, run_attempt=args.run_attempt,
    )
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"token={claim['dispatch_token']}\n")
            output.write(f"attempt_id={claim['attempt_id']}\n")
            output.write(f"claim_version={claim['claim_version']}\n")
    else:
        print(json.dumps(claim, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
