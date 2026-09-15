"""Renew the exact paid UI/manual attempt immediately before Fleet publication."""

from __future__ import annotations

import os
from typing import Any

from multi_league.core.league_update_status import (
    assert_league_update_entitled,
    record_league_update_status,
)


def renew_claim_for_publication(
    reader: Any,
    *,
    database_name: str,
    platform: str,
    writer: Any | None = None,
) -> bool:
    """Fail closed if a paid publishing attempt lost its token or entitlement.

    Updating ``running`` is a guarded compare-and-swap: it requires the same
    token, attempt, claim version and GitHub run, and extends the heartbeat and
    lease before the existing generation-fenced Fleet merge begins.
    """
    required = os.environ.get("LEAGUE_UPDATE_REQUIRE_CLAIM", "").strip().lower() in {
        "1", "true", "yes", "on",
    }
    token = os.environ.get("LEAGUE_UPDATE_TOKEN", "").strip()
    if not required and not token:
        return False
    attempt_id = os.environ.get("LEAGUE_UPDATE_ATTEMPT_ID", "").strip()
    version_raw = os.environ.get("LEAGUE_UPDATE_CLAIM_VERSION", "").strip()
    try:
        version = int(version_raw)
    except ValueError as exc:
        raise RuntimeError("League update is missing exact attempt ownership") from exc
    if not token or not attempt_id or version < 1:
        raise RuntimeError("League update is missing exact attempt ownership")
    if platform not in {"yahoo", "espn", "sleeper"}:
        raise ValueError("Unsupported league update platform")

    assert_league_update_entitled(reader, database_name=database_name)
    if writer is None:
        from multi_league.core.fly_writer import FlyWriter

        writer = FlyWriter()
    accepted = record_league_update_status(
        writer,
        database_name=database_name,
        platform=platform,
        status="running",
        dispatch_token=token,
        attempt_id=attempt_id,
        claim_version=version,
        workflow_run_id=os.environ.get("GITHUB_RUN_ID"),
    )
    if not accepted:
        raise RuntimeError("Worker no longer owns this league update claim before publication")
    return True
