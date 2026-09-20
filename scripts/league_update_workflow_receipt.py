#!/usr/bin/env python3
"""Classify the existing active-refresh receipt at the Actions publication boundary."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any


MANUAL_NO_OP_STATUSES = {"NO_FINALIZED_WEEKS", "NO_ACTIVE_RENEWAL"}


def write_refresh_receipt(receipt: Mapping[str, Any], path: Path | None) -> None:
    """Keep the previous complete receipt if a later diagnostic rewrite fails."""
    payload = json.dumps(receipt, indent=2, sort_keys=True)
    if path is not None:
        pending = path.with_name(path.name + ".tmp")
        pending.write_text(payload, encoding="utf-8")
        pending.replace(path)
    try:
        print(json.dumps(receipt, sort_keys=True), flush=True)
    except (OSError, ValueError):
        # A closed log sink must not lose an already saved publication receipt.
        pass


def record_publication_commit(
    receipt: dict[str, Any], *, result: Mapping[str, Any], bundle_id: str, path: Path | None,
) -> None:
    """Save a confirmed Fly commit before any post-publication reads or cleanup."""
    if str(result.get("status") or "").upper() != "COMMITTED":
        raise ValueError("scoped refresh did not return a confirmed COMMITTED publication")
    if receipt.get("executed") is not True or not bundle_id:
        raise ValueError("commit receipt requires an executed publication and bundle identity")
    timing_keys = (
        "elapsed_seconds",
        "merge_seconds",
        "server_total_seconds",
        "lock_wait_seconds",
        "season_stage_seconds",
        "timings",
    )
    publication_timing = {
        key: result[key] for key in timing_keys if result.get(key) is not None
    }
    receipt.update(
        status="COMMITTED", bundle_id=bundle_id, data_bundle_id=bundle_id,
        homepage_bundle_id=bundle_id,
    )
    if publication_timing:
        receipt["publication_timing"] = publication_timing
    write_refresh_receipt(receipt, path)


def record_missing_manager_rankings_repair(
    receipt: dict[str, Any],
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    platform: str,
    path: Path | None,
) -> bool:
    """Publish and record the one-table no-op repair shared by all platforms."""
    from multi_league.core.homepage_ranking_repair import (
        repair_missing_manager_rankings_if_needed,
    )
    from multi_league.core.league_update_publish_claim import renew_claim_for_publication

    repair = repair_missing_manager_rankings_if_needed(
        reader=reader,
        db_name=db_name,
        active_year=active_year,
        before_publish=lambda: renew_claim_for_publication(
            reader, database_name=db_name, platform=platform,
        ),
    )
    receipt["manager_rankings_repair"] = {
        key: value for key, value in repair.items() if key != "result"
    }
    if not repair["published"]:
        return False
    receipt["base_generation"] = repair["base_generation"]
    record_publication_commit(
        receipt,
        result=repair["result"],
        bundle_id=repair["bundle_id"],
        path=path,
    )
    receipt["published_tables"] = repair["published_tables"]
    return True


def record_missing_season_rollups_repair(
    receipt: dict[str, Any],
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    platform: str,
    path: Path | None,
) -> bool:
    """Publish the shared no-week historical aggregate repair when required."""
    from multi_league.core.league_update_publish_claim import renew_claim_for_publication
    from multi_league.core.no_week_season_rollup_repair import (
        repair_missing_season_rollups_if_needed,
    )

    repair = repair_missing_season_rollups_if_needed(
        reader=reader,
        db_name=db_name,
        active_year=active_year,
        before_publish=lambda: renew_claim_for_publication(
            reader, database_name=db_name, platform=platform,
        ),
    )
    receipt["season_rollups_repair"] = {
        key: value for key, value in repair.items() if key != "result"
    }
    if not repair["published"]:
        return False
    receipt["base_generation"] = repair["base_generation"]
    record_publication_commit(
        receipt,
        result=repair["result"],
        bundle_id=repair["bundle_id"],
        path=path,
    )
    receipt["season_rollups"] = repair["season_rollups"]
    receipt["career_rollups"] = repair["career_rollups"]
    receipt["published_tables"] = sorted(
        set(receipt.get("published_tables") or ()) | set(repair["published_tables"])
    )
    return True


def classify_publication(receipt: Mapping[str, Any] | None, *, require_publication: bool) -> bool:
    """Never warm cache or mark UI success without an executed Fly commit."""
    if not receipt:
        raise ValueError("refresh publication receipt is missing")
    status = str(receipt.get("status") or "").upper()
    if status == "COMMITTED":
        if receipt.get("executed") is not True:
            raise ValueError("COMMITTED receipt did not execute the publication")
        return True
    if status in MANUAL_NO_OP_STATUSES:
        if require_publication:
            raise ValueError(f"UI league update did not publish: {status}")
        return False
    raise ValueError(f"refresh publication receipt is not valid: {status or 'missing status'}")


def failure_status(receipt: Mapping[str, Any] | None, *, cancelled: bool = False) -> str:
    """Keep an already committed publication recoverable if cache finalization fails."""
    status = str((receipt or {}).get("status") or "").upper()
    if status == "COMMITTED" and (receipt or {}).get("executed") is True and (
        (receipt or {}).get("source_manifest_digest") or (receipt or {}).get("source_fingerprint")
    ):
        return "committed_cache_pending"
    if status == "INCOMPLETE_SOURCE" or status in MANUAL_NO_OP_STATUSES:
        return "incomplete_source"
    if status == "CREDENTIAL_REQUIRED":
        return "credential_required"
    return "cancelled" if cancelled else "failed"


def _read_receipt(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-publication", action="store_true")
    parser.add_argument("--failure-status", action="store_true")
    parser.add_argument("--cancelled", action="store_true")
    args = parser.parse_args(argv)
    receipt = _read_receipt(args.receipt)
    if args.failure_status:
        print(failure_status(receipt, cancelled=args.cancelled))
        return 0
    committed = classify_publication(receipt, require_publication=args.require_publication)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"committed={str(committed).lower()}\n")
            handle.write(f"no_op={str(not committed).lower()}\n")
            if not committed:
                handle.write(f"no_op_status={str((receipt or {}).get('status') or '').upper()}\n")
    print("COMMITTED" if committed else "NO_PUBLICATION")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
