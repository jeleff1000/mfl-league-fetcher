#!/usr/bin/env python3
"""Finish cache publication for a durably committed league update, without refetching."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.league_update_manifest import (  # noqa: E402
    manifest_digest,
    source_manifest_from_mapping,
)
from multi_league.core.league_update_status import (  # noqa: E402
    assert_league_update_entitled,
    build_cache_recovery_receipt,
    record_league_update_status,
)
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _current_generation(reader: FlyReader, db: str) -> int:
    value = reader.query_scalar(
        "SELECT COALESCE(MAX(generation), 0) "
        "FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {_literal(db)}",
        database="___leagues",
    )
    return int(value or 0)


def _load_claim(reader: FlyReader, args: argparse.Namespace, *, allow_running: bool = False) -> dict:
    claim_predicate = (
        "AND d.dispatch_token LIKE 'manual-%' AND d.attempt_id = d.dispatch_token "
        if not args.dispatch_token else
        f"AND d.dispatch_token = {_literal(args.dispatch_token)} "
        f"AND d.attempt_id = {_literal(args.attempt_id)} "
        f"AND d.claim_version = {args.claim_version} "
    )
    statuses = "'committed', 'committed_cache_pending'"
    if allow_running:
        statuses += ", 'running'"
    rows = reader.query(
        "SELECT d.database_name, d.platform, d.status, d.dispatch_token, "
        "d.attempt_id, d.claim_version, d.workflow_run_id, d.source_year, "
        "d.source_week, d.source_fingerprint, d.bundle_id, d.base_generation, "
        "d.publication_receipt_json, "
        "m.published_manifest_digest, m.published_manifest_json "
        "FROM accounts.league_update_dispatches d "
        "LEFT JOIN accounts.league_update_manifests m "
        "ON m.database_name = d.database_name "
        f"WHERE d.database_name = {_literal(args.db)} "
        f"{claim_predicate}"
        f"AND d.platform = {_literal(args.platform)} "
        f"AND d.status IN ({statuses}) "
        "AND d.cache_verified_at IS NULL LIMIT 1",
        database="___ops",
    )
    if len(rows) != 1:
        raise RuntimeError("Committed league update recovery claim is unavailable")
    return rows[0]


def _validated_original_receipt(reader: FlyReader, row: dict, path: Path) -> dict:
    """Match a captured worker receipt to its atomic fleet commit and current generation."""
    receipt = json.loads(path.read_text(encoding="utf-8"))
    db_name = row["database_name"]
    if not isinstance(receipt, dict) or receipt.get("status") != "COMMITTED" \
       or receipt.get("executed") is not True or receipt.get("db_name") != db_name:
        raise ValueError("Original receipt is not an executed commit for this league")
    bundle_id = str(receipt.get("bundle_id") or "")
    if not re.fullmatch(r"fleet-[0-9a-f]{64}", bundle_id):
        raise ValueError("Original receipt has no valid fleet bundle identity")
    bundle_hash = bundle_id.removeprefix("fleet-")
    base = receipt.get("base_generation")
    year = receipt.get("source_year")
    week = receipt.get("source_week")
    if type(base) is not int or base < 0 or type(year) is not int or year < 1 \
       or type(week) is not int or week < 1:
        raise ValueError("Original receipt has invalid publication scope or generation")
    if type(receipt.get("source_manifest_complete")) is not bool:
        raise ValueError("Original receipt has no source completeness evidence")
    source = json.loads(str(receipt.get("source_manifest_json") or ""))
    if not isinstance(source, dict):
        raise ValueError("Original receipt source manifest is invalid")
    captured = source_manifest_from_mapping(source)
    if manifest_digest(captured) != receipt.get("source_manifest_digest"):
        raise ValueError("Original receipt source manifest digest does not match its payload")
    if captured.database_name != db_name or captured.active_season != year:
        raise ValueError("Original receipt source manifest has a different league or season")
    if row.get("bundle_id") and row["bundle_id"] != bundle_id:
        raise ValueError("Original claim already records a different publication")
    run_id = str(row.get("workflow_run_id") or "")
    if not run_id.isdigit() or int(run_id) < 1:
        raise ValueError("Original claim has no workflow run identity")
    commits = reader.query(
        "SELECT db_name, bundle_id, bundle_hash, status, import_run_id, manifest_json, result_json "
        "FROM merge_admin.league_delta_merge_state "
        f"WHERE db_name = '___fleet' AND bundle_id = {_literal(bundle_id)}",
        database="___leagues",
    )
    if len(commits) != 1:
        raise ValueError("Expected one durable fleet commit for the original receipt")
    commit = commits[0]
    if commit.get("status") != "COMMITTED" or commit.get("bundle_id") != bundle_id \
       or commit.get("bundle_hash") != bundle_hash or str(commit.get("import_run_id")) != run_id:
        raise ValueError("Durable fleet commit does not match the original claim and receipt")
    manifest = json.loads(str(commit.get("manifest_json") or ""))
    result = json.loads(str(commit.get("result_json") or ""))
    if not isinstance(manifest, dict) or not isinstance(result, dict):
        raise ValueError("Durable fleet publication evidence is invalid")
    for evidence in (manifest, result):
        if evidence.get("db_name") != "___fleet" or evidence.get("bundle_id") != bundle_id \
           or evidence.get("bundle_hash") != bundle_hash:
            raise ValueError("Durable fleet publication identity does not match")
    if result.get("status") != "COMMITTED" or str(manifest.get("import_run_id")) != run_id \
       or manifest.get("db_names") != [db_name] or manifest.get("active_year") != year \
       or manifest.get("league_generations") != {db_name: base}:
        raise ValueError("Durable fleet publication scope does not match the original receipt")
    generations = reader.query(
        "SELECT generation, lane, run_id FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {_literal(db_name)}",
        database="___leagues",
    )
    if len(generations) != 1 or generations[0].get("generation") != base + 1 \
       or generations[0].get("lane") != "fleet" or str(generations[0].get("run_id")) != run_id:
        raise ValueError("A different publication superseded the original fleet commit")
    return receipt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--platform", required=True, choices=("yahoo", "espn", "sleeper"))
    parser.add_argument("--dispatch-token", default="")
    parser.add_argument("--attempt-id", default="")
    parser.add_argument("--claim-version", type=int, default=0)
    parser.add_argument("--receipt", type=Path, help="Original worker receipt to reconcile against its durable fleet commit")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-z0-9_]{1,63}", args.db):
        raise ValueError("Invalid league database name")
    manual_pending = not args.dispatch_token and not args.attempt_id and args.claim_version == 0
    if not manual_pending and (not args.dispatch_token or not args.attempt_id or args.claim_version < 1):
        raise ValueError("Invalid cache recovery claim")
    if args.receipt is not None and manual_pending:
        raise ValueError("Receipt reconciliation requires the explicit original claim")

    reader = FlyReader()
    assert_league_update_entitled(reader, database_name=args.db)
    row = _load_claim(reader, args, allow_running=args.receipt is not None)
    if manual_pending:
        token = str(row.get("dispatch_token") or "")
        if not re.fullmatch(r"manual-[1-9][0-9]*-[1-9][0-9]*", token) \
           or str(row.get("attempt_id") or "") != token \
           or int(row.get("claim_version") or 0) < 1:
            raise RuntimeError("Committed league update recovery claim is unavailable")
        args.dispatch_token = token
        args.attempt_id = token
        args.claim_version = int(row["claim_version"])
    secret = os.environ.get("REVALIDATION_SECRET")
    if not secret:
        raise RuntimeError("Cache revalidation secret is unavailable")
    if args.receipt is not None:
        receipt = _validated_original_receipt(reader, row, args.receipt)
        claimed = record_league_update_status(
            FlyWriter(), database_name=args.db, platform=args.platform,
            status="committed_cache_pending", dispatch_token=args.dispatch_token,
            attempt_id=args.attempt_id, claim_version=args.claim_version,
            workflow_run_id=row["workflow_run_id"], receipt=receipt,
        )
        if not claimed:
            raise RuntimeError("League update reconciliation lost its publication claim")
        # Read the durable OPS result; never warm from a fabricated committed row.
        row = _load_claim(reader, args)
    receipt = build_cache_recovery_receipt(
        row, current_generation=_current_generation(reader, args.db)
    )
    subprocess.run(
        [
            sys.executable, str(REPO_ROOT / "scripts" / "warm_vercel_cache.py"),
            "--db", args.db, "--secret", secret,
            "--site-url", "https://www.leaguehistory.app",
            "--mode", "quick", "--strategy", "expire",
            "--jitter-seconds", "0", "--strict", "--verify-hot",
            "--required-only", "--timeout", "5", "--warm-attempts", "1",
            "--hot-verify-attempts", "1",
        ],
        check=True,
    )
    # Do not mark this old publication current if another import committed while warming.
    build_cache_recovery_receipt(
        row, current_generation=_current_generation(reader, args.db)
    )
    claimed = record_league_update_status(
        FlyWriter(),
        database_name=args.db,
        platform=args.platform,
        status="succeeded",
        dispatch_token=args.dispatch_token,
        attempt_id=args.attempt_id,
        claim_version=args.claim_version,
        workflow_run_id=row.get("workflow_run_id"),
        receipt=receipt,
        cache_verified=True,
    )
    if not claimed:
        raise RuntimeError("League update cache recovery lost its publication claim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
