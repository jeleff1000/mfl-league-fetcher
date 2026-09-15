#!/usr/bin/env python3
"""Finish cache publication for a durably committed league update, without refetching."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--platform", required=True, choices=("yahoo", "espn", "sleeper"))
    parser.add_argument("--dispatch-token", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--claim-version", required=True, type=int)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[a-z0-9_]{1,63}", args.db):
        raise ValueError("Invalid league database name")
    if not args.dispatch_token or not args.attempt_id or args.claim_version < 1:
        raise ValueError("Invalid cache recovery claim")

    reader = FlyReader()
    assert_league_update_entitled(reader, database_name=args.db)
    rows = reader.query(
        "SELECT d.database_name, d.platform, d.status, d.dispatch_token, "
        "d.attempt_id, d.claim_version, d.workflow_run_id, d.source_year, "
        "d.source_week, d.source_fingerprint, d.bundle_id, d.base_generation, "
        "d.publication_receipt_json, "
        "m.published_manifest_digest, m.published_manifest_json "
        "FROM accounts.league_update_dispatches d "
        "JOIN accounts.league_update_manifests m "
        "ON m.database_name = d.database_name "
        f"WHERE d.database_name = {_literal(args.db)} "
        f"AND d.dispatch_token = {_literal(args.dispatch_token)} "
        f"AND d.attempt_id = {_literal(args.attempt_id)} "
        f"AND d.claim_version = {args.claim_version} "
        f"AND d.platform = {_literal(args.platform)} "
        "AND d.status = 'committed_cache_pending' LIMIT 1",
        database="___ops",
    )
    if len(rows) != 1:
        raise RuntimeError("Committed league update recovery claim is unavailable")
    row = rows[0]
    receipt = build_cache_recovery_receipt(
        row, current_generation=_current_generation(reader, args.db)
    )
    secret = os.environ.get("REVALIDATION_SECRET")
    if not secret:
        raise RuntimeError("Cache revalidation secret is unavailable")
    subprocess.run(
        [
            sys.executable, str(REPO_ROOT / "scripts" / "warm_vercel_cache.py"),
            "--db", args.db, "--secret", secret,
            "--site-url", "https://www.leaguehistory.app",
            "--mode", "quick", "--strategy", "expire",
            "--jitter-seconds", "0", "--strict", "--verify-hot",
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
