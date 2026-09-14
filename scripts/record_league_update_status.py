#!/usr/bin/env python3
"""Record a UI-triggered active-season refresh lifecycle state in Fly."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.league_update_status import (  # noqa: E402
    assert_league_update_entitled,
    record_league_update_status,
)
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--platform", required=True, choices=("yahoo", "espn", "sleeper"))
    parser.add_argument("--status", required=True, choices=("running", "succeeded", "failed"))
    parser.add_argument("--dispatch-token", required=True)
    parser.add_argument("--workflow-run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--cache-verified", action="store_true")
    parser.add_argument("--require-entitled", action="store_true")
    parser.add_argument("--error")
    args = parser.parse_args(argv)

    if args.require_entitled:
        assert_league_update_entitled(FlyReader(), database_name=args.db)
    receipt = None
    if args.receipt:
        receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    claimed = record_league_update_status(
        FlyWriter(),
        database_name=args.db,
        platform=args.platform,
        status=args.status,
        dispatch_token=args.dispatch_token,
        workflow_run_id=args.workflow_run_id,
        receipt=receipt,
        cache_verified=args.cache_verified,
        error=args.error,
    )
    if not claimed:
        raise RuntimeError("Worker no longer owns this league update claim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
