#!/usr/bin/env python3
"""
Rebuild centralized aggregate tables for a league.

Runs the standard aggregation pipeline modules in a safe order:
  1) matchup context
  2) fantasy context
  3) draft context
  4) transaction context
  5) standings
  6) homepage summary

Examples:
  python scripts/refresh_aggregates.py --db demo_league
  python scripts/refresh_aggregates.py --context config/league_context.json
  python scripts/refresh_aggregates.py --db demo_league --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass


PIPELINE_MODULES = [
    "multi_league.transformations.aggregation.aggregate_matchup_context",
    "multi_league.transformations.aggregation.aggregate_fantasy_context",
    "multi_league.transformations.aggregation.aggregate_draft_context",
    "multi_league.transformations.aggregation.aggregate_transaction_context",
    "multi_league.transformations.aggregation.aggregate_standings",
    "multi_league.transformations.aggregation.homepage_summary",
]


@dataclass(frozen=True)
class StepResult:
    module: str
    return_code: int


def _require_token(*, data_dir: str | None = None) -> None:
    """Require legacy MotherDuck credentials only for a legacy remote run.

    ``--data-dir`` is an explicit local DuckDB target.  The aggregation
    modules receive that path and must not be blocked by credentials for a
    database they will not contact.
    """
    if data_dir or os.environ.get("DATABASE_BACKEND") in {"fly", "local"}:
        return
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        raise SystemExit("MOTHERDUCK_TOKEN is not set (set DATABASE_BACKEND=fly for Fly)")


def _build_base_args(args: argparse.Namespace) -> list[str]:
    base: list[str] = []
    if args.db:
        base.extend(["--db", args.db])
    if args.context:
        base.extend(["--context", args.context])
    if args.data_dir:
        base.extend(["--data-dir", args.data_dir])
    if args.dry_run:
        base.append("--dry-run")
    return base


def _run_step(module: str, base_args: list[str]) -> StepResult:
    cmd = [sys.executable, "-m", module, *base_args]
    print(f"\n[agg] {module}")
    result = subprocess.run(cmd, check=False)
    return StepResult(module=module, return_code=result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild centralized aggregate tables for a league.")
    parser.add_argument("--db", help="League database name (e.g., demo_league)")
    parser.add_argument("--context", help="Path to league_context.json (alternative to --db)")
    parser.add_argument("--data-dir", help="Optional local data dir (runs against local DuckDB file)")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing")
    parser.add_argument(
        "--steps",
        help="Comma-separated subset of steps to run (module name suffixes, e.g. matchup,fantasy,homepage).",
    )
    args = parser.parse_args()

    if not args.db and not args.context:
        raise SystemExit("Provide either --db or --context")

    _require_token(data_dir=args.data_dir)

    selected = PIPELINE_MODULES
    if args.steps:
        allow = {s.strip().lower() for s in args.steps.split(",") if s.strip()}
        selected = [m for m in PIPELINE_MODULES if any(key in m.split(".")[-1] for key in allow)]
        if not selected:
            raise SystemExit(f"No aggregation steps matched: {sorted(allow)}")

    base_args = _build_base_args(args)
    results: list[StepResult] = []
    for module in selected:
        results.append(_run_step(module, base_args))

    failures = [r for r in results if r.return_code != 0]
    if failures:
        print("\n[agg] Failures:")
        for r in failures:
            print(f"  - {r.module} (exit {r.return_code})")
        raise SystemExit(1)
    print("\n[agg] All steps completed successfully.")


if __name__ == "__main__":
    main()
