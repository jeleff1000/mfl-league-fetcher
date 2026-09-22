#!/usr/bin/env python3
"""Authorize a live NFL Ops worker invocation at the GitHub boundary.

Manual dispatches are deliberately unrestricted so an operator can repair a
missed final slate.  Automated repository dispatches are accepted only during
the 01:30–05:00 America/New_York service window; the downstream NFLverse scope
gate still decides whether finalized REG or POST data exists to publish.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def should_run(event: str, now: datetime) -> bool:
    """Return whether this trigger may enter the live Ops refresh path."""
    normalized_event = event.strip()
    if normalized_event == "workflow_dispatch":
        return True
    if normalized_event not in {"repository_dispatch", "schedule"}:
        return False

    eastern_now = now.astimezone(EASTERN)
    start = eastern_now.replace(hour=1, minute=30, second=0, microsecond=0)
    end = eastern_now.replace(hour=5, minute=0, second=0, microsecond=0)
    return start <= eastern_now < end


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, help="GitHub event name")
    parser.add_argument("--now", help="ISO-8601 instant for deterministic validation")
    parser.add_argument("--github-output", type=Path, help="optional GitHub step output file")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.now:
        now = datetime.fromisoformat(args.now)
        if now.tzinfo is None:
            raise ValueError("--now must include a UTC offset")
    else:
        now = datetime.now(EASTERN)
    value = "true" if should_run(args.event, now) else "false"
    if args.github_output:
        args.github_output.parent.mkdir(parents=True, exist_ok=True)
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"should_run={value}\n")
    print(f"should_run={value}")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI
    raise SystemExit(main())
