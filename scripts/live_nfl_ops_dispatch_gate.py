#!/usr/bin/env python3
"""Authorize a live NFL Ops worker invocation at the GitHub boundary.

Manual dispatches are deliberately unrestricted so an operator can repair a
missed final slate. Scheduled runs trust the cron event because GitHub may
start them hours late. Dated clock dispatches retain their original date even
when delayed; successful or pending earlier runs own the same daily key.
Legacy automated repository dispatches are accepted only during
the 01:30–05:00 America/New_York service window; the downstream NFLverse scope
gate still decides whether finalized REG or POST data exists to publish.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


def should_run(event: str, now: datetime, *, scheduled_for: str = "", dispatch_key: str = "") -> bool:
    """Return whether this trigger may enter the live Ops refresh path."""
    normalized_event = event.strip()
    if normalized_event in {"workflow_dispatch", "schedule"}:
        return True
    if normalized_event != "repository_dispatch":
        return False

    if scheduled_for or dispatch_key:
        try:
            due = datetime.fromisoformat(scheduled_for.replace("Z", "+00:00"))
            if due.tzinfo is None:
                return False
            eastern_due = due.astimezone(EASTERN)
            expected_key = f"ops-{eastern_due.date().isoformat()}"
            return (
                dispatch_key == expected_key
                and (eastern_due.hour, eastern_due.minute, eastern_due.second) == (3, 0, 0)
                and due <= now
            )
        except ValueError:
            return False

    eastern_now = now.astimezone(EASTERN)
    start = eastern_now.replace(hour=1, minute=30, second=0, microsecond=0)
    end = eastern_now.replace(hour=5, minute=0, second=0, microsecond=0)
    return start <= eastern_now < end


def is_duplicate(dispatch_key: str, run_id: int, runs: list[dict]) -> bool:
    return any(
        int(run["id"]) < run_id
        and dispatch_key in run.get("display_title", "").split()
        and (run["status"] != "completed" or run.get("conclusion") == "success")
        for run in runs
    )


def previous_clock_runs() -> list[dict]:
    request = urllib.request.Request(
        "https://api.github.com/repos/jeleff1000/mfl-league-fetcher/actions/workflows/refresh_live_nfl_ops.yml/runs?event=repository_dispatch&per_page=100",
        headers={"Authorization": f"Bearer {os.environ['GH_TOKEN']}", "Accept": "application/vnd.github+json", "User-Agent": "league-history-ops-clock"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)["workflow_runs"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True, help="GitHub event name")
    parser.add_argument("--now", help="ISO-8601 instant for deterministic validation")
    parser.add_argument("--scheduled-for", default="", help="clock's original Eastern 03:00 instant")
    parser.add_argument("--dispatch-key", default="", help="daily key from the authenticated clock event")
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
    allowed = should_run(args.event, now, scheduled_for=args.scheduled_for, dispatch_key=args.dispatch_key)
    if allowed and args.dispatch_key:
        if is_duplicate(args.dispatch_key, int(os.environ["GITHUB_RUN_ID"]), previous_clock_runs()):
            print(f"duplicate_clock_dispatch={args.dispatch_key}")
            allowed = False
    value = "true" if allowed else "false"
    if args.github_output:
        args.github_output.parent.mkdir(parents=True, exist_ok=True)
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write(f"should_run={value}\n")
    print(f"should_run={value}")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI
    raise SystemExit(main())
