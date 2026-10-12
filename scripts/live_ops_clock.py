#!/usr/bin/env python3
"""A small Eastern-time clock which only dispatches the public Ops worker."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
REPOSITORY = "jeleff1000/mfl-league-fetcher"
EVENT = "live-nfl-ops-refresh"
PROBE_EVENT = "live-nfl-ops-clock-probe"
SEASON_MONTHS = {1, 2, 9, 10, 11, 12}


def instant(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return result


def due_on(day: date) -> datetime:
    return datetime.combine(day, wall_time(3), tzinfo=EASTERN)


def latest_due(now: datetime) -> datetime:
    local = now.astimezone(EASTERN)
    due = due_on(local.date())
    return due if now >= due else due_on(local.date() - timedelta(days=1))


def next_due(now: datetime) -> datetime:
    local = now.astimezone(EASTERN)
    due = due_on(local.date())
    return due if now < due else due_on(local.date() + timedelta(days=1))


def payload_for(due: datetime, *, probe: bool = False) -> dict:
    local = due.astimezone(EASTERN)
    key = f"ops-{'probe-' if probe else ''}{local.date().isoformat()}"
    if probe:
        key += f"-{int(due.timestamp())}"
    return {
        "event_type": PROBE_EVENT if probe else EVENT,
        "client_payload": {
            "dispatch_key": key,
            "scheduled_for": due.isoformat(),
            "dispatched_at": datetime.now(timezone.utc).isoformat(),
            "game_date": (local.date() - timedelta(days=1)).isoformat(),
            **({"runner": os.environ.get("OPS_PROBE_RUNNER", '["ubuntu-latest"]')} if probe else {}),
        },
    }


class GitHub:
    def __init__(self, token: str):
        self.token = token

    def request(self, method: str, suffix: str, payload: dict | None = None):
        req = urllib.request.Request(
            f"https://api.github.com/repos/{REPOSITORY}/{suffix}",
            data=None if payload is None else json.dumps(payload).encode(),
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "league-history-ops-clock",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"GitHub {method} {suffix}: HTTP {exc.code}") from None

    def accepted(self, key: str, due: datetime) -> bool:
        # Resolve a response lost after GitHub accepted the dispatch before
        # submitting another request. The worker independently deduplicates keys.
        params = urllib.parse.urlencode({
            "event": "repository_dispatch",
            "created": ">=" + due.astimezone(timezone.utc).isoformat(),
            "per_page": 100,
        })
        runs = self.request("GET", f"actions/runs?{params}")["workflow_runs"]
        return any(key in run.get("display_title", "").split() for run in runs)


class Clock:
    def __init__(self, state_path: Path, github: GitHub):
        self.state_path = state_path
        self.github = github
        self.state = json.loads(state_path.read_text()) if state_path.exists() else {}

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        staged = self.state_path.with_suffix(".pending")
        with staged.open("w", encoding="utf-8") as handle:
            json.dump(self.state, handle)
            handle.flush()
            os.fsync(handle.fileno())
        staged.replace(self.state_path)

    def dispatch(self, due: datetime, *, probe: bool = False) -> bool:
        payload = payload_for(due, probe=probe)
        key = payload["client_payload"]["dispatch_key"]
        if self.state.get("acknowledged_key") == key:
            return False
        if self.state.get("pending_key") == key:
            if self.github.accepted(key, due):
                self.state = {"acknowledged_key": key, "acknowledged_at": datetime.now(timezone.utc).isoformat()}
                self.save()
                print(json.dumps({"dispatch_key": key, "recovered_acknowledgment": True}), flush=True)
                return False
            # GitHub run creation is asynchronous. Give it one full minute to
            # expose an accepted request before considering a same-key retry.
            pending_since = instant(self.state["pending_since"])
            if (datetime.now(timezone.utc) - pending_since).total_seconds() < 60:
                return False
        self.state = {"pending_key": key, "pending_since": datetime.now(timezone.utc).isoformat()}
        self.save()
        self.github.request("POST", "dispatches", payload)
        self.state = {"acknowledged_key": key, "acknowledged_at": datetime.now(timezone.utc).isoformat()}
        self.save()
        print(json.dumps({**payload["client_payload"], "acknowledged_at": self.state["acknowledged_at"]}), flush=True)
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path("/data/clock.json"))
    parser.add_argument("--probe-at", type=instant, help="one no-write probe at this instant, then exit")
    args = parser.parse_args()
    github = GitHub(os.environ["GITHUB_DISPATCH_TOKEN"])
    clock = Clock(args.state, github)
    print(json.dumps({"next_scheduled_for": next_due(datetime.now(timezone.utc)).isoformat(), "probe": bool(args.probe_at)}), flush=True)
    while True:
        now = datetime.now(timezone.utc)
        due = args.probe_at or latest_due(now)
        if now >= due and (args.probe_at or due.month in SEASON_MONTHS):
            try:
                clock.dispatch(due, probe=bool(args.probe_at))
                if args.probe_at and clock.state.get("acknowledged_key"):
                    return
            except (RuntimeError, OSError) as exc:
                print(json.dumps({"dispatch_error": str(exc), "retry_in_seconds": 10}), flush=True)
                time.sleep(10)
                continue
        time.sleep(min(5, max(0.1, (next_due(now) - now).total_seconds())))


if __name__ == "__main__":
    main()
