#!/usr/bin/env python3
"""Track the daily NFL refresh through its Fly verification receipt."""

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
ALERT_EVENT = "live-nfl-ops-refresh-exhausted"
PROBE_EVENT = "live-nfl-ops-clock-probe"
SEASON_MONTHS = {1, 2, 9, 10, 11, 12}
RETRY_DELAYS = (timedelta(minutes=10), timedelta(minutes=30))
DISPATCH_GRACE = timedelta(minutes=1)
RUN_CREATION_LIMIT = timedelta(minutes=10)


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
        page = 1
        while True:
            runs = self.request("GET", f"actions/runs?{params}&page={page}")["workflow_runs"]
            if any(key in run.get("display_title", "").split() for run in runs):
                return True
            if len(runs) < 100:
                return False
            page += 1

    def clock_runs(self, key: str, due: datetime, *, alert: bool = False) -> list[dict]:
        workflow = "notify_live_nfl_ops.yml" if alert else "refresh_live_nfl_ops.yml"
        params = urllib.parse.urlencode({
            "event": "repository_dispatch",
            "created": ">=" + due.astimezone(timezone.utc).isoformat(),
            "per_page": 100,
        })
        runs = self.request("GET", f"actions/workflows/{workflow}/runs?{params}")["workflow_runs"]
        return [run for run in runs if key in run.get("display_title", "").split()]

    def has_receipt(self, run: dict, game_date: str) -> bool:
        name = f"live-nfl-ops-complete-{game_date}-{run['run_attempt']}"
        result = self.request("GET", f"actions/runs/{run['id']}/artifacts?name={name}")
        return any(artifact["name"] == name and not artifact["expired"] for artifact in result["artifacts"])

    def check_receipt_access(self) -> None:
        result = self.request("GET", "actions/workflows/refresh_live_nfl_ops.yml/runs?per_page=1")
        run = result["workflow_runs"][0]
        self.request("GET", f"actions/runs/{run['id']}/artifacts?per_page=1")


class Clock:
    def __init__(self, state_path: Path, github: GitHub):
        self.state_path = state_path
        self.github = github
        self.state = json.loads(state_path.read_text()) if state_path.exists() else {}
        if not self.state and os.environ.get("CLOCK_BOOTSTRAP_KEY"):
            self.state = {"acknowledged_key": os.environ["CLOCK_BOOTSTRAP_KEY"]}
            self.save()

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
        state = self.state.get("probe", {}) if probe else self.state

        def record(value: dict) -> None:
            if probe:
                self.state["probe"] = value
            else:
                self.state = {**value, **({"probe": self.state["probe"]} if "probe" in self.state else {})}
            self.save()

        if state.get("acknowledged_key") == key:
            return False
        if state.get("pending_key") == key:
            if self.github.accepted(key, due):
                record({"acknowledged_key": key, "acknowledged_at": datetime.now(timezone.utc).isoformat()})
                print(json.dumps({"dispatch_key": key, "recovered_acknowledgment": True}), flush=True)
                return False
            # GitHub run creation is asynchronous. Give it one full minute to
            # expose an accepted request before considering a same-key retry.
            pending_since = instant(state["pending_since"])
            if (datetime.now(timezone.utc) - pending_since).total_seconds() < 60:
                return False
        record({"pending_key": key, "pending_since": datetime.now(timezone.utc).isoformat(), "pending_due": due.isoformat()})
        self.github.request("POST", "dispatches", payload)
        acknowledged_at = datetime.now(timezone.utc).isoformat()
        record({"acknowledged_key": key, "acknowledged_at": acknowledged_at})
        print(json.dumps({**payload["client_payload"], "acknowledged_at": acknowledged_at}), flush=True)
        return True

    def pending_due(self, now: datetime) -> datetime:
        if "refresh" in self.state:
            return instant(self.state["refresh"]["due"])
        if self.state.get("pending_due"):
            return instant(self.state["pending_due"])
        key = self.state.get("acknowledged_key", "")
        if key.startswith("ops-"):
            following = due_on(date.fromisoformat(key.removeprefix("ops-")) + timedelta(days=1))
            while following.month not in SEASON_MONTHS:
                following = due_on(following.date() + timedelta(days=1))
            if following <= now:
                return following
        return latest_due(now)

    def refresh(self, due: datetime, now: datetime) -> None:
        payload = payload_for(due)
        fields = payload["client_payload"]
        key, game_date = fields["dispatch_key"], fields["game_date"]
        if self.state.get("acknowledged_key") == key:
            return
        if "refresh" not in self.state:
            self.state["refresh"] = {"due": due.isoformat(), "attempt": 1}
            self.save()
        refresh = self.state["refresh"]
        runs = self.github.clock_runs(key, due)
        if any(run["status"] != "completed" for run in runs):
            return
        if any(self.github.has_receipt(run, game_date) for run in runs):
            self.finish_refresh(key, now)
            return

        attempt = refresh["attempt"]
        if attempt > len(RETRY_DELAYS) + 1:
            alerts = self.github.clock_runs(key, due, alert=True)
            if any(run["conclusion"] == "success" for run in alerts):
                self.finish_refresh(key, now)
                return
            if any(run["status"] != "completed" for run in alerts):
                return
            last_alert = refresh.get("alert_sent_at")
            if last_alert and now - instant(last_alert) < RUN_CREATION_LIMIT:
                return
            refresh["alert_sent_at"] = now.isoformat()
            self.save()
            self.github.request("POST", "dispatches", {
                "event_type": ALERT_EVENT,
                "client_payload": {
                    "dispatch_key": key,
                    "game_date": game_date,
                    "run_ids": sorted({run["id"] for run in runs}),
                },
            })
            return

        if "retry_at" in refresh and now < instant(refresh["retry_at"]):
            return
        slot = [run for run in runs if f"attempt-{attempt}" in run["display_title"].split()]
        # A legacy dispatch interrupted by the clock upgrade still owns attempt one.
        if attempt == 1:
            slot.extend(run for run in runs if " attempt-" not in run["display_title"])
        started = refresh.get("slot_started_at")
        if slot or (started and now - instant(started) >= RUN_CREATION_LIMIT):
            failed_at = max(instant(run["updated_at"]) for run in slot) if slot else now
            self.state["refresh"] = {"due": due.isoformat(), "attempt": attempt + 1}
            if attempt <= len(RETRY_DELAYS):
                retry_at = failed_at + RETRY_DELAYS[attempt - 1]
                self.state["refresh"]["retry_at"] = retry_at.isoformat()
            self.save()
            print(json.dumps({"dispatch_key": key, "failed_attempt": attempt, **self.state["refresh"]}), flush=True)
            if attempt > len(RETRY_DELAYS) or now >= failed_at + RETRY_DELAYS[attempt - 1]:
                self.refresh(due, now)
            return
        sent_at = refresh.get("sent_at")
        if refresh.get("accepted") or (sent_at and now - instant(sent_at) < DISPATCH_GRACE):
            return
        refresh.setdefault("slot_started_at", now.isoformat())
        refresh["sent_at"] = now.isoformat()
        self.save()
        fields["refresh_attempt"] = attempt
        self.github.request("POST", "dispatches", payload)
        refresh["accepted"] = True
        self.save()
        print(json.dumps(fields), flush=True)

    def finish_refresh(self, key: str, now: datetime) -> None:
        for field in ("refresh", "pending_key", "pending_since", "pending_due"):
            self.state.pop(field, None)
        self.state.update(acknowledged_key=key, acknowledged_at=now.isoformat())
        self.save()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path("/data/clock.json"))
    parser.add_argument("--probe-at", type=instant, help="one no-write probe at this instant, then exit")
    args = parser.parse_args()
    github = GitHub(os.environ["GITHUB_DISPATCH_TOKEN"])
    clock = Clock(args.state, github)
    startup = {"next_scheduled_for": next_due(datetime.now(timezone.utc)).isoformat(),
               "probe": bool(args.probe_at), "revision": os.environ.get("CLOCK_REVISION")}
    if not args.probe_at:
        try:
            github.check_receipt_access()
            startup["github_receipt_access"] = True
        except (RuntimeError, OSError) as exc:
            startup["github_receipt_access"] = False
            startup["startup_error"] = str(exc)
    print(json.dumps(startup), flush=True)
    while True:
        now = datetime.now(timezone.utc)
        due = args.probe_at or clock.pending_due(now)
        if now >= due and (args.probe_at or "refresh" in clock.state or due.month in SEASON_MONTHS):
            try:
                if args.probe_at:
                    clock.dispatch(due, probe=True)
                else:
                    clock.refresh(due, now)
                if args.probe_at and clock.state.get("probe", {}).get("acknowledged_key"):
                    return
            except (RuntimeError, OSError) as exc:
                print(json.dumps({"dispatch_error": str(exc), "retry_in_seconds": 10}), flush=True)
                time.sleep(10)
                continue
        wake_at = args.probe_at or next_due(now)
        poll_seconds = 30 if "refresh" in clock.state else 5
        time.sleep(min(poll_seconds, max(0.1, (wake_at - now).total_seconds())))


if __name__ == "__main__":
    main()
