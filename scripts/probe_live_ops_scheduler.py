#!/usr/bin/env python3
"""Probe scheduler infrastructure with no access to database credentials."""

from __future__ import annotations

import argparse
import base64
import importlib.resources
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import TZPATH

from live_ops_clock import GitHub


FLY_API = "https://api.machines.dev/v1"


def request(method: str, path: str, payload: dict | None = None):
    token = os.environ["FLY_API_TOKEN"]
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        FLY_API + path,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        # Do not print responses or request payloads: they can contain secrets.
        raise RuntimeError(f"Fly {method} {path}: HTTP {exc.code}") from None


def timezone_file() -> bytes:
    for directory in TZPATH:
        path = Path(directory) / "America/New_York"
        if path.exists():
            return path.read_bytes()
    return importlib.resources.files("tzdata.zoneinfo").joinpath("America/New_York").read_bytes()


def timing_probe(probe_app: str, *, hosted: bool = False) -> dict:
    github = GitHub(os.environ["GITHUB_DISPATCH_TOKEN"])
    label = f"live-ops-probe-{os.environ['GITHUB_RUN_ID']}"
    if not hosted:
        registration = github.request("POST", "actions/runners/registration-token")
        request("POST", f"/apps/{probe_app}/machines", {
            "name": "timing-runner", "region": "iad",
            "config": {
                "image": "ghcr.io/actions/actions-runner:2.338.0",
                "guest": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 512},
                "services": [], "mounts": [], "auto_destroy": True,
                "restart": {"policy": "no"},
                "env": {"RUNNER_ALLOW_RUNASROOT": "1", "REGISTRATION_TOKEN": registration["token"], "PROBE_LABEL": label},
                "init": {"exec": ["/bin/bash", "-lc",
                    'cd /home/runner && ./config.sh --unattended --url https://github.com/jeleff1000/mfl-league-fetcher --token "$REGISTRATION_TOKEN" --name "$PROBE_LABEL" --labels "$PROBE_LABEL" --ephemeral --disableupdate && exec ./run.sh']},
            },
        })
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            runners = github.request("GET", "actions/runners")["runners"]
            if any(runner["name"] == label and runner["status"] == "online" for runner in runners):
                print(json.dumps({"runner_online": label}), flush=True)
                break
            time.sleep(1)
        else:
            raise RuntimeError("Probe runner did not become online within 40 seconds")

    due = datetime.now(timezone.utc) + timedelta(seconds=15)
    clock_source = Path(__file__).with_name("live_ops_clock.py").read_bytes()
    clock = request("POST", f"/apps/{probe_app}/machines", {
        "name": "timing-clock", "region": "iad",
        "config": {
            "image": "python:3.12-slim",
            "guest": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 256},
            "services": [], "mounts": [], "auto_destroy": True,
            "restart": {"policy": "no"},
            "env": {
                "GITHUB_DISPATCH_TOKEN": os.environ["GITHUB_DISPATCH_TOKEN"],
                "OPS_PROBE_RUNNER": json.dumps(["ubuntu-latest"] if hosted else ["self-hosted", label]),
                "PYTHONTZPATH": "/zoneinfo", "PYTHONUNBUFFERED": "1",
            },
            "files": [
                {"guest_path": "/clock.py", "raw_value": base64.b64encode(clock_source).decode()},
                {"guest_path": "/zoneinfo/America/New_York", "raw_value": base64.b64encode(timezone_file()).decode()},
            ],
            "init": {"exec": ["python", "/clock.py", "--state", "/tmp/clock.json", "--probe-at", due.isoformat()]},
        },
    })
    print(json.dumps({"clock_machine": clock["id"], "scheduled_for": due.isoformat(), "hosted": hosted}), flush=True)
    key = f"ops-probe-{due.date().isoformat()}-{int(due.timestamp())}"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        runs = github.request("GET", "actions/workflows/live_ops_runner_probe.yml/runs?event=repository_dispatch&per_page=20")["workflow_runs"]
        run = next((run for run in runs if key in run.get("display_title", "").split()), None)
        if run and run["status"] == "completed":
            receipt = {"run_id": run["id"], "conclusion": run["conclusion"], "url": run["html_url"], "scheduled_for": due.isoformat()}
            print(json.dumps(receipt), flush=True)
            if run["conclusion"] != "success":
                raise RuntimeError("Clock timing probe did not pass its 60-second start SLA")
            return receipt
        time.sleep(2)
    raise RuntimeError("Clock probe did not complete within 90 seconds")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timing", action="store_true")
    parser.add_argument("--hosted", action="store_true")
    args = parser.parse_args()
    app = request("GET", "/apps/league-history-duckdb")
    org = app["organization"]["slug"]
    print(json.dumps({"app": app["name"], "organization": org, "credential_access": True}), flush=True)
    probe_app = f"lh-ops-probe-{os.environ['GITHUB_RUN_ID']}"
    created = False
    try:
        request("POST", "/apps", {"app_name": probe_app, "org_slug": org, "network": probe_app})
        created = True
        print(json.dumps({"isolated_app_created": probe_app}), flush=True)
        if args.timing:
            timing_probe(probe_app, hosted=args.hosted)
    finally:
        if created:
            request("DELETE", f"/apps/{probe_app}?force=true")
            print(json.dumps({"isolated_app_removed": probe_app}), flush=True)
        if args.timing:
            github = GitHub(os.environ["GITHUB_DISPATCH_TOKEN"])
            label = f"live-ops-probe-{os.environ['GITHUB_RUN_ID']}"
            runners = github.request("GET", "actions/runners")["runners"]
            for runner in runners:
                if runner["name"] == label:
                    github.request("DELETE", f"actions/runners/{runner['id']}")
                    print(json.dumps({"runner_registration_removed": runner["id"]}), flush=True)


if __name__ == "__main__":
    main()
