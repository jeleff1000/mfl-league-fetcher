#!/usr/bin/env python3
"""Probe scheduler infrastructure with no access to database credentials."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


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


def main() -> None:
    app = request("GET", "/apps/league-history-duckdb")
    org = app["organization"]["slug"]
    print(json.dumps({"app": app["name"], "organization": org, "credential_access": True}), flush=True)
    probe_app = f"lh-ops-probe-{os.environ['GITHUB_RUN_ID']}"
    created = False
    try:
        request("POST", "/apps", {"app_name": probe_app, "org_slug": org, "network": probe_app})
        created = True
        print(json.dumps({"isolated_app_created": probe_app}), flush=True)
    finally:
        if created:
            request("DELETE", f"/apps/{probe_app}?force=true")
            print(json.dumps({"isolated_app_removed": probe_app}), flush=True)


if __name__ == "__main__":
    main()
