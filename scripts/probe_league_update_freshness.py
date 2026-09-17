#!/usr/bin/env python3
"""Capture the production freshness manifest for a manual league update."""

from __future__ import annotations

import argparse
from collections.abc import Callable
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


DB_NAME_RE = re.compile(r"^[a-z0-9_]+$")


def probe_freshness(
    database_name: str,
    *,
    base_url: str = "https://www.leaguehistory.app",
    timeout: float = 20.0,
    opener: Callable[..., Any] = urlopen,
) -> str:
    if not DB_NAME_RE.fullmatch(database_name):
        raise ValueError("Invalid database name")
    origin = f"{urlsplit(base_url).scheme}://{urlsplit(base_url).netloc}"
    request = Request(
        f"{base_url.rstrip('/')}/api/league/{quote(database_name)}/update/probe",
        method="POST",
        headers={"Accept": "application/json", "Origin": origin},
    )
    try:
        with opener(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"Freshness probe failed ({exc.code}): {detail}") from exc
    digest = str(payload.get("observed_manifest_digest") or "").strip()
    if payload.get("healthy") is not True or payload.get("stale") is True or not digest:
        raise RuntimeError("Freshness probe did not return a healthy source manifest")
    return digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--base-url", default="https://www.leaguehistory.app")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    digest = probe_freshness(
        args.db,
        base_url=args.base_url,
        timeout=args.timeout,
    )
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        raise RuntimeError("GITHUB_OUTPUT is required")
    with Path(output_path).open("a", encoding="utf-8") as output:
        output.write(f"digest={digest}\n")
    print("Captured guarded production freshness manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
