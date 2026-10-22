#!/usr/bin/env python3
"""Capture the production freshness manifest for a manual league update."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import re
from typing import Any
from time import monotonic, sleep, time
from threading import Timer
from urllib.error import HTTPError, URLError
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
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("Freshness probe timeout must be positive and finite")
    # Keep recovery inside the existing two-minute worker cap and reserve the
    # same final 30 seconds for publication/cache finalization as Fly reads.
    budget = 30.0
    try:
        worker_deadline = float(os.environ.get("LEAGUE_UPDATE_DEADLINE_EPOCH", ""))
        if math.isfinite(worker_deadline) and worker_deadline > 0:
            budget = min(budget, worker_deadline - time() - 30)
    except ValueError:
        pass
    if budget <= 0:
        raise RuntimeError("Freshness probe budget exhausted; publication reserve preserved")
    deadline = monotonic() + budget
    # Match the worker's existing hard-exit policy even if a socket drips data
    # forever and its per-read timeout never fires. No timer survives this probe.
    timer = Timer(budget, lambda: os._exit(124))
    timer.daemon = True
    timer.start()
    try:
        print(f"[freshness-probe] budget_seconds={budget:g} max_attempts=3", flush=True)
        for attempt in range(3):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise RuntimeError("Freshness probe budget exhausted; publication reserve preserved")
            delay = float(attempt + 1)
            cause = None
            try:
                with opener(request, timeout=min(timeout, remaining)) as response:
                    status = getattr(response, "status", 200)
                    payload = json.loads(response.read().decode("utf-8"))
                    if monotonic() >= deadline:
                        raise RuntimeError("Freshness probe budget exhausted; publication reserve preserved")
            except HTTPError as exc:
                cause = exc
                detail = exc.read().decode("utf-8", errors="replace")[:500]
                exc.close()
                failure = RuntimeError(f"Freshness probe failed ({exc.code}): {detail}")
                try:
                    code = json.loads(detail).get("code")
                except (ValueError, AttributeError):
                    code = None
                if exc.code not in {429, 500, 502, 503, 504} or code in {
                    "credential_required", "upgrade_required", "window_closed", "provider_configuration",
                }:
                    raise failure from exc
                status = exc.code
                retry_after = (exc.headers or {}).get("Retry-After", "")
                try:
                    after = float(retry_after)
                except (TypeError, ValueError):
                    try:
                        after = parsedate_to_datetime(retry_after).timestamp() - time()
                    except (TypeError, ValueError, OverflowError):
                        after = math.nan
                if math.isfinite(after) and after >= 0:
                    delay = max(delay, after)
            except (URLError, TimeoutError, ConnectionError) as exc:
                cause = exc
                status = "network"
                failure = RuntimeError("Freshness probe transport failed")
            else:
                # A lease response may contain the previous healthy digest. Wait
                # for the lease owner; never adopt that old digest as a new probe.
                if status != 202:
                    digest = str(payload.get("observed_manifest_digest") or "").strip()
                    if payload.get("healthy") is not True or not digest:
                        raise RuntimeError("Freshness probe did not return a healthy source manifest")
                    return digest
                failure = RuntimeError("Freshness probe is still in progress")
            if attempt == 2 or delay >= deadline - monotonic():
                raise failure from cause
            print(f"[freshness-probe] retry_after_attempt={attempt + 1} status={status} wait_seconds={delay:g}", flush=True)
            sleep(delay)
    finally:
        timer.cancel()


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
