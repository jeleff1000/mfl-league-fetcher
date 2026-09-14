"""Unattended D:-only handoff from a verified Actions archive run.

This watcher never touches a source cache.  It waits for one Actions run to
finish successfully, then delegates the actual landing and lane audit to the
strict receiver in ``sync_mfl_artifacts_to_pc.py``.  A failed or cancelled run
stops before any artifact is downloaded.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Callable, Iterable

from sync_mfl_artifacts_to_pc import require_d_destination


def wait_for_successful_run(
    *,
    repo: str,
    run_id: int,
    poll_seconds: int,
    run_command: Callable[[list[str]], str] | None = None,
    sleep: Callable[[int], None] | None = None,
) -> dict[str, object]:
    """Return only after one Actions run has conclusively succeeded."""

    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    if run_command is None:
        run_command = lambda command: subprocess.check_output(command, text=True)
    if sleep is None:
        sleep = time.sleep

    command = ["gh", "run", "view", str(run_id), "--repo", repo, "--json", "status,conclusion"]
    while True:
        payload = json.loads(run_command(command))
        if not isinstance(payload, dict):
            raise RuntimeError(f"GitHub returned a malformed workflow state for run {run_id}")
        status = str(payload.get("status", ""))
        conclusion = payload.get("conclusion")
        print(json.dumps({"event": "actions_state", "run_id": run_id, "status": status, "conclusion": conclusion}, sort_keys=True), flush=True)
        if status == "completed":
            if conclusion == "success":
                return payload
            raise RuntimeError(f"Actions run {run_id} ended without success: {conclusion!r}")
        sleep(poll_seconds)


def payload_audit_command(
    *,
    python: str,
    receiver_directory: Path,
    destination: Path,
    run_id: int,
) -> list[str]:
    """Build the D:-only post-landing payload audit invocation."""

    audit_root = Path(destination).parent / "audit"
    return [
        python,
        str(Path(receiver_directory) / "audit_mfl_original_payloads.py"),
        "--destination",
        str(destination),
        "--run-id",
        str(run_id),
        "--scratch-directory",
        str(audit_root / f"mfl_original_payload_scratch_{run_id}"),
        "--output-directory",
        str(audit_root / f"mfl_original_payload_audit_{run_id}"),
    ]


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Wait for a verified MFL archive run, then land it on D:")
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--repo", default="jeleff1000/league-history-workers")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--expected-lane-count", type=int, default=15)
    parser.add_argument("--artifact-prefix", default="mfl-original-source-archives-lane-")
    args = parser.parse_args(list(argv) if argv is not None else None)

    require_d_destination(args.destination)
    if args.expected_lane_count <= 0:
        parser.error("--expected-lane-count must be positive")
    wait_for_successful_run(
        repo=args.repo,
        run_id=args.run_id,
        poll_seconds=args.poll_seconds,
    )
    receiver = Path(__file__).with_name("sync_mfl_artifacts_to_pc.py")
    command = [
        sys.executable,
        str(receiver),
        "--repo",
        args.repo,
        "--run-id",
        str(args.run_id),
        "--destination",
        str(args.destination),
        "--artifact-prefix",
        args.artifact_prefix,
        "--required-file",
        "lane-manifest.json",
        "--multi-artifact-layout",
        "--audit-lane-count",
        str(args.expected_lane_count),
    ]
    print(json.dumps({"event": "receiver_start", "command": command[1:]}, sort_keys=True), flush=True)
    subprocess.run(command, check=True)
    audit_command = payload_audit_command(
        python=sys.executable,
        receiver_directory=Path(__file__).parent,
        destination=args.destination,
        run_id=args.run_id,
    )
    print(json.dumps({"event": "payload_audit_start", "command": audit_command[1:]}, sort_keys=True), flush=True)
    subprocess.run(audit_command, check=True)
    print(json.dumps({"event": "receiver_complete", "run_id": args.run_id}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
