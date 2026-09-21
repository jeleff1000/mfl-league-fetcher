"""Exercise the real workflow timeout against TERM-resistant worker processes."""

import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash")
if not BASH and os.name == "nt":
    candidate = Path("C:/Program Files/Git/bin/bash.exe")
    if candidate.is_file():
        BASH = str(candidate)


def _workflow_timeout(platform, phase):
    path = ROOT / ".github/workflows" / f"{platform}_incremental_refresh_worker.yml"
    target = f"scripts/refresh_{platform}_active_season.py" if phase == "refresh" else "scripts/record_league_update_status.py"
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()
             if line.strip().startswith("timeout ") and target in line]
    assert len(lines) == 1
    prefix = shlex.split(lines[0].split(" python ", 1)[0])
    # Accelerate only the duration, retaining the executing workflow's flags.
    prefix[-1] = "0.35s"
    return shlex.join(prefix)


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_two_minute_deadline_starts_after_setup(platform):
    path = ROOT / ".github/workflows" / f"{platform}_incremental_refresh_worker.yml"
    workflow = path.read_text(encoding="utf-8")
    deadline = workflow.index("- name: Set hard refresh deadline")
    install = workflow.index("- name: Install dependencies")
    display_name = {"yahoo": "Yahoo", "espn": "ESPN", "sleeper": "Sleeper"}[platform]
    refresh = workflow.index(f"- name: Refresh {display_name} active season")

    assert install < deadline < refresh
    assert "LEAGUE_UPDATE_DEADLINE_EPOCH=$(( $(date +%s) + 120 ))" in workflow
    assert "timeout-minutes: 3" in workflow


@pytest.mark.skipif(not BASH, reason="Requires Bash and GNU timeout as on Actions")
@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("phase", ["refresh", "status"])
def test_workflow_deadline_stops_term_resistant_worker_and_child(platform, phase):
    # Both worker and child finish naturally after ~2s if the deadline fails;
    # no hanging process or external service is needed for this proof.
    payload = """
trap '' TERM
(trap '' TERM; sleep 1.2; printf 'CHILD_OVERRUN\\n') &
printf 'STARTED\\n'
for ((i=0; i<20; i++)); do sleep 0.1; done
wait
"""
    command = f"{_workflow_timeout(platform, phase)} bash -c {shlex.quote(payload)}"
    started = time.monotonic()
    result = subprocess.run([BASH, "-c", command], capture_output=True, text=True, timeout=5)
    assert "STARTED" in result.stdout, result.stderr
    assert "CHILD_OVERRUN" not in result.stdout, "A descendant ran beyond the update deadline"
    assert result.returncode != 0, result.stderr
    if os.name != "nt":  # Git Bash exposes MSYS signal status differently on Windows.
        assert result.returncode in (137, -9), result.stderr
    assert time.monotonic() - started < 3


@pytest.mark.skipif(not BASH, reason="Requires Bash and GNU timeout as on Actions")
@pytest.mark.parametrize("exit_code", [0, 7])
def test_workflow_deadline_preserves_timely_worker_exit(exit_code):
    command = f"{_workflow_timeout('sleeper', 'refresh')} bash -c 'exit {exit_code}'"
    result = subprocess.run([BASH, "-c", command], capture_output=True, timeout=5)
    assert result.returncode == exit_code
