"""Bounded storage pilots must refuse production and terminate on deadline."""
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


def test_pilot_refuses_primary_before_opening_a_database():
    result = subprocess.run(
        [sys.executable, "scripts/fly_table_storage_pilot.py", "--action", "remove",
         "--target-table", "player_fantasy_season", "--db-name", "nyu_ffl",
         "--machine-id", "1781e011b69068", "--volume-id", "vol_rkg7mmd17llez224",
         "--deadline", str(time.time() + 30)],
        cwd=ROOT, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode == 2
    assert "primary target forbidden" in result.stdout


@pytest.mark.parametrize("delay", [-1, 41])
def test_pilot_refuses_expired_or_overlong_deadline(delay):
    from scripts.fly_table_storage_pilot import arm_deadline
    with pytest.raises(ValueError):
        arm_deadline(time.time() + delay)


def test_deadline_kills_a_stuck_pilot_instead_of_waiting_for_completion():
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-u", "-c",
         "import time; from scripts.fly_table_storage_pilot import arm_deadline; "
         "arm_deadline(time.time()+0.2); time.sleep(20)"],
        cwd=ROOT, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode == 124
    assert "deadline_armed" in result.stdout
    assert time.monotonic() - started < 3


def test_deadline_still_exits_when_logging_blocks():
    result = subprocess.run(
        [sys.executable, "-u", "-c",
         "import time; from scripts import fly_table_storage_pilot as p; "
         "p.emit=lambda *a, **k: time.sleep(20); "
         "p.arm_deadline(time.time()+0.2); time.sleep(20)"],
        cwd=ROOT, text=True, capture_output=True, timeout=3,
    )
    assert result.returncode == 124


def test_pilot_refuses_tables_outside_the_corrupt_aggregate_allowlist():
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="target table"):
        validate_target("remove", "matchup", "nyu_ffl", "isolated", "vol_test")


def test_pilot_refuses_a_nonisolated_path(tmp_path):
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="database path"):
        validate_target("remove", "player_fantasy_season", "nyu_ffl", "isolated", "vol_test", tmp_path / "anything.duckdb")
