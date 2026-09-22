import os
from pathlib import Path
import subprocess
import sys

import duckdb
import pytest


_CRASH_WRITER = r"""
import duckdb
import os
import sys

path, phase = sys.argv[1:]
conn = duckdb.connect(path)
if phase == "before_write":
    os._exit(0)
conn.execute("BEGIN TRANSACTION")
conn.execute("UPDATE generation SET value = 2")
if phase in {"during_transaction", "before_commit"}:
    os._exit(0)
conn.execute("COMMIT")
if phase in {"after_commit", "before_checkpoint"}:
    os._exit(0)
conn.execute("CHECKPOINT")
if phase == "after_checkpoint":
    os._exit(0)
raise RuntimeError(f"unknown phase: {phase}")
"""


@pytest.mark.parametrize(
    ("phase", "expected_generation"),
    [
        ("before_write", 1),
        ("during_transaction", 1),
        ("before_commit", 1),
        ("after_commit", 2),
        ("before_checkpoint", 2),
        ("after_checkpoint", 2),
    ],
)
def test_crash_boundaries_reopen_to_exactly_old_or_new_generation(
    tmp_path, phase, expected_generation
):
    for iteration in range(3):
        db_path = tmp_path / f"{phase}_{iteration}.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE generation(value INTEGER)")
        conn.execute("INSERT INTO generation VALUES (1)")
        conn.execute("CHECKPOINT")
        conn.close()

        completed = subprocess.run(
            [sys.executable, "-c", _CRASH_WRITER, str(db_path), phase],
            check=False,
            timeout=10,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        assert completed.returncode == 0

        reopened = duckdb.connect(str(db_path))
        try:
            assert reopened.execute("SELECT value FROM generation").fetchone()[0] == expected_generation
            reopened.execute("CHECKPOINT")
        finally:
            reopened.close()

        second_reopen = duckdb.connect(str(db_path), read_only=True)
        try:
            assert second_reopen.execute("SELECT value FROM generation").fetchone()[0] == expected_generation
        finally:
            second_reopen.close()
