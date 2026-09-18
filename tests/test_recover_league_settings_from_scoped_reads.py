from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "recover_league_settings_from_scoped_reads.py"
SPEC = importlib.util.spec_from_file_location("recover_league_settings", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_scoped_reads_use_bounded_batches() -> None:
    names = [f"league_{index:03d}" for index in range(205)]
    calls: list[str] = []

    def query(sql: str) -> list[dict]:
        calls.append(sql)
        return [{"db_name": name, "year": 2026} for name in names if f"'{name}'" in sql]

    rows, populated, unreadable = MODULE._read_scoped_batches(names, query, batch_size=100)

    assert len(calls) == 3
    assert len(rows) == 205
    assert populated == set(names)
    assert unreadable == set()


def test_scoped_reads_bisect_a_failed_batch_and_name_the_bad_identity() -> None:
    names = ["alpha", "bad_league", "charlie", "delta"]

    def query(sql: str) -> list[dict]:
        if "'bad_league'" in sql:
            raise OSError("corrupt block")
        return [{"db_name": name, "year": 2026} for name in names if f"'{name}'" in sql]

    with pytest.raises(RuntimeError, match="bad_league"):
        MODULE._read_scoped_batches(names, query, batch_size=4)


def test_scoped_reads_can_report_one_unreadable_identity() -> None:
    names = ["alpha", "bad_league", "charlie"]

    def query(sql: str) -> list[dict]:
        if "'bad_league'" in sql:
            raise OSError("corrupt block")
        return [{"db_name": name, "year": 2026} for name in names if f"'{name}'" in sql]

    rows, populated, unreadable = MODULE._read_scoped_batches(
        names,
        query,
        batch_size=3,
        tolerate_unreadable=True,
    )

    assert {row["db_name"] for row in rows} == {"alpha", "charlie"}
    assert populated == {"alpha", "charlie"}
    assert unreadable == {"bad_league"}
