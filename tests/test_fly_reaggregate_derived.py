from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandas as pd

from scripts import fly_reaggregate_derived as repair


@dataclass
class _Result:
    rows: list[tuple]

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


class _Connection:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))
        if "SELECT DISTINCT db_name" in sql:
            return _Result([("alpha",), ("beta",)])
        if "SELECT DISTINCT TRY_CAST(year AS INTEGER)" in sql:
            return _Result([(2025,), (2026,)])
        if "SELECT COUNT(*)" in sql:
            return _Result([(2,)])
        return _Result([])

    def close(self):
        self.sql.append("CLOSE")


def test_reaggregate_one_league_calls_only_the_five_canonical_targets(monkeypatch):
    calls: list[tuple] = []
    conn = _Connection()

    monkeypatch.setattr(
        repair,
        "aggregate_fantasy_season",
        lambda actual_conn, db_name: calls.append(("player_fantasy_season", db_name)) or 2,
    )
    monkeypatch.setattr(
        repair,
        "aggregate_fantasy_season_all",
        lambda actual_conn, db_name: calls.append(("player_fantasy_season_all", db_name)) or 2,
    )
    monkeypatch.setattr(
        repair,
        "aggregate_matchup_h2h",
        lambda actual_conn, db_name, season_years: calls.append(
            ("matchup_h2h_career", db_name, season_years)
        )
        or (0, 2),
    )
    monkeypatch.setattr(
        repair,
        "aggregate_standings",
        lambda actual_conn, db_name, years: calls.append(
            ("standings_by_year", db_name, years)
        ),
    )
    monkeypatch.setattr(
        repair,
        "compute_manager_rankings",
        lambda actual_conn, db_name: pd.DataFrame(
            [{"franchise_id": "f1", "manager": "A"}]
        ),
    )
    monkeypatch.setattr(
        repair,
        "replace_scoped_aggregate_table_from_dataframe",
        lambda actual_conn, db_name, table, frame: calls.append(
            (table, db_name, len(frame))
        ),
    )

    result = repair.reaggregate_one_league(conn, "alpha")

    assert calls == [
        ("player_fantasy_season", "alpha"),
        ("player_fantasy_season_all", "alpha"),
        ("matchup_h2h_career", "alpha", set()),
        ("standings_by_year", "alpha", [2025, 2026]),
        ("homepage_manager_rankings", "alpha", 1),
    ]
    assert set(result) == set(repair.TARGET_TABLES)
    assert not any("matchup_h2h_season" in sql for sql in conn.sql)


def test_validate_targets_queries_only_the_five_rebuilt_tables():
    conn = _Connection()

    result = repair.validate_targets(conn)

    assert set(result) == set(repair.TARGET_TABLES)
    checked_tables = {
        table
        for table in repair.TARGET_TABLES
        if any(f'public."{table}"' in sql for sql in conn.sql)
    }
    assert checked_tables == set(repair.TARGET_TABLES)
    assert all(
        not any(f'public."{other}"' in sql for other in ("matchup", "draft", "transactions"))
        for sql in conn.sql
    )


def test_discover_leagues_is_deterministic_and_deduplicated():
    conn = _Connection()

    assert repair.discover_leagues(conn) == ["alpha", "beta"]


def test_reaggregate_all_fails_fast_instead_of_repeating_storage_error(monkeypatch):
    conn = _Connection()
    calls: list[str] = []

    def fail_first(_conn, db_name):
        calls.append(db_name)
        raise RuntimeError("checksum mismatch")

    monkeypatch.setattr(repair, "reaggregate_one_league", fail_first)

    try:
        repair.reaggregate_all(conn, db_names=["alpha", "beta"])
    except RuntimeError as exc:
        assert "alpha" in str(exc)
        assert "checksum mismatch" in str(exc)
    else:
        raise AssertionError("storage failure should abort recovery")

    assert calls == ["alpha"]
    assert conn.sql[-1] == "ROLLBACK"


def test_reaggregate_all_reports_incremental_progress(monkeypatch):
    conn = _Connection()
    events: list[dict] = []
    monkeypatch.setattr(
        repair,
        "reaggregate_one_league",
        lambda _conn, _db_name: {table: 1 for table in repair.TARGET_TABLES},
    )
    monkeypatch.setattr(
        repair,
        "validate_targets",
        lambda _conn: {table: {"rows": 2, "distinct_keys": 2} for table in repair.TARGET_TABLES},
    )

    result = repair.reaggregate_all(
        conn,
        db_names=["alpha", "beta"],
        progress_callback=lambda event: events.append(dict(event)),
        checkpoint=False,
    )

    assert result["leagues"] == 2
    assert events == [
        {"stage": "reaggregating", "completed": 0, "total": 2, "current_db_name": "alpha"},
        {"stage": "reaggregating", "completed": 1, "total": 2, "current_db_name": "beta"},
        {"stage": "reaggregating", "completed": 2, "total": 2, "current_db_name": None},
        {"stage": "validating", "completed": 2, "total": 2, "current_db_name": None},
        {"stage": "complete", "completed": 2, "total": 2, "current_db_name": None},
    ]
    assert "CHECKPOINT" not in conn.sql


def test_parallel_reaggregation_uses_disjoint_scoped_transactions(monkeypatch, tmp_path):
    calls: list[str] = []
    events: list[dict] = []
    connections: list[_Connection] = []

    def connection_factory(_path):
        conn = _Connection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(
        repair,
        "reaggregate_one_league",
        lambda _conn, db_name, **_kwargs: calls.append(db_name)
        or {table: 1 for table in repair.TARGET_TABLES},
    )

    result = repair.reaggregate_parallel(
        tmp_path / "leagues.duckdb",
        db_names=[f"league_{index}" for index in range(12)],
        max_workers=4,
        progress_callback=lambda event: events.append(dict(event)),
        connection_factory=connection_factory,
    )

    assert result == {"leagues": 12}
    assert set(calls) == {f"league_{index}" for index in range(12)}
    assert len(connections) == 4
    assert all("BEGIN TRANSACTION" in conn.sql for conn in connections)
    assert all("COMMIT" in conn.sql for conn in connections)
    assert all(conn.sql[-1] == "CLOSE" for conn in connections)
    assert events[-1] == {
        "stage": "reaggregating",
        "completed": 12,
        "total": 12,
        "current_db_name": None,
    }


def test_parallel_workers_attach_shared_catalogs(monkeypatch, tmp_path):
    primary = tmp_path / "leagues.duckdb"
    ops = tmp_path / "ops.duckdb"
    ops_nfl = tmp_path / "ops_nfl.duckdb"
    duckdb.connect(str(primary)).close()
    for path, value in ((ops, 11), (ops_nfl, 22)):
        conn = duckdb.connect(str(path))
        conn.execute("CREATE TABLE marker(value INTEGER)")
        conn.execute("INSERT INTO marker VALUES (?)", [value])
        conn.close()

    observed: list[tuple[int, int]] = []

    def assert_catalogs_attached(conn, _db_name, **_kwargs):
        observed.append(
            (
                conn.execute("SELECT value FROM ___ops.main.marker").fetchone()[0],
                conn.execute("SELECT value FROM ___ops_nfl.main.marker").fetchone()[0],
            )
        )
        return {table: 1 for table in repair.TARGET_TABLES}

    monkeypatch.setattr(repair, "reaggregate_one_league", assert_catalogs_attached)

    result = repair.reaggregate_parallel(
        primary,
        db_names=["alpha"],
        ops_path=ops,
        ops_nfl_path=ops_nfl,
        max_workers=1,
    )

    assert result == {"leagues": 1}
    assert observed == [(11, 22)]


def test_parallel_reaggregation_reports_exact_stage_failure(monkeypatch, tmp_path):
    duckdb.connect(str(tmp_path / "leagues.duckdb")).close()

    def fail(_conn, _db_name, **_kwargs):
        raise RuntimeError("player_fantasy_season failed: CatalogException: missing ___ops")

    monkeypatch.setattr(repair, "reaggregate_one_league", fail)

    try:
        repair.reaggregate_parallel(
            tmp_path / "leagues.duckdb",
            db_names=["alpha"],
            max_workers=1,
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "alpha" in message
        assert "player_fantasy_season" in message
        assert "missing ___ops" in message
    else:
        raise AssertionError("the exact inner failure must be preserved")


def test_attach_if_present_is_idempotent(tmp_path):
    primary = tmp_path / "primary.duckdb"
    attached = tmp_path / "ops.duckdb"
    duckdb.connect(str(attached)).close()
    conn = duckdb.connect(str(primary))
    try:
        repair._attach_if_present(conn, attached, "___ops")
        repair._attach_if_present(conn, attached, "___ops")
        assert conn.execute(
            "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name='___ops'"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_quarantine_targets_swaps_only_the_five_corrupt_objects():
    conn = _Connection()

    result = repair.quarantine_corrupt_targets(conn)

    assert result == {
        table: f"__corrupt_recovery_{table}" for table in repair.TARGET_TABLES
    }
    sql = conn.sql
    assert sql[0] == "BEGIN TRANSACTION"
    assert sql[-1] == "COMMIT"
    assert not any(statement.startswith("DROP TABLE") for statement in sql)
    for table in repair.TARGET_TABLES:
        replacement = f"__repair_recovery_{table}"
        quarantine = f"__corrupt_recovery_{table}"
        assert any(
            f'CREATE TABLE "___leagues".public.{replacement}' in statement
            for statement in sql
        )
        assert (
            f'ALTER TABLE public."{table}" RENAME TO "{quarantine}"' in sql
        )
        assert (
            f'ALTER TABLE public."{replacement}" RENAME TO "{table}"' in sql
        )


def test_recovery_has_no_way_to_drop_quarantined_corrupt_objects():
    assert not hasattr(repair, "drop_quarantined_targets")


def test_quarantine_or_resume_reuses_complete_existing_quarantine(monkeypatch):
    conn = _Connection()
    expected = {
        table: f"__corrupt_recovery_{table}" for table in repair.TARGET_TABLES
    }
    monkeypatch.setattr(repair, "_public_table_names", lambda actual_conn: set(expected.values()))
    monkeypatch.setattr(
        repair,
        "quarantine_corrupt_targets",
        lambda actual_conn: (_ for _ in ()).throw(AssertionError("must resume")),
    )

    assert repair.quarantine_or_resume_corrupt_targets(conn) == expected


def test_quarantine_or_resume_rejects_partial_existing_quarantine(monkeypatch):
    conn = _Connection()
    monkeypatch.setattr(
        repair,
        "_public_table_names",
        lambda actual_conn: {"__corrupt_recovery_homepage_manager_rankings"},
    )

    try:
        repair.quarantine_or_resume_corrupt_targets(conn)
    except RuntimeError as exc:
        assert "partial corrupt-table quarantine" in str(exc)
    else:
        raise AssertionError("partial quarantine must be rejected")
