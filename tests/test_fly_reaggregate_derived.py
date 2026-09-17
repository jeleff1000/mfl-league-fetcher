from __future__ import annotations

from dataclasses import dataclass

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
