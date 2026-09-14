from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from apply_structured_player_signal_delta_in_place import apply


def _write_delta(path: Path) -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE delta AS
        SELECT 0::BIGINT AS player_rowid,
               'league'::VARCHAR AS db_name,
               2024::INTEGER AS year,
               1::INTEGER AS week,
               'p1'::VARCHAR AS NFL_player_id,
               'manager'::VARCHAR AS manager,
               '100'::VARCHAR AS source_artifact_ids,
               1::INTEGER AS source_win,
               0::INTEGER AS source_loss,
               1::INTEGER AS source_tie,
               101.5::DOUBLE AS source_team_points,
               1::INTEGER AS source_is_playoffs
    """)
    con.execute("COPY delta TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def _base(path: Path, *, win: int | None = None) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, win INTEGER, loss INTEGER, tie INTEGER,
          team_points DOUBLE, is_playoffs INTEGER,
          untouched VARCHAR
        )
    """)
    con.execute(
        "INSERT INTO public.player_fantasy VALUES ('league', 2024, 1, 'p1', 'manager', ?, NULL, NULL, NULL, NULL, 'keep')",
        [win],
    )
    con.close()


def test_apply_fills_only_null_target_cells_and_preserves_schema(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base)
    _write_delta(delta)

    result = apply(base=base, delta=delta, report=report)

    assert result["delta_rows"] == 1
    assert result["improvements_by_field"] == {
        "win": 1, "loss": 1, "tie": 1, "team_points": 1, "is_playoffs": 1,
    }
    assert result["readback_remaining"] == {
        "win": 0, "loss": 0, "tie": 0, "team_points": 0, "is_playoffs": 0,
    }
    check = duckdb.connect(str(base), read_only=True)
    assert check.execute(
        "SELECT win, loss, tie, team_points, is_playoffs, untouched FROM public.player_fantasy"
    ).fetchone() == (1, 0, 1, 101.5, 1, "keep")
    assert [row[0] for row in check.execute("DESCRIBE public.player_fantasy").fetchall()] == [
        "db_name", "year", "week", "NFL_player_id", "manager", "win", "loss", "tie", "team_points", "is_playoffs", "untouched"
    ]
    check.close()


def test_apply_rejects_source_conflict_with_existing_value(tmp_path: Path) -> None:
    base = tmp_path / "base.duckdb"
    delta = tmp_path / "delta.parquet"
    report = tmp_path / "report.json"
    _base(base, win=0)
    _write_delta(delta)

    with pytest.raises(ValueError, match="conflicts"):
        apply(base=base, delta=delta, report=report)
