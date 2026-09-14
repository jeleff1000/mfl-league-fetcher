from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from apply_structured_player_signal_replacement_in_place import apply


def _base(path: Path, *, win: int = 1) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          manager VARCHAR, win INTEGER, loss INTEGER, tie INTEGER,
          team_points DOUBLE, is_playoffs INTEGER, untouched VARCHAR
        )
    """)
    con.execute(
        "INSERT INTO public.player_fantasy VALUES ('league', 2024, 1, 'p1', 'manager', ?, 0, 0, 90.0, 0, 'keep')",
        [win],
    )
    con.close()


def _delta(path: Path, *, expected_win: int = 1) -> None:
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE delta AS
        SELECT 0::BIGINT AS player_rowid,
               'league'::VARCHAR AS db_name,
               2024::INTEGER AS year,
               1::INTEGER AS week,
               'p1'::VARCHAR AS NFL_player_id,
               'manager'::VARCHAR AS manager,
               0::INTEGER AS source_win,
               NULL::INTEGER AS source_loss,
               NULL::INTEGER AS source_tie,
               NULL::DOUBLE AS source_team_points,
               NULL::INTEGER AS source_is_playoffs,
               ?::INTEGER AS expected_win,
               NULL::INTEGER AS expected_loss,
               NULL::INTEGER AS expected_tie,
               NULL::DOUBLE AS expected_team_points,
               NULL::INTEGER AS expected_is_playoffs
    """, [expected_win])
    con.execute("COPY delta TO ? (FORMAT PARQUET)", [str(path)])
    con.close()


def test_replaces_only_exact_expected_source_conflict_and_reads_back(tmp_path: Path) -> None:
    base, delta, report = tmp_path / "base.duckdb", tmp_path / "delta.parquet", tmp_path / "report.json"
    _base(base)
    _delta(delta)

    result = apply(base=base, delta=delta, report=report)

    assert result["replacement_cells"] == {"win": 1, "loss": 0, "tie": 0, "team_points": 0, "is_playoffs": 0}
    assert result["readback_remaining"] == {"win": 0, "loss": 0, "tie": 0, "team_points": 0, "is_playoffs": 0}
    check = duckdb.connect(str(base), read_only=True)
    assert check.execute("SELECT win, untouched FROM public.player_fantasy").fetchone() == (0, "keep")
    check.close()


def test_rejects_stale_expected_canonical_value(tmp_path: Path) -> None:
    base, delta, report = tmp_path / "base.duckdb", tmp_path / "delta.parquet", tmp_path / "report.json"
    _base(base, win=0)
    _delta(delta, expected_win=1)

    with pytest.raises(ValueError, match="stale"):
        apply(base=base, delta=delta, report=report)
