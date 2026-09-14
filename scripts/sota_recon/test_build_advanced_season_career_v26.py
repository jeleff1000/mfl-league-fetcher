"""Regression tests for the published NGS season-value guard."""

from __future__ import annotations

import duckdb

from .build_advanced_season_career_v26 import _validate_published_ngs_values


def _write_parquet(path, sql: str) -> None:
    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def test_published_ngs_guard_rejects_misaligned_season_values(tmp_path):
    published = tmp_path / "ngs_season.parquet"
    target = tmp_path / "player_nfl_season.parquet"

    _write_parquet(
        published,
        """
        SELECT 'p1' AS NFL_player_id, 2025 AS year,
               6.030159 AS ngs_avg_cushion,
               2.725449 AS ngs_avg_separation
        """,
    )
    _write_parquet(
        target,
        """
        SELECT 'p1' AS NFL_player_id, 2025 AS year,
               48.3874 AS ngs_avg_cushion,
               2.6886 AS ngs_avg_separation
        """,
    )

    con = duckdb.connect()
    result = _validate_published_ngs_values(
        con,
        target.as_posix(),
        published.as_posix(),
        ["ngs_avg_cushion", "ngs_avg_separation"],
    )
    con.close()

    assert result == {
        "expected_rows": 1,
        "matched_rows": 1,
        "missing_rows": 0,
        "mismatch_rows": 1,
        "passed": False,
    }


def test_published_ngs_guard_accepts_exact_published_values(tmp_path):
    published = tmp_path / "ngs_season.parquet"
    target = tmp_path / "player_nfl_season.parquet"

    sql = """
        SELECT 'p1' AS NFL_player_id, 2025 AS year,
               6.030159 AS ngs_avg_cushion,
               2.725449 AS ngs_avg_separation
    """
    _write_parquet(published, sql)
    _write_parquet(target, sql)

    con = duckdb.connect()
    result = _validate_published_ngs_values(
        con,
        target.as_posix(),
        published.as_posix(),
        ["ngs_avg_cushion", "ngs_avg_separation"],
    )
    con.close()

    assert result["passed"] is True
    assert result["mismatch_rows"] == 0


def test_career_ngs_rollup_uses_explicit_denominators(tmp_path, monkeypatch):
    from scripts.sota_recon import build_advanced_season_career_v26 as builder

    published = tmp_path / "ngs_season.parquet"
    season = tmp_path / "player_nfl_season.parquet"
    _write_parquet(
        published,
        """
        SELECT 'p1' AS NFL_player_id, 2024 AS year,
               2.0 AS ngs_avg_cushion, 10.0 AS ngs_expected_rush_yards
        UNION ALL
        SELECT 'p1', 2025, 4.0, 20.0
        """,
    )
    _write_parquet(
        season,
        """
        SELECT 'p1' AS NFL_player_id, 2024 AS year, 'TST' AS nfl_team,
               100.0 AS receiving_air_yards, 10 AS targets
        UNION ALL
        SELECT 'p1', 2025, 'TST', 100.0, 30
        """,
    )
    monkeypatch.setattr(builder, "NGS_SEASON", published)
    con = duckdb.connect()
    builder._create_career_ngs_aggregate(
        con, season, ["ngs_avg_cushion", "ngs_expected_rush_yards"]
    )
    result = con.execute(
        "SELECT ngs_avg_cushion, ngs_expected_rush_yards FROM ngs_career_aggregate"
    ).fetchone()
    con.close()

    assert result == (3.5, 30.0)
