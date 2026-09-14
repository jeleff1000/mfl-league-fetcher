"""Parity tests for canonical weekly-to-season matchup artifacts."""

from pathlib import Path

import duckdb

from matchup_parity import validate_weekly_season_parity


def _write_fixture(tmp_path: Path) -> tuple[Path, Path]:
    weekly = tmp_path / "weekly.parquet"
    season = tmp_path / "season.parquet"
    con = duckdb.connect()
    con.execute("""CREATE TABLE w AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt',2024,'p1',1,10,10,10,6,4,20.0,1.5),
      ('12t','flx','half','4pt',2024,'p1',2,100,60,50,20,20,10.0,-0.5)
    ) t(teams,roster,ppr,td,year,NFL_player_id,week,eligible_leagues,
        rostered_leagues,started_leagues,wins_started,losses_started,
        lamar_weighted,clutch_weighted)""")
    con.execute(f"COPY w TO '{weekly.as_posix()}'")
    con.execute("""CREATE TABLE s AS SELECT
      '12t' teams,'flx' roster,'half' ppr,'4pt' td,4 cohort_level,
      2024 AS "year",'p1' NFL_player_id,
      70.0 start_rate_pct,80.0 healthy_start_rate_pct,
      0.7 expected_wins,0.6 expected_losses,
      1.3 expected_starts,29.0 total_lamar_started,0.5 avg_clutch_started
    """)
    con.execute(f"COPY s TO '{season.as_posix()}'")
    con.close()
    return weekly, season


def test_parity_reports_each_mismatched_metric(tmp_path):
    weekly, season = _write_fixture(tmp_path)
    errors = validate_weekly_season_parity(weekly, season)

    assert {error.metric for error in errors} == {
        "start_rate_pct",
        "healthy_start_rate_pct",
        "expected_wins",
        "expected_losses",
        "expected_starts",
        "total_lamar_started",
        "avg_clutch_started",
    }


def test_parity_keeps_exact_and_all_format_rollups_separate(tmp_path):
    weekly = tmp_path / "weekly_formats.parquet"
    season = tmp_path / "season_formats.parquet"
    con = duckdb.connect()
    con.execute("""CREATE TABLE w AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','ALL','ALL','ALL',0,2024,'p1',1,10,10,10,6,4,29.0,0.5),
      ('12t','flx','half','4pt','redraft','managed','non_keeper',3,2024,'p1',1,20,10,10,4,6,10.0,-0.5)
    ) t(teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,format_level,
        year,NFL_player_id,week,eligible_leagues,rostered_leagues,started_leagues,
        wins_started,losses_started,lamar_weighted,clutch_weighted)""")
    con.execute(f"COPY w TO '{weekly.as_posix()}'")
    con.execute("""CREATE TABLE s AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','ALL','ALL','ALL',0,4,2024,'p1',100.0,100.0,0.6,0.4,1.0,29.0,0.5),
      ('12t','flx','half','4pt','redraft','managed','non_keeper',3,4,2024,'p1',50.0,50.0,0.2,0.3,0.5,10.0,-0.5)
    ) t(teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,format_level,
        cohort_level,year,NFL_player_id,start_rate_pct,healthy_start_rate_pct,
        expected_wins,expected_losses,
        expected_starts,total_lamar_started,avg_clutch_started)""")
    con.execute(f"COPY s TO '{season.as_posix()}'")
    con.close()

    assert validate_weekly_season_parity(weekly, season) == []
