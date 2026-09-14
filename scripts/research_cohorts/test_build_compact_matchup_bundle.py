from __future__ import annotations

import inspect
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from scripts.research_cohorts.build_compact_matchup_bundle import (
    audit_capacity_allocation,
    build_task,
    compact_outer_mismatch_diagnostics,
    configure_memory_bounded_rollup,
    materialize_normalized_player_team_game_week,
)
from scripts.research_cohorts.merge_compact_matchup_parts import (
    assemble_compact_career_parts,
    build_compact_career_part,
    merge_parts,
)


def test_build_task_requires_150_leagues_before_any_season_cohort_splits() -> None:
    """Season core and playoff/champ cells must pool thin format dimensions."""
    source = inspect.getsource(build_task)

    assert source.count("split_threshold=150") == 3
    assert "split_threshold=50" not in source


def test_capacity_only_audit_validates_all_eligible_league_lanes(tmp_path) -> None:
    """The preflight reads the immutable cache and emits no compact artifact."""
    snapshot = tmp_path / "snapshot.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute(
        """
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER, cohort_teams VARCHAR,
          cohort_roster VARCHAR, cohort_scoring VARCHAR, cohort_pass_td VARCHAR,
          cohort_playoff_teams VARCHAR, cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER
        )
        """
    )
    con.execute(
        """
        INSERT INTO public.player_fantasy VALUES
          ('league_a', 2025, 1, 'rb_a', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1),
          ('league_b', 2025, 1, 'rb_b', 'RB', 1, '12tm', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 0)
        """
    )
    con.close()

    result = audit_capacity_allocation(snapshot, year=2025, position="RB")

    assert result["year"] == 2025
    assert result["position"] == "RB"
    assert result["eligible_lanes"] == 2
    assert result["invalid_rostered_lanes"] == 0
    assert result["invalid_started_lanes"] == 0


def test_capacity_only_audit_rejects_legacy_two_team_cache_labels(tmp_path) -> None:
    """A cache that collapsed 8/10 and 12/14 cannot enter the compact build."""
    snapshot = tmp_path / "legacy_snapshot.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, cohort_position_eligible INTEGER, cohort_teams VARCHAR,
          cohort_roster VARCHAR, cohort_scoring VARCHAR, cohort_pass_td VARCHAR,
          cohort_playoff_teams VARCHAR, cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
          is_rostered INTEGER, is_started INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('league_a', 2025, 1, 'rb_a', 'RB', 1, '12t', 'flx', 'half', '4pt', '6po', 'redraft', 'managed', 1, 1)
    """)
    con.close()

    with pytest.raises(RuntimeError, match="invalid rostered capacity allocation"):
        audit_capacity_allocation(snapshot, year=2025, position="RB")


def test_normalized_team_week_lookup_supplements_missing_nor_player_calendar() -> None:
    """A NOR player must inherit NO's scheduled game weeks without touching the cache."""
    con = duckdb.connect()
    con.execute("CREATE TABLE cached_weeks (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    con.execute("INSERT INTO cached_weeks VALUES ('saint', 2025, 1)")
    con.execute("CREATE TABLE targets (NFL_player_id VARCHAR)")
    con.execute("INSERT INTO targets VALUES ('saint')")
    con.execute(
        """
        CREATE TABLE stats (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER,
          nfl_team VARCHAR, season_type VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO stats VALUES
          ('saint', 2025, 1, 'NOR', 'REG'),
          ('teammate_1', 2025, 1, 'NO', 'REG'),
          ('teammate_2', 2025, 2, 'NO', 'REG')
        """
    )

    materialize_normalized_player_team_game_week(
        con,
        cached_weeks_table="cached_weeks",
        nfl_stats_table="stats",
        target_players_table="targets",
        output_table="resolved_weeks",
        year=2025,
    )

    assert con.execute("SELECT * FROM resolved_weeks ORDER BY week").fetchall() == [
        ("saint", 2025, 1),
        ("saint", 2025, 2),
    ]


def test_compact_team_alias_mapping_import_has_no_pandas_dependency() -> None:
    """The Actions compact runner can import canonical aliases with DuckDB only."""
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "from fantasy_football_data_scripts.nfl_data.nfl_franchises import NFLVERSE_TO_DISPLAY; assert NFLVERSE_TO_DISPLAY['NOR'] == 'NO'",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_memory_bounded_rollup_configuration_reserves_disk_spill(tmp_path) -> None:
    """A grouping cube must spill instead of consuming a runner's full RAM."""
    connection = duckdb.connect(str(tmp_path / "part.duckdb"))
    try:
        connection.execute("SET enable_progress_bar=true")
        configure_memory_bounded_rollup(connection, tmp_path / "compact-spill")

        assert connection.execute("SELECT current_setting('threads')").fetchone() == (1,)
        assert connection.execute("SELECT current_setting('memory_limit')").fetchone() == ("8.0 GiB",)
        assert connection.execute("SELECT current_setting('enable_progress_bar')").fetchone() == (False,)
        assert connection.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str((tmp_path / "compact-spill").resolve()).replace("\\", "/"),
        )
    finally:
        connection.close()


def test_build_task_emits_compact_weekly_and_season_outer_rows(tmp_path, capsys) -> None:
    snapshot = tmp_path / "snapshot.duckdb"
    ops = tmp_path / "ops.duckdb"
    output = tmp_path / "part.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    con.execute(
        "CREATE TABLE public.player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    con.execute(
        "INSERT INTO public.player_team_game_week VALUES ('p1',2025,1), ('p2',2025,1), ('p3',2025,1)"
    )
    con.execute(
        "INSERT INTO public.player_active_week VALUES ('p1',2025,1), ('p2',2025,1), ('p3',2025,1)"
    )
    con.execute("""
      CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        position VARCHAR, cohort_position_eligible INTEGER,
        cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
        cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
        cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
        is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
        is_playoffs INTEGER, champion INTEGER
      )
    """)
    # The serving contract is >=150 rostered leagues.  Keep this integration
    # fixture above that floor while preserving p1's 3.0 weekly clutch average.
    con.execute("""
      INSERT INTO public.player_fantasy
      SELECT 'l' || i::VARCHAR,2025,1,'p1','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',
             1,1,CASE WHEN i < 75 THEN 1.0 ELSE 0.0 END,
             CASE WHEN i < 75 THEN 2.00000000045 ELSE 4.0 END,
             CASE WHEN i < 75 THEN 1 ELSE 0 END,CASE WHEN i < 75 THEN 1 ELSE 0 END
      FROM range(150) t(i)
      UNION ALL
      SELECT 'l' || i::VARCHAR,2025,1,'p2','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',
             1,0,NULL,NULL,0,0
      FROM range(150) t(i)
      UNION ALL
      SELECT 'orphan',2025,1,'p3','RB',0,'12tm','flx','half','4pt','6po','redraft','managed',1,0,NULL,NULL,0,0
    """)
    con.close()
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("""
      CREATE OR REPLACE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR,
        position VARCHAR,
        nfl_team VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
        special_teams_snaps BIGINT
      )
    """)
    con.execute("""
      INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('p1',2024,1,'REG','WR','SF',20,0,0),
        ('p1',2025,1,'REG','RB','SF',20,0,0),('p2',2025,1,'REG','RB','SF',20,0,0),
        ('p3',2025,1,'REG','RB','SF',20,0,0)
    """)
    con.close()

    build_task(snapshot, ops, output, year=2025, position="RB")

    con = duckdb.connect(str(output), read_only=True)
    assert con.execute("SELECT NFL_player_id,year,week FROM research_matchup_compact_weekly ORDER BY 1").fetchall() == [
        ("p1", 2025, 1), ("p2", 2025, 1)
    ]
    assert con.execute("SELECT NFL_player_id,year FROM research_matchup_compact_season ORDER BY 1").fetchall() == [
        ("p1", 2025), ("p2", 2025)
    ]
    assert con.execute(
        """
        SELECT cell.clutch_season_sum
        FROM research_matchup_compact_season, UNNEST(cohort_cells) AS u(cell)
        WHERE NFL_player_id='p1' AND year=2025 AND position='RB'
          AND cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
          AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        """
    ).fetchone() == (3.0,)
    assert con.execute("SELECT COUNT(*) FROM research_matchup_compact_season, UNNEST(grade_cells)").fetchone()[0] > 0
    con.close()

    merged = tmp_path / "merged.duckdb"
    merge_parts([output], merged)
    merge_log = capsys.readouterr().out
    assert "[compact-merge] phase=append_lane" in merge_log
    assert "[compact-merge] phase=career_bucket" in merge_log
    assert "[compact-merge] phase=career_core_selector_indices" in merge_log
    assert "[compact-merge] phase=career_grade_selector_indices" in merge_log
    assert "[compact-merge] phase=merge_complete" in merge_log
    con = duckdb.connect(str(merged), read_only=True)
    assert con.execute("SELECT NFL_player_id,year FROM research_matchup_compact_season ORDER BY 1").fetchall() == [
        ("p1", 2025), ("p2", 2025)
    ]
    assert con.execute("SELECT NFL_player_id FROM research_matchup_compact_career ORDER BY 1").fetchall() == [
        ("p1",), ("p2",)
    ]
    career_columns = {
        row[0] for row in con.execute("DESCRIBE research_matchup_compact_career").fetchall()
    }
    assert {"roster_cells", "roster_selector_indices"} <= career_columns
    assert con.execute(
        "SELECT COUNT(*) FROM research_matchup_compact_career "
        "WHERE list_count(roster_selector_indices) = 288"
    ).fetchone()[0] == 2
    assert con.execute(
        "SELECT COUNT(*) FROM research_matchup_compact_career WHERE list_count(core_selector_indices) = 288"
    ).fetchone()[0] == 2
    assert con.execute(
        "SELECT COUNT(*) FROM research_matchup_compact_career "
        "WHERE list_count(grade_selector_indices) = 864"
    ).fetchone()[0] == 2
    for table in ("research_matchup_compact_weekly", "research_matchup_compact_season"):
        assert con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE list_count(core_selector_indices) = 288"
        ).fetchone()[0] == con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    assert con.execute(
        "SELECT COUNT(*) FROM research_matchup_compact_season "
        "WHERE list_count(grade_selector_indices) = 864"
    ).fetchone()[0] == 2
    con.close()

    parallel_career_parts = []
    for bucket in range(2):
        career_part = tmp_path / f"parallel-career-{bucket}.duckdb"
        build_compact_career_part(
            season_bundle=output,
            output=career_part,
            career_buckets=2,
            career_bucket=bucket,
        )
        parallel_career_parts.append(career_part)
    parallel = tmp_path / "parallel-career-merged.duckdb"
    assemble_compact_career_parts(
        base_bundle=output,
        career_parts=parallel_career_parts,
        output=parallel,
    )
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{merged.as_posix()}' AS serial (READ_ONLY)")
    con.execute(f"ATTACH '{parallel.as_posix()}' AS parallel (READ_ONLY)")
    for table in (
        "research_matchup_compact_weekly",
        "research_matchup_compact_season",
        "research_matchup_compact_career",
    ):
        assert con.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM serial.{table} EXCEPT ALL SELECT * FROM parallel.{table})"
        ).fetchone() == (0,)
        assert con.execute(
            f"SELECT COUNT(*) FROM (SELECT * FROM parallel.{table} EXCEPT ALL SELECT * FROM serial.{table})"
        ).fetchone() == (0,)
    con.close()

    subbucket_career_parts = []
    for bucket in range(2):
        career_part = tmp_path / f"subbucket-career-{bucket}.duckdb"
        build_compact_career_part(
            season_bundle=output,
            output=career_part,
            career_buckets=2,
            career_bucket=bucket,
            player_sub_buckets=2,
        )
        subbucket_career_parts.append(career_part)
    subbucket_parallel = tmp_path / "subbucket-career-merged.duckdb"
    assemble_compact_career_parts(
        base_bundle=output,
        career_parts=subbucket_career_parts,
        output=subbucket_parallel,
    )
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{parallel.as_posix()}' AS unsplit (READ_ONLY)")
    con.execute(f"ATTACH '{subbucket_parallel.as_posix()}' AS split (READ_ONLY)")
    assert con.execute(
        "SELECT COUNT(*) FROM (SELECT * FROM unsplit.research_matchup_compact_career EXCEPT ALL SELECT * FROM split.research_matchup_compact_career)"
    ).fetchone() == (0,)
    assert con.execute(
        "SELECT COUNT(*) FROM (SELECT * FROM split.research_matchup_compact_career EXCEPT ALL SELECT * FROM unsplit.research_matchup_compact_career)"
    ).fetchone() == (0,)
    con.close()

    explicit = tmp_path / "explicit-player-list.duckdb"
    build_task(snapshot, ops, explicit, year=2025, position="RB", player_ids=["p1"])
    con = duckdb.connect(str(explicit), read_only=True)
    assert con.execute("SELECT NFL_player_id,year,week,position FROM research_matchup_compact_weekly").fetchall() == [
        ("p1", 2025, 1, "RB")
    ]
    assert con.execute("SELECT NFL_player_id,year,position FROM research_matchup_compact_season").fetchall() == [
        ("p1", 2025, "RB")
    ]
    con.close()

    bucket_parts = []
    for bucket in range(2):
        part = tmp_path / f"bucket-{bucket}.duckdb"
        build_task(snapshot, ops, part, year=2025, position="RB", player_bucket_count=2, player_bucket=bucket)
        bucket_parts.append(part)
    bucketed = tmp_path / "bucketed.duckdb"
    merge_parts(bucket_parts, bucketed)
    con = duckdb.connect(str(bucketed), read_only=True)
    assert con.execute("SELECT NFL_player_id,year,week,position FROM research_matchup_compact_weekly ORDER BY 1,2,3,4").fetchall() == [
        ("p1", 2025, 1, "RB"), ("p2", 2025, 1, "RB")
    ]
    assert con.execute("SELECT NFL_player_id,year,position FROM research_matchup_compact_season ORDER BY 1,2,3").fetchall() == [
        ("p1", 2025, "RB"), ("p2", 2025, "RB")
    ]
    con.close()

    sharded = tmp_path / "sharded-career.duckdb"
    merge_parts([output], sharded, career_buckets=2)
    con = duckdb.connect(str(sharded), read_only=True)
    assert con.execute("SELECT NFL_player_id FROM research_matchup_compact_career ORDER BY 1").fetchall() == [
        ("p1",), ("p2",)
    ]
    assert con.execute("""
      SELECT COUNT(*) FROM (
        SELECT NFL_player_id, position FROM research_matchup_compact_career
        GROUP BY 1,2 HAVING COUNT(*) <> 1
      )
    """).fetchone()[0] == 0
    con.close()

    lane = tmp_path / "lane.duckdb"
    merge_parts([output], lane, include_career=False)
    con = duckdb.connect(str(lane), read_only=True)
    tables = {row[0] for row in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
    assert "research_matchup_compact_career" not in tables
    assert {"research_matchup_compact_weekly", "research_matchup_compact_season"} <= tables
    con.close()


def test_build_task_returns_compact_boundary_metrics(tmp_path) -> None:
    snapshot = tmp_path / "snapshot.duckdb"
    ops = tmp_path / "ops.duckdb"
    output = tmp_path / "part.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("CREATE TABLE public.player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    con.execute("CREATE TABLE public.player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    con.execute("INSERT INTO public.player_team_game_week VALUES ('p1', 2025, 1), ('p2', 2025, 1)")
    con.execute("INSERT INTO public.player_active_week VALUES ('p1', 2025, 1), ('p2', 2025, 1)")
    con.execute("""
      CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        position VARCHAR, cohort_position_eligible INTEGER,
        cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
        cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
        cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
        is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
        is_playoffs INTEGER, champion INTEGER
      )
    """)
    con.execute("""
      INSERT INTO public.player_fantasy
      SELECT 'l' || i::VARCHAR,2025,1,'p1','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,2.0,1,1
      FROM range(150) t(i)
      UNION ALL
      SELECT 'l' || i::VARCHAR,2025,1,'p2','RB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,0,NULL,NULL,0,0
      FROM range(150) t(i)
    """)
    con.close()
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("""
      CREATE OR REPLACE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR,
        position VARCHAR, nfl_team VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
        special_teams_snaps BIGINT
      )
    """)
    con.execute("""
      INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('p1',2025,1,'REG','RB','SF',1,0,0),
        ('p2',2025,1,'REG','RB','SF',1,0,0)
    """)
    con.close()

    metrics = build_task(snapshot, ops, output, year=2025, position="RB", player_ids=["p1"])

    assert metrics["target_players"] == 1
    assert metrics["weekly_outer_rows"] == 1
    assert metrics["season_outer_rows"] == 1
    assert {"narrow_source", "weekly", "season_core", "season_grades", "season_outer"} <= set(metrics["phases_seconds"])

def test_build_task_uses_cache_positions_for_all_eligible_rostered_players(tmp_path) -> None:
    snapshot = tmp_path / "snapshot.duckdb"
    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute(
        "CREATE TABLE public.player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    con.execute(
        "CREATE TABLE public.player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)"
    )
    con.execute(
        """
        INSERT INTO public.player_team_game_week VALUES
          ('fb',2025,1), ('punter',2025,1), ('unknown',2025,1), ('two-way',2025,1)
        """
    )
    con.execute(
        """
        INSERT INTO public.player_active_week VALUES
          ('fb',2025,1), ('punter',2025,1), ('unknown',2025,1), ('two-way',2025,1)
        """
    )
    con.execute("""
      CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        position VARCHAR, cohort_position_eligible INTEGER,
        cohort_teams VARCHAR, cohort_roster VARCHAR, cohort_scoring VARCHAR,
        cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
        cohort_dynasty VARCHAR, cohort_best_ball VARCHAR,
        is_rostered INTEGER, is_started INTEGER, win DOUBLE, clutch_equity DOUBLE,
        is_playoffs INTEGER, champion INTEGER
      )
    """)
    con.execute("""
      INSERT INTO public.player_fantasy
      SELECT 'l' || i::VARCHAR,2025,1,'fb','FB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.0,0,0
      FROM range(150) t(i)
      UNION ALL
      SELECT 'l' || i::VARCHAR,2025,1,'punter','P',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.0,0,0
      FROM range(150) t(i)
      UNION ALL
      SELECT 'l' || i::VARCHAR,2025,1,'unknown',NULL,1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.0,0,0
      FROM range(150) t(i)
      UNION ALL
      SELECT 'l' || i::VARCHAR,2025,1,'two-way','CB',1,'12tm','flx','half','4pt','6po','redraft','managed',1,1,1.0,0.0,0,0
      FROM range(150) t(i)
    """)
    con.close()
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("""
      CREATE OR REPLACE TABLE nfl_historical.nfl_player_stats_all (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR,
        position VARCHAR, nfl_team VARCHAR, offense_snaps BIGINT, defense_snaps BIGINT,
        special_teams_snaps BIGINT
      )
    """)
    con.execute("""
      INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('fb',2025,1,'REG','RB','SF',1,0,0),
        ('punter',2025,1,'REG','P','SF',0,0,1),
        ('unknown',2025,1,'REG','DL','SF',0,1,0),
        ('two-way',2025,1,'REG','WR,DB','SF',1,1,0)
    """)
    con.close()

    parts = []
    for position in ("RB", "DL", "WR", "DB"):
        part = tmp_path / f"{position}.duckdb"
        build_task(snapshot, ops, part, year=2025, position=position)
        parts.append(part)

    with pytest.raises(ValueError, match="unsupported research position"):
        build_task(snapshot, ops, tmp_path / "P.duckdb", year=2025, position="P")

    merged = tmp_path / "merged.duckdb"
    merge_parts(parts, merged)
    con = duckdb.connect(str(merged), read_only=True)
    assert con.execute("""
      SELECT NFL_player_id,position FROM research_matchup_compact_season ORDER BY 1,2
    """).fetchall() == [
        ("fb", "RB"),
        ("two-way", "DB"),
    ]
    assert con.execute("""
      SELECT NFL_player_id,position FROM research_matchup_compact_weekly ORDER BY 1,2
    """).fetchall() == [
        ("fb", "RB"),
        ("two-way", "DB"),
    ]
    con.close()


def test_outer_mismatch_diagnostic_reports_regular_stats_team_coverage() -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE core (NFL_player_id VARCHAR, year INTEGER, position VARCHAR)")
    con.execute("CREATE TABLE grade (NFL_player_id VARCHAR, year INTEGER, position VARCHAR)")
    con.execute("CREATE TABLE stats (NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, nfl_team VARCHAR)")
    con.execute("""
      CREATE TABLE source (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, db_name VARCHAR, position VARCHAR,
        cohort_position_eligible INTEGER, cohort_teams VARCHAR, cohort_roster VARCHAR,
        cohort_scoring VARCHAR, cohort_pass_td VARCHAR, cohort_playoff_teams VARCHAR,
        cohort_dynasty VARCHAR, cohort_best_ball VARCHAR, is_rostered INTEGER
      )
    """)
    con.execute("INSERT INTO grade VALUES ('missing-team',2020,'OL')")
    con.execute("INSERT INTO stats VALUES ('missing-team',2020,1,'REG',NULL),('missing-team',2020,2,'REG',NULL)")
    con.execute("INSERT INTO source VALUES ('missing-team',2020,1,'league-a','OL',1,'12t','flx','half','4pt','6po','redraft','managed',1)")

    assert compact_outer_mismatch_diagnostics(con, "core", "grade", "stats", "source") == [
        {
            "NFL_player_id": "missing-team",
            "year": 2020,
            "position": "OL",
            "mismatch": "grade_only",
            "regular_stat_rows": 2,
            "non_null_team_rows": 0,
            "teams": [],
            "source_rows": 1,
            "explicit_rostered_source_rows": 1,
            "source_weeks": [1],
            "scheduled_source_rows": 0,
            "resolved_profile_source_rows": 1,
            "scheduled_resolved_profile_source_rows": 0,
            "league_week_profile_source_rows": 1,
            "scheduled_league_week_profile_source_rows": 0,
        }
    ]
    con.close()
