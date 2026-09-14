from pathlib import Path
from uuid import uuid4

import duckdb
import pytest

from scripts.research_cohorts.normalize_canonical_research_schema import (
    audit_team_cohort_allocation,
    inspect_missing_team_count_league_years,
    normalize,
)


def test_normalize_materializes_player_id_defensive_positions_and_slot_eligibility(tmp_path: Path) -> None:
    suffix = uuid4().hex
    base = tmp_path / f"base_{suffix}.duckdb"
    ops = tmp_path / f"nfl_cache_{suffix}.duckdb"
    out = tmp_path / f"normalized_{suffix}.duckdb"

    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_playoffs INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('db_only', 2025, 1, 'db_player', 'CB', 0),
          ('db_only', 2025, 1, 'dl_player', 'CB', 0),
          ('db_only', 2025, 1, 'lb_player', 'LB', 0)
    """)
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR,
          num_teams INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
          is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
          roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
          roster_K INTEGER, roster_DEF INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('db_only', 2025, 'sleeper', 12, .5, 4, 6, false, false,
           0, 0, 0, 1, 1, 0, 0, 1, 1)
    """)
    con.close()

    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR, nfl_position VARCHAR)")
    con.execute("""
        CREATE TABLE nfl_historical.nfl_player_stats_all (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, nfl_team VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO nfl_historical.player_bio VALUES
          ('db_player', 'SS'), ('dl_player', 'DE'), ('lb_player', 'LB')
    """)
    con.close()

    normalize(base, out, ops)

    con = duckdb.connect(str(out), read_only=True)
    rows = con.execute("""
        SELECT NFL_player_id, position, cohort_position_eligible
        FROM public.player_fantasy
        ORDER BY NFL_player_id
    """).fetchall()
    assert rows == [
        ('db_player', 'DB', 1),
        ('dl_player', 'DL', 0),
        ('lb_player', 'LB', 1),
    ]


def test_normalize_keeps_literal_8_10_12_14_team_size_buckets(tmp_path: Path) -> None:
    """The team cohort is league size, never roster/start capacity."""
    base = tmp_path / "base.duckdb"
    out = tmp_path / "normalized.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_playoffs INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('league_8', 2025, 1, 'rb_8', 'RB', 0),
          ('league_10', 2025, 1, 'rb_10', 'RB', 0),
          ('league_10_b', 2025, 1, 'rb_10_b', 'RB', 0),
          ('league_12', 2025, 1, 'rb_12', 'RB', 0),
          ('league_14', 2025, 1, 'rb_14', 'RB', 0)
    """)
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR,
          num_teams INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
          is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
          roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
          roster_K INTEGER, roster_DEF INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('league_8', 2025, 'sleeper', 8, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('league_10', 2025, 'sleeper', 10, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('league_10_b', 2025, 'sleeper', 10, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('league_12', 2025, 'sleeper', 12, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('league_14', 2025, 'sleeper', 14, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1)
    """)
    con.close()

    normalize(base, out)

    con = duckdb.connect(str(out), read_only=True)
    assert con.execute("""
        SELECT db_name, cohort_teams
        FROM public.player_fantasy
        ORDER BY db_name
    """).fetchall() == [
        ("league_10", "10tm"),
        ("league_10_b", "10tm"),
        ("league_12", "12tm"),
        ("league_14", "14tm"),
        ("league_8", "08tm"),
    ]
    audit = audit_team_cohort_allocation(con)
    assert audit["non_historical_league_years"] == 5
    assert audit["team_buckets"] == {"08tm": 1, "10tm": 2, "12tm": 1, "14tm": 1}
    assert audit["ten_twelve_share"] == 0.6
    con.close()


def test_normalize_pools_league_year_without_a_real_team_count(tmp_path: Path) -> None:
    """A missing team count must not silently become a specific team cohort."""
    base = tmp_path / "base.duckdb"
    out = tmp_path / "normalized.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_playoffs INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR,
          num_teams INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
          is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
          roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
          roster_K INTEGER, roster_DEF INTEGER
        )
    """)
    con.execute("INSERT INTO public.player_fantasy VALUES ('unknown_size', 2025, 1, 'rb', 'RB', 0)")
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('unknown_size', 2025, 'sleeper', NULL, .5, 4, 6, false, false,
           0, 0, 0, 0, 0, 0, 0, 1, 1)
    """)
    con.close()

    normalize(base, out)
    con = duckdb.connect(str(out), read_only=True)
    assert con.execute("SELECT cohort_teams FROM public.player_fantasy").fetchone() == ("ALL",)
    audit = audit_team_cohort_allocation(con)
    assert audit["unknown_team_count_league_years"] == 1
    con.close()


def test_missing_team_count_inventory_reports_only_the_unallocated_league_year(tmp_path: Path) -> None:
    """The fail-fast inventory gives evidence before a fallback is ever chosen."""
    base = tmp_path / "base.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, team_key VARCHAR, NFL_player_id VARCHAR
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('known', 2025, 'known.t.1', 'a'),
          ('unknown', 2025, 'unknown.t.1', 'b'),
          ('unknown', 2025, 'unknown.t.2', 'c')
    """)
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR, num_teams INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('known', 2025, 'sleeper', 12),
          ('unknown', 2025, 'yahoo', NULL)
    """)

    assert inspect_missing_team_count_league_years(con) == [{
        "db_name": "unknown",
        "year": 2025,
        "settings_rows": 1,
        "platform": "yahoo",
        "num_teams": None,
        "player_rows": 2,
        "distinct_team_keys": 2,
    }]
    con.close()


def test_normalize_pools_unknown_team_count_without_inventing_a_size(tmp_path: Path) -> None:
    """A league with no factual size remains in the all-leagues pool."""
    base = tmp_path / "base.duckdb"
    out = tmp_path / "normalized.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_playoffs INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.player_fantasy VALUES
          ('known_10', 2025, 1, 'rb_10', 'RB', 0),
          ('known_12', 2025, 1, 'rb_12', 'RB', 0),
          ('unknown', 2025, 1, 'rb_unknown', 'RB', 0)
    """)
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR,
          num_teams INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
          is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
          roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
          roster_K INTEGER, roster_DEF INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('known_10', 2025, 'sleeper', 10, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('known_12', 2025, 'sleeper', 12, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1),
          ('unknown', 2025, 'mfl', NULL, .5, 4, 6, false, false, 0, 0, 0, 0, 0, 0, 0, 1, 1)
    """)
    con.close()

    normalize(base, out)

    con = duckdb.connect(str(out), read_only=True)
    assert con.execute("""
        SELECT db_name, cohort_teams
        FROM public.player_fantasy
        ORDER BY db_name
    """).fetchall() == [
        ("known_10", "10tm"),
        ("known_12", "12tm"),
        ("unknown", "ALL"),
    ]
    audit = audit_team_cohort_allocation(con)
    assert audit["unknown_team_count_league_years"] == 1
    assert audit["ten_twelve_share"] == 1.0
    con.close()


def test_normalize_rebuilds_existing_availability_lookups_from_ops_cache(tmp_path: Path) -> None:
    """A prior canonical cache may already contain derived availability tables."""
    base = tmp_path / "base.duckdb"
    ops = tmp_path / "ops.duckdb"
    out = tmp_path / "normalized.duckdb"
    con = duckdb.connect(str(base))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.player_fantasy (
          db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
          position VARCHAR, is_playoffs INTEGER
        )
    """)
    con.execute("INSERT INTO public.player_fantasy VALUES ('league', 2025, 1, 'rb', 'RB', 0)")
    con.execute("""
        CREATE TABLE public.league_settings (
          db_name VARCHAR, year INTEGER, platform VARCHAR,
          num_teams INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
          is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN,
          roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
          roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
          roster_K INTEGER, roster_DEF INTEGER
        )
    """)
    con.execute("""
        INSERT INTO public.league_settings VALUES
          ('league', 2025, 'sleeper', 12, .5, 4, 6, false, false,
           0, 0, 0, 0, 0, 0, 0, 1, 1)
    """)
    con.execute("CREATE TABLE public.player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    con.execute("INSERT INTO public.player_active_week VALUES ('stale', 1999, 1)")
    con.execute("CREATE TABLE public.player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    con.execute("INSERT INTO public.player_team_game_week VALUES ('stale', 1999, 1)")
    con.close()

    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("CREATE TABLE IF NOT EXISTS nfl_historical.player_bio (NFL_player_id VARCHAR, nfl_position VARCHAR)")
    con.execute("DROP TABLE IF EXISTS nfl_historical.nfl_player_stats_all")
    con.execute("""
        CREATE TABLE nfl_historical.nfl_player_stats_all (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR, nfl_team VARCHAR
        )
    """)
    con.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('rb', 2025, 1, 'REG', 'SF')")
    con.close()

    normalize(base, out, ops)

    con = duckdb.connect(str(out), read_only=True)
    assert con.execute("SELECT * FROM public.player_active_week").fetchall() == [("rb", 2025, 1)]
    assert con.execute("SELECT * FROM public.player_team_game_week").fetchall() == [("rb", 2025, 1)]
    con.close()
