"""Release-gate identities for explicit matchup expected outcomes."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from assemble_research_shards import check_population_cover
from release_gate import adaptive_table_violations, matchup_identity_violations, metric_bounds


def test_matchup_identity_gate_finds_expected_record_overcount():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup (
      expected_wins_12t_flx_half_4pt DOUBLE,
      expected_losses_12t_flx_half_4pt DOUBLE,
      expected_starts_12t_flx_half_4pt DOUBLE,
      start_rate_12t_flx_half_4pt DOUBLE)""")
    con.execute("INSERT INTO research_matchup VALUES (8.0, 7.0, 14.0, 90.0)")

    errors = matchup_identity_violations(
        con, "research_matchup", "12t_flx_half_4pt", weekly=False
    )
    con.close()

    assert errors == {"expected_record": 1}


def test_weekly_identity_gate_checks_start_rate_fraction():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup_weekly (
      expected_wins_12t_flx_half_4pt DOUBLE,
      expected_losses_12t_flx_half_4pt DOUBLE,
      expected_starts_12t_flx_half_4pt DOUBLE,
      start_rate_12t_flx_half_4pt DOUBLE)""")
    con.execute("INSERT INTO research_matchup_weekly VALUES (0.6, 0.3, 0.9, 80.0)")

    errors = matchup_identity_violations(
        con, "research_matchup_weekly", "12t_flx_half_4pt", weekly=True
    )
    con.close()

    assert errors == {"weekly_start_rate": 1}


def test_identity_gate_rejects_unresolved_expected_start():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup_weekly (
      expected_wins_12t_flx_half_4pt DOUBLE,
      expected_losses_12t_flx_half_4pt DOUBLE,
      expected_starts_12t_flx_half_4pt DOUBLE,
      start_rate_12t_flx_half_4pt DOUBLE)""")
    con.execute("INSERT INTO research_matchup_weekly VALUES (NULL, NULL, 0.8, 80.0)")

    errors = matchup_identity_violations(
        con, "research_matchup_weekly", "12t_flx_half_4pt", weekly=True
    )
    con.close()

    assert errors == {"expected_record_missing": 1}


def test_win_rate_gate_uses_expected_wins_over_expected_starts():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup (
      expected_wins_12t_flx_half_4pt DOUBLE,
      expected_losses_12t_flx_half_4pt DOUBLE,
      expected_starts_12t_flx_half_4pt DOUBLE,
      start_rate_12t_flx_half_4pt DOUBLE,
      win_rate_12t_flx_half_4pt DOUBLE)""")
    con.execute("INSERT INTO research_matchup VALUES (6.0, 4.0, 10.0, 50.0, 70.0)")

    errors = matchup_identity_violations(
        con, "research_matchup", "12t_flx_half_4pt", weekly=False
    )
    con.close()

    assert errors == {"win_rate_expected_starts": 1}


def test_release_bounds_follow_the_metric_grain():
    assert metric_bounds("research_matchup_weekly", "ppg") == (-10.0, 70.0)
    assert metric_bounds("research_matchup_weekly", "lamar") == (-35.0, 65.0)
    assert metric_bounds("research_draft_career", "points") == (-500.0, 20000.0)
    assert metric_bounds("research_draft_career", "lamar") == (-5000.0, 10000.0)
    assert metric_bounds("research_transactions", "add_lamar") == (-600.0, 600.0)
    for stat in ("expected_wins", "expected_losses", "expected_starts"):
        assert metric_bounds("research_matchup_weekly", stat) == (0.0, 1.0)
        assert metric_bounds("research_matchup", stat) == (0.0, 18.0)


def test_adaptive_gate_rejects_duplicate_request_cells():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup_adaptive (
      q_teams VARCHAR, q_roster VARCHAR, q_ppr VARCHAR, q_td VARCHAR,
      q_bracket VARCHAR, q_league_type VARCHAR, q_lineup_mode VARCHAR,
      NFL_player_id VARCHAR, pos_grp VARCHAR, year INTEGER)""")
    row = "'12t','flx','half','4pt','6po','ALL','ALL','p1','RB',2024"
    con.execute(f"INSERT INTO research_matchup_adaptive VALUES ({row})")
    con.execute(f"INSERT INTO research_matchup_adaptive VALUES ({row})")
    assert adaptive_table_violations(con, "research_matchup_adaptive") == {
        "duplicate_request_player": 1
    }
    con.close()


def test_adaptive_gate_rejects_missing_request_key():
    con = duckdb.connect()
    con.execute("""CREATE TABLE research_matchup_adaptive_weekly (
      q_teams VARCHAR, q_roster VARCHAR, q_ppr VARCHAR, q_td VARCHAR,
      q_bracket VARCHAR, q_league_type VARCHAR, q_lineup_mode VARCHAR,
      NFL_player_id VARCHAR, pos_grp VARCHAR, year INTEGER, week INTEGER)""")
    con.execute("""INSERT INTO research_matchup_adaptive_weekly VALUES
      ('12t','flx','half','4pt',NULL,'ALL','ALL','p1','RB',2024,1)""")
    assert adaptive_table_violations(
        con, "research_matchup_adaptive_weekly"
    ) == {"null_request_key": 1}
    con.close()


def test_population_cover_rejects_mixed_source_fingerprints(tmp_path):
    con = duckdb.connect()
    for index, fingerprint in enumerate(("good", "other")):
        root = tmp_path / f"shard{index}"
        root.mkdir()
        path = root / "shard_manifest.parquet"
        pq.write_table(pa.Table.from_pylist([{
            "table": "matchup", "year_start": 2024, "year_end": 2024,
            "bucket": None, "buckets": None, "fingerprint": fingerprint,
            "target_hash": None,
        }]), path)
    with pytest.raises(ValueError, match="mixed source fingerprints"):
        check_population_cover(con, [tmp_path], "matchup")
    con.close()
