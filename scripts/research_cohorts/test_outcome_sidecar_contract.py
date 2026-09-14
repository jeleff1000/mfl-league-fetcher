import duckdb
import pytest

from apply_outcome_sidecar_only import validate_outcome_payload


def test_rejects_target_inventory_without_outcome_values():
    con = duckdb.connect()
    con.execute(
        """
        create table outcome_targets (
          db_name varchar, year integer, week integer, NFL_player_id varchar,
          win integer, team_points double
        )
        """
    )
    con.execute("insert into outcome_targets values ('league', 2020, 1, 'p1', NULL, NULL)")

    with pytest.raises(ValueError, match="no populated outcome values"):
        validate_outcome_payload(con, "outcome_targets")


def test_accepts_payload_with_populated_outcome_values():
    con = duckdb.connect()
    con.execute(
        """
        create table outcome_targets (
          db_name varchar, year integer, week integer, NFL_player_id varchar,
          win integer, team_points double
        )
        """
    )
    con.execute("insert into outcome_targets values ('league', 2020, 1, 'p1', 1, 100.0)")

    assert validate_outcome_payload(con, "outcome_targets") == {
        "rows": 1,
        "populated_rows": 1,
        "value_columns": ["win", "team_points"],
    }
