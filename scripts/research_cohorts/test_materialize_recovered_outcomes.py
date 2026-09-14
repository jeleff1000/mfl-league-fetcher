import duckdb
import pytest

from materialize_recovered_outcomes import build


def test_only_recovered_rows_are_materialized(tmp_path):
    source = tmp_path / "classification.parquet"
    out = tmp_path / "overlay.duckdb"
    con = duckdb.connect()
    con.execute(
        """
        create table t as select * from (values
          ('l', 2023, 15, 'p1', 'recovered', 1, 0, 0, 100.0),
          ('l', 2023, 16, 'p2', 'no_opponent', NULL, NULL, NULL, 120.0)
        ) x(db_name, year, week, NFL_player_id, status, win, loss, tie, source_team_points)
        """
    )
    # The production contract is intentionally exact: a full audited artifact
    # must contain all 6,789 recovered rows before promotion.
    con.execute("copy t to ? (format parquet)", [str(source)])
    con.close()
    with pytest.raises(ValueError, match="expected 6,789"):
        build(source, out)
