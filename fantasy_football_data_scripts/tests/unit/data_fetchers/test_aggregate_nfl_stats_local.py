import duckdb

from multi_league.data_fetchers.aggregate_nfl_stats_fly import LocalDuckDBWriter


def test_local_writer_executes_against_an_attached_ops_artifact(tmp_path):
    """The aggregate builder can run offline without a Fly HTTP client."""
    artifact = tmp_path / "ops.duckdb"
    seed = duckdb.connect(str(artifact))
    seed.execute("CREATE SCHEMA nfl_historical")
    seed.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (NFL_player_id VARCHAR, year INTEGER)")
    seed.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('p1', 2026)")
    seed.close()

    con = duckdb.connect()
    con.execute(f"ATTACH '{artifact.as_posix()}' AS ___ops")
    writer = LocalDuckDBWriter(con)

    assert writer.execute(
        "SELECT NFL_player_id, year FROM ___ops.nfl_historical.nfl_player_stats_all",
        database="___ops",
    ) == [{"NFL_player_id": "p1", "year": 2026}]
    writer.execute("CREATE TABLE ___ops.nfl_historical.local_rollup AS SELECT 1 AS ok", database="___ops")
    assert con.execute("SELECT ok FROM ___ops.nfl_historical.local_rollup").fetchone() == (1,)
    con.close()
