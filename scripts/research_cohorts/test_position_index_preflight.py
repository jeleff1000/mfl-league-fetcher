import duckdb
import pytest

from build_research_matchup_cohort import validate_observed_position_index


def _connection():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.player_fantasy AS SELECT * FROM (VALUES
        ('p1', 2024), ('p2', 2024)
    ) t(NFL_player_id, year)""")
    return con


def test_position_index_preflight_rejects_missing_cache():
    with pytest.raises(RuntimeError, match="position index|mapped 0"):
        validate_observed_position_index(_connection())


def test_position_index_preflight_accepts_mapped_modern_rows():
    con = _connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS ops.nfl_historical")
    con.execute("DROP TABLE IF EXISTS ops.nfl_historical.nfl_player_stats_all")
    con.execute("""CREATE TABLE ops.nfl_historical.nfl_player_stats_all AS
        SELECT * FROM (VALUES ('p1', 2024, 'RB'), ('p2', 2024, 'WR'))
        t(NFL_player_id, year, position)""")
    validate_observed_position_index(con)
