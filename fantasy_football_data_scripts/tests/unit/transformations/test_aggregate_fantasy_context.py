from multi_league.transformations.aggregation.aggregate_fantasy_context import (
    _drop_scoped_nfl_lookup_tables,
    _prepare_scoped_nfl_lookup_tables,
    _scoped_nfl_lookup_ctes,
)


def test_nfl_lookup_ctes_are_limited_to_the_requested_league_players():
    sql = _scoped_nfl_lookup_ctes("league_a")

    assert "needed_player_weeks AS MATERIALIZED" in sql
    assert "needed_player_ids AS MATERIALIZED" in sql
    assert "db_name = 'league_a'" in sql
    assert "INNER JOIN needed_player_weeks" in sql
    assert "INNER JOIN needed_player_ids" in sql
    assert "FROM ___ops.nfl_historical.nfl_player_stats_all" in sql


class _RecordingConnection:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        return self


def test_prepared_nfl_lookups_are_reused_without_rescanning_ops():
    conn = _RecordingConnection()

    _prepare_scoped_nfl_lookup_tables(conn, "league_a")
    sql = _scoped_nfl_lookup_ctes("league_a", prepared=True)

    assert len(conn.statements) == 2
    assert sum("___ops.nfl_historical.nfl_player_stats_all" in statement for statement in conn.statements) == 2
    assert "FROM _weekly_refresh_super_table_dedup" in sql
    assert "FROM _weekly_refresh_nfl_team_dedup" in sql
    assert "___ops.nfl_historical.nfl_player_stats_all" not in sql


def test_nfl_lookups_use_the_scoped_fallback_unless_explicitly_prepared():
    sql = _scoped_nfl_lookup_ctes("league_b")

    assert "___ops.nfl_historical.nfl_player_stats_all" in sql
    assert "FROM _weekly_refresh_super_table_dedup" not in sql


def test_dropping_prepared_nfl_lookups_releases_both_temp_tables():
    conn = _RecordingConnection()
    _prepare_scoped_nfl_lookup_tables(conn, "league_a")

    _drop_scoped_nfl_lookup_tables(conn)

    assert conn.statements[-2:] == [
        "DROP TABLE IF EXISTS _weekly_refresh_super_table_dedup",
        "DROP TABLE IF EXISTS _weekly_refresh_nfl_team_dedup",
    ]
