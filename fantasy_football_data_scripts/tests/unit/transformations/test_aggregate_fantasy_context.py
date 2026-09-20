from multi_league.transformations.aggregation.aggregate_fantasy_context import (
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
