from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from multi_league.core.league_update_validation import (
    IncompleteSourceError,
    ProviderSnapshotExpectations,
    validate_active_roster_frame,
    validate_espn_final_matchup_frame,
    validate_provider_team_inventory,
    validate_tabular_active_scope,
    validate_provider_snapshot,
    validate_yahoo_scoreboard_pair_graph,
    validate_yahoo_week_matchup_scope,
)


def test_post_transform_player_scope_rejects_lost_provider_ids_and_unmapped_scored_rows():
    import duckdb

    from multi_league.core.league_update_validation import (
        assert_transformed_active_player_scope,
        capture_active_provider_player_scope,
    )

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.player_fantasy ("
        "db_name VARCHAR, year INTEGER, week INTEGER, espn_player_id VARCHAR, "
        "fantasy_points DOUBLE, NFL_player_id VARCHAR)"
    )
    conn.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "('afi_data', 2026, 1, '101', 14.0, NULL), "
        "('afi_data', 2026, 1, '102', 0.0, NULL), "
        "('other', 2026, 1, '999', 50.0, NULL)"
    )
    expected = capture_active_provider_player_scope(
        conn, db_name="afi_data", year=2026, weeks=(1,), provider_id_column="espn_player_id"
    )
    assert expected == {(1, "101"), (1, "102")}
    conn.execute("UPDATE public.player_fantasy SET NFL_player_id='00-101' WHERE espn_player_id='101'")
    assert assert_transformed_active_player_scope(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column="espn_player_id", expected_keys=expected,
    )["mapped_scored_players"] == 1
    conn.execute("DELETE FROM public.player_fantasy WHERE espn_player_id='102'")
    with pytest.raises(IncompleteSourceError, match="provider player IDs disappeared"):
        assert_transformed_active_player_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", expected_keys=expected,
        )
    conn.execute("INSERT INTO public.player_fantasy VALUES ('afi_data', 2026, 1, '102', 0.0, NULL)")
    conn.execute("INSERT INTO public.player_fantasy VALUES ('afi_data', 2026, 1, 'phantom', 0.0, NULL)")
    with pytest.raises(IncompleteSourceError, match="provider player inventory changed"):
        assert_transformed_active_player_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", expected_keys=expected,
        )
    conn.execute("DELETE FROM public.player_fantasy WHERE espn_player_id='phantom'")
    conn.execute("UPDATE public.player_fantasy SET NFL_player_id='ESPN-101' WHERE espn_player_id='101'")
    with pytest.raises(IncompleteSourceError, match="scored provider players lack NFL mappings"):
        assert_transformed_active_player_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", expected_keys=expected,
        )
    conn.execute("UPDATE public.player_fantasy SET NFL_player_id='00-101', fantasy_points='inf' WHERE espn_player_id='101'")
    with pytest.raises(IncompleteSourceError, match="score is malformed"):
        assert_transformed_active_player_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", expected_keys=expected,
        )


@pytest.mark.parametrize('provider_column', ['yahoo_player_id', 'espn_player_id', 'sleeper_player_id'])
def test_malformed_score_identifies_the_exact_provider_row(provider_column):
    import duckdb
    from multi_league.core.league_update_validation import assert_transformed_active_player_scope

    with duckdb.connect() as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute(f'CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, {provider_column} VARCHAR, fantasy_points DOUBLE, NFL_player_id VARCHAR)')
        conn.execute("INSERT INTO public.player_fantasy VALUES ('test_league',2026,1,'player-42',NULL,'00-42')")
        with pytest.raises(IncompleteSourceError) as failed:
            assert_transformed_active_player_scope(
                conn, db_name='test_league', year=2026, weeks=(1,),
                provider_id_column=provider_column, expected_keys={(1,'player-42')},
            )
        assert f'{provider_column}=player-42' in str(failed.value)
        assert 'year=2026 week=1' in str(failed.value)


def test_post_transform_player_scope_rejects_missing_required_columns():
    import duckdb

    from multi_league.core.league_update_validation import capture_active_provider_player_scope

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER)")
    with pytest.raises(IncompleteSourceError, match="provider player identity column"):
        capture_active_provider_player_scope(
            conn, db_name="test", year=2026, weeks=(1,), provider_id_column="yahoo_player_id"
        )


def test_post_transform_matchup_scope_pins_late_provider_score_to_published_input():
    import duckdb

    from multi_league.core.league_update_validation import (
        assert_transformed_active_matchup_scope,
        capture_active_final_matchup_scope,
    )

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, "
        "team_key VARCHAR, team_points DOUBLE, opponent_points DOUBLE)"
    )
    conn.execute(
        "INSERT INTO public.matchup VALUES "
        "('afi_data', 2026, 1, 'gray', 152.66, 129.66), "
        "('afi_data', 2026, 1, 'opponent', 129.66, 152.66), "
        "('other', 2026, 1, 'gray', 151.66, 129.66)"
    )
    expected = capture_active_final_matchup_scope(
        conn, db_name="afi_data", year=2026, weeks=(1,)
    )
    assert assert_transformed_active_matchup_scope(
        conn, db_name="afi_data", year=2026, weeks=(1,), expected_scores=expected
    )["scored_team_weeks"] == 2
    conn.execute("UPDATE public.matchup SET team_points=151.66 WHERE db_name='afi_data' AND team_key='gray'")
    with pytest.raises(IncompleteSourceError, match="provider score changed"):
        assert_transformed_active_matchup_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,), expected_scores=expected
        )
    conn.execute("DELETE FROM public.matchup WHERE db_name='afi_data' AND team_key='opponent'")
    with pytest.raises(IncompleteSourceError, match="scored team-weeks disappeared"):
        assert_transformed_active_matchup_scope(
            conn, db_name="afi_data", year=2026, weeks=(1,), expected_scores=expected
        )
    conn.execute("UPDATE public.matchup SET team_points='inf' WHERE db_name='afi_data' AND team_key='gray'")
    with pytest.raises(IncompleteSourceError, match="nonfinite points"):
        capture_active_final_matchup_scope(conn, db_name="afi_data", year=2026, weeks=(1,))


@pytest.mark.parametrize("provider_id_column", ["espn_player_id", "yahoo_player_id", "sleeper_player_id"])
def test_new_scored_provider_players_require_published_career_aggregates(provider_id_column):
    import duckdb

    from multi_league.core.league_update_validation import assert_refresh_derived_output_health

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, "
        f"{provider_id_column} VARCHAR, NFL_player_id VARCHAR, fantasy_points DOUBLE)"
    )
    conn.execute(
        "INSERT INTO public.player_fantasy VALUES "
        "('afi_data',2026,1,'rookie','00-rookie',14.0), "
        "('other',2026,1,'rookie','00-rookie',14.0)"
    )
    for table in ("player_fantasy_career", "player_fantasy_career_all"):
        conn.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, NFL_player_id VARCHAR, games_rostered INTEGER, fantasy_points DOUBLE)")
    conn.execute("CREATE TABLE public.homepage_league_summary (db_name VARCHAR)")
    conn.execute("INSERT INTO public.homepage_league_summary VALUES ('afi_data')")
    publish = ("player_fantasy_career", "player_fantasy_career_all", "homepage_league_summary")
    with pytest.raises(IncompleteSourceError, match="player_fantasy_career lacks"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=publish,
        )
    for table in ("player_fantasy_career", "player_fantasy_career_all"):
        conn.execute(f"INSERT INTO public.{table} VALUES ('afi_data','00-rookie',1,14.0)")
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column=provider_id_column, published_tables=publish,
    )["active_scored_career_players"] == 1
    conn.execute("UPDATE public.player_fantasy_career SET games_rostered=0 WHERE NFL_player_id='00-rookie'")
    with pytest.raises(IncompleteSourceError, match="invalid career values"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=publish,
        )
    conn.execute("UPDATE public.player_fantasy_career SET games_rostered=1 WHERE NFL_player_id='00-rookie'")
    # Server-owned publication rebuilds careers on Fly, not from the active-season upload. The
    # worker validates its uploaded homepage while Fly owns career validation.
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column=provider_id_column, published_tables=("homepage_league_summary",),
        server_rebuilds_career_rollups=True,
    )["active_scored_career_players"] == 1
    with pytest.raises(IncompleteSourceError, match="not in the publication bundle"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=(),
            server_rebuilds_career_rollups=True,
        )
    conn.execute("DROP TABLE public.player_fantasy_career")
    conn.execute("DROP TABLE public.player_fantasy_career_all")
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column=provider_id_column, published_tables=("homepage_league_summary",),
        server_rebuilds_career_rollups=True,
    )["active_scored_career_players"] == 1
    for table in ("player_fantasy_career", "player_fantasy_career_all"):
        conn.execute(
            f"CREATE TABLE public.{table} (db_name VARCHAR, NFL_player_id VARCHAR, "
            "games_rostered INTEGER, fantasy_points DOUBLE)"
        )
        conn.execute(f"INSERT INTO public.{table} VALUES ('afi_data','00-rookie',1,14.0)")
    with pytest.raises(IncompleteSourceError, match="careers must not be uploaded"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=publish,
            server_rebuilds_career_rollups=True,
        )
    with pytest.raises(IncompleteSourceError, match="homepage rebuilding requires"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=publish,
            server_rebuilds_homepage_rollups=True,
        )
    with pytest.raises(IncompleteSourceError, match="not in the publication bundle"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column=provider_id_column, published_tables=("homepage_league_summary",),
        )


def test_scored_matchup_franchises_require_career_and_homepage_coverage():
    import duckdb

    from multi_league.core.league_update_validation import assert_refresh_derived_output_health

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        "CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, "
        "espn_player_id VARCHAR, NFL_player_id VARCHAR, fantasy_points DOUBLE)"
    )
    conn.execute(
        "CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, "
        "franchise_id VARCHAR, team_points DOUBLE, opponent_points DOUBLE)"
    )
    conn.execute(
        "INSERT INTO public.matchup VALUES ('afi_data',2026,1,'gray-franchise',152.66,129.66)"
    )
    conn.execute("CREATE TABLE public.homepage_league_summary (db_name VARCHAR)")
    conn.execute("INSERT INTO public.homepage_league_summary VALUES ('afi_data')")
    for table, metric in (
        ("matchup_career", "games"),
        ("homepage_manager_rankings", "seasons"),
        ("homepage_current_standings", "wins"),
    ):
        conn.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, franchise_id VARCHAR, {metric} INTEGER)")
    publish = (
        "homepage_league_summary", "matchup_career",
        "homepage_manager_rankings", "homepage_current_standings",
    )
    with pytest.raises(IncompleteSourceError, match="matchup_career lacks"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", published_tables=publish,
        )
    conn.execute("INSERT INTO public.matchup_career VALUES ('afi_data','gray-franchise',1)")
    conn.execute("INSERT INTO public.homepage_manager_rankings VALUES ('afi_data','gray-franchise',1)")
    conn.execute("INSERT INTO public.homepage_current_standings VALUES ('afi_data','gray-franchise',0)")
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column="espn_player_id", published_tables=publish,
    )["active_scored_career_franchises"] == 1
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column="espn_player_id",
        published_tables=("homepage_league_summary", "homepage_manager_rankings", "homepage_current_standings"),
        server_rebuilds_career_rollups=True,
    )["active_scored_career_franchises"] == 1
    conn.execute("DELETE FROM public.homepage_current_standings WHERE db_name='afi_data'")
    with pytest.raises(IncompleteSourceError, match="homepage_current_standings lacks"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", published_tables=publish,
        )
    for table in ("homepage_league_summary", "homepage_manager_rankings", "homepage_current_standings"):
        conn.execute(f"DROP TABLE public.{table}")
    conn.execute("DROP TABLE public.matchup_career")
    health = assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column="espn_player_id", published_tables=("matchup",),
        server_rebuilds_career_rollups=True,
        server_rebuilds_homepage_rollups=True,
    )
    assert health["active_scored_career_franchises"] == 1
    assert health["homepage_summary_rows"] is None
    assert health["homepage_validation_location"] == "atomic_fly"
    with pytest.raises(IncompleteSourceError, match="homepages must not be uploaded"):
        assert_refresh_derived_output_health(
            conn, db_name="afi_data", year=2026, weeks=(1,),
            provider_id_column="espn_player_id", published_tables=("matchup", "homepage_league_summary"),
            server_rebuilds_career_rollups=True,
            server_rebuilds_homepage_rollups=True,
        )
    # Server-owned mode validates careers/homepages inside the atomic Fly transaction; the
    # worker scratch database intentionally does not build either family.
    assert assert_refresh_derived_output_health(
        conn, db_name="afi_data", year=2026, weeks=(1,),
        provider_id_column="espn_player_id", published_tables=("matchup",),
        server_rebuilds_career_rollups=True,
        server_rebuilds_homepage_rollups=True,
    )["homepage_validation_location"] == "atomic_fly"


def test_yahoo_scoreboard_pair_graph_requires_complete_reciprocal_coverage():
    rows = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "t1", "opponent_team_key": "t2", "team_points": 152.66, "opponent_points": 129.66},
        {"year": 2026, "week": 1, "team_key": "t2", "opponent_team_key": "t1", "team_points": 129.66, "opponent_points": 152.66},
    ])
    assert validate_yahoo_scoreboard_pair_graph(
        rows, season=2026, week=1, expected_team_keys=("t1", "t2"), require_full_teams=True,
    ) == 2
    with pytest.raises(IncompleteSourceError, match="coverage"):
        validate_yahoo_scoreboard_pair_graph(
            rows.iloc[:1], season=2026, week=1,
            expected_team_keys=("t1", "t2"), require_full_teams=True,
        )
    with pytest.raises(IncompleteSourceError, match="reciprocal"):
        validate_yahoo_scoreboard_pair_graph(
            rows.assign(opponent_points=[128.66, 152.66]), season=2026, week=1,
            expected_team_keys=("t1", "t2"), require_full_teams=True,
        )


def test_yahoo_partial_final_pair_requires_both_sides_without_requiring_other_games():
    rows = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "t1", "opponent_team_key": "t2", "team_points": 12, "opponent_points": 9},
        {"year": 2026, "week": 1, "team_key": "t2", "opponent_team_key": "t1", "team_points": 9, "opponent_points": 12},
    ])
    assert validate_yahoo_scoreboard_pair_graph(
        rows, season=2026, week=1,
        expected_team_keys=("t1", "t2", "t3", "t4"), require_full_teams=False,
    ) == 2
    with pytest.raises(IncompleteSourceError, match="reciprocal"):
        validate_yahoo_scoreboard_pair_graph(
            rows.iloc[:1], season=2026, week=1,
            expected_team_keys=("t1", "t2", "t3", "t4"), require_full_teams=False,
        )


def test_yahoo_playoff_week_accepts_declared_pairs_not_bye_teams():
    expected = tuple(f"t{index}" for index in range(1, 11))
    rows = pd.DataFrame([
        {"year": 2025, "week": 15, "team_key": "t1", "opponent_team_key": "t2", "team_points": 101, "opponent_points": 95},
        {"year": 2025, "week": 15, "team_key": "t2", "opponent_team_key": "t1", "team_points": 95, "opponent_points": 101},
        {"year": 2025, "week": 15, "team_key": "t3", "opponent_team_key": "t4", "team_points": 98, "opponent_points": 96},
        {"year": 2025, "week": 15, "team_key": "t4", "opponent_team_key": "t3", "team_points": 96, "opponent_points": 98},
    ])
    assert validate_yahoo_week_matchup_scope(
        raw_schedule=rows, final_matchups=rows, season=2025, week=15,
        expected_team_keys=expected, playoff_start_week=15,
    ) is True
    with pytest.raises(IncompleteSourceError, match="coverage"):
        validate_yahoo_week_matchup_scope(
            raw_schedule=rows.assign(week=14), final_matchups=rows.assign(week=14),
            season=2025, week=14,
            expected_team_keys=expected, playoff_start_week=15,
        )


def test_yahoo_playoff_week_rejects_missing_pair_and_incomplete_final_pair():
    expected = tuple(f"t{index}" for index in range(1, 11))
    rows = pd.DataFrame([
        {"year": 2025, "week": 15, "team_key": "t1", "opponent_team_key": "t2", "team_points": 101, "opponent_points": 95},
        {"year": 2025, "week": 15, "team_key": "t2", "opponent_team_key": "t1", "team_points": 95, "opponent_points": 101},
    ])
    with pytest.raises(IncompleteSourceError, match="reciprocal"):
        validate_yahoo_week_matchup_scope(
            raw_schedule=rows.iloc[:1], final_matchups=rows.iloc[:1], season=2025, week=15,
            expected_team_keys=expected, playoff_start_week=15,
        )
    with pytest.raises(IncompleteSourceError, match="reciprocal"):
        validate_yahoo_week_matchup_scope(
            raw_schedule=rows, final_matchups=rows.iloc[:1], season=2025, week=15,
            expected_team_keys=expected, playoff_start_week=15,
        )
    assert validate_yahoo_week_matchup_scope(
        raw_schedule=rows, final_matchups=rows.iloc[:0], season=2025, week=15,
        expected_team_keys=expected, playoff_start_week=15,
    ) is False


def test_provider_team_inventory_requires_settings_count_and_exact_unique_ids():
    assert validate_provider_team_inventory(
        provider="espn", settings_team_count=2, team_ids=("1", "2")
    ) == ("1", "2")
    with pytest.raises(IncompleteSourceError, match="team count mismatch"):
        validate_provider_team_inventory(
            provider="espn", settings_team_count=3, team_ids=("1", "2")
        )
    with pytest.raises(IncompleteSourceError, match="identities are incomplete"):
        validate_provider_team_inventory(
            provider="yahoo", settings_team_count=2, team_ids=("1", "1")
        )


def _espn_final_graph_fixture():
    raw = [
        {"home": {"teamId": 1, "totalPoints": 10.0}, "away": {"teamId": 2, "totalPoints": 9.0}, "winner": "HOME", "playoffTierType": "NONE"},
        {"home": {"teamId": 3, "totalPoints": 8.0}, "away": {"teamId": 4, "totalPoints": 11.0}, "winner": "AWAY", "playoffTierType": "NONE"},
        {"home": {"teamId": 5}, "away": None, "winner": "UNDECIDED", "playoffTierType": "WINNERS_BRACKET"},
    ]
    frame = pd.DataFrame([
        {"year": 2026, "week": 15, "team_key": "1", "matchup_id": 0, "is_bye_week": False, "team_points": 10.0, "opponent_points": 9.0},
        {"year": 2026, "week": 15, "team_key": "2", "matchup_id": 0, "is_bye_week": False, "team_points": 9.0, "opponent_points": 10.0},
        {"year": 2026, "week": 15, "team_key": "3", "matchup_id": 1, "is_bye_week": False, "team_points": 8.0, "opponent_points": 11.0},
        {"year": 2026, "week": 15, "team_key": "4", "matchup_id": 1, "is_bye_week": False, "team_points": 11.0, "opponent_points": 8.0},
        {"year": 2026, "week": 15, "team_key": "5", "matchup_id": 2, "is_bye_week": True, "team_points": None, "opponent_points": None},
    ])
    return raw, frame


def test_espn_final_matchup_frame_matches_raw_pairs_and_declared_byes():
    raw, frame = _espn_final_graph_fixture()
    assert validate_espn_final_matchup_frame(
        season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
        raw_schedule=raw, matchups=frame,
    ) == 5


def test_espn_final_matchup_frame_rejects_missing_pair_member():
    raw, frame = _espn_final_graph_fixture()
    with pytest.raises(IncompleteSourceError, match="coverage mismatch"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=frame.loc[frame["team_key"] != "4"],
        )


def test_espn_final_matchup_frame_rejects_broken_pair_or_blank_score():
    raw, frame = _espn_final_graph_fixture()
    wrong_pair = frame.assign(matchup_id=[0, 1, 1, 1, 2])
    with pytest.raises(IncompleteSourceError, match="pair"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=wrong_pair,
        )
    blank_score = frame.copy()
    blank_score.loc[blank_score["team_key"] == "2", "team_points"] = None
    with pytest.raises(IncompleteSourceError, match="score"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=blank_score,
        )


def test_espn_final_matchup_frame_rejects_late_gray_score_mismatch():
    raw = [{
        "home": {"teamId": 10, "totalPoints": 152.66},
        "away": {"teamId": 3, "totalPoints": 129.66},
        "winner": "HOME", "playoffTierType": "NONE",
    }]
    frame = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "10", "matchup_id": 1,
         "is_bye_week": False, "team_points": 151.66, "opponent_points": 129.66},
        {"year": 2026, "week": 1, "team_key": "3", "matchup_id": 1,
         "is_bye_week": False, "team_points": 129.66, "opponent_points": 151.66},
    ])
    with pytest.raises(IncompleteSourceError, match="raw.*score"):
        validate_espn_final_matchup_frame(
            season=2026, week=1, expected_team_ids=("10", "3"),
            raw_schedule=raw, matchups=frame,
        )


def test_espn_final_matchup_frame_accepts_period_points_when_total_points_lag():
    raw = [{
        "home": {
            "teamId": 10,
            "totalPoints": 0,
            "pointsByScoringPeriod": {"1": 152.66},
        },
        "away": {
            "teamId": 3,
            "totalPoints": 0,
            "pointsByScoringPeriod": {"1": 129.66},
        },
        "winner": "HOME",
    }]
    frame = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "10", "matchup_id": 1,
         "is_bye_week": False, "team_points": 152.66, "opponent_points": 129.66},
        {"year": 2026, "week": 1, "team_key": "3", "matchup_id": 1,
         "is_bye_week": False, "team_points": 129.66, "opponent_points": 152.66},
    ])

    assert validate_espn_final_matchup_frame(
        season=2026,
        week=1,
        expected_team_ids=("10", "3"),
        raw_schedule=raw,
        matchups=frame,
    ) == 2


def test_espn_final_matchup_frame_rejects_missing_raw_score_witness():
    raw, frame = _espn_final_graph_fixture()
    del raw[0]["home"]["totalPoints"]
    with pytest.raises(IncompleteSourceError, match="raw.*score"):
        validate_espn_final_matchup_frame(
            season=2026, week=15, expected_team_ids=("1", "2", "3", "4", "5"),
            raw_schedule=raw, matchups=frame,
        )


def test_espn_final_matchup_frame_rejects_stale_opponent_score_after_correction():
    raw = [{
        "home": {"teamId": 10, "totalPoints": 152.66},
        "away": {"teamId": 3, "totalPoints": 129.66},
        "winner": "HOME", "playoffTierType": "NONE",
    }]
    frame = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "10", "matchup_id": 1,
         "is_bye_week": False, "team_points": 152.66, "opponent_points": 129.66},
        {"year": 2026, "week": 1, "team_key": "3", "matchup_id": 1,
         "is_bye_week": False, "team_points": 129.66, "opponent_points": 151.66},
    ])
    with pytest.raises(IncompleteSourceError, match="opponent.*score"):
        validate_espn_final_matchup_frame(
            season=2026, week=1, expected_team_ids=("10", "3"),
            raw_schedule=raw, matchups=frame,
        )


def test_espn_raw_roster_scope_rejects_missing_team_even_when_other_rows_exist():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
    ])
    with pytest.raises(IncompleteSourceError, match="missing.*2"):
        validate_active_roster_frame(
            provider="espn", season=2026, expected_team_ids=("1", "2"),
            requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
        )


def test_espn_raw_roster_scope_admits_complete_live_week_without_final_scores():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1", "fantasy_points": 1.0},
        {"year": 2026, "week": 1, "team_key": "2", "espn_player_id": "p2", "fantasy_points": None},
    ])
    assert validate_active_roster_frame(
        provider="espn", season=2026, expected_team_ids=("1", "2"),
        requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
    ) == 2


def test_raw_roster_scope_rejects_duplicate_provider_player_ownership():
    rosters = pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
        {"year": 2026, "week": 1, "team_key": "1", "espn_player_id": "p1"},
        {"year": 2026, "week": 1, "team_key": "2", "espn_player_id": "p2"},
    ])
    with pytest.raises(IncompleteSourceError, match="duplicate provider player"):
        validate_active_roster_frame(
            provider="espn", season=2026, expected_team_ids=("1", "2"),
            requested_weeks=(1,), player_id_column="espn_player_id", rosters=rosters,
        )


def snapshot():
    return {
        "provider": "sleeper",
        "league_id": "s26",
        "season": 2026,
        "resource_status": {
            "settings": "ok",
            "teams": "ok",
            "rosters": "ok",
            "matchups": "ok",
            "transactions": "ok_empty",
            "draft": "ok_empty",
        },
        "teams": [
            {"team_id": "1", "franchise_id": "f1"},
            {"team_id": "2", "franchise_id": "f2"},
        ],
        "rosters": [
            {"week": 1, "team_id": "1", "players": [{"player_id": "p1", "NFL_player_id": "n1"}]},
            {"week": 1, "team_id": "2", "players": [{"player_id": "p2", "NFL_player_id": "n2"}]},
        ],
        "matchups": [
            {"week": 1, "team_id": "1", "opponent_team_id": "2", "matchup_id": "m1"},
            {"week": 1, "team_id": "2", "opponent_team_id": "1", "matchup_id": "m1"},
        ],
        "transactions": [],
        "draft": [],
    }


def expectations(**changes):
    values = dict(
        provider="sleeper",
        league_id="s26",
        season=2026,
        weeks=(1,),
        expected_team_ids=("1", "2"),
        allow_byes=False,
        uses_median=False,
    )
    values.update(changes)
    return ProviderSnapshotExpectations(**values)


def test_complete_snapshot_passes_with_explicit_valid_empty_resources():
    receipt = validate_provider_snapshot(snapshot(), expectations())
    assert receipt.healthy is True
    assert receipt.expected_teams == receipt.observed_teams == 2
    assert receipt.valid_empty_resources == ("draft", "transactions")


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(league_id="wrong"), "league identity"),
        (lambda value: value.update(season=2025), "active season"),
        (lambda value: value["teams"].pop(), "teams"),
        (lambda value: value["rosters"].pop(), "roster"),
        (lambda value: value["matchups"].pop(), "reciprocal"),
        (lambda value: value["rosters"][0]["players"][0].update(NFL_player_id=None), "NFL mapping"),
        (lambda value: value["teams"].append(deepcopy(value["teams"][0])), "duplicate team"),
        (lambda value: value["rosters"].append(deepcopy(value["rosters"][0])), "duplicate roster"),
        (lambda value: value["matchups"].append(deepcopy(value["matchups"][0])), "duplicate matchup"),
    ],
)
def test_incomplete_or_duplicate_provider_snapshots_are_rejected(mutate, message):
    value = snapshot()
    mutate(value)
    with pytest.raises(IncompleteSourceError, match=message):
        validate_provider_snapshot(value, expectations())

def test_bye_is_valid_only_when_declared_and_still_requires_a_roster():
    value = snapshot()
    value["matchups"] = [
        {"week": 1, "team_id": "1", "opponent_team_id": None, "matchup_id": None, "is_bye": True},
        {"week": 1, "team_id": "2", "opponent_team_id": None, "matchup_id": None, "is_bye": True},
    ]
    with pytest.raises(IncompleteSourceError, match="bye"):
        validate_provider_snapshot(value, expectations())
    assert validate_provider_snapshot(value, expectations(allow_byes=True)).healthy is True


def test_median_scoring_requires_one_median_row_per_team_week():
    value = snapshot()
    with pytest.raises(IncompleteSourceError, match="median"):
        validate_provider_snapshot(value, expectations(uses_median=True))
    value["median_matchups"] = [
        {"week": 1, "team_id": "1"},
        {"week": 1, "team_id": "2"},
    ]
    assert validate_provider_snapshot(value, expectations(uses_median=True)).healthy is True


def test_empty_resource_without_explicit_ok_empty_status_is_unknown_not_healthy():
    value = snapshot()
    value["resource_status"]["transactions"] = "ok"
    with pytest.raises(IncompleteSourceError, match="transactions.*unknown"):
        validate_provider_snapshot(value, expectations())


def test_unknown_resource_completeness_is_rejected_even_when_other_tables_are_nonempty():
    value = snapshot()
    del value["resource_status"]["rosters"]
    with pytest.raises(IncompleteSourceError, match="rosters.*unknown"):
        validate_provider_snapshot(value, expectations())


def _tabular_scope():
    return {
        "rosters": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1", "sleeper_player_id": "p1"},
            {"year": 2026, "week": 1, "team_key": "2", "sleeper_player_id": "p2"},
        ]),
        "matchups": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1", "matchup_id": "m1", "team_points": 100.0, "opponent_points": 90.0},
            {"year": 2026, "week": 1, "team_key": "2", "matchup_id": "m1", "team_points": 90.0, "opponent_points": 100.0},
        ]),
        "schedule": pd.DataFrame([
            {"year": 2026, "week": 1, "team_key": "1", "team_points": 100.0, "opponent_points": 90.0},
            {"year": 2026, "week": 1, "team_key": "2", "team_points": 90.0, "opponent_points": 100.0},
        ]),
        "draft": pd.DataFrame([
            {"year": 2026, "draft_id": "d1", "round": 1, "pick": 1},
            {"year": 2026, "draft_id": "d1", "round": 1, "pick": 2},
        ]),
    }


def _validate_tabular(**changes):
    values = _tabular_scope() | changes
    return validate_tabular_active_scope(
        provider="sleeper", league_id="s26", season=2026,
        expected_team_ids=("1", "2"), requested_weeks=(1,),
        finalized_weeks=(1,), player_id_column="sleeper_player_id",
        **values,
    )


def test_actual_weekly_tabular_payload_requires_every_team_week_and_draft_key():
    assert _validate_tabular()["observed_team_weeks"] == 2
    with pytest.raises(IncompleteSourceError, match="roster coverage"):
        _validate_tabular(rosters=_tabular_scope()["rosters"].iloc[:1])
    with pytest.raises(IncompleteSourceError, match="matchup coverage"):
        _validate_tabular(matchups=_tabular_scope()["matchups"].iloc[:1])
    duplicate = pd.concat([_tabular_scope()["draft"], _tabular_scope()["draft"].iloc[:1]])
    with pytest.raises(IncompleteSourceError, match="draft.*duplicate"):
        _validate_tabular(draft=duplicate)


def test_actual_weekly_tabular_payload_rejects_duplicate_matchup_team_week():
    matchups = _tabular_scope()["matchups"]
    duplicate = pd.concat([matchups, matchups.iloc[:1]], ignore_index=True)
    with pytest.raises(IncompleteSourceError, match="matchup.*duplicate team-week"):
        _validate_tabular(matchups=duplicate)


def test_actual_weekly_tabular_payload_rejects_duplicate_schedule_team_week():
    schedule = _tabular_scope()["schedule"]
    duplicate = pd.concat([schedule, schedule.iloc[:1]], ignore_index=True)
    with pytest.raises(IncompleteSourceError, match="schedule.*duplicate team-week"):
        _validate_tabular(schedule=duplicate)


def test_actual_weekly_tabular_payload_rejects_nonreciprocal_played_scores():
    matchups = _tabular_scope()["matchups"].copy()
    matchups.loc[1, "opponent_points"] = 99.0
    with pytest.raises(IncompleteSourceError, match="matchup.*reciprocal score"):
        _validate_tabular(matchups=matchups)


def test_actual_weekly_tabular_payload_rejects_missing_played_score():
    matchups = _tabular_scope()["matchups"].copy()
    matchups.loc[1, "team_points"] = float("nan")
    with pytest.raises(IncompleteSourceError, match="matchup.*final score"):
        _validate_tabular(matchups=matchups)


def test_actual_weekly_tabular_payload_rejects_stale_schedule_score():
    schedule = _tabular_scope()["schedule"].copy()
    schedule.loc[0, "team_points"] = 99.0
    with pytest.raises(IncompleteSourceError, match="schedule.*score.*matchup"):
        _validate_tabular(schedule=schedule)


def test_actual_weekly_tabular_payload_rejects_missing_schedule_final_score():
    schedule = _tabular_scope()["schedule"].copy()
    schedule.loc[1, "opponent_points"] = float("nan")
    with pytest.raises(IncompleteSourceError, match="schedule.*final score"):
        _validate_tabular(schedule=schedule)


def test_tabular_scope_accepts_explicit_null_id_bye_rows_when_present():
    values = _tabular_scope()
    values["rosters"] = pd.concat([values["rosters"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3", "sleeper_player_id": "p3"},
        {"year": 2026, "week": 1, "team_key": "4", "sleeper_player_id": "p4"},
    ])], ignore_index=True)
    values["matchups"] = pd.concat([values["matchups"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3", "matchup_id": None, "team_points": 110.0, "opponent_points": 0.0, "opponent": "BYE"},
        {"year": 2026, "week": 1, "team_key": "4", "matchup_id": None, "team_points": 105.0, "opponent_points": 0.0, "opponent": "BYE"},
    ])], ignore_index=True)
    values["schedule"] = pd.concat([values["schedule"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3", "team_points": 110.0, "opponent_points": 0.0},
        {"year": 2026, "week": 1, "team_key": "4", "team_points": 105.0, "opponent_points": 0.0},
    ])], ignore_index=True)
    for name in ("rosters", "matchups", "schedule"):
        values[name]["year"] = 2025
        values[name]["week"] = 15
    values["draft"]["year"] = 2025
    receipt = validate_tabular_active_scope(
        provider="sleeper", league_id="s25", season=2025,
        expected_team_ids=("1", "2", "3", "4"), requested_weeks=(15,),
        finalized_weeks=(15,), player_id_column="sleeper_player_id",
        **values,
    )
    assert receipt["matchup_team_weeks"] == 4


def test_actual_weekly_tabular_payload_rejects_three_teams_in_one_played_pair():
    values = _tabular_scope()
    values["rosters"] = pd.concat([values["rosters"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3", "sleeper_player_id": "p3"},
    ])], ignore_index=True)
    values["matchups"] = pd.concat([values["matchups"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3", "matchup_id": "m1", "team_points": 80.0, "opponent_points": 100.0},
    ])], ignore_index=True)
    values["schedule"] = pd.concat([values["schedule"], pd.DataFrame([
        {"year": 2026, "week": 1, "team_key": "3"},
    ])], ignore_index=True)
    with pytest.raises(IncompleteSourceError, match="matchup.*pair.*more than two"):
        validate_tabular_active_scope(
            provider="sleeper", league_id="s26", season=2026,
            expected_team_ids=("1", "2", "3"), requested_weeks=(1,),
            finalized_weeks=(1,), player_id_column="sleeper_player_id",
            **values,
        )


def test_actual_tabular_payload_allows_a_legitimately_live_unfinalized_week():
    values = _tabular_scope()
    assert validate_tabular_active_scope(
        provider="sleeper", league_id="s26", season=2026,
        expected_team_ids=("1", "2"), requested_weeks=(1,),
        finalized_weeks=(), player_id_column="sleeper_player_id",
        rosters=values["rosters"], matchups=pd.DataFrame(),
        schedule=pd.DataFrame(), draft=values["draft"],
    )["observed_final_matchup_weeks"] == 0


def test_sleeper_schedule_fetch_only_names_map_to_exact_matchup_team_keys():
    values = _tabular_scope()
    matchups = values["matchups"].assign(
        manager_week=["A_2026_1", "B_2026_1"],
        team_name=["Team A", "Team B"],
    )
    schedule = values["schedule"].drop(columns=["team_key"]).assign(
        manager_week=["A_2026_1", "B_2026_1"],
        team_name=["Team A", "Team B"],
    )
    assert _validate_tabular(matchups=matchups, schedule=schedule)["schedule_team_weeks"] == 2
    with pytest.raises(IncompleteSourceError, match="schedule.*team identity"):
        _validate_tabular(
            matchups=matchups,
            schedule=schedule.assign(team_name=["Team A", "Wrong Team"]),
        )
