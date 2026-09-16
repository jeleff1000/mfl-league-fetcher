"""Exercise full-chain aggregation through the real HTTP publication path."""

import duckdb
import pytest

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, create_aggregate_table_sql
from multi_league.core.delta_publish import canonical_table_registry
from multi_league.core.fleet_publish import build_fleet_partition_bundle
from tests.test_integration import client  # noqa: F401


@pytest.fixture
def data_dir(tmp_path, request):
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for table, spec in canonical_table_registry().items():
            if table in AGGREGATE_TABLE_SPECS:
                conn.execute(create_aggregate_table_sql('___leagues', table))
            else:
                columns = ', '.join(f'"{name}" {dtype}' for name, dtype in spec['columns'].items())
                conn.execute(f'CREATE TABLE public."{table}" ({columns})')
        conn.execute("""
            INSERT INTO public.matchup_season (db_name, franchise_id, manager, year, games, wins, losses)
            VALUES ('test_league', 'f1', 'Shared Alias', 2025, 14, 8, 6),
                   ('test_league', 'f1', 'Shared Alias', 2026, 1, 1, 0)
        """)
        conn.execute("""
            INSERT INTO public.matchup
                (db_name,year,week,manager,franchise_id,opponent,opponent_franchise_id,
                 team_name,team_points,
                 opponent_points,win,loss,tie,is_playoffs,is_consolation,is_bye_week)
            VALUES ('test_league',2025,1,'Shared Alias','f1','Opponent','f2','Team',140,100,1,0,0,0,0,0),
                   ('test_league',2026,1,'Shared Alias','f1','Opponent','f2','Team',110,120,0,1,0,0,0,0)
        """)
        if getattr(request, 'param', None) == 'missing_source':
            conn.execute('DROP TABLE public.player_fantasy')
        if getattr(request, 'param', None) == 'missing_homepage_source':
            conn.execute('DROP TABLE public.league_context')
        if getattr(request, 'param', None) == 'lost_homepage_value':
            conn.execute("""
                INSERT INTO public.homepage_league_summary
                    (db_name, data_year, season_best_pickup_player)
                VALUES ('test_league', 2026, 'Missing Pickup')
            """)
    if getattr(request, 'param', None) == 'missing_ops':
        return tmp_path
    with duckdb.connect(str(tmp_path / '___ops.duckdb')) as conn:
        conn.execute('CREATE SCHEMA nfl_historical')
        conn.execute("""
            CREATE TABLE nfl_historical.nfl_player_stats_all (
                player_week VARCHAR, NFL_player_id VARCHAR, player VARCHAR,
                nfl_team VARCHAR, year INTEGER, week INTEGER, headshot_url VARCHAR
            )
        """)
        conn.execute('CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR, player VARCHAR, headshot_url VARCHAR, yahoo_player_id VARCHAR, sleeper_player_id VARCHAR, espn_id VARCHAR)')
    return tmp_path


def _bundle(tmp_path, *, homepage=False):
    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("""
            CREATE TABLE public.matchup_season AS
            SELECT 'test_league' AS db_name, 'Shared Alias' AS manager, 'f1' AS franchise_id,
                   2026 AS year, 2 AS games, 2 AS wins, 0 AS losses
        """)
        return build_fleet_partition_bundle(
            conn, active_year=2026, league_generations={'test_league': 0},
            tables=['matchup_season'], output_dir=tmp_path / 'bundle',
            import_run_id='9001', publish_sequence=1, rebuild_career_rollups=True,
            rebuild_homepage_rollups=homepage,
        )


def _publish(http_client, bundle):
    with open(bundle.path, 'rb') as stream:
        return http_client.post('/merge-fleet-partition',
            headers={'Authorization': 'Bearer test-admin', 'X-Bundle-Id': bundle.bundle_id,
                     'X-Bundle-Hash': bundle.bundle_hash},
            files={'file': ('bundle.tar.gz', stream, 'application/gzip')})


def _query(http_client, sql):
    response = http_client.post('/query', headers={'Authorization': 'Bearer test-read'},
                          json={'sql': sql, 'database': '___leagues'})
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize('homepage', [False, True])
def test_http_weekly_merge_commits_full_careers_and_replays_without_reexecution(client, tmp_path, homepage):  # noqa: F811
    bundle = _bundle(tmp_path, homepage=homepage)
    response = _publish(client, bundle)
    assert response.status_code == 200, response.text
    assert response.json()['career_rollups']['test_league']['matchup_career'] == 1
    expected_games = 2 if homepage else 16
    assert _query(client, "SELECT games, seasons FROM public.matchup_career WHERE db_name='test_league'") == [{'games': expected_games, 'seasons': 2}]
    if homepage:
        assert response.json()['season_rollups']['test_league']['matchup_season'] == 2
        assert response.json()['homepage_rollups']['test_league']['homepage_manager_rankings'] == 1
        assert _query(client, "SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'") == [{'highest_score_points': 140.0}]
        assert _query(client, "SELECT manager,seasons,wins,losses FROM public.homepage_manager_rankings WHERE db_name='test_league'") == [{'manager':'Shared Alias','seasons':2,'wins':1,'losses':1}]
    replay = _publish(client, bundle)
    assert replay.status_code == 200, replay.text
    assert replay.json()['idempotent_replay'] is True
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation': 1}]


@pytest.mark.parametrize('data_dir', ['missing_source', 'missing_ops'], indirect=True)
def test_http_weekly_merge_does_not_commit_partial_careers(client, tmp_path):  # noqa: F811
    response = _publish(client, _bundle(tmp_path))
    assert response.status_code == 500, response.text
    assert _query(client, "SELECT status FROM merge_admin.league_delta_merge_state WHERE db_name='___fleet'") == [{'status': 'FAILED_MERGE'}]
    assert _query(client, "SELECT year, games FROM public.matchup_season WHERE db_name='test_league' ORDER BY year") == [{'year': 2025, 'games': 14}, {'year': 2026, 'games': 1}]


def test_http_attachment_failure_records_recoverable_terminal_state(client, tmp_path, monkeypatch):  # noqa: F811
    import main

    def unavailable(_conn):
        raise RuntimeError('NFL reference attachment unavailable')

    monkeypatch.setattr(main, '_acquire_ops_attachment', unavailable)
    response = _publish(client, _bundle(tmp_path))
    assert response.status_code == 500, response.text
    assert _query(client, "SELECT status FROM merge_admin.league_delta_merge_state WHERE db_name='___fleet'") == [{'status': 'FAILED_MERGE'}]


@pytest.mark.parametrize('data_dir', ['missing_homepage_source', 'lost_homepage_value'], indirect=True)
def test_http_homepage_error_rolls_back_partition_and_career(client, tmp_path):  # noqa: F811
    response = _publish(client, _bundle(tmp_path, homepage=True))
    assert response.status_code == 422, response.text
    assert _query(client, "SELECT status FROM merge_admin.league_delta_merge_state WHERE db_name='___fleet'") == [{'status':'FAILED_MERGE'}]
    assert _query(client, "SELECT year,games FROM public.matchup_season WHERE db_name='test_league' ORDER BY year") == [{'year':2025,'games':14},{'year':2026,'games':1}]
    assert _query(client, "SELECT COUNT(*) n FROM public.matchup_career WHERE db_name='test_league'") == [{'n':0}]


@pytest.mark.parametrize('data_dir', ['lost_homepage_value'], indirect=True)
def test_target_does_not_retry_deterministic_homepage_rejection(client, tmp_path, monkeypatch):  # noqa: F811
    from multi_league.core.targets.fly_target import FlyTarget

    monkeypatch.setenv('DATABASE_SERVER_URL', 'https://fly.test')
    monkeypatch.setenv('DATABASE_ADMIN_TOKEN', 'test-admin')
    requests_seen = []

    def local_transport(url, *, headers, files, timeout):
        assert url == 'https://fly.test/merge-fleet-partition'
        requests_seen.append(url)
        return client.post('/merge-fleet-partition', headers=headers, files=files)

    monkeypatch.setattr('multi_league.core.targets.fly_target.requests.post', local_transport)
    bundle = _bundle(tmp_path, homepage=True)
    with pytest.raises(RuntimeError, match='Fleet partition merge failed \\(422\\)'):
        FlyTarget().merge_fleet_partition(bundle.path, bundle_id=bundle.bundle_id, bundle_hash=bundle.bundle_hash)
    assert len(requests_seen) == 1
    assert _query(client, "SELECT season_best_pickup_player FROM public.homepage_league_summary WHERE db_name='test_league'") == [{'season_best_pickup_player':'Missing Pickup'}]
