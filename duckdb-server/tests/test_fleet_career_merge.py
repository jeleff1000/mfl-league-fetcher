"""Exercise v2 full-chain aggregation through the real HTTP publication path."""

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
        if getattr(request, 'param', None) == 'missing_source':
            conn.execute('DROP TABLE public.player_fantasy')
    if getattr(request, 'param', None) == 'missing_ops':
        return tmp_path
    with duckdb.connect(str(tmp_path / '___ops.duckdb')) as conn:
        conn.execute('CREATE SCHEMA nfl_historical')
        conn.execute("""
            CREATE TABLE nfl_historical.nfl_player_stats_all (
                player_week VARCHAR, NFL_player_id VARCHAR, player VARCHAR,
                nfl_team VARCHAR, year INTEGER, week INTEGER
            )
        """)
        conn.execute('CREATE TABLE nfl_historical.player_bio (NFL_player_id VARCHAR, player VARCHAR)')
    return tmp_path


def _bundle(tmp_path):
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


def test_http_weekly_merge_commits_full_careers_and_replays_without_reexecution(client, tmp_path):  # noqa: F811
    bundle = _bundle(tmp_path)
    response = _publish(client, bundle)
    assert response.status_code == 200, response.text
    assert response.json()['career_rollups']['test_league']['matchup_career'] == 1
    assert _query(client, "SELECT games, seasons FROM public.matchup_career WHERE db_name='test_league'") == [{'games': 16, 'seasons': 2}]
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
