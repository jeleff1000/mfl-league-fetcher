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
        if getattr(request, 'param', None) in {'missing_historical_franchise', 'recovery_source', 'recovery_empty'}:
            from fleet_merge import ensure_generation_tables

            ensure_generation_tables(conn)
            conn.execute("INSERT INTO merge_admin.league_publish_generations (db_name,generation) VALUES ('test_league',7)")
        if getattr(request, 'param', None) == 'recovery_source':
            conn.execute("""
                INSERT INTO public.player_fantasy
                    (db_name,year,week,NFL_player_id,player_week,player,manager,franchise_id,
                     fantasy_points,is_started,win,loss)
                VALUES ('test_league',2025,1,'p1','p1_2025_1','Player','Shared Alias','f1',20,1,1,0),
                       ('test_league',2026,1,'p1','p1_2026_1','Player','Shared Alias','f1',30,1,0,1)
            """)
        if getattr(request, 'param', None) == 'missing_historical_franchise':
            conn.execute("DELETE FROM public.matchup_season WHERE year=2025")
            conn.execute("""
                INSERT INTO public.matchup_season (db_name,franchise_id,manager,year,games)
                VALUES ('test_league','unrelated','Unrelated',2025,14)
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
        if getattr(request, 'param', None) == 'legacy_trade_mirror':
            conn.execute("""
                INSERT INTO public.transactions
                    (db_name, transaction_id, year, week, transaction_type, trade_direction,
                     manager, franchise_id, source_franchise_id, player, NFL_player_id,
                     sleeper_player_id, trade_asset_lamar)
                VALUES
                    ('test_league', 'pick-swap', 2025, 1, 'trade_pick', 'received',
                     'Shared Alias', 'f1', 'f2', 'Stale Player', 'STALE',
                     'pick_2025_1_1', 17),
                    ('test_league', 'pick-swap', 2025, 1, 'trade_pick', 'sent',
                     'Opponent', 'f2', 'f1', 'Correct Player', 'CORRECT',
                     'pick_2025_1_1', NULL)
            """)
        if getattr(request, 'param', None) == 'quick_new_league':
            for table in canonical_table_registry():
                conn.execute(f'DELETE FROM public."{table}"')
        if getattr(request, 'param', None) in {'quick_saved_config', 'quick_yahoo_history'}:
            conn.execute("""
                INSERT INTO public.league_context
                    (db_name, platform, league_ids_json, manager_name_overrides_json)
                VALUES ('test_league', 'sleeper', '{"2025":"old-id","2026":"new-id"}',
                        '{"Provider Name":"Shared Alias"}')
            """)
            conn.execute("INSERT INTO public.keeper_config (db_name, year) VALUES ('test_league', 0)")
            conn.execute("INSERT INTO public.league_rules (db_name, faab_budget) VALUES ('test_league', 237)")
            conn.execute("INSERT INTO public.manager_overrides (id, db_name, from_name, to_name) VALUES (1, 'test_league', 'Provider Name', 'Shared Alias')")
        if getattr(request, 'param', None) == 'quick_yahoo_history':
            conn.execute("UPDATE public.matchup SET year=2024 WHERE year=2025")
            conn.execute("UPDATE public.matchup_season SET year=2024 WHERE year=2025")
            conn.execute("INSERT INTO public.league_settings (db_name,year) VALUES ('test_league',2025)")
            conn.execute("INSERT INTO public.all_play (db_name,year,week,franchise_id,opponent_franchise_id,points) VALUES ('test_league',2026,1,'f1','f2',110)")
            conn.execute("""UPDATE public.league_context SET platform='yahoo',
                league_ids_json='{"2024":"old-id","2025":"mid-id","2026":"new-id"}',
                franchise_merges_json='[{"from_franchise_id":"provider-f1","into_franchise_id":"f1"}]'
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


def _bundle(tmp_path, *, homepage=False, generation=0):
    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("""
            CREATE TABLE public.matchup_season AS
            SELECT 'test_league' AS db_name, 'Shared Alias' AS manager, 'f1' AS franchise_id,
                   2026 AS year, 2 AS games, 2 AS wins, 0 AS losses
        """)
        return build_fleet_partition_bundle(
            conn, active_year=2026, league_generations={'test_league': generation},
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


@pytest.mark.parametrize('data_dir,worker_identity', [
    ('quick_yahoo_history', 'empty'),
    ('quick_yahoo_history', 'empty_merge'),
    ('quick_yahoo_history', 'missing'),
    ('quick_yahoo_history', 'matching'),
    ('quick_yahoo_history', 'stale'),
    ('quick_yahoo_history', 'stale_merge'),
    ('quick_new_league', 'new'),
], indirect=['data_dir'])
def test_quick_two_years_initializes_only_missing_config_and_replays(client, tmp_path, request, worker_identity, monkeypatch):
    """Yahoo quick's 2025+2026 payload must retain 2024 and saved identities."""
    from multi_league.core.delta_publish import build_delta_bundle

    existing = request.node.callspec.params['data_dir'] == 'quick_yahoo_history'
    historical = _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2024")
    settings = _query(client, "SELECT * FROM public.league_settings WHERE db_name='test_league' AND year=2025")
    facts_before = _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' ORDER BY year")
    derived_before = {table: _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league' ORDER BY year")
                      for table in ('all_play', 'matchup_season')}
    rejects_identity = worker_identity in {'empty', 'empty_merge', 'missing', 'stale', 'stale_merge'}
    configs = ('league_context', 'keeper_config', 'league_rules', 'manager_overrides')
    saved = {table: _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'")
             for table in configs}
    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("""
            CREATE TABLE public.matchup AS
            SELECT 'test_league' AS db_name, year, 1 AS week,
                   'provider_' || year AS manager_week, 'Provider Name' AS manager,
                   'provider-f1' AS franchise_id, 'Opponent' AS opponent,
                   'f2' AS opponent_franchise_id, 'Team' AS team_name,
                   110.0 AS team_points, 120.0 AS opponent_points,
                   0 AS win, 1 AS loss, 0 AS tie, 0 AS is_playoffs,
                   0 AS is_consolation, 0 AS is_bye_week
            FROM (VALUES (2025), (2026)) y(year)
        """)
        conn.execute("""
            CREATE TABLE public.league_context AS SELECT 'test_league' AS db_name,
                'yahoo' AS platform, 'new-id' AS league_id,
                '{"2025":"mid-id","2026":"new-id"}' AS league_ids_json,
                '{"Provider Name":"New Alias"}' AS manager_name_overrides_json,
                '[]' AS franchise_merges_json
        """)
        conn.execute("CREATE TABLE public.keeper_config AS SELECT 'test_league' AS db_name, 0 AS year")
        conn.execute("CREATE TABLE public.league_settings AS SELECT 'test_league' AS db_name, 2026 AS year")
        conn.execute("CREATE TABLE public.league_rules AS SELECT 'test_league' AS db_name, 100 AS faab_budget")
        conn.execute("CREATE TABLE public.manager_overrides AS SELECT 1 AS id, 'test_league' AS db_name, 'Provider Name' AS from_name, 'New Alias' AS to_name")
        if worker_identity == 'empty':
            conn.execute("UPDATE public.league_context SET manager_name_overrides_json='{}'")
        elif worker_identity == 'empty_merge':
            conn.execute("""UPDATE public.league_context
                SET manager_name_overrides_json='{"Provider Name":"Shared Alias"}'
            """)
        elif worker_identity == 'matching':
            conn.execute("""UPDATE public.league_context
                SET manager_name_overrides_json='{"Provider Name":"Shared Alias"}',
                    franchise_merges_json='[{"from_franchise_id":"provider-f1","into_franchise_id":"f1"}]'
            """)
        elif worker_identity == 'stale':
            # The worker enrichment has already applied its obsolete alias.
            # Reapplying Provider Name -> Shared Alias alone cannot undo this.
            conn.execute("UPDATE public.matchup SET manager='New Alias'")
        elif worker_identity == 'stale_merge':
            conn.execute("""UPDATE public.league_context
                SET manager_name_overrides_json='{"Provider Name":"Shared Alias"}',
                    franchise_merges_json='[{"from_franchise_id":"provider-f1","into_franchise_id":"wrong-f1"}]'
            """)
            conn.execute("UPDATE public.matchup SET franchise_id='wrong-f1'")
        elif worker_identity == 'missing':
            conn.execute('DROP TABLE public.league_context')
        if rejects_identity:
            conn.execute("""CREATE TABLE public.all_play AS
                SELECT 'test_league' AS db_name, 2026 AS year, 1 AS week,
                       'provider-f1' AS franchise_id, 'f2' AS opponent_franchise_id,
                       999.0 AS points""")
        bundle = build_delta_bundle(conn, db_name='test_league', import_mode='quick',
                                    base_generation=0, output_dir=tmp_path / 'quick_bundle')
        conn.execute('UPDATE public.matchup SET team_points=999')
        stale = build_delta_bundle(conn, db_name='test_league', import_mode='quick',
                                   base_generation=0, output_dir=tmp_path / 'stale_quick_bundle')
    mutation_steps = []
    if rejects_identity:
        import main
        execute = main._interrupting_execute

        def record_mutations(conn, sql, *args, **kwargs):
            if kwargs.get('step', '').startswith(('fleet delete ', 'fleet insert ', 'fleet create ', 'fleet add column ')):
                mutation_steps.append(kwargs['step'])
            return execute(conn, sql, *args, **kwargs)

        monkeypatch.setattr(main, '_interrupting_execute', record_mutations)
    response = _publish(client, bundle)
    if rejects_identity:
        assert response.status_code in {400, 422}, response.text
        assert 'identity configuration' in response.text
        if worker_identity in {'empty_merge', 'stale_merge'}:
            assert 'franchise_merges_json' in response.text
        assert mutation_steps == []
        assert _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' ORDER BY year") == facts_before
        for table in configs:
            assert _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'") == saved[table]
        for table, rows in derived_before.items():
            assert _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league' ORDER BY year") == rows
        return
    assert response.status_code == 200, response.text
    assert response.json()['season_rollup_years']['test_league'] == [2025, 2026]
    assert _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2024") == historical
    assert _query(client, "SELECT * FROM public.league_settings WHERE db_name='test_league' AND year=2025") == settings
    if existing:
        for table in configs:
            assert _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'") == saved[table]
    else:
        assert _query(client, "SELECT platform,league_id,league_ids_json FROM public.league_context WHERE db_name='test_league'") == [
            {'platform': 'yahoo', 'league_id': 'new-id', 'league_ids_json': '{"2025":"mid-id","2026":"new-id"}'}]
        for table in configs:
            assert len(_query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'")) == 1
    alias, fid = ('Shared Alias', 'f1') if existing else ('New Alias', 'provider-f1')
    assert _query(client, "SELECT DISTINCT manager,franchise_id FROM public.matchup WHERE db_name='test_league' AND year IN (2025,2026)") == [
        {'manager': alias, 'franchise_id': fid}]
    assert _query(client, "SELECT games,seasons FROM public.matchup_career WHERE db_name='test_league'") == [
        {'games': 16 if existing else 2, 'seasons': 3 if existing else 2}]
    assert _query(client, "SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'") == [
        {'highest_score_points': 140.0 if existing else 110.0}]
    after = {table: _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'")
             for table in (*configs, 'matchup', 'matchup_career', 'homepage_league_summary')}
    replay = _publish(client, bundle)
    assert replay.status_code == 200, replay.text
    assert replay.json()['idempotent_replay'] is True
    rejected = _publish(client, stale)
    assert rejected.status_code == 409, rejected.text
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation': 1}]
    for table, rows in after.items():
        assert _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'") == rows


@pytest.mark.parametrize('data_dir', ['quick_saved_config'], indirect=True)
def test_quick_upload_preserves_history_and_full_chain_rollups(client, tmp_path, monkeypatch):
    """A one-year worker must not replace the league's complete persisted chain."""
    from multi_league.core.local_db import LocalLeagueDB
    import multi_league.core.targets.fly_target as fly_target

    monkeypatch.setenv('DATABASE_SERVER_URL', 'https://fly.test')
    monkeypatch.setenv('DATABASE_ADMIN_TOKEN', 'test-admin')
    monkeypatch.setenv('FLY_PUBLISH_FORMAT', 'delta')
    monkeypatch.setenv('LEAGUE_IMPORT_BASE_GENERATION', '0')

    def local_transport(url, *, headers, files, timeout):
        assert url.startswith('https://fly.test/')
        return client.post(url.removeprefix('https://fly.test'), headers=headers, files=files)

    monkeypatch.setattr(fly_target.requests, 'post', local_transport)
    before = _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025")
    config = _query(client, "SELECT * FROM public.league_context WHERE db_name='test_league'")
    saved = {
        table: _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'")
        for table in ('keeper_config', 'league_rules', 'manager_overrides')
    }
    local = LocalLeagueDB(tmp_path / 'quick_worker', 'test_league')
    try:
        conn = local.connect()
        conn.execute("""
            CREATE TABLE public.matchup AS
            SELECT 'test_league' AS db_name, 2026 AS year, 1 AS week,
                   'f1_2026_1' AS manager_week, 'Shared Alias' AS manager,
                   'f1' AS franchise_id, 'Opponent' AS opponent,
                   'f2' AS opponent_franchise_id, 'Team' AS team_name,
                   110.0 AS team_points, 120.0 AS opponent_points,
                   0 AS win, 1 AS loss, 0 AS tie, 0 AS is_playoffs,
                   0 AS is_consolation, 0 AS is_bye_week
        """)
        conn.execute("""
            CREATE TABLE public.league_context AS
            SELECT 'test_league' AS db_name, 'sleeper' AS platform,
                   '{"2026":"new-id"}' AS league_ids_json,
                   '{"Provider Name":"Shared Alias"}' AS manager_name_overrides_json
        """)
        conn.execute("CREATE TABLE public.keeper_config AS SELECT 'test_league' AS db_name, 0 AS year")
        conn.execute("CREATE TABLE public.league_rules AS SELECT 'test_league' AS db_name, 100 AS faab_budget")
        conn.execute("CREATE TABLE public.manager_overrides AS SELECT 1 AS id, 'test_league' AS db_name, 'Provider Name' AS from_name, 'Default' AS to_name")
        local.upload_to_fly('test_league', import_mode='quick', platform='sleeper',
                            finalize_inventory=False, finalize_merge_source=False)
    finally:
        local.close()
    assert _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025") == before
    assert _query(client, "SELECT * FROM public.league_context WHERE db_name='test_league'") == config
    for table, before_rows in saved.items():
        assert _query(client, f"SELECT * FROM public.{table} WHERE db_name='test_league'") == before_rows
    assert _query(client, "SELECT games,seasons FROM public.matchup_career WHERE db_name='test_league'") == [
        {'games': 15, 'seasons': 2}]
    assert _query(client, "SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'") == [
        {'highest_score_points': 140.0}]


def test_quick_rejects_declared_season_without_payload(tmp_path):
    import fleet_merge

    manifest = _bundle(tmp_path, homepage=True).manifest
    manifest.update(mode='quick', quick_years=[2025, 2026])
    manifest['tables'][0]['scope'] = {'years': [2026]}
    with pytest.raises(fleet_merge.FleetValidationError, match='years'):
        fleet_merge.validate_fleet_manifest_shape(
            manifest, allowed_tables=set(canonical_table_registry()), identity_keys={})


@pytest.mark.parametrize('data_dir', ['quick_new_league'], indirect=True)
def test_quick_missing_context_rolls_back_facts_and_generation(client, tmp_path):
    from multi_league.core.delta_publish import build_delta_bundle

    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("""CREATE TABLE public.matchup AS SELECT 'test_league' AS db_name,
            2026 AS year, 1 AS week, 'f1_2026_1' AS manager_week, 'Name' AS manager""")
        bundle = build_delta_bundle(conn, db_name='test_league', import_mode='quick',
                                    base_generation=0, output_dir=tmp_path / 'missing_context')
    response = _publish(client, bundle)
    assert response.status_code in {400, 422}, response.text
    assert 'league_context' in response.text
    assert _query(client, "SELECT COUNT(*) n FROM public.matchup WHERE db_name='test_league'") == [{'n': 0}]
    assert _query(client, "SELECT table_name FROM information_schema.tables WHERE table_schema='merge_admin' AND table_name='league_publish_generations'") == []


@pytest.mark.parametrize('homepage', [False, True])
def test_http_weekly_merge_commits_full_careers_and_replays_without_reexecution(client, tmp_path, homepage):  # noqa: F811
    historical = _query(client, "SELECT * FROM public.matchup_season WHERE db_name='test_league' AND year=2025")
    bundle = _bundle(tmp_path, homepage=homepage)
    response = _publish(client, bundle)
    assert response.status_code == 200, response.text
    assert response.json()['career_rollups']['test_league']['matchup_career'] == 1
    expected_games = 15 if homepage else 16
    assert _query(client, "SELECT games, seasons FROM public.matchup_career WHERE db_name='test_league'") == [{'games': expected_games, 'seasons': 2}]
    if homepage:
        assert response.json()['season_rollups']['test_league']['matchup_season'] == 1
        assert response.json()['season_rollup_years']['test_league'] == [2026]
        assert response.json()['homepage_rollups']['test_league']['homepage_manager_rankings'] == 1
        assert _query(client, "SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'") == [{'highest_score_points': 140.0}]
        assert _query(client, "SELECT manager,seasons,wins,losses FROM public.homepage_manager_rankings WHERE db_name='test_league'") == [{'manager':'Shared Alias','seasons':2,'wins':1,'losses':1}]
    replay = _publish(client, bundle)
    assert replay.status_code == 200, replay.text
    assert replay.json()['idempotent_replay'] is True
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation': 1}]
    assert _query(client, "SELECT * FROM public.matchup_season WHERE db_name='test_league' AND year=2025") == historical


def test_homepage_merge_refreshes_game_ranks_only_once(client, tmp_path, monkeypatch):  # noqa: F811
    """Season publication owns the all-history rank refresh for v3 bundles."""
    from multi_league.transformations.aggregation import aggregation_utils

    original = aggregation_utils.aggregate_career_rollups
    calls: list[bool] = []

    def recorded(conn, db_name, *, refresh_game_ranks=True):
        calls.append(refresh_game_ranks)
        return original(conn, db_name, refresh_game_ranks=refresh_game_ranks)

    monkeypatch.setattr(aggregation_utils, "aggregate_career_rollups", recorded)
    response = _publish(client, _bundle(tmp_path, homepage=True))

    assert response.status_code == 200, response.text
    assert calls == [False]


@pytest.mark.parametrize('data_dir', ['missing_historical_franchise'], indirect=True)
def test_http_missing_historical_aggregate_is_repaired_inside_scoped_publication(client, tmp_path):  # noqa: F811
    source_before = _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025")
    response = _publish(client, _bundle(tmp_path, homepage=True, generation=7))
    assert response.status_code == 200, response.text
    assert response.json()['season_rollup_years']['test_league'] == [2025, 2026]
    assert _query(client, "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025") == source_before
    assert _query(client, "SELECT franchise_id,games,wins,losses FROM public.matchup_season WHERE db_name='test_league' AND year=2025") == [
        {'franchise_id': 'f1', 'games': 1, 'wins': 1, 'losses': 0}]
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation': 8}]
    assert _query(client, "SELECT games,seasons FROM public.matchup_career WHERE db_name='test_league'") == [
        {'games': 2, 'seasons': 2}]


def test_error_after_fleet_commit_preserves_receipt_and_does_not_republish(client, tmp_path, monkeypatch):  # noqa: F811
    import main

    execute = main._interrupting_execute

    def checkpoint_error_after_commit(conn, sql, *args, **kwargs):
        result = execute(conn, sql, *args, **kwargs)
        if kwargs.get('step') == 'commit fleet partition':
            raise OSError('checkpoint failed after transaction committed')
        return result

    monkeypatch.setattr(main, '_interrupting_execute', checkpoint_error_after_commit)
    bundle = _bundle(tmp_path)
    response = _publish(client, bundle)
    assert response.status_code == 500
    assert _query(client, "SELECT status FROM merge_admin.league_delta_merge_state WHERE db_name='___fleet'") == [{'status': 'COMMITTED'}]
    assert _query(client, "SELECT games,seasons FROM public.matchup_career WHERE db_name='test_league'") == [{'games':16, 'seasons':2}]
    replay = _publish(client, bundle)
    assert replay.status_code == 200, replay.text
    assert replay.json()['idempotent_replay'] is True
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation':1}]


@pytest.mark.parametrize('data_dir', ['legacy_trade_mirror'], indirect=True)
def test_http_weekly_merge_repairs_legacy_null_sent_pick_mirror(client, tmp_path):  # noqa: F811
    response = _publish(client, _bundle(tmp_path, homepage=True))

    assert response.status_code == 200, response.text
    assert _query(
        client,
        "SELECT trade_direction,trade_asset_lamar FROM public.transactions "
        "WHERE db_name='test_league' ORDER BY trade_direction",
    ) == [
        {'trade_direction': 'received', 'trade_asset_lamar': 17.0},
        {'trade_direction': 'sent', 'trade_asset_lamar': 17.0},
    ]


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


@pytest.mark.parametrize('data_dir', ['recovery_source', 'recovery_empty'], indirect=True)
def test_scoped_recovery_invalidates_stale_bundles_only_when_committed(client, tmp_path, monkeypatch, request):  # noqa: F811
    from pathlib import Path
    import main

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / 'scripts'))
    before = _query(client, "SELECT * FROM public.matchup_season WHERE db_name='test_league' ORDER BY year")
    response = client.post('/reaggregate-damaged-derived',
        headers={'Authorization': 'Bearer test-admin'},
        json={'mode':'scoped_rebuild', 'db_name':'test_league', 'confirm_targets':list(main._DAMAGED_DERIVED_TARGETS)})
    success = request.node.callspec.params['data_dir'] == 'recovery_source'
    assert response.status_code == (200 if success else 500), response.text
    # A leaked read-only attachment makes the next metadata write drain the
    # league pool just to obtain the OPS writer handle. Both commit and rollback
    # must release it before returning the connection to normal traffic.
    connection = main.db.acquire_connection(timeout=1)
    try:
        assert connection.execute(
            "SELECT database_name FROM duckdb_databases() WHERE database_name='___ops'"
        ).fetchall() == []
    finally:
        main.db.release_connection(connection)
    assert _query(client, "SELECT generation FROM merge_admin.league_publish_generations WHERE db_name='test_league'") == [{'generation': 8 if success else 7}]
    assert _query(client, "SELECT * FROM public.matchup_season WHERE db_name='test_league' ORDER BY year") == before
    if success:
        assert _query(client, "SELECT year,fantasy_points,clutch_equity FROM public.player_fantasy_season WHERE db_name='test_league' ORDER BY year") == [
            {'year':2025,'fantasy_points':20.0,'clutch_equity':0.0},
            {'year':2026,'fantasy_points':30.0,'clutch_equity':0.0},
        ]
        stale = _publish(client, _bundle(tmp_path, generation=7, homepage=True))
        assert stale.status_code == 409, stale.text
    else:
        assert _query(client, "SELECT COUNT(*) n FROM public.standings_by_year WHERE db_name='test_league'") == [{'n':0}]
