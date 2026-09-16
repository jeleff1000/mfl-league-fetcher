"""Exercise normal career aggregations on the merged publication connection."""

import duckdb
import pytest
import importlib.util
from pathlib import Path
import tarfile

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, create_aggregate_table_sql
from multi_league.core.delta_publish import canonical_table_registry
from multi_league.transformations.aggregation import aggregation_utils


def _fleet_server():
    server_path = Path(__file__).resolve().parents[4] / "duckdb-server" / "fleet_merge.py"
    spec = importlib.util.spec_from_file_location("career_test_fleet_merge", server_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _weekly_bundle(tmp_path, *, generation=0, homepage=False):
    from multi_league.core.fleet_publish import build_fleet_partition_bundle

    with duckdb.connect(":memory:") as staged:
        staged.execute("CREATE SCHEMA public")
        staged.execute("""
            CREATE TABLE public.matchup_season AS
            SELECT 'test_league' AS db_name, 'Shared Alias' AS manager, 'f1' AS franchise_id,
                   2026 AS year, 2 AS games, 2 AS wins, 0 AS losses
        """)
        staged.execute("""
            CREATE TABLE public.matchup_career AS
            SELECT 'test_league' AS db_name, 'Shared Alias' AS manager, 'f1' AS franchise_id, 2 AS games
        """)
        extra = {'rebuild_homepage_rollups': True} if homepage else {}
        tables = ['matchup_season', 'matchup_career']
        if homepage:
            staged.execute("CREATE TABLE public.homepage_league_summary AS SELECT 'test_league' AS db_name, 110 AS highest_score_points")
            tables.append('homepage_league_summary')
        bundle = build_fleet_partition_bundle(
            staged, active_year=2026, league_generations={'test_league': generation},
            tables=tables, output_dir=tmp_path,
            import_run_id='9001', publish_sequence=1, rebuild_career_rollups=True,
            **extra,
        )
    with tarfile.open(bundle.path) as archive:
        archive.extractall(tmp_path / 'extracted', filter='data')
    return bundle, tmp_path / 'extracted'


def test_weekly_merge_rebuilds_careers_inside_its_commit(merged_chain, tmp_path):
    server = _fleet_server()
    bundle, extracted = _weekly_bundle(tmp_path)
    assert bundle.manifest['schema_version'] == 'fleet-partition-v2'
    assert [entry['table'] for entry in bundle.manifest['tables']] == ['matchup_season']
    registry = canonical_table_registry()
    server.validate_fleet_manifest_shape(
        bundle.manifest, allowed_tables=set(registry),
        identity_keys={table: tuple(spec['primary_keys']) for table, spec in registry.items()},
    )
    conn = merged_chain
    before = conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall()
    receipt = server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert receipt['status'] == 'COMMITTED'
    assert receipt['career_rollups']['test_league']['matchup_career'] == 1
    assert conn.execute("SELECT games, seasons FROM public.matchup_career WHERE db_name='test_league'").fetchone() == (16, 2)
    assert conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall() == before
    assert server.current_generations(conn, ['test_league']) == {'test_league': 1}
    with pytest.raises(server.FleetGenerationConflict):
        server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert conn.execute("SELECT games FROM public.matchup_career WHERE db_name='test_league'").fetchone() == (16,)


def test_weekly_merge_rolls_back_partitions_when_career_rebuild_fails(merged_chain, tmp_path):
    server = _fleet_server()
    bundle, extracted = _weekly_bundle(tmp_path)
    conn = merged_chain
    conn.execute('DROP TABLE public.player_fantasy')
    before = conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall()
    with pytest.raises(RuntimeError, match='player_fantasy'):
        server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before
    assert server.current_generations(conn, ['test_league']) == {'test_league': 0}


def test_weekly_merge_rejects_a_generation_scope_not_matching_actual_rows(merged_chain, tmp_path):
    server = _fleet_server()
    bundle, extracted = _weekly_bundle(tmp_path)
    bundle.manifest['db_names'] = ['another_league']
    bundle.manifest['league_generations'] = {'another_league': 0}
    before = merged_chain.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall()
    with pytest.raises(server.FleetValidationError, match='scope'):
        server.apply_fleet_merge(merged_chain, bundle.manifest, extracted)
    assert merged_chain.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before


def test_weekly_merge_career_sql_uses_publication_timeout_executor(merged_chain, tmp_path):
    server = _fleet_server()
    bundle, extracted = _weekly_bundle(tmp_path)
    before = merged_chain.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall()

    def interrupted_execute(conn, sql, params=None, *, step=''):
        if 'DELETE FROM ___leagues.public.matchup_career' in sql:
            raise TimeoutError('career query deadline')
        return conn.execute(sql, params) if params is not None else conn.execute(sql)

    with pytest.raises(TimeoutError, match='career query deadline'):
        server.apply_fleet_merge(merged_chain, bundle.manifest, extracted, execute=interrupted_execute)
    assert merged_chain.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before
    assert server.current_generations(merged_chain, ['test_league']) == {'test_league': 0}


@pytest.fixture
def merged_chain():
    with duckdb.connect(":memory:") as conn:
        conn.execute("ATTACH ':memory:' AS ___leagues")
        conn.execute("USE ___leagues")
        conn.execute("CREATE SCHEMA public")
        for table, spec in canonical_table_registry().items():
            if table in AGGREGATE_TABLE_SPECS:
                conn.execute(create_aggregate_table_sql("___leagues", table))
            else:
                columns = ', '.join(f'"{name}" {dtype}' for name, dtype in spec['columns'].items())
                conn.execute(f'CREATE TABLE public."{table}" ({columns})')
        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.nfl_historical")
        conn.execute("""
            CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
                player_week VARCHAR, NFL_player_id VARCHAR, player VARCHAR,
                nfl_team VARCHAR, year INTEGER, week INTEGER, headshot_url VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE ___ops.nfl_historical.player_bio (
                NFL_player_id VARCHAR, player VARCHAR, headshot_url VARCHAR,
                yahoo_player_id VARCHAR, sleeper_player_id VARCHAR, espn_id VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO public.matchup_season (db_name, franchise_id, manager, year, games, wins, losses)
            VALUES ('test_league', 'f1', 'Shared Alias', 2025, 14, 8, 6),
                   ('test_league', 'f1', 'Shared Alias', 2026, 1, 1, 0)
        """)
        conn.execute("""
            INSERT INTO public.player_fantasy
                (db_name, NFL_player_id, player_week, player, year, week, manager, franchise_id,
                 position, fantasy_position, fantasy_points, player_lamar, manager_lamar, is_started,
                 clutch_equity, win, loss)
            VALUES ('test_league', 'p1', 'p1_2025_1', 'Player', 2025, 1, 'Shared Alias', 'f1',
                    'QB', 'QB', 20, 5, 5, 1, 0.2, 1, 0),
                   ('test_league', 'p1', 'p1_2026_1', 'Player', 2026, 1, 'Shared Alias', 'f1',
                    'QB', 'QB', 30, 8, 8, 1, 0.3, 1, 0),
                   ('another_league', 'p1', 'p1_2026_1', 'Player', 2026, 1, 'Other', 'f9',
                    'QB', 'QB', 999, 999, 999, 1, 999, 1, 0)
        """)
        conn.execute("""
            INSERT INTO public.player_fantasy_career (db_name, NFL_player_id, fantasy_points)
            VALUES ('another_league', 'p1', 999)
        """)
        yield conn


def test_shared_career_rebuild_uses_merged_history_and_retains_aliases(merged_chain):
    conn = merged_chain
    historical_seasons = conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall()
    for current_points, expected_career_points in [(30.0, 50.0), (30.25, 50.25), (30.25, 50.25)]:
        conn.execute("""
            UPDATE public.player_fantasy SET fantasy_points=?
            WHERE db_name='test_league' AND year=2026
        """, [current_points])
        aggregation_utils.aggregate_career_rollups(conn, 'test_league')
        assert conn.execute("""
            SELECT manager, franchise_id, games, wins, losses, seasons
            FROM public.matchup_career WHERE db_name='test_league'
        """).fetchall() == [('Shared Alias', 'f1', 15, 9, 6, 2)]
        for table in ('player_fantasy_career', 'player_fantasy_career_all'):
            row = conn.execute(f"""
                SELECT fantasy_points, years_active, games_started, managers, clutch_equity
                FROM public.{table} WHERE db_name='test_league'
            """).fetchone()
            assert row == (expected_career_points, 2, 2, 'Shared Alias', 0.5)
        assert conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall() == historical_seasons
        assert conn.execute("""
            SELECT fantasy_points FROM public.player_fantasy_career WHERE db_name='another_league'
        """).fetchone() == (999.0,)


def test_shared_career_rebuild_does_not_commit_the_callers_transaction(merged_chain):
    conn = merged_chain
    conn.execute('BEGIN TRANSACTION')
    aggregation_utils.aggregate_career_rollups(conn, 'test_league')
    assert conn.execute("SELECT count(*) FROM public.matchup_career WHERE db_name='test_league'").fetchone()[0] == 1
    conn.execute('ROLLBACK')
    assert conn.execute("SELECT count(*) FROM public.matchup_career WHERE db_name='test_league'").fetchone()[0] == 0


def test_shared_career_rebuild_rejects_worker_scratch_connection():
    with duckdb.connect(":memory:") as conn:
        with pytest.raises(RuntimeError, match="___leagues"):
            aggregation_utils.aggregate_career_rollups(conn, 'test_league')


def test_shared_career_rebuild_rejects_missing_sources_before_deleting_outputs(merged_chain):
    conn = merged_chain
    conn.execute("INSERT INTO public.matchup_career (db_name, manager, franchise_id, games) VALUES ('test_league','Alias','f1',15)")
    conn.execute("DROP TABLE public.player_fantasy")
    with pytest.raises(RuntimeError, match="player_fantasy"):
        aggregation_utils.aggregate_career_rollups(conn, 'test_league')
    assert conn.execute("SELECT games FROM public.matchup_career WHERE db_name='test_league'").fetchone() == (15,)


@pytest.fixture
def homepage_chain(merged_chain):
    conn = merged_chain
    conn.execute("""
        INSERT INTO public.matchup
            (db_name,year,week,manager,franchise_id,team_name,platform,team_points,
             opponent_points,win,loss,tie,is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',2025,1,'Shared Alias','f1','Team','yahoo',140,100,1,0,0,0,0,0),
               ('test_league',2026,1,'Shared Alias','f1','Team','sleeper',110,120,0,1,0,0,0,0),
               ('another_league',2026,1,'Other','f9','Other','espn',999,0,1,0,0,0,0,0)
    """)
    conn.execute("""
        INSERT INTO public.homepage_league_summary (db_name,highest_score_points)
        VALUES ('test_league',1),('another_league',999)
    """)
    yield conn


def test_shared_homepage_rebuild_reads_full_chain_and_joins_callers_transaction(homepage_chain):
    conn = homepage_chain
    conn.execute('BEGIN TRANSACTION')
    aggregation_utils.aggregate_career_rollups(conn, 'test_league')
    counts = aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert counts['homepage_league_summary'] == 1
    assert counts['homepage_manager_profiles'] == 1
    assert conn.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (140.0,)
    assert conn.execute("SELECT manager,seasons,wins,losses FROM public.homepage_manager_rankings WHERE db_name='test_league'").fetchone() == ('Shared Alias',2,1,1)
    assert conn.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='another_league'").fetchone() == (999.0,)
    conn.execute('ROLLBACK')
    assert conn.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (1.0,)


@pytest.mark.parametrize('table', ['homepage_manager_rankings', 'homepage_manager_profiles', 'homepage_current_standings'])
def test_homepage_rejects_missing_franchise_before_replacing_outputs(homepage_chain, monkeypatch, table):
    from multi_league.transformations.aggregation import homepage_summary

    original = homepage_summary.compute_homepage_frames

    def incomplete(conn, db_name):
        frames = original(conn, db_name)
        frames[table] = frames[table].iloc[0:0]
        return frames

    monkeypatch.setattr(homepage_summary, 'compute_homepage_frames', incomplete)
    with pytest.raises(RuntimeError, match='franchise'):
        aggregation_utils.aggregate_homepage_rollups(homepage_chain, 'test_league')
    assert homepage_chain.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (1.0,)


def test_homepage_rejects_lost_summary_value_without_silently_restoring_it(homepage_chain, monkeypatch):
    from multi_league.transformations.aggregation import homepage_summary

    original = homepage_summary.compute_homepage_frames

    def incomplete(conn, db_name):
        frames = original(conn, db_name)
        frames['homepage_league_summary']['highest_score_points'] = None
        return frames

    monkeypatch.setattr(homepage_summary, 'compute_homepage_frames', incomplete)
    with pytest.raises(RuntimeError, match='highest_score_points'):
        aggregation_utils.aggregate_homepage_rollups(homepage_chain, 'test_league')
    assert homepage_chain.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (1.0,)


def test_shared_homepage_rebuild_rejects_worker_scratch_connection():
    with duckdb.connect(':memory:') as conn:
        with pytest.raises(RuntimeError, match='___leagues'):
            aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')


def test_homepage_new_season_clears_old_season_highlight_but_keeps_alltime(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.transactions
            (db_name, year, week, transaction_type, player, manager, franchise_id, manager_lamar_ros_managed)
        VALUES ('test_league', 2025, 2, 'add', 'Historical Pickup', 'Shared Alias', 'f1', 12)
    """)
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2025,
            season_best_pickup_player='Historical Pickup', season_best_pickup_year=2025,
            alltime_best_pickup_player='Historical Pickup'
        WHERE db_name='test_league'
    """)
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT data_year, season_best_pickup_player, season_best_pickup_year,
               alltime_best_pickup_player, highest_score_points
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == (2026, None, None, 'Historical Pickup', 140.0)


def test_homepage_same_season_does_not_erase_populated_highlight(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2026,
            season_best_pickup_player='Missing Pickup'
        WHERE db_name='test_league'
    """)
    with pytest.raises(RuntimeError, match='season_best_pickup_player'):
        aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("SELECT season_best_pickup_player FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == ('Missing Pickup',)


def test_homepage_rollover_still_rejects_lost_alltime_highlight(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2025,
            alltime_best_pickup_player='Missing Pickup'
        WHERE db_name='test_league'
    """)
    with pytest.raises(RuntimeError, match='alltime_best_pickup_player'):
        aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')


def test_weekly_publication_rebuilds_homepage_on_same_full_chain(merged_chain, tmp_path):
    conn = merged_chain
    conn.execute("""
        INSERT INTO public.matchup
            (db_name,year,week,manager,franchise_id,team_name,team_points,
             opponent_points,win,loss,tie,is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',2025,1,'Shared Alias','f1','Team',140,100,1,0,0,0,0,0),
               ('test_league',2026,1,'Shared Alias','f1','Team',110,120,0,1,0,0,0,0)
    """)
    bundle, extracted = _weekly_bundle(tmp_path, homepage=True)
    assert [entry['table'] for entry in bundle.manifest['tables']] == ['matchup_season']
    server = _fleet_server()
    registry = canonical_table_registry()
    server.validate_fleet_manifest_shape(
        bundle.manifest, allowed_tables=set(registry),
        identity_keys={t: tuple(s['primary_keys']) for t, s in registry.items()},
    )
    receipt = server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert receipt['homepage_rollups']['test_league']['homepage_manager_profiles'] == 1
    assert conn.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (140.0,)
    assert conn.execute("SELECT manager,seasons,wins,losses FROM public.homepage_manager_rankings WHERE db_name='test_league'").fetchone() == ('Shared Alias',2,1,1)


def test_homepage_failure_rolls_back_careers_and_partition_changes(merged_chain, tmp_path):
    conn = merged_chain
    bundle, extracted = _weekly_bundle(tmp_path, homepage=True)
    conn.execute('DROP TABLE public.league_context')
    before = conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall()
    server = _fleet_server()
    with pytest.raises(server.FleetValidationError, match='league_context'):
        server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before
    assert conn.execute("SELECT COUNT(*) FROM public.matchup_career WHERE db_name='test_league'").fetchone() == (0,)
    assert server.current_generations(conn, ['test_league']) == {'test_league': 0}
