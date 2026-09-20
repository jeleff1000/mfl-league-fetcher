"""Exercise normal career aggregations on the merged publication connection."""

import duckdb
import pandas as pd
import pytest
import importlib.util
from pathlib import Path
import tarfile
import warnings

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, create_aggregate_table_sql
from multi_league.core.delta_publish import canonical_table_registry
from multi_league.transformations.aggregation import aggregation_utils
from multi_league.transformations.aggregation.aggregate_fantasy_context import (
    _drop_scoped_nfl_lookup_tables,
)


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


def test_full_chain_publication_corrects_game_ranks_before_deriving_careers(merged_chain, tmp_path):
    server = _fleet_server()
    bundle, extracted = _weekly_bundle(tmp_path)
    conn = merged_chain
    conn.execute("""
        UPDATE public.player_fantasy SET position_season_rank=216,position_alltime_rank=216
        WHERE db_name='test_league'
    """)
    server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert conn.execute("""
        SELECT year,position_season_rank,position_alltime_rank
        FROM public.player_fantasy WHERE db_name='test_league' ORDER BY year
    """).fetchall() == [(2025, 1, 2), (2026, 1, 1)]
    assert conn.execute("SELECT position_alltime_rank FROM public.player_fantasy WHERE db_name='another_league'").fetchone() == (None,)


def test_scoped_season_rollups_rank_complete_history_not_only_changed_year(merged_chain):
    conn = merged_chain
    prior_season = conn.execute("SELECT * FROM public.matchup_season WHERE db_name='test_league' AND year=2025").fetchall()
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league', season_years={2026})
    assert conn.execute("""
        SELECT year,position_season_rank,position_alltime_rank
        FROM public.player_fantasy WHERE db_name='test_league' ORDER BY year
    """).fetchall() == [(2025, 1, 2), (2026, 1, 1)]
    assert conn.execute("SELECT * FROM public.matchup_season WHERE db_name='test_league' AND year=2025").fetchall() == prior_season


def test_prepared_nfl_lookups_preserve_all_four_fantasy_rollups(merged_chain):
    conn = merged_chain
    aggregation_utils.aggregate_complete_chain_season_rollups(
        conn, "test_league", season_years={2026},
    )
    aggregation_utils.aggregate_career_rollups(conn, "test_league", refresh_game_ranks=False)
    tables = (
        "player_fantasy_season",
        "player_fantasy_season_all",
        "player_fantasy_career",
        "player_fantasy_career_all",
    )
    expected = {
        table: conn.execute(
            f"SELECT * EXCLUDE(last_updated) FROM public.{table} "
            "WHERE db_name='test_league' ORDER BY ALL"
        ).fetchall()
        for table in tables
    }

    try:
        aggregation_utils.aggregate_complete_chain_season_rollups(
            conn,
            "test_league",
            season_years={2026},
            prepare_shared_nfl_lookups=True,
        )
        aggregation_utils.aggregate_career_rollups(
            conn,
            "test_league",
            refresh_game_ranks=False,
            prepared_nfl_lookups=True,
        )
    finally:
        _drop_scoped_nfl_lookup_tables(conn)

    actual = {
        table: conn.execute(
            f"SELECT * EXCLUDE(last_updated) FROM public.{table} "
            "WHERE db_name='test_league' ORDER BY ALL"
        ).fetchall()
        for table in tables
    }
    assert actual == expected


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
    assert conn.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE table_name LIKE '_weekly_refresh_%'"
    ).fetchall() == []


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
    before_ranks = merged_chain.execute("""
        SELECT player_week,position_season_rank,position_alltime_rank
        FROM public.player_fantasy WHERE db_name='test_league' ORDER BY player_week
    """).fetchall()

    def interrupted_execute(conn, sql, params=None, *, step=''):
        if 'DELETE FROM ___leagues.public.matchup_career' in sql:
            raise TimeoutError('career query deadline')
        return conn.execute(sql, params) if params is not None else conn.execute(sql)

    with pytest.raises(TimeoutError, match='career query deadline'):
        server.apply_fleet_merge(merged_chain, bundle.manifest, extracted, execute=interrupted_execute)
    assert merged_chain.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before
    assert merged_chain.execute("""
        SELECT player_week,position_season_rank,position_alltime_rank
        FROM public.player_fantasy WHERE db_name='test_league' ORDER BY player_week
    """).fetchall() == before_ranks
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


def test_complete_chain_rollups_rebuild_stale_historical_season_dependencies(merged_chain):
    """Weekly publication must not build careers on stale historical season rows."""
    conn = merged_chain
    conn.execute("""
        INSERT INTO public.league_settings
            (db_name, year, platform, league_key, num_teams, playoff_start_week, uses_median)
        VALUES ('test_league', 2025, 'sleeper', 'old', 2, 15, 0),
               ('test_league', 2026, 'sleeper', 'new', 2, 15, 0)
    """)
    conn.execute("""
        INSERT INTO public.matchup
            (db_name, year, week, manager, franchise_id, opponent,
             opponent_franchise_id, team_points, opponent_points,
             win, loss, tie, is_playoffs, is_consolation, is_bye_week)
        VALUES ('test_league', 2025, 1, 'Shared Alias', 'f1', 'Opponent',
                'f2', 120, 100, 1, 0, 0, 0, 0, 0),
               ('test_league', 2026, 1, 'Shared Alias', 'f1', 'Opponent',
                'f2', 130, 110, 1, 0, 0, 0, 0, 0)
    """)
    conn.execute("""
        INSERT INTO public.player_fantasy_season
            (db_name, NFL_player_id, year, player, fantasy_points,
             games_started, games_rostered, wins, losses)
        VALUES ('test_league', 'p1', 2025, 'Player', 20, 1, 1, 99, 99)
    """)
    conn.execute("""
        INSERT INTO public.transactions
            (db_name, transaction_id, transaction_sequence, year, week,
             manager, franchise_id, player, transaction_type,
             manager_lamar_ros_managed, faab_bid, transaction_grade)
        VALUES ('test_league', 'txn-1', 1, 2025, 1,
                'Shared Alias', 'f1', 'Player', 'add', 5.0, 10.0, 'A')
    """)
    conn.execute("""
        INSERT INTO public.transaction_report_card
            (db_name, franchise_id, manager, year, adds, total_lamar)
        VALUES ('test_league', 'f1', 'Shared Alias', 2025, 99, 999)
    """)

    source_witness = conn.execute("""
        SELECT COUNT(*), bit_xor(hash(t))
        FROM (
            SELECT * EXCLUDE (position_season_rank, position_alltime_rank)
            FROM public.player_fantasy WHERE db_name='test_league'
        ) t
    """).fetchone()

    counts = aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    aggregation_utils.aggregate_career_rollups(conn, 'test_league')

    assert counts['matchup_season'] == 2
    assert counts['player_fantasy_season'] == 2
    assert counts['transaction_manager_season'] == 1
    assert counts['transaction_report_card'] == 1
    assert conn.execute("""
        SELECT year, wins, losses
        FROM public.player_fantasy_season
        WHERE db_name='test_league' AND NFL_player_id='p1'
        ORDER BY year
    """).fetchall() == [(2025, 1, 0), (2026, 1, 0)]
    assert conn.execute("""
        SELECT games, wins, losses, seasons
        FROM public.matchup_career
        WHERE db_name='test_league' AND franchise_id='f1'
    """).fetchone() == (2, 2, 0, 2)
    assert conn.execute("""
        SELECT wins, losses, years_active
        FROM public.player_fantasy_career
        WHERE db_name='test_league' AND NFL_player_id='p1'
    """).fetchone() == (2, 0, 2)
    assert conn.execute("""
        SELECT r.adds, r.total_lamar, s.adds, s.net_lamar
        FROM public.transaction_report_card r
        JOIN public.transaction_manager_season s
          ON r.db_name=s.db_name AND r.franchise_id=s.franchise_id AND r.year=s.year
        WHERE r.db_name='test_league'
    """).fetchone() == (1, 5.0, 1, 5.0)
    assert conn.execute("""
        SELECT COUNT(*), bit_xor(hash(t))
        FROM (
            SELECT * EXCLUDE (position_season_rank, position_alltime_rank)
            FROM public.player_fantasy WHERE db_name='test_league'
        ) t
    """).fetchone() == source_witness


def test_complete_chain_rollups_reapply_saved_manager_merges_before_aggregation(merged_chain):
    """A weekly rollup must not split identities already merged in Settings."""
    conn = merged_chain
    conn.execute("""
        INSERT INTO public.league_context
            (db_name, league_name, manager_name_overrides_json, franchise_merges_json)
        VALUES (
            'test_league',
            'Test League',
            '{"Old Owner":"Preferred","Current Owner":"Preferred"}',
            '[{"display_name":"Current Owner","owner_ids":["new-id","old-id"],'
            '"from_franchise_id":"old-id","into_franchise_id":"new-id"}]'
        )
    """)
    conn.execute("""
        INSERT INTO public.league_settings
            (db_name, year, platform, league_key, num_teams, playoff_start_week, uses_median)
        VALUES ('test_league', 2025, 'espn', 'old', 2, 15, 0),
               ('test_league', 2026, 'espn', 'new', 2, 15, 0)
    """)
    conn.execute("""
        INSERT INTO public.matchup
            (db_name, year, week, manager, franchise_id, opponent,
             opponent_franchise_id, team_points, opponent_points,
             win, loss, tie, is_playoffs, is_consolation, is_bye_week)
        VALUES ('test_league', 2025, 1, 'Old Owner', 'old-id', 'Opponent',
                'opponent-id', 120, 100, 1, 0, 0, 0, 0, 0),
               ('test_league', 2026, 1, 'Current Owner', 'new-id', 'Opponent',
                'opponent-id', 130, 110, 1, 0, 0, 0, 0, 0)
    """)

    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')

    assert conn.execute("""
        SELECT DISTINCT manager, franchise_id
        FROM public.matchup
        WHERE db_name='test_league'
        ORDER BY manager, franchise_id
    """).fetchall() == [('Preferred', 'new-id')]
    assert conn.execute("""
        SELECT year, manager, franchise_id, wins
        FROM public.matchup_season
        WHERE db_name='test_league'
        ORDER BY year
    """).fetchall() == [
        (2025, 'Preferred', 'new-id', 1),
        (2026, 'Preferred', 'new-id', 1),
    ]


@pytest.mark.parametrize('changed_year', [2025, 2026])
def test_scoped_season_rollups_preserve_other_years_and_keep_full_careers(homepage_chain, changed_year):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.draft
            (db_name, year, round, pick, manager, franchise_id, player,
             NFL_player_id, manager_lamar, cost)
        VALUES ('test_league',2025,1,1,'Shared Alias','f1','Player','p1',5,10),
               ('test_league',2026,1,1,'Shared Alias','f1','Player','p1',8,10)
    """)
    conn.execute("""
        INSERT INTO public.transactions
            (db_name, transaction_id, year, week, manager, franchise_id,
             player, NFL_player_id, transaction_type, manager_lamar_ros_managed)
        VALUES ('test_league','old',2025,1,'Shared Alias','f1','Player','p1','add',5),
               ('test_league','new',2026,1,'Shared Alias','f1','Player','p1','add',8)
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    tables = aggregation_utils.COMPLETE_CHAIN_SEASON_ROLLUP_TABLES
    historical = {
        table: conn.execute(
            f'SELECT * FROM public.{table} WHERE db_name=? AND year<>? ORDER BY year',
            ['test_league', changed_year],
        ).fetchall()
        for table in tables
    }
    other_league = conn.execute("SELECT * FROM public.player_fantasy WHERE db_name='another_league'").fetchall()
    conn.execute("UPDATE public.player_fantasy SET fantasy_points=40 WHERE db_name='test_league' AND year=?", [changed_year])
    conn.execute("UPDATE public.matchup SET team_points=150.25 WHERE db_name='test_league' AND year=?", [changed_year])
    conn.execute("UPDATE public.draft SET manager_lamar=12 WHERE db_name='test_league' AND year=?", [changed_year])
    conn.execute("UPDATE public.transactions SET manager_lamar_ros_managed=12 WHERE db_name='test_league' AND year=?", [changed_year])

    for _ in range(2):
        counts = aggregation_utils.aggregate_complete_chain_season_rollups(
            conn, 'test_league', season_years={changed_year},
        )
        aggregation_utils.aggregate_career_rollups(conn, 'test_league')
        assert {table: counts[table] for table in tables} == {table: 1 for table in tables}
        assert counts['standings_by_year'] == 1
        for table, before in historical.items():
            assert conn.execute(
                f'SELECT * FROM public.{table} WHERE db_name=? AND year<>? ORDER BY year',
                ['test_league', changed_year],
            ).fetchall() == before, table
        assert conn.execute(
            "SELECT fantasy_points FROM public.player_fantasy_season WHERE db_name='test_league' AND year=?",
            [changed_year],
        ).fetchone() == (40,)
        assert conn.execute(
            "SELECT total_team_points FROM public.matchup_season WHERE db_name='test_league' AND year=?",
            [changed_year],
        ).fetchone() == (150.25,)
        assert conn.execute(
            "SELECT total_manager_lamar FROM public.draft_manager_season WHERE db_name='test_league' AND year=?",
            [changed_year],
        ).fetchone() == (12,)
        assert conn.execute(
            "SELECT total_lamar FROM public.transaction_report_card WHERE db_name='test_league' AND year=?",
            [changed_year],
        ).fetchone() == (12,)
        assert conn.execute(
            "SELECT fantasy_points, years_active FROM public.player_fantasy_career WHERE db_name='test_league'",
        ).fetchone() == ((70 if changed_year == 2025 else 60), 2)
        assert conn.execute(
            "SELECT manager, games, seasons FROM public.matchup_career WHERE db_name='test_league'",
        ).fetchone() == ('Shared Alias', 2, 2)
    assert conn.execute("SELECT * FROM public.player_fantasy WHERE db_name='another_league'").fetchall() == other_league


def test_empty_season_scope_does_not_rewrite_existing_rollups(homepage_chain):
    conn = homepage_chain
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    before = conn.execute('SELECT * FROM public.matchup_season ORDER BY db_name,year').fetchall()
    assert aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league', season_years=set()) == {}
    assert conn.execute('SELECT * FROM public.matchup_season ORDER BY db_name,year').fetchall() == before


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
            (db_name,year,week,manager,franchise_id,opponent,opponent_franchise_id,
             team_name,platform,team_points,
             opponent_points,win,loss,tie,is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',2025,1,'Shared Alias','f1','Opponent','f2','Team','yahoo',140,100,1,0,0,0,0,0),
               ('test_league',2026,1,'Shared Alias','f1','Opponent','f2','Team','sleeper',110,120,0,1,0,0,0,0),
               ('another_league',2026,1,'Other','f9','Opponent','f8','Other','espn',999,0,1,0,0,0,0,0)
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


def test_scoped_homepage_profiles_recompute_active_and_preserve_inactive(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.matchup
            (db_name,year,week,manager,franchise_id,opponent,opponent_franchise_id,
             team_name,platform,team_points,opponent_points,win,loss,tie,
             is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',2025,2,'Archive Manager','f_old','Shared Alias','f1',
                'Archive Team','yahoo',90,100,0,1,0,0,0,0)
    """)
    aggregation_utils.aggregate_career_rollups(conn, 'test_league')
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    archive_before = conn.execute("""
        SELECT * FROM public.homepage_manager_profiles
        WHERE db_name='test_league' AND franchise_id='f_old'
    """).fetchone()

    counts = aggregation_utils.aggregate_homepage_rollups(
        conn,
        'test_league',
        manager_profile_franchise_ids={'f1'},
    )

    assert counts['homepage_manager_profiles'] == 2
    assert conn.execute("""
        SELECT * FROM public.homepage_manager_profiles
        WHERE db_name='test_league' AND franchise_id='f_old'
    """).fetchone() == archive_before


def test_homepage_coverage_ignores_historical_placeholder_franchises(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.matchup
            (db_name,year,week,manager,franchise_id,team_name,platform,team_points,
             is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',1994,1,'Archive Only','historical-placeholder',
                'Archive Only','sleeper',NULL,0,0,1)
    """)

    counts = aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')

    assert counts['homepage_manager_rankings'] == 1
    assert conn.execute("""
        SELECT franchise_id FROM public.homepage_manager_rankings
        WHERE db_name='test_league'
    """).fetchall() == [('f1',)]


def test_homepage_fleet_merge_preserves_unaffected_season_dependencies(
    homepage_chain, tmp_path
):
    server = _fleet_server()
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.league_settings
            (db_name, year, platform, league_key, num_teams, playoff_start_week, uses_median)
        VALUES ('test_league', 2025, 'yahoo', 'old', 2, 15, 0),
               ('test_league', 2026, 'sleeper', 'new', 2, 15, 0)
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    historical = conn.execute("SELECT * FROM public.player_fantasy_season WHERE year=2025").fetchall()
    before_source = conn.execute("""
        SELECT COUNT(*), bit_xor(hash(t))
        FROM public.player_fantasy t
        WHERE db_name='test_league'
    """).fetchone()
    bundle, extracted = _weekly_bundle(tmp_path, homepage=True)

    receipt = server.apply_fleet_merge(conn, bundle.manifest, extracted)

    assert receipt['season_rollups']['test_league']['matchup_season'] == 1
    assert receipt['season_rollups']['test_league']['player_fantasy_season'] == 1
    assert conn.execute("SELECT * FROM public.player_fantasy_season WHERE year=2025").fetchall() == historical
    assert receipt['season_seconds']['test_league'] >= 0
    assert conn.execute("""
        SELECT year, wins, losses
        FROM public.player_fantasy_season
        WHERE db_name='test_league' AND NFL_player_id='p1'
        ORDER BY year
    """).fetchall() == [(2025, 1, 0), (2026, 1, 0)]
    assert conn.execute("""
        SELECT wins, losses, years_active
        FROM public.player_fantasy_career
        WHERE db_name='test_league' AND NFL_player_id='p1'
    """).fetchone() == (2, 0, 2)
    assert conn.execute("""
        SELECT COUNT(*), bit_xor(hash(t))
        FROM public.player_fantasy t
        WHERE db_name='test_league'
    """).fetchone() == before_source


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


def test_homepage_allows_recomputed_transaction_highlight_to_clear_without_erasing_row(homepage_chain):
    """A backed source event may stop qualifying without deleting the summary."""
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.transactions
            (db_name, year, week, transaction_type, player, manager,
             franchise_id, manager_lamar_ros_managed)
        VALUES ('test_league', 2026, 1, 'add', 'Corrected Pickup',
                'Shared Alias', 'f1', 4)
    """)
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT season_best_pickup_player, season_best_pickup_lamar
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == ('Corrected Pickup', 4)

    conn.execute("""
        UPDATE public.transactions
        SET manager_lamar_ros_managed = -5
        WHERE db_name='test_league' AND player='Corrected Pickup'
    """)
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT COUNT(*), MAX(season_best_pickup_player), MAX(season_best_pickup_lamar)
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == (1, None, None)


def test_homepage_allows_optional_draft_value_to_clear_when_highlight_identity_changes(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.draft
            (db_name, year, round, pick, manager, franchise_id, player,
             NFL_player_id, manager_lamar, cost, draft_value_zscore, position)
        VALUES ('test_league', 2025, 1, 1, 'Old', 'f1', 'Old Worst',
                'old', 5, 30, -1, 'QB')
    """)
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT alltime_worst_pick_player, alltime_worst_pick_cost
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == ('Old Worst', 30)

    conn.execute("""
        INSERT INTO public.draft
            (db_name, year, round, pick, manager, franchise_id, player,
             NFL_player_id, manager_lamar, cost, draft_value_zscore, position)
        VALUES ('test_league', 2026, 1, 2, 'New', 'f1', 'New Worst',
                'new', 1, NULL, -2, 'QB')
    """)
    from multi_league.transformations.aggregation.homepage_summary import compute_homepage_frames
    candidate = compute_homepage_frames(conn, 'test_league')['homepage_league_summary'].iloc[0]
    assert candidate['alltime_worst_pick_player'] == 'New Worst'
    assert candidate.get('alltime_worst_pick_cost') is None or pd.isna(candidate.get('alltime_worst_pick_cost'))
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT alltime_worst_pick_player, alltime_worst_pick_cost
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == ('New Worst', None)


@pytest.mark.parametrize('highlight_year', [2025, 2026, None])
def test_homepage_highlight_scope_not_summary_stamp_controls_stale_trade_clear(homepage_chain, highlight_year):
    conn = homepage_chain
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2026,
            season_trade_winner='Old trade winner', season_trade_year=?,
            season_trade_week=4, season_trade_net_lamar=10
        WHERE db_name='test_league'
    """, [highlight_year])
    # An executed calculation with no qualifying trades is a valid nullable DDL
    # result, regardless of the previous highlight's stamp. Missing provider
    # transactions must be rejected at ingestion, not inferred from a highlight.
    aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
    assert conn.execute("""
        SELECT data_year,season_trade_winner,season_trade_year,season_trade_net_lamar
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == (2026, None, None, None)


def test_homepage_rollover_still_rejects_lost_alltime_highlight(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2025,
            alltime_best_pickup_player='Missing Pickup'
        WHERE db_name='test_league'
    """)
    with pytest.raises(RuntimeError, match='alltime_best_pickup_player'):
        aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')


@pytest.mark.parametrize('broken_source', [None, 'null_value', 'missing_sent_side', 'different_nfl_id'])
def test_same_season_trade_clear_requires_complete_valued_bilateral_assets(homepage_chain, broken_source):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.transactions
            (db_name,transaction_id,year,week,transaction_type,trade_direction,
             manager,franchise_id,source_franchise_id,player,NFL_player_id,trade_asset_lamar)
        VALUES ('test_league','pick-swap',2026,1,'trade_pick','received',
                'Shared Alias','f1','f2','Correct Bench Player','p1',0),
               ('test_league','pick-swap',2026,1,'trade_pick','sent',
                'Other Alias','f2','f1','Correct Bench Player','p1',0)
    """)
    conn.execute("""
        UPDATE public.homepage_league_summary SET data_year=2026,
            season_trade_winner='Shared Alias',season_trade_year=2026,
            season_trade_week=1,season_trade_net_lamar=12.93
        WHERE db_name='test_league'
    """)
    if broken_source == 'null_value':
        conn.execute("UPDATE public.transactions SET trade_asset_lamar=NULL WHERE db_name='test_league'")
    elif broken_source == 'missing_sent_side':
        conn.execute("DELETE FROM public.transactions WHERE db_name='test_league' AND trade_direction='sent'")
    elif broken_source == 'different_nfl_id':
        conn.execute("UPDATE public.transactions SET NFL_player_id='unrelated-player' WHERE db_name='test_league' AND trade_direction='sent'")
    if broken_source:
        from multi_league.transformations.aggregation.homepage_summary import _compute_best_trade

        with pytest.raises(RuntimeError, match='trade'):
            _compute_best_trade(conn, 'test_league', year=2026, platform='sleeper')
        with pytest.raises(RuntimeError, match='trade'):
            aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
        assert conn.execute("SELECT season_trade_net_lamar FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (12.93,)
    else:
        for _ in range(2):
            aggregation_utils.aggregate_homepage_rollups(conn, 'test_league')
            assert conn.execute("""
                SELECT season_trade_winner,season_trade_year,season_trade_net_lamar,
                       highest_score_points FROM public.homepage_league_summary
                WHERE db_name='test_league'
            """).fetchone() == (None,None,None,140.0)


def test_weekly_homepage_preserves_untouched_valid_trade_when_legacy_mirrors_are_incomplete(
    homepage_chain,
):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.transactions
            (db_name,transaction_id,year,week,transaction_type,trade_direction,
             manager,franchise_id,source_franchise_id,player,NFL_player_id,trade_asset_lamar)
        VALUES ('test_league','legacy-trade',2025,4,'trade','received',
                'Shared Alias','f1','f2','Legacy Player','legacy-player',12)
    """)
    conn.execute("""
        UPDATE public.homepage_league_summary SET
            alltime_trade_winner='Shared Alias', alltime_trade_loser='Other Alias',
            alltime_trade_winner_players='Legacy Player', alltime_trade_year=2025,
            alltime_trade_week=4, alltime_trade_net_lamar=12
        WHERE db_name='test_league'
    """)

    aggregation_utils.aggregate_homepage_rollups(
        conn,
        'test_league',
        changed_years={2026},
    )

    assert conn.execute("""
        SELECT alltime_trade_winner, alltime_trade_loser,
               alltime_trade_winner_players, alltime_trade_year,
               alltime_trade_week, alltime_trade_net_lamar,
               highest_score_points
        FROM public.homepage_league_summary WHERE db_name='test_league'
    """).fetchone() == (
        'Shared Alias', 'Other Alias', 'Legacy Player', 2025, 4, 12.0, 140.0
    )
    for table in (
        'homepage_league_summary',
        'homepage_manager_rankings',
        'homepage_current_standings',
        'homepage_manager_profiles',
    ):
        assert conn.execute(
            f"SELECT COUNT(*) FROM public.{table} WHERE db_name='test_league'"
        ).fetchone()[0] > 0
    assert conn.execute(
        "SELECT COUNT(*) FROM public.homepage_top_rivalries WHERE db_name='test_league'"
    ).fetchone() == (0,)


def test_weekly_homepage_rejects_incomplete_trade_mirror_in_changed_year(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.transactions
            (db_name,transaction_id,year,week,transaction_type,trade_direction,
             manager,franchise_id,source_franchise_id,player,NFL_player_id,trade_asset_lamar)
        VALUES ('test_league','current-trade',2026,1,'trade','received',
                'Shared Alias','f1','f2','Current Player','current-player',12)
    """)
    conn.execute("""
        UPDATE public.homepage_league_summary SET
            alltime_trade_winner='Shared Alias', alltime_trade_year=2025,
            alltime_trade_net_lamar=10
        WHERE db_name='test_league'
    """)

    with pytest.raises(RuntimeError, match='trade assets lack complete mirrored valuations'):
        aggregation_utils.aggregate_homepage_rollups(
            conn,
            'test_league',
            changed_years={2026},
        )


def test_trade_query_failure_is_not_an_empty_highlight(homepage_chain):
    from multi_league.transformations.aggregation.homepage_summary import _compute_best_trade

    # A real SQL binder failure, including on an initially empty league, must
    # propagate rather than masquerade as a successful empty calculation.
    homepage_chain.execute('ALTER TABLE public.transactions DROP COLUMN transaction_id')
    with pytest.raises(RuntimeError, match='trade'):
        _compute_best_trade(homepage_chain, 'test_league', year=2026, platform='sleeper')


def test_weekly_publication_rebuilds_homepage_on_same_full_chain(merged_chain, tmp_path):
    conn = merged_chain
    conn.execute("""
        INSERT INTO public.matchup
            (db_name,year,week,manager,franchise_id,team_name,team_points,
             opponent_points,win,loss,tie,is_playoffs,is_consolation,is_bye_week)
        VALUES ('test_league',2025,1,'Shared Alias','f1','Team',140,100,1,0,0,0,0,0),
               ('test_league',2026,1,'Shared Alias','f1','Team',110,120,0,1,0,0,0,0)
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    bundle, extracted = _weekly_bundle(tmp_path, homepage=True)
    assert [entry['table'] for entry in bundle.manifest['tables']] == ['matchup_season']
    server = _fleet_server()
    registry = canonical_table_registry()
    server.validate_fleet_manifest_shape(
        bundle.manifest, allowed_tables=set(registry),
        identity_keys={t: tuple(s['primary_keys']) for t, s in registry.items()},
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", pd.errors.PerformanceWarning)
        receipt = server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert receipt['homepage_rollups']['test_league']['homepage_manager_profiles'] == 1
    assert not [warning for warning in caught if warning.category is pd.errors.PerformanceWarning]
    assert conn.execute("SELECT highest_score_points FROM public.homepage_league_summary WHERE db_name='test_league'").fetchone() == (140.0,)
    assert conn.execute("SELECT manager,seasons,wins,losses FROM public.homepage_manager_rankings WHERE db_name='test_league'").fetchone() == ('Shared Alias',2,1,1)


def test_homepage_failure_rolls_back_careers_and_partition_changes(merged_chain, tmp_path):
    conn = merged_chain
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    bundle, extracted = _weekly_bundle(tmp_path, homepage=True)
    conn.execute('DROP TABLE public.league_context')
    before = conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall()
    server = _fleet_server()
    with pytest.raises(server.FleetValidationError, match='league_context'):
        server.apply_fleet_merge(conn, bundle.manifest, extracted)
    assert conn.execute("SELECT * FROM public.matchup_season ORDER BY year").fetchall() == before
    assert conn.execute("SELECT COUNT(*) FROM public.matchup_career WHERE db_name='test_league'").fetchone() == (0,)
    assert server.current_generations(conn, ['test_league']) == {'test_league': 0}


def test_scoped_season_rollups_do_not_rewrite_identity_outside_selected_year(homepage_chain):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.league_context
            (db_name,manager_name_overrides_json,franchise_merges_json)
        VALUES ('test_league','{"Shared Alias":"Preferred Alias"}',
                '[{"from_franchise_id":"f1","into_franchise_id":"canonical","name":"Preferred Alias"}]')
    """)
    before = conn.execute("""
        SELECT * EXCLUDE (position_season_rank, position_alltime_rank)
        FROM public.player_fantasy WHERE db_name='test_league' AND year=2025
    """).fetchall()

    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league', season_years={2026})

    assert conn.execute("""
        SELECT manager,franchise_id FROM public.player_fantasy
        WHERE db_name='test_league' AND year=2026
    """).fetchall() == [('Preferred Alias', 'canonical')]
    assert conn.execute("""
        SELECT * EXCLUDE (position_season_rank, position_alltime_rank)
        FROM public.player_fantasy WHERE db_name='test_league' AND year=2025
    """).fetchall() == before


def test_scoped_matchup_rollup_clears_removed_season_without_erasing_other_years(homepage_chain):
    conn = homepage_chain
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    before = conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall()
    conn.execute("DELETE FROM public.matchup WHERE db_name='test_league' AND year=2026")

    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league', season_years={2026})

    assert conn.execute("SELECT COUNT(*) FROM public.matchup_season WHERE db_name='test_league' AND year=2026").fetchone() == (0,)
    assert conn.execute("SELECT * FROM public.matchup_season WHERE year=2025").fetchall() == before


@pytest.mark.parametrize('table', aggregation_utils.DAMAGED_DERIVED_SEASON_ROLLUP_TABLES[:2])
def test_retained_season_coverage_rejects_missing_keys_not_just_empty_tables(homepage_chain, table):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.draft (db_name,year,manager,franchise_id,player,manager_lamar)
        VALUES ('test_league',2025,'Shared Alias','f1','Player',5),
               ('test_league',2026,'Shared Alias','f1','Player',8)
    """)
    conn.execute("""
        INSERT INTO public.transactions
            (db_name,year,week,transaction_id,transaction_type,manager,franchise_id,player)
        VALUES ('test_league',2025,1,'old','add','Shared Alias','f1','Player'),
               ('test_league',2026,1,'new','add','Shared Alias','f1','Player')
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    key = 'NFL_player_id' if table.startswith('player_fantasy') else 'franchise_id'
    # Keep the row count and season present, but lose the actual required key.
    conn.execute(f"UPDATE public.{table} SET {key}='unrelated' WHERE db_name='test_league' AND year=2025")
    before = conn.execute(f"SELECT * FROM public.{table} WHERE db_name='test_league' ORDER BY year").fetchall()

    with pytest.raises(RuntimeError, match=f'{table}.*2025'):
        aggregation_utils.assert_retained_season_rollup_coverage(conn, 'test_league', season_years={2026})

    assert conn.execute(f"SELECT * FROM public.{table} WHERE db_name='test_league' ORDER BY year").fetchall() == before


@pytest.mark.parametrize('table', aggregation_utils.DAMAGED_DERIVED_SEASON_ROLLUP_TABLES[:2])
def test_missing_retained_aggregate_years_are_discovered_and_rebuilt_narrowly(homepage_chain, table):
    conn = homepage_chain
    conn.execute("""
        INSERT INTO public.draft (db_name,year,manager,franchise_id,player,manager_lamar)
        VALUES ('test_league',2025,'Shared Alias','f1','Player',5),
               ('test_league',2026,'Shared Alias','f1','Player',8)
    """)
    conn.execute("""
        INSERT INTO public.transactions
            (db_name,year,week,transaction_id,transaction_type,manager,franchise_id,player)
        VALUES ('test_league',2025,1,'old','add','Shared Alias','f1','Player'),
               ('test_league',2026,1,'new','add','Shared Alias','f1','Player')
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    source_before = conn.execute(
        "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025"
    ).fetchall()
    conn.execute(f"DELETE FROM public.{table} WHERE db_name='test_league' AND year=2025")

    missing = aggregation_utils.find_missing_retained_season_rollup_years(
        conn, 'test_league', season_years={2026},
    )
    assert missing[table] == {2025}
    aggregation_utils.aggregate_complete_chain_season_rollups(
        conn,
        'test_league',
        season_years={2026},
        repair_years_by_table=missing,
    )
    aggregation_utils.assert_retained_season_rollup_coverage(
        conn, 'test_league', season_years={2026},
    )

    assert conn.execute(
        f"SELECT COUNT(*) FROM public.{table} WHERE db_name='test_league' AND year=2025"
    ).fetchone()[0] > 0
    assert conn.execute(
        "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025"
    ).fetchall() == source_before


def test_missing_retained_standings_year_is_discovered_and_rebuilt_narrowly(homepage_chain):
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    conn = homepage_chain
    aggregate_standings(conn, 'test_league', [2025, 2026])
    source_before = conn.execute(
        "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025"
    ).fetchall()
    standings_2026 = conn.execute(
        "SELECT * FROM public.standings_by_year WHERE db_name='test_league' AND year=2026"
    ).fetchall()
    conn.execute(
        "DELETE FROM public.standings_by_year WHERE db_name='test_league' AND year=2025"
    )

    missing = aggregation_utils.find_missing_retained_season_rollup_years(
        conn, 'test_league', season_years={2026},
    )
    assert missing['standings_by_year'] == {2025}
    aggregation_utils.aggregate_complete_chain_season_rollups(
        conn,
        'test_league',
        season_years={2026},
        repair_years_by_table=missing,
    )
    aggregation_utils.assert_retained_season_rollup_coverage(
        conn, 'test_league', season_years={2026},
    )

    assert conn.execute(
        "SELECT COUNT(*) FROM public.standings_by_year "
        "WHERE db_name='test_league' AND year=2025"
    ).fetchone()[0] > 0
    assert conn.execute(
        "SELECT * FROM public.standings_by_year WHERE db_name='test_league' AND year=2026"
    ).fetchall() == standings_2026
    assert conn.execute(
        "SELECT * FROM public.matchup WHERE db_name='test_league' AND year=2025"
    ).fetchall() == source_before


def test_retained_coverage_does_not_scan_unaffected_draft_aggregates(homepage_chain):
    conn = homepage_chain
    conn.execute('ALTER TABLE public.draft DROP COLUMN draft_category')
    conn.execute("""
        INSERT INTO public.draft (db_name,year,manager,franchise_id,player,manager_lamar)
        VALUES ('test_league',2025,'Shared Alias','f1','Player',5)
    """)
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    aggregation_utils.assert_retained_season_rollup_coverage(conn, 'test_league', season_years={2026})
    assert conn.execute("SELECT picks,draft_category FROM public.draft_manager_season WHERE db_name='test_league'").fetchall() == [(1, 'standard')]


def test_retained_standings_coverage_ignores_blank_manager_franchises(homepage_chain):
    conn = homepage_chain
    conn.execute("UPDATE public.matchup SET manager='' WHERE db_name='test_league' AND year=2025")
    aggregation_utils.aggregate_complete_chain_season_rollups(conn, 'test_league')
    conn.execute("DELETE FROM public.standings_by_year WHERE db_name='test_league' AND year=2025")
    aggregation_utils.assert_retained_season_rollup_coverage(
        conn, 'test_league', season_years={2026},
    )
