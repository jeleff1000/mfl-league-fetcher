"""Quick imports replace explicit seasons, never a league's full chain."""

import json

import duckdb
import pytest

from multi_league.core.delta_publish import build_delta_bundle
from multi_league.core.local_db import LocalLeagueDB
import multi_league.core.targets.fly_target as fly_target


@pytest.fixture
def conn():
    with duckdb.connect(':memory:') as connection:
        connection.execute('CREATE SCHEMA public')
        connection.execute("""
            CREATE TABLE public.matchup AS
            SELECT 'league' AS db_name, 2026 AS year, 1 AS week,
                   'f1_2026_1' AS manager_week, 'Saved Alias' AS manager
        """)
        yield connection


@pytest.mark.parametrize('year', [2019, 2026])
def test_quick_bundle_scopes_facts_and_only_initializes_configuration(conn, tmp_path, year):
    conn.execute('UPDATE public.matchup SET year=?', [year])
    conn.execute("CREATE TABLE public.keeper_config AS SELECT 'league' AS db_name, 0 AS year")
    conn.execute("CREATE TABLE public.league_context AS SELECT 'league' AS db_name, '{}' AS manager_name_overrides_json")
    conn.execute("CREATE TABLE public.matchup_career AS SELECT 'league' AS db_name, 'f1' AS franchise_id, 1 AS games")
    conn.execute("CREATE TABLE public.homepage_league_summary AS SELECT 'league' AS db_name, 1 AS total_seasons")
    bundle = build_delta_bundle(conn, db_name='league', import_mode='quick',
                                base_generation=7, output_dir=tmp_path)
    assert bundle.manifest['schema_version'] == 'fleet-partition-v3'
    assert bundle.manifest['league_generations'] == {'league': 7}
    assert bundle.manifest['active_year'] == year
    assert bundle.manifest['quick_years'] == [year]
    assert [(t['table'], t['merge_mode'], t['scope']) for t in bundle.manifest['tables']] == [
        ('keeper_config', 'initialize_missing', {}),
        ('league_context', 'initialize_missing', {}),
        ('matchup', 'replace_scope', {'years': [year]})]
    # Publication must not mutate the local user configuration either.
    assert conn.execute('SELECT year FROM public.keeper_config').fetchall() == [(0,)]


def test_quick_requires_pre_fetch_generation(conn, tmp_path):
    with pytest.raises(ValueError, match='generation'):
        build_delta_bundle(conn, db_name='league', import_mode='quick', output_dir=tmp_path)


@pytest.mark.parametrize('year', [None, 0])
def test_quick_rejects_ambiguous_or_null_year(conn, tmp_path, year):
    conn.execute("INSERT INTO public.matchup VALUES ('league', ?, 2, 'f1_other', 'Saved Alias')", [year])
    with pytest.raises(ValueError, match='year'):
        build_delta_bundle(conn, db_name='league', import_mode='quick',
                           base_generation=7, output_dir=tmp_path)


def test_quick_yahoo_years_use_per_table_scope(conn, tmp_path):
    conn.execute("INSERT INTO public.matchup VALUES ('league', 2025, 1, 'f1_2025_1', 'Saved Alias')")
    conn.execute("CREATE TABLE public.league_settings AS SELECT 'league' AS db_name, 2026 AS year")
    bundle = build_delta_bundle(conn, db_name='league', import_mode='quick',
                                base_generation=7, output_dir=tmp_path)
    assert bundle.manifest['quick_years'] == [2025, 2026]
    assert {t['table']: t['scope'] for t in bundle.manifest['tables']} == {
        'matchup': {'years': [2025, 2026]}, 'league_settings': {'years': [2026]}}


def test_quick_rejects_three_years(conn, tmp_path):
    conn.execute("INSERT INTO public.matchup VALUES ('league', 2024, 1, 'old', 'Saved Alias'), ('league', 2025, 1, 'mid', 'Saved Alias')")
    with pytest.raises(ValueError, match='years'):
        build_delta_bundle(conn, db_name='league', import_mode='quick',
                           base_generation=7, output_dir=tmp_path)


def test_quick_rejects_another_league_in_worker_data(conn, tmp_path):
    conn.execute("INSERT INTO public.matchup VALUES ('other', 2026, 1, 'f2_2026_1', 'Other')")
    with pytest.raises(ValueError, match='league'):
        build_delta_bundle(conn, db_name='league', import_mode='quick',
                           base_generation=7, output_dir=tmp_path)


@pytest.mark.parametrize('publish_format', ['delta', 'duckdb'])
def test_quick_never_falls_back_to_whole_database(monkeypatch, tmp_path, publish_format):
    class UnavailableScopedTarget:
        def merge_fleet_partition(self, *args, **kwargs):
            raise RuntimeError('404 scoped endpoint unavailable')

        def merge_league_delta(self, *args, **kwargs):
            pytest.fail('quick import reached whole-league delta endpoint')

        def merge_league(self, *args, **kwargs):
            pytest.fail('quick import reached whole-database fallback')

    monkeypatch.setattr(fly_target, 'FlyTarget', UnavailableScopedTarget)
    monkeypatch.setenv('FLY_PUBLISH_FORMAT', publish_format)
    monkeypatch.setenv('FLY_DELTA_FALLBACK_TO_DUCKDB', '1')
    monkeypatch.setenv('LEAGUE_IMPORT_BASE_GENERATION', '7')
    with LocalLeagueDB(tmp_path, 'league') as db:
        db.connect().execute("""
            CREATE TABLE public.matchup AS
            SELECT 'league' AS db_name, 2026 AS year, 1 AS week,
                   'f1_2026_1' AS manager_week
        """)
        with pytest.raises(RuntimeError, match='404 scoped endpoint unavailable'):
            db.upload_to_fly('league', import_mode=' QUICK ', finalize_inventory=False,
                             finalize_merge_source=False)
        manifest = json.loads((tmp_path / 'delta_publish_manifest.json').read_text())
        assert manifest['league_generations'] == {'league': 7}


@pytest.mark.parametrize('receipt', [None, {'status': 'VALIDATED'}, {'status': 'STALE_SKIPPED'}])
def test_quick_requires_commit_before_finalization(monkeypatch, tmp_path, receipt):
    finalized = []

    class PendingTarget:
        def merge_fleet_partition(self, *args, **kwargs):
            return receipt

        def mark_league_imported(self, *args, **kwargs):
            finalized.append(True)

    monkeypatch.setattr(fly_target, 'FlyTarget', PendingTarget)
    monkeypatch.setenv('LEAGUE_IMPORT_BASE_GENERATION', '7')
    monkeypatch.setenv('IMPORT_MODE', 'quick')
    with LocalLeagueDB(tmp_path, 'league') as db:
        db.connect().execute("""
            CREATE TABLE public.matchup AS
            SELECT 'league' AS db_name, 2026 AS year, 1 AS week,
                   'f1_2026_1' AS manager_week
        """)
        with pytest.raises(RuntimeError, match='COMMITTED receipt'):
            db.upload_to_fly('league', finalize_inventory=True, finalize_merge_source=False)
        assert finalized == []
