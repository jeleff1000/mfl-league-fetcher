from types import SimpleNamespace

import duckdb

from multi_league.core import import_utils


def test_drop_league_tables_treats_missing_database_as_first_import(monkeypatch, tmp_path):
    messages = []

    ctx = SimpleNamespace(
        league_id="1312071192336666624",
        league_name="The League Of Record III",
        data_directory=tmp_path,
    )

    monkeypatch.setattr(import_utils, "log", messages.append)

    dropped = import_utils.drop_league_tables(ctx, "the_league_of_record_iii", platform="sleeper")

    # Fresh starts are row-scoped in the Fly merge path, so first imports
    # have no local destructive cleanup to do.
    assert dropped == 0


def test_upload_league_settings_returns_true_when_settings_dir_exists(monkeypatch, tmp_path):
    # `upload_league_settings_to_database` is now a thin delegator —
    # the actual upload happens via FlyTarget during the table-merge step.
    # Behavior contract: the function returns True iff data_directory has a
    # `league_settings/` folder, otherwise False.
    messages = []
    settings_dir = tmp_path / "league_settings"
    settings_dir.mkdir()

    ctx = SimpleNamespace(data_directory=tmp_path)
    monkeypatch.setattr(import_utils, "log", messages.append)

    assert import_utils.upload_league_settings_to_database(ctx) is True
    assert any("Settings uploaded via FlyTarget merge" in m for m in messages)


def test_upload_league_settings_returns_false_when_settings_dir_missing(monkeypatch, tmp_path):
    messages = []
    ctx = SimpleNamespace(data_directory=tmp_path)
    monkeypatch.setattr(import_utils, "log", messages.append)

    assert import_utils.upload_league_settings_to_database(ctx) is False
    assert any("No league_settings directory found" in m for m in messages)


def test_track_1_verify_reads_local_ops_cache(monkeypatch, tmp_path):
    messages = []
    ops_cache = tmp_path / "ops_cache.duckdb"
    conn = duckdb.connect(str(ops_cache))
    conn.execute("CREATE SCHEMA nfl_historical")
    conn.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (year INTEGER)")
    conn.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES (2024), (2024), (2025)")
    conn.close()

    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_cache))
    monkeypatch.setattr(import_utils, "log", messages.append)

    assert import_utils.run_track_1_verify(2024, 2025) is True
    assert any("local ops cache" in message for message in messages)


def test_persist_frontend_settings_writes_flat_keeper_config(tmp_path):
    from multi_league.core.local_db import LocalLeagueDB

    ctx = SimpleNamespace(
        platform="sleeper",
        league_id="123",
        league_name="Keeper League",
        league_ids={"2025": "122", "2026": "123"},
        data_directory=tmp_path,
        is_private=True,
        franchise_merges=[{"display_name": "Greg", "owner_ids": ["owner_new", "owner_old"]}],
        keeper_rules={
            "enabled": True,
            "draft_type": "snake",
            "max_keepers": 3,
            "base_cost_rules": {
                "drafted": {"round_offset": -1},
                "fa_pickup": {"source": "last"},
            },
        },
    )

    with LocalLeagueDB(tmp_path, "keeper_league") as db:
        import_utils.persist_frontend_settings_tables(db, "keeper_league", ctx, "sleeper")
        cols = {row[1] for row in db.connect().execute("PRAGMA table_info('public.keeper_config')").fetchall()}
        row = (
            db.connect()
            .execute(
                """
            SELECT enabled, draft_type, max_keepers, snake_drafted_round_offset, snake_fa_pickup_source
            FROM public.keeper_config
            WHERE db_name = 'keeper_league' AND year = 0
            """
            )
            .fetchone()
        )
        context_row = (
            db.connect()
            .execute(
                """
            SELECT is_private, franchise_merges_json, league_ids_json
            FROM public.league_context
            WHERE db_name = 'keeper_league'
            """
            )
            .fetchone()
        )

    assert "rules_json" not in cols
    assert row == (True, "snake", 3, -1, "last")
    assert context_row == (
        True,
        '[{"display_name": "Greg", "owner_ids": ["owner_new", "owner_old"]}]',
        '{"2025": "122", "2026": "123"}',
    )


def test_compute_fetch_years_skips_local_complete_years():
    messages = []

    years = import_utils.compute_fetch_years(
        all_years=[2024, 2025],
        external_years=set(),
        remote_skip_years=[],
        local_skip_years=[2025],
        fetcher_name="Rosters",
        log_func=messages.append,
    )

    assert years == [2024]
    assert any("already complete in local DB" in message for message in messages)
