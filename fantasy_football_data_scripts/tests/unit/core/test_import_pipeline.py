from pathlib import Path
from types import SimpleNamespace

import pytest


def test_weekly_quick_graph_does_not_rewrite_saved_frontend_context(monkeypatch, tmp_path):
    from multi_league.core import import_pipeline, local_db

    persisted: list[str] = []

    class Local:
        def __init__(self, *_args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(local_db, "LocalLeagueDB", Local)
    monkeypatch.setattr(
        import_pipeline,
        "persist_frontend_settings_tables",
        lambda *_args: persisted.append("rewritten"),
    )
    monkeypatch.setattr(import_pipeline, "run_transformations", lambda *_args, **_kwargs: [])
    ctx = SimpleNamespace(data_directory=tmp_path, league_name="AFI Data")

    assert import_pipeline.run_transformation_pipeline(
        ctx, platform="espn", db_name="afi_data", data_dir=tmp_path,
        import_mode="quick", quick=True, skip_track_2_upload=True,
        context_file_path=tmp_path / "espn_context.json",
        preserve_frontend_settings=True,
    ) == []
    assert persisted == []

    import_pipeline.run_transformation_pipeline(
        ctx, platform="espn", db_name="afi_data", data_dir=tmp_path,
        import_mode="quick", quick=True, skip_track_2_upload=True,
        context_file_path=tmp_path / "espn_context.json",
    )
    assert persisted == ["rewritten"]


def test_require_sql_enrichment_success_blocks_partial_uploads():
    from multi_league.core.import_pipeline import require_sql_enrichment_success

    with pytest.raises(RuntimeError, match="resolve_all_nfl_player_ids"):
        require_sql_enrichment_success(
            {
                "resolve_all_nfl_player_ids": ("error", 'Catalog "___ops" does not exist'),
                "dedup_player_fantasy": 12,
            }
        )


def test_require_sql_enrichment_success_blocks_failed_aggregation():
    from multi_league.core.import_pipeline import require_sql_enrichment_success

    with pytest.raises(RuntimeError, match="Fantasy aggregation"):
        require_sql_enrichment_success({}, fantasy_aggregation_ok=False)


def test_require_sql_enrichment_success_accepts_complete_results():
    from multi_league.core.import_pipeline import require_sql_enrichment_success

    require_sql_enrichment_success(
        {"resolve_all_nfl_player_ids": 100, "optional_step": None},
        fantasy_aggregation_ok=True,
    )


def test_run_local_fantasy_aggregation_uses_existing_connection(monkeypatch):
    from multi_league.core import import_pipeline

    fake_conn = object()
    seen: list[object] = []

    def _fail_run_script(*args, **kwargs):
        raise AssertionError("run_script should not be used when an open DuckDB connection is supplied")

    def _capture_run_aggregation(conn, db_name=None):
        seen.append((conn, db_name))

    monkeypatch.setattr(import_pipeline, "run_script", _fail_run_script)
    monkeypatch.setattr(
        "multi_league.transformations.aggregation.aggregate_fantasy_context.run_aggregation",
        _capture_run_aggregation,
    )

    ok = import_pipeline.run_local_fantasy_aggregation(
        db_name="demo_league",
        data_dir=Path("C:/tmp/demo"),
        conn=fake_conn,
    )

    assert ok is True
    assert seen == [(fake_conn, "demo_league")]


def test_run_sql_enrichments_forwards_saved_manager_identity_settings(monkeypatch):
    from multi_league.core import import_pipeline

    captured = {}

    class FakeEnricher:
        last_run_timings = {}

        def __init__(self, db_name, **kwargs):
            captured["db_name"] = db_name
            captured.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def load_settings_from_db(self):
            return {}, {}

        def run_all(self):
            return {"resolve_all_nfl_player_ids": 0}

    monkeypatch.setattr(
        "multi_league.transformations.sql_enrichments.SQLEnrichments",
        FakeEnricher,
    )
    ctx = SimpleNamespace(
        manager_name_overrides={"Jeff - Last Place": "Jeff"},
        franchise_merges=[{"owner_ids": ["jeff-2018", "jeff-2015"]}],
    )

    assert import_pipeline.run_sql_enrichments(ctx, "monsters_of_the_midway") is True
    assert captured["manager_name_overrides"] == {"Jeff - Last Place": "Jeff"}
    assert captured["franchise_merges"] == [{"owner_ids": ["jeff-2018", "jeff-2015"]}]


def test_phase_1_7_skips_schema_conform_for_merge_source(monkeypatch):
    from multi_league.core import import_pipeline

    called = False

    def _fail_pull(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("worker-side merge_source copy should not be schema-conformed")

    monkeypatch.setattr(
        "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
        _fail_pull,
    )

    ctx = SimpleNamespace(
        league_name="TDs & Beer XVII",
        motherduck_db_name="tds_beer_xvii",
        has_external_data=True,
        merge_source={"source_db": "td_s_beer"},
        run_id="test-run",
    )

    import_pipeline.run_phase_1_7(conn=object(), ctx=ctx)

    assert called is False


def test_phase_1_7_skips_schema_conform_for_merge_sources(monkeypatch):
    from multi_league.core import import_pipeline

    called = False

    def _fail_pull(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("worker-side merge_sources copy should not be schema-conformed")

    monkeypatch.setattr(
        "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
        _fail_pull,
    )

    ctx = SimpleNamespace(
        league_name="Fantasy Elite",
        motherduck_db_name="fantasy_elite",
        has_external_data=True,
        merge_sources=[{"source_db": "fantasy_elite_yahoo"}],
        run_id="test-run",
    )

    import_pipeline.run_phase_1_7(conn=object(), ctx=ctx)

    assert called is False
