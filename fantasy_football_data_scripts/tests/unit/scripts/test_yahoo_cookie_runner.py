from __future__ import annotations

import argparse
from pathlib import Path
import sys
import types

import duckdb
import pytest

from scripts import yahoo_cookie_runner
from scripts import refresh_aggregates


def test_runner_forwards_cookie_throttle_controls_to_capture(monkeypatch, tmp_path):
    captured = {}

    def capture(args):
        captured["args"] = args
        raise RuntimeError("stop after capture arguments")

    monkeypatch.setitem(
        sys.modules,
        "quick_import_kmffl_2025_web",
        types.SimpleNamespace(run_all_years=capture),
    )
    monkeypatch.setattr(yahoo_cookie_runner, "_validate_ops_cache", lambda _path: None)
    monkeypatch.setattr(yahoo_cookie_runner, "_validate_draft_global_source", lambda _path: None)

    args = argparse.Namespace(
        import_mode="full",
        start_year=2025,
        end_year=2025,
        output_dir=str(tmp_path / "output"),
        ops_cache=str(tmp_path / "ops.duckdb"),
        draft_global_source=str(tmp_path / "draft.parquet"),
        league_keys_json='{"2025":"461.l.338926"}',
        context_json=None,
        team_count=12,
        cookie_jar=str(tmp_path / "cookies.json"),
        request_delay=1.25,
        throttle_retries=2,
        year_throttle_cooldown=0.0,
        throttle_recovery_retries=3,
        throttle_recovery_cooldown=45.0,
        roster_weeks=None,
        transaction_pages=200,
        db_name="ms_gang",
        league_name="MS GANG",
    )

    with pytest.raises(RuntimeError, match="stop after capture arguments"):
        yahoo_cookie_runner.run(args)

    capture_args = captured["args"]
    assert capture_args.year_throttle_cooldown == 0.0
    assert capture_args.throttle_recovery_retries == 3
    assert capture_args.throttle_recovery_cooldown == 45.0


def test_runner_limits_quick_cookie_capture_to_the_current_season(monkeypatch, tmp_path):
    captured = {}

    def capture(args):
        captured["args"] = args
        raise RuntimeError("stop after quick capture arguments")

    monkeypatch.setitem(
        sys.modules,
        "quick_import_kmffl_2025_web",
        types.SimpleNamespace(run_all_years=capture),
    )
    monkeypatch.setattr(yahoo_cookie_runner, "_validate_ops_cache", lambda _path: None)
    monkeypatch.setattr(yahoo_cookie_runner, "_validate_draft_global_source", lambda _path: None)

    args = argparse.Namespace(
        import_mode="quick",
        start_year=2014,
        end_year=2016,
        output_dir=str(tmp_path / "output"),
        ops_cache=str(tmp_path / "ops.duckdb"),
        draft_global_source=str(tmp_path / "draft.parquet"),
        league_keys_json='{"2014":"331.l.492605","2015":"348.l.727365","2016":"359.l.272424"}',
        context_json=None,
        team_count=10,
        cookie_jar=str(tmp_path / "cookies.json"),
        request_delay=0.5,
        throttle_retries=1,
        year_throttle_cooldown=0.0,
        throttle_recovery_retries=1,
        throttle_recovery_cooldown=300.0,
        roster_weeks=None,
        transaction_pages=200,
        db_name="you_are_a_pirate",
        league_name="You Are A Pirate",
    )

    with pytest.raises(RuntimeError, match="stop after quick capture arguments"):
        yahoo_cookie_runner.run(args)

    capture_args = captured["args"]
    assert (capture_args.start_year, capture_args.end_year) == (2016, 2016)
    assert capture_args.league_keys == {
        2016: "359.l.272424",
    }


def test_runner_exposes_full_local_stage_order():
    source = Path("scripts/yahoo_cookie_runner.py").read_text(encoding="utf-8")
    source = source[source.index("def run(args") : source.index("def main()")]
    for token in (
        "run_all_years",
        "build_frames",
        "run_transformation_pipeline",
        "run_local_enrichments",
        "validation = _validate_local_db",
    ):
        assert token in source
    assert "multi_league.transformations.matchup.expected_record_v2" in source
    assert "multi_league.transformations.matchup.playoff_odds_import" in source
    assert yahoo_cookie_runner.AGGREGATION_REFRESH_SCRIPT.name == "refresh_aggregates.py"
    assert '"fly_upload": False' in source
    assert '"oauth_used": False' in source
    assert 'fantasy_football_data_scripts' in source
    assert 'PYTHONPATH' in source
    assert 'quick_import' in source
    assert 'quick=True' in source
    assert "run_local_fantasy_aggregation" in Path("scripts/build_kmffl_cookie_model.py").read_text(encoding="utf-8")


def test_cookie_workflow_publishes_quick_before_running_history():
    workflow = Path(".github/workflows/yahoo_cookie_import_worker.yml").read_text(encoding="utf-8")
    quick = workflow.index("name: Run quick Yahoo cookie track")
    quick_publish = workflow.index("name: Publish quick cookie import")
    full = workflow.index("name: Run full Yahoo cookie track")
    full_publish = workflow.index("name: Publish full cookie import")
    assert quick < quick_publish < full < full_publish
    assert '--import-mode "quick"' in workflow
    assert '--import-mode "full"' in workflow


def test_cookie_workflow_finishes_shared_post_import_before_each_publish():
    """Cookie imports must produce the same homepage aggregates as OAuth imports."""
    workflow = Path(".github/workflows/yahoo_cookie_import_worker.yml").read_text(encoding="utf-8")

    quick = workflow.index("name: Run quick Yahoo cookie track")
    quick_post_import = workflow.index("name: Run quick cookie post-import calculations")
    quick_upload = workflow.index("name: Upload quick cookie import")
    quick_publish = workflow.index("name: Publish quick cookie import")
    full = workflow.index("name: Run full Yahoo cookie track")
    full_post_import = workflow.index("name: Run full cookie post-import calculations")
    full_upload = workflow.index("name: Upload full cookie import")
    full_publish = workflow.index("name: Publish full cookie import")

    assert quick < quick_post_import < quick_upload < quick_publish < full
    assert full < full_post_import < full_upload < full_publish
    assert workflow.count("--skip-upload") == 2
    assert workflow.count("multi_league.transformations.matchup.expected_record_v2") == 2
    assert workflow.count("multi_league.transformations.matchup.playoff_odds_import") == 2
    assert workflow.count("scripts/refresh_aggregates.py") == 2


def test_cookie_workflow_can_replay_a_saved_page_cache_without_yahoo_requests():
    """A captured source cache must be recoverable after its cookie expires."""
    workflow = Path(".github/workflows/yahoo_cookie_import_worker.yml").read_text(encoding="utf-8")

    assert "cache_only:" in workflow
    assert 'type: boolean' in workflow
    assert "COOKIE_BACKUP_CACHE_ONLY:" in workflow
    assert "inputs.cache_only" in workflow
    assert "yahoo-cookie-pages-v4-${{ inputs.database_name }}" in workflow


def test_local_aggregate_refresh_does_not_require_motherduck_token(monkeypatch, tmp_path):
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.delenv("motherduck_token", raising=False)
    monkeypatch.delenv("DATABASE_BACKEND", raising=False)

    # A local data directory is the runner's explicit local DuckDB target.
    # It must never be treated as a legacy MotherDuck refresh.
    refresh_aggregates._require_token(data_dir=str(tmp_path))


def test_cookie_local_playoff_stage_has_no_motherduck_status_labels():
    source = Path(
        "fantasy_football_data_scripts/multi_league/transformations/matchup/playoff_odds_import.py"
    ).read_text(encoding="utf-8")
    assert "Loading data from MotherDuck" not in source
    assert "Saving results to MotherDuck" not in source
    assert "Loading data from local DuckDB" in source
    assert "Saving results to local DuckDB" in source


def test_full_cookie_stage_discards_only_the_prior_local_quick_model(tmp_path):
    model = tmp_path / "you_are_a_pirate.duckdb"
    source = tmp_path / "you_are_a_pirate_2014_2016_cookie_backup.duckdb"
    model.write_text("quick model", encoding="utf-8")
    source.write_text("raw capture", encoding="utf-8")

    yahoo_cookie_runner._discard_prior_local_model(tmp_path, "you_are_a_pirate")

    assert not model.exists()
    assert source.read_text(encoding="utf-8") == "raw capture"


def test_validate_local_db_requires_all_canonical_tables(tmp_path):
    db_name = "cookie_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE SCHEMA public")
        for table in yahoo_cookie_runner.CANONICAL_TABLES:
            columns = ["db_name VARCHAR"]
            values = [db_name]
            if table == "matchup":
                columns += ["year INTEGER", "week INTEGER", "cumulative_week INTEGER", "win INTEGER", "loss INTEGER"]
                values += [2025, 1, 1, 1, 0]
            elif table == "player_fantasy":
                columns += ["player_week VARCHAR"]
                values += ["player_1"]
            elif table == "draft":
                columns += ["round INTEGER"]
                values += [1]
            elif table in {"transactions", "schedule"}:
                columns += ["year INTEGER", "week INTEGER", "cumulative_week INTEGER"]
                values += [2025, 1, 1]
            conn.execute(f'CREATE TABLE public."{table}" ({", ".join(columns)})')
            placeholders = ", ".join("?" for _ in values)
            conn.execute(f'INSERT INTO public."{table}" VALUES ({placeholders})', values)
    finally:
        conn.close()

    result = yahoo_cookie_runner._validate_local_db(tmp_path, db_name)
    assert result["row_counts"]["matchup"] == 1
    assert set(yahoo_cookie_runner.CANONICAL_TABLES).issubset(result["tables"])


def test_validate_ops_cache_rejects_public_only_cache(tmp_path):
    db_path = tmp_path / "ops.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE player_bio (NFL_player_id VARCHAR)")
    finally:
        conn.close()

    try:
        yahoo_cookie_runner._validate_ops_cache(db_path)
    except RuntimeError as exc:
        assert "nfl_historical.nfl_player_stats_all" in str(exc)
    else:
        raise AssertionError("incomplete ops cache was accepted")


def test_validate_draft_global_source_requires_file(tmp_path):
    try:
        yahoo_cookie_runner._validate_draft_global_source(tmp_path / "missing.parquet")
    except FileNotFoundError as exc:
        assert "draft baseline" in str(exc)
    else:
        raise AssertionError("missing draft baseline was accepted")


def test_context_overrides_preserve_oauth_options_but_block_auth_fields(tmp_path):
    import json

    context_path = tmp_path / "context.json"
    context_path.write_text(
        json.dumps(
            {
                "num_teams": 12,
                "playoff_teams": 6,
                "manager_name_overrides": {"old": "new"},
                "franchise_merges": [{"owner_ids": ["guid-a", "guid-b"]}],
                "oauth_file_path": "should-not-survive",
                "database_name": "wrong-db",
            }
        ),
        encoding="utf-8",
    )
    overrides = yahoo_cookie_runner._load_context_overrides(str(context_path))
    assert overrides["num_teams"] == 12
    assert overrides["playoff_teams"] == 6
    assert overrides["manager_name_overrides"] == {"old": "new"}
    assert "oauth_file_path" not in overrides
    assert "database_name" not in overrides


def test_context_writer_keeps_quick_and_full_contexts_separate(tmp_path):
    quick = yahoo_cookie_runner._write_local_context(
        tmp_path,
        "cookie_quick",
        2024,
        2025,
        {2024: "450.l.old", 2025: "461.l.new"},
        "KMFFL",
        import_mode="quick",
        filename="league_context_quick.json",
        context_overrides={"playoff_teams": 6},
    )
    full = yahoo_cookie_runner._write_local_context(
        tmp_path,
        "cookie_full",
        2015,
        2025,
        {2015: "348.l.old", 2025: "461.l.new"},
        "KMFFL",
        import_mode="full",
    )
    assert quick.name == "league_context_quick.json"
    assert full.name == "league_context.json"
    assert quick.exists() and full.exists()
    assert '"import_mode": "quick"' in quick.read_text(encoding="utf-8")
    assert '"import_mode": "full"' in full.read_text(encoding="utf-8")
