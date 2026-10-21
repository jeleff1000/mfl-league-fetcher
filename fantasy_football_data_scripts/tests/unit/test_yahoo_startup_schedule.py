"""A pre-draft schedule is not evidence of a played fantasy season."""
from types import SimpleNamespace
import sys

import duckdb
import pytest

import initial_import_v3
from multi_league.validation.checks.pipeline_checks import check_transform_output_non_empty

SETTINGS = {2026: {"metadata": {"draft_status": "predraft", "current_week": "7"}}}


@pytest.fixture
def local_shell():
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.league_settings AS SELECT 2026 AS year")
        conn.execute("CREATE TABLE public.matchup (year INTEGER, week INTEGER)")
        conn.execute("CREATE TABLE public.player_fantasy (year INTEGER, week INTEGER)")
        conn.execute("CREATE TABLE public.schedule AS SELECT 2026 AS year, w AS week, "
                     "team AS team_key, 0.0 AS team_points, 0.0 AS opponent_points "
                     "FROM range(1,15) weeks(w) CROSS JOIN range(1,13) teams(team)")
        db = SimpleNamespace(
            connect=lambda: conn,
            table_exists=lambda table: conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=?",
                [table]).fetchone()[0] > 0,
            row_count=lambda table: conn.execute(f"SELECT COUNT(*) FROM public.{table}").fetchone()[0],
        )
        ctx = SimpleNamespace(import_mode="quick", quick_import_years=[2026], start_year=2026, end_year=2026)
        yield ctx, db, conn


def test_verified_startup_with_all_168_future_rows_can_publish(local_shell):
    ctx, db, conn = local_shell
    assert initial_import_v3._allow_empty_quick_startup(ctx, db, {2026}, league_settings=SETTINGS) is True
    required = initial_import_v3._pre_upload_non_empty_tables(
        allow_empty_quick_startup=True, allow_partial_history_source=False)
    assert check_transform_output_non_empty(conn, required) == []
    assert conn.execute("SELECT COUNT(*), SUM(team_points) FROM public.schedule").fetchone() == (168, 0)
    assert db.row_count("matchup") == db.row_count("player_fantasy") == 0


@pytest.mark.parametrize("table", ["matchup", "player_fantasy"])
def test_existing_facts_cannot_use_the_empty_startup_exception(local_shell, table):
    ctx, db, conn = local_shell
    conn.execute(f"INSERT INTO public.{table} VALUES (2026, 1)")
    assert initial_import_v3._allow_empty_quick_startup(ctx, db, {2026}, league_settings=SETTINGS) is False


@pytest.mark.parametrize("column,points", [("team_points", 1), ("opponent_points", -1)])
def test_scored_schedule_cannot_mask_missing_matchups(local_shell, column, points):
    ctx, db, conn = local_shell
    conn.execute(f"UPDATE public.schedule SET {column}=? WHERE week=1 AND team_key=1", [points])
    assert initial_import_v3._allow_empty_quick_startup(ctx, db, {2026}, league_settings=SETTINGS) is False


@pytest.mark.parametrize("verified_years", [set(), {2025}])
def test_unverified_empty_data_still_fails_the_facts_gate(local_shell, verified_years):
    ctx, db, conn = local_shell
    allowed = initial_import_v3._allow_empty_quick_startup(ctx, db, verified_years, league_settings=SETTINGS)
    assert allowed is False
    required = initial_import_v3._pre_upload_non_empty_tables(
        allow_empty_quick_startup=allowed, allow_partial_history_source=False)
    assert any("matchup has 0 rows" in error for error in check_transform_output_non_empty(conn, required))


def test_full_import_cannot_bypass_missing_history_as_a_quick_shell(local_shell):
    ctx, db, _ = local_shell
    ctx.import_mode = "full"
    assert initial_import_v3._allow_empty_quick_startup(ctx, db, {2026}, league_settings=SETTINGS) is False


@pytest.mark.parametrize("metadata", [{}, {"draft_status": "postdraft", "current_week": "7"}])
def test_zero_scores_alone_do_not_prove_the_season_is_unplayed(local_shell, metadata):
    ctx, db, _ = local_shell
    assert initial_import_v3._allow_empty_quick_startup(
        ctx, db, {2026}, league_settings={2026: {"metadata": metadata}}) is False


def test_resume_reaches_validation_without_refetching_settings(tmp_path, monkeypatch):
    path = tmp_path / "context.json"
    path.write_text("{}", encoding="utf-8")
    ctx = SimpleNamespace(league_name="resume", league_id="470.l.17", league_ids={"2026": "470.l.17"},
                          start_year=2026, end_year=2026, import_mode="full", is_single_year_import=False,
                          data_directory=tmp_path, has_league_ids_mapping=lambda: True)
    db = SimpleNamespace(_conn=True, db_path=tmp_path / "resume.duckdb", connect=lambda: None,
                         ensure_table=lambda table: None, close=lambda: None)
    enricher = SimpleNamespace(conn=None, load_settings_from_db=lambda: None, run_all=lambda: {}, close=lambda: None)
    monkeypatch.setattr(initial_import_v3, "_load_ctx", lambda path: ctx)
    monkeypatch.setattr(initial_import_v3, "get_current_nfl_season_year", lambda: 2026)
    monkeypatch.setattr(initial_import_v3, "runtime_from_source", lambda *args, **kwargs:
                        SimpleNamespace(data_dir=tmp_path, db_name="resume"))
    monkeypatch.setattr(initial_import_v3, "LocalLeagueDB", lambda *args: db)
    monkeypatch.setattr(initial_import_v3, "run_transformation_pipeline", lambda *args, **kwargs: [])
    monkeypatch.setattr(initial_import_v3, "verify_unified_outputs", lambda *args, **kwargs: None)
    monkeypatch.setattr(initial_import_v3, "_create_local_sql_enricher", lambda **kwargs: enricher)
    monkeypatch.setattr(initial_import_v3, "run_local_fantasy_aggregation", lambda *args, **kwargs: True)
    class ReachedValidation(Exception): pass
    def check(*args, league_settings):
        assert league_settings == {}
        raise ReachedValidation
    monkeypatch.setattr(initial_import_v3, "_allow_empty_quick_startup", check)
    monkeypatch.setattr(sys, "argv", ["initial_import_v3.py", "--context", str(path),
                        "--start-phase", "3", "--skip-fetchers", "--skip-track-1", "--skip-track-2-upload"])
    with pytest.raises(ReachedValidation):
        initial_import_v3.main()
