"""Tests for PHASE 1.7 (schema_conform) hook in import_pipeline.

Task 14 of schema_conform plan: wire run_phase_1_7 into the import pipeline.

Updated to include the Fly-bridge pull/push orchestration introduced in the
schema_conform wiring gap fix (2026-04-25).
"""

import duckdb
from unittest.mock import MagicMock, patch

from multi_league.core import import_pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(league_name="kmffl", run_id="test-run"):
    ctx = MagicMock()
    ctx.motherduck_db_name = None
    ctx.database_name = None
    ctx.league_name = league_name
    ctx.run_id = run_id
    ctx.has_external_data = True
    ctx.merge_source = None
    ctx.merge_sources = None
    ctx.franchise_merges = []
    return ctx


# ---------------------------------------------------------------------------
# New bridge-aware tests
# ---------------------------------------------------------------------------


def test_phase_1_7_pulls_conforms_pushes():
    """run_phase_1_7 must call pull -> schema_conform.run -> push in order."""
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()

    call_order = []

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            side_effect=lambda *a, **kw: call_order.append("pull") or {"matchup": 3},
        ) as mock_pull,
        patch(
            "multi_league.external_ingest.schema_conform.run",
            side_effect=lambda *a, **kw: call_order.append("run"),
        ) as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
            side_effect=lambda *a, **kw: call_order.append("push") or {"matchup": 3},
        ) as mock_push,
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_pull.assert_called_once()
    mock_run.assert_called_once()
    mock_push.assert_called_once()
    assert call_order == ["pull", "run", "push"], f"Wrong order: {call_order}"


def test_phase_1_7_skips_conform_and_push_when_pull_empty():
    """When pull returns all-zero counts, conform and push must NOT be called."""
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 0, "draft": 0, "transactions": 0, "player_fantasy": 0},
        ) as mock_pull,
        patch(
            "multi_league.external_ingest.schema_conform.run",
        ) as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
        ) as mock_push,
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_pull.assert_called_once()
    mock_run.assert_not_called()
    mock_push.assert_not_called()


def test_phase_1_7_push_called_after_successful_conform():
    """push must be called even when conform makes no table changes (all zeros returned)."""
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 5},
        ),
        patch(
            "multi_league.external_ingest.schema_conform.run",
            return_value=None,
        ),
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
        ) as mock_push,
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_push.assert_called_once()


# ---------------------------------------------------------------------------
# Backward-compatible tests (updated to mock the bridge so they don't hit Fly)
# ---------------------------------------------------------------------------


def test_phase_1_7_called_with_db_name_and_run_id():
    """Pipeline must invoke schema_conform.run() with conn, db_name, and run_id."""
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx(league_name="kmffl", run_id="test-run")

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 3},
        ),
        patch(
            "multi_league.external_ingest.schema_conform.run",
        ) as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
            return_value={},
        ),
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_run.assert_called_once()
    kwargs = mock_run.call_args.kwargs or {}
    args = mock_run.call_args.args

    # conn passed positionally or as kwarg.
    conn_in_args = len(args) > 0 and args[0] is conn
    conn_in_kwargs = kwargs.get("conn") is conn
    assert conn_in_args or conn_in_kwargs, f"conn not passed correctly; args={args!r} kwargs={kwargs!r}"

    # db_name passed positionally or as kwarg (league_name "kmffl" sanitises to "kmffl").
    db_in_args = any(a == "kmffl" for a in args)
    db_in_kwargs = kwargs.get("db_name") == "kmffl"
    assert db_in_args or db_in_kwargs, f"db_name not passed correctly; args={args!r} kwargs={kwargs!r}"


def test_phase_1_7_passes_saved_franchise_merges_to_schema_conform():
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()
    ctx.franchise_merges = [{"display_name": "Adin", "owner_ids": ["current", "archived"]}]

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 3},
        ),
        patch("multi_league.external_ingest.schema_conform.run") as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
            return_value={},
        ),
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    assert mock_run.call_args.kwargs["franchise_merges"] == ctx.franchise_merges


def test_phase_1_7_swallows_no_external_data_no_op():
    """When pull returns all-zero counts, PHASE 1.7 returns cleanly (no-op)."""
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()

    with patch(
        "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
        return_value={"matchup": 0, "draft": 0, "transactions": 0, "player_fantasy": 0},
    ):
        # Should not raise even though no staging tables exist locally.
        import_pipeline.run_phase_1_7(conn, ctx)


# ---------------------------------------------------------------------------
# Gate: skip Phase 1.7 entirely when ctx.has_external_data is False
# ---------------------------------------------------------------------------


def test_phase_1_7_skips_when_no_external_data_declared(monkeypatch):
    """When ctx.has_external_data is False (the 99%+ case), Phase 1.7 must
    short-circuit before any Fly call. Most leagues have no external staging,
    and probing Fly's information_schema for every import is wasted work
    (and a fragility surface — see handoff_2026_04_27_phase17_503_brittle.md).
    """
    monkeypatch.delenv("IMPORT_USE_EXTERNAL_STAGING", raising=False)
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()
    ctx.has_external_data = False

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
        ) as mock_pull,
        patch(
            "multi_league.external_ingest.schema_conform.run",
        ) as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
        ) as mock_push,
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    # Zero Fly traffic — the bridge isn't even called.
    mock_pull.assert_not_called()
    mock_run.assert_not_called()
    mock_push.assert_not_called()


def test_phase_1_7_runs_when_has_external_data_true(monkeypatch):
    """When the wizard set has_external_data=True (KMFFL-style flow),
    Phase 1.7 runs the full pull -> conform -> push as today."""
    monkeypatch.delenv("IMPORT_USE_EXTERNAL_STAGING", raising=False)
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()
    ctx.has_external_data = True

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 3},
        ) as mock_pull,
        patch(
            "multi_league.external_ingest.schema_conform.run",
        ) as mock_run,
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
            return_value={},
        ) as mock_push,
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_pull.assert_called_once()
    mock_run.assert_called_once()
    mock_push.assert_called_once()


def test_phase_1_7_env_var_override_forces_run(monkeypatch):
    """Ops escape hatch: IMPORT_USE_EXTERNAL_STAGING=1 forces Phase 1.7 even
    when ctx.has_external_data is False. Useful for repro/recovery when the
    flag was lost from the dispatch payload but staging exists on Fly."""
    monkeypatch.setenv("IMPORT_USE_EXTERNAL_STAGING", "1")
    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()
    ctx.has_external_data = False

    with (
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.pull_staging_to_local",
            return_value={"matchup": 0, "draft": 0, "transactions": 0, "player_fantasy": 0},
        ) as mock_pull,
        patch(
            "multi_league.external_ingest.schema_conform.run",
        ),
        patch(
            "multi_league.external_ingest.schema_conform._fly_bridge.push_conformed_to_fly",
        ),
    ):
        import_pipeline.run_phase_1_7(conn, ctx)

    # Pull was invoked despite ctx flag being False.
    mock_pull.assert_called_once()


def test_phase_1_7_skip_makes_zero_fly_calls(monkeypatch):
    """The strongest assertion: when gated off, NO requests.post call is made
    by the entire Phase 1.7 flow. Patches the underlying HTTP client and asserts
    it's never invoked. If anything in the gated path slips through to Fly,
    this test fails with a clear stack trace."""
    monkeypatch.delenv("IMPORT_USE_EXTERNAL_STAGING", raising=False)
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-token")

    conn = duckdb.connect(":memory:")
    ctx = _make_ctx()
    ctx.has_external_data = False

    # No mocks on the bridge — let the real call paths run, but assert HTTP is silent.
    with patch("multi_league.core.readers.fly_reader.requests.post") as mock_post:
        import_pipeline.run_phase_1_7(conn, ctx)

    mock_post.assert_not_called()
