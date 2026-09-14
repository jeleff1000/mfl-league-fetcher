from __future__ import annotations

import json
import sys
import tarfile
from datetime import UTC, datetime as real_datetime
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import check_offseason_drafts as subject  # noqa: E402
import update_sleeper_offseason_draft as update_subject  # noqa: E402


class _September2026:
    @classmethod
    def now(cls, tz=None):
        return real_datetime(2026, 9, 7, tzinfo=tz or UTC)


def test_current_season_matchups_do_not_skip_the_current_season_draft(monkeypatch):
    """A 2026 Week 1 import must still check the available 2026 draft, not 2027."""
    monkeypatch.setattr(subject, "datetime", _September2026)
    monkeypatch.setattr(subject, "completed_season_for_db", lambda *_: 2026)

    plan = subject.build_plan_from_row(
        object(),
        {
            "db_name": "active_league",
            "platform": "sleeper",
            "league_id": "123456789",
            "league_name": "Active League",
        },
        draft_year=None,
        league_id_override=None,
    )

    assert plan.draft_year == 2026


@pytest.mark.parametrize(
    ("platform", "saved_id", "active_id"),
    [
        ("sleeper", "1257088277819691008", "1389755141288124417"),
        ("yahoo", "461.l.90939", "470.l.80971"),
    ],
)
def test_update_plan_uses_the_registered_target_year_renewal_id(monkeypatch, platform, saved_id, active_id):
    """Updates must consume the same persisted year-to-ID chain registration created."""
    monkeypatch.setattr(subject, "completed_season_for_db", lambda *_: 2025)
    plan = subject.build_plan_from_row(
        object(),
        {
            "db_name": "renewed_league",
            "platform": platform,
            "league_id": saved_id,
            "league_ids_json": json.dumps({"2025": saved_id, "2026": active_id}),
            "franchise_merges_json": json.dumps([
                {"display_name": "Marc", "owner_ids": ["owner-1", "owner-2"]}
            ]),
            "league_name": "Renewed League",
        },
        draft_year=2026,
        league_id_override=None,
    )

    assert plan.league_id == active_id
    assert plan.league_ids == {"2025": saved_id, "2026": active_id}
    assert plan.franchise_merges == [
        {"display_name": "Marc", "owner_ids": ["owner-1", "owner-2"]}
    ]


def test_yahoo_uses_the_registered_target_key_without_rediscovering_the_chain(monkeypatch):
    plan = subject.PlatformDraftPlan(
        db_name="kmffl",
        platform="yahoo",
        league_id="470.l.80971",
        league_name="KMFFL",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
        league_ids={"2025": "461.l.90939", "2026": "470.l.80971"},
    )
    monkeypatch.setattr(
        subject,
        "yahoo_discover_chain",
        lambda *_args, **_kwargs: pytest.fail("persisted target key should bypass renewal discovery"),
    )

    assert subject.resolve_yahoo_league_key(plan, access_token="token") == "470.l.80971"


def test_yahoo_trusts_an_explicit_target_key_without_rediscovery(monkeypatch):
    plan = subject.PlatformDraftPlan(
        db_name="kmffl",
        platform="yahoo",
        league_id="470.l.80971",
        league_name="KMFFL",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
        league_id_was_overridden=True,
    )
    monkeypatch.setattr(
        subject,
        "yahoo_discover_chain",
        lambda *_args, **_kwargs: pytest.fail("explicit target key should bypass renewal discovery"),
    )

    assert subject.resolve_yahoo_league_key(plan, access_token="token") == "470.l.80971"


def test_plan_recovers_legacy_registered_chain_from_league_settings(monkeypatch):
    monkeypatch.setattr(subject, "completed_season_for_db", lambda *_: 2025)
    monkeypatch.setattr(
        subject,
        "persisted_league_settings_ids",
        lambda *_: {"2025": "461.l.90939", "2026": "470.l.80971"},
    )

    plan = subject.build_plan_from_row(
        object(),
        {
            "db_name": "kmffl",
            "platform": "yahoo",
            "league_id": "461.l.90939",
            "league_ids_json": None,
            "league_name": "KMFFL",
        },
        draft_year=2026,
        league_id_override=None,
    )

    assert plan.league_id == "470.l.80971"
    assert plan.league_ids["2025"] == "461.l.90939"


def test_context_shards_are_disjoint_and_cover_every_context():
    rows = [
        {"db_name": "alpha", "platform": "sleeper"},
        {"db_name": "bravo", "platform": "sleeper"},
        {"db_name": "charlie", "platform": "sleeper"},
        {"db_name": "delta", "platform": "sleeper"},
        {"db_name": "echo", "platform": "sleeper"},
    ]

    shards = [subject.select_context_shard(rows, shard_index=index, shard_count=3) for index in range(3)]

    assert [[row["db_name"] for row in shard] for shard in shards] == [
        ["alpha", "delta"],
        ["bravo", "echo"],
        ["charlie"],
    ]


def test_execute_exit_code_rejects_provider_skips_but_accepts_no_change():
    assert subject.execution_exit_code(
        [subject.DraftCheckResult("x", "yahoo", "X", "1.l.1", 2026, "auth_missing")],
        execute=True,
    ) == 1
    assert subject.execution_exit_code(
        [subject.DraftCheckResult("x", "sleeper", "X", "1", 2026, "up_to_date")],
        execute=True,
    ) == 0


def test_terminal_status_invalidates_cache_before_reporting_success(monkeypatch):
    events: list[str] = []
    result = subject.DraftCheckResult(
        "kmffl",
        "yahoo",
        "KMFFL",
        "470.l.80971",
        2026,
        "changed",
        updated=True,
    )
    monkeypatch.setattr(
        subject,
        "record_offseason_update_status",
        lambda *_args, **kwargs: events.append(f"status:{kwargs['status']}"),
    )

    terminal = subject.finalize_execution_result(
        result,
        writer=object(),
        workflow_run_id=123,
        revalidate_cache=lambda db_name: events.append(f"cache:{db_name}"),
    )

    assert terminal == "succeeded"
    assert events == ["cache:kmffl", "status:succeeded"]


def test_cache_failure_becomes_a_retryable_failed_dispatch(monkeypatch):
    statuses: list[tuple[str, str | None]] = []
    result = subject.DraftCheckResult(
        "kmffl",
        "yahoo",
        "KMFFL",
        "470.l.80971",
        2026,
        "up_to_date",
    )
    monkeypatch.setattr(
        subject,
        "record_offseason_update_status",
        lambda *_args, **kwargs: statuses.append((kwargs["status"], kwargs.get("error"))),
    )

    terminal = subject.finalize_execution_result(
        result,
        writer=object(),
        workflow_run_id=123,
        revalidate_cache=lambda _db_name: (_ for _ in ()).throw(RuntimeError("cache unavailable")),
    )

    assert terminal == "failed"
    assert result.status == "error"
    assert result.error == "Cache refresh failed: cache unavailable"
    assert statuses == [("failed", "Cache refresh failed: cache unavailable")]


def test_explicit_execute_reports_a_missing_saved_context_as_an_error():
    missing = subject.missing_requested_context_results(
        [{"db_name": "found"}],
        requested_db_names=["found", "missing"],
        draft_year=2026,
    )

    assert [(result.db_name, result.draft_year, result.status) for result in missing] == [
        ("missing", 2026, "error")
    ]
    assert missing[0].error == "Fly has no saved league context for 'missing'"


def test_sleeper_check_compares_source_pick_metadata_without_import_grade_fetch(monkeypatch):
    plan = subject.PlatformDraftPlan(
        db_name="active_league",
        platform="sleeper",
        league_id="123456789",
        league_name="Active League",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )

    def sleeper_api(path):
        if path == "league/123456789/drafts":
            return [
                {
                    "draft_id": "draft-2026",
                    "season": "2026",
                    "status": "complete",
                    "type": "linear",
                    "slot_to_roster_id": {"1": 7},
                }
            ]
        if path == "draft/draft-2026/picks":
            return [
                {
                    "pick_no": 1,
                    "round": 1,
                    "draft_slot": 1,
                    "player_id": "player-1",
                    "metadata": {"amount": "12"},
                }
            ]
        raise AssertionError(f"unexpected Sleeper endpoint: {path}")

    monkeypatch.setattr(subject, "sleeper_api_json", sleeper_api, raising=False)
    monkeypatch.setattr(
        subject,
        "fetch_sleeper_publish_draft_df",
        lambda *_args, **_kwargs: pytest.fail("check path must not resolve publish-grade Sleeper draft rows"),
    )

    actual = subject.fetch_sleeper_api_draft_df(plan, allow_incomplete_draft=False)
    snapshot, draft_ids = subject.snapshot_from_dataframe(
        actual,
        snapshot_columns=subject.SLEEPER_SNAPSHOT_COLUMNS,
    )

    assert draft_ids == ["draft-2026"]
    assert snapshot == [
        {
            "draft_id": "draft-2026",
            "draft_type": "snake",
            "pick": "1",
            "round": "1",
            "draft_slot": "1",
            "team_key": "7",
            "sleeper_player_id": "player-1",
            "cost": "12",
        }
    ]


def test_platform_draft_snapshot_ignores_display_name_when_player_id_is_present():
    """Canonical name enrichment must not cause a repeat Fly refresh."""
    source = pd.DataFrame(
        [
            {
                "draft_id": "yahoo_470.l.4776_2026_default",
                "pick": 20,
                "yahoo_player_id": "12345",
                "player": "Kenneth Walker",
            }
        ]
    )
    published = source.copy()
    published.loc[0, "player"] = "Kenneth Walker III"

    source_snapshot, _ = subject.snapshot_from_dataframe(source)
    published_snapshot, _ = subject.snapshot_from_dataframe(published)

    assert subject.stable_fingerprint(source_snapshot) == subject.stable_fingerprint(published_snapshot)


def test_platform_draft_snapshot_uses_display_name_when_player_id_is_missing():
    """Name remains the source identity fallback when a platform did not provide an ID."""
    source = pd.DataFrame(
        [
            {
                "draft_id": "yahoo_470.l.4776_2026_default",
                "pick": 20,
                "player": "Kenneth Walker",
            }
        ]
    )
    changed = source.copy()
    changed.loc[0, "player"] = "Kenneth Walker III"

    source_snapshot, _ = subject.snapshot_from_dataframe(source)
    changed_snapshot, _ = subject.snapshot_from_dataframe(changed)

    assert subject.stable_fingerprint(source_snapshot) != subject.stable_fingerprint(changed_snapshot)


def test_update_is_not_marked_complete_when_fly_draft_fingerprint_differs(monkeypatch):
    plan = subject.PlatformDraftPlan(
        db_name="active_league",
        platform="sleeper",
        league_id="123456789",
        league_name="Active League",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    result = subject.DraftCheckResult(
        db_name=plan.db_name,
        platform=plan.platform,
        league_name=plan.league_name,
        league_id=plan.league_id,
        draft_year=plan.draft_year,
        status="changed",
    )
    source = pd.DataFrame(
        [
            {
                "draft_id": "draft-2026",
                "draft_type": "snake",
                "pick": 1,
                "round": 1,
                "draft_slot": 1,
                "team_key": "7",
                "sleeper_player_id": "player-1",
                "cost": 12,
            }
        ]
    )
    stale_fly_snapshot = [
        {
            "draft_id": "draft-2026",
            "draft_type": "snake",
            "pick": "1",
            "round": "1",
            "draft_slot": "1",
            "team_key": "7",
            "sleeper_player_id": "wrong-player",
            "cost": "12",
        }
    ]

    monkeypatch.setattr(subject, "fetch_sleeper_publish_draft_df", lambda *_args, **_kwargs: source)
    monkeypatch.setattr(subject, "replace_draft_year", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        subject,
        "stored_draft_snapshot",
        lambda *_args, **_kwargs: (stale_fly_snapshot, ["draft-2026"]),
    )

    with pytest.raises(RuntimeError, match="post-update draft fingerprint mismatch"):
        subject.execute_update(
            object(),
            result,
            plan,
            allow_incomplete_draft=False,
            api_retries=1,
            skip_sql_enrichments=True,
            include_ops_enrichments=False,
            strict_sql_enrichments=True,
            skip_aggregates=True,
            skip_homepage=True,
            chunk_size=50,
        )

    assert result.updated is False


def test_sleeper_update_uses_publish_rows_not_checker_snapshot(monkeypatch):
    """The checker snapshot is intentionally small and cannot be published."""
    plan = subject.PlatformDraftPlan(
        db_name="the_fucking_catalina_wine_mixer",
        platform="sleeper",
        league_id="1352102370921705472",
        league_name="The Fucking Catalina Wine Mixer",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    result = subject.DraftCheckResult(
        db_name=plan.db_name,
        platform=plan.platform,
        league_name=plan.league_name,
        league_id=plan.league_id,
        draft_year=plan.draft_year,
        status="missing_in_fly",
    )
    normalized = pd.DataFrame(
        [
            {
                "year": 2026,
                "draft_id": "draft-2026",
                "draft_type": "snake",
                "pick": 1,
                "round": 1,
                "draft_slot": 1,
                "team_key": "7",
                "sleeper_player_id": "player-1",
                "cost": None,
            }
        ]
    )
    expected_rows, expected_draft_ids = subject.snapshot_from_dataframe(
        normalized,
        snapshot_columns=subject.SLEEPER_SNAPSHOT_COLUMNS,
    )
    published: list[pd.DataFrame] = []

    monkeypatch.setattr(
        subject,
        "fetch_platform_draft_dataframe",
        lambda *_args, **_kwargs: pytest.fail("write path must not publish the checker snapshot"),
    )
    monkeypatch.setattr(subject, "fetch_sleeper_publish_draft_df", lambda *_args, **_kwargs: normalized)
    monkeypatch.setattr(
        subject,
        "replace_draft_year",
        lambda _conn, _plan, frame, **_kwargs: published.append(frame.copy()) or len(frame),
    )
    monkeypatch.setattr(subject, "stored_draft_snapshot", lambda *_args, **_kwargs: (expected_rows, expected_draft_ids))

    subject.execute_update(
        object(),
        result,
        plan,
        allow_incomplete_draft=False,
        api_retries=1,
        skip_sql_enrichments=True,
        include_ops_enrichments=False,
        strict_sql_enrichments=True,
        skip_aggregates=True,
        skip_homepage=True,
        chunk_size=50,
    )

    assert len(published) == 1
    assert published[0]["year"].tolist() == [2026]
    assert result.updated is True


def test_yahoo_update_reuses_the_checker_draft_dataframe(monkeypatch):
    """Yahoo must publish the already-fetched canonical rows, not fetch twice."""
    plan = subject.PlatformDraftPlan(
        db_name="kmffl",
        platform="yahoo",
        league_id="470.l.80971",
        league_name="KMFFL",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    result = subject.DraftCheckResult(
        db_name=plan.db_name,
        platform=plan.platform,
        league_name=plan.league_name,
        league_id=plan.league_id,
        draft_year=plan.draft_year,
        status="missing_in_fly",
    )
    checked_draft = pd.DataFrame(
        [
            {
                "draft_id": "yahoo_470.l.80971_2026_default",
                "draft_category": "default",
                "draft_type": "snake",
                "pick": 1,
                "round": 1,
                "pick_in_round": 1,
                "draft_slot": 1,
                "draft_slot_roster_id": None,
                "team_key": "470.l.80971.t.1",
                "yahoo_player_id": "123",
                "sleeper_player_id": None,
                "espn_player_id": None,
                "player": "Example Player",
                "cost": None,
                "is_keeper": 0,
                "year": 2026,
                "db_name": "kmffl",
            }
        ]
    )
    expected_snapshot = [
        {
            "draft_id": "yahoo_470.l.80971_2026_default",
            "draft_category": "default",
            "draft_type": "snake",
            "pick": "1",
            "round": "1",
            "pick_in_round": "1",
            "draft_slot": "1",
            "draft_slot_roster_id": None,
            "team_key": "470.l.80971.t.1",
            "yahoo_player_id": "123",
            "sleeper_player_id": None,
            "espn_player_id": None,
            "player": None,
            "cost": None,
            "is_keeper": "0",
        }
    ]
    published: list[pd.DataFrame] = []

    monkeypatch.setattr(
        subject,
        "fetch_platform_draft_dataframe",
        lambda *_args, **_kwargs: pytest.fail("Yahoo write path fetched the draft a second time"),
    )
    monkeypatch.setattr(
        subject,
        "replace_draft_year",
        lambda _conn, _plan, frame, **_kwargs: published.append(frame.copy()) or len(frame),
    )
    monkeypatch.setattr(
        subject,
        "stored_draft_snapshot",
        lambda *_args, **_kwargs: (expected_snapshot, ["yahoo_470.l.80971_2026_default"]),
    )

    subject.execute_update(
        object(),
        result,
        plan,
        checker_draft_df=checked_draft,
        allow_incomplete_draft=False,
        api_retries=1,
        skip_sql_enrichments=True,
        include_ops_enrichments=False,
        strict_sql_enrichments=True,
        skip_aggregates=True,
        skip_homepage=True,
        chunk_size=50,
    )

    assert len(published) == 1
    assert published[0].equals(checked_draft)
    assert result.updated is True


def test_complete_update_uses_the_local_stateful_stage_before_any_fly_publish(monkeypatch):
    """Product updates must not bypass grades, rollups, or homepage rebuilds."""
    plan = subject.PlatformDraftPlan(
        db_name="kmffl",
        platform="yahoo",
        league_id="470.l.80971",
        league_name="KMFFL",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    result = subject.DraftCheckResult(
        db_name=plan.db_name,
        platform=plan.platform,
        league_name=plan.league_name,
        league_id=plan.league_id,
        draft_year=plan.draft_year,
        status="changed",
    )
    checked_draft = pd.DataFrame(
        [{"db_name": "kmffl", "year": 2026, "draft_id": "draft", "pick": 1, "player": "Player"}]
    )
    expected_rows, expected_draft_ids = subject.snapshot_from_dataframe(
        checked_draft,
        snapshot_columns=subject.snapshot_columns_for_platform("yahoo"),
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(subject, "replace_draft_year", lambda *_args, **_kwargs: pytest.fail("raw draft publish"))
    monkeypatch.setattr(
        subject,
        "run_complete_local_offseason_update",
        lambda _conn, captured_plan, frame, **kwargs: captured.update(plan=captured_plan, frame=frame.copy(), **kwargs),
    )
    monkeypatch.setattr(subject, "stored_draft_snapshot", lambda *_args, **_kwargs: (expected_rows, expected_draft_ids))

    subject.execute_update(
        object(),
        result,
        plan,
        checker_draft_df=checked_draft,
        allow_incomplete_draft=False,
        api_retries=1,
        skip_sql_enrichments=False,
        include_ops_enrichments=True,
        strict_sql_enrichments=True,
        skip_aggregates=False,
        skip_homepage=False,
        chunk_size=50,
    )

    assert captured["plan"] == plan
    assert captured["frame"].equals(checked_draft)
    assert captured["include_ops_enrichments"] is True
    assert captured["strict_sql_enrichments"] is True
    assert captured["skip_sql_enrichments"] is False
    assert captured["skip_aggregates"] is False
    assert captured["skip_homepage"] is False
    assert result.updated is True


def test_draft_only_refresh_preserves_unaffected_homepage_outputs(monkeypatch):
    """A 2026 draft has no player/matchup mutation to justify a history pull."""
    plan = update_subject.OffseasonDraftPlan(
        db_name="kmffl",
        league_name="KMFFL",
        sleeper_league_id="470.l.80971",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    calls: list[str] = []

    class Local:
        def close(self):
            calls.append("close")

    monkeypatch.setattr(update_subject, "_hydrate_local_offseason_draft_db", lambda *_args: Local())
    monkeypatch.setattr(update_subject, "run_draft_sql_enrichments", lambda *_args, **_kwargs: calls.append("sql"))
    monkeypatch.setattr(update_subject, "rebuild_draft_aggregates", lambda *_args, **_kwargs: calls.append("aggregates"))
    monkeypatch.setattr(
        update_subject,
        "rebuild_homepage_tables",
        lambda *_args, **_kwargs: pytest.fail("draft-only refresh must preserve homepage outputs"),
    )
    monkeypatch.setattr(update_subject, "publish_local_offseason_outputs", lambda *_args: calls.append("publish"))

    update_subject.run_complete_local_offseason_update(
        object(),
        plan,
        pd.DataFrame([{"db_name": "kmffl", "year": 2026, "pick": 1}]),
        include_ops_enrichments=True,
        strict_sql_enrichments=True,
        skip_sql_enrichments=False,
        skip_aggregates=False,
        skip_homepage=False,
        chunk_size=50,
    )

    assert calls == ["sql", "aggregates", "publish", "close"]


def test_local_offseason_stage_keeps_all_null_canonical_draft_fields_typed():
    """All-null values must retain their canonical types in the local SQL stage."""
    plan = update_subject.OffseasonDraftPlan(
        db_name="kmffl",
        league_name="KMFFL",
        sleeper_league_id="470.l.80971",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    source_frames = {
        "draft": pd.DataFrame(
            [
                {
                    "db_name": "kmffl",
                    "year": 2025,
                    "pick": 1,
                    "draft_category": None,
                    "draft_type": None,
                    "draft_grade": None,
                }
            ]
        ),
        "transactions": pd.DataFrame(
            [{"db_name": "kmffl", "sleeper_player_id": None}]
        ),
        **{
            table_name: pd.DataFrame([{"db_name": "kmffl"}])
            for table_name in update_subject.LOCAL_OFFSEASON_SOURCE_TABLES
            if table_name not in {"draft", "transactions"}
        },
    }

    class Result:
        def __init__(self, frame):
            self.frame = frame

        def fetchdf(self):
            return self.frame.copy()

    class Connection:
        def execute(self, sql):
            for table_name, frame in source_frames.items():
                if f"public.{table_name}" in sql:
                    return Result(frame)
            raise AssertionError(f"unexpected query: {sql}")

    incoming = pd.DataFrame(
        [
            {
                "db_name": "kmffl",
                "year": 2026,
                "pick": 1,
                "draft_category": None,
                "draft_type": None,
                "draft_grade": None,
            }
        ]
    )
    local = update_subject._hydrate_local_offseason_draft_db(Connection(), plan, incoming)
    try:
        draft_types = local.execute(
            "SELECT typeof(draft_category), typeof(draft_type), typeof(draft_grade) FROM public.draft LIMIT 1"
        ).fetchone()
        sleeper_player_id_type = local.execute(
            "SELECT typeof(sleeper_player_id) FROM public.transactions LIMIT 1"
        ).fetchone()[0]
    finally:
        local.close()

    assert draft_types == ("VARCHAR", "VARCHAR", "VARCHAR")
    assert sleeper_player_id_type == "VARCHAR"


def test_local_offseason_stage_materializes_the_empty_target_year_player_schema():
    """Future drafts still take the normal player-aware enrichment path."""
    plan = update_subject.OffseasonDraftPlan(
        db_name="kmffl",
        league_name="KMFFL",
        sleeper_league_id="470.l.80971",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    source_frames = {
        "draft": pd.DataFrame([{"db_name": "kmffl", "year": 2025, "pick": 1}]),
        # Fly's empty-result transport produces no columns at all.  The
        # stage must recover the canonical player schema itself.
        "player_fantasy": pd.DataFrame(),
        **{
            table_name: pd.DataFrame([{"db_name": "kmffl"}])
            for table_name in update_subject.LOCAL_OFFSEASON_SOURCE_TABLES
            if table_name not in {"draft", "player_fantasy"}
        },
    }
    queries: list[str] = []

    class Result:
        def __init__(self, frame):
            self.frame = frame

        def fetchdf(self):
            return self.frame.copy()

    class Connection:
        def execute(self, sql):
            queries.append(sql)
            for table_name, frame in source_frames.items():
                if f"public.{table_name}" in sql:
                    return Result(frame)
            raise AssertionError(f"unexpected query: {sql}")

    incoming = pd.DataFrame([{"db_name": "kmffl", "year": 2026, "pick": 1}])
    local = update_subject._hydrate_local_offseason_draft_db(Connection(), plan, incoming)
    try:
        assert local.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] == 0
        assert local.execute("SELECT typeof(NFL_player_id) FROM public.player_fantasy LIMIT 0").description[0][1] == "VARCHAR"
    finally:
        local.close()

    assert any("public.player_fantasy" in query and "year = 2026" in query for query in queries)


def test_bench_insurance_uses_explicit_defaults_when_future_player_table_is_empty():
    """A 2026 draft without player weeks is not a partial-model fallback."""
    import duckdb

    from multi_league.transformations.sql_enrichments import SQLEnrichments

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute(
            "CREATE TABLE public.draft (db_name VARCHAR, position VARCHAR, manager_lamar DOUBLE, "
            "bench_insurance_discount DOUBLE, bench_lamar DOUBLE)"
        )
        conn.execute("INSERT INTO public.draft VALUES ('kmffl', 'RB', 0, NULL, NULL)")
        conn.execute(
            "CREATE TABLE public.player_fantasy (db_name VARCHAR, NFL_player_id VARCHAR, year INTEGER, "
            "week INTEGER, player_week VARCHAR, position VARCHAR, manager VARCHAR, is_started INTEGER, fantasy_points DOUBLE)"
        )

        SQLEnrichments(db_name="kmffl", data_dir="unused", conn=conn).draft_bench_insurance()

        assert conn.execute("SELECT bench_insurance_discount FROM public.draft").fetchone()[0] == 0.30
    finally:
        conn.close()


def test_local_offseason_stage_reuses_unambiguous_historical_franchise_ids():
    """A masked current Yahoo GUID must retain its established franchise/alias."""
    plan = update_subject.OffseasonDraftPlan(
        db_name="kmffl",
        league_name="KMFFL",
        sleeper_league_id="470.l.80971",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={"Yahoo Display Name": "Canonical Alias"},
    )
    source_frames = {
        "draft": pd.DataFrame(
            [
                {
                    "db_name": "kmffl",
                    "year": 2025,
                    "pick": 1,
                    "manager": "Canonical Alias",
                    "franchise_id": "stable-franchise",
                }
            ]
        ),
        **{
            table_name: pd.DataFrame([{"db_name": "kmffl"}])
            for table_name in update_subject.LOCAL_OFFSEASON_SOURCE_TABLES
            if table_name != "draft"
        },
    }

    class Result:
        def __init__(self, frame):
            self.frame = frame

        def fetchdf(self):
            return self.frame.copy()

    class Connection:
        def execute(self, sql):
            for table_name, frame in source_frames.items():
                if f"public.{table_name}" in sql:
                    return Result(frame)
            raise AssertionError(f"unexpected query: {sql}")

    incoming = pd.DataFrame(
        [
            {
                "db_name": "kmffl",
                "year": 2026,
                "pick": 1,
                "manager": "Yahoo Display Name",
                "manager_guid": "--hidden--",
                "franchise_id": None,
            }
        ]
    )
    local = update_subject._hydrate_local_offseason_draft_db(Connection(), plan, incoming)
    try:
        franchise_id = local.execute(
            "SELECT franchise_id FROM public.draft WHERE year = 2026"
        ).fetchone()[0]
    finally:
        local.close()

    assert franchise_id == "stable-franchise"


def test_draft_rollups_have_stable_fleet_publish_identities():
    """Full draft rollups must be safely replaceable through the fleet lane."""
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    assert registry["draft_manager_season"]["primary_keys"] == [
        "db_name",
        "franchise_id",
        "year",
        "draft_category",
    ]
    assert registry["draft_manager_career"]["primary_keys"] == [
        "db_name",
        "franchise_id",
        "draft_category",
    ]


def test_fast_sleeper_publish_fetch_has_canonical_year_player_and_manager_fields(monkeypatch):
    plan = subject.PlatformDraftPlan(
        db_name="the_fucking_catalina_wine_mixer",
        platform="sleeper",
        league_id="1352102370921705472",
        league_name="The Fucking Catalina Wine Mixer",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={"old-name": "Renamed Manager"},
    )

    def sleeper_api(path):
        if path == "league/1352102370921705472/drafts":
            return [
                {
                    "draft_id": "draft-2026",
                    "season": "2026",
                    "status": "complete",
                    "type": "snake",
                    "slot_to_roster_id": {"1": 7},
                    "settings": {"player_type": 0, "rounds": 14},
                }
            ]
        if path == "draft/draft-2026/picks":
            return [
                {
                    "pick_no": 1,
                    "round": 1,
                    "draft_slot": 1,
                    "roster_id": 7,
                    "picked_by": "user-7",
                    "player_id": "player-1",
                    "metadata": {
                        "first_name": "Player",
                        "last_name": "One",
                        "position": "RB",
                        "team": "DET",
                        "amount": "12",
                    },
                }
            ]
        if path == "league/1352102370921705472/rosters":
            return [{"roster_id": 7, "owner_id": "user-7"}]
        if path == "league/1352102370921705472/users":
            return [{"user_id": "user-7", "display_name": "old-name", "metadata": {"team_name": "Team Seven"}}]
        raise AssertionError(f"unexpected Sleeper endpoint: {path}")

    monkeypatch.setattr(subject, "sleeper_api_json", sleeper_api)

    actual = subject.fetch_sleeper_publish_draft_df(plan, allow_incomplete_draft=False)

    assert actual[["db_name", "year", "draft_id", "round", "pick"]].to_dict("records") == [
        {
            "db_name": "the_fucking_catalina_wine_mixer",
            "year": 2026,
            "draft_id": "draft-2026",
            "round": 1,
            "pick": 1,
        }
    ]
    assert actual.loc[0, "player"] == "Player One"
    assert actual.loc[0, "position"] == "RB"
    assert actual.loc[0, "nfl_team_api"] == "DET"
    assert actual.loc[0, "manager"] == "Renamed Manager"
    assert actual.loc[0, "manager_guid"] == "user-7"
    assert actual.loc[0, "team_name"] == "Team Seven"
    assert actual.loc[0, "draft_category"] == "startup"


@pytest.mark.parametrize("platform", ["espn", "yahoo"])
def test_offseason_platform_context_preserves_manager_aliases(monkeypatch, platform):
    """The one-year updater must receive the same aliases as a full import."""
    plan = subject.PlatformDraftPlan(
        db_name=f"{platform}_alias_league",
        platform=platform,
        league_id="12345.l.67890" if platform == "yahoo" else "12345",
        league_name="Alias League",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={"Marc": "Tom"},
    )
    captured: dict[str, object] = {}

    if platform == "yahoo":
        from multi_league.core import league_context as league_context_module
        from multi_league.data_fetchers.yahoo import yahoo_draft as yahoo_draft_module

        original_context = league_context_module.LeagueContext

        def recording_context(**kwargs):
            captured["overrides"] = kwargs["manager_name_overrides"]
            return original_context(**kwargs)

        monkeypatch.setattr(subject, "retrieve_required_yahoo_credentials", lambda _plan: {"refresh_token": "refresh"})
        monkeypatch.setattr(subject, "yahoo_client_id_secret", lambda: ("client", "secret"))
        monkeypatch.setattr(subject, "yahoo_refresh_token", lambda *_args: {"access_token": "access"})
        monkeypatch.setattr(subject, "resolve_yahoo_league_key", lambda *_args, **_kwargs: "12345.l.67890")
        monkeypatch.setattr(league_context_module, "LeagueContext", recording_context)
        monkeypatch.setattr(yahoo_draft_module, "fetch_draft_data", lambda **_kwargs: pd.DataFrame())

        actual = subject.fetch_yahoo_api_draft_df(plan)
    else:
        from multi_league.data_fetchers.espn import espn_context as espn_context_module
        from multi_league.data_fetchers.espn import espn_draft as espn_draft_module

        original_context = espn_context_module.ESPNContext

        def recording_context(**kwargs):
            captured["overrides"] = kwargs["manager_name_overrides"]
            return original_context(**kwargs)

        monkeypatch.setattr(
            subject,
            "retrieve_required_espn_credentials",
            lambda _plan: {"league_id": "12345", "espn_s2": "s2", "swid": "{swid}"},
        )
        monkeypatch.setattr(subject, "hydrate_espn_team_maps", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(espn_context_module, "ESPNContext", recording_context)
        monkeypatch.setattr(espn_draft_module, "fetch_espn_draft", lambda *_args, **_kwargs: pd.DataFrame())

        actual = subject.fetch_espn_api_draft_df(plan)

    assert actual.empty
    assert captured["overrides"] == {"Marc": "Tom"}


def test_draft_enrichments_include_the_stateful_value_score_step(monkeypatch):
    """A complete offseason update must retain the standard draft score model."""
    expected_steps = [
        "backfill_draft_managers",
        "player_to_draft",
        "draft_manager_aggregates",
        "draft_cost_buckets",
        "draft_value_zscore",
        "draft_bench_insurance",
        "draft_starter_designation",
        "draft_failure_rates",
        "draft_bench_value_by_rank",
        "draft_pick_conveyances",
    ]
    calls: list[str] = []

    class RecordingEnricher:
        def __init__(self, *_args, **_kwargs):
            self._ops_attached = False

        def load_settings_from_db(self):
            return {}, {}

        def _update_scoring_params(self, _scoring_params):
            return None

        def __getattr__(self, name):
            def run_step():
                calls.append(name)
                return name

            return run_step

    from multi_league.transformations import sql_enrichments

    monkeypatch.setattr(sql_enrichments, "SQLEnrichments", RecordingEnricher)

    results = update_subject.run_draft_sql_enrichments(
        object(),
        "active_league",
        include_ops_enrichments=False,
        strict=True,
    )

    assert calls == expected_steps
    assert results == {step: step for step in expected_steps}


def test_replacing_one_offseason_draft_year_uses_scoped_fleet_publish(monkeypatch):
    """A selected league/year must use Fly's scoped publisher, never raw DML."""

    class _Cursor:
        def __init__(self, rows=None):
            self._rows = rows or []

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

    class RecordingConnection:
        def __init__(self):
            self.calls: list[str] = []

        def execute(self, sql):
            self.calls.append(sql)
            assert "DELETE FROM" not in sql
            assert "INSERT INTO" not in sql
            return _Cursor([(7,)])

    captured: dict = {}

    class FakeFlyTarget:
        def merge_fleet_partition(self, bundle_path, *, bundle_id, bundle_hash):
            with tarfile.open(bundle_path, "r:gz") as archive:
                manifest_file = archive.extractfile("manifest.json")
                assert manifest_file is not None
                captured["manifest"] = json.loads(manifest_file.read())
            captured["bundle_id"] = bundle_id
            captured["bundle_hash"] = bundle_hash
            return {"status": "COMMITTED", "tables": {"draft": 2}}

    from multi_league.core.targets import fly_target

    monkeypatch.setattr(fly_target, "FlyTarget", FakeFlyTarget)

    plan = update_subject.OffseasonDraftPlan(
        db_name="the_fucking_catalina_wine_mixer",
        league_name="The Fucking Catalina Wine Mixer",
        sleeper_league_id="123456789",
        draft_year=2026,
        completed_season=2025,
        manager_name_overrides={},
    )
    draft_df = pd.DataFrame(
        [
            {
                "year": 2026,
                "draft_id": "draft-2026",
                "draft_type": "snake",
                "round": 1,
                "pick": 1,
                "player": "Player One",
            },
            {
                "year": 2026,
                "draft_id": "draft-2026",
                "draft_type": "snake",
                "round": 1,
                "pick": 2,
                "player": "Player Two",
            },
        ]
    )
    conn = RecordingConnection()

    replaced = update_subject.replace_draft_year(
        conn,
        plan,
        draft_df,
        chunk_size=1,
        dry_run=False,
    )

    assert replaced == 2
    assert len(conn.calls) == 1
    assert "league_publish_generations" in conn.calls[0]
    assert captured["bundle_id"] == captured["manifest"]["bundle_id"]
    assert captured["bundle_hash"] == captured["manifest"]["bundle_hash"]
    assert captured["manifest"]["active_year"] == 2026
    assert captured["manifest"]["db_names"] == [plan.db_name]
    assert captured["manifest"]["league_generations"] == {plan.db_name: 7}
    assert len(captured["manifest"]["tables"]) == 1
    entry = captured["manifest"]["tables"][0]
    assert entry["table"] == "draft"
    assert entry["row_count"] == 2
    assert entry["merge_mode"] == "replace_scope"
    assert entry["cadence_class"] == "active_season"
    assert entry["scope"] == {"year": 2026}
