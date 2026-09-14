from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from pathlib import Path

import json
import pytest

import multi_league.data_fetchers.yahoo.yahoo_recovery as recovery_mod
from multi_league.core.local_db import LocalLeagueDB
from multi_league.data_fetchers.yahoo.yahoo_recovery import (
    Gap,
    _canonicalize_gap_units,
    _detect_gaps_from_local_db,
    _resolve_draft_gap,
    _resolve_matchup_gap,
    _resolve_roster_gap,
    _resolve_schedule_gap,
    _resolve_transaction_gap,
    _rewrite_active_manifests,
    run_yahoo_recovery,
)
from multi_league.data_fetchers.yahoo.yahoo_recovery_api import (
    RecoveryDeferredError,
    fetch_single_team_roster,
    fetch_single_week_matchups,
)


@patch("multi_league.data_fetchers.yahoo.yahoo_matchups.parse_matchups_for_week")
def test_fetch_single_week_matchups_raises_deferred_on_request_denied(mock_parse):
    mock_parse.side_effect = RuntimeError("b'Request denied\\r\\n'")

    with pytest.raises(RecoveryDeferredError):
        fetch_single_week_matchups(MagicMock(), "414.l.413370", 2022, 1)


def test_fetch_single_team_roster_uses_team_key_when_guid_missing():
    fetcher = MagicMock()
    fetcher.fetch_roster_for_week.return_value = [
        {
            "year": 2025,
            "week": 1,
            "team_key": "461.l.1.t.7",
            "manager_name": "Alex",
            "player_name": "Player A",
            "player_id": "101",
        }
    ]

    df = fetch_single_team_roster(fetcher, 2025, 1, "461.l.1.t.7", "Alex", manager_guid=None)

    assert df.loc[0, "manager_week"] == "461.l.1.t.7_2025_1"
    assert recovery_mod.pd.isna(df.loc[0, "franchise_id"])


def test_empty_matchup_endpoint_is_permanent_not_a_rate_limit():
    assert recovery_mod._is_non_retryable_recovery_error("No matchup data found") is True


def test_try_resolve_gap_marks_permanently_empty_matchups_hard_missing(monkeypatch):
    gap = Gap("matchup", 2005, "full_year", "manifest")
    monkeypatch.setattr(
        recovery_mod,
        "_resolve_matchup_gap",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("No matchup data found")),
    )

    result = recovery_mod._try_resolve_gap(
        gap,
        SimpleNamespace(),
        Path("."),
        local_db=MagicMock(),
    )

    assert result == "empty"
    assert gap.hard_missing is True


def test_run_yahoo_recovery_leaves_deferred_gap_unresolved(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    gap = Gap("matchup", 2022, "full_year", "manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 2)

    with (
        patch.object(recovery_mod, "_detect_gaps_from_manifests", return_value=[gap]),
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_rewrite_active_manifests"),
        patch.object(recovery_mod, "_try_resolve_gap", return_value="rate_limited"),
        patch.object(recovery_mod, "clear_all_manifests"),
    ):
        result = run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    assert result.success is False
    assert [g.key for g in result.unresolved] == [gap.key]
    assert result.hard_missing == []
    assert result.rounds == 2


def test_run_yahoo_recovery_continues_queue_after_deferred_item(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    deferred_gap = Gap("matchup", 2022, "full_year", "manifest")
    resolved_gap = Gap("transaction", 2022, "full_year", "manifest")

    _rewrite_active_manifests(tmp_path, [deferred_gap, resolved_gap])
    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    def _resolve(current_gap, *_args, **_kwargs):
        if current_gap.fetcher == "matchup":
            current_gap.last_error = "b'Request denied\\r\\n'"
            return "rate_limited"
        return True

    with (
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_resolve),
    ):
        result = run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    manifest = json.loads((tmp_path / ".fetch_failures" / "matchup_2022.json").read_text())
    assert manifest["failed_year"] is True
    assert "Request denied" in manifest["error"]
    assert [gap.key for gap in result.unresolved] == [deferred_gap.key]
    assert [gap.key for gap in result.resolved] == [resolved_gap.key]


def test_run_yahoo_recovery_stops_after_cooldown_cap_for_transient_failures(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2019": "390.l.1", "2020": "399.l.1", "2021": "406.l.1"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    gaps = [
        Gap("matchup", 2019, "full_year", "manifest"),
        Gap("matchup", 2020, "full_year", "manifest"),
        Gap("matchup", 2021, "full_year", "manifest"),
    ]

    _rewrite_active_manifests(tmp_path, gaps)
    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_COOLDOWN_EVENTS", 2)
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 15)

    attempted = []

    def _resolve(current_gap, *_args, **_kwargs):
        attempted.append(current_gap.key)
        current_gap.last_error = "temporary Yahoo failure"
        return False

    with (
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_resolve),
    ):
        result = run_yahoo_recovery(
            ctx,
            {"2019": {}, "2020": {}, "2021": {}},
            dead_man_minutes=1,
            local_db=local_db,
        )

    assert len(attempted) == 3
    assert result.success is False
    assert result.rounds == 1
    assert {gap.key for gap in result.unresolved} == {gap.key for gap in gaps}


def test_run_yahoo_recovery_keeps_excluded_years_out_after_reaudit(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2021": "406.l.38187", "2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    active_gap = Gap("transaction", 2021, "full_year", "manifest")
    excluded_gap = Gap("schedule", 2022, "full_year", "audit")

    _rewrite_active_manifests(tmp_path, [active_gap])
    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    with (
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[excluded_gap]),
        patch.object(recovery_mod, "_try_resolve_gap", return_value=True),
    ):
        result = run_yahoo_recovery(
            ctx,
            {"2021": {}, "2022": {}},
            dead_man_minutes=1,
            local_db=local_db,
            exclude_years={2022},
        )

    assert result.success is True
    assert [gap.key for gap in result.resolved] == [active_gap.key]
    assert result.unresolved == []


def test_rewrite_active_manifests_replaces_stale_gap_details_and_updates_error(tmp_path):
    manifest_dir = tmp_path / ".fetch_failures"
    manifest_dir.mkdir()
    (manifest_dir / "roster_2022.json").write_text(
        """
        {
          "fetcher": "roster",
          "year": 2022,
          "failed_details": ["wk1_Adin", "wk1_Daniel"],
          "error": "old recovery error"
        }
        """.strip()
    )

    gap = Gap("roster", 2022, "wk1_Adin", "manifest", last_error="Yahoo API access denied while fetching teams")
    _rewrite_active_manifests(tmp_path, [gap])

    manifest = (manifest_dir / "roster_2022.json").read_text()
    assert "wk1_Adin" in manifest
    assert "wk1_Daniel" not in manifest
    assert "Yahoo API access denied while fetching teams" in manifest


def test_rewrite_active_manifests_drops_broad_overlap_in_favor_of_exact_gap(tmp_path):
    _rewrite_active_manifests(
        tmp_path,
        [
            Gap("transaction", 2022, "full_year", "manifest"),
            Gap("transaction", 2022, "pair::1:2", "audit"),
        ],
    )

    manifest = json.loads((tmp_path / ".fetch_failures" / "transaction_2022.json").read_text())
    assert manifest["failed_details"] == ["pair::1:2"]
    assert "failed_year" not in manifest


def test_run_yahoo_recovery_persists_deferred_error_into_manifest(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    gap = Gap("matchup", 2022, "full_year", "manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    def _defer_with_error(current_gap, *_args, **_kwargs):
        current_gap.last_error = "b'Request denied\\r\\n'"
        return "rate_limited"

    with (
        patch.object(recovery_mod, "_detect_gaps_from_manifests", return_value=[gap]),
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_defer_with_error),
    ):
        result = run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    manifest_path = tmp_path / ".fetch_failures" / "matchup_2022.json"
    assert manifest_path.exists()
    assert "Request denied" in manifest_path.read_text()
    assert result.hard_missing == []


def test_run_yahoo_recovery_uses_seed_queue_for_first_pass(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    gap = Gap("matchup", 2022, "full_year", "manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    collect_calls = []

    def _collect(*_args, **_kwargs):
        collect_calls.append(1)
        return [gap], [], [gap]

    with (
        patch.object(recovery_mod, "_collect_retryable_gaps", side_effect=_collect),
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", return_value="rate_limited"),
    ):
        run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    assert len(collect_calls) == 2


def test_run_yahoo_recovery_blocks_remaining_scope_after_rate_limit(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2021": "406.l.38187", "2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    roster_gap_1 = Gap("roster", 2021, "wk17_mw::A_2021_17", "manifest")
    roster_gap_2 = Gap("roster", 2021, "wk17_mw::B_2021_17", "manifest")
    tx_gap = Gap("transaction", 2022, "full_year", "manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    attempted = []

    collect_results = [
        ([roster_gap_1, roster_gap_2, tx_gap], [], [roster_gap_1, roster_gap_2, tx_gap]),
        ([roster_gap_1, roster_gap_2], [], [roster_gap_1, roster_gap_2]),
    ]

    def _resolve(current_gap, *_args, **_kwargs):
        attempted.append(current_gap.key)
        if current_gap.key == roster_gap_1.key:
            current_gap.last_error = "Yahoo API access denied"
            return "rate_limited"
        return True

    with (
        patch.object(recovery_mod, "_collect_retryable_gaps", side_effect=collect_results),
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_resolve),
    ):
        result = run_yahoo_recovery(ctx, {"2021": {}, "2022": {}}, dead_man_minutes=1, local_db=local_db)

    assert attempted == [roster_gap_1.key, tx_gap.key]
    assert [gap.key for gap in result.resolved] == [tx_gap.key]
    assert [gap.key for gap in result.unresolved] == [("roster", 2021, "wk17")]


def test_run_yahoo_recovery_attempts_matchup_before_schedule_for_same_year(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()
    schedule_gap = Gap("schedule", 2022, "full_year", "manifest")
    matchup_gap = Gap("matchup", 2022, "full_year", "manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 1)

    attempted = []
    collect_results = [
        ([schedule_gap, matchup_gap], [], [schedule_gap, matchup_gap]),
        ([], [], []),
    ]

    def _resolve(current_gap, *_args, **_kwargs):
        attempted.append(current_gap.key)
        return True

    with (
        patch.object(recovery_mod, "_collect_retryable_gaps", side_effect=collect_results),
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_resolve),
    ):
        run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    assert attempted == [matchup_gap.key, schedule_gap.key]


def test_run_yahoo_recovery_manifest_lifecycle_seeded_then_rediscovers_then_clears(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()

    recovery_mod.write_manifest(tmp_path, "matchup", 2022, failed_year=True, error="seeded test manifest")

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 3)

    attempted = []
    snapshots = []
    real_rewrite = recovery_mod._rewrite_active_manifests
    discovered_gap = Gap("transaction", 2022, "full_year", "audit")

    audit_sequence = [
        [],
        [discovered_gap],
        [],
        [],
        [],
    ]

    def _audit(*_args, **_kwargs):
        if audit_sequence:
            return audit_sequence.pop(0)
        return []

    def _resolve(current_gap, *_args, **_kwargs):
        attempted.append(current_gap.key)
        return True

    def _recording_rewrite(data_dir, gaps):
        real_rewrite(data_dir, gaps)
        manifest_dir = tmp_path / recovery_mod.MANIFEST_DIR_NAME
        snapshot = {}
        if manifest_dir.exists():
            for path in sorted(manifest_dir.glob("*.json")):
                snapshot[path.name] = json.loads(path.read_text())
        snapshots.append(snapshot)

    with (
        patch.object(recovery_mod, "_detect_gaps_from_local_db", side_effect=_audit),
        patch.object(recovery_mod, "_try_resolve_gap", side_effect=_resolve),
        patch.object(recovery_mod, "_rewrite_active_manifests", side_effect=_recording_rewrite),
    ):
        result = run_yahoo_recovery(ctx, {"2022": {}}, dead_man_minutes=1, local_db=local_db)

    snapshot_keys = [tuple(sorted(snapshot.keys())) for snapshot in snapshots]
    matchup_index = snapshot_keys.index(("matchup_2022.json",))
    clear_after_matchup = snapshot_keys.index((), matchup_index + 1)
    transaction_index = snapshot_keys.index(("transaction_2022.json",), clear_after_matchup + 1)

    assert snapshot_keys[matchup_index] == ("matchup_2022.json",)
    assert snapshot_keys[clear_after_matchup] == ()
    assert snapshot_keys[transaction_index] == ("transaction_2022.json",)
    assert () in snapshot_keys[transaction_index + 1 :]
    assert attempted == [("matchup", 2022, "full_year"), ("transaction", 2022, "full_year")]
    assert result.success is True
    assert result.rounds == 2
    assert result.unresolved == []
    assert not (tmp_path / recovery_mod.MANIFEST_DIR_NAME).exists()


def test_run_yahoo_recovery_smoke_fake_manifest_replays_canonical_units_and_clears(tmp_path, monkeypatch):
    ctx = SimpleNamespace(
        data_dir=str(tmp_path),
        league_ids={"2021": "406.l.38187", "2022": "414.l.413370"},
        oauth=MagicMock(),
    )
    local_db = MagicMock()

    recovery_mod.write_manifest(
        tmp_path,
        "roster",
        2021,
        failed_details=["wk8_mw::GUID_A_2021_8", "wk8_mw::GUID_B_2021_8"],
        error="seeded roster recovery manifest",
    )
    recovery_mod.write_manifest(
        tmp_path,
        "transaction",
        2022,
        failed_details=["txid::12345"],
        error="seeded transaction recovery manifest",
    )
    recovery_mod.write_manifest(
        tmp_path,
        "schedule",
        2022,
        failed_year=True,
        error="seeded schedule recovery manifest",
    )

    monkeypatch.setattr(recovery_mod, "COOLDOWN_SCHEDULE", [0])
    monkeypatch.setattr(recovery_mod, "MAX_PASSES", 2)

    attempted = []
    snapshots = []
    real_rewrite = recovery_mod._rewrite_active_manifests

    def _recording_rewrite(data_dir, gaps):
        real_rewrite(data_dir, gaps)
        manifest_dir = tmp_path / recovery_mod.MANIFEST_DIR_NAME
        snapshot = {}
        if manifest_dir.exists():
            for path in sorted(manifest_dir.glob("*.json")):
                snapshot[path.name] = json.loads(path.read_text())
        snapshots.append(snapshot)

    def _resolve_schedule(gap, *_args, **_kwargs):
        attempted.append(gap.key)
        return True

    def _resolve_roster(gap, *_args, **_kwargs):
        attempted.append(gap.key)
        return True

    def _resolve_transaction(gap, *_args, **_kwargs):
        attempted.append(gap.key)
        return True

    with (
        patch.object(recovery_mod, "_detect_gaps_from_local_db", return_value=[]),
        patch.object(recovery_mod, "_rewrite_active_manifests", side_effect=_recording_rewrite),
        patch.object(recovery_mod, "_resolve_schedule_gap", side_effect=_resolve_schedule),
        patch.object(recovery_mod, "_resolve_roster_gap", side_effect=_resolve_roster),
        patch.object(recovery_mod, "_resolve_transaction_gap", side_effect=_resolve_transaction),
    ):
        result = run_yahoo_recovery(ctx, {"2021": {}, "2022": {}}, dead_man_minutes=1, local_db=local_db)

    first_non_empty = next(snapshot for snapshot in snapshots if snapshot)
    roster_manifest = first_non_empty["roster_2021.json"]
    assert roster_manifest.get("failed_weeks") == [8] or roster_manifest.get("failed_details") == ["wk8"]
    assert first_non_empty["schedule_2022.json"]["failed_year"] is True
    assert first_non_empty["transaction_2022.json"]["failed_year"] is True

    assert set(attempted) == {
        ("schedule", 2022, "full_year"),
        ("roster", 2021, "wk8"),
        ("transaction", 2022, "full_year"),
    }
    assert result.success is True
    assert result.unresolved == []
    assert not (tmp_path / recovery_mod.MANIFEST_DIR_NAME).exists()


def test_canonicalize_gap_units_collapses_to_fetcher_replay_units():
    gaps = [
        Gap("matchup", 2022, "wk3", "audit"),
        Gap("transaction", 2022, "txid::123", "audit"),
        Gap("schedule", 2022, "wk7", "audit"),
        Gap("draft", 2022, "pick::1", "audit"),
        Gap("roster", 2022, "wk9_mw::GUID_A_2022_9", "audit"),
    ]

    canonical = _canonicalize_gap_units(gaps)

    assert {gap.key for gap in canonical} == {
        ("matchup", 2022, "full_year"),
        ("transaction", 2022, "full_year"),
        ("schedule", 2022, "full_year"),
        ("draft", 2022, "full_year"),
        ("roster", 2022, "wk9"),
    }


def test_roster_full_year_does_not_expand_without_observed_matchups(tmp_path):
    db = LocalLeagueDB(tmp_path, "league")
    for table_name in ["league_settings", "matchup", "schedule"]:
        db.ensure_table(table_name)

    conn = db.connect()
    conn.execute(
        """
        INSERT INTO public.league_settings (db_name, year, num_teams, start_week, end_week, playoff_start_week)
        VALUES ('league', 2020, 8, 1, 18, 15)
        """
    )
    conn.execute(
        """
        INSERT INTO public.schedule (db_name, year, week, manager)
        VALUES ('league', 2020, 1, 'Team A'), ('league', 2020, 2, 'Team A')
        """
    )

    canonical = _canonicalize_gap_units(
        [Gap("roster", 2020, "full_year", "manifest")],
        local_db=db,
    )

    assert canonical == []


def test_detect_gaps_from_local_db_emits_canonical_fetch_units(tmp_path):
    settings = {"2025": {}}
    db = LocalLeagueDB(tmp_path, "league")
    for table_name in ["league_settings", "matchup", "player_fantasy", "schedule", "transactions"]:
        db.ensure_table(table_name)

    conn = db.connect()
    conn.execute(
        """
        INSERT INTO public.league_settings (db_name, year, num_teams, start_week, end_week, playoff_start_week)
        VALUES ('league', 2025, 2, 1, 1, 2)
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup (db_name, year, week, manager, manager_guid, franchise_id, manager_week, opponent, team_key)
        VALUES
          ('league', 2025, 1, 'Team A', 'GUID_A', '1', 'GUID_A_2025_1', 'Team B', '461.l.1.t.1'),
          ('league', 2025, 1, 'Team B', 'GUID_B', '2', 'GUID_B_2025_1', 'Team A', '461.l.1.t.2')
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy (db_name, year, week, manager, manager_guid, franchise_id, manager_week, yahoo_player_id, player)
        VALUES ('league', 2025, 1, 'Team B', 'GUID_B', '2', 'GUID_B_2025_1', '123', 'Player B')
        """
    )
    conn.execute(
        """
        INSERT INTO public.schedule (db_name, year, week, manager)
        VALUES ('league', 2025, 1, 'Team A')
        """
    )
    conn.execute(
        """
        INSERT INTO public.transactions (db_name, year, week, transaction_id, manager, transaction_type, yahoo_player_id, player)
        VALUES ('league', 2025, 1, 'tx1', 'Unknown', 'add', '101', 'Player X')
        """
    )

    gaps = _detect_gaps_from_local_db(db, settings)

    assert {gap.key for gap in gaps} == {
        ("roster", 2025, "wk1"),
        ("schedule", 2025, "full_year"),
        ("transaction", 2025, "full_year"),
    }


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._get_recovery_roster_fetcher")
def test_resolve_roster_gap_replays_whole_week_for_narrow_detail(mock_get_fetcher, mock_write_rows, tmp_path):
    fetcher = MagicMock()
    fetcher.fetch_teams.return_value = {
        "461.l.1.t.1": {"manager_name": "Team A", "manager_guid": "GUID_A"},
        "461.l.1.t.2": {"manager_name": "Team B", "manager_guid": "GUID_B"},
    }
    fetcher.fetch_all_rosters_for_week.return_value = (
        recovery_mod.pd.DataFrame(
            {
                "year": [2025],
                "week": [1],
                "player_name": ["Player A"],
                "manager_name": ["Team A"],
                "player_id": ["101"],
            }
        ),
        [],
    )
    mock_get_fetcher.return_value = fetcher

    gap = Gap("roster", 2025, "wk1_mw::GUID_A_2025_1", "audit")
    ctx = SimpleNamespace(league_ids={"2025": "461.l.1"}, oauth=MagicMock())

    result = _resolve_roster_gap(
        gap,
        ctx,
        tmp_path,
        oauth=ctx.oauth,
        roster_fetchers={},
        roster_teams={},
        local_db=MagicMock(),
    )

    assert result is True
    fetcher.fetch_all_rosters_for_week.assert_called_once()
    mock_write_rows.assert_called_once()


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._get_recovery_roster_fetcher")
def test_resolve_roster_gap_backfills_identity_from_matchup_team_key(mock_get_fetcher, mock_write_rows, tmp_path):
    db = LocalLeagueDB(tmp_path, "league")
    db.ensure_table("matchup")
    db.ensure_table("player_fantasy")

    conn = db.connect()
    conn.execute(
        """
        INSERT INTO public.matchup (
            db_name, year, week, manager, manager_guid, franchise_id, manager_week, opponent, team_key, team_name
        )
        VALUES ('league', 2025, 1, 'Canonical Alex', NULL, 'fid_alpha', 'fid_alpha_2025_1', 'Opponent', '461.l.1.t.1', 'Alpha Squad')
        """
    )

    fetcher = MagicMock()
    fetcher.fetch_teams.return_value = {
        "461.l.1.t.1": {"manager_name": "Alex", "manager_guid": None, "team_name": "Alpha Squad"},
    }
    fetcher.fetch_all_rosters_for_week.return_value = (
        recovery_mod.pd.DataFrame(
            {
                "year": [2025],
                "week": [1],
                "team_key": ["461.l.1.t.1"],
                "manager_name": ["Alex"],
                "player_name": ["Player A"],
                "player_id": ["101"],
            }
        ),
        [],
    )
    mock_get_fetcher.return_value = fetcher

    gap = Gap("roster", 2025, "wk1_mw::461.l.1.t.1_2025_1", "audit")
    ctx = SimpleNamespace(league_ids={"2025": "461.l.1"}, oauth=MagicMock())

    result = _resolve_roster_gap(
        gap,
        ctx,
        tmp_path,
        oauth=ctx.oauth,
        roster_fetchers={},
        roster_teams={},
        local_db=db,
    )

    written_df = mock_write_rows.call_args.args[2]

    assert result is True
    assert written_df[["franchise_id", "manager_week"]].drop_duplicates().values.tolist() == [
        ["fid_alpha", "fid_alpha_2025_1"]
    ]


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_matchups.weekly_matchup_data")
def test_resolve_matchup_gap_uses_original_fetcher(mock_fetch, mock_write_rows, tmp_path):
    mock_fetch.return_value = (
        recovery_mod.pd.DataFrame({"year": [2022], "week": [1], "manager": ["A"], "opponent": ["B"]}),
        [],
    )
    gap = Gap("matchup", 2022, "full_year", "manifest")
    ctx = SimpleNamespace(league_ids={"2022": "414.l.413370"}, oauth=MagicMock())

    result = _resolve_matchup_gap(gap, ctx, tmp_path, local_db=MagicMock())

    assert result is True
    mock_fetch.assert_called_once_with(ctx=ctx, year=2022, week=None)
    mock_write_rows.assert_called_once()


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_schedules.fetch_schedule_for_year")
def test_resolve_schedule_gap_uses_original_fetcher(mock_fetch, mock_write_rows, tmp_path):
    local_db = MagicMock()
    mock_fetch.return_value = recovery_mod.pd.DataFrame({"year": [2022], "week": [1], "manager": ["A"]})
    gap = Gap("schedule", 2022, "full_year", "manifest")
    ctx = SimpleNamespace(league_ids={"2022": "414.l.413370"}, oauth=MagicMock())

    result = _resolve_schedule_gap(gap, ctx, tmp_path, local_db=local_db)

    assert result is True
    mock_fetch.assert_called_once_with(ctx=ctx, year=2022, local_db=local_db)
    mock_write_rows.assert_called_once()


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_draft.fetch_draft_data")
def test_resolve_draft_gap_uses_original_fetcher(mock_fetch, mock_write_rows, tmp_path):
    mock_fetch.return_value = recovery_mod.pd.DataFrame(
        {"year": [2022], "pick": [1], "player": ["A"], "yahoo_player_id": ["101"]}
    )
    gap = Gap("draft", 2022, "full_year", "manifest")
    ctx = SimpleNamespace(league_ids={"2022": "414.l.413370"}, oauth=MagicMock())

    result = _resolve_draft_gap(gap, ctx, tmp_path, local_db=MagicMock())

    assert result is True
    mock_fetch.assert_called_once_with(ctx=ctx, year=2022)
    mock_write_rows.assert_called_once()


@patch("multi_league.data_fetchers.yahoo.yahoo_recovery._write_recovery_rows")
@patch("multi_league.data_fetchers.yahoo.yahoo_recovery.fetch_transactions_for_year_recovery")
def test_resolve_transaction_gap_replays_full_year_even_for_narrow_detail(mock_fetch, mock_write_rows, tmp_path):
    df = recovery_mod.pd.DataFrame(
        {
            "transaction_id": ["tx1", "tx2"],
            "yahoo_player_id": ["101", "102"],
            "player": ["A", "B"],
            "manager": ["M1", "M2"],
            "transaction_type": ["add", "drop"],
            "year": [2025, 2025],
            "week": [1, 2],
        }
    )
    mock_fetch.return_value = df
    gap = Gap("transaction", 2025, "txid::tx1", "audit")
    ctx = SimpleNamespace(league_ids={"2025": "461.l.90939"}, oauth=MagicMock())

    result = _resolve_transaction_gap(gap, ctx, tmp_path, local_db=MagicMock())

    assert result is True
    written_df = mock_write_rows.call_args.args[2]
    assert written_df.equals(df)
