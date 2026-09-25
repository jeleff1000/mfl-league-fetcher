"""Focused safety tests for ESPN's active-season refresh entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_unfinalized_espn_schedule_log_exposes_the_safe_period_witness(capsys):
    from refresh_espn_active_season import _finalized_espn_matchup_weeks

    client = SimpleNamespace(
        get_raw_schedule=lambda *_args: [
            {
                "matchupPeriodId": 2,
                "winner": "UNDECIDED",
                "home": {"teamId": 1},
                "away": {"teamId": 2},
            }
        ]
    )

    assert _finalized_espn_matchup_weeks(
        client,
        year=2026,
        weeks=[1],
        expected_team_ids=("1", "2"),
    ) == []
    output = capsys.readouterr().out
    assert "outcomes=['UNDECIDED']" in output
    assert "matchup_periods=[2]" in output
    assert "teams=['1', '2']" in output


def test_current_espn_period_rejects_a_zero_score_tie_shell():
    """ESPN can label an unplayed current matchup TIE before kickoff."""
    from refresh_espn_active_season import _finalized_espn_matchup_weeks

    client = SimpleNamespace(
        get_raw_schedule=lambda *_args: [
            {
                "matchupPeriodId": 3,
                "winner": "TIE",
                "home": {"teamId": 1, "totalPoints": 0.0},
                "away": {"teamId": 2, "totalPoints": 0.0},
            }
        ]
    )

    assert _finalized_espn_matchup_weeks(
        client,
        year=2026,
        weeks=[3],
        expected_team_ids=("1", "2"),
        current_matchup_period=3,
    ) == []


def test_closed_espn_period_derives_missing_winners_without_finalizing_current_period():
    from refresh_espn_active_season import _finalized_espn_matchup_weeks

    schedules = {
        1: [
            {
                "matchupPeriodId": 1,
                "winner": "UNDECIDED",
                "home": {"teamId": 1, "totalPoints": 112.5},
                "away": {"teamId": 2, "totalPoints": 99.25},
            }
        ],
        2: [
            {
                "matchupPeriodId": 2,
                "winner": "UNDECIDED",
                "home": {"teamId": 1, "totalPoints": 40.0},
                "away": {"teamId": 2, "totalPoints": 35.0},
            }
        ],
    }
    client = SimpleNamespace(get_raw_schedule=lambda _year, week: schedules[week])
    finalized_schedules = {}

    assert _finalized_espn_matchup_weeks(
        client,
        year=2026,
        weeks=[1, 2],
        expected_team_ids=("1", "2"),
        current_matchup_period=2,
        schedule_out=finalized_schedules,
    ) == [1]
    assert finalized_schedules[1][0]["winner"] == "HOME"
    assert 2 not in finalized_schedules


def test_closed_espn_period_still_rejects_an_incomplete_team_graph():
    from refresh_espn_active_season import _finalized_espn_matchup_weeks

    client = SimpleNamespace(
        get_raw_schedule=lambda *_args: [
            {
                "matchupPeriodId": 1,
                "winner": "UNDECIDED",
                "home": {"teamId": 1, "totalPoints": 112.5},
                "away": {"teamId": 2, "totalPoints": 99.25},
            }
        ]
    )

    assert _finalized_espn_matchup_weeks(
        client,
        year=2026,
        weeks=[1],
        expected_team_ids=("1", "2", "3", "4"),
        current_matchup_period=2,
    ) == []


def test_zero_espn_schedule_scores_are_hydrated_from_started_lineups():
    from refresh_espn_active_season import _hydrate_zero_espn_schedule_scores

    schedules = {
        1: [{
            "matchupPeriodId": 1,
            "winner": "TIE",
            "home": {"teamId": 1, "totalPoints": 0.0},
            "away": {"teamId": 2, "totalPoints": 0.0},
        }]
    }
    rosters = pd.DataFrame([
        {"week": 1, "team_key": "1", "fantasy_points": 70.0, "is_started": True},
        {"week": 1, "team_key": "1", "fantasy_points": 42.5, "is_started": True},
        {"week": 1, "team_key": "1", "fantasy_points": 10.0, "is_started": False},
        {"week": 1, "team_key": "2", "fantasy_points": 99.25, "is_started": True},
    ])

    _hydrate_zero_espn_schedule_scores(schedules, rosters)

    assert schedules[1][0]["home"]["totalPoints"] == 112.5
    assert schedules[1][0]["away"]["totalPoints"] == 99.25
    assert schedules[1][0]["winner"] == "HOME"


def test_zero_espn_schedule_scores_require_every_team_lineup():
    from refresh_espn_active_season import _hydrate_zero_espn_schedule_scores
    from multi_league.core.league_update_validation import IncompleteSourceError

    schedules = {
        1: [{
            "matchupPeriodId": 1,
            "winner": "TIE",
            "home": {"teamId": 1, "totalPoints": 0.0},
            "away": {"teamId": 2, "totalPoints": 0.0},
        }]
    }
    rosters = pd.DataFrame([
        {"week": 1, "team_key": "1", "fantasy_points": 112.5, "is_started": True},
    ])

    with pytest.raises(IncompleteSourceError, match="coverage mismatch.*2"):
        _hydrate_zero_espn_schedule_scores(schedules, rosters)


def test_full_espn_schedule_expands_every_regular_week_with_stable_identities():
    from refresh_espn_active_season import _full_espn_schedule_frame

    ctx = SimpleNamespace(
        get_league_id_for_year=lambda _year: 123,
        get_manager_name=lambda team_id, _team_name, _year: {1: "Gray", 2: "Will"}[team_id],
        get_manager_guid=lambda team_id, _year: {1: "guid-gray", 2: "guid-will"}[team_id],
        get_franchise_id=lambda team_id, _year: {1: "gray-0", 2: "will-0"}[team_id],
        get_team_name=lambda team_id, _year: {1: "Gray Team", 2: "Will Team"}[team_id],
    )
    raw = [
        {
            "matchupPeriodId": week,
            "playoffTierType": "NONE",
            "winner": "UNDECIDED",
            "home": {"teamId": 1},
            "away": {"teamId": 2},
        }
        for week in (1, 2, 3)
    ]

    schedule = _full_espn_schedule_frame(
        ctx=ctx,
        raw_schedule=raw,
        year=2026,
        regular_season_weeks=3,
        expected_team_ids=("1", "2"),
    )

    assert len(schedule) == 6
    assert sorted(schedule["week"].unique().tolist()) == [1, 2, 3]
    future = schedule.loc[schedule["week"].eq(3)].sort_values("franchise_id")
    assert future[["manager", "franchise_id", "opponent_franchise_id"]].values.tolist() == [
        ["Gray", "gray-0", "will-0"],
        ["Will", "will-0", "gray-0"],
    ]


def test_full_espn_schedule_rejects_a_truncated_regular_season():
    from multi_league.core.league_update_validation import IncompleteSourceError
    from refresh_espn_active_season import _full_espn_schedule_frame

    ctx = SimpleNamespace(
        get_league_id_for_year=lambda _year: 123,
        get_manager_name=lambda team_id, _team_name, _year: str(team_id),
        get_manager_guid=lambda team_id, _year: f"guid-{team_id}",
        get_franchise_id=lambda team_id, _year: f"franchise-{team_id}",
        get_team_name=lambda team_id, _year: f"Team {team_id}",
    )
    raw = [{
        "matchupPeriodId": 1,
        "playoffTierType": "NONE",
        "home": {"teamId": 1},
        "away": {"teamId": 2},
    }]

    with pytest.raises(IncompleteSourceError, match="missing regular-season weeks.*2"):
        _full_espn_schedule_frame(
            ctx=ctx,
            raw_schedule=raw,
            year=2026,
            regular_season_weeks=2,
            expected_team_ids=("1", "2"),
        )


def test_espn_refresh_rejects_a_missing_prior_week_matchup():
    from refresh_espn_active_season import assert_espn_closed_matchup_weeks
    from multi_league.core.league_refresh import RefreshScopeError

    with pytest.raises(
        RefreshScopeError,
        match=r"prior requested weeks.*\[1\]",
    ):
        assert_espn_closed_matchup_weeks(
            refresh_weeks=[1, 2],
            finalized_matchup_weeks=[],
        )


def test_espn_refresh_allows_the_latest_requested_week_to_remain_live():
    from refresh_espn_active_season import assert_espn_closed_matchup_weeks

    assert_espn_closed_matchup_weeks(
        refresh_weeks=[1, 2],
        finalized_matchup_weeks=[1],
    )


def test_espn_incomplete_matchups_write_an_actionable_receipt_before_failing():
    text = (ROOT / "scripts" / "refresh_espn_active_season.py").read_text(encoding="utf-8")

    catch = text.split("except (RefreshScopeError, IncompleteSourceError) as exc:", 1)[1].split("raise", 1)[0]
    assert 'receipt["status"] = "INCOMPLETE_SOURCE"' in catch
    assert 'receipt["error_code"] = "espn_provider_response_incomplete"' in catch
    assert "_write_receipt(receipt, args.json_out)" in catch


def test_espn_draft_names_are_hydrated_from_the_fetched_roster():
    import refresh_espn_active_season as worker

    league = SimpleNamespace(draft=[
        SimpleNamespace(playerId=1001, playerName=""),
        SimpleNamespace(playerId=1002, playerName="Already Named"),
    ])
    rosters = pd.DataFrame({
        "espn_player_id": [1001, 1001, 1002],
        "player": ["Roster Player", "Roster Player", "Already Named"],
    })
    hydrate = getattr(worker, "_hydrate_espn_draft_player_names", lambda *_args: {})

    names = hydrate(league, rosters)

    assert names == {"1001": "Roster Player", "1002": "Already Named"}
    assert [pick.playerName for pick in league.draft] == ["Roster Player", "Already Named"]


def test_espn_roster_fetch_exposes_box_scores_for_matchup_reuse():
    from multi_league.data_fetchers.espn.espn_rosters import fetch_espn_rosters_modern

    cached = {}
    league = SimpleNamespace(
        _uses_league_history=False,
        box_scores=lambda week: [] if week == 1 else (_ for _ in ()).throw(AssertionError(week)),
    )
    ctx = SimpleNamespace(get_league_id_for_year=lambda _year: 123, espn_s2=None, swid=None)

    assert fetch_espn_rosters_modern(
        ctx,
        2026,
        max_weeks=1,
        weeks=[1],
        client=SimpleNamespace(),
        league=league,
        box_scores_out=cached,
    ) is None
    assert cached == {1: []}


def test_espn_draft_fetch_reuses_the_loaded_league(monkeypatch):
    from multi_league.data_fetchers.espn import espn_draft

    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("a supplied league must not be fetched again")
        ),
    )
    monkeypatch.setattr(espn_draft, "_resolve_espn_nfl_id", lambda _player_id: "nfl-1")
    team = SimpleNamespace(team_id=1, team_name="One", roster=[])
    pick = SimpleNamespace(
        playerId=1001,
        playerName="Player One",
        round_num=1,
        round_pick=1,
        bid_amount=0,
        keeper_status=False,
        team=team,
        position="QB",
        proTeam="KC",
    )
    league = SimpleNamespace(draft=[pick], teams=[team])
    ctx = SimpleNamespace(
        get_league_id_for_year=lambda _year: 123,
        get_manager_name=lambda *_args, **_kwargs: "Manager One",
        get_manager_guid=lambda *_args, **_kwargs: "guid-1",
        get_franchise_id=lambda *_args, **_kwargs: "franchise-1",
        espn_s2=None,
        swid=None,
    )

    frame = espn_draft.fetch_espn_draft(ctx, 2026, league=league)

    assert frame is not None
    assert frame[["espn_player_id", "NFL_player_id", "manager"]].iloc[0].tolist() == [
        1001,
        "nfl-1",
        "Manager One",
    ]


def test_build_context_reuses_supplied_frontend_settings(tmp_path, monkeypatch):
    """The active worker must not re-read context after it has already hydrated it."""
    from multi_league.data_fetchers.espn import espn_api_client, espn_context
    from multi_league.utils import credential_store
    from refresh_espn_active_season import _build_context

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.team_to_guid = {}
            self.team_to_team_name = {}
            self.team_to_manager_by_year = {}
            self.team_to_guid_by_year = {}

        def save(self, path):
            path.write_text("{}", encoding="utf-8")

    class Client:
        def __init__(self, *_args):
            pass

        def get_league(self, _year):
            return SimpleNamespace(
                teams=[SimpleNamespace(team_id=1, team_name="One", owners=[{"id": "owner-1"}])]
            )

    class Reader:
        def query(self, *_args, **_kwargs):
            raise AssertionError("hydrated frontend settings must avoid a second Fly context query")

    monkeypatch.setattr(
        credential_store,
        "retrieve_espn_credentials",
        lambda _db_name, **_kwargs: {
            "league_id": 134179,
            "league_name": "Registry Name",
            "espn_s2": "s2",
            "swid": "{SWID}",
        },
    )
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    monkeypatch.setattr(espn_context, "ESPNContext", Context)
    monkeypatch.setattr(espn_context, "build_manager_names", lambda _teams: {1: "Manager One"})

    ctx, context_path, _client, _league = _build_context(
        reader=Reader(),
        db_name="private_espn",
        active_year=2026,
        work_dir=tmp_path,
        frontend_settings={
            "league_name": "Canonical Name",
            "manager_name_overrides": {"Manager One": "One"},
            "franchise_merges": [],
            "keeper_rules": None,
            "league_rules": None,
            "standings_weights": None,
            "is_private": True,
        },
    )

    assert ctx.league_name == "Canonical Name"
    assert ctx.is_private is True
    assert context_path.is_file()


def test_build_context_uses_saved_active_segment_id_not_registry_anchor(tmp_path, monkeypatch):
    """A year-specific ESPN identity must win over the credential row's old ID."""
    from multi_league.data_fetchers.espn import espn_api_client, espn_context
    from multi_league.utils import credential_store
    from refresh_espn_active_season import _build_context

    observed = {}

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.team_to_guid = {}
            self.team_to_team_name = {}
            self.team_to_manager_by_year = {}
            self.team_to_guid_by_year = {}

        def save(self, path):
            path.write_text("{}", encoding="utf-8")

    class Client:
        def __init__(self, league_id, *_args):
            observed["client_league_id"] = league_id

        def get_league(self, year):
            observed["year"] = year
            return SimpleNamespace(
                teams=[SimpleNamespace(team_id=1, team_name="One", owners=[{"id": "owner-1"}])]
            )

    monkeypatch.setattr(
        credential_store,
        "retrieve_espn_credentials",
        lambda _db_name, **_kwargs: {
            "league_id": 111111,
            "league_name": "Registry Name",
            "espn_s2": "s2",
            "swid": "{SWID}",
        },
    )
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    monkeypatch.setattr(espn_context, "ESPNContext", Context)
    monkeypatch.setattr(espn_context, "build_manager_names", lambda _teams: {1: "Manager One"})

    ctx, _, _, _ = _build_context(
        reader=SimpleNamespace(),
        db_name="private_espn",
        active_year=2026,
        active_league_id="222222",
        work_dir=tmp_path,
        frontend_settings={
            "league_name": "Canonical Name",
            "manager_name_overrides": {},
            "franchise_merges": [],
            "keeper_rules": None,
            "league_rules": None,
            "standings_weights": None,
            "is_private": True,
        },
    )

    assert ctx.league_id == 222222
    assert ctx.league_ids == {"2026": 222222}
    assert observed == {"client_league_id": 222222, "year": 2026}


def test_build_context_retains_the_complete_imported_espn_year_map(tmp_path, monkeypatch):
    from multi_league.data_fetchers.espn import espn_api_client, espn_context
    from multi_league.utils import credential_store
    from refresh_espn_active_season import _build_context

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.team_to_guid = {}
            self.team_to_team_name = {}
            self.team_to_manager_by_year = {}
            self.team_to_guid_by_year = {}

        def save(self, path):
            path.write_text("{}", encoding="utf-8")

    class Client:
        def __init__(self, *_args):
            pass

        def get_league(self, _year):
            return SimpleNamespace(
                teams=[SimpleNamespace(team_id=1, team_name="One", owners=[{"id": "owner-1"}])]
            )

    monkeypatch.setattr(
        credential_store,
        "retrieve_espn_credentials",
        lambda _db_name, **_kwargs: {
            "league_id": 222,
            "league_name": "Registry Name",
            "espn_s2": "s2",
            "swid": "{SWID}",
        },
    )
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    monkeypatch.setattr(espn_context, "ESPNContext", Context)
    monkeypatch.setattr(espn_context, "build_manager_names", lambda _teams: {1: "Manager One"})

    ctx, _, _, _ = _build_context(
        reader=SimpleNamespace(),
        db_name="private_espn",
        active_year=2026,
        active_league_id="222",
        league_ids={"2024": "222", "2025": "222", "2026": "222"},
        work_dir=tmp_path,
        frontend_settings={
            "league_name": "Canonical Name",
            "manager_name_overrides": {},
            "franchise_merges": [],
            "keeper_rules": None,
            "league_rules": None,
            "standings_weights": None,
            "is_private": True,
        },
    )

    assert ctx.league_ids == {"2024": 222, "2025": 222, "2026": 222}


def _draft_payload(*, drafted=True, pick_count=6, rounds=2):
    return {
        "draftDetail": {
            "drafted": drafted,
            "inProgress": False,
            "picks": [
                {"overallPickNumber": i, "playerId": 1000 + i}
                for i in range(1, pick_count + 1)
            ],
        },
        "settings": {
            "size": 3,
            "rosterSettings": {"lineupSlotCounts": {"0": 1, "20": rounds - 1, "21": 2}},
            "draftSettings": {"type": "SNAKE", "pickOrder": [1, 2, 3]},
        },
    }


def _parsed_draft(count):
    return [
        SimpleNamespace(playerId=1000 + i, playerName=f"Player {i}")
        for i in range(1, count + 1)
    ]


def test_espn_draft_manifest_accepts_verified_afi_2026_pick_shape():
    from refresh_espn_active_season import _espn_draft_manifest

    # Fly's encrypted credential yielded a complete ESPN response with 12
    # teams, 14 non-IR roster slots and overall picks 1..168.
    payload = _draft_payload(pick_count=168, rounds=14)
    payload["settings"]["size"] = 12
    payload["settings"]["draftSettings"]["pickOrder"] = list(range(1, 13))
    payload["settings"]["rosterSettings"]["lineupSlotCounts"] = {
        "0": 1, "2": 2, "4": 2, "6": 1, "16": 1, "17": 1,
        "20": 5, "21": 2, "23": 1,
    }
    client = SimpleNamespace(get_raw_league=lambda *_args: payload)
    league = SimpleNamespace(draft=_parsed_draft(168))
    manifest, no_draft = _espn_draft_manifest(client, league, 2026)
    assert not no_draft
    assert len(manifest) == 168


def test_espn_draft_manifest_requires_raw_complete_pick_identities():
    from refresh_espn_active_season import _espn_draft_manifest

    client = SimpleNamespace(get_raw_league=lambda *_args: _draft_payload())
    league = SimpleNamespace(draft=_parsed_draft(6))
    manifest, absent = _espn_draft_manifest(client, league, 2026)
    assert manifest.equals(pd.DataFrame({
        "pick": [1, 2, 3, 4, 5, 6],
        "espn_player_id": [1001, 1002, 1003, 1004, 1005, 1006],
        "player": [f"Player {i}" for i in range(1, 7)],
    }))
    assert absent is False


def test_espn_draft_manifest_rejects_empty_or_short_parsed_and_raw_drafts():
    import pytest
    from multi_league.core.league_refresh import RefreshScopeError
    from refresh_espn_active_season import _espn_draft_manifest

    for raw_count, parsed_count in ((6, 0), (3, 3), (6, 3)):
        client = SimpleNamespace(get_raw_league=lambda *_args, n=raw_count: _draft_payload(pick_count=n))
        league = SimpleNamespace(draft=_parsed_draft(parsed_count))
        with pytest.raises(RefreshScopeError):
            _espn_draft_manifest(client, league, 2026)


def test_espn_draft_manifest_confirms_absence_only_from_raw_undrafted_status():
    from refresh_espn_active_season import _espn_draft_manifest

    client = SimpleNamespace(get_raw_league=lambda *_args: _draft_payload(drafted=False, pick_count=0))
    manifest, absent = _espn_draft_manifest(client, SimpleNamespace(draft=[]), 2026)
    assert manifest.empty
    assert absent is True


def test_espn_draft_manifest_confirms_undrafted_placeholder_slots_are_not_picks():
    from refresh_espn_active_season import _espn_draft_manifest

    payload = _draft_payload(drafted=False, pick_count=6)
    for pick in payload["draftDetail"]["picks"]:
        pick["playerId"] = -1
    client = SimpleNamespace(get_raw_league=lambda *_args: payload)

    manifest, absent = _espn_draft_manifest(client, SimpleNamespace(draft=[]), 2026)

    assert manifest.empty
    assert absent is True


def test_espn_placeholder_cleanup_removes_only_unresolved_active_draft_rows():
    import pytest
    from multi_league.core.league_refresh import RefreshScopeError
    import refresh_espn_active_season as worker

    class Connection:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))

    class LocalDB:
        def __init__(self):
            self.conn = Connection()

        def table_exists(self, table):
            return table == "draft"

        def read_table(self, table, *, year):
            assert (table, year) == ("draft", 2026)
            return pd.DataFrame({
                "db_name": ["league", "league"],
                "year": [2026, 2026],
                "platform": ["espn", "espn"],
                "espn_player_id": [-1, -1],
                "player": ["Unknown", "Unknown"],
            })

        def connect(self):
            return self.conn

    local_db = LocalDB()
    discard = getattr(worker, "_discard_espn_placeholder_draft", lambda *_args, **_kwargs: 0)

    removed = discard(local_db, db_name="league", year=2026, league_id="123")

    assert removed == 2
    assert len(local_db.conn.calls) == 1
    assert "DELETE FROM public.draft" in local_db.conn.calls[0][0]

    local_db.read_table = lambda *_args, **_kwargs: pd.DataFrame({
        "db_name": ["league"],
        "year": [2026],
        "platform": ["espn"],
        "espn_player_id": [1001],
        "player": ["Real Player"],
    })
    with pytest.raises(RefreshScopeError, match="refusing to remove real picks"):
        discard(local_db, db_name="league", year=2026, league_id="123")
    assert len(local_db.conn.calls) == 1


def test_espn_placeholder_cleanup_requests_only_draft_partition_deletion():
    import refresh_espn_active_season as worker

    assert worker._explicit_empty_partitions({"placeholder_draft_rows_removed": 128}) == {"draft"}
    assert worker._explicit_empty_partitions({"placeholder_draft_rows_removed": 0}) == set()
    assert worker._explicit_empty_partitions({}) == set()


def test_espn_draft_manifest_accepts_complete_picks_when_drafted_flag_is_stale():
    from refresh_espn_active_season import _espn_draft_manifest

    client = SimpleNamespace(
        get_raw_league=lambda *_args: _draft_payload(drafted=False, pick_count=6)
    )
    manifest, absent = _espn_draft_manifest(
        client,
        SimpleNamespace(draft=_parsed_draft(6)),
        2026,
    )

    assert manifest["pick"].tolist() == [1, 2, 3, 4, 5, 6]
    assert absent is False


def test_espn_draft_manifest_ignores_verified_trailing_empty_rounds():
    from refresh_espn_active_season import _espn_draft_manifest

    payload = _draft_payload(pick_count=12, rounds=4)
    for pick in payload["draftDetail"]["picks"][-3:]:
        pick["playerId"] = 0
    parsed = _parsed_draft(9) + [
        SimpleNamespace(playerId=0, playerName="Unknown")
        for _ in range(3)
    ]

    manifest, absent = _espn_draft_manifest(
        SimpleNamespace(get_raw_league=lambda *_args: payload),
        SimpleNamespace(draft=parsed),
        2026,
    )

    assert absent is False
    assert manifest["pick"].tolist() == list(range(1, 10))
    assert manifest["espn_player_id"].tolist() == list(range(1001, 1010))


def test_espn_draft_manifest_rejects_unresolved_player_identity():
    import pytest
    from multi_league.core.league_refresh import RefreshScopeError
    from refresh_espn_active_season import _espn_draft_manifest

    parsed = _parsed_draft(6)
    parsed[0].playerName = ""
    client = SimpleNamespace(get_raw_league=lambda *_args: _draft_payload())

    with pytest.raises(
        RefreshScopeError,
        match=r"unresolved player identities \(count=1, sample=\['1001'\]\)",
    ):
        _espn_draft_manifest(client, SimpleNamespace(draft=parsed), 2026)


def test_espn_draft_manifest_reports_the_rejected_completion_witness():
    import pytest
    from multi_league.core.league_refresh import RefreshScopeError
    from refresh_espn_active_season import _espn_draft_manifest

    payload = _draft_payload(drafted=True, pick_count=1)
    payload["draftDetail"]["inProgress"] = True
    client = SimpleNamespace(get_raw_league=lambda *_args: payload)
    with pytest.raises(
        RefreshScopeError,
        match=r"drafted=True, in_progress=True, raw_picks=1, parsed_picks=1",
    ):
        _espn_draft_manifest(client, SimpleNamespace(draft=_parsed_draft(1)), 2026)
