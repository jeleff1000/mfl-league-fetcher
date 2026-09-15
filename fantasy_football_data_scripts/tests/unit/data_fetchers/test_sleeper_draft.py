from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))
if str(SCRIPT_ROOT.parent / "scripts") not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT.parent / "scripts"))

from multi_league.data_fetchers.sleeper import sleeper_draft
from multi_league.data_fetchers.sleeper.sleeper_draft import SleeperDraftFetcher


def test_sleeper_drafts_endpoint_null_is_not_confirmed_no_draft():
    import pytest
    from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient

    client = object.__new__(SleeperAPIClient)
    client._get = lambda _endpoint: None
    with pytest.raises(ValueError, match="drafts endpoint.*array"):
        client.get_league_drafts("2026-league")


def test_sleeper_no_draft_requires_explicit_active_draft_identity_field():
    import pytest
    from refresh_sleeper_active_season import _sleeper_confirmed_no_draft

    manifest = pd.DataFrame(columns=["draft_id", "pick"])
    with pytest.raises(ValueError, match="draft_id"):
        _sleeper_confirmed_no_draft({"league_id": "2026-league"}, manifest)
    assert _sleeper_confirmed_no_draft({"league_id": "2026-league", "draft_id": None}, manifest)
    assert not _sleeper_confirmed_no_draft(
        {"league_id": "2026-league", "draft_id": "real-draft"}, manifest,
    )


class _FakeContext:
    cache_directory = Path(".")
    manager_name_overrides = {}

    def get_league_id_for_year(self, year: int) -> str:
        return f"{year}-league"


class _FakeClient:
    def __init__(self, slot_to_roster_id: dict[str, int | None] | None = None):
        self.slot_to_roster_id = slot_to_roster_id or {"11": 42}

    def get_league_rosters(self, league_id: str):
        return [{"roster_id": 42, "owner_id": "user-42"}]

    def get_league_users(self, league_id: str):
        return [
            {
                "user_id": "user-42",
                "display_name": "beanboismokes",
                "metadata": {"team_name": "The Bean Room"},
            }
        ]

    def get_league_drafts(self, league_id: str):
        return [
            {
                "draft_id": "draft-2024",
                "type": "snake",
                "status": "complete",
                "season": "2024",
                "slot_to_roster_id": self.slot_to_roster_id,
                "draft_order": {"user-42": 11},
                "settings": {"player_type": 0, "rounds": 31},
            }
        ]

    def get_draft_picks(self, draft_id: str):
        return [
            {
                "pick_no": 11,
                "round": 1,
                "draft_slot": 11,
                "roster_id": None,
                "player_id": "1234",
                "metadata": {"years_exp": "3"},
            }
        ]


class _FakePlayerCache:
    VALID_NFL_TEAMS = set()
    TEAM_ABBREV_TO_NAME = {}
    NFL_TEAM_SHORT_NAMES = {}

    def refresh_if_stale(self, client):
        return None

    def get_player_name(self, player_id: str) -> str:
        return "Test Player"

    def get_player_position(self, player_id: str) -> str:
        return "WR"

    def get_player_team(self, player_id: str) -> str:
        return "NYJ"


def test_sleeper_draft_uses_slot_to_roster_when_pick_roster_id_missing(monkeypatch):
    monkeypatch.setattr(sleeper_draft, "_resolve_sleeper_nfl_id", lambda player_id: "00-TEST")

    fetcher = SleeperDraftFetcher(_FakeContext(), client=_FakeClient(), player_cache=_FakePlayerCache())

    df = fetcher.fetch_draft_for_year(2024)

    assert df.loc[0, "team_key"] == "42"
    assert df.loc[0, "manager"] == "beanboismokes"
    assert df.loc[0, "manager_guid"] == "user-42"
    assert df.loc[0, "team_name"] == "The Bean Room"
    assert df.loc[0, "draft_slot_roster_id"] == 42


def test_sleeper_draft_uses_blank_slot_to_roster_fallback(monkeypatch):
    monkeypatch.setattr(sleeper_draft, "_resolve_sleeper_nfl_id", lambda player_id: "00-TEST")

    fetcher = SleeperDraftFetcher(
        _FakeContext(),
        client=_FakeClient(slot_to_roster_id={"11": None, "": 42}),
        player_cache=_FakePlayerCache(),
    )

    df = fetcher.fetch_draft_for_year(2024)

    assert df.loc[0, "team_key"] == "42"
    assert df.loc[0, "manager"] == "beanboismokes"
    assert df.loc[0, "draft_slot_roster_id"] == 42


def test_sleeper_draft_manifest_includes_each_current_season_draft_identity():
    """A partial local rookie draft cannot hide behind a complete startup draft."""
    fetcher = SleeperDraftFetcher(_FakeContext(), client=_FakeClient(), player_cache=_FakePlayerCache())

    manifest = fetcher.fetch_draft_manifest_for_year(2024)

    assert manifest.equals(pd.DataFrame({"draft_id": ["draft-2024"], "pick": [11]}))


def test_completed_primary_sleeper_draft_rejects_consistently_short_pick_responses():
    import pytest

    fetcher = SleeperDraftFetcher(_FakeContext(), client=_FakeClient(), player_cache=_FakePlayerCache())
    with pytest.raises(ValueError, match="primary draft.*incomplete"):
        fetcher.fetch_draft_manifest_for_year(2024, expected_primary_draft_id="draft-2024")


def test_sparse_supplemental_draft_does_not_invalidate_complete_primary_draft():
    class Client(_FakeClient):
        def get_league_drafts(self, league_id: str):
            primary = {**super().get_league_drafts(league_id)[0], "draft_id": "primary", "settings": {"rounds": 1, "teams": 1}, "draft_order": {"user-42": 1}}
            supplemental = {**primary, "draft_id": "supplemental", "settings": {"rounds": 10, "teams": 1}}
            return [primary, supplemental]

        def get_draft_picks(self, draft_id: str):
            pick = super().get_draft_picks(draft_id)[0]
            return [{**pick, "pick_no": 1 if draft_id == "primary" else 9}]

    fetcher = SleeperDraftFetcher(_FakeContext(), client=Client(), player_cache=_FakePlayerCache())
    manifest = fetcher.fetch_draft_manifest_for_year(2024, expected_primary_draft_id="primary")
    assert set(zip(manifest.draft_id, manifest.pick)) == {("primary", 1), ("supplemental", 9)}
