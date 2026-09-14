from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.sleeper import sleeper_draft
from multi_league.data_fetchers.sleeper.sleeper_draft import SleeperDraftFetcher


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
