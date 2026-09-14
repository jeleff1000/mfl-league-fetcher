import datetime as dt
from types import SimpleNamespace

import pandas as pd

from fleaflicker_initial_import import _bootstrap_context
from multi_league.core.canonical_draft import normalize_draft_df
from multi_league.core.canonical_roster import normalize_roster_df
from multi_league.core.canonical_settings import flatten_settings
from multi_league.core.canonical_transaction import normalize_transaction_df
from multi_league.data_fetchers.fleaflicker.fleaflicker_context import FleaflickerContext
from multi_league.data_fetchers.fleaflicker.fleaflicker_rosters import fetch_fleaflicker_rosters
from multi_league.data_fetchers.fleaflicker.fleaflicker_transactions import (
    _populate_trade_counterparties,
    _year_from_ms,
)


class _DiscoveryClient:
    def __init__(self, years: list[int]):
        self.years = years
        self.config = SimpleNamespace(rate_limit_per_min=0)
        self.discover_calls: list[tuple[str, int, int]] = []

    def discover_available_years(self, league_id: str, *, start_year: int, end_year: int) -> list[int]:
        self.discover_calls.append((league_id, start_year, end_year))
        return [year for year in self.years if start_year <= year <= end_year]

    def fetch_standings(self, league_id: str, season: int | None = None) -> dict:
        return {
            "league": {"name": "Football To The Groin"},
            "divisions": [{"teams": [{"id": 1}, {"id": 2}]}],
        }


def _fleaflicker_args(tmp_path, **overrides):
    defaults = {
        "league_id": "311150",
        "context": None,
        "data_dir": str(tmp_path),
        "database_name": "fleaflicker_public_311150",
        "start_year": None,
        "end_year": None,
        "year": None,
        "import_mode": "full",
        "discover_years": False,
        "max_workers": 12,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_fleaflicker_full_bootstrap_discovers_available_history(tmp_path, monkeypatch):
    monkeypatch.setattr("fleaflicker_initial_import._default_full_history_end_year", lambda: 2025)
    client = _DiscoveryClient([2005, 2006, 2007, 2025])

    ctx = _bootstrap_context(_fleaflicker_args(tmp_path), client)

    assert ctx.start_year == 2005
    assert ctx.end_year == 2025
    assert set(ctx.league_ids) == {"2005", "2006", "2007", "2025"}
    assert client.discover_calls == [("311150", 2005, 2025)]


def test_fleaflicker_explicit_full_years_skip_discovery(tmp_path):
    client = _DiscoveryClient([2005, 2006, 2007, 2025])

    ctx = _bootstrap_context(_fleaflicker_args(tmp_path, start_year=2024, end_year=2025), client)

    assert ctx.start_year == 2024
    assert ctx.end_year == 2025
    assert set(ctx.league_ids) == {"2024", "2025"}
    assert client.discover_calls == []


def test_fleaflicker_transaction_year_uses_timestamp_calendar_year():
    feb_2025_ms = int(dt.datetime(2025, 2, 28, tzinfo=dt.UTC).timestamp() * 1000)

    assert _year_from_ms(feb_2025_ms, fallback_year=2005) == 2025
    assert _year_from_ms(feb_2025_ms, fallback_year=2025) == 2025


def test_fleaflicker_player_id_lands_in_platform_column():
    roster = normalize_roster_df(
        pd.DataFrame(
            [
                {
                    "year": 2025,
                    "week": 1,
                    "manager": "Manager",
                    "manager_guid": "ff_owner_1",
                    "player_id": 18372,
                    "player": "Jayden Daniels",
                    "position": "QB",
                    "fantasy_position": "QB",
                }
            ]
        ),
        platform="fleaflicker",
        league_id="277588",
    )
    draft = normalize_draft_df(
        pd.DataFrame([{"year": 2025, "round": 1, "pick": 1, "player_id": 18372, "player": "Jayden Daniels"}]),
        platform="fleaflicker",
        league_id="277588",
    )
    transaction = normalize_transaction_df(
        pd.DataFrame(
            [
                {
                    "transaction_id": "tx1",
                    "year": 2025,
                    "week": 1,
                    "player_id": 18372,
                    "player": "Jayden Daniels",
                }
            ]
        ),
        platform="fleaflicker",
        league_id="277588",
    )

    for frame in (roster, draft, transaction):
        assert frame.loc[0, "fleaflicker_player_id"] == "18372"
        assert pd.isna(frame.loc[0, "yahoo_player_id"])
        assert frame.loc[0, "platform"] == "fleaflicker"


class _RosterFallbackClient:
    def fetch_standings(self, league_id, season=None):
        return {
            "divisions": [
                {
                    "id": 1,
                    "teams": [
                        {
                            "id": 10,
                            "name": "Team A",
                            "owners": [{"id": 100, "displayName": "Manager A"}],
                        }
                    ],
                }
            ]
        }

    def fetch_scoreboard(self, league_id, season, scoring_period):
        return {"eligibleSchedulePeriods": [{"value": 1}]}

    def fetch_league_rosters(self, league_id, season, scoring_period):
        return {
            "rosters": [
                {
                    "team": {"id": 10, "name": "Team A"},
                    "players": [
                        {
                            "proPlayer": {
                                "id": 123,
                                "nameFull": "Roster Player",
                                "position": "RB",
                                "proTeamAbbreviation": "GB",
                            },
                            "requestedGamesPeriod": {"season": 2025},
                            "viewingActualPoints": {"value": 8.5},
                        }
                    ],
                }
            ]
        }

    def fetch_roster(self, league_id, team_id, season, scoring_period):
        return None


def test_fleaflicker_rosters_keep_league_roster_fallback_when_team_roster_blocked(tmp_path):
    ctx = FleaflickerContext(
        league_id="311150",
        league_name="Fallback League",
        start_year=2025,
        end_year=2025,
        data_directory=tmp_path,
        league_ids={"2025": "311150"},
        max_workers=1,
    )

    roster = fetch_fleaflicker_rosters(ctx, 2025, client=_RosterFallbackClient())

    assert len(roster) == 1
    assert roster.loc[0, "manager"] == "Manager A"
    assert roster.loc[0, "player"] == "Roster Player"
    assert roster.loc[0, "fleaflicker_player_id"] == "123"
    assert roster.loc[0, "fantasy_position"] == "BN"
    assert roster.loc[0, "is_rostered"] == 1


class _CurrentRosterForOldSeasonClient(_RosterFallbackClient):
    def fetch_standings(self, league_id, season=None):
        return {
            "divisions": [
                {
                    "id": 1,
                    "teams": [
                        {
                            "id": 10,
                            "name": "Team A",
                            "owners": [{"id": 100, "displayName": "Manager A"}],
                        }
                    ],
                }
            ]
        }

    def fetch_scoreboard(self, league_id, season, scoring_period):
        return {"eligibleSchedulePeriods": [{"value": 1}]}

    def fetch_league_rosters(self, league_id, season, scoring_period):
        return {
            "rosters": [
                {
                    "team": {"id": 10, "name": "Team A"},
                    "players": [
                        {
                            "proPlayer": {"id": 123, "nameFull": "Current Player", "position": "RB"},
                            "requestedGamesPeriod": {"ordinal": 1},
                        }
                    ],
                }
            ]
        }

    def fetch_roster(self, league_id, team_id, season, scoring_period):
        return {
            "groups": [
                {
                    "group": "START",
                    "slots": [
                        {
                            "position": {"label": "RB"},
                            "leaguePlayer": {
                                "proPlayer": {"id": 123, "nameFull": "Current Player", "position": "RB"},
                                "requestedGamesPeriod": {"ordinal": 1},
                            },
                        }
                    ],
                }
            ]
        }


def test_fleaflicker_rosters_skip_current_roster_payload_for_old_season(tmp_path):
    ctx = FleaflickerContext(
        league_id="311150",
        league_name="Old League",
        start_year=2005,
        end_year=2005,
        data_directory=tmp_path,
        league_ids={"2005": "311150"},
        max_workers=1,
    )

    roster = fetch_fleaflicker_rosters(ctx, 2005, client=_CurrentRosterForOldSeasonClient())

    assert roster.empty


def test_fleaflicker_settings_extracts_basic_roster_and_scoring():
    raw = {
        "rules": {
            "rosterPositions": [
                {"label": "QB", "group": "START", "start": 1},
                {"label": "RB", "group": "START", "start": 2},
                {"label": "RB/WR/TE", "group": "START", "start": 1},
                {"label": "BN", "group": "BENCH", "max": 6},
            ],
            "groups": [
                {
                    "scoringRules": [
                        {
                            "category": {"nameSingular": "Reception"},
                            "points": {"value": 1},
                        },
                        {
                            "category": {"nameSingular": "Passing TD"},
                            "points": {"value": 4},
                        },
                    ]
                }
            ],
        },
        "standings": {"league": {"size": 12}},
        "scoreboards": [{"eligibleSchedulePeriods": [{"value": 1}, {"value": 17}]}],
    }

    flat = flatten_settings(raw, platform="fleaflicker", year=2025, league_key="277588")

    assert flat["platform"] == "fleaflicker"
    assert flat["num_teams"] == 12
    assert flat["scoring_type"] == "ppr"
    assert flat["scoring_rec"] == 1
    assert flat["scoring_pass_td"] == 4
    assert flat["roster_QB"] == 1
    assert flat["roster_RB"] == 2
    assert flat["roster_FLX"] == 1
    assert flat["end_week"] == 17
    assert flat["playoff_start_week"] == 18
    assert flat["playoff_teams"] == 0


def test_fleaflicker_trade_id_expands_to_sent_and_received_perspectives():
    rows = [
        {
            "_fleaflicker_trade_id": "10261088",
            "transaction_id": "trade_10261088",
            "transaction_sequence": 1,
            "transaction_type": "trade",
            "year": 2025,
            "week": 1,
            "manager": "A",
            "manager_guid": "ff_team_1",
            "franchise_id": "ff_team_1",
            "team_name": "Team A",
            "destination_manager": "A",
            "destination_manager_guid": "ff_team_1",
            "destination_franchise_id": "ff_team_1",
            "destination_team_name": "Team A",
            "player": "Player One",
            "player_id": "101",
        },
        {
            "_fleaflicker_trade_id": "10261088",
            "transaction_id": "trade_10261088",
            "transaction_sequence": 2,
            "transaction_type": "trade",
            "year": 2025,
            "week": 1,
            "manager": "B",
            "manager_guid": "ff_team_2",
            "franchise_id": "ff_team_2",
            "team_name": "Team B",
            "destination_manager": "B",
            "destination_manager_guid": "ff_team_2",
            "destination_franchise_id": "ff_team_2",
            "destination_team_name": "Team B",
            "player": "Player Two",
            "player_id": "202",
        },
    ]

    _populate_trade_counterparties(rows)
    normalized = normalize_transaction_df(pd.DataFrame(rows), platform="fleaflicker", league_id="311150")

    assert set(normalized["trade_direction"]) == {"received", "sent"}
    assert len(normalized) == 4
    assert set(normalized["transaction_id"]) == {"trade_10261088"}
    assert normalized["source_manager"].notna().all()
    assert normalized["source_franchise_id"].notna().all()


def test_fleaflicker_trade_counterparty_can_come_from_pick_only_side():
    rows = [
        {
            "_fleaflicker_trade_id": "10008830",
            "transaction_id": "trade_10008830",
            "transaction_sequence": 1,
            "transaction_type": "trade",
            "year": 2025,
            "week": 6,
            "manager": "A",
            "manager_guid": "ff_team_1",
            "franchise_id": "ff_team_1",
            "team_name": "Team A",
            "destination_manager": "A",
            "destination_manager_guid": "ff_team_1",
            "destination_franchise_id": "ff_team_1",
            "destination_team_name": "Team A",
            "player": "Player One",
            "player_id": "101",
        }
    ]
    parties = {
        "10008830": {
            "ff_team_1": {
                "manager": "A",
                "manager_guid": "ff_team_1",
                "franchise_id": "ff_team_1",
                "team_name": "Team A",
            },
            "ff_team_2": {
                "manager": "B",
                "manager_guid": "ff_team_2",
                "franchise_id": "ff_team_2",
                "team_name": "Team B",
            },
        }
    }

    _populate_trade_counterparties(rows, parties)
    normalized = normalize_transaction_df(pd.DataFrame(rows), platform="fleaflicker", league_id="311150")

    assert len(normalized) == 2
    assert set(normalized["manager"]) == {"A", "B"}
    assert set(normalized["trade_direction"]) == {"received", "sent"}
    assert normalized["source_manager"].notna().all()
    assert normalized["source_franchise_id"].notna().all()
