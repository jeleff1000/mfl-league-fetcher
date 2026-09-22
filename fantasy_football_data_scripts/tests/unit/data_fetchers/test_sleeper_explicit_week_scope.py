"""The weekly worker must never apply exploratory full-import stop rules."""

from types import SimpleNamespace

import pytest

from multi_league.data_fetchers.sleeper.sleeper_matchups import SleeperMatchupFetcher
from multi_league.data_fetchers.sleeper.sleeper_schedules import SleeperScheduleFetcher


def _context():
    return SimpleNamespace(get_league_id_for_year=lambda year: "played-league")


def test_explicit_matchup_scope_reaches_later_week_after_three_empty_responses(monkeypatch):
    fetcher = SleeperMatchupFetcher(_context(), client=object())
    monkeypatch.setattr(fetcher, "_get_roster_mappings", lambda league_id: {})
    monkeypatch.setattr(fetcher, "_calculate_derived_metrics", lambda frame: frame)
    monkeypatch.setattr(
        fetcher, "fetch_matchups_for_week",
        lambda league_id, year, week, roster_map: (
            [{"year": year, "week": week, "manager": "A", "team_points": 100.0}]
            if week == 17 else []
        ),
    )

    result = fetcher.fetch_matchups_for_year(2025, weeks=[14, 15, 16, 17])

    assert result["week"].tolist() == [17]


def test_explicit_matchup_scope_propagates_failed_week_request(monkeypatch):
    fetcher = SleeperMatchupFetcher(_context(), client=object())
    monkeypatch.setattr(fetcher, "_get_roster_mappings", lambda league_id: {})

    def response(league_id, year, week, roster_map):
        if week == 16:
            raise RuntimeError("provider timeout")
        return []

    monkeypatch.setattr(fetcher, "fetch_matchups_for_week", response)

    with pytest.raises(RuntimeError, match="provider timeout"):
        fetcher.fetch_matchups_for_year(2025, weeks=[15, 16, 17])


def test_explicit_schedule_scope_reaches_later_week_after_three_empty_responses(monkeypatch):
    fetcher = SleeperScheduleFetcher(_context(), client=object())
    monkeypatch.setattr(fetcher, "_get_roster_mappings", lambda league_id: {})
    monkeypatch.setattr(
        fetcher, "fetch_schedule_for_week",
        lambda league_id, year, week, roster_map: (
            [{"year": year, "week": week, "manager": "A"}] if week == 17 else []
        ),
    )

    result = fetcher.fetch_schedule_for_year(2025, weeks=[14, 15, 16, 17])

    assert result["week"].tolist() == [17]


def test_explicit_schedule_scope_propagates_failed_week_request(monkeypatch):
    fetcher = SleeperScheduleFetcher(_context(), client=object())
    monkeypatch.setattr(fetcher, "_get_roster_mappings", lambda league_id: {})

    def response(league_id, year, week, roster_map):
        if week == 16:
            raise RuntimeError("provider timeout")
        return []

    monkeypatch.setattr(fetcher, "fetch_schedule_for_week", response)

    with pytest.raises(RuntimeError, match="provider timeout"):
        fetcher.fetch_schedule_for_year(2025, weeks=[15, 16, 17])


def test_schedule_rows_carry_both_provider_team_identities(monkeypatch):
    client = SimpleNamespace(
        get_league_matchups=lambda league_id, week: [
            {"roster_id": 1, "matchup_id": 1, "points": 101.0},
            {"roster_id": 2, "matchup_id": 1, "points": 99.0},
        ]
    )
    fetcher = SleeperScheduleFetcher(_context(), client=client)
    monkeypatch.setattr(
        fetcher,
        "_get_playoff_structure",
        lambda league_id: {"playoff_week_start": 15, "playoff_teams": 6},
    )
    roster_map = {
        1: {"manager_name": "A", "manager_guid": "owner-a", "team_name": "Alpha"},
        2: {"manager_name": "B", "manager_guid": "owner-b", "team_name": "Beta"},
    }

    rows = fetcher.fetch_schedule_for_week("league", 2026, 3, roster_map)

    assert rows[0]["team_key"] == "1"
    assert rows[0]["opponent_team_key"] == "2"
    assert rows[0]["manager_guid"] == "owner-a"
    assert rows[0]["opponent_guid"] == "owner-b"
    assert rows[1]["team_key"] == "2"
    assert rows[1]["opponent_team_key"] == "1"
