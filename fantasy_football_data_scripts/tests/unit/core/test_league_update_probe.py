from __future__ import annotations

from copy import deepcopy

import pytest

from multi_league.core.league_update_probe import (
    ProviderProbeError,
    probe_espn,
    probe_multiplatform,
    probe_sleeper,
    probe_yahoo_oauth,
)


class FakeYahooClient:
    def __init__(self, *, status: int | None = None, missing_team: bool = False):
        self.status = status
        self.missing_team = missing_team

    def _raise(self):
        if self.status is not None:
            error = RuntimeError(f"Yahoo HTTP {self.status}")
            error.status_code = self.status
            raise error

    def get_league(self, league_id):
        self._raise()
        return {
            "league_id": league_id,
            "name": "Test Yahoo League",
            "settings": {"num_teams": 2, "current_week": 3},
            "roster_positions": ["QB", "RB", "BN"],
        }

    def get_league_users(self, league_id):
        self._raise()
        rows = [
            {"user_id": "u2", "team_key": f"{league_id}.t.2", "team_name": "B"},
            {"user_id": "u1", "team_key": f"{league_id}.t.1", "team_name": "A"},
        ]
        return rows[:1] if self.missing_team else rows

    def get_league_rosters(self, league_id):
        rows = [
            {"roster_id": f"{league_id}.t.2", "owner_id": "u2", "players": [{"player_id": "p2"}]},
            {"roster_id": f"{league_id}.t.1", "owner_id": "u1", "players": [{"player_id": "p1"}]},
        ]
        return rows[:1] if self.missing_team else rows

    def get_week_matchups(self, league_id, week):
        return [
            {"week": week, "team_key": f"{league_id}.t.2", "points": 91.2},
            {"week": week, "team_key": f"{league_id}.t.1", "points": 100.5},
        ]

    def get_week_transactions(self, league_id, week):
        return []

    def get_drafts(self, league_id):
        return [{"draft_id": f"{league_id}.d.1", "status": "postdraft"}]

    def get_draft_picks(self, draft_id):
        return [{"draft_id": draft_id, "pick": 2}, {"draft_id": draft_id, "pick": 1}]


class FakeSleeperClient:
    def __init__(self, *, malformed: bool = False):
        self.malformed = malformed

    def get_league(self, league_id):
        if self.malformed:
            return ["not", "a", "league"]
        return {
            "league_id": league_id,
            "name": "Test Sleeper League",
            "season": "2026",
            "total_rosters": 2,
            "settings": {"leg": 3, "playoff_week_start": 15},
            "scoring_settings": {"rec": 1},
            "roster_positions": ["QB", "RB", "BN"],
        }

    def get_league_users(self, league_id):
        return [{"user_id": "u2"}, {"user_id": "u1"}]

    def get_league_rosters(self, league_id):
        return [
            {"roster_id": 2, "owner_id": "u2", "players": ["p2"]},
            {"roster_id": 1, "owner_id": "u1", "players": ["p1"]},
        ]

    def get_league_matchups(self, league_id, week):
        return [
            {"roster_id": 2, "matchup_id": 1, "points": 91.2},
            {"roster_id": 1, "matchup_id": 1, "points": 100.5},
        ]

    def get_league_transactions(self, league_id, week):
        return []

    def get_league_drafts(self, league_id):
        return [{"draft_id": "d1", "season": "2026", "status": "complete"}]

    def get_draft_picks(self, draft_id):
        return [{"draft_id": draft_id, "pick_no": 2}, {"draft_id": draft_id, "pick_no": 1}]


class FakeEspnClient:
    def __init__(self, *, status: int | None = None):
        self.status = status

    def get_raw_league(self, year, views, **extra):
        if self.status is not None:
            error = RuntimeError(f"ESPN HTTP {self.status}")
            error.status_code = self.status
            raise error
        return {
            "id": 1234,
            "seasonId": year,
            "scoringPeriodId": 3,
            "settings": {"name": "Test ESPN League", "size": 2},
            "members": [{"id": "u2"}, {"id": "u1"}],
            "teams": [
                {"id": 2, "primaryOwner": "u2", "roster": {"entries": [{"playerId": 2}]}},
                {"id": 1, "primaryOwner": "u1", "roster": {"entries": [{"playerId": 1}]}},
            ],
            "schedule": [
                {"id": 1, "matchupPeriodId": 3, "home": {"teamId": 1}, "away": {"teamId": 2}}
            ],
            "transactions": [],
            "draftDetail": {"drafted": True},
        }


def _revision_map(result):
    return {(row.resource, row.scope): row for row in result.revisions}


@pytest.mark.parametrize(
    ("probe", "client", "league_id"),
    [
        (probe_yahoo_oauth, FakeYahooClient(), "470.l.1"),
        (probe_espn, FakeEspnClient(), "1234"),
        (probe_sleeper, FakeSleeperClient(), "1257088277819691008"),
    ],
)
def test_provider_probe_returns_healthy_complete_secret_free_revisions(probe, client, league_id):
    result = probe(client, league_id=league_id, season=2026, through_week=3)

    assert result.healthy is True
    assert result.error_code is None
    assert result.active_season == 2026
    assert result.expected_teams == result.observed_teams == 2
    assert {row.resource for row in result.revisions} >= {
        "settings", "teams", "rosters", "matchups", "transactions", "draft",
    }
    assert "token" not in repr(result).lower()
    assert "cookie" not in repr(result).lower()


def test_empty_transactions_are_valid_and_revisioned():
    result = probe_sleeper(
        FakeSleeperClient(),
        league_id="1257088277819691008",
        season=2026,
        through_week=3,
    )

    transactions = _revision_map(result)[("transactions", "2026:3")]
    assert transactions.status == "ok"
    assert transactions.expected_count == 0
    assert transactions.observed_count == 0
    assert transactions.revision


def test_source_order_does_not_change_any_revision():
    client = FakeYahooClient()
    first = probe_yahoo_oauth(client, league_id="470.l.1", season=2026, through_week=3)

    original_users = client.get_league_users
    original_rosters = client.get_league_rosters
    original_matchups = client.get_week_matchups
    original_picks = client.get_draft_picks
    client.get_league_users = lambda league_id: list(reversed(original_users(league_id)))
    client.get_league_rosters = lambda league_id: list(reversed(original_rosters(league_id)))
    client.get_week_matchups = lambda league_id, week: list(reversed(original_matchups(league_id, week)))
    client.get_draft_picks = lambda draft_id: list(reversed(original_picks(draft_id)))
    second = probe_yahoo_oauth(client, league_id="470.l.1", season=2026, through_week=3)

    assert second.revisions == first.revisions


@pytest.mark.parametrize(
    ("status", "code"),
    [(429, "rate_limited"), (401, "credential_required")],
)
def test_yahoo_classifies_throttle_and_revoked_oauth(status, code):
    with pytest.raises(ProviderProbeError) as caught:
        probe_yahoo_oauth(
            FakeYahooClient(status=status),
            league_id="470.l.1",
            season=2026,
            through_week=3,
        )
    assert caught.value.code == code
    assert str(caught.value) in {"Yahoo is rate limited", "Yahoo authorization must be reconnected"}


def test_malformed_provider_payload_is_rejected():
    with pytest.raises(ProviderProbeError, match="malformed") as caught:
        probe_sleeper(
            FakeSleeperClient(malformed=True),
            league_id="1257088277819691008",
            season=2026,
            through_week=3,
        )
    assert caught.value.code == "malformed_response"


def test_one_missing_team_is_incomplete_not_healthy():
    with pytest.raises(ProviderProbeError) as caught:
        probe_yahoo_oauth(
            FakeYahooClient(missing_team=True),
            league_id="470.l.1",
            season=2026,
            through_week=3,
        )
    assert caught.value.code == "incomplete_source"
    assert caught.value.expected_count == 2
    assert caught.value.observed_count == 1


def test_multiplatform_probe_combines_disjoint_segments_deterministically():
    segments = [
        {
            "provider": "sleeper",
            "client": FakeSleeperClient(),
            "league_id": "1257088277819691008",
            "season": 2026,
            "through_week": 3,
        },
        {
            "provider": "espn",
            "client": FakeEspnClient(),
            "league_id": "1234",
            "season": 2025,
            "through_week": 17,
        },
    ]
    first = probe_multiplatform(segments)
    second = probe_multiplatform(list(reversed(deepcopy(segments))))

    assert first.healthy is True
    assert second.revisions == first.revisions
    assert [result.provider for result in first.segments] == ["espn", "sleeper"]
