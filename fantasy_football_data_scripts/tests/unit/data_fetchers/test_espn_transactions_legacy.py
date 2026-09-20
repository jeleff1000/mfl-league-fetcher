"""
Test ESPN pre-2019 legacy transaction inference.

Pre-2019 ESPN has no transaction API — only draft vs final roster snapshots.
Since we can't distinguish trades from independent waiver pickups, ALL player
movements are classified as drop + add.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_player(player_id, name):
    p = MagicMock()
    p.playerId = player_id
    p.name = name
    p.position = "RB"
    p.proTeam = "ARI"
    return p


def _make_mock_pick(player_id, team_id, player_name="Player"):
    pick = MagicMock()
    pick.playerId = player_id
    pick.team = MagicMock()
    pick.team.team_id = team_id
    pick.playerName = player_name
    return pick


def _make_mock_team(team_id, roster_players):
    team = MagicMock()
    team.team_id = team_id
    team.roster = roster_players
    return team


def _make_mock_ctx(teams_info):
    """Build a mock ESPN context that resolves team_id -> manager/team info."""
    ctx = MagicMock()
    ctx.get_league_id_for_year.return_value = 12345

    def get_manager_name(team_id, team_name=None, year=None):
        return teams_info.get(team_id, {}).get("manager", f"Manager_{team_id}")

    def get_team_name(team_id, year=None):
        return teams_info.get(team_id, {}).get("team_name", f"Team_{team_id}")

    def get_manager_guid(team_id, year=None):
        return f"guid_{team_id}"

    def get_franchise_id(team_id, year=None):
        return f"fran_{team_id}"

    ctx.get_manager_name.side_effect = get_manager_name
    ctx.get_team_name.side_effect = get_team_name
    ctx.get_manager_guid.side_effect = get_manager_guid
    ctx.get_franchise_id.side_effect = get_franchise_id
    return ctx


def _run_legacy(ctx, draft, teams, year=2018):
    """Helper to run fetch_espn_transactions_legacy with mocked ESPN API."""
    from multi_league.data_fetchers.espn.espn_transactions import (
        fetch_espn_transactions_legacy,
    )

    mock_league = MagicMock()
    mock_league.draft = draft
    mock_league.teams = teams

    mock_client = MagicMock()
    mock_client.get_league.return_value = mock_league

    with patch(
        "multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient",
        return_value=mock_client,
    ):
        return fetch_espn_transactions_legacy(ctx, year)


def test_modern_transactions_can_stop_at_active_refresh_week(monkeypatch):
    """A weekly refresh must not poll ESPN transaction periods that have not happened."""
    from multi_league.data_fetchers.espn import espn_api_client
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions_modern

    class FakeClient:
        waiver_max_weeks = []
        trade_max_weeks = []

        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, _year):
            return SimpleNamespace(teams=[])

        def get_raw_waivers(self, _year, *, max_weeks):
            self.waiver_max_weeks.append(max_weeks)
            return []

        def get_raw_trades(self, _year, *, max_weeks):
            self.trade_max_weeks.append(max_weeks)
            return []

    ctx = MagicMock()
    ctx.get_league_id_for_year.return_value = 12345
    ctx.espn_s2 = "test"
    ctx.swid = "test"
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", FakeClient)

    assert fetch_espn_transactions_modern(ctx, 2026, max_week=1) is None
    assert FakeClient.waiver_max_weeks == [1]
    assert FakeClient.trade_max_weeks == [1]


def test_modern_transactions_reuse_the_active_season_client(monkeypatch):
    """The weekly path already loaded this league; do not create a second client."""
    from multi_league.data_fetchers.espn import espn_api_client
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions_modern

    class ExistingClient:
        def get_raw_waivers(self, _year, *, max_weeks):
            assert max_weeks == 1
            return []

        def get_raw_trades(self, _year, *, max_weeks):
            assert max_weeks == 1
            return []

    ctx = MagicMock()
    ctx.get_league_id_for_year.return_value = 12345
    ctx.espn_s2 = "test"
    ctx.swid = "test"
    monkeypatch.setattr(
        espn_api_client,
        "ESPNAPIClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must reuse the supplied client")),
    )
    assert (
        fetch_espn_transactions_modern(
            ctx,
            2026,
            max_week=1,
            client=ExistingClient(),
            league=SimpleNamespace(teams=[]),
        )
        is None
    )


def test_active_espn_transaction_fetch_rejects_failed_waiver_surface():
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions_modern

    class Client:
        def get_raw_waivers_strict(self, _year, *, max_weeks):
            assert max_weeks == 1
            raise TimeoutError("provider timeout")

        def get_raw_trades_strict(self, _year, *, max_weeks):
            return []

    ctx = MagicMock()
    ctx.get_league_id_for_year.return_value = 12345
    with pytest.raises(RuntimeError, match="waivers"):
        fetch_espn_transactions_modern(
            ctx, 2026, max_week=1, client=Client(), league=SimpleNamespace(teams=[]),
        )


def test_strict_espn_transaction_endpoint_rejects_timeout_and_malformed_empty(monkeypatch):
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient, ESPNAPIError

    class Response:
        def __init__(self, payload):
            self.payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self.payload

    class Session:
        def __init__(self, payload):
            self.payload = payload

        def get(self, *_args, **_kwargs):
            if self.payload == "timeout":
                raise TimeoutError("provider timeout")
            return Response(self.payload)

    client = ESPNAPIClient(12345)
    monkeypatch.setattr(client, "_session", Session({"transactions": []}))
    assert client.get_raw_transactions(2026, 1, strict=True) == []
    monkeypatch.setattr(client, "_session", Session({}))
    with pytest.raises(
        ESPNAPIError,
        match=r"malformed.*payload_type=dict.*keys=\[\].*transactions_type=missing",
    ):
        client.get_raw_transactions(2026, 1, strict=True)
    monkeypatch.setattr(client, "_session", Session("timeout"))
    with pytest.raises(ESPNAPIError, match="raw transactions"):
        client.get_raw_transactions(2026, 1, strict=True)

class TestPreTwoThousandNineteenNoTrades:
    """Pre-2019 ESPN should never produce trade rows — only drops and adds."""

    def test_player_swap_is_drop_add_not_trade(self):
        """Even if two teams swap players, it's drop+add — we can't prove it was a trade."""
        teams_info = {
            1: {"manager": "Dani", "team_name": "Dani's Team"},
            2: {"manager": "Tani", "team_name": "Tani's Team"},
        }
        ctx = _make_mock_ctx(teams_info)

        draft = [
            _make_mock_pick(101, 1, "David Johnson"),
            _make_mock_pick(102, 2, "Tyreek Hill"),
        ]
        # Swapped: Dani has P2, Tani has P1
        team1 = _make_mock_team(1, [_make_mock_player(102, "Tyreek Hill")])
        team2 = _make_mock_team(2, [_make_mock_player(101, "David Johnson")])

        df = _run_legacy(ctx, draft, [team1, team2])

        assert df is not None
        trades = df[df["transaction_type"] == "trade"]
        adds = df[df["transaction_type"] == "add"]
        drops = df[df["transaction_type"] == "drop"]

        assert len(trades) == 0, f"Expected 0 trades, got {len(trades)}"
        # Each player movement = 1 drop + 1 add = 4 rows total
        assert len(drops) == 2
        assert len(adds) == 2

    def test_one_way_movement_is_drop_add(self):
        """Player drafted by Dani ends on Tani's team — drop + add."""
        teams_info = {
            1: {"manager": "Dani", "team_name": "Dani's Team"},
            2: {"manager": "Tani", "team_name": "Tani's Team"},
        }
        ctx = _make_mock_ctx(teams_info)

        draft = [
            _make_mock_pick(101, 1, "David Johnson"),
            _make_mock_pick(102, 2, "Tyreek Hill"),
        ]
        team1 = _make_mock_team(1, [])
        team2 = _make_mock_team(
            2,
            [
                _make_mock_player(101, "David Johnson"),
                _make_mock_player(102, "Tyreek Hill"),
            ],
        )

        df = _run_legacy(ctx, draft, [team1, team2])

        assert df is not None
        trades = df[df["transaction_type"] == "trade"]
        drops = df[df["transaction_type"] == "drop"]
        adds = df[df["transaction_type"] == "add"]

        assert len(trades) == 0
        # P1 moved: 1 drop by Dani + 1 add by Tani
        dj_drops = drops[drops["player"] == "David Johnson"]
        dj_adds = adds[adds["player"] == "David Johnson"]
        assert len(dj_drops) == 1
        assert len(dj_adds) == 1
        assert dj_drops.iloc[0]["manager"] == "Dani"
        assert dj_adds.iloc[0]["manager"] == "Tani"

    def test_undrafted_player_on_roster_is_add(self):
        """Player not in draft but on final roster = add."""
        teams_info = {1: {"manager": "Alice", "team_name": "Team A"}}
        ctx = _make_mock_ctx(teams_info)

        draft = [_make_mock_pick(101, 1, "Player1")]
        team1 = _make_mock_team(
            1,
            [
                _make_mock_player(101, "Player1"),
                _make_mock_player(999, "Waiver Pickup"),
            ],
        )

        df = _run_legacy(ctx, draft, [team1])

        assert df is not None
        adds = df[df["transaction_type"] == "add"]
        assert len(adds) == 1
        assert adds.iloc[0]["player"] == "Waiver Pickup"
        assert adds.iloc[0]["manager"] == "Alice"

    def test_drafted_player_not_on_roster_is_drop(self):
        """Player drafted but not on any final roster = drop."""
        teams_info = {1: {"manager": "Alice", "team_name": "Team A"}}
        ctx = _make_mock_ctx(teams_info)

        draft = [
            _make_mock_pick(101, 1, "Player1"),
            _make_mock_pick(102, 1, "Dropped Guy"),
        ]
        team1 = _make_mock_team(1, [_make_mock_player(101, "Player1")])

        df = _run_legacy(ctx, draft, [team1])

        assert df is not None
        drops = df[df["transaction_type"] == "drop"]
        assert len(drops) == 1
        assert drops.iloc[0]["player"] == "Dropped Guy"
        assert drops.iloc[0]["manager"] == "Alice"

    def test_all_estimated(self):
        """All pre-2019 rows should be marked is_estimated=True."""
        teams_info = {
            1: {"manager": "Alice", "team_name": "Team A"},
            2: {"manager": "Bob", "team_name": "Team B"},
        }
        ctx = _make_mock_ctx(teams_info)

        draft = [_make_mock_pick(101, 1, "Player1")]
        team1 = _make_mock_team(1, [])
        team2 = _make_mock_team(2, [_make_mock_player(101, "Player1")])

        df = _run_legacy(ctx, draft, [team1, team2])

        assert df is not None
        assert df["is_estimated"].all(), "All pre-2019 rows must be is_estimated=True"
