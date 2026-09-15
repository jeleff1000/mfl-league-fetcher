import sys
from pathlib import Path

import pytest


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext
from multi_league.data_fetchers.sleeper.sleeper_roster_identity import build_year_roster_map
from multi_league.data_fetchers.sleeper.sleeper_transactions import SleeperTransactionFetcher
from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient, SleeperAPIError


class _FakeClient:
    def __init__(self):
        self.rosters = {
            "2022-league": [{"roster_id": rid, "owner_id": f"user-{rid}"} for rid in range(1, 13)],
            "2021-league": [{"roster_id": rid, "owner_id": f"user-{rid}"} for rid in range(1, 15)],
        }
        self.users = {
            "2022-league": [
                {"user_id": f"user-{rid}", "display_name": f"Current-{rid}", "metadata": {"team_name": f"Team {rid}"}}
                for rid in range(1, 13)
            ],
            "2021-league": [
                {
                    "user_id": f"user-{rid}",
                    "display_name": f"Legacy-{rid}",
                    "metadata": {"team_name": f"Legacy Team {rid}"},
                }
                for rid in range(1, 15)
            ],
        }
        self.transactions = {
            ("2022-league", 1): [
                {
                    "transaction_id": "tx-commish",
                    "type": "commissioner",
                    "status": "complete",
                    "created": 1652022621876,
                    "leg": 1,
                    "draft_picks": [],
                    "adds": {"1049": 14},
                    "drops": {"1049": 9},
                    "roster_ids": [9, 14],
                    "waiver_budget": [],
                },
                {
                    "transaction_id": "tx-drop",
                    "type": "free_agent",
                    "status": "complete",
                    "created": 1644357251828,
                    "leg": 1,
                    "draft_picks": [],
                    "adds": None,
                    "drops": {"138": 14},
                    "roster_ids": [14],
                    "waiver_budget": [],
                },
                {
                    "transaction_id": "tx-pick",
                    "type": "trade",
                    "status": "complete",
                    "created": 1647103168687,
                    "leg": 1,
                    "adds": {"4981": 8},
                    "drops": {"4981": 14},
                    "roster_ids": [8, 14],
                    "draft_picks": [
                        {
                            "round": 1,
                            "season": "2022",
                            "roster_id": 14,
                            "owner_id": 8,
                            "previous_owner_id": 14,
                        }
                    ],
                    "waiver_budget": [],
                },
            ]
        }
        self.transaction_calls = []

    def get_league_rosters(self, league_id):
        return self.rosters.get(league_id, [])

    def get_league_users(self, league_id):
        return self.users.get(league_id, [])

    def get_league_transactions(self, league_id, week):
        self.transaction_calls.append((league_id, week))
        return self.transactions.get((league_id, week), [])


class _FakePlayerCache:
    VALID_NFL_TEAMS = set()
    TEAM_ABBREV_TO_NAME = {}
    NFL_TEAM_SHORT_NAMES = {}

    def refresh_if_stale(self, client):
        return None

    def get_player_name(self, player_id):
        return f"Player {player_id}"

    def get_player_team(self, player_id):
        return "BUF"

    def get_player_position(self, player_id):
        return "WR"


def _make_ctx(tmp_path):
    return SleeperContext(
        league_id="2022-league",
        league_name="Dynasty for fun",
        username="tester",
        start_year=2021,
        end_year=2022,
        league_ids={"2021": "2021-league", "2022": "2022-league"},
        data_directory=tmp_path,
    )


def test_build_year_roster_map_backfills_retired_rosters_from_previous_league(tmp_path):
    ctx = _make_ctx(tmp_path)
    roster_map = build_year_roster_map(
        ctx,
        _FakeClient(),
        league_id="2022-league",
        year=2022,
        required_roster_ids={13, 14},
    )

    assert roster_map[13]["manager_name"] == "Legacy-13"
    assert roster_map[13]["manager_guid"] == "user-13"
    assert roster_map[14]["manager_name"] == "Legacy-14"
    assert roster_map[14]["team_name"] == "Legacy Team 14"


def test_fetch_transactions_for_year_resolves_retired_roster_ids(tmp_path):
    ctx = _make_ctx(tmp_path)
    fetcher = SleeperTransactionFetcher(ctx, client=_FakeClient(), player_cache=_FakePlayerCache())

    df = fetcher.fetch_transactions_for_year(2022)

    commissioner_row = df.loc[df["transaction_id"] == "tx-commish"].iloc[0]
    drop_row = df.loc[df["transaction_id"] == "tx-drop"].iloc[0]
    pick_row = df.loc[(df["transaction_id"] == "tx-pick") & (df["transaction_type"] == "trade_pick")].iloc[0]

    assert commissioner_row["manager"] == "Legacy-14"
    assert commissioner_row["manager_guid"] == "user-14"
    assert commissioner_row["team_name"] == "Legacy Team 14"

    assert drop_row["manager"] == "Legacy-14"
    assert drop_row["manager_guid"] == "user-14"

    assert pick_row["manager"] == "Current-8"
    assert pick_row["source_manager"] == "Legacy-14"
    assert pick_row["source_manager_guid"] == "user-14"


def test_fetch_transactions_for_year_can_stop_at_active_refresh_week(tmp_path):
    """Weekly refreshes must not poll unplayed future Sleeper transaction weeks."""
    ctx = _make_ctx(tmp_path)
    client = _FakeClient()
    fetcher = SleeperTransactionFetcher(ctx, client=client, player_cache=_FakePlayerCache())

    df = fetcher.fetch_transactions_for_year(2022, max_week=1)

    assert not df.empty
    assert client.transaction_calls == [("2022-league", 1)]


def test_active_refresh_transactions_do_not_stop_after_five_empty_weeks(tmp_path):
    ctx = _make_ctx(tmp_path)
    client = _FakeClient()
    client.transactions[("2022-league", 6)] = client.transactions.pop(("2022-league", 1))
    fetcher = SleeperTransactionFetcher(ctx, client=client, player_cache=_FakePlayerCache())

    result = fetcher.fetch_transactions_for_year(2022, max_week=6)

    assert not result.empty
    assert client.transaction_calls == [("2022-league", week) for week in range(1, 7)]


def test_active_refresh_transaction_week_failure_is_not_valid_empty(tmp_path):
    ctx = _make_ctx(tmp_path)

    class FailingClient(_FakeClient):
        def get_league_transactions(self, league_id, week):
            if week == 2:
                raise TimeoutError("provider timeout")
            return super().get_league_transactions(league_id, week)

    fetcher = SleeperTransactionFetcher(
        ctx, client=FailingClient(), player_cache=_FakePlayerCache(),
    )
    with pytest.raises(RuntimeError, match="week 2"):
        fetcher.fetch_transactions_for_year(2022, max_week=3)


def test_sleeper_strict_transaction_endpoint_distinguishes_empty_from_failed(monkeypatch):
    client = SleeperAPIClient()
    monkeypatch.setattr(client, "_get", lambda _endpoint: [])
    assert client.get_league_transactions_strict("league", 1) == []
    monkeypatch.setattr(client, "_get", lambda _endpoint: None)
    with pytest.raises(SleeperAPIError, match="not verified"):
        client.get_league_transactions_strict("league", 1)
    monkeypatch.setattr(client, "_get", lambda _endpoint: {"transactions": []})
    with pytest.raises(SleeperAPIError, match="not verified"):
        client.get_league_transactions_strict("league", 1)
