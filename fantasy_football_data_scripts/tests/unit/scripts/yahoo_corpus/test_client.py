from __future__ import annotations

import json

import pytest

from scripts.yahoo_corpus.client import (
    RequestBudgetExceeded,
    YahooCensusClient,
    YahooRequestFailure,
    parse_football_games,
    parse_game_leagues,
)


class FakeResponse:
    def __init__(self, status: int, *, payload: dict | None = None, text: str = "", headers: dict | None = None):
        self.status_code = status
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")
        self.headers = headers or {}

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict]] = []

    def post(self, url: str, **kwargs):
        self.calls.append(("POST", url, kwargs))
        return self.responses.pop(0)

    def get(self, url: str, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return self.responses.pop(0)


def _games_payload() -> dict:
    return {
        "fantasy_content": {
            "users": {
                "0": {
                    "user": [
                        {"guid": "private-guid"},
                        {
                            "games": {
                                "0": {"game": [{"game_key": "461", "code": "nfl", "season": "2025"}]},
                                "1": {"game": {"game_key": "mlb", "code": "mlb", "season": "2025"}},
                                "2": {"game": [{"game_key": "449", "code": "nfl", "season": "2024"}]},
                                "count": 3,
                            }
                        },
                    ]
                }
            }
        }
    }


def _leagues_payload() -> dict:
    return {
        "fantasy_content": {
            "users": {
                "0": {
                    "user": [
                        {},
                        {
                            "games": {
                                "0": {
                                    "game": [
                                        {"game_key": "461"},
                                        {
                                            "leagues": {
                                                "0": {
                                                    "league": [
                                                        {
                                                            "league_key": "461.l.12345",
                                                            "league_id": "12345",
                                                            "name": "Private League",
                                                            "season": "2025",
                                                            "num_teams": 12,
                                                        }
                                                    ]
                                                },
                                                "1": {"league": {"league_key": "", "league_id": "broken"}},
                                                "count": 2,
                                            }
                                        },
                                    ]
                                }
                            }
                        },
                    ]
                }
            }
        }
    }


def test_parse_football_games_filters_non_nfl_and_handles_wrappers() -> None:
    assert parse_football_games(_games_payload()) == [
        {"game_key": "461", "season": "2025"},
        {"game_key": "449", "season": "2024"},
    ]


def test_parse_game_leagues_skips_malformed_sibling() -> None:
    assert parse_game_leagues(_leagues_payload(), game_key="461", season="2025") == [
        {
            "league_key": "461.l.12345",
            "league_id": "12345",
            "name": "Private League",
            "season": "2025",
            "num_teams": 12,
            "game_key": "461",
        }
    ]


def test_client_retries_rate_limit_and_uses_retry_after() -> None:
    sleeps: list[float] = []
    session = FakeSession(
        [
            FakeResponse(429, headers={"Retry-After": "2"}),
            FakeResponse(200, payload=_games_payload()),
        ]
    )
    client = YahooCensusClient(
        "client",
        "secret",
        "refresh-live-secret",
        session=session,
        sleeper=sleeps.append,
        max_retries=2,
    )
    client.access_token = "access-live-secret"

    games = client.discover_games()

    assert len(games) == 2
    assert sleeps == [2.0]
    assert client.request_count == 2


@pytest.mark.parametrize("status", [401, 403, 404])
def test_client_terminal_failures_are_redacted(status: int) -> None:
    session = FakeSession([FakeResponse(status, text="access-live-secret refresh-live-secret")])
    client = YahooCensusClient(
        "client",
        "secret",
        "refresh-live-secret",
        session=session,
        sleeper=lambda _: None,
    )
    client.access_token = "access-live-secret"

    with pytest.raises(YahooRequestFailure) as exc_info:
        client.fetch_settings_xml("461.l.12345")

    message = str(exc_info.value)
    assert str(status) in message
    assert "access-live-secret" not in message
    assert "refresh-live-secret" not in message


def test_client_enforces_request_budget() -> None:
    client = YahooCensusClient(
        "client",
        "secret",
        "refresh",
        session=FakeSession([]),
        sleeper=lambda _: None,
        request_budget=0,
    )

    with pytest.raises(RequestBudgetExceeded):
        client.refresh()
