"""Refresh reads must leave time to finalize without changing import defaults."""

from unittest.mock import Mock

import pytest
import requests

from multi_league.data_fetchers.sleeper.sleeper_api_client import (
    SleeperAPIClient,
    SleeperAPIConfig,
    SleeperAPIError,
)


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    state = {"now": 1000.0, "sleeps": []}

    def sleep(seconds):
        state["sleeps"].append(seconds)
        state["now"] += seconds

    monkeypatch.setattr("time.time", lambda: state["now"])
    monkeypatch.setattr("time.sleep", sleep)
    monkeypatch.setattr("multi_league.utils.retry.random.random", lambda: 0.5)
    return state


@pytest.fixture
def client(monkeypatch, clock):
    def reject_network(*args, **kwargs):
        raise AssertionError("Sleeper budget tests must mock HTTP")

    monkeypatch.setattr(requests.sessions.Session, "request", reject_network)
    reader = SleeperAPIClient(SleeperAPIConfig(rate_limit_per_min=0))
    reader.session.get = Mock(return_value=_response())
    return reader


def _response(status=200, retry_after=None):
    response = Mock()
    response.status_code = status
    response.headers = {} if retry_after is None else {"Retry-After": retry_after}
    response.text = "rate limited" if status == 429 else '{"week": 3}'
    response.content = response.text.encode()
    response.json.return_value = {"week": 3}
    return response


@pytest.mark.parametrize("configured,want", [(30, 10), (4, 4), (0.5, 0.5), (90, 10)])
def test_refresh_caps_timeout_without_expanding_smaller_config(client, clock, monkeypatch, configured, want):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1120")
    client.config.timeout = configured

    assert client.get_nfl_state() == {"week": 3}
    assert client.session.get.call_args.kwargs["timeout"] == want
    assert client.session.get.call_count == 1
    assert clock["sleeps"] == []


def test_refresh_caps_timeout_to_remaining_time_before_reserve(client, clock, monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1032.5")

    assert client.get_nfl_state() == {"week": 3}
    assert client.session.get.call_args.kwargs["timeout"] == 2.5


@pytest.mark.parametrize("deadline", ["999", "1029", "1030"])
def test_exhausted_refresh_budget_is_not_a_retriable_network_error(client, clock, monkeypatch, deadline):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", deadline)
    with pytest.raises(SleeperAPIError, match="refresh read budget exhausted"):
        client.get_nfl_state()

    client.session.get.assert_not_called()
    assert clock["sleeps"] == []


def test_refresh_rechecks_budget_after_local_rate_limiter(client, clock, monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1032")
    monkeypatch.setattr(client, "_rate_limit_wait", lambda: clock.update(now=1002))

    with pytest.raises(SleeperAPIError, match="refresh read budget exhausted"):
        client.get_nfl_state()

    client.session.get.assert_not_called()


@pytest.mark.parametrize("error_type", [requests.ConnectionError, requests.Timeout])
def test_decorated_retry_recalculates_timeout_after_backoff(client, clock, monkeypatch, error_type):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1032")
    timeouts = []

    def get(url, *, timeout):
        timeouts.append(timeout)
        if len(timeouts) == 1:
            clock["now"] += 0.5
            raise error_type("provider stalled")
        return _response()

    client.session.get.side_effect = get
    assert client.get_nfl_state() == {"week": 3}
    assert timeouts == [2, 0.5]
    assert clock["sleeps"] == [1]


@pytest.mark.parametrize("error_type", [requests.ConnectionError, requests.Timeout])
def test_refresh_stops_before_http_when_backoff_consumes_budget(client, clock, monkeypatch, error_type):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1031")
    client.session.get.side_effect = error_type("provider stalled")

    with pytest.raises(SleeperAPIError, match="refresh read budget exhausted"):
        client.get_nfl_state()

    assert client.session.get.call_count == 1
    assert client.session.get.call_args.kwargs["timeout"] == 1
    assert clock["sleeps"] == [1]


def test_timeout_that_exhausts_refresh_budget_does_not_sleep_again(client, clock, monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1032")

    def get(url, *, timeout):
        clock["now"] += timeout
        raise requests.Timeout("provider stalled")

    client.session.get.side_effect = get
    with pytest.raises(SleeperAPIError, match="refresh read budget exhausted"):
        client.get_nfl_state()

    assert client.session.get.call_count == 1
    assert clock["sleeps"] == []


@pytest.mark.parametrize("retry_after,want", [(None, "60"), ("120", "120"), ("invalid", "invalid")])
def test_refresh_429_reports_retry_after_without_sleep_or_retry(client, clock, monkeypatch, retry_after, want):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1120")
    client.session.get.return_value = _response(429, retry_after)

    with pytest.raises(SleeperAPIError, match=f"Retry-After={want}") as error:
        client.get_nfl_state()

    assert error.value.status_code == 429
    assert client.session.get.call_count == 1
    assert clock["sleeps"] == []


@pytest.mark.parametrize("deadline", [None, "", "invalid", "nan", "inf", "-inf", "0", "-1"])
def test_missing_or_invalid_deadline_preserves_import_timeout(client, clock, monkeypatch, deadline):
    if deadline is not None:
        monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", deadline)

    assert client.get_nfl_state() == {"week": 3}
    assert client.session.get.call_args.kwargs["timeout"] == 30
    assert clock["sleeps"] == []


@pytest.mark.parametrize("deadline", [None, "invalid", "nan", "inf", "0", "-1"])
@pytest.mark.parametrize("retry_after,want", [(None, 60), ("2", 2)])
def test_import_429_preserves_existing_wait_without_retry(client, clock, monkeypatch, deadline, retry_after, want):
    if deadline is not None:
        monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", deadline)
    client.session.get.return_value = _response(429, retry_after)

    with pytest.raises(SleeperAPIError) as error:
        client.get_nfl_state()

    assert error.value.status_code == 429
    assert client.session.get.call_count == 1
    assert clock["sleeps"] == [want]


def test_import_retains_configured_timeout_and_four_network_attempts(client, clock):
    client.config.timeout = 4
    client.session.get.side_effect = requests.Timeout("provider stalled")

    with pytest.raises(requests.Timeout):
        client.get_nfl_state()

    assert [call.kwargs["timeout"] for call in client.session.get.call_args_list] == [4, 4, 4, 4]
    assert clock["sleeps"] == [1, 2, 4]
