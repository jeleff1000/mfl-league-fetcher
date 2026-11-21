"""Yahoo transient-error retry: empty-body/UDB outages retry, real errors raise."""

import pytest

from multi_league.data_fetchers.yahoo.yahoo_runtime import (
    call_yahoo_with_transient_retry,
    is_transient_yahoo_service_error,
)


def test_empty_body_runtime_error_is_transient():
    # yahoo_fantasy_api raises RuntimeError(response.content); demo_league's
    # 2026-11-21 scheduled refresh died on exactly this.
    assert is_transient_yahoo_service_error(RuntimeError(b""))
    assert is_transient_yahoo_service_error(RuntimeError("udb validation unavailable"))


def test_auth_and_other_errors_are_not_transient():
    assert not is_transient_yahoo_service_error(RuntimeError(b"<error>Please provide valid credentials</error>"))
    assert not is_transient_yahoo_service_error(ValueError(b""))
    assert not is_transient_yahoo_service_error(RuntimeError("401 Unauthorized"))


def test_retry_recovers_after_transient_empty_body():
    calls = []
    sleeps = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError(b"")
        return "league"

    assert call_yahoo_with_transient_retry(flaky, label="t", sleep=sleeps.append) == "league"
    assert len(calls) == 3
    assert sleeps == [1, 2]


def test_retry_gives_up_after_attempts_and_raises_original():
    def always_empty():
        raise RuntimeError(b"")

    with pytest.raises(RuntimeError):
        call_yahoo_with_transient_retry(always_empty, label="t", sleep=lambda _s: None)


def test_non_transient_error_raises_immediately():
    calls = []

    def auth_fail():
        calls.append(1)
        raise RuntimeError(b"<error>Please provide valid credentials</error>")

    with pytest.raises(RuntimeError):
        call_yahoo_with_transient_retry(auth_fail, label="t", sleep=lambda _s: None)
    assert len(calls) == 1
