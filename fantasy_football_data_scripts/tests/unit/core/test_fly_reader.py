"""Unit tests for FlyReader._post retry behavior.

Bug: Phase 1.7 staging probe was crashing on transient Fly 5xx responses.
The reader previously retried 503 only (with fixed 2s sleep). It must also
retry 502/504, and use exponential backoff so a multi-second proxy hiccup
doesn't exhaust retries within a few seconds.
"""

from unittest.mock import Mock, patch
from io import BytesIO

import pytest
import requests

from multi_league.core.readers.fly_reader import (
    FlyReader,
    FlyReaderNetworkError,
    FlyReaderTableNotFound,
)


def test_constructor_rejects_corpus_mode_even_with_credentials(monkeypatch):
    monkeypatch.setenv("CORPUS_MODE", "1")
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")

    with pytest.raises(RuntimeError, match="corpus mode"):
        FlyReader()


@pytest.fixture
def fly_env(monkeypatch):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")


def _resp(status_code, payload=None, text=""):
    r = Mock()
    r.status_code = status_code
    r.json.return_value = payload if payload is not None else []
    r.text = text
    return r


def test_post_succeeds_on_200(fly_env):
    """Sanity: a single 200 returns parsed JSON."""
    reader = FlyReader()
    with patch("multi_league.core.readers.fly_reader.requests.post", return_value=_resp(200, [{"x": 1}])):
        rows = reader.query("SELECT 1", database="___leagues")
    assert rows == [{"x": 1}]


def test_query_df_parquet_decodes_binary_response(fly_env):
    import pandas as pd

    payload = BytesIO()
    pd.DataFrame([{"NFL_player_id": "player-1", "year": 2026}]).to_parquet(payload, index=False)
    response = _resp(200)
    response.content = payload.getvalue()
    reader = FlyReader()

    with patch("multi_league.core.readers.fly_reader.requests.post", return_value=response) as post:
        frame = reader.query_df_parquet("SELECT 1", database="___ops")

    assert frame.to_dict("records") == [{"NFL_player_id": "player-1", "year": 2026}]
    assert post.call_args.args[0] == "https://fly.test/query-parquet"


def test_post_retries_503_then_succeeds(fly_env):
    """Existing behavior: 503 is retried until success."""
    reader = FlyReader()
    sequence = [_resp(503), _resp(200, [{"ok": True}])]
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence),
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        rows = reader.query("SELECT 1", database="___leagues")
    assert rows == [{"ok": True}]
    assert mock_sleep.call_count == 1


def test_post_survives_long_503_burst(fly_env):
    """Empty 503 bursts from Fly's proxy should get more than three chances."""
    reader = FlyReader()
    sequence = [_resp(503) for _ in range(FlyReader.MAX_RETRIES - 1)]
    sequence.append(_resp(200, [{"ok": True}]))
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        rows = reader.query("SELECT 1", database="___leagues")
    assert rows == [{"ok": True}]
    assert mock_post.call_count == FlyReader.MAX_RETRIES
    assert mock_sleep.call_count == FlyReader.MAX_RETRIES - 1


def test_post_retries_502_then_succeeds(fly_env):
    """502 (bad gateway during proxy hiccup) must be retried, not raised."""
    reader = FlyReader()
    sequence = [_resp(502, text="bad gateway"), _resp(200, [{"ok": True}])]
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence),
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        rows = reader.query("SELECT 1", database="___leagues")
    assert rows == [{"ok": True}]
    assert mock_sleep.call_count == 1


def test_post_retries_504_then_succeeds(fly_env):
    """504 (gateway timeout) must be retried, not raised."""
    reader = FlyReader()
    sequence = [_resp(504, text="gateway timeout"), _resp(200, [{"ok": True}])]
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence),
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        rows = reader.query("SELECT 1", database="___leagues")
    assert rows == [{"ok": True}]
    assert mock_sleep.call_count == 1


def test_post_does_not_retry_4xx(fly_env):
    """Client errors (400, 401, 404) must surface immediately — no retry."""
    reader = FlyReader()
    with (
        patch(
            "multi_league.core.readers.fly_reader.requests.post",
            return_value=_resp(401, text="unauthorized"),
        ) as mock_post,
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        with pytest.raises(RuntimeError, match=r"401"):
            reader.query("SELECT 1", database="___leagues")
    assert mock_post.call_count == 1
    mock_sleep.assert_not_called()


def test_post_uses_exponential_backoff(fly_env):
    """Backoff must grow per attempt (1s, 2s) so a multi-second outage isn't
    burned through immediately."""
    reader = FlyReader()
    sequence = [_resp(503), _resp(503), _resp(200, [{"ok": True}])]
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence),
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        reader.query("SELECT 1", database="___leagues")
    delays = [c.args[0] for c in mock_sleep.call_args_list]
    assert len(delays) == 2
    assert delays[1] > delays[0], f"Expected exponential backoff, got {delays}"


def test_post_raises_after_exhausting_retries(fly_env):
    """When all retries return 5xx, the final response surfaces as RuntimeError."""
    reader = FlyReader()
    with (
        patch(
            "multi_league.core.readers.fly_reader.requests.post",
            return_value=_resp(503, text="still down"),
        ),
        patch("multi_league.core.readers.fly_reader.time.sleep"),
    ):
        with pytest.raises(RuntimeError, match=r"503"):
            reader.query("SELECT 1", database="___leagues")


def test_table_not_found_raises_typed(monkeypatch):
    """404 response with 'does not exist' text raises FlyReaderTableNotFound."""

    class _Resp:
        status_code = 404
        text = "Catalog Error: Table 'foo' does not exist"

    monkeypatch.setattr("requests.post", lambda *a, **k: _Resp())
    monkeypatch.setenv("DATABASE_SERVER_URL", "http://x")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "t")
    with pytest.raises(FlyReaderTableNotFound):
        FlyReader()._post("SELECT 1", "x")


def test_catalog_missing_table_500_is_not_retried_for_thirty_seconds(fly_env):
    reader = FlyReader()
    response = _resp(
        500,
        text="Catalog Error: Table with name league_update_manifests does not exist",
    )
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", return_value=response) as mock_post,
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        with pytest.raises(FlyReaderTableNotFound):
            reader.query("SELECT * FROM accounts.league_update_manifests", database="___ops")
    assert mock_post.call_count == 1
    mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    "error_text",
    [
        "ParserException: Parser Error: syntax error at or near years",
        'Binder Error: Referenced column "missing" not found',
    ],
)
def test_deterministic_query_errors_are_not_retried(fly_env, error_text):
    reader = FlyReader()
    response = _resp(500, text=error_text)
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", return_value=response) as mock_post,
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        with pytest.raises(RuntimeError, match=r"500"):
            reader.query("SELECT broken", database="___leagues")
    assert mock_post.call_count == 1
    mock_sleep.assert_not_called()


def test_transient_500_still_retries_after_catalog_fast_fail(fly_env):
    reader = FlyReader()
    sequence = [_resp(500, text="temporary DuckDB ATTACH race"), _resp(200, [{"ok": True}])]
    with (
        patch("multi_league.core.readers.fly_reader.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.readers.fly_reader.time.sleep") as mock_sleep,
    ):
        assert reader.query("SELECT 1", database="___ops") == [{"ok": True}]
    assert mock_post.call_count == 2
    assert mock_sleep.call_count == 1


def test_network_error_raises_typed(monkeypatch):
    """ConnectionError from requests raises FlyReaderNetworkError."""
    calls = []

    def _raise(*a, **k):
        calls.append(1)
        raise requests.ConnectionError("nope")

    monkeypatch.setattr("requests.post", _raise)
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", lambda _: None)
    monkeypatch.setenv("DATABASE_SERVER_URL", "http://x")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "t")
    with pytest.raises(FlyReaderNetworkError):
        FlyReader()._post("SELECT 1", "x")
    assert len(calls) == FlyReader.MAX_RETRIES
