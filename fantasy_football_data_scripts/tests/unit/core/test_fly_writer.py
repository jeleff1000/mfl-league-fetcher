"""Unit tests for FlyWriter."""

from unittest.mock import Mock, patch

import pytest
import requests

from multi_league.core.fly_writer import FlyWriter


@pytest.fixture
def fly_env(monkeypatch):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-token")


def test_constructor_rejects_corpus_mode_even_with_credentials(fly_env, monkeypatch):
    monkeypatch.setenv("CORPUS_MODE", "true")

    with pytest.raises(RuntimeError, match="corpus mode"):
        FlyWriter()


def test_constructor_requires_server_url(monkeypatch):
    monkeypatch.delenv("DATABASE_SERVER_URL", raising=False)
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-token")
    with pytest.raises(RuntimeError, match="DATABASE_SERVER_URL"):
        FlyWriter()


def test_constructor_requires_admin_token(monkeypatch):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.delenv("DATABASE_ADMIN_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_ADMIN_TOKEN"):
        FlyWriter()


def test_execute_posts_to_query_rw_with_admin_bearer(fly_env):
    writer = FlyWriter()
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = []
    with patch("multi_league.core.fly_writer.requests.post", return_value=mock_resp) as mock_post:
        writer.execute("CREATE TABLE t (x INT)", database="testdb")
    assert mock_post.call_count == 1
    args, kwargs = mock_post.call_args
    assert args[0] == "https://fly.test/query-rw"
    assert kwargs["headers"]["Authorization"] == "Bearer admin-token"
    assert kwargs["json"] == {"sql": "CREATE TABLE t (x INT)", "database": "testdb"}


def test_execute_targets_primary_machine_when_configured(fly_env, monkeypatch):
    monkeypatch.setenv("FLY_PRIMARY_MACHINE_ID", "machine-primary")
    writer = FlyWriter()
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = []
    with patch("multi_league.core.fly_writer.requests.post", return_value=mock_resp) as mock_post:
        writer.execute("CREATE TABLE t (x INT)", database="testdb")
    headers = mock_post.call_args.kwargs["headers"]
    assert headers["fly-force-instance-id"] == "machine-primary"


def test_execute_raises_on_non_200(fly_env):
    writer = FlyWriter()
    mock_resp = Mock(status_code=400, text="bad sql")
    with patch("multi_league.core.fly_writer.requests.post", return_value=mock_resp):
        with pytest.raises(RuntimeError, match=r"Query failed \(400\).*bad sql"):
            writer.execute("SELECT bogus", database="testdb")


def test_execute_retries_on_503(fly_env):
    writer = FlyWriter()
    resp_503 = Mock(status_code=503, text="draining")
    resp_200 = Mock(status_code=200)
    resp_200.json.return_value = []
    with patch("multi_league.core.fly_writer.time.sleep"):
        with patch(
            "multi_league.core.fly_writer.requests.post",
            side_effect=[resp_503, resp_200],
        ) as mock_post:
            writer.execute("DROP TABLE t", database="testdb")
    assert mock_post.call_count == 2


def test_execute_survives_long_503_burst(fly_env):
    writer = FlyWriter()
    resp_503 = Mock(status_code=503, text="")
    resp_200 = Mock(status_code=200)
    resp_200.json.return_value = []
    sequence = [resp_503 for _ in range(FlyWriter.MAX_RETRIES - 1)] + [resp_200]
    with (
        patch("multi_league.core.fly_writer.time.sleep") as mock_sleep,
        patch("multi_league.core.fly_writer.requests.post", side_effect=sequence) as mock_post,
    ):
        writer.execute("DROP TABLE t", database="testdb")
    assert mock_post.call_count == FlyWriter.MAX_RETRIES
    assert mock_sleep.call_count == FlyWriter.MAX_RETRIES - 1


def test_execute_retries_timeout_then_succeeds(fly_env):
    writer = FlyWriter()
    resp_200 = Mock(status_code=200)
    resp_200.json.return_value = []
    with (
        patch("multi_league.core.fly_writer.time.sleep") as mock_sleep,
        patch(
            "multi_league.core.fly_writer.requests.post",
            side_effect=[requests.exceptions.ReadTimeout("slow"), resp_200],
        ) as mock_post,
    ):
        writer.execute("DROP TABLE t", database="testdb")
    assert mock_post.call_count == 2
    mock_sleep.assert_called_once_with(FlyWriter.RETRY_BASE_DELAY)


def test_execute_honors_per_call_timeout_and_retry_limit(fly_env):
    writer = FlyWriter()
    timeout = requests.exceptions.ReadTimeout("slow")
    with patch("multi_league.core.fly_writer.requests.post", side_effect=timeout) as mock_post:
        with pytest.raises(RuntimeError, match="after 1/1 attempts"):
            writer.execute(
                "UPDATE main.league_credentials SET updated_at = current_timestamp",
                database="___ops",
                timeout_seconds=3,
                server_timeout_seconds=1,
                max_retries=1,
            )

    assert mock_post.call_count == 1
    assert mock_post.call_args.kwargs["timeout"] == 3
    assert mock_post.call_args.kwargs["json"]["timeout_seconds"] == 1


def test_execute_exhausts_retries_then_raises(fly_env):
    writer = FlyWriter()
    resp_503 = Mock(status_code=503, text="draining")
    with patch("multi_league.core.fly_writer.time.sleep"):
        with patch("multi_league.core.fly_writer.requests.post", return_value=resp_503):
            with pytest.raises(RuntimeError):
                writer.execute("DROP TABLE t", database="testdb")
