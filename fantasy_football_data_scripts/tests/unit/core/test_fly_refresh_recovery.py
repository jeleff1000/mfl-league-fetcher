"""A temporary Fly handoff must not end a valid refresh after three short reads."""
from io import BytesIO
import json

import pandas as pd
import pytest
import requests

from multi_league.core.readers.fly_reader import FlyReader, FlyReaderError, FlyReaderNetworkError


@pytest.fixture
def recovering_reader(monkeypatch):
    clock = {"now": 1000.0, "calls": []}
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "test-token")
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1120")
    monkeypatch.setenv("LEAGUE_UPDATE_READ_RECOVERY_SECONDS", "60")
    monkeypatch.delenv("CORPUS_MODE", raising=False)
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.time", lambda: clock["now"])
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", lambda delay: clock.update(now=clock["now"] + delay))
    return clock, FlyReader()


@pytest.mark.parametrize("method", ["query", "query_df_parquet"])
@pytest.mark.parametrize("failure", ["timeout", "checkpointing", "ops_writing", "gateway"])
def test_refresh_survives_34_second_database_handoff(recovering_reader, monkeypatch, method, failure):
    clock, reader = recovering_reader
    payload = BytesIO()
    pd.DataFrame([{"rank_qb_4pt": 7}]).to_parquet(payload, index=False)

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        if clock["now"] < 1034:
            if failure == "timeout":
                if clock["now"] + kwargs["timeout"] < 1034:
                    clock["now"] += kwargs["timeout"]
                    raise requests.exceptions.ReadTimeout("OPS snapshot handoff")
                clock["now"] = 1034
            else:
                response = requests.Response()
                response.status_code = 502 if failure == "gateway" else 503
                response._content = json.dumps({"reason": failure}).encode()
                response.headers["Retry-After"] = "1"
                return response
        response = requests.Response()
        response.status_code = 200
        response._content = payload.getvalue() if method.endswith("parquet") else b'[{"rank_qb_4pt":7}]'
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    actual = getattr(reader, method)("SELECT rank_qb_4pt", database="___ops", timeout_seconds=8, max_retries=3)
    rows = actual.to_dict("records") if isinstance(actual, pd.DataFrame) else actual
    assert rows == [{"rank_qb_4pt": 7}]
    assert len(clock["calls"]) >= (2 if failure == "timeout" else 4)
    assert 1034 <= clock["now"] < 1060


@pytest.mark.parametrize("method", ["query", "query_df_parquet"])
def test_read_recovery_is_bounded_by_publication_reserve(recovering_reader, monkeypatch, method):
    clock, reader = recovering_reader
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1065")

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        clock["now"] += kwargs["timeout"]
        raise requests.exceptions.ReadTimeout("still unavailable")

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    with pytest.raises(FlyReaderNetworkError):
        getattr(reader, method)("SELECT 1", database="___ops", timeout_seconds=8, max_retries=3)
    assert clock["now"] == 1035


@pytest.mark.parametrize("method", ["query", "query_df_parquet"])
@pytest.mark.parametrize("status,body", [(401, "Unauthorized"), (500, "Binder Error: missing column"), (500, "Catalog Error: Table missing does not exist")])
def test_recovery_does_not_retry_permanent_errors(recovering_reader, monkeypatch, method, status, body):
    clock, reader = recovering_reader
    response = requests.Response()
    response.status_code = status
    response._content = body.encode()
    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", lambda *_args, **kwargs: (clock["calls"].append(kwargs), response)[1])
    with pytest.raises(FlyReaderError):
        getattr(reader, method)("SELECT 1", database="___ops", timeout_seconds=8, max_retries=3)
    assert len(clock["calls"]) == 1


@pytest.mark.parametrize("method", ["query", "query_df_parquet"])
def test_permanent_outage_cannot_expand_the_recovery_window(recovering_reader, monkeypatch, method):
    clock, reader = recovering_reader
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "2000")
    monkeypatch.setenv("LEAGUE_UPDATE_READ_RECOVERY_SECONDS", "5000")

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        clock["now"] += kwargs["timeout"]
        raise requests.exceptions.ReadTimeout("still unavailable")

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    with pytest.raises(FlyReaderNetworkError):
        getattr(reader, method)("SELECT 1", database="___ops", timeout_seconds=8, max_retries=3)
    assert clock["now"] == 1060


def test_all_provider_refresh_workflows_enable_shared_recovery():
    from pathlib import Path

    root = Path(__file__).resolve().parents[4]
    for provider in ("espn", "sleeper", "yahoo"):
        workflow = (root / ".github" / "workflows" / f"{provider}_incremental_refresh_worker.yml").read_text(encoding="utf-8")
        assert "LEAGUE_UPDATE_READ_RECOVERY_SECONDS: '60'" in workflow


@pytest.mark.parametrize("method", ["query", "query_df_parquet"])
def test_recovery_gives_a_healthy_slow_read_time_to_finish(recovering_reader, monkeypatch, method):
    clock, reader = recovering_reader
    payload = BytesIO()
    pd.DataFrame([{"value": 7}]).to_parquet(payload, index=False)

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        if kwargs["timeout"] < 12:
            clock["now"] += kwargs["timeout"]
            raise requests.exceptions.ReadTimeout("healthy query needs twelve seconds")
        clock["now"] += 12
        response = requests.Response()
        response.status_code = 200
        response._content = payload.getvalue() if method.endswith("parquet") else b'[{"value":7}]'
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    result = getattr(reader, method)("SELECT 7 AS value", database="___ops", timeout_seconds=8, max_retries=3)
    rows = result.to_dict("records") if isinstance(result, pd.DataFrame) else result
    assert rows == [{"value": 7}]
    assert clock["calls"][0]["timeout"] == 8
    assert len(clock["calls"]) == 2
    assert clock["now"] < 1060
