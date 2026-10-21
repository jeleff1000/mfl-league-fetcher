"""Checkpoint admission is temporary, not missing NFL ranking data."""

import json
from io import BytesIO

import pandas as pd
import pytest
import requests

from multi_league.core.readers.fly_reader import FlyReader, FlyReaderError, FlyReaderNetworkError


@pytest.fixture
def clock(monkeypatch):
    state = {"now": 1000.0, "sleeps": [], "calls": []}
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "test-token")
    monkeypatch.delenv("CORPUS_MODE", raising=False)
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.time", lambda: state["now"])

    def sleep(seconds):
        assert seconds >= 0
        state["sleeps"].append(seconds)
        state["now"] += seconds

    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", sleep)
    return state


def busy_response(reason="checkpointing", retry_after="1"):
    response = requests.Response()
    response.status_code = 503
    response._content = json.dumps({"detail": "Query service is busy", "reason": reason}).encode()
    response.headers["Retry-After"] = retry_after
    return response


@pytest.mark.parametrize("method", ["query_df", "query_df_parquet"])
@pytest.mark.parametrize("refresh", [False, True])
def test_checkpoint_finishes_without_losing_rank_inputs(clock, monkeypatch, method, refresh):
    if refresh:
        monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1120")
    frame = pd.DataFrame([{"rank_alltime_qb_4pt": 216, "rank_qb_4pt": 7}])
    payload = BytesIO()
    frame.to_parquet(payload, index=False)

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        if clock["now"] < 1004:
            return busy_response()
        response = requests.Response()
        response.status_code = 200
        response._content = payload.getvalue() if method.endswith("parquet") else frame.to_json(orient="records").encode()
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    result = getattr(FlyReader(), method)("SELECT rank_alltime_qb_4pt, rank_qb_4pt", database="___ops", timeout_seconds=8, max_retries=3)
    assert result.to_dict("records") == [{"rank_alltime_qb_4pt": 216, "rank_qb_4pt": 7}]
    assert len(clock["calls"]) == 2
    assert clock["sleeps"] == [5]
    assert all(call["timeout"] == 8 for call in clock["calls"])


@pytest.mark.parametrize("method", ["query_df", "query_df_parquet"])
def test_checkpoint_wait_respects_publication_reserve(clock, monkeypatch, method):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1031.5")

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        return busy_response()

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    with pytest.raises(FlyReaderNetworkError, match="refresh read budget exhausted"):
        getattr(FlyReader(), method)("SELECT 1", database="___ops", timeout_seconds=8, max_retries=3)
    assert len(clock["calls"]) == 1
    assert clock["sleeps"] == [1.5]
    assert clock["now"] == 1001.5


@pytest.mark.parametrize("method", ["query_df", "query_df_parquet"])
@pytest.mark.parametrize("refresh,attempts,sleeps", [(False,3,[5,5]),(True,2,[5])])
def test_checkpoint_outage_does_not_expand_attempt_limit(clock, monkeypatch, method, refresh, attempts, sleeps):
    if refresh:
        monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1120")

    def post(_url, **kwargs):
        clock["calls"].append(kwargs)
        return busy_response()

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    with pytest.raises(FlyReaderError, match="503"):
        getattr(FlyReader(), method)("SELECT 1", database="___ops", timeout_seconds=8, max_retries=3)
    assert len(clock["calls"]) == attempts
    assert clock["sleeps"] == sleeps


@pytest.mark.parametrize("retry_after", ["-1", "nan", "inf", "bad"])
def test_invalid_checkpoint_retry_header_cannot_break_wait(clock, monkeypatch, retry_after):
    responses = iter([busy_response(retry_after=retry_after)])
    good = requests.Response()
    good.status_code = 200
    good._content = b'[{"rank_alltime_qb_4pt":216}]'
    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", lambda *_args, **_kwargs: next(responses, good))
    assert FlyReader().query("SELECT rank_alltime_qb_4pt", database="___ops", timeout_seconds=8, max_retries=2) == [{"rank_alltime_qb_4pt":216}]
    assert clock["sleeps"] == [5]
