from __future__ import annotations

import json
import inspect
import subprocess
import sys
from io import BytesIO
from urllib.error import HTTPError, URLError

import pytest

from scripts.probe_league_update_freshness import probe_freshness
from scripts import probe_league_update_freshness as probe_module


class _Response:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def test_probe_uses_the_guarded_production_route_and_returns_its_digest():
    captured = {}

    def opener(request, *, timeout):
        captured.update(url=request.full_url, method=request.method, origin=request.headers["Origin"], timeout=timeout)
        return _Response({"healthy": True, "stale": False, "observed_manifest_digest": "exact-digest"})

    assert probe_freshness("paid_league", timeout=7, opener=opener) == "exact-digest"
    assert captured == {
        "url": "https://www.leaguehistory.app/api/league/paid_league/update/probe",
        "method": "POST",
        "origin": "https://www.leaguehistory.app",
        "timeout": 7,
    }


def test_probe_returns_a_healthy_stale_manifest_for_manual_update():
    payload = {"healthy": True, "stale": True, "observed_manifest_digest": "stale-digest"}

    assert probe_freshness(
        "paid_league",
        opener=lambda *_args, **_kwargs: _Response(payload),
    ) == "stale-digest"


@pytest.mark.parametrize("payload", [
    {"healthy": False, "stale": False, "observed_manifest_digest": "digest"},
    {"healthy": True, "stale": False, "observed_manifest_digest": None},
])
def test_probe_rejects_an_unverified_manifest(payload):
    with pytest.raises(RuntimeError, match="healthy source manifest"):
        probe_freshness("paid_league", opener=lambda *_args, **_kwargs: _Response(payload))


@pytest.fixture
def clock(monkeypatch):
    elapsed, sleeps = [0.0], []
    monkeypatch.setattr(probe_module, "monotonic", lambda: elapsed[0], raising=False)
    monkeypatch.setattr(probe_module, "time", lambda: 1000 + elapsed[0], raising=False)
    def sleep(seconds):
        sleeps.append(seconds)
        elapsed[0] += seconds
    monkeypatch.setattr(probe_module, "sleep", sleep, raising=False)
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    return elapsed, sleeps


def http_error(status, payload=None, headers=None):
    return HTTPError("https://www.leaguehistory.app/probe", status, "upstream failure", headers or {},
                     BytesIO(json.dumps(payload or {"code": "provider_error"}).encode()))


@pytest.mark.parametrize("failure", [502, 503, 504, 500, URLError("temporary DNS failure"), TimeoutError()])
def test_transient_probe_failure_recovers_without_using_the_failed_digest(clock, failure):
    calls = []
    def opener(request, *, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            raise http_error(failure, {"code": "provider_error", "observed_manifest_digest": "old"}) \
                if isinstance(failure, int) else failure
        return _Response({"healthy": True, "observed_manifest_digest": "verified-new"})
    assert probe_freshness("paid_league", opener=opener) == "verified-new"
    assert len(calls) == 2
    assert clock[1] == [1]


def test_healthy_probe_adds_no_retry_delay(clock):
    assert probe_freshness("paid_league", opener=lambda *a, **k:
                           _Response({"healthy": True, "observed_manifest_digest": "verified"})) == "verified"
    assert clock[1] == []


@pytest.mark.parametrize("status", [400, 401, 403, 409])
def test_real_guard_and_credential_failures_are_not_retried(clock, status):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        raise http_error(status)
    with pytest.raises(RuntimeError, match=f"failed \\({status}\\)"):
        probe_freshness("paid_league", opener=opener)
    assert len(calls) == 1
    assert clock[1] == []


def test_provider_configuration_failure_is_not_retried(clock):
    with pytest.raises(RuntimeError, match="provider_configuration"):
        probe_freshness("paid_league", opener=lambda *a, **k:
                       (_ for _ in ()).throw(http_error(500, {"code": "provider_configuration"})))
    assert clock[1] == []


def test_persistent_gateway_failure_stops_after_three_attempts(clock):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        raise http_error(502)
    with pytest.raises(RuntimeError, match="failed \\(502\\)"):
        probe_freshness("paid_league", opener=opener)
    assert len(calls) == 3
    assert clock[1] == [1, 2]


def test_in_progress_lease_cannot_publish_the_previous_digest(clock):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        response = _Response({"healthy": True, "leased": True, "observed_manifest_digest": "old"}
                             if len(calls) == 1 else {"healthy": True, "observed_manifest_digest": "new"})
        response.status = 202 if len(calls) == 1 else 200
        return response
    assert probe_freshness("paid_league", opener=opener) == "new"
    assert len(calls) == 2


@pytest.mark.parametrize("after,expected_calls", [(2, 2), (120, 1)])
def test_throttle_retry_after_is_respected_without_long_sleep(clock, after, expected_calls):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1: raise http_error(429, headers={"Retry-After": str(after)})
        return _Response({"healthy": True, "observed_manifest_digest": "new"})
    if expected_calls == 1:
        with pytest.raises(RuntimeError): probe_freshness("paid_league", opener=opener)
        assert clock[1] == []
    else:
        assert probe_freshness("paid_league", opener=opener) == "new"
        assert clock[1] == [2]
    assert len(calls) == expected_calls


def test_timeout_recovery_stays_inside_the_probe_budget(clock):
    calls = []
    def opener(request, *, timeout):
        calls.append(timeout)
        if len(calls) == 1:
            clock[0][0] += 20
            raise TimeoutError()
        return _Response({"healthy": True, "observed_manifest_digest": "new"})
    assert probe_freshness("paid_league", opener=opener) == "new"
    assert calls == [20, 9]


@pytest.mark.parametrize("after,expected_wait", [("Thu, 01 Jan 1970 00:16:42 GMT", 2),
                                               ("Thu, 01 Jan 1970 00:17:40 GMT", None)])
def test_http_date_retry_after_is_respected(clock, after, expected_wait):
    calls = []
    def opener(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1: raise http_error(429, headers={"Retry-After": after})
        return _Response({"healthy": True, "observed_manifest_digest": "new"})
    if expected_wait is None:
        with pytest.raises(RuntimeError): probe_freshness("paid_league", opener=opener)
        assert len(calls) == 1
    else:
        assert probe_freshness("paid_league", opener=opener) == "new"
    assert clock[1] == ([] if expected_wait is None else [expected_wait])


def test_slow_response_cannot_return_a_healthy_manifest_after_deadline(clock):
    class SlowResponse(_Response):
        def read(self):
            clock[0][0] += 31
            return super().read()
    with pytest.raises(RuntimeError, match="budget"):
        probe_freshness("paid_league", opener=lambda *a, **k:
                       SlowResponse({"healthy": True, "observed_manifest_digest": "late"}))


def test_probe_hard_deadline_terminates_a_blocked_transport():
    # Exercise the real probe in a child: a hung opener cannot interrupt itself.
    source = inspect.getsource(probe_module)
    code = ("import os,time\nnamespace={'__name__':'deadline_test'}\n"
            f"exec({source!r},namespace)\n"
            "os.environ['LEAGUE_UPDATE_DEADLINE_EPOCH']=str(time.time()+30.25)\n"
            "def blocked(*args,**kwargs): time.sleep(10)\n"
            "namespace['probe_freshness']('paid_league',opener=blocked)\n")
    result = subprocess.run([sys.executable, "-I", "-S", "-c", code],
                            capture_output=True, timeout=5)
    assert result.returncode == 124


def test_expired_worker_budget_fails_before_any_request(clock, monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1029")
    def unexpected(*args, **kwargs): raise AssertionError("no HTTP call after the read budget")
    with pytest.raises(RuntimeError, match="budget"):
        probe_freshness("paid_league", opener=unexpected)


def test_probe_deadline_keeps_the_existing_publication_reserve(clock, monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1035")
    calls = []
    def opener(request, *, timeout):
        calls.append(timeout)
        clock[0][0] += 4
        raise http_error(503)
    with pytest.raises(RuntimeError): probe_freshness("paid_league", opener=opener)
    assert calls == [5]
    assert clock[1] == []


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_each_worker_manual_path_claims_only_the_recovered_manifest(clock, monkeypatch, platform):
    from scripts import claim_manual_league_update as manual
    calls, claims = [], []
    original_probe = probe_freshness
    def opener(request, **kwargs):
        calls.append(request.full_url)
        if len(calls) == 1: raise http_error(502)
        return _Response({"healthy": True, "observed_manifest_digest": "verified"})
    monkeypatch.setattr(probe_module, "probe_freshness", lambda db:
                        original_probe(db, opener=opener))
    def claim(reader, writer, **kwargs):
        assert len(calls) == 2
        claims.append(kwargs["platform"])
        return {"dispatch_token": "owned", "attempt_id": "attempt", "claim_version": 1}
    result = manual.prepare_update_execution(object(), object(), db_name="paid_league", platform=platform,
        execute=True, observed_manifest_digest="", dispatch_token="", attempt_id="", claim_version=1,
        run_id=10, run_attempt=1, environment={}, claim=claim)
    assert result["observed_manifest_digest"] == "verified"
    assert result["dispatch_token"] == "owned"
    assert claims == [platform]


def test_exhausted_probe_never_claims_or_changes_execution_environment(clock, monkeypatch):
    from scripts import claim_manual_league_update as manual
    original_probe = probe_freshness
    def opener(*args, **kwargs): raise http_error(502)
    monkeypatch.setattr(probe_module, "probe_freshness", lambda db: original_probe(db, opener=opener))
    environment = {"existing": "unchanged"}
    def claim(*args, **kwargs): raise AssertionError("unverified probe must never claim")
    with pytest.raises(RuntimeError):
        manual.prepare_update_execution(object(), object(), db_name="paid_league", platform="yahoo",
            execute=True, observed_manifest_digest="", dispatch_token="", attempt_id="", claim_version=1,
            run_id=10, run_attempt=1, environment=environment, claim=claim)
    assert environment == {"existing": "unchanged"}
