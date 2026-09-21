from __future__ import annotations

import json

import pytest

from scripts.probe_league_update_freshness import probe_freshness


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
