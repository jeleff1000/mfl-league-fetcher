from __future__ import annotations

import pandas as pd

from scripts import refresh_espn_active_season as subject


def test_espn_rosters_only_cover_weeks_with_finalized_nfl_games() -> None:
    selector = getattr(
        subject,
        "_espn_finalized_roster_weeks",
        lambda *, refresh_weeks, finalized_ops: list(refresh_weeks),
    )

    assert selector(
        refresh_weeks=[2, 3],
        finalized_ops=pd.DataFrame({"week": [2, 2]}),
    ) == [2]


def _stub_context_modules(monkeypatch, *, credentials, teams):
    import sys
    import types

    calls: dict = {}

    class Client:
        def __init__(self, league_id, espn_s2=None, swid=None):
            calls["client"] = (league_id, espn_s2, swid)

        def get_league(self, year):
            return types.SimpleNamespace(teams=teams)

    store = types.ModuleType("multi_league.utils.credential_store")
    store.retrieve_espn_credentials = lambda db_name, reader=None: credentials
    api = types.ModuleType("multi_league.data_fetchers.espn.espn_api_client")
    api.ESPNAPIClient = Client
    monkeypatch.setitem(sys.modules, store.__name__, store)
    monkeypatch.setitem(sys.modules, api.__name__, api)
    return calls


def test_public_espn_refresh_reads_without_stored_cookies(monkeypatch, tmp_path) -> None:
    # the_boogie_belt 2026-11-11: a public league has no cookie row.
    import pytest

    team = type("Team", (), {"team_id": 1, "team_name": "A", "owners": []})()
    calls = _stub_context_modules(monkeypatch, credentials=None, teams=[team])
    ctx = subject._build_context(
        reader=None, db_name="espn_public", active_year=2026, work_dir=tmp_path,
        active_league_id="72374464", frontend_settings={"league_name": "Public", "is_private": False},
    )[0]
    assert calls["client"] == (72374464, None, None)
    assert ctx.espn_s2 is None and ctx.swid is None

    # A private league read anonymously returns no teams and fails closed.
    _stub_context_modules(monkeypatch, credentials=None, teams=[])
    with pytest.raises(RuntimeError, match="no active league teams"):
        subject._build_context(
            reader=None, db_name="espn_private", active_year=2026, work_dir=tmp_path,
            active_league_id="1", frontend_settings={"league_name": "Private"},
        )
