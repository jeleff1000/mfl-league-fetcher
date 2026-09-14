"""Focused safety tests for ESPN's active-season refresh entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


def test_build_context_reuses_supplied_frontend_settings(tmp_path, monkeypatch):
    """The active worker must not re-read context after it has already hydrated it."""
    from multi_league.data_fetchers.espn import espn_api_client, espn_context
    from multi_league.utils import credential_store
    from refresh_espn_active_season import _build_context

    class Context:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.team_to_guid = {}
            self.team_to_team_name = {}
            self.team_to_manager_by_year = {}
            self.team_to_guid_by_year = {}

        def save(self, path):
            path.write_text("{}", encoding="utf-8")

    class Client:
        def __init__(self, *_args):
            pass

        def get_league(self, _year):
            return SimpleNamespace(
                teams=[SimpleNamespace(team_id=1, team_name="One", owners=[{"id": "owner-1"}])]
            )

    class Reader:
        def query(self, *_args, **_kwargs):
            raise AssertionError("hydrated frontend settings must avoid a second Fly context query")

    monkeypatch.setattr(
        credential_store,
        "retrieve_espn_credentials",
        lambda _db_name, **_kwargs: {
            "league_id": 134179,
            "league_name": "Registry Name",
            "espn_s2": "s2",
            "swid": "{SWID}",
        },
    )
    monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
    monkeypatch.setattr(espn_context, "ESPNContext", Context)
    monkeypatch.setattr(espn_context, "build_manager_names", lambda _teams: {1: "Manager One"})

    ctx, context_path, _client, _league = _build_context(
        reader=Reader(),
        db_name="private_espn",
        active_year=2026,
        work_dir=tmp_path,
        frontend_settings={
            "league_name": "Canonical Name",
            "manager_name_overrides": {"Manager One": "One"},
            "franchise_merges": [],
            "keeper_rules": None,
            "league_rules": None,
            "standings_weights": None,
            "is_private": True,
        },
    )

    assert ctx.league_name == "Canonical Name"
    assert ctx.is_private is True
    assert context_path.is_file()


def test_espn_draft_manifest_uses_the_complete_loaded_draft_order():
    """A retained ESPN draft can be checked without a second league request."""
    from refresh_espn_active_season import _espn_draft_manifest

    league = SimpleNamespace(draft=[SimpleNamespace(), SimpleNamespace(), SimpleNamespace()])

    assert _espn_draft_manifest(league).equals(pd.DataFrame({"pick": [1, 2, 3]}))
