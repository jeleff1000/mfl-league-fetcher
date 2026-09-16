"""Focused lineage safety tests for Sleeper's active-season worker."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))


class ChainReader:
    def __init__(self, *, context: dict, settings: list[dict]):
        self.context = context
        self.settings = settings

    def query(self, sql: str, *, database: str):
        assert database == "___leagues"
        if "information_schema.columns" in sql:
            return [{"column_name": name} for name in self.context]
        if "FROM public.league_context" in sql:
            assert "WHERE db_name = 'mixed_league'" in sql
            return [self.context]
        if "FROM public.league_settings" in sql:
            assert "WHERE db_name = 'mixed_league'" in sql
            return self.settings
        raise AssertionError(f"unexpected Fly query: {sql}")


@pytest.mark.parametrize('previous', [None, 'old'])
def test_context_only_onboarding_identity_reuses_import_history_discovery(tmp_path, monkeypatch, previous):
    from refresh_sleeper_active_season import _build_context
    from multi_league.data_fetchers.sleeper import sleeper_api_client

    class Provider:
        def get_league(self, league_id):
            return {
                'active': {'league_id':'active', 'season':'2026', 'previous_league_id':previous, 'name':'Provider Name'},
                'old': {'league_id':'old', 'season':'2025', 'previous_league_id':None},
            }[league_id]

        def get_league_users(self, league_id):
            assert league_id == 'active'
            return [{'user_id':'u1', 'display_name':'Owner', 'metadata':{}}]

        def get_league_rosters(self, league_id):
            assert league_id == 'active'
            return [{'roster_id':1, 'owner_id':'u1', 'players':[], 'settings':{}}]

    monkeypatch.setattr(sleeper_api_client, 'SleeperAPIClient', Provider)
    context = {'platform':'sleeper', 'league_id':'active', 'league_ids_json':None,
               'league_name':'Saved Name', 'manager_name_overrides_json':'{"Owner":"Shared Alias"}'}
    ctx, path, _, league = _build_context(
        reader=ChainReader(context=context, settings=[]), db_name='mixed_league',
        active_year=2026, work_dir=tmp_path,
    )
    assert ctx.league_id == 'active'
    assert ctx.league_name == 'Saved Name'
    assert ctx.manager_name_overrides == {'Owner':'Shared Alias'}
    assert ctx.league_ids == ({'2026':'active'} if previous is None else {'2026':'active','2025':'old'})
    assert league['league_id'] == 'active'
    assert path.is_file()


@pytest.mark.parametrize('broken', ['cycle', 'wrong_identity', 'missing_link', 'duplicate_season'])
def test_shared_import_discovery_rejects_unprovable_chain(broken):
    from multi_league.data_fetchers.sleeper.sleeper_context import discover_league_history

    leagues = {
        'new': {'league_id':'new', 'season':'2026', 'previous_league_id':'old'},
        'old': {'league_id':'old', 'season':'2025', 'previous_league_id':None},
    }
    if broken == 'cycle':
        leagues['old']['previous_league_id'] = 'new'
    elif broken == 'wrong_identity':
        leagues['old']['league_id'] = 'unrelated'
    elif broken == 'missing_link':
        leagues['old'] = {}
    else:
        leagues['old']['season'] = '2026'

    class Provider:
        calls = 0

        def get_league(self, league_id):
            self.calls += 1
            if self.calls > 4:
                raise AssertionError('unbounded renewal discovery')
            return leagues[league_id]

    with pytest.raises(ValueError, match='chain|identity|season'):
        discover_league_history(Provider(), 'new', skip_empty_seasons=False)


def test_sleeper_worker_excludes_other_platform_context_chain():
    from refresh_sleeper_active_season import _load_persisted_sleeper_chain

    reader = ChainReader(
        context={
            "platform": "yahoo", "league_id": "yahoo-2014",
            "league_name": "Mixed League",
            "league_ids_json": '{"2014":"yahoo-2014","2025":"yahoo-2025"}',
        },
        settings=[{"year": 2025, "platform": "sleeper", "league_key": "sleeper-2025"}],
    )
    _, known = _load_persisted_sleeper_chain(reader, db_name="mixed_league")
    assert known == {"2025": "sleeper-2025"}


def test_sleeper_worker_rejects_a_multiplatform_timeline_owned_by_yahoo_in_the_active_year():
    """A Sleeper workflow cannot fetch a year assigned to Yahoo's active leg."""
    import pytest
    from refresh_sleeper_active_season import _load_persisted_sleeper_chain

    reader = ChainReader(
        context={
            "platform": "sleeper", "league_id": "sleeper-2025",
            "league_name": "Mixed League",
            "league_ids_json": '{"2025":"sleeper-2025"}',
        },
        settings=[
            {"year": 2025, "platform": "sleeper", "league_key": "sleeper-2025"},
            {"year": 2026, "platform": "yahoo", "league_key": "470.l.999"},
        ],
    )

    with pytest.raises(RuntimeError, match="belongs to yahoo"):
        _load_persisted_sleeper_chain(reader, db_name="mixed_league", active_year=2026)


def test_sleeper_worker_retains_matching_context_chain_and_rejects_conflict():
    from refresh_sleeper_active_season import _load_persisted_sleeper_chain
    import pytest

    context = {
        "platform": "sleeper", "league_id": "sleeper-2025",
        "league_name": "Sleeper League",
        "league_ids_json": '{"2024":"sleeper-2024","2025":"sleeper-2025"}',
    }
    _, known = _load_persisted_sleeper_chain(
        ChainReader(context=context, settings=[{"year": 2025, "platform": "sleeper", "league_key": "sleeper-2025"}]),
        db_name="mixed_league",
    )
    assert known == {"2024": "sleeper-2024", "2025": "sleeper-2025"}
    with pytest.raises(RuntimeError, match="conflicting Sleeper league IDs"):
        _load_persisted_sleeper_chain(
            ChainReader(context=context, settings=[{"year": 2025, "platform": "sleeper", "league_key": "other-2025"}]),
            db_name="mixed_league",
        )


def test_sleeper_worker_excludes_externally_merged_yahoo_years():
    from refresh_sleeper_active_season import _load_persisted_sleeper_chain

    reader = ChainReader(
        context={
            "platform": "sleeper", "league_id": "sleeper-2025",
            "league_name": "Mixed League",
            "league_ids_json": '{"2014":"yahoo-2014","2024":"sleeper-2024","2025":"sleeper-2025"}',
        },
        settings=[
            {"year": 2014, "platform": "yahoo", "league_key": "yahoo-2014"},
            {"year": 2025, "platform": "sleeper", "league_key": "sleeper-2025"},
        ],
    )
    _, known = _load_persisted_sleeper_chain(reader, db_name="mixed_league")
    assert known == {"2024": "sleeper-2024", "2025": "sleeper-2025"}


def test_sleeper_worker_rejects_overlapping_imported_provider_year():
    from refresh_sleeper_active_season import _load_persisted_sleeper_chain
    import pytest

    reader = ChainReader(
        context={
            "platform": "sleeper", "league_id": "sleeper-2025",
            "league_name": "Mixed League", "league_ids_json": None,
        },
        settings=[
            {"year": 2025, "platform": "yahoo", "league_key": "461.l.123"},
            {"year": 2025, "platform": "sleeper", "league_key": "sleeper-2025"},
        ],
    )
    with pytest.raises(RuntimeError, match="overlapping provider ownership"):
        _load_persisted_sleeper_chain(reader, db_name="mixed_league")


def test_sleeper_worker_rejects_wrong_intermediate_renewal_identity():
    from refresh_sleeper_active_season import _renewal_chain_reaches_seed

    leagues = {
        "new": {"league_id": "new", "previous_league_id": "link"},
        "link": {"league_id": "unrelated", "previous_league_id": "old"},
    }
    assert not _renewal_chain_reaches_seed(
        "new", seed_league_id="old", get_league=leagues.get,
    )


def test_active_sleeper_roster_scope_exposes_points_before_finalized_game_gate():
    """The weekly gate consumes canonical scoring, while the fetcher emits points."""
    import pandas as pd

    from refresh_sleeper_active_season import _active_sleeper_roster_scope

    source = pd.DataFrame(
        [{"week": 1, "nfl_team": "CHI", "points": 21.5, "sleeper_player_id": "1"}]
    )
    actual = _active_sleeper_roster_scope(source)

    assert actual["fantasy_points"].tolist() == [21.5]
    assert actual["points"].tolist() == [21.5]
    assert actual["nfl_team"].tolist() == ["CHI"]
