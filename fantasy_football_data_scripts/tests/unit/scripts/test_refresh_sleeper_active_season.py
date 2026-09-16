"""Focused lineage safety tests for Sleeper's active-season worker."""

from __future__ import annotations

import sys
from pathlib import Path


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
