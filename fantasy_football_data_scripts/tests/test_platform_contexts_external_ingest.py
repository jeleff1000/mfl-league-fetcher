"""Tests that SleeperContext and ESPNContext have external_ingest fields (mirrors LeagueContext)."""

from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext
from multi_league.data_fetchers.espn.espn_context import ESPNContext


def test_sleeper_context_has_external_ingest_fields():
    ctx = SleeperContext(league_id="917358392482123776", league_name="Test League", username="test_user")
    assert ctx.external_column_maps == []
    assert ctx.external_identity_maps == {}
    assert ctx.has_external_data is False
    assert ctx.merge_source is None
    assert ctx.merge_sources == []
    assert ctx.is_private is False


def test_sleeper_context_preserves_private_flag_from_payload():
    ctx = SleeperContext.from_dict(
        {
            "league_id": "917358392482123776",
            "league_name": "Test League",
            "username": "test_user",
            "is_private": True,
        }
    )
    assert ctx.is_private is True


def test_sleeper_context_preserves_external_data_flag_from_payload():
    ctx = SleeperContext.from_dict(
        {
            "league_id": "917358392482123776",
            "league_name": "Test League",
            "username": "test_user",
            "has_external_data": True,
        }
    )
    assert ctx.has_external_data is True


def test_sleeper_context_preserves_merge_source_from_payload():
    merge_source = {
        "source_db": "old_yahoo_league",
        "year_range": {"start": 2010, "end": 2020},
    }
    ctx = SleeperContext.from_dict(
        {
            "league_id": "917358392482123776",
            "league_name": "Test League",
            "username": "test_user",
            "has_external_data": True,
            "merge_source": merge_source,
        }
    )
    assert ctx.merge_source == merge_source


def test_sleeper_context_preserves_merge_sources_from_payload():
    merge_sources = [
        {"source_db": "old_yahoo_league", "merge_years": [2018, 2019]},
        {"source_db": "old_espn_league", "merge_years": [2020]},
    ]
    ctx = SleeperContext.from_dict(
        {
            "league_id": "917358392482123776",
            "league_name": "Test League",
            "username": "test_user",
            "has_external_data": True,
            "merge_sources": merge_sources,
        }
    )
    assert ctx.merge_sources == merge_sources


def test_espn_context_has_external_ingest_fields():
    ctx = ESPNContext(league_id=71580, league_name="Test League")
    assert ctx.external_column_maps == []
    assert ctx.external_identity_maps == {}
    assert ctx.has_external_data is False
    assert ctx.merge_source is None
    assert ctx.merge_sources == []
    assert ctx.is_private is False


def test_espn_context_preserves_private_flag_from_payload():
    ctx = ESPNContext.from_dict(
        {
            "league_id": 71580,
            "league_name": "Test League",
            "is_private": True,
        }
    )
    assert ctx.is_private is True


def test_espn_context_preserves_external_data_flag_from_payload():
    ctx = ESPNContext.from_dict(
        {
            "league_id": 71580,
            "league_name": "Test League",
            "has_external_data": True,
        }
    )
    assert ctx.has_external_data is True


def test_espn_context_preserves_merge_source_from_payload():
    merge_source = {
        "source_db": "old_yahoo_league",
        "year_range": {"start": 2010, "end": 2020},
    }
    ctx = ESPNContext.from_dict(
        {
            "league_id": 71580,
            "league_name": "Test League",
            "has_external_data": True,
            "merge_source": merge_source,
        }
    )
    assert ctx.merge_source == merge_source


def test_espn_context_preserves_merge_sources_from_payload():
    merge_sources = [
        {"source_db": "old_yahoo_league", "merge_years": [2018, 2019]},
        {"source_db": "old_sleeper_league", "merge_years": [2020]},
    ]
    ctx = ESPNContext.from_dict(
        {
            "league_id": 71580,
            "league_name": "Test League",
            "has_external_data": True,
            "merge_sources": merge_sources,
        }
    )
    assert ctx.merge_sources == merge_sources
