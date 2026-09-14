# tests/test_league_context_external_ingest.py
from multi_league.core.league_context import LeagueContext


def test_league_context_has_external_ingest_fields():
    ctx = LeagueContext(league_id="x", league_name="X", require_oauth=False)
    assert ctx.external_column_maps == []
    assert ctx.external_identity_maps == {}
    assert ctx.franchise_merges == []
    assert ctx.is_private is False


def test_league_context_round_trip_external_fields(tmp_path):
    ctx = LeagueContext(
        league_id="x",
        league_name="X",
        is_private=True,
        franchise_merges=[{"display_name": "Greg", "owner_ids": ["a", "b"]}],
        external_column_maps=[{"table": "matchup", "column_map": {"year": "yr"}}],
        external_identity_maps={"Ezra": {"franchise_id": "g_1"}},
        require_oauth=False,
    )
    path = tmp_path / "ctx.json"
    ctx.save(path)
    loaded = LeagueContext.load(path)
    assert loaded.is_private is True
    assert loaded.franchise_merges == [{"display_name": "Greg", "owner_ids": ["a", "b"]}]
    assert loaded.external_column_maps == [{"table": "matchup", "column_map": {"year": "yr"}}]
    assert loaded.external_identity_maps == {"Ezra": {"franchise_id": "g_1"}}
