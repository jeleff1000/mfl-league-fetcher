import pytest

from scripts.sota_recon.witness_audit_v2.source_column_matrix import (
    discover_sources,
    discover_columns,
    build_canonical_registry,
    map_source_column,
    is_newspaper_source,
    normalize_column_name,
    relationship_type,
)


def test_contract_discovery_preserves_source_and_columns(tmp_path):
    contract_path = tmp_path / "contracts.json"
    contract_path.write_text(
        '{"contracts": {"pfr_season:passing": {"path": "D:/pfr.parquet", "grain": "season", "n_rows": 10, "atoms": {"passing_yards": {"col": "passing_yards", "era_min": 1933, "era_max": 2025, "total_nonzero": 8}}, "by_year": {"2020": {"passing_yards": 4}}}}}'
    )
    sources = discover_sources(contract_path)
    assert sources[0]["source_id"] == "pfr_season:passing"
    cols = discover_columns(sources[0])
    assert cols[0]["raw_column"] == "passing_yards"
    assert cols[0]["year_min"] == 1933
    assert cols[0]["year_max"] == 2025


def test_contract_discovery_includes_declared_metadata_columns_once(tmp_path):
    contract_path = tmp_path / "contracts.json"
    contract_path.write_text(
        '{"contracts": {"x": {"path": "x.parquet", "atoms": {"year": {"col": "year", "era_min": 2000, "era_max": 2020}}, "meta_cols": ["year", "player", "source_url"]}}}'
    )
    cols = discover_columns(discover_sources(contract_path)[0])
    names = [c["raw_column"] for c in cols]
    assert names.count("year") == 1
    assert "player" in names and "source_url" in names
    assert next(c for c in cols if c["raw_column"] == "player")["column_kind"] == "metadata"


def test_source_specific_semantic_hint_preserves_generic_raw_column():
    source = {
        "source_id": "nflcom_season:rushing",
        "grain": "season",
        "atoms": {"rushing__40": {"col": "40", "era_min": 1981, "era_max": 2025}},
        "meta_cols": [],
    }
    column = discover_columns(source)[0]
    assert column["raw_column"] == "40"
    assert column["canonical_hint"] == "rushing_40plus"
    registry = build_canonical_registry([column])
    mapped = map_source_column(column, registry)
    assert mapped["canonical_column"] == "rushing_40plus"


def test_contract_discovery_keeps_newspaper_source_but_marks_excluded(tmp_path):
    p = tmp_path / "contracts.json"
    p.write_text(
        '{"contracts": {"newspaper_promoted:box": {"path": "D:/news.duckdb", "grain": "game", "n_rows": 1, "atoms": {"passing_yards": {"col": "passing_yards"}}, "by_year": {}}}}'
    )
    source = discover_sources(p)[0]
    assert source["newspaper_excluded"] is True


def test_newspaper_sources_are_excluded_from_matrix_matching():
    assert is_newspaper_source("newspaper_promoted:player_game_stat_claim")
    assert is_newspaper_source("newspaper_sidecar:weekly_player_stat_cells")
    assert not is_newspaper_source("pfr_season:passing")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Pass Yds", "pass_yds"),
        ("passing yds", "passing_yds"),
        ("Passing-Yards", "passing_yards"),
        ("  Receiving  Yards  ", "receiving_yards"),
    ],
)
def test_normalize_column_name_is_conservative(raw, expected):
    assert normalize_column_name(raw) == expected


def test_passing_and_receiving_yards_are_not_aliases():
    assert relationship_type("passing_yards", "receiving_yards") == "not_comparable"


def test_same_semantics_can_be_an_alias():
    assert relationship_type("passing_yards", "pass_yds") == "alias"


def test_mapping_emits_alias_family_and_gate_eligibility():
    column = {"source_id": "pfr", "raw_column": "pass yds", "dtype": "BIGINT"}
    registry = build_canonical_registry([{"raw_column": "passing_yards", "source_id": "canon"}])
    mapped = map_source_column(column, registry, {"passing_yards": ["pass yds"]})
    assert mapped["canonical_column"] == "passing_yards"
    assert mapped["relationship_type"] == "alias"
    assert mapped["alias_family"] == "passing_yards"
    assert mapped["gate_eligible"] is True
