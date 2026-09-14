"""Contract tests for NGS grain coverage and denominator declarations."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "scripts/sota_recon/witness_gate/contracts/stat_contracts.v1.json"


def _write_parquet(path: Path, sql: str) -> None:
    import duckdb

    con = duckdb.connect()
    con.execute(f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()


def _ngs_contracts():
    payload = json.loads(CONTRACT.read_text())
    return {row["stat_id"]: row for row in payload["stats"] if row["stat_id"].startswith("ngs_")}


def test_ngs_career_is_contractually_derived_and_bio_is_identity_only():
    contracts = _ngs_contracts()
    for row in contracts.values():
        assert "weekly" in row["grains"]
        assert "season" in row["grains"]
        assert "season_all" in row["grains"]
        assert "career" in row["grains"]
        assert "career_all" in row["grains"]
        assert "bio" not in row["grains"]


def test_ngs_direct_published_rates_do_not_claim_generic_denominators():
    contracts = _ngs_contracts()
    for stat_id in (
        "ngs_avg_separation",
        "ngs_avg_yac_above_expectation",
        "ngs_expected_completion_pct",
    ):
        rate = contracts[stat_id].get("rate", {})
        assert rate.get("direct_rate_no_components") is True
        assert "weighted_by" not in rate, stat_id
    contracts_expected = {
        "ngs_avg_air_yards_differential": "passing_air_yards_components",
        "ngs_pct_share_intended_air_yards": "team_receiving_air_yards",
        "ngs_rush_efficiency": "rushing_yards",
    }
    for stat_id, denominator in contracts_expected.items():
        assert contracts[stat_id]["rate"]["career_denominator"] == denominator


def test_ngs_denominator_map_covers_every_non_additive_metric():
    from scripts.sota_recon.build_advanced_season_career_v26 import (
        NGS_ADDITIVE,
        NGS_COMPONENT_DERIVED,
        NGS_DENOMINATOR,
        _ngs_cols,
    )

    import duckdb

    con = duckdb.connect()
    ngs_cols = set(_ngs_cols(con))
    con.close()
    assert NGS_ADDITIVE | set(NGS_DENOMINATOR) | set(NGS_COMPONENT_DERIVED) == ngs_cols
    assert not NGS_ADDITIVE & set(NGS_DENOMINATOR)
    assert not NGS_COMPONENT_DERIVED.keys() & set(NGS_DENOMINATOR)


def test_ngs_source_columns_have_weekly_and_season_witness_maps():
    import duckdb
    from scripts.sota_recon.witness_map import WITNESS_MAP

    con = duckdb.connect()
    paths = {
        "ngs_weekly_raw": r"D:/league-history-data/nfl/raw/nextgen_stats/ngs_weekly_2016_2025.parquet",
        "ngs_season_published": r"D:/league-history-data/nfl/raw/nextgen_stats/ngs_season_2016_2025.parquet",
    }
    for source_key, path in paths.items():
        columns = {
            row[0]
            for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
            if row[0].startswith("ngs_")
        }
        mapped = {
            spec.source_col
            for spec in WITNESS_MAP
            if spec.source_key == source_key and spec.source_col.startswith("ngs_")
        }
        reverse_mapped = {
            spec.v26_col
            for spec in WITNESS_MAP
            if spec.source_key == source_key and spec.v26_col.startswith("ngs_")
        }
        assert mapped == columns
        assert reverse_mapped == columns
    con.close()


def test_ngs_target_schema_is_complete_at_every_materialized_grain():
    """The source->map->target path must preserve the complete 18-column NGS set."""
    import duckdb

    root = Path(r"D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables")
    paths = {
        "weekly": root / "nfl_player_stats_all.parquet",
        "season": root / "season_career_v26/player_nfl_season.parquet",
        "season_all": root / "season_career_v26/player_nfl_season_all.parquet",
        "career": root / "season_career_v26/player_nfl_career.parquet",
        "career_all": root / "season_career_v26/player_nfl_career_all.parquet",
    }
    con = duckdb.connect()
    schemas = {}
    for grain, path in paths.items():
        columns = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path.as_posix()}')"
            ).fetchall()
        }
        schemas[grain] = {column for column in columns if column.startswith("ngs_")}

    source_columns = {
        row[0]
        for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet('D:/league-history-data/nfl/raw/nextgen_stats/ngs_weekly_2016_2025.parquet')"
        ).fetchall()
        if row[0].startswith("ngs_")
    }
    con.close()

    assert len(source_columns) == 18
    assert all(columns == source_columns for columns in schemas.values()), schemas


def test_ngs_player_ids_are_anchored_in_player_bio():
    import duckdb
    from scripts.sota_recon.build_advanced_season_career_v26 import (
        _validate_ngs_bio_identity,
    )

    result = _validate_ngs_bio_identity(duckdb.connect())
    assert result["passed"] is True
    assert result["source_ids"] == result["source_ids_in_bio"]
    assert result["bio_ids_with_ngs"] == result["source_ids"]
    assert result["bio_duplicate_ids"] == 0


def test_weekly_join_receipts_boundary_rows_without_hiding_in_scope_gaps(tmp_path, monkeypatch):
    import duckdb
    from scripts.sota_recon import build_advanced_layer2_v26 as layer2

    source = tmp_path / "ngs_weekly.parquet"
    target = tmp_path / "weekly.parquet"
    bio = tmp_path / "player_bio.parquet"
    _write_parquet(source, """
        SELECT 'p1' AS NFL_player_id, 2025 AS year, 1 AS week
        UNION ALL SELECT 'p1', 2025, 2
    """)
    _write_parquet(target, "SELECT 'p1' AS NFL_player_id, 2025 AS year, 1 AS week")
    _write_parquet(bio, "SELECT 'p1' AS NFL_player_id")
    monkeypatch.setattr(layer2, "NGS_SOURCE", source)
    monkeypatch.setattr(layer2, "PLAYER_BIO", bio)

    con = duckdb.connect()
    result = layer2._ngs_join_coverage(con, target.as_posix())
    con.close()

    assert result["source_rows"] == 2
    assert result["target_rows"] == 1
    assert result["source_native_boundary_rows"] == 1
    assert result["unexpected_unmatched_rows"] == 0
    assert result["source_ids_missing_bio"] == 0
    assert result["passed"] is True
