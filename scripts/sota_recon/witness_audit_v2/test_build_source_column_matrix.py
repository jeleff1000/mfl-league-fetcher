from pathlib import Path

from build_source_column_matrix import build_matrix


def test_matrix_excludes_newspapers_and_has_complete_keys(tmp_path):
    contract = tmp_path / "contracts.json"
    contract.write_text(
        '{"contracts": {"pfr:passing": {"path": "x.parquet", "n_rows": 4, "grain": "season", "atoms": {"pass_yds": {"col": "pass_yds", "era_min": 2000, "era_max": 2001, "total_nonzero": 3}}, "by_year": {"2000": {"pass_yds": 2}}, "meta_cols": []}, "newspaper_promoted:x": {"path": "x.duckdb", "n_rows": 1, "atoms": {"claim": {"col": "claim", "era_min": 1930, "era_max": 1930, "total_nonzero": 1}}}}}',
        encoding="utf-8",
    )
    aliases = tmp_path / "aliases.json"
    aliases.write_text('{"passing_yards": ["pass_yds"]}', encoding="utf-8")
    matrix = build_matrix(contract, aliases)
    assert matrix["summary"]["source_count"] == 1
    assert len(matrix["exclusions"]) == 1
    assert all(not row["source_id"].startswith("newspaper") for row in matrix["observations"])
    assert matrix["observations"][0]["canonical_column"] == "passing_yards"
    assert matrix["coverage"][0]["unavailable_reason"] == "contract_signal_counts_only"
