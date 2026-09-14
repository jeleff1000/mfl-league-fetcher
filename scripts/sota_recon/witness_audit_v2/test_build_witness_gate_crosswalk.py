import json

from .build_witness_gate_crosswalk import build_crosswalk


def test_crosswalk_reports_gate_and_source_side_gaps(tmp_path):
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"tables": {"weekly": {"passing_yards": {"verdict": "direct", "family": "atom"}, "missing_stat": {"verdict": "derived_witnessed", "inputs": ["missing_input"]}}}}), encoding="utf-8")
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"observations": [{"source_id": "pfr", "raw_column": "pass_yds", "canonical_column": "passing_yards"}, {"source_id": "pfr", "raw_column": "targets", "canonical_column": "targets"}], "coverage": [{"source_id": "pfr", "raw_column": "pass_yds", "year": 2020, "all_row_density": 1.0}, {"source_id": "pfr", "raw_column": "targets", "year": 2020, "all_row_density": 1.0}]}), encoding="utf-8")
    result = build_crosswalk(gate, source)
    statuses = {(row["gate_column"], row["status"]) for row in result["rows"]}
    assert ("passing_yards", "sourced") in statuses
    assert ("missing_stat", "derivation_input_gap") in statuses
    assert result["summary"]["source_not_in_gate_count"] == 1


def test_crosswalk_labels_interior_year_holes(tmp_path):
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"tables": {"weekly": {"stat": {"verdict": "direct"}}}}), encoding="utf-8")
    source = tmp_path / "source.json"
    source.write_text(json.dumps({
        "sources": [{"source_id": "pfr", "atoms": {"stat": {"col": "stat", "era_min": 2020, "era_max": 2022}}}],
        "observations": [{"source_id": "pfr", "raw_column": "stat", "canonical_column": "stat"}],
        "coverage": [{"source_id": "pfr", "raw_column": "stat", "year": 2020, "all_row_density": 1.0},
                     {"source_id": "pfr", "raw_column": "stat", "year": 2022, "all_row_density": 1.0}],
    }), encoding="utf-8")
    result = build_crosswalk(gate, source)
    row = next(r for r in result["rows"] if r["row_type"] == "gate_column")
    assert row["status"] == "partial_year_coverage"
    assert row["missing_year_shape"] == "interior"
