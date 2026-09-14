import json

from .build_external_witness_crosswalk import build_crosswalk
from . import build_external_witness_crosswalk as crosswalk_module


def test_external_workbook_crosswalk_maps_sheets(tmp_path, monkeypatch):
    xlsx = tmp_path / "columns.xlsx"
    monkeypatch.setattr(crosswalk_module, "load_workbook", lambda path: {"Sheet2": [{"Column": "passing_yards", "Family": "atom", "Status": "Sealed", "Witnesses": "pfr", "Witness era": "1932+", "Populated": "", "Gap": ""}], "Sheet1": [], "Sheet3": [], "Sheet4": []})
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"tables": {"weekly": {"passing_yards": {"verdict": "direct"}}}}), encoding="utf-8")
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"observations": [{"source_id": "pfr", "raw_column": "passing_yards", "canonical_column": "passing_yards"}], "coverage": [{"source_id": "pfr", "raw_column": "passing_yards", "year": 2020, "all_row_density": 1.0}]}), encoding="utf-8")
    local = tmp_path / "local.json"
    local.write_text(json.dumps({"rows": [{"row_type": "gate_column", "gate_table": "weekly", "gate_column": "passing_yards", "status": "sourced", "gap_type": None}]}), encoding="utf-8")
    result = build_crosswalk(xlsx, gate, source, local)
    row = next(r for r in result["rows"] if r["row_type"] == "workbook_column")
    assert row["gate_table"] == "weekly"
    assert row["source_status"] == "sourced"
    assert row["crosswalk_status"] == "fully_crossed"


def test_external_witness_ref_taxonomy_keeps_family_and_newspaper_distinct(tmp_path, monkeypatch):
    monkeypatch.setattr(crosswalk_module, "load_workbook", lambda path: {
        "Sheet1": [
            {"Column": "allpro", "Witnesses": "pfr_context:voting_pages", "Family": "honor"},
            {"Column": "height", "Witnesses": "newspaper_promoted:roster", "Family": "bio"},
        ],
        "Sheet2": [], "Sheet3": [], "Sheet4": [],
    })
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"tables": {"weekly": {}, "player_nfl_season": {}, "player_nfl_career": {}}}), encoding="utf-8")
    source = tmp_path / "source.json"
    source.write_text(json.dumps({
        "sources": [{"source_id": "pfr_context:voting_mvp"}, {"source_id": "pfr_context:voting_apmvp"}],
        "exclusions": [], "observations": [], "coverage": [],
    }), encoding="utf-8")
    local = tmp_path / "local.json"
    local.write_text(json.dumps({"rows": []}), encoding="utf-8")
    result = build_crosswalk(tmp_path / "columns.xlsx", gate, source, local)
    rows = {row["column"]: row for row in result["rows"] if row["row_type"] == "workbook_column"}
    assert rows["allpro"]["ambiguous_workbook_witness_refs"]
    assert not rows["allpro"]["unregistered_workbook_witness_refs"]
    assert rows["height"]["excluded_workbook_witness_refs"] == "newspaper_promoted:roster"
