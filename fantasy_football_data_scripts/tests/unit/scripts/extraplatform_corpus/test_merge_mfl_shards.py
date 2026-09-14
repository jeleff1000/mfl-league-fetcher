import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[5] / "scripts" / "extraplatform_corpus" / "merge_mfl_shards.py"
SPEC = importlib.util.spec_from_file_location("merge_mfl_shards", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_merge_deduplicates_season_leagues_and_keeps_draft_evidence():
    payloads = [
        {"records": [{"year": 2003, "id": "77", "draft_status": "none_or_restricted", "lineage_seed": "2024:123"}]},
        {"records": [{"year": 2003, "id": "77", "draft_status": "available", "lineage_seed": "2024:999"}]},
    ]
    result = MODULE.merge_payloads(payloads, [])
    assert result["unique_season_leagues"] == 1
    assert result["records"][0]["draft_status"] == "available"
    assert result["records"][0]["lineage_seeds"] == ["2024:123", "2024:999"]
    assert len(result["leagues"]) == 2
    assert {row["seed"] for row in result["leagues"]} == {"2024:123", "2024:999"}


def test_merge_accepts_keyword_directory_seed_records():
    result = MODULE.merge_payloads(
        [{"leagues": [{"year": 2014, "id": "55", "seed": "2014:55", "name": "Dynasty"}]}],
        [],
    )
    assert result["unique_season_leagues"] == 1
    assert result["records"][0]["lineage_seeds"] == ["2014:55"]
    assert result["leagues"][0]["seed"] == "2014:55"


def test_merge_links_independent_seeds_by_name_and_roster_evidence():
    payload = {"records": [
        {"year": 2024, "id": "1", "lineage_seed": "2024:1", "name_key": "old friends dynasty",
         "roster_names": ["a", "b", "c", "d"], "roster_fingerprint": "same"},
        {"year": 2023, "id": "2", "lineage_seed": "2023:2", "name_key": "old friends dynasty",
         "roster_names": ["a", "b", "c", "d"], "roster_fingerprint": "same"},
    ]}
    result = MODULE.merge_payloads([payload], [])
    assert result["lineage_count"] == 1
    evidence = [edge["evidence"] for edges in result["lineage_link_evidence"].values() for edge in edges]
    assert ["league_name_exact", "roster_fingerprint_exact", "roster_overlap_ge_0.8"] in evidence
