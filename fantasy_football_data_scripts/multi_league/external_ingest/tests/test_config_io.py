"""Tests for config_io: reading/writing external source config and alias mappings."""

import json
from pathlib import Path

from multi_league.external_ingest.config_io import (
    read_external_source_config,
    write_external_source_config,
    read_external_alias_mappings,
    write_external_alias_mappings,
    SourceMapping,
    IdentityMapping,
)


def test_round_trip_external_source_config(tmp_path: Path):
    path = tmp_path / "external_source_config.json"
    mappings = [
        SourceMapping(
            source_signature="abc123def",
            filename_glob="matchup_*.parquet",
            table="matchup",
            column_map={"year": "year", "week": "week", "manager": "owner"},
        )
    ]
    write_external_source_config(path, mappings)
    loaded = read_external_source_config(path)
    # Check all fields except decided_at (which is auto-populated on write)
    assert len(loaded) == 1
    assert loaded[0].source_signature == "abc123def"
    assert loaded[0].filename_glob == "matchup_*.parquet"
    assert loaded[0].table == "matchup"
    assert loaded[0].column_map == {"year": "year", "week": "week", "manager": "owner"}
    assert loaded[0].decided_by == "user"
    assert loaded[0].decided_at != ""  # populated on write


def test_read_missing_external_source_config_returns_empty(tmp_path: Path):
    assert read_external_source_config(tmp_path / "nope.json") == []


def test_round_trip_identity_mappings(tmp_path: Path):
    franchise_config_path = tmp_path / "franchise_config.json"
    franchise_config_path.write_text(
        json.dumps(
            {
                "version": "1.1",
                "franchises": [],
                "hidden_guid_merges": {},
                "guid_merges": {},
            }
        )
    )
    mappings = {
        "Ezra69420": IdentityMapping(franchise_id="g_1", source="manual_wizard"),
        "Donny T": IdentityMapping(franchise_id=None, new_franchise_seed={"manager_name": "Donny T"}),
        "TEST_USER": IdentityMapping(
            franchise_id=None,
            ignore=True,
            row_count_at_decision=47,
            tables_at_decision=["matchup"],
            years_at_decision=[2018],
        ),
    }
    write_external_alias_mappings(franchise_config_path, mappings)
    loaded = read_external_alias_mappings(franchise_config_path)
    assert loaded["Ezra69420"].franchise_id == "g_1"
    assert loaded["Donny T"].new_franchise_seed == {"manager_name": "Donny T"}
    assert loaded["TEST_USER"].ignore is True
    assert loaded["TEST_USER"].row_count_at_decision == 47


def test_franchise_config_v1_1_reads_with_empty_mappings(tmp_path: Path):
    path = tmp_path / "franchise_config.json"
    path.write_text(
        json.dumps(
            {
                "version": "1.1",
                "franchises": [],
                "hidden_guid_merges": {},
                "guid_merges": {},
            }
        )
    )
    assert read_external_alias_mappings(path) == {}


def test_write_franchise_config_bumps_to_v1_2(tmp_path: Path):
    path = tmp_path / "franchise_config.json"
    path.write_text(json.dumps({"version": "1.1", "franchises": []}))
    write_external_alias_mappings(path, {"X": IdentityMapping(franchise_id="g_1")})
    payload = json.loads(path.read_text())
    assert payload["version"] == "1.2"
    assert "external_alias_mappings" in payload
    # Existing fields preserved
    assert payload["franchises"] == []
