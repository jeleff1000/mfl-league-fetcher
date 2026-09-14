from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from scripts.sota_recon.witness_gate.field_registry import audit_field_closure, load_field_registry
from scripts.sota_recon.witness_gate.census import load_census


CONTRACT = Path("scripts/sota_recon/witness_gate/contracts/field_mappings.v1.json")
LAKE = Path("D:/league-history-data/nfl")


def observed_fields(path: Path) -> set[str]:
    return {
        field
        for parquet_path in path.rglob("*.parquet")
        for field in pq.ParquetFile(parquet_path).schema_arrow.names
    }


def test_field_closure_fails_for_added_or_missing_field() -> None:
    registry = load_field_registry(CONTRACT)
    contract = registry.datasets["lake.nflcom.player_logs"]

    added = audit_field_closure(contract, set(contract.reviewed_fields) | {"mystery"})
    missing = audit_field_closure(contract, set(contract.reviewed_fields) - {"season"})

    assert any(item.code == "UNREGISTERED_FIELD" and item.scope["field"] == "mystery" for item in added.findings)
    assert any(item.code == "CONTRACTED_FIELD_MISSING" and item.scope["field"] == "season" for item in missing.findings)


def test_every_reviewed_field_gets_a_stable_raw_atom() -> None:
    registry = load_field_registry(CONTRACT)

    for contract in registry.datasets.values():
        result = audit_field_closure(contract, set(contract.reviewed_fields))
        assert result.findings == ()
        assert len(result.raw_atom_ids) == len(contract.reviewed_fields) - len(contract.excluded_fields)
        assert len(result.raw_atom_ids) == len(set(result.raw_atom_ids.values()))


def test_every_local_lake_census_dataset_has_a_field_contract() -> None:
    registry = load_field_registry(CONTRACT)
    census = load_census("scripts/sota_recon/witness_gate/contracts/source_census.v1.json")

    local_dataset_ids = {
        item.dataset_id for item in census.datasets if item.producer.kind == "local_lake"
    }

    assert local_dataset_ids == set(registry.datasets)


def test_real_nflcom_and_external_intake_schemas_are_closed() -> None:
    if not LAKE.exists():
        return
    registry = load_field_registry(CONTRACT)
    physical_paths = {
        "lake.nflcom.player_career": LAKE / "raw/nflcom/tables/player_career",
        "lake.nflcom.player_logs": LAKE / "raw/nflcom/tables/player_logs",
        "lake.nflcom.player_logs_targeted": LAKE / "raw/nflcom/tables/player_logs_targeted",
        "lake.nflcom.player_season": LAKE / "raw/nflcom/tables/player_season",
        "lake.nflcom.player_situational": LAKE / "raw/nflcom/tables/player_situational",
        "lake.nflcom.player_splits": LAKE / "raw/nflcom/tables/player_splits",
        "lake.nflcom.team_stats": LAKE / "raw/nflcom/tables/team_stats",
    }
    for dataset_id, path in physical_paths.items():
        result = audit_field_closure(registry.datasets[dataset_id], observed_fields(path))
        assert result.findings == (), dataset_id

    external = LAKE / "derived/external_witness_intake/EXTERNAL_WITNESS_INDEX.parquet"
    fields = set(pq.ParquetFile(external).schema_arrow.names)
    result = audit_field_closure(registry.datasets["lake.external_witness_intake"], fields)
    assert result.findings == ()
