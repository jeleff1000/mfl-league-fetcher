from __future__ import annotations

import inspect

from scripts.sota_recon import sources, witness_coverage, witness_map
from scripts.sota_recon.witness_gate.migrate_legacy import migrate_legacy_contracts


def test_every_legacy_map_and_coverage_entry_gets_a_typed_contract() -> None:
    result = migrate_legacy_contracts(
        map_specs=witness_map.WITNESS_MAP,
        coverage=witness_coverage.COVERAGE,
        source_registry=sources.registry(include_subject=False),
    )

    assert result.findings == ()
    assert len(result.field_mappings) == len(witness_map.WITNESS_MAP) + sum(
        len(witnesses) for _, witnesses in witness_coverage.COVERAGE.values()
    )
    assert len(result.atom_contracts) == len({item.atom_id for item in result.field_mappings})
    assert all(item.semantic_type.partition_keys for item in result.field_mappings)
    assert all(item.year_start <= item.year_end for item in result.field_mappings)


def test_legacy_source_fields_and_years_are_not_dropped() -> None:
    result = migrate_legacy_contracts(
        map_specs=witness_map.WITNESS_MAP,
        coverage=witness_coverage.COVERAGE,
        source_registry=sources.registry(include_subject=False),
    )
    migrated_map_keys = {
        (item.dataset_id, item.source_field, item.year_start, item.year_end)
        for item in result.field_mappings
        if item.legacy_origin == "witness_map"
    }

    for spec in witness_map.WITNESS_MAP:
        source = sources.registry(include_subject=False)[spec.source_key]
        assert (spec.source_key, spec.source_col, source.year_min, source.year_max) in migrated_map_keys


def test_coverage_is_compatibility_input_not_a_voting_policy() -> None:
    source = inspect.getsource(witness_coverage)

    assert "majority can settle" not in source
    assert witness_coverage.DEPRECATED_FOR_PROMOTION is True
    assert witness_map.DEPRECATED_FOR_PROMOTION is True
