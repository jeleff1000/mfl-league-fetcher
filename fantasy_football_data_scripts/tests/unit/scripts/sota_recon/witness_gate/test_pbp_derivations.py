from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import duckdb

from scripts.aggregate_merged_pbp_for_supertable_audit import STAT_COLUMNS, build_event_sql, create_bio_lookup
from scripts.sota_recon.witness_gate.compiler import TypedExpressionCompiler
from scripts.sota_recon.witness_gate.pbp_contracts import (
    build_pbp_contracts,
    load_pbp_contract_registry,
    validate_pbp_rollup_schema,
)


REGISTRY_PATH = Path("scripts/sota_recon/witness_gate/contracts/pbp_contracts.v1.json")
ROLLUP_PATH = Path(
    "D:/league-history-data/nfl/raw/stathead/generated/"
    "pbp_supertable_audit_1978_2025/pbp_player_week_rollup.parquet"
)


def test_pipeline_stat_surface_is_pinned_and_fully_typed() -> None:
    registry = load_pbp_contract_registry(REGISTRY_PATH)
    contracts = build_pbp_contracts(STAT_COLUMNS, registry)

    assert len(contracts) == 102
    assert set(contracts) == set(STAT_COLUMNS)
    assert all(contract.year_start >= 1978 for contract in contracts.values())
    assert all(contract.semantic_type.partition_keys == ("year", "week", "player_id", "season_type") for contract in contracts.values())


def test_event_builder_emits_every_contracted_stat() -> None:
    sql = build_event_sql()

    for stat in STAT_COLUMNS:
        assert stat in sql


def test_schema_validation_fails_for_any_missing_pbp_atom() -> None:
    registry = load_pbp_contract_registry(REGISTRY_PATH)
    observed = set(STAT_COLUMNS) - {"fumbles_lost", "passing_epa"}

    findings = validate_pbp_rollup_schema(observed, registry)

    assert {item.scope["field"] for item in findings} == {"fumbles_lost", "passing_epa"}
    assert all(item.severity == "fail" for item in findings)


def test_reflexive_pbp_formulas_compile_with_safe_division_and_typed_parts() -> None:
    registry = load_pbp_contract_registry(REGISTRY_PATH)
    contracts = build_pbp_contracts(STAT_COLUMNS, registry)
    compiler = TypedExpressionCompiler(
        atom_types={f"pbp.{name}": item.semantic_type for name, item in contracts.items()},
        column_bindings={f"pbp.{name}": name for name in contracts},
    )

    compiled = {
        atom_id: compiler.compile(expression)
        for atom_id, expression in registry.reflexive_atoms.items()
    }

    assert "NULLIF" in compiled["pbp.pass_success_rate"].sql
    assert compiled["pbp.total_epa"].atom_dependencies == (
        "pbp.passing_epa",
        "pbp.receiving_epa",
        "pbp.rushing_epa",
    )


def test_materialized_real_rollup_contains_the_full_pinned_surface() -> None:
    if not ROLLUP_PATH.exists():
        return
    registry = load_pbp_contract_registry(REGISTRY_PATH)
    observed = set(pq.ParquetFile(ROLLUP_PATH).schema_arrow.names)

    assert validate_pbp_rollup_schema(observed, registry) == ()


def test_manual_alias_cannot_override_distinct_direct_pfr_identity(tmp_path: Path) -> None:
    bio = tmp_path / "bio.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "NFL_player_id": "WIL374796",
                    "player": "John Williams",
                    "nfl_position": "RB",
                    "pfr_id": "WillJo27",
                    "first_year": 1985,
                    "last_year": 1998,
                },
                {
                    "NFL_player_id": "WillJo00",
                    "player": "John Williams",
                    "nfl_position": "FB",
                    "pfr_id": "WillJo00",
                    "first_year": 1986,
                    "last_year": 1995,
                },
            ]
        ),
        bio,
    )
    connection = duckdb.connect()
    try:
        create_bio_lookup(connection, bio)
        resolved = connection.execute(
            "SELECT NFL_player_id FROM bio_lookup WHERE lookup_id = 'pfr:WillJo00'"
        ).fetchone()[0]
    finally:
        connection.close()

    assert resolved == "WillJo00"
