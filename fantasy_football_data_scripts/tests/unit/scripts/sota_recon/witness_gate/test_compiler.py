from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.compiler import TypedExpressionCompiler, validate_derived_acyclic
from scripts.sota_recon.witness_gate.models import Aggregation, Domain, Entity, Grain, Unit
from scripts.sota_recon.witness_gate.semantic_types import MissingPolicy, SemanticType


def typed(
    unit: Unit = Unit.COUNT,
    entity: Entity = Entity.PLAYER,
    grain: Grain = Grain.PLAYER_GAME,
    partitions: tuple[str, ...] = ("game_id", "player_id", "season_type"),
    aggregation: Aggregation = Aggregation.ADDITIVE,
) -> SemanticType:
    return SemanticType(
        unit=unit,
        entity=entity,
        grain=grain,
        domain=Domain.NONNEGATIVE,
        aggregation=aggregation,
        partition_keys=partitions,
        missing_policy=MissingPolicy.PRESERVE_NULL,
    )


def compiler() -> TypedExpressionCompiler:
    return TypedExpressionCompiler(
        atom_types={
            "passing.attempts": typed(),
            "passing.completions": typed(),
            "passing.yards": typed(Unit.YARDS),
            "team.passing_attempts": typed(
                entity=Entity.TEAM,
                grain=Grain.TEAM_GAME,
                partitions=("game_id", "team_id", "season_type"),
            ),
            "identity.jersey_number": typed(aggregation=Aggregation.SNAPSHOT),
        },
        column_bindings={
            "passing.attempts": "pass_attempts",
            "passing.completions": "completions",
            "passing.yards": "passing yards",
            "team.passing_attempts": "team_attempts",
            "identity.jersey_number": "jersey_number",
        },
    )


def atom(atom_id: str) -> dict[str, str]:
    return {"op": "atom", "atom_id": atom_id}


def test_unknown_atom_and_unit_mismatch_fail_before_sql() -> None:
    with pytest.raises(ValueError, match="unknown atom"):
        compiler().compile(atom("passing.mystery"))

    with pytest.raises(ValueError, match="unit mismatch"):
        compiler().compile(
            {"op": "add", "left": atom("passing.attempts"), "right": atom("passing.yards")}
        )


def test_unsafe_division_and_partition_mismatch_are_rejected() -> None:
    with pytest.raises(ValueError, match="unsafe division"):
        compiler().compile(
            {"op": "divide", "left": atom("passing.completions"), "right": atom("passing.attempts")}
        )

    with pytest.raises(ValueError, match="entity/grain mismatch"):
        compiler().compile(
            {"op": "lte", "left": atom("passing.attempts"), "right": atom("team.passing_attempts")}
        )


def test_illegal_aggregation_is_rejected() -> None:
    with pytest.raises(ValueError, match="not additive"):
        compiler().compile(
            {
                "op": "aggregate",
                "arg": atom("identity.jersey_number"),
                "target_grain": "player_season",
                "target_partitions": ["year", "player_id", "season_type"],
            }
        )


def test_typed_constraint_compiles_quoted_duckdb_sql_and_proof_metadata() -> None:
    compiled = compiler().compile(
        {"op": "lte", "left": atom("passing.completions"), "right": atom("passing.attempts")},
        law_id="completions_lte_attempts",
    )

    assert compiled.sql == '("completions" <= "pass_attempts")'
    assert compiled.semantic_type.unit is Unit.BOOLEAN
    assert compiled.atom_dependencies == ("passing.attempts", "passing.completions")
    assert compiled.proof_node.atom_id == "law:completions_lte_attempts"


def test_safe_division_has_explicit_zero_policy() -> None:
    compiled = compiler().compile(
        {
            "op": "safe_divide",
            "left": atom("passing.completions"),
            "right": atom("passing.attempts"),
            "zero_policy": "null",
        }
    )

    assert "NULLIF" in compiled.sql
    assert compiled.semantic_type.unit is Unit.DIMENSIONLESS


def test_derived_atom_contracts_must_be_acyclic() -> None:
    contracts = {
        "derived.a": atom("derived.b"),
        "derived.b": {"op": "add", "left": atom("derived.a"), "right": atom("passing.attempts")},
    }

    with pytest.raises(ValueError, match=r"derived atom cycle.*derived.a.*derived.b"):
        validate_derived_acyclic(contracts)
