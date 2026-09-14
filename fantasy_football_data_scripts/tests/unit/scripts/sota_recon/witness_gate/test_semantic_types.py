from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.models import Aggregation, Domain, Entity, Grain, Unit
from scripts.sota_recon.witness_gate.semantic_types import (
    MissingPolicy,
    SemanticType,
    aggregate_type,
    binary_result_type,
    validate_partition,
)


def stat_type(
    unit: Unit = Unit.COUNT,
    entity: Entity = Entity.PLAYER,
    grain: Grain = Grain.PLAYER_GAME,
    partitions: tuple[str, ...] = ("game_id", "player_id"),
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


def test_addition_requires_matching_units() -> None:
    with pytest.raises(ValueError, match="unit mismatch"):
        binary_result_type("add", stat_type(Unit.YARDS), stat_type(Unit.COUNT))


def test_comparison_requires_same_entity_and_grain() -> None:
    player_season = stat_type(grain=Grain.PLAYER_SEASON, partitions=("year", "player_id"))
    team_game = stat_type(entity=Entity.TEAM, grain=Grain.TEAM_GAME, partitions=("game_id", "team_id"))

    with pytest.raises(ValueError, match="entity/grain mismatch"):
        binary_result_type("lte", player_season, team_game)


def test_partition_validation_fails_when_competition_or_season_type_is_lost() -> None:
    required = ("year", "team_id", "competition", "season_type")

    with pytest.raises(ValueError, match="missing partition keys"):
        validate_partition(required, ("year", "team_id", "season_type"))


def test_same_unit_addition_and_ratio_are_typed() -> None:
    attempts = stat_type()
    completions = stat_type()

    summed = binary_result_type("add", attempts, completions)
    ratio = binary_result_type("divide", completions, attempts)

    assert summed.unit is Unit.COUNT
    assert ratio.unit is Unit.DIMENSIONLESS
    assert ratio.domain is Domain.REAL

    yards_per_attempt = binary_result_type("divide", stat_type(Unit.YARDS), attempts)
    assert yards_per_attempt.unit is Unit.YARDS


def test_grain_change_requires_explicit_additive_aggregation() -> None:
    player_game = stat_type()
    player_season = aggregate_type(
        player_game,
        target_grain=Grain.PLAYER_SEASON,
        target_partitions=("year", "player_id", "season_type"),
    )

    assert player_season.grain is Grain.PLAYER_SEASON
    assert player_season.partition_keys == ("year", "player_id", "season_type")

    with pytest.raises(ValueError, match="not additive"):
        aggregate_type(
            stat_type(aggregation=Aggregation.SNAPSHOT),
            target_grain=Grain.PLAYER_SEASON,
            target_partitions=("year", "player_id"),
        )


def test_missing_values_cannot_be_implicitly_converted_to_zero() -> None:
    with pytest.raises(ValueError, match="explicit missing policy"):
        SemanticType(
            unit=Unit.COUNT,
            entity=Entity.PLAYER,
            grain=Grain.PLAYER_GAME,
            domain=Domain.NONNEGATIVE,
            aggregation=Aggregation.ADDITIVE,
            partition_keys=("game_id", "player_id"),
            missing_policy=None,
        )
