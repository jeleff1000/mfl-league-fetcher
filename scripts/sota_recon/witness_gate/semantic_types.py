from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from .models import Aggregation, Domain, Entity, Grain, Unit


class MissingPolicy(StrEnum):
    PRESERVE_NULL = "preserve_null"
    OBSERVED_ZERO = "observed_zero"
    INAPPLICABLE = "inapplicable"


@dataclass(frozen=True)
class SemanticType:
    unit: Unit
    entity: Entity
    grain: Grain
    domain: Domain
    aggregation: Aggregation
    partition_keys: tuple[str, ...]
    missing_policy: MissingPolicy

    def __post_init__(self) -> None:
        if self.missing_policy is None:
            raise ValueError("an explicit missing policy is required")
        if not self.partition_keys:
            raise ValueError("partition_keys cannot be empty")
        if len(self.partition_keys) != len(set(self.partition_keys)):
            raise ValueError("partition_keys cannot contain duplicates")


def validate_partition(required: tuple[str, ...], available: tuple[str, ...]) -> None:
    missing = set(required) - set(available)
    if missing:
        raise ValueError(f"missing partition keys: {sorted(missing)}")


def _require_same_frame(left: SemanticType, right: SemanticType) -> None:
    if (left.entity, left.grain) != (right.entity, right.grain):
        raise ValueError(
            "entity/grain mismatch: "
            f"{left.entity.value}/{left.grain.value} != {right.entity.value}/{right.grain.value}"
        )
    if set(left.partition_keys) != set(right.partition_keys):
        raise ValueError(
            f"partition mismatch: {sorted(left.partition_keys)} != {sorted(right.partition_keys)}"
        )


def binary_result_type(operator: str, left: SemanticType, right: SemanticType) -> SemanticType:
    _require_same_frame(left, right)
    if operator in {"add", "subtract", "lte", "gte", "eq"} and left.unit is not right.unit:
        raise ValueError(f"unit mismatch: {left.unit.value} != {right.unit.value}")
    if operator in {"add", "subtract"}:
        return left
    if operator in {"lte", "gte", "eq"}:
        return replace(
            left,
            unit=Unit.BOOLEAN,
            domain=Domain.INTEGER,
            aggregation=Aggregation.NON_AGGREGATABLE,
        )
    if operator == "divide":
        if left.unit is right.unit:
            result_unit = Unit.DIMENSIONLESS
        elif right.unit is Unit.COUNT:
            result_unit = left.unit
        else:
            raise ValueError(f"unit mismatch for ratio: {left.unit.value} != {right.unit.value}")
        return replace(
            left,
            unit=result_unit,
            domain=Domain.REAL,
            aggregation=Aggregation.RATIO,
        )
    raise ValueError(f"unsupported semantic operator: {operator}")


_VALID_GRAIN_ROLLUPS = {
    (Grain.PLAY, Grain.PLAYER_GAME),
    (Grain.PLAY, Grain.TEAM_GAME),
    (Grain.PLAYER_WEEK, Grain.PLAYER_SEASON),
    (Grain.PLAYER_GAME, Grain.PLAYER_SEASON),
    (Grain.TEAM_GAME, Grain.TEAM_SEASON),
    (Grain.PLAYER_SEASON, Grain.PLAYER_CAREER),
}

_ENTITY_KEYS = {
    Entity.PLAYER: "player_id",
    Entity.TEAM: "team_id",
    Entity.GAME: "game_id",
    Entity.PLAY: "play_id",
    Entity.LEAGUE: "league_id",
}


def aggregate_type(
    source: SemanticType,
    *,
    target_grain: Grain,
    target_partitions: tuple[str, ...],
) -> SemanticType:
    if source.aggregation is not Aggregation.ADDITIVE:
        raise ValueError(f"atom is not additive: {source.aggregation.value}")
    if (source.grain, target_grain) not in _VALID_GRAIN_ROLLUPS:
        raise ValueError(f"invalid grain rollup: {source.grain.value} -> {target_grain.value}")
    entity_key = _ENTITY_KEYS[source.entity]
    validate_partition((entity_key,), target_partitions)
    return replace(source, grain=target_grain, partition_keys=target_partitions)
