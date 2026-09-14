from __future__ import annotations

from dataclasses import asdict, dataclass, fields
from enum import Enum, StrEnum
from typing import Any, ClassVar


class SourceClass(StrEnum):
    CANONICAL = "canonical"
    PRIMARY = "primary"
    SECONDARY = "secondary"
    MIRROR = "mirror"
    DERIVED = "derived"
    IDENTITY = "identity"


class ObservationRole(StrEnum):
    AUTHORITATIVE = "authoritative"
    CORROBORATING = "corroborating"
    FALLBACK = "fallback"
    IDENTITY_ONLY = "identity_only"
    INADMISSIBLE = "inadmissible"


class Unit(StrEnum):
    COUNT = "count"
    YARDS = "yards"
    POINTS = "points"
    SECONDS = "seconds"
    PERCENT = "percent"
    DIMENSIONLESS = "dimensionless"
    BOOLEAN = "boolean"
    TEXT = "text"


class Entity(StrEnum):
    PLAYER = "player"
    TEAM = "team"
    GAME = "game"
    PLAY = "play"
    LEAGUE = "league"


class Grain(StrEnum):
    PLAY = "play"
    PLAYER_GAME = "player_game"
    PLAYER_WEEK = "player_week"
    TEAM_GAME = "team_game"
    PLAYER_SEASON = "player_season"
    TEAM_SEASON = "team_season"
    PLAYER_CAREER = "player_career"


class Domain(StrEnum):
    NONNEGATIVE = "nonnegative"
    INTEGER = "integer"
    REAL = "real"
    PROBABILITY = "probability"
    TEXT = "text"


class Aggregation(StrEnum):
    ADDITIVE = "additive"
    MAXIMUM = "maximum"
    RATIO = "ratio"
    SNAPSHOT = "snapshot"
    NON_AGGREGATABLE = "non_aggregatable"


class ProofNodeKind(StrEnum):
    LEAF = "leaf"
    TRANSFORM = "transform"
    CONSTRAINT = "constraint"
    CANDIDATE = "candidate"


class GatePlane(StrEnum):
    GLOBAL_RESEARCH_HEALTH = "global_research_health"
    SOURCE_HEALTH = "source_health"
    CANDIDATE_PROMOTION = "candidate_promotion"
    RELEASE = "release"


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


class StrictContract:
    _enum_fields: ClassVar[dict[str, type[Enum]]] = {}
    _tuple_fields: ClassVar[set[str]] = set()

    def to_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]):
        allowed = {field.name for field in fields(cls)}
        unknown = set(payload) - allowed
        if unknown:
            raise ValueError(f"unknown fields for {cls.__name__}: {sorted(unknown)}")
        values = dict(payload)
        for name, enum_type in cls._enum_fields.items():
            if name in values and values[name] is not None:
                values[name] = enum_type(values[name])
        for name in cls._tuple_fields:
            if name in values:
                values[name] = tuple(values[name])
        try:
            return cls(**values)
        except TypeError as exc:
            raise ValueError(f"invalid {cls.__name__}: {exc}") from exc


@dataclass(frozen=True)
class DatasetContract(StrictContract):
    contract_version: str
    dataset_id: str
    source_class: SourceClass
    physical_globs: tuple[str, ...] = ()
    year_start: int | None = None
    year_end: int | None = None
    required_years: tuple[int, ...] = ()
    year_field: str = "year"
    schema_variant_count: int = 1

    _enum_fields: ClassVar[dict[str, type[Enum]]] = {"source_class": SourceClass}
    _tuple_fields: ClassVar[set[str]] = {"physical_globs", "required_years"}


@dataclass(frozen=True)
class AtomContract(StrictContract):
    contract_version: str
    atom_id: str
    unit: Unit
    entity: Entity = Entity.PLAYER
    grain: Grain = Grain.PLAYER_GAME
    partition_keys: tuple[str, ...] = ()
    domain: Domain = Domain.NONNEGATIVE
    aggregation: Aggregation = Aggregation.ADDITIVE

    _enum_fields: ClassVar[dict[str, type[Enum]]] = {
        "unit": Unit,
        "entity": Entity,
        "grain": Grain,
        "domain": Domain,
        "aggregation": Aggregation,
    }
    _tuple_fields: ClassVar[set[str]] = {"partition_keys"}


@dataclass(frozen=True)
class AdmissibilityRule(StrictContract):
    contract_version: str
    rule_id: str
    atom_id: str
    dataset_id: str
    role: ObservationRole
    year_start: int
    year_end: int
    precedence: int
    grain: Grain | None = None
    competition: str | None = None
    season_type: str | None = None

    _enum_fields: ClassVar[dict[str, type[Enum]]] = {"role": ObservationRole, "grain": Grain}


@dataclass(frozen=True)
class ProofNode(StrictContract):
    contract_version: str
    node_id: str
    kind: ProofNodeKind
    atom_id: str
    dependencies: tuple[str, ...] = ()
    lineage_id: str | None = None
    artifact_fingerprint: str | None = None

    _enum_fields: ClassVar[dict[str, type[Enum]]] = {"kind": ProofNodeKind}
    _tuple_fields: ClassVar[set[str]] = {"dependencies"}


@dataclass(frozen=True)
class GateResult(StrictContract):
    contract_version: str
    gate_id: str
    plane: GatePlane
    status: str
    findings: tuple[dict[str, Any], ...] = ()

    _enum_fields: ClassVar[dict[str, type[Enum]]] = {"plane": GatePlane}
    _tuple_fields: ClassVar[set[str]] = {"findings"}

    def __post_init__(self) -> None:
        if self.status not in {"pass", "review", "fail"}:
            raise ValueError(f"unknown gate status: {self.status}")
