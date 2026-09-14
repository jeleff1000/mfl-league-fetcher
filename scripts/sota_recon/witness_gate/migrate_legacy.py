from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .gate_planes import Finding
from .models import (
    Aggregation,
    AtomContract,
    Domain,
    Entity,
    Grain,
    ObservationRole,
    Unit,
)
from .semantic_types import MissingPolicy, SemanticType


@dataclass(frozen=True)
class MigratedFieldMapping:
    dataset_id: str
    source_field: str
    atom_id: str
    semantic_type: SemanticType
    year_start: int
    year_end: int
    role: ObservationRole
    precedence: int
    source_aggregation: str
    scale: float
    season_type: str | None
    filters: str
    legacy_origin: str


@dataclass(frozen=True)
class LegacyMigrationResult:
    field_mappings: tuple[MigratedFieldMapping, ...]
    atom_contracts: tuple[AtomContract, ...]
    findings: tuple[Finding, ...]


_COVERAGE_SOURCE_ALIASES = {
    "defense_advanced": "pfr_box_defense_advanced",
    "kicking": "pfr_box_kicking",
    "nfl_team_games_all": "pfr_team_games",
    "passing_advanced": "pfr_box_passing_advanced",
    "pbp": "pfr_box_pbp",
    "player_defense": "pfr_player_defense_box",
    "player_offense": "pfr_player_offense_box",
    "receiving_advanced": "pfr_box_receiving_advanced",
    "returns": "pfr_box_returns",
    "rushing_advanced": "pfr_box_rushing_advanced",
    "scoring": "pfr_box_scoring",
    "team_stats": "pfr_box_team_stats",
}


def _unit(stat: str) -> Unit:
    name = stat.casefold()
    if any(token in name for token in ("player_id", "team_id", "_name", "position")):
        return Unit.TEXT
    if "time_to_throw" in name or name.endswith("_seconds"):
        return Unit.SECONDS
    if "points" in name or name.endswith("_pts"):
        return Unit.POINTS
    if any(token in name for token in ("percent", "pct", "rate", "average", "_avg", "per_", "epa", "cpoe")):
        return Unit.DIMENSIONLESS
    if any(token in name for token in ("yard", "_yds", "_long")):
        return Unit.YARDS
    return Unit.COUNT


def _domain(unit: Unit) -> Domain:
    if unit is Unit.TEXT:
        return Domain.TEXT
    if unit is Unit.DIMENSIONLESS:
        return Domain.REAL
    return Domain.NONNEGATIVE


def _semantic_type(stat: str, *, grain: Grain, aggregation: Aggregation) -> SemanticType:
    entity = Entity.PLAYER if grain is Grain.PLAYER_SEASON else Entity.TEAM
    partitions = (
        ("year", "player_id", "season_type")
        if grain is Grain.PLAYER_SEASON
        else ("game_id", "team_id", "season_type")
    )
    unit = _unit(stat)
    if unit in {Unit.DIMENSIONLESS, Unit.SECONDS}:
        aggregation = Aggregation.RATIO
    return SemanticType(
        unit=unit,
        entity=entity,
        grain=grain,
        domain=_domain(unit),
        aggregation=aggregation,
        partition_keys=partitions,
        missing_policy=MissingPolicy.PRESERVE_NULL,
    )


def _atom_contract(atom_id: str, semantic: SemanticType) -> AtomContract:
    return AtomContract(
        contract_version="1",
        atom_id=atom_id,
        unit=semantic.unit,
        entity=semantic.entity,
        grain=semantic.grain,
        partition_keys=semantic.partition_keys,
        domain=semantic.domain,
        aggregation=semantic.aggregation,
    )


def _source_policy(source: Any) -> tuple[ObservationRole, int]:
    if source.witness_class in {"derived", "subject_history", "context"}:
        return ObservationRole.INADMISSIBLE, 100
    if source.witness_class == "identity":
        return ObservationRole.IDENTITY_ONLY, 50
    if source.role == "authority":
        return ObservationRole.AUTHORITATIVE, 10
    if source.role == "oracle":
        return ObservationRole.CORROBORATING, 20
    return ObservationRole.CORROBORATING, 30


def migrate_legacy_contracts(
    *,
    map_specs: Iterable[Any],
    coverage: dict[str, tuple[str, list[tuple[str, str, int, int]]]],
    source_registry: dict[str, Any],
) -> LegacyMigrationResult:
    mappings: list[MigratedFieldMapping] = []
    atoms: dict[str, AtomContract] = {}
    findings: list[Finding] = []

    for spec in map_specs:
        source = source_registry.get(spec.source_key)
        if source is None:
            findings.append(
                Finding(
                    code="LEGACY_SOURCE_UNREGISTERED",
                    severity="fail",
                    scope={"source_key": spec.source_key, "field": spec.source_col},
                    evidence_refs=(),
                    remediation="register the physical source in the finite census",
                )
            )
            continue
        if spec.agg not in {"sum", "max", "value"}:
            findings.append(
                Finding(
                    code="LEGACY_AGGREGATION_AMBIGUOUS",
                    severity="fail",
                    scope={"source_key": spec.source_key, "aggregation": spec.agg},
                    evidence_refs=(),
                    remediation="map the aggregation to a semantic contract",
                )
            )
            continue
        aggregation = {
            "sum": Aggregation.ADDITIVE,
            "max": Aggregation.MAXIMUM,
            "value": Aggregation.SNAPSHOT,
        }[spec.agg]
        atom_id = f"player_season.{spec.v26_col}"
        semantic = _semantic_type(spec.v26_col, grain=Grain.PLAYER_SEASON, aggregation=aggregation)
        role, precedence = _source_policy(source)
        mappings.append(
            MigratedFieldMapping(
                dataset_id=spec.source_key,
                source_field=spec.source_col,
                atom_id=atom_id,
                semantic_type=semantic,
                year_start=source.year_min,
                year_end=source.year_max,
                role=role,
                precedence=precedence,
                source_aggregation=spec.agg,
                scale=spec.scale,
                season_type=spec.season_type,
                filters=spec.filters,
                legacy_origin="witness_map",
            )
        )
        contract = _atom_contract(atom_id, semantic)
        if atom_id in atoms and atoms[atom_id] != contract:
            findings.append(
                Finding(
                    code="LEGACY_ATOM_TYPE_COLLISION",
                    severity="fail",
                    scope={"atom_id": atom_id, "source_key": spec.source_key},
                    evidence_refs=(),
                    remediation="split the atom or reconcile its semantic type",
                )
            )
        atoms.setdefault(atom_id, contract)

    for coverage_atom, (_, witnesses) in coverage.items():
        atom_id = f"team_game.{coverage_atom}"
        semantic = _semantic_type(
            coverage_atom,
            grain=Grain.TEAM_GAME,
            aggregation=Aggregation.ADDITIVE,
        )
        atoms.setdefault(atom_id, _atom_contract(atom_id, semantic))
        for table, method, year_start, year_end in witnesses:
            source_key = _COVERAGE_SOURCE_ALIASES.get(table)
            source = source_registry.get(source_key or "")
            if source is None:
                findings.append(
                    Finding(
                        code="LEGACY_COVERAGE_SOURCE_AMBIGUOUS",
                        severity="fail",
                        scope={"coverage_table": table, "atom_id": coverage_atom},
                        evidence_refs=(),
                        remediation="map the coverage table to a finite physical dataset",
                    )
                )
                continue
            role, precedence = _source_policy(source)
            mappings.append(
                MigratedFieldMapping(
                    dataset_id=source_key,
                    source_field=f"{method}:{coverage_atom}",
                    atom_id=atom_id,
                    semantic_type=semantic,
                    year_start=year_start,
                    year_end=year_end,
                    role=role,
                    precedence=precedence,
                    source_aggregation="sum",
                    scale=1.0,
                    season_type="REG",
                    filters="",
                    legacy_origin="witness_coverage",
                )
            )
    return LegacyMigrationResult(
        field_mappings=tuple(mappings),
        atom_contracts=tuple(atoms[key] for key in sorted(atoms)),
        findings=tuple(findings),
    )
