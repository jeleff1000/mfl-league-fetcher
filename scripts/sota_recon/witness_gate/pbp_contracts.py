from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .gate_planes import Finding
from .models import Aggregation, Domain, Entity, Grain, Unit
from .semantic_types import MissingPolicy, SemanticType


@dataclass(frozen=True)
class PbpContractRegistry:
    contract_version: str
    stat_columns_count: int
    stat_columns_fingerprint: str
    max_columns: frozenset[str]
    coverage_year_overrides: dict[str, int]
    reflexive_atoms: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class PbpStatContract:
    stat: str
    year_start: int
    year_end: int
    semantic_type: SemanticType
    proof_role: str


def _columns_fingerprint(stat_columns: list[str]) -> str:
    canonical = json.dumps(stat_columns, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def load_pbp_contract_registry(path: str | Path) -> PbpContractRegistry:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    allowed = {
        "contract_version",
        "stat_columns_count",
        "stat_columns_fingerprint",
        "max_columns",
        "coverage_year_overrides",
        "reflexive_atoms",
    }
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError(f"unknown PBP contract fields: {sorted(unknown)}")
    return PbpContractRegistry(
        contract_version=str(payload["contract_version"]),
        stat_columns_count=int(payload["stat_columns_count"]),
        stat_columns_fingerprint=str(payload["stat_columns_fingerprint"]),
        max_columns=frozenset(payload["max_columns"]),
        coverage_year_overrides={key: int(value) for key, value in payload["coverage_year_overrides"].items()},
        reflexive_atoms=dict(payload["reflexive_atoms"]),
    )


def _unit(stat: str) -> Unit:
    if stat.endswith("_n") or stat.endswith("_plays"):
        return Unit.COUNT
    if stat.endswith("_epa"):
        return Unit.POINTS
    if stat.endswith("_wpa") or "cpoe" in stat:
        return Unit.DIMENSIONLESS
    if any(token in stat for token in ("yards", "_long", "air_yards")):
        return Unit.YARDS
    return Unit.COUNT


def build_pbp_contracts(
    stat_columns: list[str], registry: PbpContractRegistry
) -> dict[str, PbpStatContract]:
    fingerprint = _columns_fingerprint(stat_columns)
    if len(stat_columns) != registry.stat_columns_count or fingerprint != registry.stat_columns_fingerprint:
        raise ValueError(
            "PBP stat surface changed without a contract version bump: "
            f"count={len(stat_columns)} fingerprint={fingerprint}"
        )
    contracts: dict[str, PbpStatContract] = {}
    for stat in stat_columns:
        unit = _unit(stat)
        aggregation = Aggregation.MAXIMUM if stat in registry.max_columns else Aggregation.ADDITIVE
        contracts[stat] = PbpStatContract(
            stat=stat,
            year_start=registry.coverage_year_overrides.get(stat, 1978),
            year_end=2025,
            semantic_type=SemanticType(
                unit=unit,
                entity=Entity.PLAYER,
                grain=Grain.PLAYER_WEEK,
                domain=Domain.REAL if unit in {Unit.POINTS, Unit.DIMENSIONLESS} else Domain.NONNEGATIVE,
                aggregation=aggregation,
                partition_keys=("year", "week", "player_id", "season_type"),
                missing_policy=MissingPolicy.PRESERVE_NULL,
            ),
            proof_role="pbp_derived_leaf",
        )
    return contracts


def validate_pbp_rollup_schema(
    observed_fields: set[str], registry: PbpContractRegistry
) -> tuple[Finding, ...]:
    from scripts.aggregate_merged_pbp_for_supertable_audit import STAT_COLUMNS

    contracts = build_pbp_contracts(STAT_COLUMNS, registry)
    return tuple(
        Finding(
            code="PBP_DERIVED_ATOM_MISSING",
            severity="fail",
            scope={"dataset_id": "pbp_player_week_rollup", "field": stat},
            evidence_refs=(registry.stat_columns_fingerprint,),
            remediation="rebuild the PBP rollup with the pinned aggregation pipeline",
        )
        for stat in sorted(set(contracts) - observed_fields)
    )
