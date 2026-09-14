#!/usr/bin/env python3
"""Draft intelligence state registry.

The miner is allowed to discover many possible feature affinities. This module
is the deterministic vocabulary that decides how a discovered feature maps into
state-machine language and how much prior evidence it needs before promotion.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

MODEL_VERSION = "draft-state-registry-v0.1"


@dataclass(frozen=True)
class DraftStateDefinition:
    feature_type: str
    state_family: str
    prior_weight: float
    ready_abs_z: float
    watch_abs_z: float
    ready_years: int
    watch_years: int
    ready_picks: int
    watch_picks: int
    ready_repeatability: float
    watch_repeatability: float
    description: str


_ENTITY_DEFINITION = DraftStateDefinition(
    feature_type="entity",
    state_family="entity_affinity",
    prior_weight=90.0,
    ready_abs_z=4.0,
    watch_abs_z=2.5,
    ready_years=3,
    watch_years=2,
    ready_picks=8,
    watch_picks=5,
    ready_repeatability=0.75,
    watch_repeatability=0.60,
    description="Specific team, college, or conference affinity discovered from draft behavior.",
)

_DEFINITIONS: dict[str, DraftStateDefinition] = {
    "current_nfl_team": _ENTITY_DEFINITION,
    "nfl_draft_team": _ENTITY_DEFINITION,
    "college": _ENTITY_DEFINITION,
    "conference": _ENTITY_DEFINITION,
    "archetype": DraftStateDefinition(
        feature_type="archetype",
        state_family="player_archetype",
        prior_weight=35.0,
        ready_abs_z=3.0,
        watch_abs_z=2.0,
        ready_years=3,
        watch_years=2,
        ready_picks=6,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Player archetype affinity discovered from bio and prior production.",
    ),
    "draft_state": DraftStateDefinition(
        feature_type="draft_state",
        state_family="board_state",
        prior_weight=30.0,
        ready_abs_z=3.0,
        watch_abs_z=2.0,
        ready_years=3,
        watch_years=2,
        ready_picks=8,
        watch_picks=5,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Board-context behavior such as phase and position-run response.",
    ),
    "position_age": DraftStateDefinition(
        feature_type="position_age",
        state_family="position_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Position-specific age tendency.",
    ),
    "position_experience": DraftStateDefinition(
        feature_type="position_experience",
        state_family="position_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Position-specific experience tendency.",
    ),
    "age_bucket": DraftStateDefinition(
        feature_type="age_bucket",
        state_family="player_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Age-profile tendency weighted by draft capital.",
    ),
    "experience_bucket": DraftStateDefinition(
        feature_type="experience_bucket",
        state_family="player_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Experience-profile tendency weighted by draft capital.",
    ),
    "nfl_draft_capital": DraftStateDefinition(
        feature_type="nfl_draft_capital",
        state_family="player_profile",
        prior_weight=50.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="NFL draft-pedigree tendency.",
    ),
    "production_profile": DraftStateDefinition(
        feature_type="production_profile",
        state_family="prior_production_profile",
        prior_weight=42.0,
        ready_abs_z=3.0,
        watch_abs_z=2.0,
        ready_years=3,
        watch_years=2,
        ready_picks=6,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Draft-time preference tied to prior NFL production shape.",
    ),
    "award_history": DraftStateDefinition(
        feature_type="award_history",
        state_family="award_profile",
        prior_weight=52.0,
        ready_abs_z=3.15,
        watch_abs_z=2.05,
        ready_years=3,
        watch_years=2,
        ready_picks=6,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Draft-safe preference for previous-season or cumulative NFL award history.",
    ),
    "bio_profile": DraftStateDefinition(
        feature_type="bio_profile",
        state_family="player_profile",
        prior_weight=48.0,
        ready_abs_z=3.0,
        watch_abs_z=2.0,
        ready_years=3,
        watch_years=2,
        ready_picks=6,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Draft-safe player reputation profile derived from prior bio and award history.",
    ),
    "ras_bucket": DraftStateDefinition(
        feature_type="ras_bucket",
        state_family="athletic_profile",
        prior_weight=50.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Athletic-profile tendency.",
    ),
    "height_bucket": DraftStateDefinition(
        feature_type="height_bucket",
        state_family="athletic_profile",
        prior_weight=55.0,
        ready_abs_z=3.5,
        watch_abs_z=2.25,
        ready_years=3,
        watch_years=2,
        ready_picks=8,
        watch_picks=5,
        ready_repeatability=0.70,
        watch_repeatability=0.55,
        description="Height-profile tendency.",
    ),
    "weight_bucket": DraftStateDefinition(
        feature_type="weight_bucket",
        state_family="athletic_profile",
        prior_weight=55.0,
        ready_abs_z=3.5,
        watch_abs_z=2.25,
        ready_years=3,
        watch_years=2,
        ready_picks=8,
        watch_picks=5,
        ready_repeatability=0.70,
        watch_repeatability=0.55,
        description="Weight-profile tendency.",
    ),
    "prior_league_production": DraftStateDefinition(
        feature_type="prior_league_production",
        state_family="prior_league_production",
        prior_weight=40.0,
        ready_abs_z=3.0,
        watch_abs_z=2.0,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Draft-time preference or edge tied to prior league production and usage.",
    ),
    "draft_board_rank": DraftStateDefinition(
        feature_type="draft_board_rank",
        state_family="board_value_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=7,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Draft-board percentile or positional rank tendency.",
    ),
    "manager_history_context": DraftStateDefinition(
        feature_type="manager_history_context",
        state_family="manager_history_context",
        prior_weight=90.0,
        ready_abs_z=4.0,
        watch_abs_z=2.5,
        ready_years=4,
        watch_years=2,
        ready_picks=12,
        watch_picks=6,
        ready_repeatability=0.75,
        watch_repeatability=0.60,
        description="Manager-history context; useful for league inefficiency discovery, not direct player affinity.",
    ),
    "league_format_context": DraftStateDefinition(
        feature_type="league_format_context",
        state_family="league_format_context",
        prior_weight=110.0,
        ready_abs_z=4.25,
        watch_abs_z=2.75,
        ready_years=4,
        watch_years=2,
        ready_picks=16,
        watch_picks=8,
        ready_repeatability=0.75,
        watch_repeatability=0.60,
        description="League-format context that needs stronger evidence before promotion.",
    ),
    "draft_profile": DraftStateDefinition(
        feature_type="draft_profile",
        state_family="draft_profile",
        prior_weight=45.0,
        ready_abs_z=3.25,
        watch_abs_z=2.1,
        ready_years=3,
        watch_years=2,
        ready_picks=8,
        watch_picks=4,
        ready_repeatability=0.65,
        watch_repeatability=0.55,
        description="Actionable player-profile residual discovered from point-in-time draft features.",
    ),
    "position_market": DraftStateDefinition(
        feature_type="position_market",
        state_family="position_market",
        prior_weight=55.0,
        ready_abs_z=3.5,
        watch_abs_z=2.25,
        ready_years=3,
        watch_years=2,
        ready_picks=10,
        watch_picks=5,
        ready_repeatability=0.70,
        watch_repeatability=0.60,
        description="Position and capital-tier residual market signal.",
    ),
}

_DEFAULT_DEFINITION = DraftStateDefinition(
    feature_type="unknown",
    state_family="discovered_feature",
    prior_weight=70.0,
    ready_abs_z=3.75,
    watch_abs_z=2.5,
    ready_years=3,
    watch_years=2,
    ready_picks=8,
    watch_picks=5,
    ready_repeatability=0.70,
    watch_repeatability=0.60,
    description="Unregistered feature discovered by the miner.",
)

_DEFINITIONS["player_archetype"] = _DEFINITIONS["archetype"]
_DEFINITIONS["qb_mobility_profile"] = _DEFINITIONS["production_profile"]
_DEFINITIONS["rb_receiving_profile"] = _DEFINITIONS["production_profile"]
_DEFINITIONS["rb_workload_profile"] = _DEFINITIONS["production_profile"]
_DEFINITIONS["receiving_volume_profile"] = _DEFINITIONS["production_profile"]
_DEFINITIONS["availability_profile"] = _DEFINITIONS["production_profile"]
_DEFINITIONS["position_height"] = _DEFINITIONS["height_bucket"]
_DEFINITIONS["position_weight"] = _DEFINITIONS["weight_bucket"]
_DEFINITIONS["position_ras"] = _DEFINITIONS["ras_bucket"]
_DEFINITIONS["capital_bucket"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["capital_tier"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_capital_bucket"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_capital_tier"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_draft_phase"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["format_position"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["format_capital_tier"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_slot"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_slot_tier"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_slot_capital_tier"] = _DEFINITIONS["draft_state"]
_DEFINITIONS["position_slot_phase"] = _DEFINITIONS["draft_state"]


def get_state_definition(feature_type: str) -> DraftStateDefinition:
    """Return promotion thresholds for a mined feature type."""
    raw = str(feature_type or "")
    if raw in _DEFINITIONS:
        return _DEFINITIONS[raw]

    normalized = raw.lower()
    if normalized in {
        "draft.nfl_team",
        "draft.nfl_team_api",
        "bio.nfl_draft_team",
    }:
        return _ENTITY_DEFINITION
    if normalized in {"bio.college", "bio.conference"}:
        return _ENTITY_DEFINITION

    if normalized.startswith(("player_prev.", "player_prior.")):
        return _DEFINITIONS["prior_league_production"]
    if normalized.startswith(("manager_draft_prior.", "manager_matchup_prior.")):
        return _DEFINITIONS["manager_history_context"]
    if normalized.startswith("league."):
        return _DEFINITIONS["league_format_context"]
    if normalized.startswith("profile."):
        return _DEFINITIONS["draft_profile"]
    if normalized == "position_market" or normalized.startswith("position_market."):
        return _DEFINITIONS["position_market"]

    if "draft_age" in normalized or normalized.startswith("bio.age_at_draft"):
        return _DEFINITIONS["age_bucket"]
    if any(token in normalized for token in ("draft_round", "draft_overall", "is_undrafted")):
        return _DEFINITIONS["nfl_draft_capital"]
    if any(
        token in normalized for token in ("ras_score", "forty", "bench", "vertical", "broad_jump", "cone", "shuttle")
    ):
        return _DEFINITIONS["ras_bucket"]
    if "height" in normalized:
        return _DEFINITIONS["height_bucket"]
    if "weight" in normalized:
        return _DEFINITIONS["weight_bucket"]
    if any(token in normalized for token in ("position_percentile", "position_draft_rank", "position_draft_label")):
        return _DEFINITIONS["draft_board_rank"]

    return _DEFAULT_DEFINITION


def canonical_feature_value(feature_type: str, feature_value: Any) -> str:
    """Normalize feature values for stable state keys while preserving discovery."""
    raw = str(feature_value or "").strip()
    normalized = str(feature_type or "").lower()
    if feature_type in {"current_nfl_team", "nfl_draft_team"} or normalized.endswith(
        ("nfl_team", "nfl_team_api", "nfl_draft_team")
    ):
        raw = raw.upper()
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", raw).strip("_").lower()
    return slug or "unknown"


def classify_feature(feature_type: str, feature_value: Any) -> dict[str, Any]:
    """Map a mined feature into deterministic state-machine metadata."""
    feature_type = str(feature_type or "unknown")
    definition = get_state_definition(feature_type)
    canonical_value = canonical_feature_value(feature_type, feature_value)
    return {
        "state_key": f"{definition.state_family}:{feature_type}:{canonical_value}",
        "state_family": definition.state_family,
        "canonical_value": canonical_value,
        "prior_weight": definition.prior_weight,
        "ready_abs_z": definition.ready_abs_z,
        "watch_abs_z": definition.watch_abs_z,
        "ready_years": definition.ready_years,
        "watch_years": definition.watch_years,
        "ready_picks": definition.ready_picks,
        "watch_picks": definition.watch_picks,
        "ready_repeatability": definition.ready_repeatability,
        "watch_repeatability": definition.watch_repeatability,
        "description": definition.description,
        "registry_version": MODEL_VERSION,
    }
