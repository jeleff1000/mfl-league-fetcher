from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz.fuzz import ratio

from scripts.sota_recon.witness_gate.position_taxonomy import (
    BROAD_POSITIONS,
    normalize_position,
    positions_compatible,
    validate_position_pair,
)


_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = "".join(char for char in normalized if not unicodedata.combining(char))
    tokens = re.findall(r"[a-z0-9]+", ascii_value.casefold())
    if tokens and tokens[-1] in _SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def normalize_team(value: str) -> str:
    return "".join(char for char in value.upper() if char.isalnum())


def _normalize_source_id(value: str) -> str:
    return value.strip().casefold()


def _validate_broad_position(value: object) -> None:
    if value is None:
        return
    normalized = normalize_position(value)
    if normalized.nfl_position not in BROAD_POSITIONS:
        raise ValueError(f"position must be one broad position, got {value!r}")


@dataclass(frozen=True)
class IdentityQuery:
    name: str
    year: int
    team: str | None = None
    position: str | None = None
    nfl_position: str | None = None
    source_id: str | None = None

    def __post_init__(self) -> None:
        # Compatibility for the original positional form: (name, team, year).
        if isinstance(self.year, str) and isinstance(self.team, int):
            old_team = self.year
            object.__setattr__(self, "year", self.team)
            object.__setattr__(self, "team", old_team)
        _validate_broad_position(self.position)
        if self.position is not None and self.nfl_position is not None:
            validate_position_pair(self.position, self.nfl_position)


@dataclass(frozen=True)
class IdentityCandidate:
    player_id: str
    name: str
    start_year: int | None = None
    end_year: int | None = None
    teams: tuple[str, ...] = ()
    position: str | None = None
    nfl_position: str | None = None
    aliases: tuple[str, ...] = ()
    source_ids: tuple[str, ...] = ()
    # Deprecated newspaper-lane fields retained until its stored witness rows migrate.
    team: str | None = None
    year_start: int | None = None
    year_end: int | None = None
    source_id: str | None = None
    proof_leaf: str = ""
    precedence: int = 20

    def __post_init__(self) -> None:
        if self.start_year is None and self.year_start is not None:
            object.__setattr__(self, "start_year", self.year_start)
        if self.end_year is None and self.year_end is not None:
            object.__setattr__(self, "end_year", self.year_end)
        if not self.teams and self.team:
            object.__setattr__(self, "teams", (self.team,))
        if not self.source_ids and self.source_id:
            object.__setattr__(self, "source_ids", (self.source_id,))
        _validate_broad_position(self.position)
        if self.position is not None and self.nfl_position is not None:
            validate_position_pair(self.position, self.nfl_position)


@dataclass(frozen=True)
class IdentityDecision:
    status: str
    player_id: str | None
    candidates: tuple[IdentityCandidate, ...]
    selected_candidate: IdentityCandidate | None
    proof_leaves: tuple[str, ...] = ()
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlayerBioPatch:
    status: str
    player_id: str | None
    changes: dict[str, Any]
    proof_leaves: tuple[str, ...]
    evidence: dict[str, Any] = field(default_factory=dict)


def _active_in_year(item: IdentityCandidate, year: int) -> bool:
    if item.start_year is not None and year < item.start_year:
        return False
    return item.end_year is None or year <= item.end_year


def _team_matches(item: IdentityCandidate, team: str | None) -> bool:
    if not team or not item.teams:
        return True
    normalized_team = normalize_team(team)
    return any(normalize_team(candidate_team) == normalized_team for candidate_team in item.teams)


def _broad_compatibility(query: IdentityQuery, item: IdentityCandidate) -> bool | None:
    if query.position is None or item.position is None:
        return None
    return positions_compatible(query.position, item.position)


def _detailed_match(query: IdentityQuery, item: IdentityCandidate) -> bool | None:
    if query.nfl_position is None or item.nfl_position is None:
        return None
    return normalize_position(query.nfl_position).nfl_position == normalize_position(item.nfl_position).nfl_position


def _proof_leaves(items: Iterable[IdentityCandidate]) -> tuple[str, ...]:
    return tuple(sorted({item.proof_leaf for item in items if item.proof_leaf}))


def resolve_identity(
    query: IdentityQuery,
    candidates: Iterable[IdentityCandidate],
    *,
    fuzzy_threshold: float = 85.0,
) -> IdentityDecision:
    query_name = normalize_name(query.name)
    query_source_id = _normalize_source_id(query.source_id) if query.source_id else None
    candidate_list = list(candidates)
    scored: list[dict[str, Any]] = []
    for item in candidate_list:
        normalized_name = normalize_name(item.name)
        source_id_match = bool(
            query_source_id
            and any(_normalize_source_id(source_id) == query_source_id for source_id in item.source_ids)
        )
        scored.append(
            {
                "player_id": item.player_id,
                "source_id_match": source_id_match,
                "alias_match": any(normalize_name(alias) == query_name for alias in item.aliases),
                "active_year": _active_in_year(item, query.year),
                "team_year": _team_matches(item, query.team),
                "exact_name": normalized_name == query_name,
                "name_similarity": ratio(query_name, normalized_name),
                "broad_position_compatible": _broad_compatibility(query, item),
                "detailed_position_match": _detailed_match(query, item),
                "source_continuity": len(item.source_ids),
            }
        )

    evidence: dict[str, Any] = {"scored_candidates": tuple(scored), "winning_rule": None}

    def decision(
        status: str,
        items: list[IdentityCandidate],
        selected: IdentityCandidate | None = None,
        winning_rule: str | None = None,
    ) -> IdentityDecision:
        evidence["winning_rule"] = winning_rule
        return IdentityDecision(
            status,
            selected.player_id if selected else None,
            tuple(items),
            selected,
            _proof_leaves(items),
            evidence,
        )

    source_matches = [
        (item, row)
        for item, row in zip(candidate_list, scored)
        if row["source_id_match"]
        and row["active_year"]
        and row["team_year"]
        and row["broad_position_compatible"] is not False
    ]
    if source_matches:
        source_items = [item for item, _ in source_matches]
        player_ids = {item.player_id for item in source_items}
        if len(player_ids) == 1:
            selected = min(source_items, key=lambda item: (item.precedence, item.player_id))
            return decision("resolved", [selected], selected, "source_id")
        candidate_list = source_items
        scored = [row for _, row in source_matches]

    alias_matches = [
        (item, row)
        for item, row in zip(candidate_list, scored)
        if row["alias_match"]
        and row["active_year"]
        and row["team_year"]
        and row["broad_position_compatible"] is not False
    ]
    if alias_matches:
        alias_items = [item for item, _ in alias_matches]
        player_ids = {item.player_id for item in alias_items}
        if len(player_ids) == 1:
            selected = min(alias_items, key=lambda item: (item.precedence, item.player_id))
            return decision("resolved", [selected], selected, "alias")
        candidate_list = alias_items
        scored = [row for _, row in alias_matches]

    eligible_pairs = [
        (item, row)
        for item, row in zip(candidate_list, scored)
        if row["active_year"]
        and row["team_year"]
        and row["name_similarity"] >= fuzzy_threshold
        and row["broad_position_compatible"] is not False
    ]
    if not eligible_pairs:
        return decision("unresolved", [], winning_rule=None)

    exact_pairs = [(item, row) for item, row in eligible_pairs if row["exact_name"]]
    winning_rule = "exact_name" if exact_pairs else "fuzzy_name"
    if exact_pairs:
        eligible_pairs = exact_pairs

    grouped: dict[str, list[tuple[IdentityCandidate, dict[str, Any]]]] = {}
    for item, row in eligible_pairs:
        grouped.setdefault(item.player_id, []).append((item, row))

    if len(grouped) > 1 and query.nfl_position is not None:
        detailed_ids = {
            player_id
            for player_id, rows in grouped.items()
            if any(row["detailed_position_match"] is True for _, row in rows)
        }
        if len(detailed_ids) == 1:
            grouped = {player_id: grouped[player_id] for player_id in detailed_ids}
            winning_rule = "detailed_position"

    if len(grouped) > 1:
        continuity = {
            player_id: max(row["source_continuity"] for _, row in rows)
            for player_id, rows in grouped.items()
        }
        maximum_continuity = max(continuity.values())
        continuity_ids = {
            player_id for player_id, count in continuity.items() if count == maximum_continuity
        }
        if maximum_continuity > 0 and len(continuity_ids) == 1:
            grouped = {player_id: grouped[player_id] for player_id in continuity_ids}
            winning_rule = "source_continuity"

    representatives = [
        min(rows, key=lambda pair: (pair[0].precedence, -pair[1]["source_continuity"], pair[0].player_id))[0]
        for _, rows in sorted(grouped.items())
    ]
    if len(representatives) > 1:
        return decision("collision_review", representatives, winning_rule="collision")

    selected = representatives[0]
    evidence["winning_rule"] = winning_rule
    return IdentityDecision(
        "resolved",
        selected.player_id,
        (selected,),
        selected,
        _proof_leaves(item for item, _ in grouped[selected.player_id]),
        evidence,
    )


def propose_player_bio_patch(existing: dict[str, Any], decision: IdentityDecision) -> PlayerBioPatch:
    if decision.status != "resolved" or decision.selected_candidate is None:
        return PlayerBioPatch(decision.status, decision.player_id, {}, decision.proof_leaves, decision.evidence)
    selected = decision.selected_candidate
    _validate_broad_position(existing.get("position"))
    existing_id = existing.get("NFL_player_id")
    if existing_id not in {None, "", selected.player_id}:
        return PlayerBioPatch(
            "collision_review",
            selected.player_id,
            {},
            decision.proof_leaves,
            {**decision.evidence, "mismatch": "NFL_player_id"},
        )

    changes: dict[str, Any] = {}
    existing_name = existing.get("player")
    if existing_name in {None, ""}:
        changes["player"] = selected.name
    elif normalize_name(str(existing_name)) != normalize_name(selected.name):
        return PlayerBioPatch(
            "collision_review",
            selected.player_id,
            {},
            decision.proof_leaves,
            {**decision.evidence, "mismatch": "player"},
        )

    for field_name in ("position", "nfl_position"):
        selected_value = getattr(selected, field_name)
        existing_value = existing.get(field_name)
        if existing_value in {None, ""} and selected_value:
            changes[field_name] = selected_value
        elif selected_value and normalize_position(existing_value).nfl_position != normalize_position(selected_value).nfl_position:
            return PlayerBioPatch(
                "collision_review",
                selected.player_id,
                {},
                decision.proof_leaves,
                {**decision.evidence, "mismatch": field_name},
            )
    if existing_id in {None, ""}:
        changes["NFL_player_id"] = selected.player_id
    return PlayerBioPatch("candidate_patch", selected.player_id, changes, decision.proof_leaves, decision.evidence)
