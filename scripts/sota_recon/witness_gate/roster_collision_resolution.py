from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd
from rapidfuzz.fuzz import ratio

from .identity_lane import IdentityCandidate, IdentityQuery, normalize_name, normalize_team
from .position_taxonomy import normalize_position, positions_compatible


# nflverse documents GSIS player identifiers for the 1999-forward play-by-play era.
# Version this boundary because changing it changes the canonical identifier selected.
GSIS_PRIMARY_ERA_START = 1999
GSIS_PRIMARY_ERA_VERSION = "gsis-primary-era.v1:1999"


@dataclass(frozen=True)
class CollisionDecision:
    status: Literal["resolved", "collision_review"]
    canonical_player_id: str | None
    id_aliases: tuple[str, ...]
    rule: str
    evidence: tuple[dict[str, object], ...]
    selected_candidate: IdentityCandidate | None = field(default=None, repr=False)
    considered_candidates: tuple[IdentityCandidate, ...] = field(default=(), repr=False)


@dataclass(frozen=True)
class CollisionBioPatch:
    status: Literal["candidate_patch", "collision_review"]
    player_id: str | None
    changes: dict[str, object]
    reason: str | None = None


def _clean(value: object) -> str:
    if value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
        return ""
    return str(value).strip()


def normalize_id_aliases(value: object) -> tuple[str, ...]:
    """Return a deterministic alias set across JSON, Parquet arrays, and iterables."""
    if value is None:
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        if stripped.startswith("["):
            decoded = json.loads(stripped)
            if not isinstance(decoded, list):
                raise ValueError("id_aliases JSON must be a list")
            value = decoded
        else:
            value = (stripped,)
    elif hasattr(value, "tolist"):
        value = value.tolist()
    elif not isinstance(value, (list, tuple, set)):
        if pd.isna(value):
            return ()
        value = (value,)
    return tuple(sorted({_clean(item) for item in value if _clean(item)}))


def _is_gsis(player_id: str) -> bool:
    return player_id.startswith("00-")


def _active(candidate: IdentityCandidate, year: int) -> bool:
    return (candidate.start_year is None or candidate.start_year <= year) and (
        candidate.end_year is None or year <= candidate.end_year
    )


def _name_similarity(query: IdentityQuery, candidate: IdentityCandidate) -> float:
    return ratio(normalize_name(query.name), normalize_name(candidate.name))


def _alias_match(query: IdentityQuery, candidate: IdentityCandidate) -> bool:
    query_name = normalize_name(query.name)
    return any(normalize_name(alias) == query_name for alias in candidate.aliases)


def _source_match(query: IdentityQuery, candidate: IdentityCandidate) -> bool:
    source_id = _clean(query.source_id).casefold()
    if not source_id:
        return False
    candidate_ids = (candidate.player_id, *candidate.source_ids)
    return source_id in {_clean(value).casefold() for value in candidate_ids}


def _candidate_teams(candidate: IdentityCandidate, rows: Sequence[dict[str, object]]) -> set[str]:
    return {
        normalize_team(value)
        for value in (*candidate.teams, *(row.get("team") for row in rows))
        if _clean(value)
    }


def _position_values(
    candidate: IdentityCandidate, rows: Sequence[dict[str, object]], field_name: str
) -> tuple[str, ...]:
    values = [getattr(candidate, field_name)]
    values.extend(row.get(field_name) for row in rows)
    return tuple(sorted({_clean(value) for value in values if _clean(value)}))


def _broad_compatible(
    query: IdentityQuery, candidate: IdentityCandidate, rows: Sequence[dict[str, object]]
) -> bool | None:
    if query.position is None:
        return None
    positions = _position_values(candidate, rows, "position")
    if not positions:
        return None
    # Query-relevant known facts are conjunctive identity evidence. A matching
    # witness cannot erase a conflicting canonical or witness position.
    return all(positions_compatible(query.position, value) for value in positions)


def _detailed_match(
    query: IdentityQuery, candidate: IdentityCandidate, rows: Sequence[dict[str, object]]
) -> bool | None:
    if query.nfl_position is None:
        return None
    positions = _position_values(candidate, rows, "nfl_position")
    if not positions:
        return None
    expected = normalize_position(query.nfl_position).nfl_position
    return any(normalize_position(value).nfl_position == expected for value in positions)


class PfaAppearanceIndex:
    """Reusable keyed access to PFA evidence; no query performs DataFrame scans."""

    def __init__(self, appearances: pd.DataFrame):
        self._by_source_year: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
        self._by_name_year: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
        for row in appearances.to_dict("records"):
            year_value = pd.to_numeric(row.get("season"), errors="coerce")
            if pd.isna(year_value):
                continue
            year = int(year_value)
            source_id = _clean(row.get("source_player_id")).casefold()
            name = normalize_name(_clean(row.get("player")))
            if source_id:
                self._by_source_year[(source_id, year)].append(row)
            if name:
                self._by_name_year[(name, year)].append(row)

    def rows_for(self, query: IdentityQuery, candidate: IdentityCandidate) -> tuple[dict[str, object], ...]:
        matching_rows: list[dict[str, object]] = []
        candidate_ids = {
            _clean(value).casefold() for value in (candidate.player_id, *candidate.source_ids)
        }
        for source_id in (candidate.player_id, *candidate.source_ids):
            for row in self._by_source_year.get((_clean(source_id).casefold(), query.year), ()):
                matching_rows.append(row)
        for row in self._by_name_year.get((normalize_name(candidate.name), query.year), ()):
            row_source_id = _clean(row.get("source_player_id")).casefold()
            if row_source_id and row_source_id not in candidate_ids:
                continue
            matching_rows.append(row)

        # The source and name indexes can cite the same row, while separate
        # materialized rows can legitimately share game/source IDs. Merge only
        # semantically identical witnesses; conflicting facts remain separate.
        merged: dict[str, dict[str, object]] = {}
        for raw_row in matching_rows:
            row = dict(raw_row)
            semantic_key = json.dumps(
                {key: value for key, value in row.items() if key != "lineage_leaves"},
                sort_keys=True,
                default=str,
            )
            leaves = _typed_lineage_leaves(row)
            if semantic_key not in merged:
                row["lineage_leaves"] = list(leaves)
                merged[semantic_key] = row
                continue
            existing_leaves = _typed_lineage_leaves(merged[semantic_key])
            leaf_index = {
                (int(leaf["shard_id"]), str(leaf["manifest_sha256"])): leaf
                for leaf in (*existing_leaves, *leaves)
            }
            merged[semantic_key]["lineage_leaves"] = [
                leaf_index[key] for key in sorted(leaf_index)
            ]
        return tuple(merged[key] for key in sorted(merged))


def _typed_lineage_leaves(row: dict[str, object]) -> tuple[dict[str, object], ...]:
    raw_leaves = row.get("lineage_leaves")
    leaves: object = raw_leaves
    if not isinstance(raw_leaves, (list, tuple)):
        is_forbidden_container = isinstance(raw_leaves, (str, bytes, Mapping))
        to_list = getattr(raw_leaves, "tolist", None)
        if is_forbidden_container or pd.api.types.is_scalar(raw_leaves) or not callable(to_list):
            raise ValueError("PFA witness requires typed lineage leaves")
        leaves = to_list()
    if not isinstance(leaves, (list, tuple)):
        raise ValueError("PFA witness requires typed lineage leaves")
    unique: dict[tuple[int, str], dict[str, object]] = {}
    for leaf in leaves:
        if not isinstance(leaf, dict) or "shard_id" not in leaf or "manifest_sha256" not in leaf:
            raise ValueError("PFA witness requires a typed lineage leaf record")
        key = (int(leaf["shard_id"]), str(leaf["manifest_sha256"]))
        unique[key] = {"shard_id": key[0], "manifest_sha256": key[1]}
    return tuple(unique[key] for key in sorted(unique))


def _pfa_evidence(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    evidence: list[dict[str, object]] = []
    for row in rows:
        for leaf in _typed_lineage_leaves(row):
            evidence.append(
                {
                    "evidence_type": "pfa_lineage_leaf",
                    "season": int(row["season"]),
                    "game_id": _clean(row.get("game_id")),
                    "team": _clean(row.get("team")),
                    "player": _clean(row.get("player")),
                    "source_player_id": _clean(row.get("source_player_id")),
                    "position": _clean(row.get("position")) or None,
                    "nfl_position": _clean(row.get("nfl_position")) or None,
                    "shard_id": int(leaf["shard_id"]),
                    "manifest_sha256": str(leaf["manifest_sha256"]),
                }
            )
    return sorted(
        evidence,
        key=lambda row: (
            str(row["source_player_id"]), str(row["game_id"]),
            int(row["shard_id"]), str(row["manifest_sha256"]),
        ),
    )


def _linked_duplicate_ids(candidates: Sequence[IdentityCandidate]) -> bool:
    if len(candidates) < 2 or len({normalize_name(item.name) for item in candidates}) != 1:
        return False
    ids = {item.player_id.casefold() for item in candidates}
    graph: dict[str, set[str]] = {item.player_id.casefold(): set() for item in candidates}
    for item in candidates:
        item_id = item.player_id.casefold()
        for source_id in item.source_ids:
            other_id = _clean(source_id).casefold()
            if other_id in ids:
                graph[item_id].add(other_id)
                graph[other_id].add(item_id)
    seen = {next(iter(ids))}
    pending = list(seen)
    while pending:
        current = pending.pop()
        for linked in graph[current] - seen:
            seen.add(linked)
            pending.append(linked)
    return seen == ids


def _canonical_duplicate(
    year: int, candidates: Sequence[IdentityCandidate]
) -> tuple[IdentityCandidate, str] | None:
    if not _linked_duplicate_ids(candidates):
        return None
    gsis = [item for item in candidates if _is_gsis(item.player_id)]
    pfr = [item for item in candidates if not _is_gsis(item.player_id)]
    if year >= GSIS_PRIMARY_ERA_START and len(gsis) == 1:
        return gsis[0], "gsis_primary_with_pfr_alias"
    if year < GSIS_PRIMARY_ERA_START and len(pfr) == 1:
        return pfr[0], "pfr_primary_before_gsis_era"
    return None


def _aliases_for(selected: IdentityCandidate, candidates: Sequence[IdentityCandidate]) -> tuple[str, ...]:
    values = {
        value
        for item in candidates
        for value in (item.player_id, *item.source_ids)
        if _clean(value) and _clean(value) != selected.player_id
    }
    return tuple(sorted(values))


def _resolve_with_index(
    query: IdentityQuery,
    candidates: Sequence[IdentityCandidate],
    index: PfaAppearanceIndex,
    *,
    fuzzy_threshold: float = 85.0,
) -> CollisionDecision:
    ordered = sorted(candidates, key=lambda item: item.player_id)
    rows_by_id = {item.player_id: index.rows_for(query, item) for item in ordered}
    records = [
        {
            "candidate": item,
            "source": _source_match(query, item),
            "alias": _alias_match(query, item),
            "active": _active(item, query.year),
            "team": not query.team or not _candidate_teams(item, rows_by_id[item.player_id])
            or normalize_team(query.team) in _candidate_teams(item, rows_by_id[item.player_id]),
            "name_score": _name_similarity(query, item),
            "broad": _broad_compatible(query, item, rows_by_id[item.player_id]),
            "detailed": _detailed_match(query, item, rows_by_id[item.player_id]),
            "continuity": len(set(item.source_ids)),
        }
        for item in ordered
    ]
    last_rule = "normalized_name"
    no_eligible = False

    def prefer_true(key: str, rule: str) -> None:
        nonlocal records, last_rule
        matched = [row for row in records if row[key] is True]
        if matched and len(matched) < len(records):
            records = matched
            last_rule = rule

    source_matches = [
        row for row in records
        if row["source"] is True and row["active"] is True and row["team"] is True
        and row["broad"] is not False
    ]
    alias_matches = [
        row for row in records
        if row["alias"] is True and row["active"] is True and row["team"] is True
        and row["broad"] is not False
    ]
    direct_unique = False
    direct_subset = False
    if source_matches:
        records = source_matches
        last_rule = "verified_source_id_crosswalk"
        direct_subset = True
        direct_unique = len(records) == 1
    elif alias_matches:
        records = alias_matches
        last_rule = "alias"
        direct_subset = True
        direct_unique = len(records) == 1
    if not direct_subset:
        for key, rule in (("active", "active_year"), ("team", "team_year")):
            filtered = [row for row in records if row[key] is True]
            if len(filtered) < len(records):
                last_rule = rule
            records = filtered
            if not records:
                no_eligible = True
                break
    if not no_eligible and not direct_unique:
        exact = [row for row in records if float(row["name_score"]) == 100.0]
        if exact:
            if len(exact) < len(records):
                last_rule = "normalized_name"
            records = exact
        else:
            name_eligible = [row for row in records if float(row["name_score"]) >= fuzzy_threshold]
            if name_eligible:
                if len(name_eligible) < len(records):
                    last_rule = "normalized_fuzzy_name"
                records = name_eligible
            elif not direct_subset:
                records = []
                no_eligible = True

    linked_duplicates = _linked_duplicate_ids([row["candidate"] for row in records])
    if not no_eligible and not direct_unique:
        if linked_duplicates:
            if any(row["broad"] is False for row in records):
                records = []
                no_eligible = True
                last_rule = "broad_position"
        else:
            broad = [row for row in records if row["broad"] is not False]
            if len(broad) < len(records):
                last_rule = "broad_position"
            records = broad
            if not records:
                no_eligible = True
            else:
                prefer_true("broad", "broad_position")

    if not no_eligible and not direct_unique and not linked_duplicates:
        prefer_true("detailed", "primary_position")

    if not no_eligible and not direct_unique and not linked_duplicates and len(records) > 1:
        max_continuity = max(int(row["continuity"]) for row in records)
        continuous = [row for row in records if int(row["continuity"]) == max_continuity]
        if max_continuity > 0 and len(continuous) < len(records):
            records = continuous
            last_rule = "source_continuity"

    remaining = [row["candidate"] for row in records]
    duplicate = None if no_eligible else _canonical_duplicate(query.year, remaining)
    if duplicate is not None:
        selected, last_rule = duplicate
    elif len(remaining) == 1:
        selected = remaining[0]
    else:
        selected = None

    cited = ordered if no_eligible else (remaining if selected is None or duplicate is not None else [selected])
    evidence: list[dict[str, object]] = [
        {
            "evidence_type": "era_policy",
            "era_policy_version": GSIS_PRIMARY_ERA_VERSION,
            "era_start": GSIS_PRIMARY_ERA_START,
        }
    ]
    for item in cited:
        if item.proof_leaf:
            evidence.append(
                {
                    "evidence_type": "identity_candidate",
                    "player_id": item.player_id,
                    "proof_leaf": item.proof_leaf,
                }
            )
        evidence.extend(_pfa_evidence(rows_by_id[item.player_id]))

    if selected is None:
        return CollisionDecision(
            "collision_review", None, (),
            "no_eligible_candidate" if no_eligible else "exact_tie",
            tuple(evidence), None, tuple(remaining),
        )
    aliases = _aliases_for(selected, remaining)
    return CollisionDecision(
        "resolved", selected.player_id, aliases, last_rule, tuple(evidence), selected, tuple(remaining)
    )


def resolve_roster_collision(
    query: IdentityQuery,
    candidates: Sequence[IdentityCandidate],
    *,
    pfa_appearances: pd.DataFrame,
) -> CollisionDecision:
    return _resolve_with_index(query, candidates, PfaAppearanceIndex(pfa_appearances))


def build_collision_ledgers(
    decisions: Iterable[tuple[str, CollisionDecision]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    decision_rows: list[dict[str, object]] = []
    evidence_rows: list[dict[str, object]] = []
    for collision_id, decision in decisions:
        decision_rows.append(
            {
                "collision_id": collision_id,
                "status": decision.status,
                "canonical_player_id": decision.canonical_player_id,
                "id_aliases_json": json.dumps(decision.id_aliases),
                "rule": decision.rule,
            }
        )
        evidence_rows.extend({"collision_id": collision_id, **row} for row in decision.evidence)
    return pd.DataFrame(decision_rows), pd.DataFrame(evidence_rows)


def propose_collision_bio_patch(
    existing: dict[str, object], decision: CollisionDecision
) -> CollisionBioPatch:
    selected = decision.selected_candidate
    if decision.status != "resolved" or selected is None:
        return CollisionBioPatch("collision_review", decision.canonical_player_id, {}, "unresolved_collision")
    existing_id = _clean(existing.get("NFL_player_id"))
    if existing_id and existing_id != selected.player_id:
        return CollisionBioPatch("collision_review", selected.player_id, {}, "conflicting_NFL_player_id")

    changes: dict[str, object] = {}
    facts: dict[str, object] = {
        "NFL_player_id": selected.player_id,
    }
    candidates = decision.considered_candidates or (selected,)
    for field_name in ("player", "position", "nfl_position"):
        candidate_field = "name" if field_name == "player" else field_name
        values = [
            getattr(item, candidate_field)
            for item in candidates
            if _clean(getattr(item, candidate_field))
        ]
        if not values:
            continue
        if field_name == "player":
            semantic_values = {normalize_name(str(value)) for value in values}
        else:
            semantic_values = {normalize_position(value).nfl_position for value in values}
        if len(semantic_values) == 1:
            selected_value = getattr(selected, candidate_field)
            facts[field_name] = selected_value if _clean(selected_value) else sorted(values, key=str)[0]
    for field_name, value in facts.items():
        if value in {None, ""}:
            continue
        current = existing.get(field_name)
        if current is None or (not isinstance(current, (list, tuple, dict)) and pd.isna(current)) or current == "":
            changes[field_name] = value
            continue
        if field_name == "player":
            agrees = normalize_name(str(current)) == normalize_name(str(value))
        elif field_name in {"position", "nfl_position"}:
            agrees = normalize_position(current).nfl_position == normalize_position(value).nfl_position
        else:
            agrees = str(current) == str(value)
        if not agrees:
            return CollisionBioPatch("collision_review", selected.player_id, {}, f"conflicting_{field_name}")
    if decision.id_aliases:
        changes["id_aliases"] = tuple(
            sorted(set(normalize_id_aliases(existing.get("id_aliases"))) | set(decision.id_aliases))
        )
    return CollisionBioPatch("candidate_patch", selected.player_id, changes)


def resolve_collision_batch(
    items: Iterable[tuple[str, IdentityQuery, Sequence[IdentityCandidate]]],
    pfa_appearances: pd.DataFrame,
) -> list[tuple[str, CollisionDecision]]:
    index = PfaAppearanceIndex(pfa_appearances)
    return [
        (collision_id, _resolve_with_index(query, candidates, index))
        for collision_id, query, candidates in items
    ]
