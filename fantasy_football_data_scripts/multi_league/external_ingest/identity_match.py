"""Three-layer identity resolution + ranked suggestions for external managers.

Resolution states:
- bound_via_guid:   real manager_guid in source matches canonical owner_guid
- auto_suggested:   normalized name appears in exactly one canonical alias set
- unresolved:       zero or multiple matches; wizard prompts with ranked suggestions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any
from collections.abc import Iterable

from multi_league.external_ingest.auto_map import normalize


@dataclass
class ExternalManagerOccurrence:
    manager: str  # original-case external manager string
    rows_with_guid: dict[str, int]  # {real_guid: row_count} from source
    years: set[int]
    tables: set[str]
    row_count_total: int = 0

    def __post_init__(self):
        if not self.row_count_total:
            self.row_count_total = sum(self.rows_with_guid.values())


@dataclass
class IdentityResolution:
    manager: str
    state: str  # "bound_via_guid" | "auto_suggested" | "unresolved"
    franchise_id: str | None = None
    suggestions: list[RankedSuggestion] = field(default_factory=list)


@dataclass
class RankedSuggestion:
    franchise_id: str
    franchise_name: str
    score: float


@dataclass
class IdentityCluster:
    franchise_id: str
    external_managers: list[str]
    state: str = "auto_suggested"


def build_alias_set(franchise: Any, external_alias_mappings: dict) -> set[str]:
    """Return the normalized alias set for a franchise.

    Sources:
      - current franchise_name
      - all historical manager strings (via .historical_managers)
      - all external_alias_mappings keys whose value['franchise_id'] == this franchise's id
    """
    aliases: set[str] = set()
    aliases.add(normalize(franchise.franchise_name))
    for hm in getattr(franchise, "historical_managers", ()) or ():
        aliases.add(normalize(hm))
    for ext_str, mapping in (external_alias_mappings or {}).items():
        if isinstance(mapping, dict) and mapping.get("franchise_id") == franchise.franchise_id:
            aliases.add(normalize(ext_str))
    return aliases


def resolve_identity(occ: ExternalManagerOccurrence, registry: Any) -> IdentityResolution:
    """Run the three-layer resolution against the canonical franchise registry.

    `registry` must expose:
      - franchises: iterable of objects with franchise_id, franchise_name, owner_guid, historical_managers
      - external_alias_mappings: dict[str, dict]
    """
    # Layer 1: bound_via_guid
    if occ.rows_with_guid:
        guid_to_franchise = {f.owner_guid: f for f in registry.franchises}
        for guid in occ.rows_with_guid:
            if guid in guid_to_franchise:
                return IdentityResolution(
                    manager=occ.manager,
                    state="bound_via_guid",
                    franchise_id=guid_to_franchise[guid].franchise_id,
                )

    # Layer 2: strict-unique alias match
    norm_external = normalize(occ.manager)
    matching = [f for f in registry.franchises if norm_external in build_alias_set(f, registry.external_alias_mappings)]
    if len(matching) == 1:
        return IdentityResolution(
            manager=occ.manager,
            state="auto_suggested",
            franchise_id=matching[0].franchise_id,
        )

    # Layer 3: unresolved
    return IdentityResolution(
        manager=occ.manager,
        state="unresolved",
        suggestions=suggest_top_n(occ, list(registry.franchises), top_n=3, score_floor=0.3),
    )


def _name_score(external: str, candidate: str) -> float:
    """Token overlap (Jaccard) + ratio tiebreak. Both normalized."""
    e_tokens = set(normalize(external).split("_"))
    c_tokens = set(normalize(candidate).split("_"))
    if not e_tokens or not c_tokens:
        return 0.0
    jaccard = len(e_tokens & c_tokens) / len(e_tokens | c_tokens)
    ratio = SequenceMatcher(None, normalize(external), normalize(candidate)).ratio()
    return 0.7 * jaccard + 0.3 * ratio


def suggest_top_n(
    occ: ExternalManagerOccurrence,
    franchises: Iterable[Any],
    top_n: int = 3,
    score_floor: float = 0.3,
) -> list[RankedSuggestion]:
    scored: list[RankedSuggestion] = []
    for f in franchises:
        # Score against current name AND historical names; take the best
        candidates = [f.franchise_name, *(getattr(f, "historical_managers", ()) or ())]
        best = max((_name_score(occ.manager, c) for c in candidates), default=0.0)
        if best >= score_floor:
            scored.append(RankedSuggestion(f.franchise_id, f.franchise_name, best))
    scored.sort(key=lambda s: s.score, reverse=True)
    return scored[:top_n]


def cluster_resolutions(resolutions: list[IdentityResolution]) -> list:
    """Group multiple external strings auto-suggesting to the same franchise.

    Only `auto_suggested` resolutions cluster — bound_via_guid stays per-string
    (already confirmed; no benefit from UI grouping), unresolved stays per-string
    (each needs its own user decision).

    Returns a mixed list of IdentityCluster (for groups of 2+) and
    IdentityResolution (for singletons / non-clusterable).
    """
    by_fid: dict[str, list[IdentityResolution]] = {}
    output: list = []
    for r in resolutions:
        if r.state == "auto_suggested" and r.franchise_id is not None:
            by_fid.setdefault(r.franchise_id, []).append(r)
        else:
            output.append(r)
    for fid, group in by_fid.items():
        if len(group) >= 2:
            output.append(IdentityCluster(franchise_id=fid, external_managers=[r.manager for r in group]))
        else:
            output.extend(group)
    return output
