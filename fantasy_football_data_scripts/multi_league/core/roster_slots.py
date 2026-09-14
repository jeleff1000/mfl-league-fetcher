"""Canonical flex slot definitions, alias resolution, and roster helpers.

Single source of truth for which NFL positions can fill each roster slot,
across all platforms (Yahoo, Sleeper, ESPN).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical flex types -> eligible NFL positions
# ---------------------------------------------------------------------------
FLEX_ELIGIBLE: dict[str, set[str]] = {
    "FLX": {"WR", "RB", "TE"},
    "SUPER_FLEX": {"QB", "WR", "RB", "TE"},
    "REC_FLEX": {"WR", "TE"},
    "W/R": {"WR", "RB"},
    "R/T": {"RB", "TE"},
    "IDP": {"DL", "LB", "DB"},
    "DL_LB": {"DL", "LB"},
    "DB_LB": {"DB", "LB"},
}

# ---------------------------------------------------------------------------
# Dedicated (non-flex) starter positions
# ---------------------------------------------------------------------------
DEDICATED_POSITIONS: set[str] = {"QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB"}

# ---------------------------------------------------------------------------
# Non-starter slots (excluded from lineup calculations)
# ---------------------------------------------------------------------------
NON_STARTER_SLOTS: set[str] = {"BN", "IR", "IL", "TAXI", "RESERVE", "RES", "COVID", "PUP", "INJ"}

# ---------------------------------------------------------------------------
# IDP position families (granular NFL position -> normalized group)
# ---------------------------------------------------------------------------
IDP_FAMILIES: dict[str, set[str]] = {
    "LB": {"LB", "ILB", "OLB", "MLB"},
    "DL": {"DL", "DE", "DT", "EDGE", "NT", "ED"},
    "DB": {"DB", "CB", "S", "SS", "FS", "SAF"},
}

# Front-7 combined family (LB + DL).  Platforms classify edge rushers
# inconsistently (Will Anderson = DL on Sleeper, LB elsewhere), so for
# *manager* optimal lineup we treat LB and DL slots as interchangeable.
FRONT_7: set[str] = IDP_FAMILIES["LB"] | IDP_FAMILIES["DL"]
FRONT_7_SLOTS: set[str] = {"DL", "LB"}

# ---------------------------------------------------------------------------
# Alias -> canonical name (all platform variants)
# ---------------------------------------------------------------------------
ALIASES: dict[str, str] = {
    # Yahoo offense flex
    "W/R/T": "FLX",
    "FLEX": "FLX",
    # ESPN flex variants
    "RB/WR/TE": "FLX",
    "RB/WR": "W/R",
    "WR/TE": "REC_FLEX",
    # Yahoo/Sleeper superflex
    "Q/W/R/T": "SUPER_FLEX",
    "OP": "SUPER_FLEX",
    # Yahoo/Sleeper rec flex
    "W/T": "REC_FLEX",
    # Sleeper WR/RB flex
    "WRRB_FLEX": "W/R",
    # IDP flex variants
    "IDP_FLEX": "IDP",
    "D": "IDP",
    "DP": "IDP",
    # Granular IDP -> dedicated slot
    "DE": "DL",
    "DT": "DL",
    "NT": "DL",
    "ED": "DL",
    "EDGE": "DL",
    "ILB": "LB",
    "OLB": "LB",
    "MLB": "LB",
    "CB": "DB",
    "S": "DB",
    "SS": "DB",
    "FS": "DB",
    "SAF": "DB",
    # Fullback -> RB for fantasy purposes
    "FB": "RB",
    # DST aliases
    "DST": "DEF",
    "D/ST": "DEF",
    # Kicker alias
    "PK": "K",
    # ESPN bench / reserve aliases
    "BE": "BN",
    "ER": "IR",
    # Draft enrichment uses SUPERFLEX (no underscore)
    "SUPERFLEX": "SUPER_FLEX",
}

# Build case-insensitive lookup (uppercase keys)
_ALIASES_UPPER: dict[str, str] = {k.upper(): v for k, v in ALIASES.items()}

# All canonical names (flex + dedicated) for passthrough detection
_ALL_CANONICAL: set[str] = set(FLEX_ELIGIBLE) | DEDICATED_POSITIONS | NON_STARTER_SLOTS

# Pre-build uppercase -> correctly-cased canonical lookup for O(1) resolve
_CANONICAL_CASE: dict[str, str] = {c.upper(): c for c in _ALL_CANONICAL}


def resolve(slot_name: str) -> str:
    """Resolve a platform-specific slot name to its canonical name.

    Handles Yahoo (W/R/T), Sleeper (FLEX), granular IDP (DE->DL),
    and DST aliases (DST->DEF). Case-insensitive.

    Passthrough for already-canonical names and dedicated positions.
    """
    upper = slot_name.upper()
    # Already canonical
    canonical_cased = _CANONICAL_CASE.get(upper)
    if canonical_cased is not None:
        return canonical_cased
    # Check alias table
    canonical = _ALIASES_UPPER.get(upper)
    if canonical is not None:
        return canonical
    return slot_name


def eligible_positions(slot_name: str) -> set[str]:
    """Return the set of NFL positions eligible to fill a roster slot.

    For flex slots: returns the eligible position set.
    For dedicated positions (QB, RB, etc.): returns {position}.
    Resolves aliases automatically.

    Raises ValueError for unknown slot names.
    """
    canonical = resolve(slot_name)
    if canonical in FLEX_ELIGIBLE:
        return FLEX_ELIGIBLE[canonical]
    if canonical in DEDICATED_POSITIONS:
        return {canonical}
    raise ValueError(f"Unknown roster slot: {slot_name!r} (resolved to {canonical!r})")


def normalize_position(pos: str) -> str:
    """Normalize an NFL position to its canonical fantasy group.

    Handles granular IDP positions (CB→DB, DT→DL, FS→DB, etc.),
    FB→RB, and dual positions ('WR,RB' → 'WR').
    """
    if not pos:
        return pos
    # Take primary position for dual-position players
    primary = pos.split(",")[0].strip()
    return resolve(primary)


def is_flex(slot_name: str) -> bool:
    """Return True if the slot is a flex slot (not dedicated, not bench)."""
    canonical = resolve(slot_name)
    return canonical in FLEX_ELIGIBLE


def is_bench(slot_name: str) -> bool:
    """Return True if the slot is a non-starter slot (BN, IR, TAXI, etc.)."""
    return resolve(slot_name).upper() in {s.upper() for s in NON_STARTER_SLOTS}


def get_flex_pools(
    roster_settings: dict[str, int],
) -> list[tuple[str, set[str], int]]:
    """Extract flex slot pools from a league's roster settings.

    Args:
        roster_settings: Mapping of slot name -> count (e.g. {"QB": 1, "W/R/T": 2}).

    Returns:
        List of (canonical_name, eligible_positions, count) tuples,
        sorted by pool size ascending (most restrictive first).
        Only includes flex slots with count > 0.
        If multiple aliases resolve to the same canonical (e.g., FLEX + W/R/T),
        only the first encountered is included (real rosters never have both).
    """
    pools = []
    seen_canonical: set[str] = set()
    for slot_name, count in roster_settings.items():
        if not isinstance(count, (int, float)):
            continue
        if count <= 0:
            continue
        canonical = resolve(slot_name)
        if canonical in FLEX_ELIGIBLE and canonical not in seen_canonical:
            seen_canonical.add(canonical)
            pools.append((canonical, FLEX_ELIGIBLE[canonical], count))
    pools.sort(key=lambda p: len(p[1]))
    return pools
