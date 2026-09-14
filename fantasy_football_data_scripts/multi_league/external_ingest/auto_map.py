"""Column auto-mapping from source files into the canonical slot manifest."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from multi_league.external_ingest.manifests import TableManifest


_SEP_RE = re.compile(r"[-_/\s]+")


def normalize(col: str) -> str:
    """Normalize a column name: lowercase, collapse separators, strip whitespace."""
    return _SEP_RE.sub("_", col.strip().lower())


@dataclass
class AutoMapResult:
    column_map: dict[str, str] = field(default_factory=dict)  # slot_name -> file_column (original casing preserved)
    ambiguous_slots: list[str] = field(default_factory=list)
    unfilled_slots: list[str] = field(default_factory=list)
    satisfied: bool = False


def auto_map(file_columns: list[str], manifest: TableManifest) -> AutoMapResult:
    """Map source file columns onto manifest slots.

    Rules (in priority order):
    1. Canonical-name match wins. If a file column normalizes to slot.name, bind it.
    2. Unique alias match. A file column matches via aliases iff exactly one file
       column normalizes to one of the slot's aliases (and no canonical match was
       already found for that slot).
    3. Multiple file columns aliasing the same slot → slot is ambiguous (no binding).
    """
    file_norm = {normalize(c): c for c in file_columns}

    result = AutoMapResult()
    bound_file_cols: set[str] = set()

    for slot in manifest.slots:
        slot_norm = normalize(slot.name)

        # Pass 1: canonical name wins
        if slot_norm in file_norm:
            result.column_map[slot.name] = file_norm[slot_norm]
            bound_file_cols.add(file_norm[slot_norm])
            continue

        # Pass 2: alias match (only file columns not already bound)
        alias_norms = {normalize(a) for a in slot.aliases if normalize(a) != slot_norm}
        matches = [file_norm[n] for n in alias_norms if n in file_norm and file_norm[n] not in bound_file_cols]
        if len(matches) == 1:
            result.column_map[slot.name] = matches[0]
            bound_file_cols.add(matches[0])
        elif len(matches) > 1:
            result.ambiguous_slots.append(slot.name)
        # else: zero matches; will be reported as unfilled if required

    # Compute unfilled required slots
    filled = set(result.column_map)
    derivable = {s.name for s in manifest.slots if s.derivable_from is not None}
    for slot in manifest.slots:
        if slot.group in ("identity", "outcome") and slot.name not in filled and slot.name not in derivable:
            # Don't flag derivable slots as unfilled — they may be synthesized later
            result.unfilled_slots.append(slot.name)

    # Satisfaction: pass all rules using the union of filled + derivable-if-source-present
    # For satisfaction check, treat derivable slots as filled if any of their input slots are filled.
    sat_filled = set(filled)
    if "win" not in sat_filled and {"team_points", "opponent_points"}.issubset(sat_filled):
        sat_filled.add("win")  # win derivable from points

    result.satisfied = all(rule(sat_filled) for rule in manifest.satisfaction_rules)
    return result


def column_set_hash(file_columns: list[str]) -> str:
    """Stable signature for a file column set.

    Normalized + sorted + deduped. Order, case, separator, and duplicate columns
    do NOT affect the hash. Dtype is excluded by design.
    """
    normalized = sorted({normalize(c) for c in file_columns})
    payload = ",".join(normalized).encode()
    return hashlib.sha256(payload).hexdigest()[:16]
