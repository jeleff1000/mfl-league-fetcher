"""Fuzzy lookup + derivation rules for IDENTITY slots not directly present in source."""

from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
from rapidfuzz import process, fuzz

from ._config import FUZZY_CUTOFF, FUZZY_GAP
from ._normalize import _norm


@dataclass
class FuzzyResult:
    value: str | None  # resolved value, or None if ambiguous/below-cutoff
    candidates: list[tuple[str, int]] = field(default_factory=list)  # [(name, score), ...]


def _fuzzy_lookup(normalized_name: str, mapping: dict) -> FuzzyResult:
    """Lookup with cutoff + ambiguity guard.

    Returns FuzzyResult.value = matched value if:
      - top score ≥ FUZZY_CUTOFF, AND
      - top score - runner_up_score ≥ FUZZY_GAP

    Otherwise FuzzyResult.value = None. Always includes top-2 candidates so the
    abort path can surface them to the user for disambiguation.
    """
    if not normalized_name or not mapping:
        return FuzzyResult(value=None, candidates=[])

    if normalized_name in mapping:
        return FuzzyResult(value=mapping[normalized_name], candidates=[(normalized_name, 100)])

    raw = process.extract(
        normalized_name,
        mapping.keys(),
        scorer=fuzz.ratio,
        limit=2,
    )
    if not raw:
        return FuzzyResult(value=None, candidates=[])
    candidates = [(name, int(score)) for name, score, _ in raw]
    top_name, top_score = candidates[0]

    if top_score < FUZZY_CUTOFF:
        return FuzzyResult(value=None, candidates=candidates)

    if len(candidates) > 1 and (top_score - candidates[1][1]) < FUZZY_GAP:
        return FuzzyResult(value=None, candidates=candidates)

    return FuzzyResult(value=mapping[top_name], candidates=candidates)


# Derivation rules: list of (rule_name, fn(row, ctx) -> value-or-None) per slot.
DERIVATION_RULES = {
    "manager_guid": [
        ("from_manager_name", lambda row, ctx: _fuzzy_lookup(_norm(row.get("manager", "")), ctx.name_to_guid).value),
        (
            "from_team_key",
            lambda row, ctx: ctx.team_key_to_guid.get(row.get("team_key")) if row.get("team_key") else None,
        ),
    ],
}


def apply_derivation(df: pd.DataFrame, slot: str, ctx) -> pd.Series:
    """Apply DERIVATION_RULES[slot] in order. First non-None wins; otherwise NaN."""
    rules = DERIVATION_RULES.get(slot, [])
    if not rules:
        return pd.Series([None] * len(df), index=df.index)

    def _resolve(row):
        for _rule_name, fn in rules:
            try:
                v = fn(row, ctx)
            except Exception:
                v = None
            if v is not None:
                return v
        return None

    return df.apply(_resolve, axis=1)
