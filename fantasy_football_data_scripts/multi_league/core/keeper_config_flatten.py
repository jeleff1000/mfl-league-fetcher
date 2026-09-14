"""Convert nested rules_json blobs to flat column dicts for the new keeper_config DDL.

Pure Python — no DuckDB dependency. Per spec, validation happens here so we
do not rely on json_extract NULL-on-missing behavior.
"""

from __future__ import annotations

from typing import Any

from multi_league.core.keeper_config_schema import KEEPER_CONFIG_FIELD_COLUMNS

_FIXED_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "FLEX")


class UnknownPositionKeysError(ValueError):
    def __init__(self, unknown_keys: list[str]):
        super().__init__(f"Unknown position_limit keys: {unknown_keys}")
        self.unknown_keys = unknown_keys


def _g(blob: dict, *path, default=None):
    cur: Any = blob
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p, default)
        if cur is default:
            return default
    return cur


def _coerce_bool(v):
    return bool(v) if v is not None else None


def _coerce_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _coerce_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _coerce_str(v, allowed: tuple[str, ...] | None = None):
    if v is None:
        return None
    s = str(v)
    if allowed and s not in allowed:
        return None
    return s


def _pick_year2plus_formula(formulas: dict | None) -> dict:
    """Return the single year-2+ escalation formula entry, or empty dict."""
    if not isinstance(formulas, dict):
        return {}
    for key, val in formulas.items():
        s = str(key)
        if "+" in s or (s.isdigit() and int(s) >= 2):
            return val if isinstance(val, dict) else {}
    return {}


def flatten_rules_to_columns(blob: dict, *, strict: bool = True) -> dict:
    """Flatten a nested rules_json blob into the 40 field columns.

    Args:
        blob: parsed JSON dict (use {} for empty/missing rules_json)
        strict: if True, raises UnknownPositionKeysError on unknown position_limit keys.
                If False, drops them silently (used by migration script after logging).

    Returns:
        Dict with all 40 field columns; missing values are None.
    """
    blob = blob or {}
    out: dict = {col: None for col in KEEPER_CONFIG_FIELD_COLUMNS}

    # Top-level toggles
    out["enabled"] = _coerce_bool(_g(blob, "enabled"))
    out["draft_type"] = _coerce_str(_g(blob, "draft_type"), ("auction", "snake"))
    out["max_keepers"] = _coerce_int(_g(blob, "max_keepers"))
    out["max_years"] = _coerce_int(_g(blob, "max_years"))
    out["budget"] = _coerce_int(_g(blob, "budget"))
    out["min_price"] = _coerce_int(_g(blob, "min_price"))
    out["max_price"] = _coerce_int(_g(blob, "max_price"))
    out["num_rounds"] = _coerce_int(_g(blob, "num_rounds"))
    out["min_round"] = _coerce_int(_g(blob, "min_round"))
    out["round_up"] = _coerce_bool(_g(blob, "round_up"))

    # Snake base-cost
    out["snake_drafted_round_offset"] = _coerce_int(_g(blob, "base_cost_rules", "drafted", "round_offset"))
    out["snake_fa_pickup_source"] = _coerce_str(_g(blob, "base_cost_rules", "fa_pickup", "source"), ("last", "fixed"))
    out["snake_fa_pickup_round"] = _coerce_int(_g(blob, "base_cost_rules", "fa_pickup", "round"))

    # Auction base-cost
    out["auction_drafted_mult"] = _coerce_float(_g(blob, "base_cost_rules", "auction", "multiplier"))
    out["auction_drafted_flat"] = _coerce_int(_g(blob, "base_cost_rules", "auction", "flat"))
    out["auction_faab_mult"] = _coerce_float(_g(blob, "base_cost_rules", "faab_only", "multiplier"))
    out["auction_faab_flat"] = _coerce_int(_g(blob, "base_cost_rules", "faab_only", "flat"))
    out["auction_fa_value"] = _coerce_int(_g(blob, "base_cost_rules", "free_agent", "value"))

    # Year-2+ escalation
    esc = _pick_year2plus_formula(_g(blob, "formulas_by_keeper_year"))
    out["escalation_type"] = _coerce_str(esc.get("type"), ("compounding", "from_base", "round_escalation", "none"))
    out["escalation_mult"] = _coerce_float(esc.get("multiplier"))
    out["escalation_flat_add"] = _coerce_int(esc.get("flat_add"))
    out["escalation_flat_per_year"] = _coerce_int(esc.get("flat_per_year"))
    out["escalation_rounds_per_year"] = _coerce_int(esc.get("rounds_per_year"))

    # Eligibility
    out["fa_keepable"] = _coerce_bool(_g(blob, "fa_keepable"))
    out["restricted_rounds"] = _coerce_int(_g(blob, "restricted_rounds"))
    out["keeper_payroll_cap"] = _coerce_int(_g(blob, "keeper_payroll_cap"))

    # Trade rules
    out["trade_cost_follows_player"] = _coerce_bool(_g(blob, "trade_cost_follows_player"))
    out["trade_resets_years"] = _coerce_bool(_g(blob, "trade_resets_years"))

    # Franchise tag
    out["franchise_tag_enabled"] = _coerce_bool(_g(blob, "franchise_tag_enabled"))
    out["franchise_tag_cost_mult"] = _coerce_float(_g(blob, "franchise_tag_cost_mult"))

    # Rookie discount
    out["rookie_discount_enabled"] = _coerce_bool(_g(blob, "rookie_discount_enabled"))
    out["rookie_discount_rounds"] = _coerce_int(_g(blob, "rookie_discount_rounds"))
    out["rookie_discount_mult"] = _coerce_float(_g(blob, "rookie_discount_mult"))

    # Position limits — fixed slots
    pos = _g(blob, "position_limits")
    if isinstance(pos, dict):
        unknown = [k for k in pos if k not in _FIXED_POSITIONS]
        if unknown and strict:
            raise UnknownPositionKeysError(unknown)
        for slot in _FIXED_POSITIONS:
            out[f"position_limit_{slot.lower()}"] = _coerce_int(pos.get(slot))

    return out
