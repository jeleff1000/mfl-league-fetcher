"""Map league settings to one of the 36 precomputed LAMAR variant keys.

The variant format is ``{size}_{roster}_{ppr}_{td}``, for example:
``12t_flx_half_4pt``.

These keys align with the precomputed ``lamar_*`` columns already present in
the shared super table.
"""

from __future__ import annotations

from typing import Any

_VALID_VARIANTS: frozenset[str] = frozenset(
    f"{size}_{roster}_{ppr}_{td}"
    for size in ("10t", "12t")
    for roster in ("flx", "sflx", "idp")
    for ppr in ("std", "half", "ppr")
    for td in ("4pt", "6pt")
)


def _coerce_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric, not bool")
    try:
        return int(float(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _coerce_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric, not bool")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "off", ""}:
            return False
    return bool(value)


def _bucket_ppr(ppr: float) -> str:
    """Round PPR to the nearest supported bucket.

    Ties favor the higher bucket for midpoints above 0.5 and the lower bucket
    for midpoints below 0.5, so 0.25 -> std and 0.75 -> ppr.
    """
    if ppr <= 0.25:
        return "std"
    if ppr < 0.75:
        return "half"
    return "ppr"


def derive_scoring_variant(
    num_teams: Any,
    has_superflex: Any,
    has_idp: Any,
    pass_td_pts: Any,
    ppr: Any,
    te_premium: Any = 0,
) -> str:
    """Map league settings to one of the 36 precomputed LAMAR variants."""
    teams = _coerce_int(12 if num_teams is None else num_teams, "num_teams")
    superflex = _coerce_bool(has_superflex)
    idp = _coerce_bool(has_idp)
    td_pts = _coerce_float(4 if pass_td_pts is None else pass_td_pts, "pass_td_pts")
    base_ppr = _coerce_float(0 if ppr is None else ppr, "ppr")
    _coerce_float(0 if te_premium is None else te_premium, "te_premium")  # Accepted for interface parity.

    size = "10t" if teams <= 11 else "12t"
    roster = "sflx" if superflex else "idp" if idp else "flx"
    td = "4pt" if td_pts <= 5 else "6pt"
    variant = f"{size}_{roster}_{_bucket_ppr(base_ppr)}_{td}"

    if variant not in _VALID_VARIANTS:
        raise ValueError(f"Invalid scoring variant generated: {variant}")

    return variant
