"""Shared manager identity helpers."""

from __future__ import annotations

import pandas as pd


HIDDEN_MANAGER_GUID_TOKENS = frozenset({"", "--", "--hidden--", "none", "nan", "<na>", "n/a", "null"})


def normalize_manager_guid_token(value) -> str:
    """Return a lowercase token for placeholder/hidden GUID checks."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower()


def is_hidden_manager_guid(value) -> bool:
    """True when a manager GUID is missing or a platform placeholder."""
    return normalize_manager_guid_token(value) in HIDDEN_MANAGER_GUID_TOKENS


def hidden_manager_guid_mask(series: pd.Series) -> pd.Series:
    """Vectorized hidden/placeholder manager GUID mask."""
    return series.isna() | series.astype(str).str.strip().str.lower().isin(HIDDEN_MANAGER_GUID_TOKENS)


def hidden_manager_owner_id(manager_name: str, team_name: str | None = None) -> str:
    """Build the pseudo-owner ID used for hidden Yahoo managers."""
    manager_slug = str(manager_name or "unknown").lower().replace(" ", "_")
    if team_name is None:
        return f"hidden_{manager_slug}"
    team_slug = str(team_name or "unknown").strip().lower().replace(" ", "_")[:20] or "unknown"
    return f"hidden_{manager_slug}_{team_slug}"
