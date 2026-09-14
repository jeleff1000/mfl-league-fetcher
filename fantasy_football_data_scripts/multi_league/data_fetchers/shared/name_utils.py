"""
Consolidated player name normalization.

This is the SINGLE SOURCE OF TRUTH for name normalization across all platforms.
Replaces 10 independent normalize_name() implementations.

Operations (in order):
1. Handle None/NaN -> empty string
2. Lowercase + strip
3. Remove accents (Unicode NFKD decomposition)
4. Remove suffixes (Jr, Sr, II, III, IV, V)
5. Remove punctuation (keep spaces and word chars)
6. Collapse whitespace
"""

import re
import unicodedata

import pandas as pd

_SUFFIXES = frozenset(
    {
        " jr",
        " sr",
        " iii",
        " iv",
        " v",
        " ii",
        " jr.",
        " sr.",
    }
)


def normalize_name(name) -> str:
    """Normalize a player name for matching across platforms.

    Args:
        name: Raw player name (str, None, or NaN)

    Returns:
        Normalized lowercase name with no accents, suffixes, or punctuation.
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    try:
        if pd.isna(name):
            return ""
    except (TypeError, ValueError):
        pass

    s = str(name).lower().strip()
    if not s:
        return ""

    # Remove accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))

    # Remove suffixes (longest match first to avoid partial matches)
    for suffix in sorted(_SUFFIXES, key=len, reverse=True):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break

    # Remove punctuation (keep word chars and spaces)
    s = re.sub(r"[^\w\s]", "", s)

    # Collapse whitespace
    s = " ".join(s.split())

    return s


def apply_name_aliases(name: str, aliases: dict[str, str] | None = None) -> str:
    """Apply name aliases after normalization. Sleeper-specific post-processing.

    Args:
        name: Already-normalized player name
        aliases: Dict mapping normalized names to canonical names

    Returns:
        Aliased name if match found, otherwise input unchanged.
    """
    if aliases and name in aliases:
        return aliases[name]
    return name


# ---------------------------------------------------------------------------
# Shared parsing primitives
# ---------------------------------------------------------------------------


def safe_float(text, default=None):
    """Convert text to float, returning default on error.

    Canonical implementation — do NOT duplicate this in platform fetchers.
    """
    try:
        if text is None:
            return default
        s = str(text).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default
