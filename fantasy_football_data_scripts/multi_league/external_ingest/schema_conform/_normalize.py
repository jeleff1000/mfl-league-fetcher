"""Column-name and value normalization. Applied symmetrically to source AND reference."""

from __future__ import annotations
import re
import unicodedata
import pandas as pd

from ._config import ALIASES, IDENTITY_TOKENS


def _norm_col_name(name: str) -> str:
    """snake_case + lowercase + strip non-alnum runs."""
    return re.sub(r"[^a-z0-9]+", "_", str(name).strip().lower()).strip("_")


def _norm(value: str) -> str:
    """For manager-name comparison: NFKD, strip accents/emoji/suffixes, collapse to a-z0-9."""
    s = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\.?\b", "", s.lower())
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def _alias_norm(snake_name: str) -> str:
    """Token-level alias expansion. Context-sensitive 'id' → 'guid' when an identity
    token (manager, player, team, owner, mgr, gm) appears alongside.

    Without the guard, league_id / matchup_id / game_id would drift toward 'guid'
    and spuriously increase name_similarity against manager_guid / opponent_guid.
    """
    tokens = snake_name.split("_")
    has_identity = any(t in IDENTITY_TOKENS for t in tokens)
    out = []
    for t in tokens:
        if t == "id" and has_identity:
            out.append("guid")
        else:
            out.append(ALIASES.get(t, t))
    return "_".join(out)


# Strings that mean "no value" — treat as NULL during normalization. Pulling
# from Fly via VARCHAR-passthrough turns Python None into the literal string
# "None"; some platforms also export "Unknown", "N/A", etc. as placeholders.
_NULL_LITERALS = frozenset({"none", "null", "nan", "n/a", "na", "unknown", ""})


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename to snake_case, trim whitespace, and convert NULL-placeholder strings
    (None / Null / NaN / N/A / Unknown / empty) to actual NaN. Run before scoring.
    """
    out = df.copy()
    out.columns = [_norm_col_name(c) for c in out.columns]
    for c in out.select_dtypes(include="object").columns:
        s = out[c].astype(str).str.strip()
        # Mask null-literal strings (case-insensitive)
        mask = s.str.lower().isin(_NULL_LITERALS)
        out[c] = s.where(~mask, other=None)
    return out
