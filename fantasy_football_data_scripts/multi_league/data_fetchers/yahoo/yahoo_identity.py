"""Yahoo-only manager identity resolution.

Yahoo redacts the per-user ``guid`` to the literal string ``--hidden--`` for any
league the API token does not own. This is the norm for older seasons and for
leagues imported via a non-member token, so the franchise system's "rock solid"
``manager_guid`` anchor is unavailable. Worse, every Yahoo season is a *separate*
league (a fresh ``league_key`` each year), so ``team_id`` / ``manager_id`` are
join-order artifacts with no cross-year meaning, and team names change yearly.

Without intervention the downstream franchise derivation collapses every
hidden-guid team to ``--hidden--`` and then re-splits it by the meaningless team
slot, producing franchise_ids like ``--hidden--_3`` that conflate many unrelated
managers across many years.

This module synthesizes a *stable, per-person* ``manager_guid`` for hidden-guid
Yahoo rows, using confidence tiers (first match wins):

1. **Real guid** — kept as-is (modern, owned leagues).
2. **Non-default profile image** — ``yh-img-<hash>``. The Yahoo profile image URL
   embeds a stable per-account token that survives guid redaction. This is the
   key cross-year anchor for ancient seasons. The default placeholder image is
   shared by every photo-less / deleted account, so it is treated as *no signal*.
3. **Unredacted nickname** — ``yh-nick-<normalized>``. For photo-less users with a
   distinct display name.
4. **Unresolved** — ``yh-team-<team_key>`` singleton (redacted nickname AND no
   usable image). Each becomes its own franchise, mergeable later via config.

A cross-year reconciliation pass merges a nickname's image-tier identity with its
nickname-tier identity *only when they never co-occur in the same season* — i.e.
one person who added/removed a profile photo mid-career. When the two identities
*do* co-occur in a season they are genuinely different people sharing a display
name (e.g. two "Danish"s), and are kept separate so the shared
``franchise_id = manager_guid`` + "Name - TeamName" disambiguation can split them.

Everything here is Yahoo-specific and is invoked only from the Yahoo normalizer;
it must never run on ESPN / Sleeper data.
"""

from __future__ import annotations

import re

import pandas as pd

# Import resilience: the worker runs the fetchers with a flattened sys.path where
# the package is rooted at ``core`` / ``data_fetchers`` (not ``multi_league.*``),
# so the relative import can fail. Fall back through the same roots the rest of
# the Yahoo fetchers use, and finally to an inline definition so identity
# resolution can NEVER be silently skipped due to an import error.
try:
    from ...core.manager_identity import is_hidden_manager_guid
except ImportError:  # pragma: no cover - exercised only in the flat worker layout
    try:
        from multi_league.core.manager_identity import is_hidden_manager_guid
    except ImportError:
        try:
            from core.manager_identity import is_hidden_manager_guid
        except ImportError:
            _HIDDEN_GUID_TOKENS = frozenset({"", "--", "--hidden--", "none", "nan", "<na>", "n/a", "null"})

            def is_hidden_manager_guid(value) -> bool:
                if value is None:
                    return True
                try:
                    if pd.isna(value):
                        return True
                except (TypeError, ValueError):
                    pass
                return str(value).strip().lower() in _HIDDEN_GUID_TOKENS


SYNTH_PREFIX = "yh"

# Tokens that mean "no usable nickname" (redacted / placeholder).
_HIDDEN_NICK_TOKENS = frozenset({"", "--", "--hidden--", "hidden", "none", "nan", "<na>", "n/a", "null"})


def image_identity_hash(url: object) -> str | None:
    """Return a stable per-account token from a Yahoo profile image URL.

    Yahoo profile images look like
    ``https://s.yimg.com/ag/images/4532/24494747418_36e9c1_64sq.jpg`` (path +
    id) or ``.../images/<uuid>_64sq.jpg``. The portion before ``_64sq`` is stable
    per account across years. The shared default placeholder carries no identity
    and yields ``None``.
    """
    if url is None:
        return None
    try:
        if pd.isna(url):
            return None
    except (TypeError, ValueError):
        pass
    text = str(url).strip()
    if not text or "default_user_profile" in text:
        return None
    match = re.search(r"images/(.+?)_\d+sq", text)
    token = match.group(1) if match else text
    token = re.sub(r"[^A-Za-z0-9]+", "-", token).strip("-").lower()
    return token or None


def _normalize_nickname(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return re.sub(r"[^A-Za-z0-9]+", "", str(value).strip().lower())


def _is_hidden_nick(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower() in _HIDDEN_NICK_TOKENS


def resolve_yahoo_manager_guids(
    df: pd.DataFrame,
    *,
    guid_col: str = "manager_guid",
    nickname_col: str = "manager_nickname_raw",
    image_col: str = "manager_image_url",
    team_key_col: str = "team_key",
    year_col: str = "year",
    manager_col: str = "manager",
) -> pd.Series:
    """Compute a stable synthetic ``manager_guid`` per row for hidden-guid Yahoo rows.

    Returns a Series aligned to ``df.index``. Rows that already carry a real
    (non-hidden) guid are returned unchanged. Only hidden-guid rows are rewritten.
    Pure function: does not mutate ``df``.
    """
    if df.empty:
        return pd.Series([], dtype="object", index=df.index)

    def col(name: str) -> pd.Series:
        if name in df.columns:
            return df[name]
        return pd.Series([None] * len(df), index=df.index)

    guid = col(guid_col)
    raw_nick = col(nickname_col)
    # Fall back to the display `manager` when the raw nickname column is absent.
    if nickname_col not in df.columns and manager_col in df.columns:
        raw_nick = df[manager_col]
    image = col(image_col)
    team_key = col(team_key_col)

    hidden_mask = guid.map(is_hidden_manager_guid)

    # ---- Tier assignment (per row, deterministic) ------------------------
    resolved: list[str | None] = []
    norm_nicks: list[str] = []
    for idx in df.index:
        g = guid.loc[idx]
        if not is_hidden_manager_guid(g):
            resolved.append(str(g).strip())
            norm_nicks.append("")
            continue
        nick_norm = _normalize_nickname(raw_nick.loc[idx])
        norm_nicks.append(nick_norm)
        img_hash = image_identity_hash(image.loc[idx])
        if img_hash:
            resolved.append(f"{SYNTH_PREFIX}-img-{img_hash}")
        elif nick_norm and not _is_hidden_nick(raw_nick.loc[idx]):
            resolved.append(f"{SYNTH_PREFIX}-nick-{nick_norm}")
        else:
            tk = team_key.loc[idx]
            tk = str(tk).strip() if tk is not None and str(tk).strip() else None
            resolved.append(f"{SYNTH_PREFIX}-team-{tk}" if tk else None)

    out = pd.Series(resolved, index=df.index, dtype="object")
    nick_series = pd.Series(norm_nicks, index=df.index, dtype="object")

    # ---- Cross-year reconciliation: one person who changed their photo ----
    # For each normalized nickname, if it has exactly one image-tier guid and one
    # nickname-tier guid AND they never appear together in the same season, they
    # are the same person (added/removed a profile photo) -> merge nick->img.
    # If they co-occur in any season they are different people sharing a name
    # (e.g. two "Danish"s) -> keep separate.
    if year_col in df.columns:
        work = pd.DataFrame(
            {
                "fid": out,
                "nick": nick_series,
                "year": df[year_col],
            }
        )
        merges: dict[str, str] = {}
        for nick, grp in work[hidden_mask & (nick_series != "")].groupby("nick"):
            img_guids = sorted({f for f in grp["fid"] if isinstance(f, str) and f.startswith(f"{SYNTH_PREFIX}-img-")})
            nick_guid = f"{SYNTH_PREFIX}-nick-{nick}"
            if len(img_guids) != 1 or nick_guid not in set(grp["fid"]):
                continue
            img_guid = img_guids[0]
            img_years = set(grp.loc[grp["fid"] == img_guid, "year"])
            nick_years = set(grp.loc[grp["fid"] == nick_guid, "year"])
            if img_years.isdisjoint(nick_years):
                merges[nick_guid] = img_guid
        if merges:
            out = out.map(lambda f: merges.get(f, f) if isinstance(f, str) else f)

    return out
