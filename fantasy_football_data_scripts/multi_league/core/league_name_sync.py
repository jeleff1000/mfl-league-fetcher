"""Keep `league_context.league_name` in step with the platform's own name.

Why this exists
---------------
Both Yahoo and ESPN seed the import context with a league name supplied at
signup, and neither refreshed it from the platform afterwards. Two failures
follow:

1. A name that was wrong at signup stays wrong forever. ESPN was worst hit --
   its database name is DERIVED from the supplied name, so 30 of 117 leagues
   (25.6%) ended up displaying their own slug ("a_league_of_their_own").
2. A name that was right goes stale. Leagues get renamed between seasons
   ("U CaN'T HanDLe THiS Season 23"), and we never noticed.

THE ORDERING HAZARD
-------------------
`fetch_runtime._resolve_db_name()` falls back to `_sanitize_database_name(
league_name)` when `database_name` is unset. Renaming an unpinned context
therefore RETARGETS THE IMPORT. For a league whose database carries a
disambiguation suffix (`any_given_sunday_40d008`), the sanitized real name
collapses to `any_given_sunday` -- a DIFFERENT league's database, so the import
would write one league's data over another's.

`adopt_api_league_name` pins the database first and refuses to rename if it
cannot. Never reorder those two steps.
"""
from __future__ import annotations

import re
from typing import Any, Callable

# A value that is really a database slug rather than a name.
_SLUG_SHAPED = re.compile(r"^[a-z0-9_]+$")


def looks_like_slug(value: str | None) -> bool:
    """True when a stored 'name' is really a slug (or missing).

    NOTE: this cannot distinguish a slug from a genuine lowercase one-word name.
    Yahoo and ESPN both confirmed real leagues called "xii", "skankasaurus",
    "beerleague" and "footballers". Use this to decide whether a REPLACEMENT is
    an improvement, never to decide that existing data is broken.
    """
    text = (value or "").strip()
    return not text or bool(_SLUG_SHAPED.match(text))


def adopt_api_league_name(
    ctx: Any,
    api_name: str | None,
    *,
    log: Callable[[str], None] | None = None,
) -> bool:
    """Adopt the platform's own league name onto `ctx`. Returns True if changed.

    Pins `ctx.database_name` before renaming so the import cannot be retargeted
    (see module docstring). Declines when the incoming name is blank, is itself
    slug-shaped, or already matches.
    """
    emit = log or (lambda _msg: None)
    incoming = (api_name or "").strip()
    if not incoming:
        return False

    current = str(getattr(ctx, "league_name", "") or "").strip()
    if incoming == current:
        return False
    if looks_like_slug(incoming):
        return False  # no better than what we hold

    # THE STORED NAME IS CANONICAL. Users rename a league in Settings, which
    # sets both the name and the slug, and that choice must survive every later
    # import. So this only ever FILLS A GAP -- a name we never captured -- and
    # never overwrites a real one. A league renamed on the platform keeps the
    # name it has here until someone changes it in Settings. (Joe, 2026-07-19.)
    if not looks_like_slug(current):
        return False

    # Pin the database so it cannot follow the name.
    if not getattr(ctx, "database_name", None):
        try:
            # Deferred: avoids a package-level import cycle during startup.
            from multi_league.core.fetch_runtime import runtime_from_source

            ctx.database_name = runtime_from_source(ctx).db_name
        except Exception as exc:  # cannot pin -> renaming is unsafe
            emit(f"  [NAME] Could not pin database name, keeping '{current}' ({exc})")
            return False

    ctx.league_name = incoming
    emit(f"  [NAME] Platform league name: '{current}' -> '{incoming}' (database {ctx.database_name})")
    return True


def adopt_from_yearly_settings(
    ctx: Any,
    settings_by_year: dict[int, dict],
    *,
    key: str,
    nested: str | None = None,
    log: Callable[[str], None] | None = None,
) -> bool:
    """Adopt the name from the most recent season present.

    Newest year wins because that is the league's current name.

    Args:
        key: the settings key holding the name ("league_name" for ESPN,
             "name" for Yahoo).
        nested: optional sub-dict to look inside (Yahoo nests under "metadata").
    """
    if not settings_by_year:
        return False
    for year in sorted(settings_by_year, reverse=True):
        blob = settings_by_year[year] or {}
        if nested:
            blob = blob.get(nested) or {}
        candidate = str(blob.get(key) or "").strip()
        if candidate:
            return adopt_api_league_name(ctx, candidate, log=log)
    return False
