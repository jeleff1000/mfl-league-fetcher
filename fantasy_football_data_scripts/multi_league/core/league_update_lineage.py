"""Resolve the one provider leg eligible for an active-season update.

Full multi-platform imports persist provider ownership per season in
``league_settings``.  A weekly update extends only the active provider leg;
older legs remain canonical Fly history and are never re-fetched.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass


_SUPPORTED_PLATFORMS = frozenset({"yahoo", "espn", "sleeper"})


class ActiveUpdateSegmentError(RuntimeError):
    """The saved timeline cannot safely select exactly one active provider."""


@dataclass(frozen=True, slots=True)
class ActiveUpdateSegment:
    """The active provider leg and its persisted renewal chain."""

    platform: str
    current_league_id: str | None
    league_ids: dict[str, str]
    historical_platforms: tuple[str, ...]


def _platform(value: object) -> str:
    return str(value or "").strip().lower()


def _league_id(value: object) -> str:
    return str(value or "").strip()


def resolve_active_update_segment(
    *,
    active_year: int,
    context_platform: object,
    context_league_id: object,
    settings_rows: Iterable[Mapping[str, object]],
    expected_platform: str | None = None,
) -> ActiveUpdateSegment:
    """Select one active leg from the saved import timeline.

    If the new season has not yet been persisted, the full-import target
    platform is the only valid continuation.  Provider-specific workers pass
    ``expected_platform`` and therefore fail closed before any network fetch
    when the UI dispatches the wrong worker.
    """

    by_year: dict[int, dict[str, str]] = {}
    platforms: set[str] = set()
    for row in settings_rows:
        try:
            year = int(row.get("year"))
        except (TypeError, ValueError):
            continue
        platform = _platform(row.get("platform"))
        league_id = _league_id(row.get("league_key"))
        if platform not in _SUPPORTED_PLATFORMS or not league_id:
            continue
        existing = by_year.setdefault(year, {})
        if platform in existing and existing[platform] != league_id:
            raise ActiveUpdateSegmentError(
                f"saved {platform} renewal chain has conflicting IDs for {year}"
            )
        existing[platform] = league_id
        platforms.add(platform)

    active_owners = by_year.get(int(active_year), {})
    if len(active_owners) > 1:
        raise ActiveUpdateSegmentError(
            f"active season {active_year} has multiple providers: {', '.join(sorted(active_owners))}"
        )

    persisted_platform = _platform(context_platform)
    if persisted_platform not in _SUPPORTED_PLATFORMS:
        raise ActiveUpdateSegmentError("saved import target has no supported provider platform")
    platform = next(iter(active_owners), persisted_platform)
    requested = _platform(expected_platform)
    if requested and requested != platform:
        raise ActiveUpdateSegmentError(
            f"active season {active_year} belongs to {platform}; expected {requested}"
        )
    if platform != persisted_platform:
        # Full imports persist their chosen target provider in league_context.
        # Current-year rows from a misrouted prior update are not authority to
        # change that target and append an unrelated provider's league chain.
        raise ActiveUpdateSegmentError(
            f"active season {active_year} provider {platform} conflicts with saved import target "
            f"{persisted_platform}; reconcile the imported league chain before updating"
        )

    league_ids = {
        str(year): values[platform]
        for year, values in sorted(by_year.items())
        if platform in values
    }
    current_league_id = active_owners.get(platform)
    historical_platforms = tuple(sorted(platforms - {platform}))
    return ActiveUpdateSegment(
        platform=platform,
        current_league_id=current_league_id,
        league_ids=league_ids,
        historical_platforms=historical_platforms,
    )
