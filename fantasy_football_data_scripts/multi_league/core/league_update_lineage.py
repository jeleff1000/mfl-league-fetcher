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


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def assert_canonical_history_complete(
    reader: object,
    *,
    database_name: str,
    active_season: int,
) -> dict[str, object]:
    """Reject an update when canonical history is stranded under a prior slug.

    League renames retain ``league_db`` as the canonical pointer in inventory.
    If a partial rename leaves old seasons under the former ``database_name``,
    an active-only refresh would otherwise rebuild all-time rollups from only
    the current season. This small manifest check runs before provider fetch
    or publication and fails closed until the existing server-side league
    merge repairs the split.
    """

    canonical = str(database_name).strip()
    if not canonical:
        raise ActiveUpdateSegmentError("canonical database name is missing")
    canonical_sql = _sql_literal(canonical)
    alias_rows = reader.query(
        f"""
        WITH target AS (
            SELECT platform, league_id
            FROM accounts.league_inventory
            WHERE database_name = {canonical_sql}
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        )
        SELECT DISTINCT inventory.database_name
        FROM accounts.league_inventory AS inventory
        JOIN target
          ON COALESCE(NULLIF(TRIM(inventory.platform), ''), '__missing__') =
             COALESCE(NULLIF(TRIM(target.platform), ''), '__missing__')
         AND COALESCE(NULLIF(TRIM(CAST(inventory.league_id AS VARCHAR)), ''), '__missing__') =
             COALESCE(NULLIF(TRIM(CAST(target.league_id AS VARCHAR)), ''), '__missing__')
        WHERE inventory.database_name <> {canonical_sql}
          AND inventory.league_db = {canonical_sql}
        ORDER BY inventory.database_name
        """,
        database="___ops",
    )
    aliases = sorted(
        {
            str(row.get("database_name") or "").strip()
            for row in alias_rows
            if str(row.get("database_name") or "").strip()
        }
    )
    if not aliases:
        return {
            "canonical_db": canonical,
            "legacy_databases": [],
            "historical_years_verified": [],
        }

    databases = [canonical, *aliases]
    db_list_sql = ", ".join(_sql_literal(value) for value in databases)
    year_rows = reader.query(
        "SELECT db_name, TRY_CAST(year AS INTEGER) AS year "
        "FROM ___leagues.public.matchup "
        f"WHERE db_name IN ({db_list_sql}) AND TRY_CAST(year AS INTEGER) < {int(active_season)} "
        "GROUP BY db_name, TRY_CAST(year AS INTEGER) "
        "ORDER BY db_name, year",
        database="___leagues",
    )
    years_by_db: dict[str, set[int]] = {value: set() for value in databases}
    for row in year_rows:
        db_name = str(row.get("db_name") or "").strip()
        year = row.get("year")
        if db_name in years_by_db and year is not None:
            years_by_db[db_name].add(int(year))

    canonical_years = years_by_db[canonical]
    missing_by_alias = {
        alias: sorted(years_by_db[alias] - canonical_years)
        for alias in aliases
        if years_by_db[alias] - canonical_years
    }
    if missing_by_alias:
        details = "; ".join(
            f"{alias}: {', '.join(str(year) for year in years)}"
            for alias, years in sorted(missing_by_alias.items())
        )
        raise ActiveUpdateSegmentError(
            f"canonical league history is split before {active_season}; "
            f"repair the server-side league merge before updating ({details})"
        )

    verified = sorted(set().union(*(years_by_db[alias] for alias in aliases)))
    return {
        "canonical_db": canonical,
        "legacy_databases": aliases,
        "historical_years_verified": verified,
    }


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
