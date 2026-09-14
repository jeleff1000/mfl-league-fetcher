"""
Shared year filtering logic for import orchestrators.

Consolidates the year-range filtering scattered across Yahoo, Sleeper,
and ESPN orchestrators into a single module.
"""

from __future__ import annotations

from typing import Any


def coerce_int(value: Any) -> int | None:
    """Best-effort int conversion for workflow/API year values."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def calculate_year_filter(import_mode: str, end_year: int) -> int | None:
    """Return the year to filter to, or None for all years.

    Legacy helper for single-target quick fetchers. Full (or anything else) -> None.
    """
    return end_year if import_mode == "quick" else None


def resolve_history_years(
    league_ids: dict[str, Any] | dict[int, Any] | None,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
    cap_year: int | None = None,
    extra_years: list[int] | set[int] | tuple[int, ...] | None = None,
) -> list[int]:
    """Resolve concrete season years from mappings/ranges for quick import logic."""
    years: set[int] = set()

    for raw_year, league_id in dict(league_ids or {}).items():
        if league_id in (None, ""):
            continue
        year = coerce_int(raw_year)
        if year is None:
            continue
        if cap_year is not None and year > cap_year:
            continue
        years.add(year)

    if not years and start_year is not None and end_year is not None:
        start = coerce_int(start_year)
        end = coerce_int(end_year)
        if start is not None and end is not None and start <= end:
            for year in range(start, end + 1):
                if cap_year is None or year <= cap_year:
                    years.add(year)

    for raw_year in extra_years or ():
        year = coerce_int(raw_year)
        if year is None:
            continue
        if cap_year is not None and year > cap_year:
            continue
        years.add(year)

    return sorted(years)


def resolve_most_recent_available_year(
    league_ids: dict[str, Any] | dict[int, Any] | None,
    *,
    fallback_year: int | None = None,
    cap_year: int | None = None,
) -> int | None:
    """Resolve the latest usable season year from a league_ids mapping."""
    if not league_ids:
        return fallback_year

    candidate_years: list[int] = []
    for raw_year, league_id in dict(league_ids).items():
        if league_id in (None, ""):
            continue
        try:
            year = int(raw_year)
        except (TypeError, ValueError):
            continue
        if cap_year is not None and year > cap_year:
            continue
        candidate_years.append(year)

    if candidate_years:
        return max(candidate_years)
    return fallback_year


def resolve_quick_import_years(
    history_years: list[int] | set[int] | tuple[int, ...],
    target_year: int,
    nfl_state: dict[str, Any] | None = None,
    *,
    include_previous_available: bool = False,
) -> list[int]:
    """Return quick-import years, including a prior scored season for empty shells.

    Some platforms renew fantasy leagues into the upcoming NFL season before any
    games have scores. In that state, the new shell has settings and sometimes
    draft/transaction activity, while the previous season still has the most
    recent matchup and roster data. Yahoo can also expose historical shells with
    no played matchup data; importers may opt into the same two-year fallback.
    """
    target = int(target_year)
    years = sorted({int(year) for year in history_years if coerce_int(year) is not None})
    if not years:
        return [target]

    if target not in years:
        eligible_years = [year for year in years if year <= target]
        target = max(eligible_years) if eligible_years else max(years)

    selected = [target]
    state = nfl_state or {}
    state_season = coerce_int(state.get("league_season") or state.get("season"))
    previous_season = coerce_int(state.get("previous_season"))
    current_shell_has_scores = current_state_shell_has_scores(state)
    previous_available = max((year for year in years if year < target), default=None)

    if (
        state_season == target
        and not current_shell_has_scores
        and (previous_season in years or previous_available is not None)
    ):
        selected.insert(0, previous_season if previous_season in years else previous_available)
    elif include_previous_available and previous_available is not None:
        selected.insert(0, previous_available)

    return selected


def current_state_shell_has_scores(nfl_state: dict[str, Any] | None = None) -> bool:
    """Best-effort check for whether the current API season shell has scored weeks.

    Sleeper's ``season_has_scores`` can remain true in the offseason after it has
    advanced ``season`` to the next fantasy year. In that state ``week``/``leg``
    are still 0 and the new shell has draft/transaction activity but no matchup
    or roster scoring yet, so quick imports should also include the previous
    scored season.
    """
    state = nfl_state or {}
    if state.get("season_has_scores") is False:
        return False

    season_type = str(state.get("season_type") or "").lower()
    week = coerce_int(state.get("week"))
    leg = coerce_int(state.get("leg"))
    display_week = coerce_int(state.get("display_week"))

    if season_type == "off":
        return False
    if all(value is not None and value <= 0 for value in (week, leg, display_week)):
        return False

    return True


def unscored_current_shell_years(
    years: list[int] | set[int] | tuple[int, ...],
    nfl_state: dict[str, Any] | None = None,
) -> set[int]:
    """Return quick-import years that represent an unscored current shell."""
    state = nfl_state or {}
    state_season = coerce_int(state.get("league_season") or state.get("season"))
    if state_season is None or current_state_shell_has_scores(state):
        return set()
    return {int(year) for year in years if coerce_int(year) == state_season}


def format_year_filter(year_filter: int | list[int] | tuple[int, ...] | set[int] | None) -> str:
    """Human-readable year filter description for importer logs."""
    if isinstance(year_filter, (list, tuple, set)):
        return "years " + ", ".join(str(year) for year in sorted(int(y) for y in year_filter))
    if year_filter:
        return f"year {year_filter}"
    return "all years"


def filter_years_for_fetcher(
    all_years: list[int],
    year_filter: int | None = None,
    skip_years: set[int] | None = None,
    md_cache_years: set[int] | None = None,
) -> list[int]:
    """Given a full year range and various skip sets, return years to fetch.

    Args:
        all_years: Complete list of years the league spans.
        year_filter: If set, return ONLY this year (quick import).
        skip_years: Years with external/cached data — skip these.
        md_cache_years: Years already cached from a prior stage.

    Returns:
        Sorted list of years the fetcher should actually process.
    """
    if year_filter is not None:
        return [year_filter] if year_filter in all_years or not all_years else [year_filter]

    skip = set()
    if skip_years:
        skip |= skip_years
    if md_cache_years:
        skip |= md_cache_years

    return sorted(y for y in all_years if y not in skip)


def detect_import_mode(args_mode: str | None, ctx: object = None) -> str:
    """Normalize import mode from CLI args or context flags.

    Priority: explicit arg > ctx.is_single_year_import > default 'full'.
    """
    if args_mode:
        return args_mode
    if ctx is not None and getattr(ctx, "is_single_year_import", False):
        return "quick"
    return "full"
