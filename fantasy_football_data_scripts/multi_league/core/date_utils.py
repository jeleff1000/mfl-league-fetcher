"""
Date utilities for NFL season calculations.

Primary method: Use Sleeper API's /state/nfl endpoint for authoritative season info.
Fallback method: Date-based calculation when API is unavailable.

The NFL season year follows this pattern:
- The "2025 NFL season" starts in September 2025
- It ends with the Super Bowl in February 2026
- So in January/February 2026, we're still in the "2025 season"
"""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Cache for NFL state to avoid repeated API calls
_nfl_state_cache: dict[str, Any] = {}
_cache_timestamp: datetime | None = None
_CACHE_TTL_SECONDS = 3600  # 1 hour


def _get_nfl_state_from_api() -> dict[str, Any] | None:
    """
    Fetch NFL state from Sleeper API.

    Returns:
        Dict with season info or None if API unavailable.
    """
    global _nfl_state_cache, _cache_timestamp

    # Check cache
    if _cache_timestamp and _nfl_state_cache:
        age = (datetime.now() - _cache_timestamp).total_seconds()
        if age < _CACHE_TTL_SECONDS:
            return _nfl_state_cache

    try:
        # Import here to avoid circular imports
        from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient

        client = SleeperAPIClient()
        state = client.get_nfl_state()
        if state and "season" in state:
            _nfl_state_cache = state
            _cache_timestamp = datetime.now()
            return state
    except Exception as e:
        logger.debug(f"Could not fetch NFL state from API: {e}")

    return None


def get_current_nfl_season_year(reference_date: datetime = None) -> int:
    """
    Get the current NFL season year.

    Primary: Uses Sleeper API's /state/nfl endpoint (authoritative).
    Fallback: Date-based calculation when API is unavailable.

    Args:
        reference_date: Date to calculate from (only used for fallback).
                       If provided, skips API and uses date calculation.

    Returns:
        The NFL season year (e.g., 2025 for the 2025 NFL season)

    Examples:
        - API returns season="2025" -> 2025
        - December 15, 2025 (fallback) -> 2025
        - January 18, 2026 (fallback) -> 2025
    """
    # If reference_date provided, use date-based calculation (for testing/historical)
    if reference_date is not None:
        return _calculate_season_from_date(reference_date)

    # Try API first (authoritative source)
    state = _get_nfl_state_from_api()
    if state and "season" in state:
        try:
            return int(state["season"])
        except (ValueError, TypeError):
            pass

    # Fallback to date-based calculation
    return _calculate_season_from_date(datetime.now())


def _calculate_season_from_date(reference_date: datetime) -> int:
    """
    Calculate NFL season year from a date.

    The NFL season that starts in September 2025 is the "2025 season".
    The Super Bowl for that season happens in February 2026.

    Args:
        reference_date: Date to calculate from.

    Returns:
        The NFL season year.
    """
    year = reference_date.year
    month = reference_date.month

    # September-December: current year is the NFL season
    # January-August: previous year is the NFL season
    if month >= 9:  # September or later
        return year
    else:  # January through August
        return year - 1


def get_nfl_state() -> dict[str, Any] | None:
    """
    Get full NFL state from Sleeper API.

    Returns:
        Dict with keys like 'season', 'week', 'season_type', etc.
        Or None if API unavailable.

    Example response:
        {
            "week": 2,
            "season": "2025",
            "season_type": "post",
            "league_season": "2026",
            ...
        }
    """
    return _get_nfl_state_from_api()


def get_current_nfl_week() -> int | None:
    """
    Get the current NFL week from Sleeper API.

    Returns:
        Current week number (1-18 for regular season, 1-4 for playoffs)
        Or None if API unavailable.
    """
    state = _get_nfl_state_from_api()
    if state and "week" in state:
        return state.get("week")
    return None


def get_nfl_season_type() -> str | None:
    """
    Get the current NFL season type from Sleeper API.

    Returns:
        'pre' (preseason), 'regular', 'post' (playoffs), or 'off' (offseason)
        Or None if API unavailable.
    """
    state = _get_nfl_state_from_api()
    if state and "season_type" in state:
        return state.get("season_type")
    return None


def get_nfl_season_year_for_date(date: datetime) -> int:
    """
    Get NFL season year for a specific date (uses date calculation, not API).

    Args:
        date: The date to calculate the NFL season for.

    Returns:
        The NFL season year for that date.
    """
    return _calculate_season_from_date(date)


def is_nfl_regular_season(reference_date: datetime = None) -> bool:
    """
    Check if we're currently in the NFL regular season.

    Uses API if available, otherwise date-based approximation.

    Args:
        reference_date: Date to check. Defaults to now.

    Returns:
        True if in regular season.
    """
    if reference_date is None:
        # Try API first
        season_type = get_nfl_season_type()
        if season_type is not None:
            return season_type == "regular"

        reference_date = datetime.now()

    # Fallback to date-based approximation
    month = reference_date.month
    # Regular season: September through December, plus first week or two of January
    return month >= 9 or month == 1


def is_nfl_offseason(reference_date: datetime = None) -> bool:
    """
    Check if we're currently in the NFL offseason.

    Uses API if available, otherwise date-based approximation.

    Args:
        reference_date: Date to check. Defaults to now.

    Returns:
        True if in offseason (Feb - Aug).
    """
    if reference_date is None:
        # Try API first
        season_type = get_nfl_season_type()
        if season_type is not None:
            return season_type == "off"

        reference_date = datetime.now()

    # Fallback to date-based approximation
    month = reference_date.month
    return 2 <= month <= 8


def clear_nfl_state_cache():
    """Clear the cached NFL state (useful for testing)."""
    global _nfl_state_cache, _cache_timestamp
    _nfl_state_cache = {}
    _cache_timestamp = None
