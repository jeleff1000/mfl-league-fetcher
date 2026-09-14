"""
Base Fetcher Classes

Abstract base classes for data fetchers that provide common patterns
for fetching data week-by-week and handling phantom weeks.
"""

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING
import logging

import pandas as pd

if TYPE_CHECKING:
    from .base_client import BasePlatformClient
    from ..base.base_context import ContextProtocol

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """
    Abstract base class for data fetchers.

    Provides common patterns for fetching data week-by-week,
    handling phantom weeks, and saving output files.

    The fetch_for_year() method is a template method that handles
    week iteration, phantom week detection, and error handling.
    Subclasses implement _fetch_week_data() for platform-specific logic.

    Example:
        class SleeperRosterFetcher(BaseRosterFetcher):
            def _fetch_week_data(self, league_id, year, week, roster_map):
                matchups = self.client.get_week_matchups(league_id, week)
                # Parse matchups into roster entries...
                return roster_entries
    """

    def __init__(self, ctx: "ContextProtocol", client: "BasePlatformClient"):
        """
        Initialize the fetcher.

        Args:
            ctx: Context with league configuration and directory paths
            client: Platform API client for making requests
        """
        self.ctx = ctx
        self.client = client
        self.consecutive_empty_weeks = 0
        self.max_empty_weeks = 3  # Stop after this many empty weeks

    @property
    @abstractmethod
    def data_type(self) -> str:
        """Data type: 'roster', 'matchup', 'draft', 'transaction'"""

    def fetch_for_year(self, year: int) -> pd.DataFrame:
        """
        Fetch data for a full season.

        Template method that handles week iteration, phantom week
        detection, and error handling. Subclasses implement
        _fetch_week_data() for platform-specific logic.

        Args:
            year: Season year to fetch

        Returns:
            DataFrame with fetched data, or empty DataFrame if no data
        """
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            self._log(f"No league ID found for year {year}")
            return pd.DataFrame()

        self._log(f"Fetching {self.data_type} data for {year}, league_id={league_id}")

        roster_map = self.client.get_roster_mappings(league_id)
        all_data = []
        self.consecutive_empty_weeks = 0

        for week in range(1, 23):  # Cover regular + playoffs
            try:
                week_data = self._fetch_week_data(league_id, year, week, roster_map)

                if self._is_phantom_week(week_data):
                    self.consecutive_empty_weeks += 1
                    self._log(f"  Week {week}: empty/phantom week")
                    if self.consecutive_empty_weeks >= self.max_empty_weeks:
                        self._log(f"  {self.max_empty_weeks}+ consecutive empty weeks - stopping")
                        break
                    continue

                self.consecutive_empty_weeks = 0
                all_data.extend(week_data)
                self._log(f"  Week {week}: {len(week_data)} entries")

            except Exception as e:
                self._log(f"  Error fetching week {week}: {e}", level="warning")
                continue

        if not all_data:
            self._log(f"No {self.data_type} data found for {year}")
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        self._log(f"Total {self.data_type} entries for {year}: {len(df)}")
        return df

    @abstractmethod
    def _fetch_week_data(
        self, league_id: str, year: int, week: int, roster_map: dict[Any, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Fetch data for a specific week.

        Platform-specific implementation that fetches and parses
        data for a single week.

        Args:
            league_id: Platform-specific league identifier
            year: Season year
            week: Week number (1-indexed)
            roster_map: Mapping of roster_id to manager info

        Returns:
            List of data entries as dicts
        """

    def _is_phantom_week(self, week_data: list[dict[str, Any]]) -> bool:
        """
        Check if week has no real data (all zeros or empty).

        Args:
            week_data: List of data entries for the week

        Returns:
            True if the week should be skipped (phantom/empty)
        """
        if not week_data:
            return True

        # Check if all points are 0 (phantom week)
        points_key = self._get_points_key()
        if points_key:
            total = sum(d.get(points_key, 0) or 0 for d in week_data)
            if total == 0:
                return True

        return False

    def _get_points_key(self) -> str | None:
        """Return the key for points in this data type."""
        return {
            "roster": "points",
            "matchup": "team_points",
            "draft": None,
            "transaction": None,
        }.get(self.data_type)

    def _log(self, message: str, level: str = "info"):
        """Log a message."""
        getattr(logger, level)(message)
        print(message)


class BaseRosterFetcher(BaseFetcher):
    """Base class for roster/player data fetchers."""

    @property
    def data_type(self) -> str:
        return "roster"


class BaseMatchupFetcher(BaseFetcher):
    """Base class for matchup data fetchers."""

    @property
    def data_type(self) -> str:
        return "matchup"


class BaseDraftFetcher(BaseFetcher):
    """
    Base class for draft data fetchers.

    Draft data doesn't iterate weekly, so this overrides the
    template method to fetch all picks at once.
    """

    @property
    def data_type(self) -> str:
        return "draft"

    def fetch_for_year(self, year: int) -> pd.DataFrame:
        """
        Fetch draft data for a year.

        Overrides parent template since draft data is fetched
        all at once, not week by week.

        Args:
            year: Season year

        Returns:
            DataFrame with draft picks
        """
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            self._log(f"No league ID found for year {year}")
            return pd.DataFrame()

        self._log(f"Fetching draft data for {year}, league_id={league_id}")

        draft = self._get_draft_for_year(league_id, year)
        if not draft:
            self._log(f"No draft found for {year}")
            return pd.DataFrame()

        df = self._parse_draft(draft, year)
        self._log(f"Total draft picks for {year}: {len(df)}")
        return df

    def _fetch_week_data(
        self, league_id: str, year: int, week: int, roster_map: dict[Any, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """Not used for draft fetcher - raises error if called."""
        raise NotImplementedError("Draft fetcher doesn't use _fetch_week_data")

    @abstractmethod
    def _get_draft_for_year(self, league_id: str, year: int) -> dict[str, Any] | None:
        """
        Get draft object for a specific year.

        Args:
            league_id: Platform-specific league identifier
            year: Season year

        Returns:
            Draft dict or None if not found
        """

    @abstractmethod
    def _parse_draft(self, draft: dict[str, Any], year: int) -> pd.DataFrame:
        """
        Parse draft data into DataFrame.

        Args:
            draft: Raw draft data from API
            year: Season year

        Returns:
            DataFrame with parsed draft picks
        """


class BaseTransactionFetcher(BaseFetcher):
    """Base class for transaction data fetchers."""

    @property
    def data_type(self) -> str:
        return "transaction"
