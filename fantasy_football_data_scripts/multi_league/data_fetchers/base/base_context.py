"""
Base Context Protocol

This module defines the protocol (interface) that both LeagueContext (Yahoo)
and SleeperContext must implement for duck-typing compatibility.

This allows factory functions and orchestrators to work with either context
type without knowing the specific platform at compile time.

Usage:
    from data_fetchers.base import ContextProtocol

    def process_league(ctx: ContextProtocol):
        # Works with both LeagueContext and SleeperContext
        print(f"Processing {ctx.league_name} on {ctx.platform}")
"""

from pathlib import Path
from typing import Protocol, Any, runtime_checkable


@runtime_checkable
class ContextProtocol(Protocol):
    """
    Protocol for league context objects.

    Both LeagueContext (Yahoo) and SleeperContext (Sleeper) implement this
    protocol, enabling platform-agnostic code.

    Required Attributes:
        league_id: Platform-specific league identifier
        league_name: Human-readable league name
        platform: 'yahoo' or 'sleeper'
        data_directory: Path to league data files
        start_year: First year of league history
        end_year: Last year (current season)

    Required Methods:
        get_league_id_for_year(): Get platform-specific ID for a given year
        get_year_range(): Get range of years to process

    Directory Properties:
        player_data_directory: Path to player data files
        matchup_data_directory: Path to matchup data files
        draft_data_directory: Path to draft data files
        transaction_data_directory: Path to transaction data files

    Canonical File Properties:
        canonical_player_file: Path to consolidated player.parquet
        canonical_matchup_file: Path to consolidated matchup.parquet
        canonical_draft_file: Path to consolidated draft.parquet
        canonical_transaction_file: Path to consolidated transactions.parquet
    """

    # Required attributes
    league_id: str
    league_name: str
    platform: str
    data_directory: Path
    start_year: int
    end_year: int | None

    # Required methods
    def get_league_id_for_year(self, year: int) -> str | None:
        """
        Get platform-specific league ID for a given year.

        Yahoo leagues get new IDs each year (e.g., 449.l.123456).
        Sleeper leagues keep the same ID across years.

        Args:
            year: Season year

        Returns:
            Platform-specific league ID, or None if year not available
        """
        ...

    def get_year_range(self) -> range:
        """
        Get range of years to process.

        Returns:
            range(start_year, end_year + 1)
        """
        ...

    # Directory properties
    @property
    def player_data_directory(self) -> Path:
        """Path to directory containing player/roster data files."""
        ...

    @property
    def matchup_data_directory(self) -> Path:
        """Path to directory containing matchup data files."""
        ...

    @property
    def draft_data_directory(self) -> Path:
        """Path to directory containing draft data files."""
        ...

    @property
    def transaction_data_directory(self) -> Path:
        """Path to directory containing transaction data files."""
        ...

    # Canonical file properties
    @property
    def canonical_player_file(self) -> Path:
        """Path to consolidated player_fantasy.parquet."""
        ...

    @property
    def canonical_matchup_file(self) -> Path:
        """Path to consolidated matchup.parquet."""
        ...

    @property
    def canonical_draft_file(self) -> Path:
        """Path to consolidated draft.parquet."""
        ...

    @property
    def canonical_transaction_file(self) -> Path:
        """Path to consolidated transactions.parquet."""
        ...


def is_yahoo_context(ctx: Any) -> bool:
    """
    Check if context is for Yahoo platform.

    Args:
        ctx: Context object to check

    Returns:
        True if Yahoo platform
    """
    platform = getattr(ctx, "platform", None)
    return platform == "yahoo" or platform is None  # Default to Yahoo for legacy


def is_sleeper_context(ctx: Any) -> bool:
    """
    Check if context is for Sleeper platform.

    Args:
        ctx: Context object to check

    Returns:
        True if Sleeper platform
    """
    return getattr(ctx, "platform", None) == "sleeper"


def is_fleaflicker_context(ctx: Any) -> bool:
    """
    Check if context is for Fleaflicker platform.

    Args:
        ctx: Context object to check

    Returns:
        True if Fleaflicker platform
    """
    return getattr(ctx, "platform", None) == "fleaflicker"


def get_platform(ctx: Any) -> str:
    """
    Get platform identifier from context.

    Args:
        ctx: Context object

    Returns:
        'yahoo' or 'sleeper'
    """
    return getattr(ctx, "platform", "yahoo")


def validate_context(ctx: Any) -> list[str]:
    """
    Validate that context has required attributes.

    Args:
        ctx: Context object to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    required_attrs = [
        "league_id",
        "league_name",
        "data_directory",
        "start_year",
    ]

    for attr in required_attrs:
        if not hasattr(ctx, attr):
            errors.append(f"Context missing required attribute: {attr}")
        elif getattr(ctx, attr) is None:
            errors.append(f"Context attribute is None: {attr}")

    # Validate data_directory exists
    if hasattr(ctx, "data_directory"):
        data_dir = ctx.data_directory
        if data_dir and not Path(data_dir).exists():
            errors.append(f"Data directory does not exist: {data_dir}")

    return errors
