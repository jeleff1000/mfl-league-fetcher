"""
Base Normalizer Abstract Class

This module provides the abstract base class for platform-specific normalizers.
Both YahooNormalizer and SleeperNormalizer extend this class to ensure:
1. Consistent method signatures across platforms
2. Shared utility methods for composite keys and validation
3. Enforcement of canonical output schemas

Usage:
    class YahooNormalizer(BaseNormalizer):
        @property
        def platform(self) -> str:
            return 'yahoo'

        def normalize_player_data(self, df, league_id) -> pd.DataFrame:
            # Yahoo-specific normalization
            ...
"""

import logging
from abc import ABC, abstractmethod
import pandas as pd

from .canonical_columns import (
    COLUMN_ALIASES,
)

logger = logging.getLogger(__name__)


class NormalizationResult:
    """Result of a normalization operation."""

    def __init__(
        self,
        df: pd.DataFrame,
        warnings: list[str] | None = None,
        errors: list[str] | None = None,
    ):
        self.df = df
        self.warnings = warnings or []
        self.errors = errors or []

    @property
    def success(self) -> bool:
        """True if no errors occurred."""
        return len(self.errors) == 0

    @property
    def row_count(self) -> int:
        """Number of rows in result."""
        return len(self.df) if self.df is not None else 0


class BaseNormalizer(ABC):
    """
    Abstract base class for platform-specific data normalizers.

    Subclasses must implement:
    - platform property: Returns 'yahoo' or 'sleeper'
    - normalize_player_data(): Normalize roster/player data
    - normalize_matchup_data(): Normalize matchup data
    - normalize_draft_data(): Normalize draft data
    - normalize_transaction_data(): Normalize transaction data

    Provided utilities:
    - _add_composite_keys(): Add player_week, cumulative_week
    - _validate_required_columns(): Check for missing columns
    - _resolve_column_aliases(): Rename aliased columns to canonical names
    - _add_platform_identifier(): Add platform and league_id columns
    """

    def __init__(self, validate_output: bool = True):
        """
        Initialize normalizer.

        Args:
            validate_output: If True, validate output schema after normalization
        """
        self.validate_output = validate_output
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    @abstractmethod
    def platform(self) -> str:
        """
        Return platform identifier.

        Returns:
            'yahoo' or 'sleeper'
        """
        pass

    @abstractmethod
    def normalize_player_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize roster/player data to canonical schema.

        Args:
            df: Raw player data from fetcher
            league_id: Platform-specific league ID

        Returns:
            DataFrame with canonical column names and types
        """
        pass

    @abstractmethod
    def normalize_matchup_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize matchup data to canonical schema.

        Args:
            df: Raw matchup data from fetcher
            league_id: Platform-specific league ID

        Returns:
            DataFrame with canonical column names and types
        """
        pass

    @abstractmethod
    def normalize_draft_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize draft data to canonical schema.

        Args:
            df: Raw draft data from fetcher
            league_id: Platform-specific league ID

        Returns:
            DataFrame with canonical column names and types
        """
        pass

    @abstractmethod
    def normalize_transaction_data(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Normalize transaction data to canonical schema.

        Args:
            df: Raw transaction data from fetcher
            league_id: Platform-specific league ID

        Returns:
            DataFrame with canonical column names and types
        """
        pass

    # -------------------------------------------------------------------------
    # Shared Utility Methods
    # -------------------------------------------------------------------------

    def _add_composite_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add composite key columns to DataFrame.

        Adds:
        - player_week: {NFL_player_id}_{year}_{week}
        - player_year: {NFL_player_id}_{year}
        - cumulative_week: year * 100 + week

        Args:
            df: DataFrame with year, week, and NFL_player_id columns

        Returns:
            DataFrame with composite key columns added
        """
        if df.empty:
            return df

        result = df.copy()

        # player_week = {NFL_player_id}_{year}_{week}
        if all(col in result.columns for col in ["NFL_player_id", "year", "week"]):
            result["player_week"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip()
                + "_"
                + result["year"].fillna(0).astype(int).astype(str)
                + "_"
                + result["week"].fillna(0).astype(int).astype(str)
            )

        # player_year = {NFL_player_id}_{year}
        if all(col in result.columns for col in ["NFL_player_id", "year"]):
            result["player_year"] = (
                result["NFL_player_id"].fillna("").astype(str).str.strip()
                + "_"
                + result["year"].fillna(0).astype(int).astype(str)
            )

        # cumulative_week = year * 100 + week
        if all(col in result.columns for col in ["year", "week"]):
            result["cumulative_week"] = result["year"].fillna(0).astype(int) * 100 + result["week"].fillna(0).astype(
                int
            )

        return result

    def _add_platform_identifier(
        self,
        df: pd.DataFrame,
        league_id: str,
    ) -> pd.DataFrame:
        """
        Add platform and league_id columns.

        Args:
            df: DataFrame to modify
            league_id: Platform-specific league ID

        Returns:
            DataFrame with platform and league_id columns
        """
        if df.empty:
            return df

        result = df.copy()
        result["platform"] = self.platform
        result["league_id"] = str(league_id)
        return result

    def _resolve_column_aliases(
        self,
        df: pd.DataFrame,
        rename_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """
        Rename aliased columns to canonical names.

        Uses COLUMN_ALIASES mapping and optional custom rename_map.

        Args:
            df: DataFrame with potentially aliased columns
            rename_map: Optional additional column mappings {old: new}

        Returns:
            DataFrame with canonical column names
        """
        if df.empty:
            return df

        result = df.copy()

        # Build rename mapping from aliases
        auto_renames = {}
        for canonical, aliases in COLUMN_ALIASES.items():
            if canonical not in result.columns:
                for alias in aliases:
                    if alias in result.columns:
                        auto_renames[alias] = canonical
                        break

        # Apply auto renames
        if auto_renames:
            result = result.rename(columns=auto_renames)
            self.logger.debug(f"Auto-renamed columns: {auto_renames}")

        # Apply custom rename map
        if rename_map:
            actual_renames = {k: v for k, v in rename_map.items() if k in result.columns and k != v}
            if actual_renames:
                result = result.rename(columns=actual_renames)
                self.logger.debug(f"Custom renamed columns: {actual_renames}")

        return result

    def _add_backward_compatibility_aliases(
        self,
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Add backward compatibility column aliases.

        Creates copies of canonical columns with legacy names for UI compatibility:
        - player -> player_name
        - position -> yahoo_position
        - fantasy_position -> roster_position
        - points -> team_points

        Args:
            df: DataFrame with canonical columns

        Returns:
            DataFrame with alias columns added
        """
        if df.empty:
            return df

        result = df.copy()

        # Add common aliases
        aliases = [
            ("player", "player_name"),
            ("position", "yahoo_position"),
            ("fantasy_position", "roster_position"),
            ("points", "team_points"),
            ("pick", "pick_number"),
            ("cost", "keeper_cost"),
        ]

        for canonical, alias in aliases:
            if canonical in result.columns and alias not in result.columns:
                result[alias] = result[canonical]

        return result

    def _validate_required_columns(
        self,
        df: pd.DataFrame,
        required: list[str],
        table_name: str,
    ) -> list[str]:
        """
        Validate DataFrame has required columns.

        Args:
            df: DataFrame to validate
            required: List of required column names
            table_name: Name of table (for error messages)

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        for col in required:
            if col not in df.columns:
                errors.append(f"{table_name} missing required column: {col}")

        if errors:
            self.logger.warning(f"Validation errors for {table_name}: {errors}")

        return errors

    def _ensure_column_types(
        self,
        df: pd.DataFrame,
        type_specs: dict[str, str],
    ) -> pd.DataFrame:
        """
        Convert columns to specified types.

        Args:
            df: DataFrame to convert
            type_specs: Dict mapping column names to pandas dtype strings

        Returns:
            DataFrame with converted column types
        """
        if df.empty:
            return df

        result = df.copy()

        for col, dtype in type_specs.items():
            if col not in result.columns:
                continue

            try:
                if dtype == "Int64":
                    # Handle nullable integer
                    result[col] = pd.to_numeric(result[col], errors="coerce")
                    result[col] = result[col].astype("Int64")
                elif dtype == "float64":
                    result[col] = pd.to_numeric(result[col], errors="coerce")
                elif dtype == "string":
                    result[col] = result[col].astype(str).replace("nan", pd.NA)
                elif dtype == "bool":
                    # Handle various boolean representations
                    result[col] = result[col].map(lambda x: bool(x) if pd.notna(x) else None)
            except Exception as e:
                self.logger.warning(f"Failed to convert {col} to {dtype}: {e}")

        return result

    def _ensure_null_for_platform_columns(
        self,
        df: pd.DataFrame,
        null_columns: list[str],
    ) -> pd.DataFrame:
        """
        Ensure specified columns exist with NULL values.

        Used for platform-specific columns that don't exist on this platform
        (e.g., Yahoo ADP columns for Sleeper data).

        Args:
            df: DataFrame to modify
            null_columns: List of column names to ensure exist as NULL

        Returns:
            DataFrame with null columns added
        """
        if df.empty:
            return df

        result = df.copy()

        for col in null_columns:
            if col not in result.columns:
                result[col] = None

        return result

    def _compute_is_started(
        self,
        df: pd.DataFrame,
        bench_values: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Compute is_started column from fantasy_position.

        A player is started if their fantasy_position is NOT in bench_values.

        Args:
            df: DataFrame with fantasy_position column
            bench_values: List of bench position values (default: ['BN', 'IR', 'IL', 'TAXI'])

        Returns:
            DataFrame with is_started column
        """
        if df.empty or "fantasy_position" not in df.columns:
            return df

        if bench_values is None:
            bench_values = ["BN", "IR", "IL", "TAXI", "RES", "BENCH"]

        result = df.copy()
        result["is_started"] = ~result["fantasy_position"].str.upper().isin([v.upper() for v in bench_values])

        return result

    def _log_normalization_summary(
        self,
        table_name: str,
        input_rows: int,
        output_rows: int,
        columns_renamed: dict[str, str] | None = None,
    ):
        """
        Log a summary of normalization operation.

        Args:
            table_name: Name of table being normalized
            input_rows: Number of input rows
            output_rows: Number of output rows
            columns_renamed: Optional dict of column renames performed
        """
        self.logger.info(f"Normalized {table_name}: {input_rows} -> {output_rows} rows " f"(platform={self.platform})")
        if columns_renamed:
            self.logger.debug(f"Columns renamed: {columns_renamed}")
