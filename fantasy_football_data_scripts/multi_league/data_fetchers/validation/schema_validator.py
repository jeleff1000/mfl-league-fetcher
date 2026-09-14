"""
Schema Validator

Validates DataFrame output against canonical schema definitions.
Ensures all required columns are present with correct types before
data is passed to downstream transformations.

Usage:
    validator = SchemaValidator()

    # Validate player data
    result = validator.validate_player_data(df)
    if not result.is_valid:
        for error in result.errors:
            print(f"ERROR: {error}")

    # Or use convenience method that raises on error
    validator.validate_or_raise(df, 'player')
"""

import logging
from dataclasses import dataclass, field
import pandas as pd

from ..base.canonical_columns import (
    PLAYER_REQUIRED_COLUMNS,
    MATCHUP_REQUIRED_COLUMNS,
    DRAFT_REQUIRED_COLUMNS,
    TRANSACTION_REQUIRED_COLUMNS,
    PLAYER_COLUMN_TYPES,
    MATCHUP_COLUMN_TYPES,
    DRAFT_COLUMN_TYPES,
    TRANSACTION_COLUMN_TYPES,
)

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when schema validation fails."""

    def __init__(self, errors: list[str], table_name: str = "unknown"):
        self.errors = errors
        self.table_name = table_name
        message = f"Schema validation failed for {table_name}:\n" + "\n".join(f"  - {e}" for e in errors)
        super().__init__(message)


@dataclass
class ValidationResult:
    """Result of a schema validation operation."""

    table_name: str
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    row_count: int = 0
    column_count: int = 0

    def __str__(self) -> str:
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"ValidationResult({self.table_name}): {status}, "
            f"{self.row_count} rows, {self.column_count} columns, "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        )


class SchemaValidator:
    """
    Validates DataFrame output against canonical schema.

    Performs checks for:
    - Required columns presence
    - Column data types
    - Value range constraints
    - Referential integrity (optional)
    """

    # Value range constraints
    CONSTRAINTS = {
        "fantasy_points": {"min": -50, "max": 200},
        "year": {"min": 1920, "max": 2100},
        "week": {"min": 0, "max": 25},
        "pick": {"min": 1, "max": 500},
        "round": {"min": 1, "max": 50},
        "cost": {"min": 0, "max": 10000},
    }

    def __init__(
        self,
        strict_mode: bool = False,
        warn_on_suspicious: bool = True,
    ):
        """
        Initialize validator.

        Args:
            strict_mode: If True, treat warnings as errors
            warn_on_suspicious: If True, warn on suspicious but not invalid values
        """
        self.strict_mode = strict_mode
        self.warn_on_suspicious = warn_on_suspicious
        self.logger = logging.getLogger(f"{__name__}.SchemaValidator")

    def validate_player_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate player/roster data against canonical schema.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with errors and warnings
        """
        return self._validate(
            df=df,
            table_name="player_fantasy",
            required_columns=PLAYER_REQUIRED_COLUMNS,
            column_types=PLAYER_COLUMN_TYPES,
        )

    def validate_matchup_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate matchup data against canonical schema.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with errors and warnings
        """
        return self._validate(
            df=df,
            table_name="matchup",
            required_columns=MATCHUP_REQUIRED_COLUMNS,
            column_types=MATCHUP_COLUMN_TYPES,
        )

    def validate_draft_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate draft data against canonical schema.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with errors and warnings
        """
        return self._validate(
            df=df,
            table_name="draft",
            required_columns=DRAFT_REQUIRED_COLUMNS,
            column_types=DRAFT_COLUMN_TYPES,
        )

    def validate_transaction_data(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate transaction data against canonical schema.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with errors and warnings
        """
        return self._validate(
            df=df,
            table_name="transactions",
            required_columns=TRANSACTION_REQUIRED_COLUMNS,
            column_types=TRANSACTION_COLUMN_TYPES,
        )

    def validate_or_raise(
        self,
        df: pd.DataFrame,
        table_type: str,
    ) -> pd.DataFrame:
        """
        Validate DataFrame and raise exception if invalid.

        Args:
            df: DataFrame to validate
            table_type: One of 'player', 'matchup', 'draft', 'transaction'

        Returns:
            Original DataFrame if valid

        Raises:
            ValidationError: If validation fails
        """
        validators = {
            "player": self.validate_player_data,
            "matchup": self.validate_matchup_data,
            "draft": self.validate_draft_data,
            "transaction": self.validate_transaction_data,
        }

        if table_type not in validators:
            raise ValueError(f"Unknown table type: {table_type}")

        result = validators[table_type](df)

        if not result.is_valid:
            raise ValidationError(result.errors, result.table_name)

        return df

    def _validate(
        self,
        df: pd.DataFrame,
        table_name: str,
        required_columns: list[str],
        column_types: dict[str, str],
    ) -> ValidationResult:
        """
        Internal validation implementation.

        Args:
            df: DataFrame to validate
            table_name: Name of table for error messages
            required_columns: List of required column names
            column_types: Dict of column name -> expected dtype

        Returns:
            ValidationResult
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Handle empty DataFrame
        if df is None:
            errors.append(f"{table_name}: DataFrame is None")
            return ValidationResult(
                table_name=table_name,
                is_valid=False,
                errors=errors,
            )

        if df.empty:
            warnings.append(f"{table_name}: DataFrame is empty")
            return ValidationResult(
                table_name=table_name,
                is_valid=True,  # Empty is valid but suspicious
                warnings=warnings,
                row_count=0,
                column_count=len(df.columns),
            )

        # Check required columns
        missing_cols = self._check_required_columns(df, required_columns)
        errors.extend(f"{table_name} missing required column: {col}" for col in missing_cols)

        # Check column types
        type_errors = self._check_column_types(df, column_types)
        errors.extend(f"{table_name}.{col}: expected {expected}, got {actual}" for col, expected, actual in type_errors)

        # Check value ranges
        range_issues = self._check_value_ranges(df)
        for col, issue_type, count, detail in range_issues:
            msg = f"{table_name}.{col}: {issue_type} ({count} rows) - {detail}"
            if issue_type == "out_of_range":
                errors.append(msg)
            else:
                warnings.append(msg)

        # Check for NULLs in required columns
        null_issues = self._check_null_values(df, required_columns)
        for col, null_count, total in null_issues:
            pct = (null_count / total) * 100
            msg = f"{table_name}.{col}: {null_count}/{total} ({pct:.1f}%) NULL values"
            if col in ["player_week", "NFL_player_id", "manager"]:
                errors.append(msg)
            else:
                warnings.append(msg)

        # Check for duplicates in key columns
        dupe_issues = self._check_duplicates(df, table_name)
        errors.extend(dupe_issues)

        # Determine validity
        is_valid = len(errors) == 0
        if self.strict_mode and warnings:
            is_valid = False
            errors.extend(f"[STRICT] {w}" for w in warnings)

        result = ValidationResult(
            table_name=table_name,
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            row_count=len(df),
            column_count=len(df.columns),
        )

        # Log result
        if is_valid:
            self.logger.info(str(result))
        else:
            self.logger.warning(str(result))

        return result

    def _check_required_columns(
        self,
        df: pd.DataFrame,
        required: list[str],
    ) -> list[str]:
        """Check for missing required columns."""
        return [col for col in required if col not in df.columns]

    def _check_column_types(
        self,
        df: pd.DataFrame,
        type_specs: dict[str, str],
    ) -> list[tuple]:
        """Check column types match specifications."""
        issues = []

        for col, expected_dtype in type_specs.items():
            if col not in df.columns:
                continue

            actual_dtype = str(df[col].dtype)

            # Type compatibility checks
            if expected_dtype == "Int64":
                if not pd.api.types.is_integer_dtype(df[col]):
                    if not pd.api.types.is_float_dtype(df[col]):
                        issues.append((col, expected_dtype, actual_dtype))
            elif expected_dtype == "float64":
                if not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append((col, expected_dtype, actual_dtype))
            elif expected_dtype == "string":
                if not (pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col])):
                    issues.append((col, expected_dtype, actual_dtype))
            elif expected_dtype == "bool":
                if not pd.api.types.is_bool_dtype(df[col]):
                    issues.append((col, expected_dtype, actual_dtype))

        return issues

    def _check_value_ranges(
        self,
        df: pd.DataFrame,
    ) -> list[tuple]:
        """Check values are within expected ranges."""
        issues = []

        for col, constraints in self.CONSTRAINTS.items():
            if col not in df.columns:
                continue

            col_data = pd.to_numeric(df[col], errors="coerce")

            # Check minimum
            if "min" in constraints:
                below_min = col_data < constraints["min"]
                if below_min.any():
                    count = below_min.sum()
                    min_val = col_data[below_min].min()
                    issues.append((col, "out_of_range", count, f"values below {constraints['min']} (min: {min_val})"))

            # Check maximum
            if "max" in constraints:
                above_max = col_data > constraints["max"]
                if above_max.any():
                    count = above_max.sum()
                    max_val = col_data[above_max].max()
                    issues.append((col, "suspicious", count, f"values above {constraints['max']} (max: {max_val})"))

        return issues

    def _check_null_values(
        self,
        df: pd.DataFrame,
        required_columns: list[str],
    ) -> list[tuple]:
        """Check for NULL values in columns."""
        issues = []

        for col in required_columns:
            if col not in df.columns:
                continue

            null_count = df[col].isna().sum()
            if null_count > 0:
                issues.append((col, null_count, len(df)))

        return issues

    def _check_duplicates(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> list[str]:
        """Check for duplicate rows based on key columns."""
        issues = []

        # Define key columns for each table type
        key_columns = {
            "player_fantasy": ["player_week"],
            "matchup": ["year", "week", "manager", "opponent"],
            "draft": ["year", "pick"],
            "transactions": ["transaction_id", "player"],
        }

        keys = key_columns.get(table_name, [])
        available_keys = [k for k in keys if k in df.columns]

        if available_keys:
            dupes = df.duplicated(subset=available_keys, keep=False)
            dupe_count = dupes.sum()
            if dupe_count > 0:
                issues.append(f"{table_name}: {dupe_count} duplicate rows on {available_keys}")

        return issues
