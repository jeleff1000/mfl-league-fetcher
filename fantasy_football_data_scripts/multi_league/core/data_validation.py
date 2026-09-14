"""
Data Validation Module

Provides fail-fast validation checkpoints for the data pipeline.
Validates data integrity at critical points to catch issues early.

Usage:
    from multi_league.core.data_validation import (
        ValidationError,
        validate_checkpoint,
        validate_post_fetch,
        validate_post_merge,
        validate_post_transform,
    )

    # Validate after data fetch
    validate_post_fetch(player_df, "yahoo_fantasy_data")

    # Validate after merge
    validate_post_merge(merged_df, "yahoo_nfl_merge")

    # Validate after transformation
    validate_post_transform(transformed_df, "player_stats_v2")
"""

import pandas as pd
from typing import Any
from dataclasses import dataclass

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

# Import logging with fallbacks
try:
    from multi_league.core.logging_config import get_logger
except ImportError:
    try:
        from core.logging_config import get_logger
    except ImportError:
        # Fallback to a simple logger if logging_config isn't available
        import logging

        def get_logger(name):
            logger = logging.getLogger(name)
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
                logger.addHandler(handler)
                logger.setLevel(logging.INFO)
            return logger


logger = get_logger(__name__)


class ValidationError(Exception):
    """
    Raised when data validation fails in fail-fast mode.

    Attributes:
        checkpoint: Name of the checkpoint that failed
        rule_name: Name of the validation rule that failed
        details: Additional details about the failure
    """

    def __init__(self, checkpoint: str, rule_name: str, details: str):
        self.checkpoint = checkpoint
        self.rule_name = rule_name
        self.details = details
        super().__init__(f"Validation failed at '{checkpoint}' - {rule_name}: {details}")


@dataclass
class ValidationResult:
    """Result of a validation check."""

    passed: bool
    rule_name: str
    message: str
    details: dict[str, Any] | None = None


# =============================================================================
# Validation Rules
# =============================================================================


def _check_non_empty(df: pd.DataFrame, name: str) -> ValidationResult:
    """Check that DataFrame is not empty."""
    if df.empty:
        return ValidationResult(
            passed=False,
            rule_name="non_empty",
            message=f"DataFrame '{name}' is empty",
        )
    return ValidationResult(
        passed=True,
        rule_name="non_empty",
        message=f"DataFrame has {len(df):,} rows",
    )


def _check_required_columns(df: pd.DataFrame, columns: list[str], name: str) -> ValidationResult:
    """Check that required columns are present."""
    missing = [col for col in columns if col not in df.columns]
    if missing:
        return ValidationResult(
            passed=False,
            rule_name="required_columns",
            message=f"Missing required columns: {missing}",
            details={"missing": missing, "available": list(df.columns)},
        )
    return ValidationResult(
        passed=True,
        rule_name="required_columns",
        message=f"All {len(columns)} required columns present",
    )


def _check_no_null_keys(df: pd.DataFrame, key_columns: list[str], name: str) -> ValidationResult:
    """Check that key columns have no null values."""
    null_counts = {}
    for col in key_columns:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                null_counts[col] = int(null_count)

    if null_counts:
        return ValidationResult(
            passed=False,
            rule_name="no_null_keys",
            message=f"Null values in key columns: {null_counts}",
            details={"null_counts": null_counts},
        )
    return ValidationResult(
        passed=True,
        rule_name="no_null_keys",
        message="No null values in key columns",
    )


def _check_unique_keys(df: pd.DataFrame, key_columns: list[str], name: str) -> ValidationResult:
    """Check that composite key is unique."""
    existing_keys = [col for col in key_columns if col in df.columns]
    if not existing_keys:
        return ValidationResult(
            passed=True,
            rule_name="unique_keys",
            message="No key columns to check",
        )

    duplicates = df.duplicated(subset=existing_keys, keep=False)
    dup_count = duplicates.sum()

    if dup_count > 0:
        # Get sample duplicates for debugging
        sample_dups = df[duplicates].head(5)[existing_keys].to_dict("records")
        return ValidationResult(
            passed=False,
            rule_name="unique_keys",
            message=f"{dup_count:,} duplicate key combinations found",
            details={"duplicate_count": dup_count, "sample": sample_dups},
        )
    return ValidationResult(
        passed=True,
        rule_name="unique_keys",
        message=f"All {len(df):,} rows have unique keys",
    )


def _check_value_range(df: pd.DataFrame, column: str, min_val: float, max_val: float, name: str) -> ValidationResult:
    """Check that values are within expected range."""
    if column not in df.columns:
        return ValidationResult(
            passed=True,
            rule_name="value_range",
            message=f"Column '{column}' not present, skipping range check",
        )

    out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
    if len(out_of_range) > 0:
        return ValidationResult(
            passed=False,
            rule_name="value_range",
            message=f"{len(out_of_range):,} values outside range [{min_val}, {max_val}]",
            details={
                "column": column,
                "out_of_range_count": len(out_of_range),
                "actual_min": float(df[column].min()),
                "actual_max": float(df[column].max()),
            },
        )
    return ValidationResult(
        passed=True,
        rule_name="value_range",
        message=f"All values in [{min_val}, {max_val}]",
    )


def _check_row_count(df: pd.DataFrame, min_rows: int, max_rows: int | None, name: str) -> ValidationResult:
    """Check that row count is within expected range."""
    row_count = len(df)

    if row_count < min_rows:
        return ValidationResult(
            passed=False,
            rule_name="row_count",
            message=f"Only {row_count:,} rows (minimum: {min_rows:,})",
        )

    if max_rows is not None and row_count > max_rows:
        return ValidationResult(
            passed=False,
            rule_name="row_count",
            message=f"{row_count:,} rows exceeds maximum ({max_rows:,})",
        )

    return ValidationResult(
        passed=True,
        rule_name="row_count",
        message=f"Row count {row_count:,} is valid",
    )


# =============================================================================
# Checkpoint Validators
# =============================================================================


def validate_checkpoint(
    df: pd.DataFrame,
    checkpoint_name: str,
    rules: list[dict[str, Any]],
    fail_fast: bool = True,
) -> list[ValidationResult]:
    """
    Validate DataFrame against a list of rules.

    Args:
        df: DataFrame to validate
        checkpoint_name: Name of this checkpoint (for error messages)
        rules: List of rule dictionaries, each with:
            - "type": Rule type (non_empty, required_columns, no_null_keys,
                     unique_keys, value_range, row_count)
            - Additional params depending on rule type
        fail_fast: If True, raise ValidationError on first failure

    Returns:
        List of ValidationResult objects

    Raises:
        ValidationError: If fail_fast=True and any rule fails
    """
    results = []

    for rule in rules:
        rule_type = rule.get("type")
        result = None

        if rule_type == "non_empty":
            result = _check_non_empty(df, checkpoint_name)

        elif rule_type == "required_columns":
            result = _check_required_columns(df, rule.get("columns", []), checkpoint_name)

        elif rule_type == "no_null_keys":
            result = _check_no_null_keys(df, rule.get("columns", []), checkpoint_name)

        elif rule_type == "unique_keys":
            result = _check_unique_keys(df, rule.get("columns", []), checkpoint_name)

        elif rule_type == "value_range":
            result = _check_value_range(
                df,
                rule.get("column", ""),
                rule.get("min", float("-inf")),
                rule.get("max", float("inf")),
                checkpoint_name,
            )

        elif rule_type == "row_count":
            result = _check_row_count(
                df,
                rule.get("min", 0),
                rule.get("max"),
                checkpoint_name,
            )

        if result:
            results.append(result)

            if not result.passed:
                logger.warning(f"Validation failed at {checkpoint_name}: " f"{result.rule_name} - {result.message}")
                if fail_fast:
                    raise ValidationError(checkpoint_name, result.rule_name, result.message)
            else:
                logger.debug(f"Validation passed: {result.rule_name} - {result.message}")

    return results


# =============================================================================
# Pre-configured Checkpoints
# =============================================================================


def validate_post_fetch(
    df: pd.DataFrame,
    source_name: str,
    required_columns: list[str] | None = None,
    fail_fast: bool = True,
) -> list[ValidationResult]:
    """
    Validate data immediately after fetching from an external source.

    Checks:
    - DataFrame is not empty
    - Required columns are present

    Args:
        df: Fetched DataFrame
        source_name: Name of data source (e.g., "yahoo_fantasy_data")
        required_columns: List of columns that must be present
        fail_fast: Raise on first failure

    Returns:
        List of validation results
    """
    rules = [{"type": "non_empty"}]

    if required_columns:
        rules.append({"type": "required_columns", "columns": required_columns})

    return validate_checkpoint(df, f"post_fetch_{source_name}", rules, fail_fast)


def validate_post_merge(
    df: pd.DataFrame,
    merge_name: str,
    key_columns: list[str] | None = None,
    check_uniqueness: bool = True,
    fail_fast: bool = True,
) -> list[ValidationResult]:
    """
    Validate data after merging datasets.

    Checks:
    - DataFrame is not empty
    - No null values in key columns
    - Key combinations are unique (if check_uniqueness=True)

    Args:
        df: Merged DataFrame
        merge_name: Name of merge operation (e.g., "yahoo_nfl_merge")
        key_columns: Columns that form the composite key
        check_uniqueness: Whether to check for duplicate keys
        fail_fast: Raise on first failure

    Returns:
        List of validation results
    """
    rules = [{"type": "non_empty"}]

    if key_columns:
        rules.append({"type": "no_null_keys", "columns": key_columns})
        if check_uniqueness:
            rules.append({"type": "unique_keys", "columns": key_columns})

    return validate_checkpoint(df, f"post_merge_{merge_name}", rules, fail_fast)


def validate_post_transform(
    df: pd.DataFrame,
    transform_name: str,
    expected_columns: list[str] | None = None,
    min_rows: int = 1,
    fail_fast: bool = True,
) -> list[ValidationResult]:
    """
    Validate data after a transformation.

    Checks:
    - DataFrame has minimum expected rows
    - Expected output columns are present

    Args:
        df: Transformed DataFrame
        transform_name: Name of transformation (e.g., "player_stats_v2")
        expected_columns: Columns the transformation should have added
        min_rows: Minimum expected row count
        fail_fast: Raise on first failure

    Returns:
        List of validation results
    """
    rules = [{"type": "row_count", "min": min_rows}]

    if expected_columns:
        rules.append({"type": "required_columns", "columns": expected_columns})

    return validate_checkpoint(df, f"post_transform_{transform_name}", rules, fail_fast)


def validate_canonical_file(
    df: pd.DataFrame,
    file_name: str,
    fail_fast: bool = True,
) -> list[ValidationResult]:
    """
    Validate a canonical output file (player.parquet, matchup.parquet, etc.).

    Applies file-specific validation rules.

    Args:
        df: DataFrame to write to canonical file
        file_name: Canonical file name
        fail_fast: Raise on first failure

    Returns:
        List of validation results
    """
    # Define validation rules per canonical file
    file_rules = {
        "player.parquet": {
            "required": ["yahoo_player_id", "year", "week", "league_id"],
            "keys": ["yahoo_player_id", "year", "week", "league_id"],
            "min_rows": 100,
        },
        "matchup.parquet": {
            "required": ["year", "week", "manager", "league_id"],
            "keys": ["year", "week", "manager", "league_id"],
            "min_rows": 10,
        },
        "draft.parquet": {
            "required": ["yahoo_player_id", "year", "league_id"],
            "keys": ["yahoo_player_id", "year", "league_id"],
            "min_rows": 10,
        },
        "transactions.parquet": {
            "required": ["transaction_id", "year", "league_id"],
            "keys": ["transaction_id", "league_id"],
            "min_rows": 0,  # May be empty for new leagues
        },
    }

    config = file_rules.get(file_name, {})
    rules = [{"type": "non_empty"}]

    if config.get("required"):
        rules.append({"type": "required_columns", "columns": config["required"]})

    if config.get("keys"):
        rules.append({"type": "no_null_keys", "columns": config["keys"]})

    if config.get("min_rows", 0) > 0:
        rules.append({"type": "row_count", "min": config["min_rows"]})

    return validate_checkpoint(df, f"canonical_{file_name}", rules, fail_fast)


# =============================================================================
# Summary Functions
# =============================================================================


def summarize_validation_results(results: list[ValidationResult]) -> dict[str, Any]:
    """
    Summarize a list of validation results.

    Args:
        results: List of ValidationResult objects

    Returns:
        Dictionary with summary statistics
    """
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    return {
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "success_rate": passed / len(results) if results else 1.0,
        "failures": [{"rule": r.rule_name, "message": r.message} for r in results if not r.passed],
    }
