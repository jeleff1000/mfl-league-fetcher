"""
Data Validation Module

Validates unified parquet outputs for consistency, completeness, and data quality.
Runs after merges complete to catch data issues before transformations.

Usage:
    from multi_league.data_fetchers.data_validator import validate_unified_outputs

    issues = validate_unified_outputs(
        ctx=league_context,
        log_func=print
    )
"""

from typing import Any
from collections.abc import Callable
import pandas as pd


class ValidationIssue:
    """Represents a data validation issue."""

    def __init__(self, severity: str, table: str, check: str, message: str, details: dict[str, Any] = None):
        """
        Args:
            severity: 'ERROR', 'WARNING', or 'INFO'
            table: Table name (e.g., 'player', 'matchup')
            check: Check name (e.g., 'row_count', 'null_check')
            message: Human-readable description
            details: Additional context (counts, examples, etc.)
        """
        self.severity = severity
        self.table = table
        self.check = check
        self.message = message
        self.details = details or {}

    def __repr__(self):
        return f"[{self.severity}] {self.table}.{self.check}: {self.message}"


def validate_unified_outputs(ctx, log_func: Callable = print, strict: bool = False) -> list[ValidationIssue]:
    """
    Validate all canonical parquet files for consistency and quality.

    Args:
        ctx: LeagueContext with paths to canonical files
        log_func: Logging function (default: print)
        strict: If True, treat warnings as errors

    Returns:
        List of ValidationIssue objects (empty if all checks pass)
    """
    issues = []

    # Define tables to validate
    tables = {
        "player": ctx.canonical_player_file,
        "matchup": ctx.canonical_matchup_file,
        "draft": ctx.canonical_draft_file,
        "transactions": ctx.canonical_transaction_file,
    }

    log_func("[VALIDATE] Starting validation checks...")

    for table_name, file_path in tables.items():
        log_func(f"\n[VALIDATE] Checking {table_name}.parquet...")

        # Check 1: File exists
        if not file_path.exists():
            issues.append(
                ValidationIssue(
                    severity="ERROR", table=table_name, check="file_exists", message=f"File not found: {file_path}"
                )
            )
            continue

        # Load data
        try:
            df = pd.read_parquet(file_path)
        except Exception as e:
            issues.append(
                ValidationIssue(
                    severity="ERROR",
                    table=table_name,
                    check="file_readable",
                    message=f"Failed to read parquet file: {e}",
                )
            )
            continue

        # Check 2: Not empty
        if df.empty:
            issues.append(
                ValidationIssue(
                    severity="ERROR", table=table_name, check="not_empty", message="Table is empty (0 rows)"
                )
            )
            continue

        log_func(f"  [OK] File exists: {len(df):,} rows, {len(df.columns)} columns")

        # Check 3: Required columns exist
        required_cols = get_required_columns(table_name)
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(
                ValidationIssue(
                    severity="ERROR",
                    table=table_name,
                    check="required_columns",
                    message=f"Missing required columns: {missing_cols}",
                    details={"missing": missing_cols},
                )
            )
        else:
            log_func(f"  [OK] Required columns present: {required_cols}")

        # Check 4: Year range validation
        if "year" in df.columns:
            min_year = df["year"].min()
            max_year = df["year"].max()

            if pd.notna(min_year) and pd.notna(max_year):
                # Check for reasonable year range
                if min_year < 1920 or max_year > 2030:
                    issues.append(
                        ValidationIssue(
                            severity="WARNING",
                            table=table_name,
                            check="year_range",
                            message=f"Unusual year range: {min_year}-{max_year}",
                            details={"min_year": int(min_year), "max_year": int(max_year)},
                        )
                    )
                else:
                    log_func(f"  [OK] Year range: {int(min_year)}-{int(max_year)}")

                # Check for gaps in years (league-specific tables only)
                if table_name in ["matchup", "draft", "transactions"]:
                    expected_years = set(range(ctx.start_year, ctx.end_year + 1))
                    actual_years = set(df["year"].dropna().astype(int).unique())
                    missing_years = expected_years - actual_years
                    if missing_years:
                        issues.append(
                            ValidationIssue(
                                severity="INFO",
                                table=table_name,
                                check="year_gaps",
                                message=f"Missing data for years: {sorted(missing_years)}",
                                details={"missing_years": sorted(missing_years)},
                            )
                        )

        # Check 5: Null value analysis (critical columns)
        critical_cols = get_critical_columns(table_name)
        for col in critical_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                null_pct = (null_count / len(df)) * 100

                if null_pct > 50:
                    issues.append(
                        ValidationIssue(
                            severity="WARNING",
                            table=table_name,
                            check="null_check",
                            message=f"High null rate in {col}: {null_pct:.1f}% ({null_count:,}/{len(df):,})",
                            details={"column": col, "null_count": null_count, "null_pct": null_pct},
                        )
                    )
                elif null_pct > 0:
                    log_func(f"  ⓘ {col}: {null_pct:.1f}% null ({null_count:,} rows)")

        # Check 6: Duplicate check (on primary keys)
        primary_keys = get_primary_keys(table_name)
        if primary_keys and all(pk in df.columns for pk in primary_keys):
            dup_count = df.duplicated(subset=primary_keys, keep=False).sum()
            if dup_count > 0:
                issues.append(
                    ValidationIssue(
                        severity="ERROR",
                        table=table_name,
                        check="duplicates",
                        message=f"Found {dup_count:,} duplicate rows on keys: {primary_keys}",
                        details={"keys": primary_keys, "duplicate_count": dup_count},
                    )
                )
            else:
                log_func(f"  [OK] No duplicates on primary keys: {primary_keys}")

        # Check 7: Data type validation
        type_issues = validate_column_types(df, table_name)
        issues.extend(type_issues)
        if not type_issues:
            log_func("  [OK] Column types valid")

        # Check 8: Value range validation
        range_issues = validate_value_ranges(df, table_name)
        issues.extend(range_issues)
        if not range_issues:
            log_func("  [OK] Value ranges valid")

        # Check 9: League ID consistency (multi-league safety)
        if "league_id" in df.columns:
            unique_leagues = df["league_id"].dropna().unique()
            if len(unique_leagues) > 1:
                issues.append(
                    ValidationIssue(
                        severity="ERROR",
                        table=table_name,
                        check="league_isolation",
                        message=f"Multiple league IDs found: {list(unique_leagues)} (data mixing!)",
                        details={"league_ids": list(unique_leagues)},
                    )
                )
            elif len(unique_leagues) == 1:
                log_func(f"  [OK] League ID consistent: {unique_leagues[0]}")

    # Summary
    log_func("\n[VALIDATE] Validation Summary:")
    errors = [i for i in issues if i.severity == "ERROR"]
    warnings = [i for i in issues if i.severity == "WARNING"]
    infos = [i for i in issues if i.severity == "INFO"]

    if errors:
        log_func(f"  [FAIL] {len(errors)} ERROR(S) found:")
        for issue in errors:
            log_func(f"    - {issue}")

    if warnings:
        log_func(f"  ⚠ {len(warnings)} WARNING(S) found:")
        for issue in warnings:
            log_func(f"    - {issue}")

    if infos:
        log_func(f"  ⓘ {len(infos)} INFO message(s):")
        for issue in infos:
            log_func(f"    - {issue}")

    if not issues:
        log_func("  [OK] All validation checks passed!")

    # Return issues (or raise if strict mode)
    if strict and (errors or warnings):
        raise ValueError(f"Validation failed: {len(errors)} errors, {len(warnings)} warnings")

    return issues


def get_required_columns(table_name: str) -> list[str]:
    """Get required columns for each table type."""
    required = {
        "player": ["year", "week", "player"],
        "matchup": ["year", "week", "manager", "team_name"],
        "draft": ["year", "yahoo_player_id"],
        "transactions": ["year", "transaction_id"],
    }
    return required.get(table_name, [])


def get_critical_columns(table_name: str) -> list[str]:
    """Get columns that should have minimal nulls."""
    critical = {
        "player": ["year", "week", "player", "yahoo_player_id"],
        "matchup": ["year", "week", "manager", "team_points", "opponent"],
        "draft": ["year", "yahoo_player_id", "player", "round", "pick"],
        "transactions": ["year", "transaction_id", "yahoo_player_id", "transaction_type"],
    }
    return critical.get(table_name, [])


def get_primary_keys(table_name: str) -> list[str]:
    """Get primary key columns for duplicate detection."""
    keys = {
        "player": ["yahoo_player_id", "year", "week", "manager"],
        "matchup": ["year", "week", "manager", "team_name"],
        "draft": ["year", "yahoo_player_id"],
        "transactions": ["transaction_id", "yahoo_player_id"],
    }
    return keys.get(table_name, [])


def validate_column_types(df: pd.DataFrame, table_name: str) -> list[ValidationIssue]:
    """Validate that columns have expected data types."""
    issues = []

    # Expected numeric columns
    numeric_cols = {
        "player": ["year", "week", "fantasy_points"],
        "matchup": ["year", "week", "team_points", "opponent_points"],
        "draft": ["year", "round", "pick"],
        "transactions": ["year"],
    }

    for col in numeric_cols.get(table_name, []):
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            issues.append(
                ValidationIssue(
                    severity="WARNING",
                    table=table_name,
                    check="column_type",
                    message=f"Column '{col}' should be numeric but is {df[col].dtype}",
                    details={"column": col, "actual_type": str(df[col].dtype)},
                )
            )

    return issues


def validate_value_ranges(df: pd.DataFrame, table_name: str) -> list[ValidationIssue]:
    """Validate that numeric values are in reasonable ranges."""
    issues = []

    # Week should be 1-22 (regular season weeks 1-18, playoffs can extend to week 22)
    if "week" in df.columns and pd.api.types.is_numeric_dtype(df["week"]):
        invalid_weeks = df[(df["week"] < 1) | (df["week"] > 22)]["week"].dropna()
        if len(invalid_weeks) > 0:
            issues.append(
                ValidationIssue(
                    severity="WARNING",
                    table=table_name,
                    check="value_range",
                    message=f"Invalid week values: {invalid_weeks.unique()[:5].tolist()}",
                    details={"column": "week", "invalid_count": len(invalid_weeks)},
                )
            )

    # Points should be non-negative (in most cases)
    if table_name == "matchup":
        if "team_points" in df.columns:
            negative_points = df[df["team_points"] < 0]["team_points"]
            if len(negative_points) > 0:
                issues.append(
                    ValidationIssue(
                        severity="WARNING",
                        table=table_name,
                        check="value_range",
                        message=f"Negative team_points found: {len(negative_points)} rows",
                        details={"column": "team_points", "negative_count": len(negative_points)},
                    )
                )

    # Draft round/pick should be reasonable
    if table_name == "draft":
        if "round" in df.columns:
            max_round = df["round"].max()
            if max_round > 30:
                issues.append(
                    ValidationIssue(
                        severity="INFO",
                        table=table_name,
                        check="value_range",
                        message=f"Unusually high draft round: {max_round}",
                        details={"column": "round", "max_value": int(max_round)},
                    )
                )

    return issues
