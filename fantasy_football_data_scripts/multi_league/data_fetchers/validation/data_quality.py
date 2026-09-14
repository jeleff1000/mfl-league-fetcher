"""
Data Quality Validator

Provides anomaly detection and data integrity checks beyond schema validation.
Identifies suspicious patterns that might indicate data issues:
- Statistical outliers
- Missing required relationships
- Inconsistent values across related columns
- Duplicate detection

Usage:
    validator = DataQualityValidator()
    report = validator.check_player_data(df)

    for issue in report.errors:
        print(f"ERROR: {issue.message}")
    for issue in report.warnings:
        print(f"WARNING: {issue.message}")
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import pandas as pd

logger = logging.getLogger(__name__)


class IssueSeverity(Enum):
    """Severity level for data quality issues."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DataQualityIssue:
    """A single data quality issue."""

    severity: IssueSeverity
    message: str
    column: str | None = None
    row_count: int = 0
    sample_rows: list[int] | None = None
    details: dict[str, Any] | None = None

    def __str__(self) -> str:
        prefix = f"[{self.severity.value.upper()}]"
        col_info = f" ({self.column})" if self.column else ""
        row_info = f" - {self.row_count} rows affected" if self.row_count > 0 else ""
        return f"{prefix}{col_info} {self.message}{row_info}"


@dataclass
class DataQualityReport:
    """Complete data quality report for a DataFrame."""

    table_name: str
    row_count: int
    column_count: int
    issues: list[DataQualityIssue] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def errors(self) -> list[DataQualityIssue]:
        """Get all error-level issues."""
        return [i for i in self.issues if i.severity == IssueSeverity.ERROR]

    @property
    def warnings(self) -> list[DataQualityIssue]:
        """Get all warning-level issues."""
        return [i for i in self.issues if i.severity == IssueSeverity.WARNING]

    @property
    def is_clean(self) -> bool:
        """True if no errors or warnings."""
        return len(self.errors) == 0 and len(self.warnings) == 0

    @property
    def has_errors(self) -> bool:
        """True if any errors found."""
        return len(self.errors) > 0

    def __str__(self) -> str:
        status = "CLEAN" if self.is_clean else ("ERRORS" if self.has_errors else "WARNINGS")
        return (
            f"DataQualityReport({self.table_name}): {status}, "
            f"{self.row_count} rows, {len(self.errors)} errors, "
            f"{len(self.warnings)} warnings"
        )


class DataQualityValidator:
    """
    Validates data quality beyond schema requirements.

    Checks include:
    - Statistical outliers (z-score > threshold)
    - Missing required values
    - Duplicate detection
    - Cross-column consistency
    - Temporal consistency (year/week relationships)
    """

    # Default thresholds
    OUTLIER_Z_THRESHOLD = 4.0  # Flag values > 4 standard deviations
    NULL_PCT_WARNING = 0.1  # Warn if > 10% null
    NULL_PCT_ERROR = 0.5  # Error if > 50% null
    MAX_SAMPLE_ROWS = 10  # Max rows to include in issue details

    def __init__(
        self,
        outlier_threshold: float = 4.0,
        null_warning_pct: float = 0.1,
        null_error_pct: float = 0.5,
    ):
        """
        Initialize validator.

        Args:
            outlier_threshold: Z-score threshold for outlier detection
            null_warning_pct: NULL percentage threshold for warnings
            null_error_pct: NULL percentage threshold for errors
        """
        self.outlier_threshold = outlier_threshold
        self.null_warning_pct = null_warning_pct
        self.null_error_pct = null_error_pct
        self.logger = logging.getLogger(f"{__name__}.DataQualityValidator")

    def check_player_data(self, df: pd.DataFrame) -> DataQualityReport:
        """
        Check player/roster data quality.

        Args:
            df: Player data DataFrame

        Returns:
            DataQualityReport with issues found
        """
        issues = []

        if df is None or df.empty:
            return DataQualityReport(
                table_name="player_fantasy",
                row_count=0,
                column_count=0,
                issues=[DataQualityIssue(severity=IssueSeverity.WARNING, message="DataFrame is empty or None")],
            )

        # Outlier detection for fantasy_points
        if "fantasy_points" in df.columns:
            issues.extend(self._check_outliers(df, "fantasy_points"))

        # Null value checks
        issues.extend(self._check_null_values(df, ["player", "manager", "NFL_player_id", "player_week"]))

        # Duplicate detection
        issues.extend(self._check_duplicates(df, ["player_week"], "player_fantasy"))

        # Cross-column consistency
        issues.extend(self._check_player_consistency(df))

        # Temporal consistency
        issues.extend(self._check_temporal_consistency(df))

        # Compute stats
        stats = self._compute_player_stats(df)

        report = DataQualityReport(
            table_name="player_fantasy",
            row_count=len(df),
            column_count=len(df.columns),
            issues=issues,
            stats=stats,
        )

        self.logger.info(str(report))
        return report

    def check_matchup_data(self, df: pd.DataFrame) -> DataQualityReport:
        """
        Check matchup data quality.

        Args:
            df: Matchup data DataFrame

        Returns:
            DataQualityReport with issues found
        """
        issues = []

        if df is None or df.empty:
            return DataQualityReport(
                table_name="matchup",
                row_count=0,
                column_count=0,
            )

        # Outlier detection for points
        if "points" in df.columns:
            issues.extend(self._check_outliers(df, "points"))

        # Null value checks
        issues.extend(self._check_null_values(df, ["manager", "opponent", "points", "opponent_points"]))

        # Duplicate detection - use team_name to distinguish multi-team owners
        # (e.g., "Michael" owns both "Tig Ole Bitties" and "VD and CRABtree")
        dedup_cols = (
            ["year", "week", "manager", "team_name"] if "team_name" in df.columns else ["year", "week", "manager"]
        )
        issues.extend(self._check_duplicates(df, dedup_cols, "matchup"))

        # Win/loss consistency
        issues.extend(self._check_matchup_consistency(df))

        stats = self._compute_matchup_stats(df)

        return DataQualityReport(
            table_name="matchup",
            row_count=len(df),
            column_count=len(df.columns),
            issues=issues,
            stats=stats,
        )

    def check_draft_data(self, df: pd.DataFrame) -> DataQualityReport:
        """
        Check draft data quality.

        Args:
            df: Draft data DataFrame

        Returns:
            DataQualityReport with issues found
        """
        issues = []

        if df is None or df.empty:
            return DataQualityReport(
                table_name="draft",
                row_count=0,
                column_count=0,
            )

        # Null value checks
        issues.extend(self._check_null_values(df, ["player", "manager", "pick", "round", "NFL_player_id"]))

        # Duplicate detection
        issues.extend(self._check_duplicates(df, ["year", "pick"], "draft"))

        # Draft sequence consistency
        issues.extend(self._check_draft_consistency(df))

        stats = self._compute_draft_stats(df)

        return DataQualityReport(
            table_name="draft",
            row_count=len(df),
            column_count=len(df.columns),
            issues=issues,
            stats=stats,
        )

    def check_transaction_data(self, df: pd.DataFrame) -> DataQualityReport:
        """
        Check transaction data quality.

        Args:
            df: Transaction data DataFrame

        Returns:
            DataQualityReport with issues found
        """
        issues = []

        if df is None or df.empty:
            return DataQualityReport(
                table_name="transactions",
                row_count=0,
                column_count=0,
            )

        # Null value checks
        issues.extend(self._check_null_values(df, ["player", "manager", "transaction_type", "transaction_id"]))

        # Duplicate detection
        issues.extend(self._check_duplicates(df, ["transaction_id", "player"], "transactions"))

        stats = self._compute_transaction_stats(df)

        return DataQualityReport(
            table_name="transactions",
            row_count=len(df),
            column_count=len(df.columns),
            issues=issues,
            stats=stats,
        )

    # -------------------------------------------------------------------------
    # Internal Check Methods
    # -------------------------------------------------------------------------

    def _check_outliers(
        self,
        df: pd.DataFrame,
        column: str,
    ) -> list[DataQualityIssue]:
        """Check for statistical outliers in numeric column."""
        issues = []

        if column not in df.columns:
            return issues

        col_data = pd.to_numeric(df[column], errors="coerce")
        valid_data = col_data.dropna()

        if len(valid_data) < 10:  # Need enough data for statistics
            return issues

        mean = valid_data.mean()
        std = valid_data.std()

        if std == 0:  # No variation
            return issues

        z_scores = (col_data - mean) / std
        outliers = df[z_scores.abs() > self.outlier_threshold]

        if len(outliers) > 0:
            sample_indices = outliers.index[: self.MAX_SAMPLE_ROWS].tolist()
            outlier_values = col_data[outliers.index]

            issues.append(
                DataQualityIssue(
                    severity=IssueSeverity.WARNING,
                    message=f"Statistical outliers detected (z > {self.outlier_threshold})",
                    column=column,
                    row_count=len(outliers),
                    sample_rows=sample_indices,
                    details={
                        "mean": mean,
                        "std": std,
                        "min_outlier": outlier_values.min(),
                        "max_outlier": outlier_values.max(),
                    },
                )
            )

        return issues

    def _check_null_values(
        self,
        df: pd.DataFrame,
        columns: list[str],
    ) -> list[DataQualityIssue]:
        """Check for excessive NULL values."""
        issues = []

        for col in columns:
            if col not in df.columns:
                continue

            null_count = df[col].isna().sum()
            null_pct = null_count / len(df) if len(df) > 0 else 0

            if null_pct > self.null_error_pct:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.ERROR,
                        message=f"{null_pct:.1%} NULL values (>{self.null_error_pct:.0%})",
                        column=col,
                        row_count=null_count,
                    )
                )
            elif null_pct > self.null_warning_pct:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.WARNING,
                        message=f"{null_pct:.1%} NULL values (>{self.null_warning_pct:.0%})",
                        column=col,
                        row_count=null_count,
                    )
                )

        return issues

    def _check_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: list[str],
        table_name: str,
    ) -> list[DataQualityIssue]:
        """Check for duplicate rows based on key columns."""
        issues = []

        available_keys = [k for k in key_columns if k in df.columns]
        if not available_keys:
            return issues

        dupes = df.duplicated(subset=available_keys, keep=False)
        dupe_count = dupes.sum()

        if dupe_count > 0:
            dupe_indices = df[dupes].index[: self.MAX_SAMPLE_ROWS].tolist()
            issues.append(
                DataQualityIssue(
                    severity=IssueSeverity.ERROR,
                    message=f"Duplicate rows found on key {available_keys}",
                    column=str(available_keys),
                    row_count=dupe_count,
                    sample_rows=dupe_indices,
                )
            )

        return issues

    def _check_player_consistency(
        self,
        df: pd.DataFrame,
    ) -> list[DataQualityIssue]:
        """Check player-specific cross-column consistency."""
        issues = []

        # Check fantasy_position vs is_started consistency
        if "fantasy_position" in df.columns and "is_started" in df.columns:
            bench_pos = df["fantasy_position"].str.upper().isin(["BN", "IR", "IL", "TAXI"])
            expected_not_started = bench_pos
            actual_not_started = ~df["is_started"].fillna(False)

            mismatch = expected_not_started != actual_not_started
            mismatch_count = mismatch.sum()

            if mismatch_count > 0:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.WARNING,
                        message="is_started inconsistent with fantasy_position",
                        row_count=mismatch_count,
                    )
                )

        # Check position validity
        if "position" in df.columns:
            valid_positions = {"QB", "RB", "WR", "TE", "K", "DEF", "DST", "D/ST", "DL", "LB", "DB", "IDP"}
            invalid_pos = ~df["position"].str.upper().isin(valid_positions)
            invalid_count = invalid_pos.sum()

            if invalid_count > 0:
                invalid_values = df.loc[invalid_pos, "position"].unique()[:5]
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.WARNING,
                        message=f"Invalid positions found: {list(invalid_values)}",
                        column="position",
                        row_count=invalid_count,
                    )
                )

        return issues

    def _check_matchup_consistency(
        self,
        df: pd.DataFrame,
    ) -> list[DataQualityIssue]:
        """Check matchup-specific consistency."""
        issues = []

        # Check win/loss/tie sum to 1
        if all(col in df.columns for col in ["win", "loss", "tie"]):
            row_sums = df["win"].fillna(0) + df["loss"].fillna(0) + df["tie"].fillna(0)
            invalid = row_sums != 1

            # Exclude byes (can be all zeros)
            if "is_bye" in df.columns:
                invalid = invalid & ~df["is_bye"].fillna(False)

            invalid_count = invalid.sum()
            if invalid_count > 0:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.ERROR,
                        message="win + loss + tie != 1",
                        row_count=invalid_count,
                    )
                )

        # Check margin consistency
        if all(col in df.columns for col in ["points", "opponent_points", "margin"]):
            expected_margin = df["points"].fillna(0) - df["opponent_points"].fillna(0)
            actual_margin = df["margin"].fillna(0)
            margin_diff = (expected_margin - actual_margin).abs()

            inconsistent = margin_diff > 0.01  # Allow small floating point differences
            inconsistent_count = inconsistent.sum()

            if inconsistent_count > 0:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.ERROR,
                        message="margin != points - opponent_points",
                        column="margin",
                        row_count=inconsistent_count,
                    )
                )

        return issues

    def _check_draft_consistency(
        self,
        df: pd.DataFrame,
    ) -> list[DataQualityIssue]:
        """Check draft-specific consistency."""
        issues = []

        # Check pick sequence
        if "year" in df.columns and "pick" in df.columns:
            for year in df["year"].unique():
                year_df = df[df["year"] == year]
                picks = year_df["pick"].dropna().sort_values()

                if len(picks) > 0:
                    expected_picks = range(1, len(picks) + 1)
                    actual_picks = picks.tolist()

                    missing = set(expected_picks) - set(actual_picks)
                    if missing and len(missing) < len(expected_picks) / 2:
                        issues.append(
                            DataQualityIssue(
                                severity=IssueSeverity.WARNING,
                                message=f"Missing picks in {year}: {sorted(missing)[:10]}",
                                column="pick",
                                details={"year": year, "missing_picks": sorted(missing)[:20]},
                            )
                        )

        return issues

    def _check_temporal_consistency(
        self,
        df: pd.DataFrame,
    ) -> list[DataQualityIssue]:
        """Check year/week temporal consistency."""
        issues = []

        if "year" not in df.columns:
            return issues

        # Check year range
        min_year = df["year"].min()
        max_year = df["year"].max()

        if min_year < 1920:
            issues.append(
                DataQualityIssue(
                    severity=IssueSeverity.ERROR,
                    message=f"Year before 1920 found: {min_year}",
                    column="year",
                )
            )

        if max_year > 2100:
            issues.append(
                DataQualityIssue(
                    severity=IssueSeverity.ERROR,
                    message=f"Year after 2100 found: {max_year}",
                    column="year",
                )
            )

        # Check week range
        if "week" in df.columns:
            min_week = df["week"].min()
            max_week = df["week"].max()

            if min_week < 0:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.ERROR,
                        message=f"Negative week found: {min_week}",
                        column="week",
                    )
                )

            if max_week > 25:
                issues.append(
                    DataQualityIssue(
                        severity=IssueSeverity.WARNING,
                        message=f"Week > 25 found: {max_week}",
                        column="week",
                    )
                )

        return issues

    # -------------------------------------------------------------------------
    # Statistics Computation
    # -------------------------------------------------------------------------

    def _compute_player_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute summary statistics for player data."""
        stats = {
            "total_rows": len(df),
            "unique_players": df["player"].nunique() if "player" in df.columns else 0,
            "unique_managers": df["manager"].nunique() if "manager" in df.columns else 0,
            "year_range": (
                int(df["year"].min()) if "year" in df.columns else None,
                int(df["year"].max()) if "year" in df.columns else None,
            ),
        }

        if "fantasy_points" in df.columns:
            stats["fantasy_points"] = {
                "mean": df["fantasy_points"].mean(),
                "std": df["fantasy_points"].std(),
                "min": df["fantasy_points"].min(),
                "max": df["fantasy_points"].max(),
            }

        return stats

    def _compute_matchup_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute summary statistics for matchup data."""
        return {
            "total_rows": len(df),
            "unique_managers": df["manager"].nunique() if "manager" in df.columns else 0,
            "total_wins": df["win"].sum() if "win" in df.columns else 0,
            "total_losses": df["loss"].sum() if "loss" in df.columns else 0,
        }

    def _compute_draft_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute summary statistics for draft data."""
        return {
            "total_rows": len(df),
            "unique_players": df["player"].nunique() if "player" in df.columns else 0,
            "years": sorted(df["year"].unique()) if "year" in df.columns else [],
        }

    def _compute_transaction_stats(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute summary statistics for transaction data."""
        stats = {
            "total_rows": len(df),
            "unique_players": df["player"].nunique() if "player" in df.columns else 0,
        }

        if "transaction_type" in df.columns:
            stats["by_type"] = df["transaction_type"].value_counts().to_dict()

        return stats
