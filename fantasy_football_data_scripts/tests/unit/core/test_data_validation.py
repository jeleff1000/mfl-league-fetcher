"""
Unit tests for data_validation.py

Tests data validation checkpoints for the pipeline.
Target coverage: 90%+
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add the multi_league directory to path for imports
_test_dir = Path(__file__).resolve().parent
_tests_dir = _test_dir.parent.parent
_scripts_dir = _tests_dir.parent
sys.path.insert(0, str(_scripts_dir))

from multi_league.core.data_validation import (
    ValidationError,
    ValidationResult,
    validate_checkpoint,
    validate_post_fetch,
    validate_post_merge,
    validate_post_transform,
    validate_canonical_file,
    summarize_validation_results,
    _check_non_empty,
    _check_required_columns,
    _check_no_null_keys,
    _check_unique_keys,
    _check_value_range,
    _check_row_count,
)


class TestValidationError:
    """Test ValidationError exception."""

    def test_error_message_format(self):
        """Test error message includes checkpoint and rule info."""
        error = ValidationError("post_fetch", "non_empty", "DataFrame is empty")

        assert "post_fetch" in str(error)
        assert "non_empty" in str(error)
        assert "DataFrame is empty" in str(error)

    def test_error_attributes(self):
        """Test error attributes are set correctly."""
        error = ValidationError("checkpoint", "rule", "details")

        assert error.checkpoint == "checkpoint"
        assert error.rule_name == "rule"
        assert error.details == "details"


class TestCheckNonEmpty:
    """Test _check_non_empty validation rule."""

    def test_passes_non_empty_dataframe(self):
        """Test passes for non-empty DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = _check_non_empty(df, "test")

        assert result.passed is True
        assert "3 rows" in result.message

    def test_fails_empty_dataframe(self):
        """Test fails for empty DataFrame."""
        df = pd.DataFrame()
        result = _check_non_empty(df, "test")

        assert result.passed is False
        assert "empty" in result.message


class TestCheckRequiredColumns:
    """Test _check_required_columns validation rule."""

    def test_passes_all_columns_present(self):
        """Test passes when all required columns exist."""
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = _check_required_columns(df, ["a", "b"], "test")

        assert result.passed is True

    def test_fails_missing_columns(self):
        """Test fails when columns are missing."""
        df = pd.DataFrame({"a": [1]})
        result = _check_required_columns(df, ["a", "b", "c"], "test")

        assert result.passed is False
        assert "b" in result.message
        assert "c" in result.message
        assert "b" in result.details["missing"]
        assert "c" in result.details["missing"]

    def test_passes_empty_required_list(self):
        """Test passes with empty required columns list."""
        df = pd.DataFrame({"a": [1]})
        result = _check_required_columns(df, [], "test")

        assert result.passed is True


class TestCheckNoNullKeys:
    """Test _check_no_null_keys validation rule."""

    def test_passes_no_nulls(self):
        """Test passes when no null values in key columns."""
        df = pd.DataFrame(
            {
                "key1": [1, 2, 3],
                "key2": ["a", "b", "c"],
            }
        )
        result = _check_no_null_keys(df, ["key1", "key2"], "test")

        assert result.passed is True

    def test_fails_with_nulls(self):
        """Test fails when null values present."""
        df = pd.DataFrame(
            {
                "key1": [1, None, 3],
                "key2": ["a", "b", None],
            }
        )
        result = _check_no_null_keys(df, ["key1", "key2"], "test")

        assert result.passed is False
        assert result.details["null_counts"]["key1"] == 1
        assert result.details["null_counts"]["key2"] == 1

    def test_ignores_missing_columns(self):
        """Test ignores columns not in DataFrame."""
        df = pd.DataFrame({"key1": [1, 2, 3]})
        result = _check_no_null_keys(df, ["key1", "missing_col"], "test")

        assert result.passed is True


class TestCheckUniqueKeys:
    """Test _check_unique_keys validation rule."""

    def test_passes_unique_keys(self):
        """Test passes when keys are unique."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2023],
                "week": [1, 2, 1],
            }
        )
        result = _check_unique_keys(df, ["year", "week"], "test")

        assert result.passed is True

    def test_fails_duplicate_keys(self):
        """Test fails when duplicate key combinations exist."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024, 2024],
                "week": [1, 1, 2],
            }
        )
        result = _check_unique_keys(df, ["year", "week"], "test")

        assert result.passed is False
        assert result.details["duplicate_count"] == 2

    def test_passes_empty_key_list(self):
        """Test passes with empty key columns list."""
        df = pd.DataFrame({"a": [1, 1, 1]})
        result = _check_unique_keys(df, [], "test")

        assert result.passed is True


class TestCheckValueRange:
    """Test _check_value_range validation rule."""

    def test_passes_in_range(self):
        """Test passes when all values in range."""
        df = pd.DataFrame({"points": [10.0, 20.0, 30.0]})
        result = _check_value_range(df, "points", 0.0, 100.0, "test")

        assert result.passed is True

    def test_fails_out_of_range(self):
        """Test fails when values outside range."""
        df = pd.DataFrame({"points": [-5.0, 20.0, 150.0]})
        result = _check_value_range(df, "points", 0.0, 100.0, "test")

        assert result.passed is False
        assert result.details["out_of_range_count"] == 2
        assert result.details["actual_min"] == -5.0
        assert result.details["actual_max"] == 150.0

    def test_skips_missing_column(self):
        """Test skips validation for missing column."""
        df = pd.DataFrame({"other": [1, 2, 3]})
        result = _check_value_range(df, "points", 0.0, 100.0, "test")

        assert result.passed is True
        assert "not present" in result.message


class TestCheckRowCount:
    """Test _check_row_count validation rule."""

    def test_passes_in_range(self):
        """Test passes when row count in range."""
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        result = _check_row_count(df, min_rows=1, max_rows=10, name="test")

        assert result.passed is True

    def test_fails_below_minimum(self):
        """Test fails when row count below minimum."""
        df = pd.DataFrame({"a": [1, 2]})
        result = _check_row_count(df, min_rows=5, max_rows=None, name="test")

        assert result.passed is False
        assert "2" in result.message
        assert "5" in result.message

    def test_fails_above_maximum(self):
        """Test fails when row count above maximum."""
        df = pd.DataFrame({"a": list(range(100))})
        result = _check_row_count(df, min_rows=0, max_rows=50, name="test")

        assert result.passed is False
        assert "100" in result.message
        assert "50" in result.message


class TestValidateCheckpoint:
    """Test validate_checkpoint function."""

    def test_runs_multiple_rules(self):
        """Test runs multiple validation rules."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        rules = [
            {"type": "non_empty"},
            {"type": "required_columns", "columns": ["a", "b"]},
        ]

        results = validate_checkpoint(df, "test", rules, fail_fast=False)

        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_fail_fast_raises_on_first_failure(self):
        """Test fail_fast raises exception on first failure."""
        df = pd.DataFrame()
        rules = [
            {"type": "non_empty"},
            {"type": "required_columns", "columns": ["missing"]},
        ]

        with pytest.raises(ValidationError) as exc_info:
            validate_checkpoint(df, "test", rules, fail_fast=True)

        assert exc_info.value.rule_name == "non_empty"

    def test_no_fail_fast_continues_after_failure(self):
        """Test without fail_fast continues checking after failure."""
        df = pd.DataFrame({"a": [1]})
        rules = [
            {"type": "required_columns", "columns": ["missing1"]},
            {"type": "required_columns", "columns": ["missing2"]},
        ]

        results = validate_checkpoint(df, "test", rules, fail_fast=False)

        assert len(results) == 2
        assert all(not r.passed for r in results)


class TestValidatePostFetch:
    """Test validate_post_fetch function."""

    def test_validates_non_empty(self):
        """Test validates DataFrame is non-empty."""
        df = pd.DataFrame({"a": [1]})
        results = validate_post_fetch(df, "test_source", fail_fast=False)

        assert any(r.rule_name == "non_empty" for r in results)

    def test_validates_required_columns(self):
        """Test validates required columns if specified."""
        df = pd.DataFrame({"a": [1], "b": [2]})
        results = validate_post_fetch(df, "test_source", required_columns=["a", "b"], fail_fast=False)

        assert any(r.rule_name == "required_columns" for r in results)

    def test_raises_on_empty(self):
        """Test raises ValidationError for empty DataFrame."""
        df = pd.DataFrame()

        with pytest.raises(ValidationError) as exc_info:
            validate_post_fetch(df, "test_source", fail_fast=True)

        assert "post_fetch_test_source" in exc_info.value.checkpoint


class TestValidatePostMerge:
    """Test validate_post_merge function."""

    def test_validates_key_columns(self):
        """Test validates key columns for nulls and uniqueness."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024],
                "week": [1, 2],
            }
        )
        results = validate_post_merge(df, "test_merge", key_columns=["year", "week"], fail_fast=False)

        rule_names = [r.rule_name for r in results]
        assert "no_null_keys" in rule_names
        assert "unique_keys" in rule_names

    def test_skips_uniqueness_check_if_disabled(self):
        """Test skips uniqueness check when check_uniqueness=False."""
        df = pd.DataFrame(
            {
                "year": [2024, 2024],
                "week": [1, 1],  # Duplicate
            }
        )
        results = validate_post_merge(
            df,
            "test_merge",
            key_columns=["year", "week"],
            check_uniqueness=False,
            fail_fast=False,
        )

        assert not any(r.rule_name == "unique_keys" for r in results)


class TestValidatePostTransform:
    """Test validate_post_transform function."""

    def test_validates_row_count(self):
        """Test validates minimum row count."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        results = validate_post_transform(df, "test_transform", min_rows=1, fail_fast=False)

        assert any(r.rule_name == "row_count" for r in results)

    def test_validates_expected_columns(self):
        """Test validates expected output columns."""
        df = pd.DataFrame({"lamar": [1.0], "points": [25.0]})
        results = validate_post_transform(
            df,
            "test_transform",
            expected_columns=["lamar", "points"],
            fail_fast=False,
        )

        assert any(r.rule_name == "required_columns" for r in results)


class TestValidateCanonicalFile:
    """Test validate_canonical_file function."""

    def test_validates_player_parquet(self):
        """Test validation rules for player.parquet."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "year": [2024, 2024],
                "week": [1, 2],
                "league_id": ["123", "123"],
            }
        )
        results = validate_canonical_file(df, "player.parquet", fail_fast=False)

        assert any(r.passed for r in results)

    def test_validates_matchup_parquet(self):
        """Test validation rules for matchup.parquet."""
        df = pd.DataFrame(
            {
                "year": [2024],
                "week": [1],
                "manager": ["Team A"],
                "league_id": ["123"],
            }
        )
        results = validate_canonical_file(df, "matchup.parquet", fail_fast=False)

        assert any(r.passed for r in results)

    def test_fails_missing_required_columns(self):
        """Test fails when required columns missing."""
        df = pd.DataFrame({"year": [2024]})  # Missing other required columns

        with pytest.raises(ValidationError):
            validate_canonical_file(df, "player.parquet", fail_fast=True)


class TestSummarizeValidationResults:
    """Test summarize_validation_results function."""

    def test_summarizes_all_passed(self):
        """Test summary when all checks pass."""
        results = [
            ValidationResult(passed=True, rule_name="a", message="ok"),
            ValidationResult(passed=True, rule_name="b", message="ok"),
        ]
        summary = summarize_validation_results(results)

        assert summary["total_checks"] == 2
        assert summary["passed"] == 2
        assert summary["failed"] == 0
        assert summary["success_rate"] == 1.0
        assert summary["failures"] == []

    def test_summarizes_with_failures(self):
        """Test summary with some failures."""
        results = [
            ValidationResult(passed=True, rule_name="a", message="ok"),
            ValidationResult(passed=False, rule_name="b", message="failed"),
            ValidationResult(passed=False, rule_name="c", message="also failed"),
        ]
        summary = summarize_validation_results(results)

        assert summary["total_checks"] == 3
        assert summary["passed"] == 1
        assert summary["failed"] == 2
        assert summary["success_rate"] == pytest.approx(0.333, rel=0.01)
        assert len(summary["failures"]) == 2

    def test_handles_empty_results(self):
        """Test summary with empty results list."""
        summary = summarize_validation_results([])

        assert summary["total_checks"] == 0
        assert summary["success_rate"] == 1.0


class TestIntegration:
    """Integration tests for validation workflow."""

    def test_full_validation_pipeline(self, sample_player_df):
        """Test validating data through multiple checkpoints."""
        # Simulate post-fetch validation
        fetch_results = validate_post_fetch(
            sample_player_df,
            "yahoo_data",
            required_columns=["yahoo_player_id", "player", "year"],
            fail_fast=False,
        )
        assert all(r.passed for r in fetch_results)

        # Simulate post-merge validation
        merge_results = validate_post_merge(
            sample_player_df,
            "yahoo_nfl_merge",
            key_columns=["yahoo_player_id", "year", "week"],
            fail_fast=False,
        )
        assert all(r.passed for r in merge_results)

        # Simulate post-transform validation
        transform_results = validate_post_transform(
            sample_player_df,
            "player_stats",
            min_rows=1,
            fail_fast=False,
        )
        assert all(r.passed for r in transform_results)
