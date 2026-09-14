"""
Unit tests for type_utils.py

Tests type normalization utilities used across transformations.
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

from multi_league.transformations.common.type_utils import (
    STANDARD_JOIN_KEYS,
    normalize_join_keys,
    safe_merge,
    ensure_canonical_types,
    validate_join_keys,
    get_join_key_info,
)


class TestStandardJoinKeys:
    """Test the STANDARD_JOIN_KEYS configuration."""

    def test_standard_keys_defined(self):
        """Verify all expected join keys are defined."""
        expected_keys = [
            "yahoo_player_id",
            "NFL_player_id",
            "year",
            "week",
            "cumulative_week",
            "season",
            "transaction_sequence",
            "league_id",
        ]
        for key in expected_keys:
            assert key in STANDARD_JOIN_KEYS

    def test_numeric_keys_are_int64(self):
        """Verify numeric join keys are Int64 type."""
        numeric_keys = [
            "yahoo_player_id",
            "year",
            "week",
            "cumulative_week",
            "season",
            "transaction_sequence",
        ]
        for key in numeric_keys:
            assert STANDARD_JOIN_KEYS[key] == "Int64"

    def test_nfl_player_id_is_string(self):
        """Verify NFL_player_id stays string to support DEF-* identifiers."""
        assert STANDARD_JOIN_KEYS["NFL_player_id"] == "string"

    def test_league_id_is_string(self):
        """Verify league_id is string type."""
        assert STANDARD_JOIN_KEYS["league_id"] == "string"


class TestNormalizeJoinKeys:
    """Test normalize_join_keys function."""

    def test_converts_string_to_int64(self):
        """Test conversion of string IDs to Int64."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002", "1003"],
                "year": ["2024", "2024", "2024"],
            }
        )

        result = normalize_join_keys(df)

        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["year"].dtype == "Int64"
        assert list(result["yahoo_player_id"]) == [1001, 1002, 1003]

    def test_converts_float_to_int64(self):
        """Test conversion of float IDs to Int64."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": [1001.0, 1002.0, 1003.0],
                "week": [1.0, 2.0, 3.0],
            }
        )

        result = normalize_join_keys(df)

        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["week"].dtype == "Int64"

    def test_handles_null_values(self):
        """Test that null values are preserved as pd.NA."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": [1001, None, 1003],
                "year": [2024, 2024, None],
            }
        )

        result = normalize_join_keys(df)

        assert pd.isna(result["yahoo_player_id"].iloc[1])
        assert pd.isna(result["year"].iloc[2])

    def test_converts_int_league_id_to_string(self):
        """Test conversion of int league_id to string."""
        df = pd.DataFrame(
            {
                "league_id": [123, 456, 789],
                "year": [2024, 2024, 2024],
            }
        )

        result = normalize_join_keys(df)

        assert result["league_id"].dtype == "string"
        assert list(result["league_id"]) == ["123", "456", "789"]

    def test_skips_already_correct_types(self):
        """Test that correctly typed columns are not modified."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, 1002, 1003], dtype="Int64"),
                "league_id": pd.array(["123", "456", "789"], dtype="string"),
            }
        )

        result = normalize_join_keys(df)

        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["league_id"].dtype == "string"

    def test_normalize_specific_keys_only(self):
        """Test normalizing only specified keys."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],
                "year": ["2024", "2024"],
                "week": ["1", "2"],
            }
        )

        result = normalize_join_keys(df, keys=["yahoo_player_id"])

        assert result["yahoo_player_id"].dtype == "Int64"
        # year and week should remain unchanged
        assert result["year"].dtype == "object"
        assert result["week"].dtype == "object"

    def test_ignores_missing_columns(self):
        """Test that missing columns are silently ignored."""
        df = pd.DataFrame(
            {
                "year": ["2024", "2024"],
                "other_col": ["a", "b"],
            }
        )

        # Should not raise even though yahoo_player_id is missing
        result = normalize_join_keys(df, keys=["yahoo_player_id", "year"])

        assert "yahoo_player_id" not in result.columns
        assert result["year"].dtype == "Int64"

    def test_handles_non_standard_keys(self):
        """Test that non-standard keys are ignored."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001"],
                "custom_column": ["value"],
            }
        )

        result = normalize_join_keys(df, keys=["yahoo_player_id", "custom_column"])

        assert result["yahoo_player_id"].dtype == "Int64"
        # custom_column should be unchanged (no standard type defined)
        assert result["custom_column"].dtype == "object"

    def test_creates_copy(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],
            }
        )
        original_dtype = df["yahoo_player_id"].dtype

        normalize_join_keys(df)

        assert df["yahoo_player_id"].dtype == original_dtype

    def test_handles_invalid_numeric_values(self):
        """Test handling of non-convertible values."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "invalid", "1003"],
                "year": [2024, 2024, 2024],
            }
        )

        result = normalize_join_keys(df)

        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["yahoo_player_id"].iloc[0] == 1001
        assert pd.isna(result["yahoo_player_id"].iloc[1])  # 'invalid' becomes NA
        assert result["yahoo_player_id"].iloc[2] == 1003


class TestSafeMerge:
    """Test safe_merge function."""

    def test_basic_merge(self):
        """Test basic merge operation."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "player": ["Player A", "Player B"],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "points": [25.5, 30.2],
            }
        )

        result = safe_merge(left, right, on="yahoo_player_id")

        assert len(result) == 2
        assert "player" in result.columns
        assert "points" in result.columns

    def test_normalizes_types_before_merge(self):
        """Test that types are normalized before merging."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],  # String
                "player": ["Player A", "Player B"],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],  # Int
                "points": [25.5, 30.2],
            }
        )

        result = safe_merge(left, right, on="yahoo_player_id")

        assert len(result) == 2
        assert result["yahoo_player_id"].dtype == "Int64"

    def test_handles_multiple_join_keys(self):
        """Test merge with multiple join keys."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1001"],
                "year": ["2023", "2024"],
                "player": ["Player A", "Player A"],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1001],
                "year": [2023, 2024],
                "points": [200.5, 250.3],
            }
        )

        result = safe_merge(left, right, on=["yahoo_player_id", "year"])

        assert len(result) == 2
        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["year"].dtype == "Int64"

    def test_left_join_preserves_left_rows(self):
        """Test left join preserves all left rows."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002, 1003],
                "player": ["A", "B", "C"],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "points": [25.5, 30.2],
            }
        )

        result = safe_merge(left, right, on="yahoo_player_id", how="left")

        assert len(result) == 3
        assert pd.isna(result[result["yahoo_player_id"] == 1003]["points"].iloc[0])

    def test_inner_join(self):
        """Test inner join only keeps matching rows."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002, 1003],
                "player": ["A", "B", "C"],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "points": [25.5, 30.2],
            }
        )

        result = safe_merge(left, right, on="yahoo_player_id", how="inner")

        assert len(result) == 2

    def test_applies_suffixes(self):
        """Test suffix handling for overlapping columns."""
        left = pd.DataFrame(
            {
                "yahoo_player_id": [1001],
                "points": [25.5],
            }
        )
        right = pd.DataFrame(
            {
                "yahoo_player_id": [1001],
                "points": [30.2],
            }
        )

        result = safe_merge(left, right, on="yahoo_player_id", suffixes=("_left", "_right"))

        assert "points_left" in result.columns
        assert "points_right" in result.columns


class TestEnsureCanonicalTypes:
    """Test ensure_canonical_types function."""

    def test_normalizes_all_standard_keys(self):
        """Test that all standard keys present are normalized."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001"],
                "NFL_player_id": ["DEF-14"],
                "year": ["2024"],
                "week": [1.0],
                "league_id": [123],
                "player": ["Player A"],
            }
        )

        result = ensure_canonical_types(df)

        assert result["yahoo_player_id"].dtype == "Int64"
        assert result["NFL_player_id"].dtype == "string"
        assert result["NFL_player_id"].iloc[0] == "DEF-14"
        assert result["year"].dtype == "Int64"
        assert result["week"].dtype == "Int64"
        assert result["league_id"].dtype == "string"

    def test_is_thin_wrapper(self):
        """Test that ensure_canonical_types is a thin wrapper around normalize_join_keys."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001"],
                "year": ["2024"],
            }
        )

        result = ensure_canonical_types(df)
        expected = normalize_join_keys(df, keys=None)

        pd.testing.assert_frame_equal(result, expected)


class TestValidateJoinKeys:
    """Test validate_join_keys function."""

    def test_passes_valid_dataframe(self):
        """Test validation passes for valid DataFrame."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, 1002], dtype="Int64"),
                "year": pd.array([2024, 2024], dtype="Int64"),
                "league_id": pd.array(["123", "123"], dtype="string"),
            }
        )

        result = validate_join_keys(df, required_keys=["yahoo_player_id", "year"])

        assert result is True

    def test_raises_on_missing_columns(self):
        """Test that missing columns raise ValueError."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
            }
        )

        with pytest.raises(ValueError, match="missing required join keys"):
            validate_join_keys(df, required_keys=["yahoo_player_id", "year"])

    def test_raises_on_wrong_types(self):
        """Test that wrong types raise ValueError."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],  # String, should be Int64
                "year": [2024, 2024],
            }
        )

        with pytest.raises(ValueError, match="incorrect join key types"):
            validate_join_keys(df, required_keys=["yahoo_player_id"])

    def test_includes_dataframe_name_in_error(self):
        """Test that DataFrame name appears in error message."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": [1001],
            }
        )

        with pytest.raises(ValueError, match="PlayerData"):
            validate_join_keys(df, required_keys=["yahoo_player_id", "year"], name="PlayerData")

    def test_ignores_non_standard_keys(self):
        """Test that non-standard keys skip type validation."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001], dtype="Int64"),
                "custom_column": ["value"],  # Not in STANDARD_JOIN_KEYS
            }
        )

        # Should not raise even though custom_column has 'object' type
        result = validate_join_keys(df, required_keys=["yahoo_player_id", "custom_column"])

        assert result is True


class TestGetJoinKeyInfo:
    """Test get_join_key_info function."""

    def test_returns_info_for_present_keys(self):
        """Test info is returned for keys present in DataFrame."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, 1002, None], dtype="Int64"),
                "year": pd.array([2024, 2024, 2024], dtype="Int64"),
                "player": ["A", "B", "C"],  # Not a standard key
            }
        )

        info = get_join_key_info(df)

        assert "yahoo_player_id" in info
        assert "year" in info
        assert "player" not in info  # Not a standard join key

    def test_includes_dtype_info(self):
        """Test dtype information is included."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, 1002], dtype="Int64"),
            }
        )

        info = get_join_key_info(df)

        assert info["yahoo_player_id"]["dtype"] == "Int64"
        assert info["yahoo_player_id"]["expected_dtype"] == "Int64"
        assert info["yahoo_player_id"]["correct_type"] is True

    def test_includes_null_count(self):
        """Test null count is calculated correctly."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, None, None], dtype="Int64"),
            }
        )

        info = get_join_key_info(df)

        assert info["yahoo_player_id"]["null_count"] == 2
        assert info["yahoo_player_id"]["null_pct"] == pytest.approx(66.67, rel=0.1)

    def test_includes_sample_values(self):
        """Test sample values are included."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([1001, 1002, 1003, 1004], dtype="Int64"),
            }
        )

        info = get_join_key_info(df)

        assert len(info["yahoo_player_id"]["sample_values"]) == 3
        assert 1001 in info["yahoo_player_id"]["sample_values"]

    def test_handles_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": pd.array([], dtype="Int64"),
            }
        )

        info = get_join_key_info(df)

        assert info["yahoo_player_id"]["null_count"] == 0
        assert info["yahoo_player_id"]["null_pct"] == 0
        assert info["yahoo_player_id"]["sample_values"] == []

    def test_identifies_incorrect_types(self):
        """Test identification of incorrect types."""
        df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],  # String, should be Int64
            }
        )

        info = get_join_key_info(df)

        assert info["yahoo_player_id"]["dtype"] == "object"
        assert info["yahoo_player_id"]["expected_dtype"] == "Int64"
        assert info["yahoo_player_id"]["correct_type"] is False


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_pipeline_workflow(self, sample_player_df_mixed_types):
        """Test a typical workflow: normalize -> validate -> get info."""
        df = sample_player_df_mixed_types

        # Step 1: Normalize
        normalized = normalize_join_keys(df)

        # Step 2: Validate (should pass after normalization)
        assert validate_join_keys(normalized, required_keys=["yahoo_player_id", "year", "week"])

        # Step 3: Get info
        info = get_join_key_info(normalized)
        assert info["yahoo_player_id"]["correct_type"] is True
        assert info["year"]["correct_type"] is True

    def test_merge_with_different_source_types(self):
        """Test merging data from different sources with different types."""
        # Yahoo data (strings)
        yahoo_df = pd.DataFrame(
            {
                "yahoo_player_id": ["1001", "1002"],
                "player": ["Player A", "Player B"],
                "year": ["2024", "2024"],
            }
        )

        # NFL data (integers)
        nfl_df = pd.DataFrame(
            {
                "yahoo_player_id": [1001, 1002],
                "year": [2024, 2024],
                "passing_yards": [300, 275],
            }
        )

        # Merge should work despite type differences
        result = safe_merge(yahoo_df, nfl_df, on=["yahoo_player_id", "year"])

        assert len(result) == 2
        assert "player" in result.columns
        assert "passing_yards" in result.columns
        assert result["yahoo_player_id"].dtype == "Int64"
