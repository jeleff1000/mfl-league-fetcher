"""Tests for shared rostered/unrostered filters."""

import pytest
import pandas as pd

from multi_league.shared.filters import (
    UNROSTERED_VALUES,
    is_rostered_value,
    rostered_mask,
    unrostered_mask,
    rostered_filter_sql,
    unrostered_filter_sql,
)


class TestIsRosteredValue:
    """Test the single-value check."""

    def test_real_manager(self):
        assert is_rostered_value("Tom") is True

    def test_real_manager_with_spaces(self):
        assert is_rostered_value("  Tom  ") is True

    def test_unrostered(self):
        assert is_rostered_value("Unrostered") is False

    def test_unrostered_lowercase(self):
        assert is_rostered_value("unrostered") is False

    def test_fa(self):
        assert is_rostered_value("FA") is False

    def test_free_agent(self):
        assert is_rostered_value("Free Agent") is False

    def test_waivers(self):
        assert is_rostered_value("Waivers") is False

    def test_none(self):
        assert is_rostered_value(None) is False

    def test_nan(self):
        assert is_rostered_value(float("nan")) is False

    def test_pandas_na(self):
        assert is_rostered_value(pd.NA) is False

    def test_empty_string(self):
        assert is_rostered_value("") is False

    def test_whitespace_only(self):
        assert is_rostered_value("   ") is False

    def test_mixed_case(self):
        assert is_rostered_value("UNROSTERED") is False
        assert is_rostered_value("Free AGENT") is False


class TestRosteredMask:
    """Test DataFrame filtering."""

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "manager": ["Tom", "Jerry", "Unrostered", "FA", "Free Agent", "Waivers", None, "", "  ", "Alice"],
                "points": range(10),
            }
        )

    def test_filters_correctly(self, sample_df):
        mask = rostered_mask(sample_df)
        result = sample_df[mask]["manager"].tolist()
        assert result == ["Tom", "Jerry", "Alice"]

    def test_unrostered_is_inverse(self, sample_df):
        r = rostered_mask(sample_df)
        u = unrostered_mask(sample_df)
        assert (r | u).all()
        assert not (r & u).any()

    def test_custom_column(self):
        df = pd.DataFrame(
            {
                "managers": ["Tom", "Unrostered", "Jerry"],
            }
        )
        mask = rostered_mask(df, col="managers")
        assert df[mask]["managers"].tolist() == ["Tom", "Jerry"]

    def test_missing_column(self):
        df = pd.DataFrame({"points": [1, 2, 3]})
        mask = rostered_mask(df, col="manager")
        assert not mask.any()

    def test_empty_dataframe(self):
        df = pd.DataFrame({"manager": pd.Series([], dtype=str)})
        mask = rostered_mask(df)
        assert len(mask) == 0


class TestRosteredFilterSql:
    """Test SQL generation."""

    def test_no_alias(self):
        sql = rostered_filter_sql()
        assert "manager IS NOT NULL" in sql
        assert "LOWER(TRIM(manager))" in sql
        assert "'unrostered'" in sql
        assert "'fa'" in sql
        assert "'free agent'" in sql
        assert "'waivers'" in sql

    def test_with_alias(self):
        sql = rostered_filter_sql("p")
        assert "p.manager IS NOT NULL" in sql
        assert "LOWER(TRIM(p.manager))" in sql

    def test_managers_column(self):
        sql = rostered_filter_sql("fs", "managers")
        assert "fs.managers IS NOT NULL" in sql

    def test_coalesce_included(self):
        sql = rostered_filter_sql(include_coalesce=True)
        assert "COALESCE(manager, '')" in sql

    def test_coalesce_excluded(self):
        sql = rostered_filter_sql(include_coalesce=False)
        assert "COALESCE" not in sql

    def test_all_unrostered_values_present(self):
        sql = rostered_filter_sql()
        for val in UNROSTERED_VALUES:
            assert f"'{val}'" in sql


class TestUnrosteredFilterSql:
    """Test unrostered SQL generation."""

    def test_basic(self):
        sql = unrostered_filter_sql()
        assert "LOWER(TRIM(COALESCE(manager, '')))" in sql
        assert "'unrostered'" in sql

    def test_with_alias(self):
        sql = unrostered_filter_sql("p")
        assert "p.manager" in sql

    def test_excludes_empty_string_from_in_list(self):
        # Empty string shouldn't be in the IN list for unrostered check
        # (it's handled by the COALESCE)
        sql = unrostered_filter_sql()
        # The empty string value is excluded from the IN list
        values_part = sql.split("IN (")[1].rstrip(")")
        assert "''" not in values_part
