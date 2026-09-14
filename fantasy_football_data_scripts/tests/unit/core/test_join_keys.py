"""Tests for canonical join-key helpers."""

import pandas as pd
import polars as pl
import sys
from pathlib import Path

_test_dir = Path(__file__).resolve().parent
_tests_dir = _test_dir.parent.parent
_scripts_dir = _tests_dir.parent
sys.path.insert(0, str(_scripts_dir))

from multi_league.core.join_keys import (
    build_manager_week_series,
    canonical_cumulative_week_series,
    ensure_cumulative_week_column,
    franchise_identity_column_name,
    franchise_identity_join_sql,
    franchise_identity_sql_ref,
    franchise_identity_sql_select,
    manager_week_expr,
)


def test_canonical_cumulative_week_series_strips_float_suffix():
    series = pd.Series([202517.0, "202516.0", 202515, None])

    result = canonical_cumulative_week_series(series)

    assert result.iloc[0] == "202517"
    assert result.iloc[1] == "202516"
    assert result.iloc[2] == "202515"
    assert pd.isna(result.iloc[3])


def test_ensure_cumulative_week_column_builds_from_year_and_week():
    df = pd.DataFrame({"year": [2025, "2024"], "week": [1, "17"]})

    result = ensure_cumulative_week_column(df.copy())

    assert result["cumulative_week"].tolist() == [202501, 202417]


def test_ensure_cumulative_week_column_preserves_existing_values():
    df = pd.DataFrame({"year": [2025, 2024], "week": [1, 17], "cumulative_week": [202501.0, None]})

    result = ensure_cumulative_week_column(df.copy())

    assert result["cumulative_week"].tolist() == [202501, 202417]


def test_build_manager_week_prefers_franchise_id_over_manager_name():
    df = pd.DataFrame(
        {
            "manager": ["Dave", "Dave"],
            "manager_guid": ["guid_a", "guid_b"],
            "franchise_id": ["fid_a", "fid_b"],
            "cumulative_week": [202517.0, 202517.0],
        }
    )

    result = build_manager_week_series(df)

    assert result.tolist() == ["fid_a202517", "fid_b202517"]


def test_build_manager_week_falls_back_to_manager_guid_then_manager_name():
    df = pd.DataFrame(
        {
            "manager": ["Dave One", "Dave Two", "Unrostered"],
            "manager_guid": ["guid_a", None, None],
            "cumulative_week": [202517.0, "202517.0", 202517],
        }
    )

    result = build_manager_week_series(df)

    assert result.iloc[0] == "guid_a202517"
    assert result.iloc[1] == "DaveTwo202517"
    assert pd.isna(result.iloc[2])


def test_build_manager_week_uses_team_name_for_hidden_or_unknown_managers():
    df = pd.DataFrame(
        {
            "manager": ["Unknown", "Unknown"],
            "manager_guid": ["--", "--"],
            "team_name": ["Jets Legacy", "Giants Legacy"],
            "cumulative_week": [202517.0, 202517.0],
        }
    )

    result = build_manager_week_series(df)

    assert result.iloc[0] == "JetsLegacy202517"
    assert result.iloc[1] == "GiantsLegacy202517"


def test_manager_week_expr_matches_pandas_behavior():
    df = pl.DataFrame(
        {
            "manager": ["Dave", "Dave", "Unrostered"],
            "manager_guid": ["guid_a", "guid_b", None],
            "franchise_id": ["fid_a", None, None],
            "cumulative_week": ["202517.0", "202517.0", "202517"],
        }
    )

    result = df.with_columns(manager_week_expr(df.columns).alias("manager_week"))

    assert result["manager_week"].to_list() == ["fid_a202517", "guid_b202517", None]


def test_manager_week_expr_uses_team_name_for_hidden_guid_rows():
    df = pl.DataFrame(
        {
            "manager": ["Unknown", "Unknown"],
            "manager_guid": ["--", "--"],
            "team_name": ["Jets Legacy", "Giants Legacy"],
            "cumulative_week": ["202517.0", "202517.0"],
        }
    )

    result = df.with_columns(manager_week_expr(df.columns).alias("manager_week"))

    assert result["manager_week"].to_list() == ["JetsLegacy202517", "GiantsLegacy202517"]


def test_franchise_identity_sql_helpers_prefer_franchise_id():
    assert franchise_identity_column_name(True) == "franchise_id"
    assert franchise_identity_column_name(False) == "franchise_id"
    assert franchise_identity_sql_ref("m", True) == "m.franchise_id"
    assert franchise_identity_sql_select("m", True, output_alias="_grp_key") == "m.franchise_id AS _grp_key"


def test_franchise_identity_join_sql_stays_on_franchise_id_when_flag_is_false():
    sql = franchise_identity_join_sql(
        "left_table",
        "right_table",
        left_has_franchise_id=False,
        right_has_franchise_id=False,
        extra_conditions=["left_table.year = right_table.year"],
    )

    assert sql == "left_table.franchise_id = right_table.franchise_id AND left_table.year = right_table.year"
