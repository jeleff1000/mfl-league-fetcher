"""Tests for the odds-writing helper."""

import pytest
import numpy as np
import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(SCRIPTS_DIR / "multi_league"))
sys.path.insert(0, str(SCRIPTS_DIR / "multi_league" / "transformations" / "matchup"))

from multi_league.transformations.matchup.playoff_odds_import import (
    _ensure_matchup_output_columns,
    normalize_power_rating_by_season,
    write_odds_to_row,
)


class _FakeCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, describe_rows=None):
        self.describe_rows = describe_rows or []
        self.sql = []

    def execute(self, sql):
        self.sql.append(sql)
        if sql.startswith("DESCRIBE "):
            return _FakeCursor(self.describe_rows)
        if "current_database" in sql:
            return _FakeCursor([("test_db",)])
        return _FakeCursor()


class TestWriteOddsToRow:
    def test_writes_standard_cols(self):
        df = pd.DataFrame(
            {
                "manager": ["Alice"],
                "avg_seed": [np.nan],
                "p_playoffs": [np.nan],
                "p_champ": [np.nan],
                "power_rating": [np.nan],
            }
        )
        odds_df = pd.DataFrame(
            {
                "Avg_Seed": [2.5],
                "P_Playoffs": [75.0],
                "P_Champ": [15.0],
                "Power_Rating": [110.5],
            },
            index=["Alice"],
        )
        write_odds_to_row(df, 0, "Alice", odds_df, pd.DataFrame(), None)
        assert df.at[0, "avg_seed"] == 2.5
        assert df.at[0, "p_playoffs"] == 75.0
        assert df.at[0, "p_champ"] == 15.0
        assert df.at[0, "power_rating"] == 110.5

    def test_writes_raw_power_to_temp_column_when_present(self):
        df = pd.DataFrame(
            {
                "manager": ["Alice"],
                "power_rating": [np.nan],
                "_power_rating_raw": [np.nan],
            }
        )
        odds_df = pd.DataFrame({"Power_Rating": [110.5]}, index=["Alice"])

        write_odds_to_row(df, 0, "Alice", odds_df, pd.DataFrame(), None)

        assert df.at[0, "_power_rating_raw"] == 110.5
        assert pd.isna(df.at[0, "power_rating"])

    def test_skips_missing_manager(self):
        df = pd.DataFrame({"manager": ["Alice"], "p_champ": [np.nan]})
        odds_df = pd.DataFrame({"P_Champ": [10.0]}, index=["Bob"])
        write_odds_to_row(df, 0, "Alice", odds_df, pd.DataFrame(), None)
        assert pd.isna(df.at[0, "p_champ"])

    def test_writes_seed_cols(self):
        df = pd.DataFrame(
            {
                "manager": ["Alice"],
                "x1_seed": [np.nan],
                "x2_seed": [np.nan],
            }
        )
        odds_df = pd.DataFrame({"Avg_Seed": [1.5]}, index=["Alice"])
        seed_df = pd.DataFrame({1: [80.0], 2: [20.0]}, index=["Alice"])
        write_odds_to_row(df, 0, "Alice", odds_df, seed_df, None)
        assert df.at[0, "x1_seed"] == 80.0
        assert df.at[0, "x2_seed"] == 20.0

    def test_writes_win_cols(self):
        df = pd.DataFrame(
            {
                "manager": ["Alice"],
                "x0_win": [np.nan],
                "x5_win": [np.nan],
            }
        )
        odds_df = pd.DataFrame({"Avg_Seed": [1.5]}, index=["Alice"])
        win_df = pd.DataFrame({"x0_win": [10.0], "x5_win": [30.0]}, index=["Alice"])
        write_odds_to_row(df, 0, "Alice", odds_df, pd.DataFrame(), win_df)
        assert df.at[0, "x0_win"] == 10.0
        assert df.at[0, "x5_win"] == 30.0

    def test_win_df_none_skips_wins(self):
        df = pd.DataFrame(
            {
                "manager": ["Alice"],
                "x0_win": [np.nan],
            }
        )
        odds_df = pd.DataFrame({"Avg_Seed": [1.5]}, index=["Alice"])
        write_odds_to_row(df, 0, "Alice", odds_df, pd.DataFrame(), None)
        assert pd.isna(df.at[0, "x0_win"])


def test_normalize_power_rating_by_season_uses_season_median_raw_values():
    df = pd.DataFrame(
        {
            "year": [2024, 2024, 2024, 2025, 2025],
            "power_rating": [np.nan, np.nan, np.nan, np.nan, np.nan],
            "_power_rating_raw": [100.0, 200.0, 300.0, 50.0, 100.0],
        }
    )

    result = normalize_power_rating_by_season(df)

    assert "_power_rating_raw" not in result.columns
    assert list(result["power_rating"]) == [50.0, 100.0, 150.0, 66.67, 133.33]


def test_ensure_matchup_output_columns_uses_canonical_types():
    fake_conn = _FakeConn(describe_rows=[("year", "INTEGER"), ("week", "INTEGER"), ("manager", "VARCHAR")])

    _ensure_matchup_output_columns(fake_conn, "test_db", ["avg_seed", "p_playoffs", "matchup_key", "x36_win"])

    assert any(
        'ALTER TABLE test_db.public.matchup ADD COLUMN IF NOT EXISTS "avg_seed" DOUBLE' in sql for sql in fake_conn.sql
    )
    assert any(
        'ALTER TABLE test_db.public.matchup ADD COLUMN IF NOT EXISTS "p_playoffs" DOUBLE' in sql
        for sql in fake_conn.sql
    )
    assert any(
        'ALTER TABLE test_db.public.matchup ADD COLUMN IF NOT EXISTS "matchup_key" VARCHAR' in sql
        for sql in fake_conn.sql
    )
    assert any(
        'ALTER TABLE test_db.public.matchup ADD COLUMN IF NOT EXISTS "x36_win" DOUBLE' in sql for sql in fake_conn.sql
    )


def test_ensure_matchup_output_columns_rejects_noncanonical_columns():
    fake_conn = _FakeConn()

    with pytest.raises(ValueError, match="mystery_metric"):
        _ensure_matchup_output_columns(fake_conn, "test_db", ["mystery_metric"])


def test_ensure_matchup_output_columns_rejects_redundant_placeholder_flag():
    fake_conn = _FakeConn()

    with pytest.raises(ValueError, match="is_placeholder"):
        _ensure_matchup_output_columns(fake_conn, "test_db", ["is_placeholder"])
