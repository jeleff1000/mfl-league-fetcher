"""Whole-point-bucket (uses_fractional_points=0) scoring, the pre-~2014 Yahoo era.

Old Yahoo floored each stat's point contribution to a whole number (1 pt per 50
passing yards, 1 pt per 10 rush/receiving yards), then summed. The settings rate
is identical to the decimal era, so `uses_fractional_points` is the only signal
that distinguishes the two rulesets. Applying the linear rate to a floored season
over-counts the sub-bucket remainder — the exact source of the lineup-vs-team-score
drift in 2003-2017. These tests pin that the recompute floors per component when
the flag is 0 and stays linear when it is 1/absent.
"""

import duckdb

from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row
from multi_league.transformations.player.modules.scoring_calculator import (
    build_components_fantasy_points_sql,
)

YARDAGE_RULES = {"pass_yd": 0.02, "rush_yd": 0.1, "rec_yd": 0.1}


def test_floor_applied_only_when_not_fractional():
    floored = build_components_fantasy_points_sql({**YARDAGE_RULES, "uses_fractional_points": False}, table_alias="s")
    linear = build_components_fantasy_points_sql({**YARDAGE_RULES, "uses_fractional_points": True}, table_alias="s")
    assert "FLOOR(" in floored
    assert "FLOOR(" not in linear
    # Absent flag defaults to decimal (no floor).
    assert "FLOOR(" not in build_components_fantasy_points_sql(YARDAGE_RULES, table_alias="s")


def test_floored_passing_yards_match_yahoo_whole_point_bucket():
    floored = build_components_fantasy_points_sql({"pass_yd": 0.02, "uses_fractional_points": False}, table_alias="s")
    linear = build_components_fantasy_points_sql({"pass_yd": 0.02, "uses_fractional_points": True}, table_alias="s")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE s (passing_yards DOUBLE)")
    con.execute("INSERT INTO s VALUES (181)")  # 181 * 0.02 = 3.62
    assert con.execute(f"SELECT {floored} FROM s").fetchone()[0] == 3.0  # FLOOR(3.62)
    assert round(con.execute(f"SELECT {linear} FROM s").fetchone()[0], 2) == 3.62


def test_floor_applies_to_precomputed_point_columns_too():
    # rush/rec yards resolve to precomputed point columns (pts_*_yd_p1); FLOOR must
    # still apply so 14.1 receiving points (141 yds) floors to 14, not 14.1.
    floored = build_components_fantasy_points_sql({"rec_yd": 0.1, "uses_fractional_points": False}, table_alias="s")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE s (pts_rec_yd_p1 DOUBLE)")
    con.execute("INSERT INTO s VALUES (14.1)")
    assert con.execute(f"SELECT {floored} FROM s").fetchone()[0] == 14.0


def test_flat_row_carries_uses_fractional_points_into_scoring_dict():
    assert (
        extract_scoring_settings_from_flat_row({"scoring_pass_td": 6, "uses_fractional_points": False})[
            "uses_fractional_points"
        ]
        is False
    )
    assert (
        extract_scoring_settings_from_flat_row({"scoring_pass_td": 6, "uses_fractional_points": True})[
            "uses_fractional_points"
        ]
        is True
    )
    # Absent -> decimal default (True) so non-Yahoo / NULL rows are never floored.
    assert extract_scoring_settings_from_flat_row({"scoring_pass_td": 6})["uses_fractional_points"] is True
