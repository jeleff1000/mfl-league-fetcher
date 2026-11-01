"""One yards-allowed column (Joe 2026-10-31): def_yards_allowed, with the retiring
total_yds_allowed filling only rows where it is empty until the next promotion."""
from multi_league.transformations.common.dst_brackets import (
    bracket_onehot_sql, scalar_available, scalar_sql,
)


def test_yards_brackets_read_def_yards_allowed_with_legacy_fallback():
    assert scalar_sql("def_yards_allowed", "s", {"def_yards_allowed", "total_yds_allowed"}) == \
        "COALESCE(s.def_yards_allowed, s.total_yds_allowed)"
    assert scalar_sql("def_yards_allowed", "s", {"def_yards_allowed"}) == "s.def_yards_allowed"
    assert "COALESCE(s.def_yards_allowed, s.total_yds_allowed) BETWEEN 100 AND 199" in \
        bracket_onehot_sql("yds_allow_100_199", alias="s", available={"def_yards_allowed", "total_yds_allowed"})


def test_points_brackets_unchanged_and_availability():
    assert bracket_onehot_sql("pts_allow_0", alias="s") == "CASE WHEN s.dst_points_allowed BETWEEN 0 AND 0 THEN 1 ELSE 0 END"
    assert scalar_available("def_yards_allowed", {"total_yds_allowed"})
    assert not scalar_available("def_yards_allowed", {"dst_points_allowed"})
