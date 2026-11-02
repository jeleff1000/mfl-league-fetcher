"""One yards-allowed column (Joe 2026-10-31): def_yards_allowed, with the retiring
total_yds_allowed filling only rows where it is empty until the next promotion."""
import duckdb
import pytest

from multi_league.transformations.common.dst_brackets import (
    bracket_onehot_sql, bracket_scored_sql, scalar_available, scalar_sql,
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


@pytest.mark.parametrize("columns, rows, expected", [
    (["total_yds_allowed"], [(150,), (250,), (None,)], [1, 0, 0]),
    (["def_yards_allowed"], [(150,), (250,), (None,)], [1, 0, 0]),
    (
        ["def_yards_allowed", "total_yds_allowed"],
        [(150, 250), (250, 150), (None, 150), (None, None), (0, 150)],
        [1, 0, 1, 0, 0],
    ),
])
@pytest.mark.parametrize("alias", [None, "s"])
def test_yards_bracket_generated_sql_executes_for_available_schema(columns, rows, expected, alias):
    """Bind and execute the expression, including the legacy-only live schema."""
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE stats (" + ", ".join(f"{col} DOUBLE" for col in columns) + ")")
        conn.executemany("INSERT INTO stats VALUES (" + ", ".join("?" for _ in columns) + ")", rows)
        expression = bracket_scored_sql("yds_allow_100_199", 3, alias=alias, available=set(columns))
        relation = "stats AS s" if alias else "stats"
        assert conn.execute(f"SELECT {expression} FROM {relation}").fetchall() == [
            (value * 3,) for value in expected
        ]


def test_legacy_only_scalar_sql_never_references_missing_canonical_column():
    assert scalar_sql("def_yards_allowed", "s", {"total_yds_allowed"}) == "s.total_yds_allowed"
