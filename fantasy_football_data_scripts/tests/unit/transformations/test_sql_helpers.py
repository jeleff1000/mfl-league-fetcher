"""Tests for SQL builder helpers."""

from multi_league.transformations.common.sql_helpers import update_from_subquery


class TestUpdateFromSubquery:
    def test_simple_update(self):
        sql = update_from_subquery(
            table="player_fantasy",
            set_cols={"manager_lamar": "sub.lamar_value"},
            subquery_sql="SELECT player_week, lamar_value FROM lamar_calc",
            join_condition="player_fantasy.player_week = sub.player_week",
        )
        assert "UPDATE player_fantasy" in sql
        assert "SET manager_lamar = sub.lamar_value" in sql
        assert "FROM (SELECT player_week, lamar_value FROM lamar_calc) AS sub" in sql
        assert "WHERE player_fantasy.player_week = sub.player_week" in sql

    def test_multiple_set_cols(self):
        sql = update_from_subquery(
            table="draft",
            set_cols={
                "pick_quality_score": "sub.pqs",
                "grade": "sub.grade",
            },
            subquery_sql="SELECT pick, pqs, grade FROM draft_grades",
            join_condition="draft.pick = sub.pick AND draft.year = sub.year",
        )
        assert "pick_quality_score = sub.pqs" in sql
        assert "grade = sub.grade" in sql

    def test_with_where_filter(self):
        sql = update_from_subquery(
            table="player_fantasy",
            set_cols={"optimal_points": "sub.opt"},
            subquery_sql="SELECT player_week, opt FROM optimal",
            join_condition="player_fantasy.player_week = sub.player_week",
            where_filter="player_fantasy.year = 2024",
        )
        assert "WHERE player_fantasy.player_week = sub.player_week" in sql
        assert "AND player_fantasy.year = 2024" in sql

    def test_custom_alias(self):
        sql = update_from_subquery(
            table="matchup",
            set_cols={"win": "calc.win"},
            subquery_sql="SELECT id, win FROM wins",
            join_condition="matchup.id = calc.id",
            subquery_alias="calc",
        )
        assert "AS calc" in sql
        assert "matchup.id = calc.id" in sql

    def test_returns_string(self):
        sql = update_from_subquery(
            table="t",
            set_cols={"a": "sub.a"},
            subquery_sql="SELECT a FROM s",
            join_condition="t.id = sub.id",
        )
        assert isinstance(sql, str)

    def test_no_where_filter_by_default(self):
        sql = update_from_subquery(
            table="t",
            set_cols={"a": "sub.a"},
            subquery_sql="SELECT a FROM s",
            join_condition="t.id = sub.id",
        )
        # Should not contain AND after WHERE (only the join condition)
        lines = sql.strip().split("\n")
        and_lines = [l for l in lines if l.strip().startswith("AND")]
        assert len(and_lines) == 0
