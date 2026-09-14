"""Tests for all_play, schedule_swap, h2h_season, and schedule_swap_season enrichments."""

import duckdb
import pytest


@pytest.fixture
def luck_db():
    conn = duckdb.connect(":memory:")
    # ATTACH a second in-memory database so we can use catalog-qualified names
    conn.execute("ATTACH ':memory:' AS test_luck")
    conn.execute("CREATE SCHEMA test_luck.public")
    conn.execute("""
        CREATE TABLE test_luck.public.matchup AS
        SELECT * FROM (VALUES
            ('test_luck', 2025, 1, 'A', 100.0, 'B', 90.0, 0, 0, 'fid_a', 'fid_b'),
            ('test_luck', 2025, 1, 'B', 90.0, 'A', 100.0, 0, 0, 'fid_b', 'fid_a'),
            ('test_luck', 2025, 2, 'A', 80.0, 'B', 85.0, 0, 0, 'fid_a', 'fid_b'),
            ('test_luck', 2025, 2, 'B', 85.0, 'A', 80.0, 0, 0, 'fid_b', 'fid_a')
        ) AS t(db_name, year, week, manager, team_points, opponent, opponent_points,
               is_playoffs, is_consolation, franchise_id, opponent_franchise_id)
    """)
    yield conn, "test_luck"
    conn.close()


def _make_enrichments(conn, db_name):
    """Create a MatchupEnrichmentsMixin instance wired to the test connection."""
    from multi_league.transformations.matchup.sql_matchup_enrichments import (
        MatchupEnrichmentsMixin,
    )

    class _Stub(MatchupEnrichmentsMixin):
        """Minimal stub providing the base-class API the mixin needs."""

        def __init__(self, conn, db_name):
            self._conn = conn
            self.db_name = db_name
            self.schema = "public"
            self.data_dir = None  # MotherDuck-style qualified names

        def _qualified_name(self, table_name):
            return f"{self.db_name}.{self.schema}.{table_name}"

        def _table_exists(self, table_name):
            qn = self._qualified_name(table_name)
            try:
                self._conn.execute(f"SELECT 1 FROM {qn} LIMIT 0")
                return True
            except Exception:
                return False

        def _execute(self, sql, description):
            self._conn.execute(sql)
            return 1

        def _db_filter(self, alias=""):
            prefix = f"{alias}." if alias else ""
            return f"{prefix}db_name = '{self.db_name}'"

        def _get_connection(self):
            return self._conn

        def _column_exists(self, table_name, column_name):
            qn = self._qualified_name(table_name)
            try:
                cols = {r[0] for r in self._conn.execute(f"DESCRIBE {qn}").fetchall()}
                return column_name in cols
            except Exception:
                return False

    return _Stub(conn, db_name)


class TestBuildAllPlay:
    """Tests for build_all_play enrichment."""

    def test_row_count(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_all_play()

        rows = conn.execute(f"SELECT COUNT(*) FROM {db_name}.public.all_play").fetchone()[0]
        # 2 managers * 1 opponent each * 2 weeks = 4 rows
        assert rows == 4

    def test_result_correctness(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_all_play()

        # Week 1: A(100) > B(90) -> W for A
        w1 = conn.execute(
            f"""SELECT result FROM {db_name}.public.all_play
                WHERE franchise_id = 'fid_a' AND week = 1"""
        ).fetchone()[0]
        assert w1 == "W"

        # Week 2: A(80) < B(85) -> L for A
        w2 = conn.execute(
            f"""SELECT result FROM {db_name}.public.all_play
                WHERE franchise_id = 'fid_a' AND week = 2"""
        ).fetchone()[0]
        assert w2 == "L"

    def test_excludes_playoffs(self, luck_db):
        conn, db_name = luck_db
        # Add a playoff row
        conn.execute(f"""
            INSERT INTO {db_name}.public.matchup VALUES
            ('{db_name}', 2025, 3, 'A', 110.0, 'B', 95.0, 1, 0, 'fid_a', 'fid_b'),
            ('{db_name}', 2025, 3, 'B', 95.0, 'A', 110.0, 1, 0, 'fid_b', 'fid_a')
        """)
        eng = _make_enrichments(conn, db_name)
        eng.build_all_play()

        rows = conn.execute(f"SELECT COUNT(*) FROM {db_name}.public.all_play").fetchone()[0]
        # Still 4 -- playoff rows excluded
        assert rows == 4


class TestBuildH2HSeason:
    """Tests for build_h2h_season enrichment (aggregation of all_play)."""

    def test_season_aggregation(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_all_play()
        eng.build_h2h_season()

        row = conn.execute(
            f"""SELECT wins, losses, ties, games
                FROM {db_name}.public.h2h_season
                WHERE franchise_id = 'fid_a'
                  AND opponent_franchise_id = 'fid_b'"""
        ).fetchone()
        # A vs B: W1=W, W2=L -> 1 win, 1 loss, 0 ties, 2 games
        assert row == (1, 1, 0, 2)


class TestBuildScheduleSwap:
    """Tests for build_schedule_swap enrichment."""

    def test_row_count(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_schedule_swap()

        rows = conn.execute(f"SELECT COUNT(*) FROM {db_name}.public.schedule_swap").fetchone()[0]
        # 2 managers * own schedule only * 2 weeks; impossible self-opponent swaps are skipped.
        assert rows == 4

    def test_skips_self_opponent_from_other_schedule(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_schedule_swap()

        row = conn.execute(
            f"""SELECT result, my_points, their_opponent_points
                FROM {db_name}.public.schedule_swap
                WHERE franchise_id = 'fid_a'
                  AND schedule_of_franchise_id = 'fid_b'
                  AND week = 1"""
        ).fetchone()
        assert row is None

    def test_includes_own_schedule(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_schedule_swap()

        row = conn.execute(
            f"""SELECT result, my_points, their_opponent_points
                FROM {db_name}.public.schedule_swap
                WHERE franchise_id = 'fid_a'
                  AND schedule_of_franchise_id = 'fid_a'
                  AND week = 1"""
        ).fetchone()
        assert row == ("W", 100.0, 90.0)


class TestBuildScheduleSwapSeason:
    """Tests for build_schedule_swap_season enrichment."""

    def test_row_count(self, luck_db):
        conn, db_name = luck_db
        eng = _make_enrichments(conn, db_name)
        eng.build_schedule_swap()
        eng.build_schedule_swap_season()

        rows = conn.execute(f"SELECT COUNT(*) FROM {db_name}.public.schedule_swap_season").fetchone()[0]
        # 2 franchise_id * own schedule only; impossible self-opponent swaps are skipped.
        assert rows == 2
