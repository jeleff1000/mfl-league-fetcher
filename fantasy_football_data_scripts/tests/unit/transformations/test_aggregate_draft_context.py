import duckdb
import pytest
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _attach_leagues_db():
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    return conn


def test_aggregate_draft_manager_season_raises_when_franchise_id_missing():
    """Bug #1.8: warn-and-skip on missing franchise_id is replaced with a
    loud KeyError so the franchise_id no-fallback invariant fails fast
    rather than producing silently-empty aggregates."""
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_season,
    )

    conn = _attach_leagues_db()
    conn.execute(create_aggregate_table_sql("___leagues", "draft_manager_season"))

    # draft table created WITHOUT franchise_id column. manager_lamar present
    # so the upstream "no LAMAR" early-exit doesn't shadow the franchise_id check.
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            manager_lamar DOUBLE
        )
        """
    )
    conn.execute(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?)",
        ["demo_league", "Alice", 2024, 5.0],
    )

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_draft_manager_season(conn, "demo_league")


def test_aggregate_draft_manager_career_raises_when_franchise_id_missing():
    """Bug #1.8: career aggregation reads from draft_manager_season; if that
    upstream table somehow lacks franchise_id, fail loudly instead of returning
    0 silently."""
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career,
    )

    conn = _attach_leagues_db()
    conn.execute(create_aggregate_table_sql("___leagues", "draft_manager_career"))

    # Hand-rolled draft_manager_season without franchise_id (canonical DDL
    # always includes it, so we have to drop it explicitly).
    conn.execute(
        """
        CREATE TABLE public.draft_manager_season (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            picks INTEGER,
            total_manager_lamar DOUBLE,
            manager_draft_grade VARCHAR
        )
        """
    )

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_draft_manager_career(conn, "demo_league")


def test_aggregate_draft_manager_career_uses_named_columns_with_schema_order_drift():
    from multi_league.core.aggregate_ddl import (
        DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        create_aggregate_table_sql,
    )
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career,
    )

    conn = _attach_leagues_db()
    conn.execute(create_aggregate_table_sql("___leagues", "draft_manager_season"))

    drifted_order = [
        "db_name",
        "manager",
        "franchise_id",
        "draft_category",
        "total_manager_lamar",
        "avg_manager_lamar",
        "years_active",
        "total_picks",
        "total_keeper_picks",
        "total_cost",
        *[
            col
            for col in DRAFT_MANAGER_CAREER_COLUMN_TYPES
            if col
            not in {
                "db_name",
                "manager",
                "franchise_id",
                "draft_category",
                "total_manager_lamar",
                "avg_manager_lamar",
                "years_active",
                "total_picks",
                "total_keeper_picks",
                "total_cost",
            }
        ],
    ]
    cols_sql = ", ".join(f'"{col}" {DRAFT_MANAGER_CAREER_COLUMN_TYPES[col]}' for col in drifted_order)
    conn.execute(f"CREATE TABLE public.draft_manager_career ({cols_sql})")

    conn.executemany(
        """
        INSERT INTO public.draft_manager_season (
            db_name, manager, year, franchise_id, draft_category, picks, keeper_picks,
            total_cost, total_manager_lamar, total_fantasy_points, avg_season_ppg,
            hits, busts, breakouts, avg_pick_quality_zscore, avg_games_played,
            manager_draft_grade, best_pick_player, best_pick_lamar, worst_pick_player, worst_pick_lamar
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "demo_league",
                "Alice",
                2023,
                "fid_a",
                "standard",
                2,
                0,
                10.0,
                30.0,
                100.0,
                8.0,
                1,
                0,
                1,
                0.2,
                14.0,
                "A",
                "Best A",
                12.0,
                "Worst A",
                -3.0,
            ),
            (
                "demo_league",
                "Alice",
                2024,
                "fid_a",
                "standard",
                3,
                1,
                20.0,
                45.0,
                150.0,
                9.0,
                2,
                1,
                0,
                0.4,
                15.0,
                "B",
                "Best B",
                18.0,
                "Worst B",
                -4.0,
            ),
        ],
    )

    aggregate_draft_manager_career(conn, "demo_league")

    row = conn.execute(
        """
        SELECT manager, franchise_id, years_active, total_picks, total_keeper_picks,
               total_cost, total_manager_lamar, career_hit_rate, total_busts, total_breakouts
        FROM public.draft_manager_career
        WHERE db_name = 'demo_league'
        """
    ).fetchone()

    assert row == ("Alice", "fid_a", 2, 5, 1, 30.0, 75.0, 0.6, 1, 1)


def test_aggregate_draft_player_career_raises_when_franchise_id_missing():
    """Bug #1.8: draft_player_career used to fall back to STRING_AGG -> NULL
    when franchise_id was absent. Hardening: raise KeyError instead so the
    no-fallback invariant fails fast."""
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_player_career,
    )

    conn = _attach_leagues_db()
    conn.execute(create_aggregate_table_sql("___leagues", "draft_player_career"))

    # draft table without franchise_id, but WITH manager_lamar + yahoo_position
    # so the upstream "Missing required columns" early-exit doesn't shadow
    # the franchise_id check.
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            year INTEGER,
            manager_lamar DOUBLE,
            yahoo_position VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?)",
        ["demo_league", "Player A", "Alice", 2024, 5.0, "QB"],
    )

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_draft_player_career(conn, "demo_league")
