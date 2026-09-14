import duckdb
import pytest
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_aggregate_transaction_manager_season_raises_when_franchise_id_missing():
    """Bug #1.7: warn-and-skip on missing franchise_id is replaced with a
    loud KeyError so the franchise_id no-fallback invariant fails fast
    rather than producing silently-empty aggregates."""
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_season,
        create_transaction_manager_season_table,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "transaction_manager_season"))

    # transactions table created WITHOUT franchise_id column
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            transaction_type VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?)",
        ["demo_league", "Alice", 2024, "add"],
    )

    create_transaction_manager_season_table(conn, "demo_league")

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_transaction_manager_season(conn, "demo_league")


def test_aggregate_transaction_manager_season_inserts_scoped_row():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_season,
        create_transaction_manager_season_table,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "transaction_manager_season"))

    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            year INTEGER,
            transaction_type VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?)",
        [
            ("demo_league", "Alice", "fid_alice", 2024, "add"),
            ("demo_league", "Alice", "fid_alice", 2024, "drop"),
            ("other_league", "Bob", "fid_bob", 2024, "add"),
        ],
    )

    create_transaction_manager_season_table(conn, "demo_league")

    count = aggregate_transaction_manager_season(conn, "demo_league")
    row = conn.execute(
        """
        SELECT db_name, manager, year, adds, drops, total_moves, trades
        FROM ___leagues.public.transaction_manager_season
        WHERE db_name = 'demo_league'
        """
    ).fetchone()

    assert count == 1
    assert row == ("demo_league", "Alice", 2024, 1, 1, 2, 0)


def test_aggregate_transaction_manager_career_uses_named_columns_with_schema_order_drift():
    from multi_league.core.aggregate_ddl import (
        TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        create_aggregate_table_sql,
    )
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_career,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "transaction_manager_season"))

    drifted_order = [
        "db_name",
        "manager",
        "franchise_id",
        "total_moves",
        "total_faab_bid",
        "net_lamar",
        "seasons",
        "total_adds",
        "total_drops",
        "total_trades",
        *[
            col
            for col in TRANSACTION_MANAGER_CAREER_COLUMN_TYPES
            if col
            not in {
                "db_name",
                "manager",
                "franchise_id",
                "total_moves",
                "total_faab_bid",
                "net_lamar",
                "seasons",
                "total_adds",
                "total_drops",
                "total_trades",
            }
        ],
    ]
    cols_sql = ", ".join(f'"{col}" {TRANSACTION_MANAGER_CAREER_COLUMN_TYPES[col]}' for col in drifted_order)
    conn.execute(f"CREATE TABLE public.transaction_manager_career ({cols_sql})")

    conn.executemany(
        """
        INSERT INTO public.transaction_manager_season (
            db_name, manager, year, franchise_id, adds, drops, total_moves,
            total_faab_bid, net_lamar, net_points_ros, total_transaction_score,
            avg_transaction_score, avg_timing_mult, trades, trade_net_lamar,
            trade_wins, trade_net_points
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("demo_league", "Alice", 2023, "fid_a", 2, 1, 3, 10.0, 20.0, 5.0, 30.0, 10.0, 1.0, 1, 4.0, 1, 7.0),
            ("demo_league", "Alice", 2024, "fid_a", 3, 2, 5, 15.0, 25.0, 6.0, 40.0, 8.0, 1.5, 2, 6.0, 1, 9.0),
        ],
    )

    aggregate_transaction_manager_career(conn, "demo_league")

    row = conn.execute(
        """
        SELECT manager, franchise_id, seasons, total_adds, total_drops, total_moves,
               total_faab_bid, net_lamar, total_transaction_score, total_trades,
               trade_net_lamar, trade_win_rate, trade_net_points
        FROM public.transaction_manager_career
        WHERE db_name = 'demo_league'
        """
    ).fetchone()

    assert row == ("Alice", "fid_a", 2, 5, 3, 8, 25.0, 45.0, 36.25, 3, 10.0, 2 / 3, 16.0)


def test_aggregate_transaction_report_card_separates_duplicate_manager_names_by_franchise_id():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_season,
        aggregate_transaction_report_card,
        create_transaction_manager_season_table,
        create_transaction_report_card_table,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "transaction_manager_season"))
    conn.execute(create_aggregate_table_sql("___leagues", "transaction_report_card"))

    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            transaction_type VARCHAR,
            player VARCHAR,
            position VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            faab_bid DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("demo_league", "Ryan", "fid_a", 2024, 1, "add", "Player A", "QB", 5.0, 10.0),
            ("demo_league", "Ryan", "fid_b", 2024, 2, "add", "Player B", "RB", 7.5, 0.0),
        ],
    )

    create_transaction_manager_season_table(conn, "demo_league")
    create_transaction_report_card_table(conn, "demo_league")

    season_count = aggregate_transaction_manager_season(conn, "demo_league")
    report_count = aggregate_transaction_report_card(conn, "demo_league")
    rows = conn.execute(
        """
        SELECT manager, franchise_id, year, adds, trades
        FROM ___leagues.public.transaction_report_card
        WHERE db_name = 'demo_league'
        ORDER BY franchise_id
        """
    ).fetchall()

    assert season_count == 2
    assert report_count == 2
    assert rows == [
        ("Ryan", "fid_a", 2024, 1, 0),
        ("Ryan", "fid_b", 2024, 1, 0),
    ]
