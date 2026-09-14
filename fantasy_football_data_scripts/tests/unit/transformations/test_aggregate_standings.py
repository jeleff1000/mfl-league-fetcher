import duckdb
import pytest


def _db_name(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def test_aggregate_standings_raises_when_franchise_id_missing():
    """Bug #1.7: warn-and-skip on missing franchise_id is replaced with a
    loud KeyError so the franchise_id no-fallback invariant fails fast
    rather than producing silently-empty standings_by_year rows."""
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = _db_name(conn)

    # matchup table has opponent_franchise_id but NOT franchise_id
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE
        )
        """
    )

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_standings(conn, db_name, [2024])


def test_aggregate_standings_raises_when_opponent_franchise_id_missing():
    """Bug #1.7: warn-and-skip on missing opponent_franchise_id is replaced
    with a loud KeyError."""
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = _db_name(conn)

    # matchup table has franchise_id but NOT opponent_franchise_id
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE
        )
        """
    )

    with pytest.raises(KeyError, match="opponent_franchise_id"):
        aggregate_standings(conn, db_name, [2024])


def test_aggregate_standings_normalizes_text_week_columns():
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = _db_name(conn)

    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week VARCHAR,
            manager VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win VARCHAR,
            loss VARCHAR,
            is_playoffs VARCHAR,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            champion VARCHAR,
            sacko VARCHAR,
            is_consolation VARCHAR,
            final_playoff_seed VARCHAR,
            is_bye_week VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            above_league_median VARCHAR
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE public.league_settings (
            db_name VARCHAR DEFAULT '{db_name}',
            year INTEGER,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(f"INSERT INTO public.league_settings VALUES ('{db_name}', 2024, TRUE)")

    rows = [
        (
            db_name,
            2024,
            "1",
            "Alice",
            "Alpha",
            "Bob",
            110.0,
            100.0,
            "1",
            "0",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "1",
            "0",
            "A",
            "B",
            "1",
        ),
        (
            db_name,
            2024,
            "1",
            "Bob",
            "Beta",
            "Alice",
            100.0,
            110.0,
            "0",
            "1",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "2",
            "0",
            "B",
            "A",
            "0",
        ),
        (
            db_name,
            2024,
            "15",
            "Alice",
            "Alpha",
            "Bob",
            120.0,
            90.0,
            "1",
            "0",
            "1",
            "championship",
            None,
            "1",
            "0",
            "0",
            "1",
            "0",
            "A",
            "B",
            "1",
        ),
        (
            db_name,
            2024,
            "15",
            "Bob",
            "Beta",
            "Alice",
            90.0,
            120.0,
            "0",
            "1",
            "1",
            "championship",
            None,
            "0",
            "0",
            "0",
            "2",
            "0",
            "B",
            "A",
            "0",
        ),
    ]
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )

    aggregate_standings(conn, db_name, [2024])

    result = conn.execute(
        """
        SELECT manager, wins, losses, above_median_wins, total_wins, final_result, seed
        FROM public.standings_by_year
        ORDER BY seed
        """
    ).fetchall()

    assert result == [
        ("Alice", 1, 0, 1, 2, "Won Championship", 1),
        ("Bob", 0, 1, 0, 0, "Lost Championship", 2),
    ]


def test_aggregate_standings_keeps_duplicate_manager_names_separate_by_franchise_id():
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = _db_name(conn)

    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week VARCHAR,
            manager VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win VARCHAR,
            loss VARCHAR,
            is_playoffs VARCHAR,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            champion VARCHAR,
            sacko VARCHAR,
            is_consolation VARCHAR,
            final_playoff_seed VARCHAR,
            is_bye_week VARCHAR,
            franchise_id VARCHAR,
            above_league_median VARCHAR
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE public.league_settings (
            db_name VARCHAR DEFAULT '{db_name}',
            year INTEGER,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(f"INSERT INTO public.league_settings VALUES ('{db_name}', 2024, FALSE)")

    rows = [
        (
            db_name,
            2024,
            "1",
            "Alex",
            "Alpha",
            "Bob",
            "fid_b",
            110.0,
            90.0,
            "1",
            "0",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "1",
            "0",
            "fid_a",
            "0",
        ),
        (
            db_name,
            2024,
            "1",
            "Bob",
            "Beta",
            "Alex",
            "fid_a",
            90.0,
            110.0,
            "0",
            "1",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "2",
            "0",
            "fid_b",
            "0",
        ),
        (
            db_name,
            2024,
            "1",
            "Alex",
            "Gamma",
            "Carl",
            "fid_d",
            95.0,
            100.0,
            "0",
            "1",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "3",
            "0",
            "fid_c",
            "0",
        ),
        (
            db_name,
            2024,
            "1",
            "Carl",
            "Delta",
            "Alex",
            "fid_c",
            100.0,
            95.0,
            "1",
            "0",
            "0",
            None,
            None,
            "0",
            "0",
            "0",
            "4",
            "0",
            "fid_d",
            "0",
        ),
    ]
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    aggregate_standings(conn, db_name, [2024])

    result = conn.execute(
        """
        SELECT manager, franchise_id, team_name, wins, losses
        FROM public.standings_by_year
        ORDER BY franchise_id
        """
    ).fetchall()

    assert result == [
        ("Alex", "fid_a", "Alpha", 1, 0),
        ("Bob", "fid_b", "Beta", 0, 1),
        ("Alex", "fid_c", "Gamma", 0, 1),
        ("Carl", "fid_d", "Delta", 1, 0),
    ]
