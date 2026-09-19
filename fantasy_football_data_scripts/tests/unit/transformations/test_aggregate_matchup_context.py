import duckdb
import pytest
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def test_h2h_career_refresh_uses_full_chain_without_rewriting_seasons():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import aggregate_matchup_h2h

    with duckdb.connect(":memory:") as conn:
        conn.execute("ATTACH ':memory:' AS ___leagues")
        conn.execute("USE ___leagues")
        conn.execute("CREATE SCHEMA public")
        conn.execute(create_aggregate_table_sql("___leagues", "matchup_h2h_season"))
        conn.execute(create_aggregate_table_sql("___leagues", "matchup_h2h_career"))
        conn.execute("""
            INSERT INTO public.matchup_h2h_season
                (db_name, manager, opponent, franchise_id, opponent_franchise_id, year, games)
            VALUES ('test_league', 'Historical Alias', 'Opponent', 'f1', 'f2', 2025, 14)
        """)
        conn.execute("""
            CREATE TABLE public.matchup (
                db_name VARCHAR, manager VARCHAR, opponent VARCHAR,
                franchise_id VARCHAR, opponent_franchise_id VARCHAR,
                year INTEGER, week INTEGER, team_points DOUBLE, margin DOUBLE,
                win INTEGER, loss INTEGER, tie INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO public.matchup VALUES
              ('test_league', 'Old Name', 'Opponent', 'f1', 'f2', 2025, 1, 100, 10, 1, 0, 0),
              ('test_league', 'Shared Alias', 'Opponent', 'f1', 'f2', 2026, 1, 110, -5, 0, 1, 0),
              ('another_league', 'Other', 'Other Opponent', 'f1', 'f2', 2024, 1, 900, 90, 1, 0, 0)
        """)
        conn.execute("""
            CREATE TABLE public.league_settings AS
            SELECT 'test_league' AS db_name, 2025 AS year, 15 AS playoff_start_week
            UNION ALL SELECT 'test_league', 2026, 15
        """)
        prior_seasons = conn.execute("SELECT * FROM public.matchup_h2h_season").fetchall()
        for current_points, expected_total in [(110.0, 210.0), (110.25, 210.25), (110.25, 210.25)]:
            conn.execute("UPDATE public.matchup SET team_points=? WHERE db_name='test_league' AND year=2026", [current_points])
            aggregate_matchup_h2h(conn, "test_league", season_years=set())
            assert conn.execute("""
                SELECT manager, franchise_id, games, wins, losses, total_team_points, results
                FROM public.matchup_h2h_career WHERE db_name='test_league'
            """).fetchall() == [("Shared Alias", "f1", 2, 1, 1, expected_total, [1, 0])]
            assert conn.execute("SELECT * FROM public.matchup_h2h_season").fetchall() == prior_seasons


def test_aggregate_matchup_season_raises_when_franchise_id_missing():
    """Bug #1.7: warn-and-skip on missing franchise_id is replaced with a
    loud KeyError so the franchise_id no-fallback invariant fails fast
    rather than producing silently-empty matchup_season rows."""
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_matchup_season,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "matchup_season"))

    # matchup table created WITHOUT franchise_id column
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE
        )
        """
    )
    conn.execute(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
        ["demo_league", "Alice", 2024, 1, 100.0],
    )

    with pytest.raises(KeyError, match="franchise_id"):
        aggregate_matchup_season(conn, "demo_league")


def test_aggregate_matchup_season_excludes_playoffs_from_record_rollup():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_matchup_season,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "matchup_season"))
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            playoff_start_week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            is_placeholder INTEGER,
            margin DOUBLE,
            wins_to_date INTEGER,
            losses_to_date INTEGER,
            playoff_seed_to_date INTEGER
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES ('demo_league', 2024, 3)")
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("demo_league", "Alice", "fid_alice", 2024, 1, "Bob", 100.0, 90.0, 1, 0, 0, 0, 0, 0, 10.0, 1, 0, 1),
            ("demo_league", "Bob", "fid_bob", 2024, 1, "Alice", 90.0, 100.0, 0, 1, 0, 0, 0, 0, -10.0, 0, 1, 2),
            ("demo_league", "Alice", "fid_alice", 2024, 2, "Bob", 105.0, 110.0, 0, 1, 0, 0, 0, 0, -5.0, 1, 1, 2),
            ("demo_league", "Bob", "fid_bob", 2024, 2, "Alice", 110.0, 105.0, 1, 0, 0, 0, 0, 0, 5.0, 1, 1, 1),
            ("demo_league", "Alice", "fid_alice", 2024, 3, "Bob", 120.0, 115.0, 1, 0, 1, 0, 0, 0, 5.0, 2, 1, 1),
            ("demo_league", "Bob", "fid_bob", 2024, 3, "Alice", 115.0, 120.0, 0, 1, 1, 0, 0, 0, -5.0, 1, 2, 2),
        ],
    )

    aggregate_matchup_season(conn, "demo_league")

    rows = conn.execute(
        """
        SELECT manager, games, wins, losses, total_team_points, wins_to_date, losses_to_date
        FROM public.matchup_season
        WHERE db_name = 'demo_league'
        ORDER BY manager
        """
    ).fetchall()

    assert rows == [
        ("Alice", 2, 1, 1, 205.0, 1, 1),
        ("Bob", 2, 1, 1, 200.0, 1, 1),
    ]


def test_rebuild_matchup_season_fleet_matches_scoped_semantics_and_leaves_homepage_untouched():
    from multi_league.core.aggregate_ddl import create_named_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        rebuild_matchup_season_fleet,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA public")
    conn.execute(create_named_aggregate_table_sql("___leagues", "matchup_season", "matchup_season_rebuilt"))
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR, year INTEGER, playoff_start_week INTEGER, uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, manager VARCHAR, franchise_id VARCHAR,
            year INTEGER, week INTEGER, opponent VARCHAR,
            team_points DOUBLE, opponent_points DOUBLE,
            win INTEGER, loss INTEGER, tie INTEGER,
            is_playoffs INTEGER, is_consolation INTEGER,
            is_bye_week INTEGER, is_placeholder INTEGER,
            margin DOUBLE, close_margin INTEGER,
            above_league_median INTEGER, below_league_median INTEGER,
            power_rating DOUBLE, wins_to_date INTEGER, losses_to_date INTEGER,
            ties_to_date INTEGER, points_scored_to_date DOUBLE,
            playoff_seed_to_date INTEGER, final_playoff_seed INTEGER,
            champion INTEGER, sacko INTEGER,
            playoff_round VARCHAR, consolation_round VARCHAR
        )
        """
    )
    conn.execute("CREATE TABLE public.homepage_manager_rankings (db_name VARCHAR, payload VARCHAR)")
    conn.execute("INSERT INTO public.homepage_manager_rankings VALUES ('alpha', 'must-survive')")
    homepage_before = conn.execute("SELECT * FROM public.homepage_manager_rankings").fetchall()
    conn.executemany(
        "INSERT INTO public.league_settings VALUES (?, ?, ?, ?)",
        [("alpha", 2025, 3, False), ("beta", 2026, 3, True)],
    )
    rows = [
        ("alpha", "Alice", "a", 2025, 1, "Bob", 100.0, 90.0, 1, 0, 0, 0, 0, 0, 0, 10.0, 0, 1, 0, 101.0, 1, 0, 0, 100.0, 1, None, 0, 0, None, None),
        ("alpha", "Alice", "a", 2025, 2, "Bob", 95.0, 105.0, 0, 1, 0, 0, 0, 0, 0, -10.0, 0, 0, 1, 99.0, 1, 1, 0, 195.0, 2, None, 0, 0, None, None),
        ("alpha", "Alice", "a", 2025, 3, "Bob", 120.0, 110.0, 1, 0, 0, 1, 0, 0, 0, 10.0, 0, 0, 0, 110.0, 2, 1, 0, 315.0, 1, 1, 1, 0, "championship", None),
        ("beta", "Bea", "b", 2026, 1, "Cal", 80.0, 70.0, 1, 0, 0, 0, 0, 0, 0, 10.0, 0, 1, 0, 88.0, 1, 0, 0, 80.0, 1, None, 0, 0, None, None),
        ("beta", "Cal", "c", 2026, 1, "Bea", 70.0, 80.0, 0, 1, 0, 0, 0, 0, 0, -10.0, 0, 0, 1, 77.0, 0, 1, 0, 70.0, 2, None, 0, 0, None, None),
        # An unfinished scoreless row is not a played game.
        ("beta", "Bea", "b", 2026, 2, "Cal", 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0, 90.0, 1, 0, 0, 80.0, 1, None, 0, 0, None, None),
    ]
    conn.executemany(
        "INSERT INTO public.matchup VALUES (" + ",".join(["?"] * 30) + ")",
        rows,
    )

    result = rebuild_matchup_season_fleet(conn, target_table="matchup_season_rebuilt")

    assert result == {
        "rows": 3,
        "distinct_keys": 3,
        "leagues": 2,
        "league_years": 2,
    }
    assert conn.execute(
        """
        SELECT db_name, manager, franchise_id, year, games, wins, losses,
               total_team_points, made_playoffs, is_champion, power_rating
        FROM public.matchup_season_rebuilt
        ORDER BY db_name, franchise_id
        """
    ).fetchall() == [
        ("alpha", "Alice", "a", 2025, 2, 1, 1, 195.0, 1, 1, 99.0),
        ("beta", "Bea", "b", 2026, 1, 2, 0, 80.0, 0, 0, 88.0),
        ("beta", "Cal", "c", 2026, 1, 0, 2, 70.0, 0, 0, 77.0),
    ]
    assert conn.execute("SELECT * FROM public.homepage_manager_rankings").fetchall() == homepage_before


def test_aggregate_matchup_season_preserves_no_playoff_regular_season_flags():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_matchup_season,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(create_aggregate_table_sql("___leagues", "matchup_season"))
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            playoff_start_week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            is_placeholder INTEGER,
            margin DOUBLE,
            champion INTEGER,
            sacko INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            playoff_seed_to_date INTEGER,
            final_playoff_seed INTEGER
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES ('demo_league', 2024, 3)")
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "demo_league",
                "Alice",
                "fid_alice",
                2024,
                1,
                "Bob",
                100.0,
                90.0,
                1,
                0,
                0,
                0,
                0,
                0,
                10.0,
                0,
                0,
                None,
                None,
                1,
                1,
            ),
            (
                "demo_league",
                "Bob",
                "fid_bob",
                2024,
                1,
                "Alice",
                90.0,
                100.0,
                0,
                1,
                0,
                0,
                0,
                0,
                -10.0,
                0,
                0,
                None,
                None,
                2,
                2,
            ),
            (
                "demo_league",
                "Alice",
                "fid_alice",
                2024,
                2,
                "Bob",
                105.0,
                110.0,
                0,
                1,
                0,
                0,
                0,
                0,
                -5.0,
                1,
                0,
                None,
                None,
                1,
                1,
            ),
            (
                "demo_league",
                "Bob",
                "fid_bob",
                2024,
                2,
                "Alice",
                110.0,
                105.0,
                1,
                0,
                0,
                0,
                0,
                0,
                5.0,
                0,
                1,
                None,
                None,
                2,
                2,
            ),
        ],
    )

    aggregate_matchup_season(conn, "demo_league")

    rows = conn.execute(
        """
        SELECT manager, games, wins, losses, made_playoffs, is_champion, is_sacko, playoff_seed_to_date
        FROM public.matchup_season
        WHERE db_name = 'demo_league'
        ORDER BY manager
        """
    ).fetchall()

    assert rows == [
        ("Alice", 2, 1, 1, 0, 1, 0, 1),
        ("Bob", 2, 1, 1, 0, 0, 1, 2),
    ]


def test_aggregate_luck_all_play_builds_all_play_rollups():
    from multi_league.core.aggregate_ddl import create_aggregate_table_sql
    from multi_league.transformations.aggregation.aggregate_matchup_context import (
        aggregate_luck_all_play,
    )

    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    for table_name in ("all_play", "h2h_season", "schedule_swap", "schedule_swap_season"):
        conn.execute(create_aggregate_table_sql("___leagues", table_name))

    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            playoff_start_week INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            is_placeholder INTEGER
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES ('demo_league', 2024, 3)")
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("demo_league", "Alice", "fid_alice", "fid_bob", 2024, 1, 100.0, 90.0, 0, 0, 0, 0),
            ("demo_league", "Bob", "fid_bob", "fid_alice", 2024, 1, 90.0, 100.0, 0, 0, 0, 0),
            ("demo_league", "Alice", "fid_alice", "fid_bob", 2024, 2, 80.0, 85.0, 0, 0, 0, 0),
            ("demo_league", "Bob", "fid_bob", "fid_alice", 2024, 2, 85.0, 80.0, 0, 0, 0, 0),
            ("demo_league", "Alice", "fid_alice", "fid_bob", 2024, 3, 120.0, 110.0, 1, 0, 0, 0),
            ("demo_league", "Bob", "fid_bob", "fid_alice", 2024, 3, 110.0, 120.0, 1, 0, 0, 0),
        ],
    )

    assert aggregate_luck_all_play(conn, "demo_league") == (4, 2, 4, 2)

    h2h_row = conn.execute(
        """
        SELECT wins, losses, ties, games
        FROM public.h2h_season
        WHERE db_name = 'demo_league'
          AND franchise_id = 'fid_alice'
          AND opponent_franchise_id = 'fid_bob'
        """
    ).fetchone()
    assert h2h_row == (1, 1, 0, 2)

    impossible_swap_row = conn.execute(
        """
        SELECT wins, losses, ties, games
        FROM public.schedule_swap_season
        WHERE db_name = 'demo_league'
          AND franchise_id = 'fid_alice'
          AND schedule_of_franchise_id = 'fid_bob'
        """
    ).fetchone()
    assert impossible_swap_row is None

    own_schedule_row = conn.execute(
        """
        SELECT wins, losses, ties, games
        FROM public.schedule_swap_season
        WHERE db_name = 'demo_league'
          AND franchise_id = 'fid_alice'
          AND schedule_of_franchise_id = 'fid_alice'
        """
    ).fetchone()
    assert own_schedule_row == (1, 1, 0, 2)
