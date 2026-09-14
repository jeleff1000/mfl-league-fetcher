import duckdb
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _setup_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            franchise_id VARCHAR,
            manager VARCHAR,
            NFL_player_id VARCHAR,
            position VARCHAR,
            cost DOUBLE,
            pick INTEGER,
            round INTEGER,
            cost_bucket INTEGER,
            nfl_team VARCHAR,
            nfl_team_api VARCHAR,
            draft_age INTEGER,
            is_keeper INTEGER,
            draft_category VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            latest_team VARCHAR,
            nfl_draft_team VARCHAR,
            college VARCHAR,
            conference VARCHAR,
            rookie_year DOUBLE,
            is_undrafted BIGINT,
            draft_round DOUBLE,
            ras_score DOUBLE,
            height DOUBLE,
            weight DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            NFL_player_id VARCHAR,
            year DOUBLE,
            week DOUBLE,
            season_type VARCHAR,
            rushing_yards DOUBLE,
            carries DOUBLE,
            targets DOUBLE,
            receptions DOUBLE,
            receiving_yards DOUBLE,
            target_share DOUBLE,
            wopr DOUBLE
        )
        """
    )
    return conn


def test_manager_tendency_miner_controls_by_capital_and_position():
    from multi_league.transformations.draft.tendency_miner import run_manager_tendency_miner

    conn = _setup_conn()
    rows = [
        ("demo_league", 2024, "alice", "Alice", "p1", "WR", 0, 1, 1, None, "DEN", None, 23, 0, None),
        ("demo_league", 2024, "alice", "Alice", "p2", "RB", 0, 2, 1, None, "DEN", None, 24, 0, None),
        ("demo_league", 2024, "bob", "Bob", "p3", "WR", 0, 3, 1, None, "KC", None, 25, 0, None),
        ("demo_league", 2024, "bob", "Bob", "p4", "RB", 0, 4, 1, None, "KC", None, 26, 0, None),
        ("demo_league", 2025, "alice", "Alice", "p5", "WR", 0, 1, 1, None, "DEN", None, 23, 0, None),
        ("demo_league", 2025, "alice", "Alice", "p6", "RB", 0, 2, 1, None, "DEN", None, 24, 0, None),
        ("demo_league", 2025, "bob", "Bob", "p7", "WR", 0, 3, 1, None, "KC", None, 25, 0, None),
        ("demo_league", 2025, "bob", "Bob", "p8", "RB", 0, 4, 1, None, "KC", None, 26, 0, None),
    ]
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(f"p{i}", None, None, None, None, None, None, None, None, None, None) for i in range(1, 9)],
    )

    result = run_manager_tendency_miner(
        conn,
        "demo_league",
        min_picks=2,
        min_expected_capital=0.1,
        min_abs_z=0,
        limit=20,
    )

    alice_den = next(
        row
        for row in result
        if row["scope_label"] == "Alice" and row["feature_type"] == "current_nfl_team" and row["feature_value"] == "DEN"
    )
    assert alice_den["years_seen"] == 2
    assert alice_den["positive_years"] == 2
    assert alice_den["repeatability"] == 1
    assert alice_den["picks"] == 4
    assert alice_den["observed_capital"] > alice_den["expected_capital"]
    assert alice_den["lift"] > 1


def test_replace_manager_tendency_candidates_writes_scoped_rows():
    from multi_league.transformations.draft.tendency_miner import (
        replace_manager_tendency_candidates,
    )

    conn = _setup_conn()
    rows = [
        ("demo_league", 2024, "alice", "Alice", "p1", "RB", 20, 1, 1, 5, "DEN", None, 23, 0, None),
        ("demo_league", 2024, "alice", "Alice", "p2", "RB", 15, 2, 1, 4, "DEN", None, 24, 0, None),
        ("demo_league", 2024, "bob", "Bob", "p3", "RB", 20, 3, 1, 5, "KC", None, 25, 0, None),
        ("demo_league", 2024, "bob", "Bob", "p4", "RB", 15, 4, 1, 4, "KC", None, 26, 0, None),
    ]
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(f"p{i}", None, None, None, None, None, None, None, None, None, None) for i in range(1, 5)],
    )

    count = replace_manager_tendency_candidates(
        conn,
        "demo_league",
        min_picks=1,
        min_expected_capital=0.1,
        min_abs_z=0,
        limit=10,
    )

    assert count > 0
    table_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM public.draft_tendency_candidate
        WHERE db_name = 'demo_league' AND scope_type = 'manager'
        """
    ).fetchone()[0]
    assert table_count == count


def test_manager_tendency_miner_discovers_generated_archetypes():
    from multi_league.transformations.draft.tendency_miner import run_manager_tendency_miner

    conn = _setup_conn()
    rows = [
        ("demo_league", 2025, "alice", "Alice", "q1", "QB", 0, 1, 1, None, "BAL", None, 26, 0, None),
        ("demo_league", 2025, "alice", "Alice", "q2", "QB", 0, 2, 1, None, "BUF", None, 25, 0, None),
        ("demo_league", 2025, "bob", "Bob", "q3", "QB", 0, 3, 1, None, "KC", None, 29, 0, None),
        ("demo_league", 2025, "bob", "Bob", "q4", "QB", 0, 4, 1, None, "LAR", None, 31, 0, None),
    ]
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(f"q{i}", None, None, None, None, 2020, None, None, None, None, None) for i in range(1, 5)],
    )
    conn.executemany(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("q1", 2024, 1, "REG", 360, 65, 0, 0, 0, 0, 0),
            ("q2", 2024, 1, "REG", 420, 70, 0, 0, 0, 0, 0),
            ("q3", 2024, 1, "REG", 80, 20, 0, 0, 0, 0, 0),
            ("q4", 2024, 1, "REG", 40, 10, 0, 0, 0, 0, 0),
        ],
    )

    result = run_manager_tendency_miner(
        conn,
        "demo_league",
        min_picks=2,
        min_expected_capital=0.1,
        min_abs_z=0,
        limit=40,
    )

    mobile_qb = next(
        row
        for row in result
        if row["scope_label"] == "Alice" and row["feature_type"] == "archetype" and row["feature_value"] == "QB_mobile"
    )
    assert mobile_qb["picks"] == 2
    assert mobile_qb["lift"] > 1
