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
            draft_type VARCHAR,
            cost_bucket INTEGER,
            nfl_team VARCHAR,
            nfl_team_api VARCHAR,
            position_draft_label VARCHAR,
            draft_age INTEGER,
            draft_age_grade VARCHAR,
            drafted_as_starter INTEGER,
            manager_lamar DOUBLE,
            expected_lamar DOUBLE,
            pick_score DOUBLE,
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


def test_league_inefficiency_miner_finds_repeatable_rookie_discount():
    from multi_league.transformations.draft.league_inefficiency_miner import run_league_inefficiency_miner

    conn = _setup_conn()
    rows = [
        (
            "demo_league",
            2024,
            "m1",
            "M1",
            "r1",
            "RB",
            0,
            1,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            20,
            10,
            90,
            0,
            None,
        ),
        (
            "demo_league",
            2024,
            "m2",
            "M2",
            "r2",
            "RB",
            0,
            2,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            20,
            10,
            90,
            0,
            None,
        ),
        (
            "demo_league",
            2024,
            "m3",
            "M3",
            "v1",
            "RB",
            0,
            3,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            27,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
        (
            "demo_league",
            2024,
            "m4",
            "M4",
            "v2",
            "RB",
            0,
            4,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            28,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m1",
            "M1",
            "r3",
            "RB",
            0,
            1,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            22,
            10,
            92,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m2",
            "M2",
            "r4",
            "RB",
            0,
            2,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            22,
            10,
            92,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m3",
            "M3",
            "v3",
            "RB",
            0,
            3,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            27,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m4",
            "M4",
            "v4",
            "RB",
            0,
            4,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            28,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
    ]
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    bio_rows = [
        ("r1", None, None, None, None, 2024, None, 2, None, None, None),
        ("r2", None, None, None, None, 2024, None, 2, None, None, None),
        ("r3", None, None, None, None, 2025, None, 2, None, None, None),
        ("r4", None, None, None, None, 2025, None, 2, None, None, None),
        ("v1", None, None, None, None, 2018, None, 2, None, None, None),
        ("v2", None, None, None, None, 2018, None, 2, None, None, None),
        ("v3", None, None, None, None, 2018, None, 2, None, None, None),
        ("v4", None, None, None, None, 2018, None, 2, None, None, None),
    ]
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        bio_rows,
    )

    result = run_league_inefficiency_miner(
        conn,
        "demo_league",
        min_picks=2,
        min_years=2,
        min_abs_z=0,
        limit=20,
    )

    rookie = next(
        row
        for row in result
        if row["feature_type"] == "experience_bucket" and row["feature_value"] == "experience_rookie"
    )
    assert rookie["years_seen"] == 2
    assert rookie["repeatability"] == 1
    assert rookie["excess_residual"] > 0
    assert rookie["pick_score_delta"] > 0


def test_replace_league_inefficiency_candidates_writes_scoped_rows():
    from multi_league.transformations.draft.league_inefficiency_miner import (
        replace_league_inefficiency_candidates,
    )

    conn = _setup_conn()
    rows = [
        (
            "demo_league",
            2024,
            "m1",
            "M1",
            "r1",
            "RB",
            0,
            1,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            20,
            10,
            90,
            0,
            None,
        ),
        (
            "demo_league",
            2024,
            "m2",
            "M2",
            "v1",
            "RB",
            0,
            2,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            27,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m1",
            "M1",
            "r2",
            "RB",
            0,
            1,
            1,
            "snake",
            None,
            "ATL",
            None,
            "RB1",
            22,
            None,
            1,
            20,
            10,
            90,
            0,
            None,
        ),
        (
            "demo_league",
            2025,
            "m2",
            "M2",
            "v2",
            "RB",
            0,
            2,
            1,
            "snake",
            None,
            "KC",
            None,
            "RB1",
            27,
            None,
            1,
            10,
            10,
            50,
            0,
            None,
        ),
    ]
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
    )
    bio_rows = [
        ("r1", None, None, None, None, 2024, None, 2, None, None, None),
        ("r2", None, None, None, None, 2025, None, 2, None, None, None),
        ("v1", None, None, None, None, 2018, None, 2, None, None, None),
        ("v2", None, None, None, None, 2018, None, 2, None, None, None),
    ]
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.player_bio VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        bio_rows,
    )

    count = replace_league_inefficiency_candidates(
        conn,
        "demo_league",
        min_picks=1,
        min_years=2,
        min_abs_z=0,
        limit=10,
    )

    assert count > 0
    table_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM public.draft_inefficiency_candidate
        WHERE db_name = 'demo_league'
        """
    ).fetchone()[0]
    assert table_count == count
