from pathlib import Path

import duckdb

from audit_research_lineages import audit_snapshot, rank_lineages


def test_audit_snapshot_reports_required_grains_and_signal_invariants(tmp_path: Path):
    root = tmp_path / "research_public_lake"
    root.mkdir()
    db = root / "corpus_snapshot.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA public")
    con.execute("""
        CREATE TABLE public.league_settings(
          db_name VARCHAR, year INTEGER, platform VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE public.player_fantasy(
          db_name VARCHAR, year INTEGER, NFL_player_id VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE public.matchup(
          db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR,
          opponent VARCHAR, opponent_points DOUBLE, win INTEGER, loss INTEGER,
          tie INTEGER, is_playoffs INTEGER, is_championship INTEGER,
          franchise_id VARCHAR
        )
    """)
    con.executemany("INSERT INTO public.league_settings VALUES (?, ?, ?)", [("smpl_a", 2020, "sleeper")])
    con.executemany("INSERT INTO public.player_fantasy VALUES (?, ?, ?)", [("smpl_a", 2020, "p1")])
    con.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [("smpl_a", 2020, 15, "m1", "m2", 100, 1, 0, 0, 1, 0, "f1"),
         ("smpl_a", 2020, 16, "m1", "m2", 90, 1, 0, 0, 1, 1, "f1")],
    )
    con.close()

    result = audit_snapshot(root)

    assert result["league_settings_rows"] == 1
    assert result["player_league_years"] == 1
    assert result["matchup_league_years"] == 1
    assert result["championship_outside_playoffs"] == 0
    assert result["premature_championship_teams"] == 0


def test_rank_lineages_rejects_regression_and_selects_unique_best():
    records = [
        {
            "lineage": "old",
            "missing_required_tables": [],
            "missing_outcome_columns": [],
            "championship_outside_playoffs": 2,
            "premature_championship_teams": 0,
            "matchup_league_years": 100,
            "player_league_years": 100,
            "null_opponent": 0,
            "null_win": 0,
            "null_loss": 0,
            "null_tie": 0,
        },
        {
            "lineage": "winner",
            "missing_required_tables": [],
            "missing_outcome_columns": [],
            "championship_outside_playoffs": 0,
            "premature_championship_teams": 0,
            "matchup_league_years": 100,
            "player_league_years": 100,
            "null_opponent": 0,
            "null_win": 0,
            "null_loss": 0,
            "null_tie": 0,
        },
    ]
    comparison = rank_lineages(records)
    assert comparison["winner"] == "winner"
