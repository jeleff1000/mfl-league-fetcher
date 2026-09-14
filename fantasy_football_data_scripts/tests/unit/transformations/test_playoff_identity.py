import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.transformations.aggregation.sql_aggregation_enrichments import AggregationEnrichmentsMixin
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.modules.playoff_helpers import (
    get_head_to_head_record,
    rank_and_seed,
)
from multi_league.transformations.matchup.playoff_odds_import import (
    apply_authoritative_playoff_seeds,
    future_regular_from_schedule,
    resolve_sim_key,
    write_odds_to_row,
)


class _AggregationRunner(AggregationEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_rank_and_seed_preserves_franchise_ids_and_latest_display_names():
    played_raw = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "manager": "Alice",
                "franchise_id": "fid_a",
                "opponent": "Bob",
                "opponent_franchise_id": "fid_b",
                "team_points": 100.0,
                "opponent_points": 90.0,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 1,
                "manager": "Bob",
                "franchise_id": "fid_b",
                "opponent": "Alice",
                "opponent_franchise_id": "fid_a",
                "team_points": 90.0,
                "opponent_points": 100.0,
                "win": 0,
                "loss": 1,
            },
            {
                "year": 2024,
                "week": 2,
                "manager": "Alice Renamed",
                "franchise_id": "fid_a",
                "opponent": "Bob",
                "opponent_franchise_id": "fid_b",
                "team_points": 110.0,
                "opponent_points": 105.0,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 2,
                "manager": "Bob",
                "franchise_id": "fid_b",
                "opponent": "Alice Renamed",
                "opponent_franchise_id": "fid_a",
                "team_points": 105.0,
                "opponent_points": 110.0,
                "win": 0,
                "loss": 1,
            },
        ]
    )

    wins = pd.Series({"fid_a": 2.0, "fid_b": 0.0})
    points = pd.Series({"fid_a": 210.0, "fid_b": 195.0})

    seeded = rank_and_seed(wins, points, playoff_slots=1, bye_slots=0, played_raw=played_raw)

    assert seeded["franchise_id"].tolist() == ["fid_a", "fid_b"]
    assert seeded["franchise_id"].notna().all()
    assert seeded.iloc[0]["manager"] == "Alice Renamed"
    assert seeded.iloc[0]["seed"] == 1
    assert bool(seeded.iloc[0]["made_playoffs"]) is True


def test_get_head_to_head_record_uses_franchise_identity_columns():
    played_raw = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 1,
                "manager": "Alex",
                "franchise_id": "fid_a",
                "opponent": "Alex",
                "opponent_franchise_id": "fid_b",
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 1,
                "manager": "Alex",
                "franchise_id": "fid_b",
                "opponent": "Alex",
                "opponent_franchise_id": "fid_a",
                "win": 0,
                "loss": 1,
            },
        ]
    )

    wins_a, wins_b, ties = get_head_to_head_record("fid_a", "fid_b", played_raw)

    assert (wins_a, wins_b, ties) == (1, 0, 0)


def test_resolve_sim_key_prefers_franchise_id_index_when_available():
    odds_index = pd.Index(["fid_a", "fid_b"])
    row = pd.Series({"manager": "Alice Renamed", "franchise_id": "fid_a"})

    assert resolve_sim_key(row, odds_index, {"fid_a": "Alice"}) == "fid_a"


def test_future_regular_from_schedule_preserves_duplicate_manager_identity():
    df_sched = pd.DataFrame(
        [
            {
                "year": 2024,
                "week": 13,
                "manager": "Ryan",
                "opponent": "Ryan",
                "franchise_id": "fid_a",
                "is_playoffs": 0,
            },
            {
                "year": 2024,
                "week": 13,
                "manager": "Ryan",
                "opponent": "Ryan",
                "franchise_id": "fid_b",
                "is_playoffs": 0,
            },
            {
                "year": 2024,
                "week": 14,
                "manager": "Alex",
                "opponent": "Ryan",
                "franchise_id": "fid_c",
                "is_playoffs": 0,
            },
            {
                "year": 2024,
                "week": 14,
                "manager": "Ryan",
                "opponent": "Alex",
                "franchise_id": "fid_b",
                "is_playoffs": 0,
            },
        ]
    )

    future = future_regular_from_schedule(df_sched, 2024, 12)

    week_13 = future[future["week"] == 13].sort_values("franchise_id").reset_index(drop=True)
    assert week_13["franchise_id"].tolist() == ["fid_a"]
    assert week_13["opponent_franchise_id"].tolist() == ["fid_b"]


def test_franchise_keyed_playoff_odds_write_back_cleanly():
    odds_df = pd.DataFrame(
        {
            "P_Semis": [100.0],
            "P_Final": [63.5],
            "P_Champ": [38.2],
        },
        index=["fid_a"],
    )
    seed_df = pd.DataFrame(index=["fid_a"])
    df = pd.DataFrame(
        [
            {
                "manager": "Christopher",
                "franchise_id": "fid_a",
                "p_semis": np.nan,
                "p_final": np.nan,
                "p_champ": np.nan,
            }
        ]
    )

    sim_key = resolve_sim_key(df.loc[0], odds_df.index, {"fid_a": "Christopher"})
    write_odds_to_row(df, 0, sim_key, odds_df, seed_df, None)

    assert df.at[0, "p_semis"] == 100.0
    assert df.at[0, "p_final"] == 63.5
    assert df.at[0, "p_champ"] == 38.2


def test_authoritative_playoff_seeds_override_standings_when_bracket_disagrees():
    standings_seeds = pd.DataFrame(
        [
            {
                "seed": 1,
                "manager": "Standings One",
                "franchise_id": "fid_1",
                "W": 10.0,
                "L": 4.0,
                "PF": 1500.0,
                "made_playoffs": True,
                "bye": True,
            },
            {
                "seed": 2,
                "manager": "Standings Two",
                "franchise_id": "fid_2",
                "W": 9.0,
                "L": 5.0,
                "PF": 1490.0,
                "made_playoffs": True,
                "bye": True,
            },
            {
                "seed": 3,
                "manager": "Standings Three",
                "franchise_id": "fid_3",
                "W": 8.0,
                "L": 6.0,
                "PF": 1480.0,
                "made_playoffs": True,
                "bye": False,
            },
            {
                "seed": 4,
                "manager": "Standings Four",
                "franchise_id": "fid_4",
                "W": 8.0,
                "L": 6.0,
                "PF": 1470.0,
                "made_playoffs": True,
                "bye": False,
            },
            {
                "seed": 5,
                "manager": "Standings Five",
                "franchise_id": "fid_5",
                "W": 7.0,
                "L": 7.0,
                "PF": 1460.0,
                "made_playoffs": True,
                "bye": False,
            },
            {
                "seed": 6,
                "manager": "Standings Six",
                "franchise_id": "fid_6",
                "W": 7.0,
                "L": 7.0,
                "PF": 1450.0,
                "made_playoffs": True,
                "bye": False,
            },
            {
                "seed": 7,
                "manager": "Outside In Standings",
                "franchise_id": "fid_7",
                "W": 7.0,
                "L": 7.0,
                "PF": 1440.0,
                "made_playoffs": False,
                "bye": False,
            },
        ]
    )
    df_to_date = pd.DataFrame(
        [
            {"manager": "Actual One", "franchise_id": "fid_7", "final_playoff_seed": 1},
            {"manager": "Actual Two", "franchise_id": "fid_2", "final_playoff_seed": 2},
            {"manager": "Actual Three", "franchise_id": "fid_3", "final_playoff_seed": 3},
            {"manager": "Actual Four", "franchise_id": "fid_4", "final_playoff_seed": 4},
            {"manager": "Actual Five", "franchise_id": "fid_5", "final_playoff_seed": 5},
            {"manager": "Actual Six", "franchise_id": "fid_6", "final_playoff_seed": 6},
        ]
    )

    seeded, source = apply_authoritative_playoff_seeds(
        standings_seeds,
        df_to_date,
        num_playoff_teams=6,
        bye_teams=2,
    )

    assert source == "final_playoff_seed"
    playoff_ids = seeded.loc[seeded["made_playoffs"], "franchise_id"].tolist()
    assert playoff_ids == ["fid_7", "fid_2", "fid_3", "fid_4", "fid_5", "fid_6"]
    assert seeded.loc[seeded["franchise_id"] == "fid_7", "bye"].iloc[0]
    assert not seeded.loc[seeded["franchise_id"] == "fid_1", "made_playoffs"].iloc[0]
    assert str(seeded["seed"].dtype) == "Int64"


def test_compute_manager_optimal_handles_apostrophes_in_manager_names(tmp_path):
    db_name = "manager_optimal_identity_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE,
            position_rank INTEGER,
            flex_week_rank INTEGER,
            sflex_week_rank INTEGER,
            optimal_player INTEGER,
            optimal_position VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                db_name,
                "dual_player",
                2021,
                2,
                "Ryan - Conrad's Heartthrobs",
                "fid_ryan",
                "QB,TE",
                "QB",
                20.0,
                1,
                1,
                1,
                0,
                None,
            ),
            (
                db_name,
                "qb_only",
                2021,
                2,
                "Ryan - Conrad's Heartthrobs",
                "fid_ryan",
                "QB",
                "QB",
                5.0,
                2,
                2,
                2,
                0,
                None,
            ),
            (
                db_name,
                "te_only",
                2021,
                2,
                "Ryan - Conrad's Heartthrobs",
                "fid_ryan",
                "TE",
                "TE",
                18.0,
                3,
                3,
                3,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        processed = runner.compute_manager_optimal({2021: {"QB": 1, "TE": 1}})
        selected = runner.conn.execute(
            """
            SELECT player_week, optimal_player, optimal_position
            FROM public.player_fantasy
            ORDER BY player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert processed == 1
    assert selected == [
        ("dual_player", 1, "QB"),
        ("qb_only", 0, None),
        ("te_only", 1, "TE"),
    ]


def test_compute_manager_optimal_keeps_duplicate_manager_names_separate(tmp_path):
    db_name = "manager_optimal_duplicate_name_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE,
            position_rank INTEGER,
            flex_week_rank INTEGER,
            sflex_week_rank INTEGER,
            optimal_player INTEGER,
            optimal_position VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, "fid_a_qb", 2021, 2, "Ryan", "fid_a", "QB", "QB", 20.0, 1, 1, 1, 0, None),
            (db_name, "fid_a_te", 2021, 2, "Ryan", "fid_a", "TE", "TE", 18.0, 1, 1, 1, 0, None),
            (db_name, "fid_b_qb", 2021, 2, "Ryan", "fid_b", "QB", "QB", 17.0, 2, 2, 2, 0, None),
            (db_name, "fid_b_te", 2021, 2, "Ryan", "fid_b", "TE", "TE", 15.0, 2, 2, 2, 0, None),
        ],
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        processed = runner.compute_manager_optimal({2021: {"QB": 1, "TE": 1}})
        selected = runner.conn.execute(
            """
            SELECT franchise_id, player_week, optimal_player, optimal_position
            FROM public.player_fantasy
            ORDER BY franchise_id, player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert processed == 1
    assert selected == [
        ("fid_a", "fid_a_qb", 1, "QB"),
        ("fid_a", "fid_a_te", 1, "TE"),
        ("fid_b", "fid_b_qb", 1, "QB"),
        ("fid_b", "fid_b_te", 1, "TE"),
    ]


def test_compute_manager_optimal_ignores_phantom_bye_rows(tmp_path):
    db_name = "manager_optimal_bye_filter_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE,
            position_rank INTEGER,
            flex_week_rank INTEGER,
            sflex_week_rank INTEGER,
            is_bye_week INTEGER,
            optimal_player INTEGER,
            optimal_position VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, "shared_star", 2024, 17, "Finalist", "fid_final", "QB", "QB", 24.0, 1, 1, 1, 0, 0, None),
            (db_name, "shared_te", 2024, 17, "Finalist", "fid_final", "TE", "TE", 17.0, 1, 1, 1, 0, 0, None),
            (db_name, "shared_star", 2024, 17, "Phantom Bye", "fid_bye", "QB", "QB", 24.0, 1, 1, 1, 1, 0, None),
            (db_name, "bye_te", 2024, 17, "Phantom Bye", "fid_bye", "TE", "TE", 19.0, 1, 1, 1, 1, 0, None),
        ],
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        processed = runner.compute_manager_optimal({2024: {"QB": 1, "TE": 1}})
        selected = runner.conn.execute(
            """
            SELECT franchise_id, player_week, is_bye_week, optimal_player, optimal_position
            FROM public.player_fantasy
            ORDER BY franchise_id, player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert processed == 1
    assert selected == [
        ("fid_bye", "bye_te", 1, 0, None),
        ("fid_bye", "shared_star", 1, 0, None),
        ("fid_final", "shared_star", 0, 1, "QB"),
        ("fid_final", "shared_te", 0, 1, "TE"),
    ]


def test_calculate_lamar_skips_unsupported_wr_flex_columns_cleanly(tmp_path):
    db_name = "lamar_wr_flex_skip_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            nfl_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            fantasy_points DOUBLE,
            is_started INTEGER,
            is_rostered INTEGER,
            player_lamar DOUBLE,
            manager_lamar DOUBLE,
            bench_lamar DOUBLE,
            replacement_ppg DOUBLE,
            player_lamar_ytd DOUBLE,
            manager_lamar_ytd DOUBLE,
            bench_lamar_ytd DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                db_name,
                "wr1_2024_1",
                "wr1",
                2024,
                1,
                "Alice",
                "fid_a",
                "WR",
                18.0,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                db_name,
                "rb1_2024_1",
                "rb1",
                2024,
                1,
                "Bob",
                "fid_b",
                "RB",
                12.0,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        updated = runner.calculate_lamar_for_all_players({2024: {"QB": 1, "RB": 2, "WR": 2, "W/R": 1}})
        cols = {row[0] for row in runner.conn.execute("DESCRIBE public.player_fantasy").fetchall()}
        lamar_rows = runner.conn.execute(
            """
            SELECT player_week, player_lamar, manager_lamar, replacement_ppg
            FROM public.player_fantasy
            ORDER BY player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert updated == 2
    assert "replacement_ppg_w_r" not in cols
    assert all(row[1] is not None for row in lamar_rows)
