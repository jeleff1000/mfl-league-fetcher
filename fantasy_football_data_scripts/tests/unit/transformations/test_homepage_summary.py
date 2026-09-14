import duckdb
import pytest

from multi_league.transformations.aggregation.homepage_summary import (
    _compute_best_trade,
    _compute_manager_career_stats,
    _compute_manager_draft_profile,
    _compute_manager_txn_profile,
    _compute_transaction_highlights,
    _normalize_platform_id_sql,
    compute_all_manager_profiles,
    compute_current_standings,
    compute_manager_rankings,
    compute_top_rivalries,
)
from multi_league.transformations.aggregation.aggregation_utils import LocalProfileContext


def test_normalize_platform_id_sql_casts_to_string():
    sql = _normalize_platform_id_sql("bio.sleeper_player_id")

    assert "CAST(bio.sleeper_player_id AS VARCHAR)" in sql
    assert "REGEXP_REPLACE" in sql


def test_compute_manager_rankings_handles_text_year_week_columns():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = conn.execute("SELECT current_database()").fetchone()[0]
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR,
            team_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, 2025, 1, "Alice", "fid_a", "Alpha", 110.0, 1, 0, 0, 0, 0, 0),
            (db_name, 2025, 1, "Bob", "fid_b", "Beta", 95.0, 0, 1, 0, 0, 0, 0),
            (db_name, 2025, 2, "Alice", "fid_a", "Alpha", 108.0, 0, 1, 0, 0, 0, 0),
            (db_name, 2025, 2, "Bob", "fid_b", "Beta", 112.0, 1, 0, 0, 0, 0, 0),
        ],
    )

    rankings = compute_manager_rankings(conn, db_name)
    standings = compute_current_standings(conn, db_name)

    assert set(rankings["manager"]) == {"Alice", "Bob"}
    assert set(standings["manager"]) == {"Alice", "Bob"}


def test_compute_manager_rankings_uses_season_power_when_weekly_power_is_null():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = conn.execute("SELECT current_database()").fetchone()[0]
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR,
            team_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            power_rating DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, 2024, 1, "Alice", "fid_a", "Alpha", 110.0, 1, 0, 0, 0, 0, 0, None),
            (db_name, 2025, 1, "Alice", "fid_a", "Alpha", 105.0, 0, 1, 0, 0, 0, 0, None),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.matchup_season (
            db_name VARCHAR,
            year INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            power_rating DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup_season VALUES (?, ?, ?, ?, ?)",
        [
            (db_name, 2024, "Alice", "fid_a", 92.0),
            (db_name, 2025, "Alice", "fid_a", 108.0),
        ],
    )

    rankings = compute_manager_rankings(conn, db_name).set_index("franchise_id")

    assert rankings.loc["fid_a", "power_rating"] == 100.0


def test_compute_current_standings_uses_holdover_odds_without_counting_holdover_games():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = "holdover_homepage_test"
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            opponent VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            team_name VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            p_playoffs DOUBLE,
            p_champ DOUBLE,
            power_rating DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                db_name,
                2025,
                14,
                "Alice",
                "Bob",
                "fid_a",
                "fid_b",
                "Alpha",
                110.0,
                90.0,
                1,
                0,
                0,
                0,
                0,
                0,
                100.0,
                0.0,
                78.5,
            ),
            (
                db_name,
                2025,
                14,
                "Bob",
                "Alice",
                "fid_b",
                "fid_a",
                "Beta",
                90.0,
                110.0,
                0,
                1,
                0,
                0,
                0,
                0,
                100.0,
                20.0,
                81.0,
            ),
            (
                db_name,
                2025,
                15,
                "Alice",
                None,
                "fid_a",
                None,
                "Alpha",
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                0.0,
                0.0,
                78.5,
            ),
        ],
    )

    standings = compute_current_standings(conn, db_name).set_index("franchise_id")

    assert standings.loc["fid_a", "p_playoffs"] == 0.0
    assert standings.loc["fid_a", "p_champ"] == 0.0
    assert standings.loc["fid_a", "power_rating"] == 78.5
    assert standings.loc["fid_a", "wins"] == 1
    assert standings.loc["fid_a", "points_for"] == 110.0


def test_compute_top_rivalries_uses_latest_franchise_labels():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = "rivalry_label_test"
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            opponent VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            team_name VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                db_name,
                2003,
                1,
                "Zain Dhanani",
                "Zain Dhanani",
                "fid_a",
                "fid_b",
                "Old A",
                100.0,
                90.0,
                1,
                0,
                0,
                0,
                0,
                0,
            ),
            (
                db_name,
                2003,
                1,
                "Zain Dhanani",
                "Zain Dhanani",
                "fid_b",
                "fid_a",
                "Old B",
                90.0,
                100.0,
                0,
                1,
                0,
                0,
                0,
                0,
            ),
            (db_name, 2004, 1, "Zain Dhanani", "Zain Dhanani", "fid_a", "fid_b", "Old A", 88.0, 91.0, 0, 1, 0, 0, 0, 0),
            (db_name, 2004, 1, "Zain Dhanani", "Zain Dhanani", "fid_b", "fid_a", "Old B", 91.0, 88.0, 1, 0, 0, 0, 0, 0),
            (
                db_name,
                2025,
                1,
                "Jon H",
                "Zain Dhanani",
                "fid_a",
                "fid_b",
                "Current Jon",
                120.0,
                101.0,
                1,
                0,
                0,
                0,
                0,
                0,
            ),
            (
                db_name,
                2025,
                1,
                "Zain Dhanani",
                "Jon H",
                "fid_b",
                "fid_a",
                "Current Zain",
                101.0,
                120.0,
                0,
                1,
                0,
                0,
                0,
                0,
            ),
        ],
    )

    rivalries = compute_top_rivalries(conn, db_name, limit=5)

    assert len(rivalries) == 1
    row = rivalries.iloc[0]
    assert row["manager1"] == "Jon H"
    assert row["manager2"] == "Zain Dhanani"
    assert row["franchise_id_1"] == "fid_a"
    assert row["franchise_id_2"] == "fid_b"
    assert row["total_games"] == 3


def test_compute_all_manager_profiles_separates_duplicate_manager_names_by_franchise_id():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = "league_identity_test"
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, 2025, 1, "Chris", "fid_alpha", "Alpha", 110.0, 90.0, 1, 0, 0, 0, 0, 0),
            (db_name, 2025, 1, "Chris", "fid_beta", "Beta", 90.0, 110.0, 0, 1, 0, 0, 0, 0),
            (db_name, 2025, 2, "Chris", "fid_alpha", "Alpha", 105.0, 95.0, 1, 0, 0, 0, 0, 0),
            (db_name, 2025, 2, "Chris", "fid_beta", "Beta", 95.0, 105.0, 0, 1, 0, 0, 0, 0),
        ],
    )
    # Minimal draft / transactions tables so per-manager profile helpers
    # reach the matchup-level assertions this test targets.  franchise_id
    # is required (no-fallback invariant); rows are intentionally empty.
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            year INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            transaction_type VARCHAR
        )
        """
    )

    profiles = compute_all_manager_profiles(conn, db_name).sort_values("franchise_id").reset_index(drop=True)

    assert profiles["franchise_id"].tolist() == ["fid_alpha", "fid_beta"]
    assert profiles["manager"].tolist() == ["Chris", "Chris"]
    assert profiles["reg_wins"].tolist() == [2, 0]
    assert profiles["reg_losses"].tolist() == [0, 2]
    assert profiles["current_team_name"].tolist() == ["Alpha", "Beta"]


def test_compute_transaction_highlights_lookup_headshots_after_selecting_rows():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS \"___ops\"")
    conn.execute('CREATE SCHEMA "___ops".nfl_historical')
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            transaction_type VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            NFL_player_id VARCHAR,
            yahoo_player_id VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            player_lamar_ros_total DOUBLE,
            total_points_ros_total DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE "___ops".nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            yahoo_player_id VARCHAR,
            player VARCHAR,
            headshot_url VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("txn_highlight_test", "add", "Pickup Star", "Alice", 2024, 3, "nfl_add", "101", 12.5, 11.0, 55.0),
            ("txn_highlight_test", "drop", "Drop Regret", "Bob", 2024, 7, "nfl_drop", "202", 0.0, 18.0, 60.0),
        ],
    )
    conn.executemany(
        'INSERT INTO "___ops".nfl_historical.player_bio VALUES (?, ?, ?, ?)',
        [
            ("nfl_add", "101", "Pickup Star", "https://img.example/add.png"),
            ("nfl_drop", "202", "Drop Regret", "https://img.example/drop.png"),
        ],
    )

    highlights = _compute_transaction_highlights(conn, "txn_highlight_test", platform="yahoo")

    assert highlights["best_pickup_player"] == "Pickup Star"
    assert highlights["best_pickup_headshot"] == "https://img.example/add.png"
    assert highlights["worst_drop_player"] == "Drop Regret"
    assert highlights["worst_drop_headshot"] == "https://img.example/drop.png"


def test_local_profile_context_player_lookup_without_remote_register():
    class ExecuteOnlyConnection:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, sql, params=None):
            if params:
                return self.conn.execute(sql, params)
            return self.conn.execute(sql)

    remote = duckdb.connect(":memory:")
    remote.execute("CREATE SCHEMA IF NOT EXISTS public")
    remote.execute("ATTACH ':memory:' AS \"___ops\"")
    remote.execute('CREATE SCHEMA "___ops".nfl_historical')
    db_name = "profile_lookup_no_register_test"
    remote.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    remote.execute("CREATE TABLE public.draft (db_name VARCHAR)")
    remote.execute("CREATE TABLE public.transactions (db_name VARCHAR)")
    remote.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            is_started INTEGER,
            manager_lamar DOUBLE
        )
        """
    )
    remote.execute(
        """
        CREATE TABLE "___ops".nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            player VARCHAR,
            headshot_url VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    remote.execute(
        """
        CREATE TABLE "___ops".nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            headshot_url VARCHAR
        )
        """
    )
    remote.execute("INSERT INTO public.matchup VALUES (?, ?, ?)", [db_name, 2024, 1])
    remote.execute("INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?)", [db_name, "nfl_1_2024_1", 1, 5.0])
    remote.execute(
        'INSERT INTO "___ops".nfl_historical.nfl_player_stats_all VALUES (?, ?, ?, ?)',
        ["nfl_1_2024_1", "Lookup Player", "https://img.example/lookup.png", "nfl_1"],
    )
    remote.execute(
        'INSERT INTO "___ops".nfl_historical.player_bio VALUES (?, ?)',
        ["nfl_1", "https://img.example/bio.png"],
    )

    ctx = LocalProfileContext(ExecuteOnlyConnection(remote), db_name)

    rows = ctx.local.execute("SELECT player, headshot_url, NFL_player_id FROM player_lookup").fetchall()
    assert rows == [("Lookup Player", "https://img.example/lookup.png", "nfl_1")]


def _memory_conn_with_schema():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    return conn


def test_compute_best_trade_raises_when_franchise_id_missing():
    """Bug #1.8: silent return-empty-dict on missing franchise_id is replaced
    with a loud KeyError so the no-fallback invariant fails fast."""
    conn = _memory_conn_with_schema()
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
        ["demo_league", "Alice", 2024, "trade"],
    )

    with pytest.raises(KeyError, match="franchise_id"):
        _compute_best_trade(conn, "demo_league")


def test_compute_manager_career_stats_raises_when_franchise_id_missing():
    """Bug #1.8: when matchup_cols is provided explicitly without franchise_id
    (or auto-detected and missing), raise KeyError instead of silently
    returning {}."""
    conn = _memory_conn_with_schema()
    # No matchup table needed — caller passes matchup_cols explicitly to
    # bypass auto-detection. The check is on the passed cols set.
    with pytest.raises(KeyError, match="franchise_id"):
        _compute_manager_career_stats(
            conn,
            "demo_league",
            "fid_a",
            "Alice",
            matchup_cols={"manager", "year", "week", "team_points"},
        )


def test_compute_manager_draft_profile_raises_when_franchise_id_missing():
    """Bug #1.8: silent return-empty-profile on missing franchise_id is replaced
    with a loud KeyError."""
    conn = _memory_conn_with_schema()
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            manager VARCHAR,
            year INTEGER,
            player VARCHAR
        )
        """
    )

    with pytest.raises(KeyError, match="franchise_id"):
        _compute_manager_draft_profile(conn, "demo_league", "fid_a", "Alice")


def test_compute_manager_txn_profile_raises_when_franchise_id_missing():
    """Bug #1.8: silent return-empty-profile on missing franchise_id is replaced
    with a loud KeyError."""
    conn = _memory_conn_with_schema()
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

    with pytest.raises(KeyError, match="franchise_id"):
        _compute_manager_txn_profile(conn, "demo_league", "fid_a", "Alice")
