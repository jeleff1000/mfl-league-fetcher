import duckdb
import pandas as pd
import pytest


def _db_name(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def _public_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    return conn


def test_ensure_aggregate_table_creates_missing_table():
    from multi_league.core.aggregate_ddl import ensure_aggregate_table

    conn = _public_conn()
    db_name = _db_name(conn)

    ensure_aggregate_table(conn, db_name, "transaction_manager_season")

    cols = [row[0] for row in conn.execute("DESCRIBE public.transaction_manager_season").fetchall()]
    assert "manager" in cols
    assert "trade_net_lamar" in cols


def test_ensure_aggregate_table_fails_on_stale_schema():
    from multi_league.core.aggregate_ddl import ensure_aggregate_table

    conn = _public_conn()
    db_name = _db_name(conn)
    conn.execute("CREATE TABLE public.player_fantasy_season (NFL_player_id VARCHAR, year INTEGER)")

    with pytest.raises(ValueError, match="schema drift detected"):
        ensure_aggregate_table(conn, db_name, "player_fantasy_season")


def test_replace_snapshot_table_from_dataframe_creates_explicit_table():
    from multi_league.core.aggregate_ddl import replace_snapshot_table_from_dataframe

    conn = _public_conn()
    db_name = _db_name(conn)
    df = pd.DataFrame(
        {
            "manager": ["alice"],
            "wins": [10],
            "power_rating": [142.5],
        }
    )

    replace_snapshot_table_from_dataframe(conn, db_name, "ad_hoc_snapshot", df)

    desc = conn.execute("DESCRIBE public.ad_hoc_snapshot").fetchall()
    columns = [row[0] for row in desc]
    assert columns == ["manager", "wins", "power_rating"]
    assert conn.execute("SELECT COUNT(*) FROM public.ad_hoc_snapshot").fetchone()[0] == 1


def test_replace_snapshot_table_from_dataframe_fails_on_column_drift():
    from multi_league.core.aggregate_ddl import replace_snapshot_table_from_dataframe

    conn = _public_conn()
    db_name = _db_name(conn)
    initial = pd.DataFrame({"manager": ["alice"], "wins": [10]})
    drifted = pd.DataFrame({"manager": ["alice"], "wins": [10], "losses": [2]})

    replace_snapshot_table_from_dataframe(conn, db_name, "ad_hoc_snapshot", initial)

    with pytest.raises(ValueError, match="schema drift detected"):
        replace_snapshot_table_from_dataframe(conn, db_name, "ad_hoc_snapshot", drifted)


def test_replace_aggregate_table_from_dataframe_uses_registered_homepage_schema():
    from multi_league.core.aggregate_ddl import replace_aggregate_table_from_dataframe

    conn = _public_conn()
    db_name = _db_name(conn)
    df = pd.DataFrame(
        {
            "db_name": [db_name],
            "manager": ["alice"],
            "franchise_id": ["fid_alice"],
            "wins": [10],
            "power_rating": [142.5],
        }
    )

    replace_aggregate_table_from_dataframe(conn, db_name, "homepage_manager_rankings", df)

    desc = conn.execute("DESCRIBE public.homepage_manager_rankings").fetchall()
    columns = [row[0] for row in desc]
    assert columns == [
        "db_name",
        "manager",
        "franchise_id",
        "wins",
        "losses",
        "ties",
        "win_pct",
        "championships",
        "playoff_appearances",
        "seasons",
        "power_rating",
        "first_year",
        "last_year",
        "career_rank",
    ]
    row = conn.execute(
        "SELECT manager, franchise_id, wins, losses, ties, power_rating FROM public.homepage_manager_rankings"
    ).fetchone()
    assert row == ("alice", "fid_alice", 10, None, None, 142.5)


def test_replace_aggregate_table_from_dataframe_fails_on_noncanonical_columns():
    from multi_league.core.aggregate_ddl import replace_aggregate_table_from_dataframe

    conn = _public_conn()
    db_name = _db_name(conn)
    df = pd.DataFrame({"manager": ["alice"], "wins": [10], "rogue_metric": [1]})

    with pytest.raises(ValueError, match="non-canonical columns"):
        replace_aggregate_table_from_dataframe(conn, db_name, "homepage_manager_rankings", df)


def test_replace_aggregate_table_from_dataframe_reuses_existing_canonical_table(monkeypatch):
    import multi_league.core.aggregate_ddl as mod

    conn = _public_conn()
    db_name = _db_name(conn)
    initial = pd.DataFrame(
        {
            "db_name": [db_name],
            "manager": ["alice"],
            "franchise_id": ["fid_alice"],
            "wins": [10],
            "power_rating": [142.5],
        }
    )
    updated = pd.DataFrame(
        {"db_name": [db_name], "manager": ["bob"], "franchise_id": ["fid_bob"], "wins": [12], "power_rating": [155.0]}
    )

    mod.replace_aggregate_table_from_dataframe(conn, db_name, "homepage_manager_rankings", initial)

    def _fail_recreate(*args, **kwargs):
        raise AssertionError("replace should not recreate a canonical aggregate table")

    monkeypatch.setattr(mod, "recreate_aggregate_table", _fail_recreate)

    mod.replace_aggregate_table_from_dataframe(conn, db_name, "homepage_manager_rankings", updated)

    rows = conn.execute(
        "SELECT manager, wins, power_rating FROM public.homepage_manager_rankings ORDER BY manager"
    ).fetchall()
    assert rows == [("bob", 12, 155.0)]


def test_matchup_season_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import MATCHUP_SEASON_COLUMN_TYPES

    cols = MATCHUP_SEASON_COLUMN_TYPES

    # Renamed columns exist
    assert "total_team_points" in cols
    assert "total_opponent_points" in cols
    assert "avg_team_points" in cols
    assert "avg_opponent_points" in cols
    assert "avg_margin" in cols
    assert "max_team_points" in cols
    assert "min_team_points" in cols
    assert "std_dev_team_points" in cols
    assert "above_league_median" in cols
    assert "max_win_streak" in cols
    assert "max_loss_streak" in cols
    assert "optimal_games" in cols
    assert "optimal_ceiling_pts" in cols
    assert "optimal_wins" in cols
    assert "optimal_missed_wins" in cols
    assert "optimal_lucky_wins" in cols
    assert "optimal_wins_actual" in cols
    assert "optimal_losses_actual" in cols
    assert "optimal_outcome_changes" in cols
    assert "optimal_margin" in cols
    assert "proj_total_team_points" in cols
    assert "proj_total_opponent_points" in cols
    assert "proj_favored_pct" in cols
    assert "proj_avg_win_pct" in cols
    assert "proj_expected_wins" in cols
    assert "is_champion" in cols

    # Old abbreviated names removed
    assert "total_pf" not in cols
    assert "total_pa" not in cols
    assert "ppg" not in cols
    assert "pag" not in cols
    assert "diff" not in cols
    assert "best" not in cols
    assert "worst" not in cols
    assert "std_dev" not in cols
    assert "above_median" not in cols
    assert "max_w_streak" not in cols
    assert "max_l_streak" not in cols
    assert "opt_games" not in cols
    assert "opt_optimal_pts" not in cols
    assert "proj_total_pf" not in cols
    assert "proj_proj_wins" not in cols
    assert "is_champ" not in cols
    assert "champion" not in cols
    assert "franchise_name" not in cols

    # New columns added
    assert "win_pct" in cols
    assert "close_win_pct" in cols
    assert "optimal_bench_pts" in cols
    assert "optimal_efficiency" in cols
    assert "optimal_losses" in cols
    assert "proj_total_upsets" in cols
    assert "mean_avg_seed" in cols
    assert "mean_p_playoffs" in cols
    assert "mean_p_bye" in cols
    assert "mean_p_semis" in cols
    assert "mean_p_final" in cols
    assert "mean_p_champ" in cols
    assert "mean_exp_final_wins" in cols
    assert "mean_exp_final_pf" in cols

    # Aggregation counts use BIGINT
    assert cols["games"] == "BIGINT"
    assert cols["wins"] == "BIGINT"
    assert cols["losses"] == "BIGINT"
    assert cols["above_league_median"] == "BIGINT"
    assert cols["optimal_games"] == "BIGINT"
    assert cols["optimal_wins"] == "BIGINT"
    assert cols["proj_expected_wins"] == "BIGINT"
    assert cols["is_champion"] == "BIGINT"


def test_matchup_career_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import MATCHUP_CAREER_COLUMN_TYPES

    cols = MATCHUP_CAREER_COLUMN_TYPES

    assert "total_team_points" in cols
    assert "total_opponent_points" in cols
    assert "avg_team_points" in cols
    assert "avg_opponent_points" in cols
    assert "avg_margin" in cols
    assert "champion_seasons" in cols
    assert "max_team_points" in cols
    assert "min_team_points" in cols
    assert "optimal_bench_pts" in cols
    assert "optimal_efficiency" in cols
    assert "optimal_losses" in cols
    assert "proj_total_upsets" in cols
    assert "close_win_pct" in cols

    # Old names removed
    assert "total_pf" not in cols
    assert "total_pa" not in cols
    assert "ppg" not in cols
    assert "pag" not in cols
    assert "diff" not in cols
    assert "best" not in cols
    assert "worst" not in cols
    assert "champ_seasons" not in cols
    assert "franchise_name" not in cols

    # BIGINT types
    assert cols["games"] == "BIGINT"
    assert cols["seasons"] == "BIGINT"
    assert cols["champion_seasons"] == "BIGINT"


def test_matchup_h2h_season_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import MATCHUP_H2H_SEASON_COLUMN_TYPES

    cols = MATCHUP_H2H_SEASON_COLUMN_TYPES

    assert "total_team_points" in cols
    assert "max_team_points" in cols
    assert "min_team_points" in cols
    assert "franchise_id" in cols
    assert "opponent_franchise_id" in cols

    assert "total_pf" not in cols
    assert "highest" not in cols
    assert "lowest" not in cols

    assert cols["games"] == "BIGINT"


def test_matchup_h2h_career_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import MATCHUP_H2H_CAREER_COLUMN_TYPES

    cols = MATCHUP_H2H_CAREER_COLUMN_TYPES

    assert "total_team_points" in cols
    assert "max_team_points" in cols
    assert "min_team_points" in cols
    assert "recent_team_points" in cols
    assert "franchise_id" in cols
    assert "opponent_franchise_id" in cols

    assert "total_pf" not in cols
    assert "highest" not in cols
    assert "lowest" not in cols
    assert "recent_pts" not in cols

    assert cols["games"] == "BIGINT"
    assert cols["recent_win"] == "BIGINT"


def test_luck_all_play_tables_registered_for_centralized_publish():
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    for table_name in ("all_play", "h2h_season", "schedule_swap", "schedule_swap_season"):
        assert table_name in AGGREGATE_TABLE_SPECS
        assert AGGREGATE_TABLE_SPECS[table_name].column_types["db_name"] == "VARCHAR"

    assert AGGREGATE_TABLE_SPECS["all_play"].primary_key == (
        "db_name",
        "franchise_id",
        "opponent_franchise_id",
        "year",
        "week",
    )
    assert AGGREGATE_TABLE_SPECS["h2h_season"].primary_key == (
        "db_name",
        "franchise_id",
        "opponent_franchise_id",
        "year",
    )
    assert AGGREGATE_TABLE_SPECS["schedule_swap"].primary_key == (
        "db_name",
        "franchise_id",
        "schedule_of_franchise_id",
        "year",
        "week",
    )
    assert AGGREGATE_TABLE_SPECS["schedule_swap_season"].primary_key == (
        "db_name",
        "franchise_id",
        "schedule_of_franchise_id",
        "year",
    )


def test_simulation_summary_removed():
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    assert "simulation_summary" not in AGGREGATE_TABLE_SPECS

    import multi_league.core.aggregate_ddl as mod

    assert not hasattr(mod, "SIMULATION_SUMMARY_STAT_COLUMNS")
    assert not hasattr(mod, "SIMULATION_SUMMARY_COLUMN_TYPES")


def test_player_season_no_platform_specific_columns():
    from multi_league.core.aggregate_ddl import PLAYER_FANTASY_SEASON_COLUMN_TYPES

    assert "yahoo_player_id" not in PLAYER_FANTASY_SEASON_COLUMN_TYPES
    assert "headshot_url" not in PLAYER_FANTASY_SEASON_COLUMN_TYPES


def test_player_career_no_platform_specific_columns():
    from multi_league.core.aggregate_ddl import PLAYER_FANTASY_CAREER_COLUMN_TYPES

    assert "yahoo_player_id" not in PLAYER_FANTASY_CAREER_COLUMN_TYPES
    assert "headshot_url" not in PLAYER_FANTASY_CAREER_COLUMN_TYPES


def test_player_season_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import PLAYER_FANTASY_SEASON_COLUMN_TYPES

    cols = PLAYER_FANTASY_SEASON_COLUMN_TYPES

    # Renamed columns
    assert "fantasy_points" in cols
    assert "games_rostered" in cols
    assert "position" in cols
    assert "nfl_team" in cols
    assert "franchise_id" in cols

    # Old names removed
    assert "points" not in cols
    assert "fantasy_games" not in cols
    assert "champion" not in cols
    assert "sacko_count" not in cols


def test_player_career_uses_normalized_column_names():
    from multi_league.core.aggregate_ddl import PLAYER_FANTASY_CAREER_COLUMN_TYPES

    cols = PLAYER_FANTASY_CAREER_COLUMN_TYPES

    # Renamed columns
    assert "fantasy_points" in cols
    assert "games_rostered" in cols
    assert "position" in cols
    assert "nfl_team" in cols
    assert "franchise_id" in cols

    # Career-specific retained
    assert "first_year" in cols
    assert "last_year" in cols
    assert "years_active" in cols

    # Old names removed
    assert "points" not in cols
    assert "fantasy_games" not in cols
    assert "champion" not in cols
    assert "sacko_count" not in cols


def test_franchise_id_in_all_manager_dimension_tables():
    from multi_league.core.aggregate_ddl import (
        DRAFT_MANAGER_SEASON_COLUMN_TYPES,
        DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        TRANSACTION_MANAGER_SEASON_COLUMN_TYPES,
        TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        TRANSACTION_REPORT_CARD_COLUMN_TYPES,
        STANDINGS_BY_YEAR_COLUMN_TYPES,
        HOMEPAGE_CURRENT_STANDINGS_COLUMN_TYPES,
        HOMEPAGE_MANAGER_RANKINGS_COLUMN_TYPES,
        HOMEPAGE_MANAGER_PROFILES_COLUMN_TYPES,
        MATCHUP_SEASON_COLUMN_TYPES,
        MATCHUP_CAREER_COLUMN_TYPES,
    )

    manager_tables = {
        "draft_manager_season": DRAFT_MANAGER_SEASON_COLUMN_TYPES,
        "draft_manager_career": DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        "transaction_manager_season": TRANSACTION_MANAGER_SEASON_COLUMN_TYPES,
        "transaction_manager_career": TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        "transaction_report_card": TRANSACTION_REPORT_CARD_COLUMN_TYPES,
        "standings_by_year": STANDINGS_BY_YEAR_COLUMN_TYPES,
        "homepage_current_standings": HOMEPAGE_CURRENT_STANDINGS_COLUMN_TYPES,
        "homepage_manager_rankings": HOMEPAGE_MANAGER_RANKINGS_COLUMN_TYPES,
        "homepage_manager_profiles": HOMEPAGE_MANAGER_PROFILES_COLUMN_TYPES,
        "matchup_season": MATCHUP_SEASON_COLUMN_TYPES,
        "matchup_career": MATCHUP_CAREER_COLUMN_TYPES,
    }

    for table_name, cols in manager_tables.items():
        assert "franchise_id" in cols, f"franchise_id missing from {table_name}"


def test_franchise_ids_in_player_centric_tables():
    from multi_league.core.aggregate_ddl import (
        DRAFT_PLAYER_CAREER_COLUMN_TYPES,
        TRANSACTION_PLAYER_CAREER_COLUMN_TYPES,
    )

    assert "franchise_ids" in DRAFT_PLAYER_CAREER_COLUMN_TYPES
    assert "franchise_ids" in TRANSACTION_PLAYER_CAREER_COLUMN_TYPES


def test_transaction_tables_use_transaction_grade():
    from multi_league.core.aggregate_ddl import (
        TRANSACTION_MANAGER_SEASON_COLUMN_TYPES,
        TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        TRANSACTION_REPORT_CARD_COLUMN_TYPES,
    )

    for cols in [
        TRANSACTION_MANAGER_SEASON_COLUMN_TYPES,
        TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        TRANSACTION_REPORT_CARD_COLUMN_TYPES,
    ]:
        assert "transaction_grade" in cols
        assert "transaction_gpa" in cols
        assert "grade" not in cols
        assert "gpa" not in cols


def test_draft_tables_use_total_manager_lamar():
    from multi_league.core.aggregate_ddl import (
        DRAFT_MANAGER_SEASON_COLUMN_TYPES,
        DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        DRAFT_PLAYER_CAREER_COLUMN_TYPES,
    )

    for cols in [
        DRAFT_MANAGER_SEASON_COLUMN_TYPES,
        DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        DRAFT_PLAYER_CAREER_COLUMN_TYPES,
    ]:
        assert "total_manager_lamar" in cols
        assert "avg_manager_lamar" in cols
        assert "total_lamar" not in cols
        assert "avg_lamar" not in cols


def test_no_txn_prefix_anywhere():
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    for table_name, spec in AGGREGATE_TABLE_SPECS.items():
        for col_name in spec.column_types:
            assert not col_name.startswith("txn_"), f"Column {col_name} in {table_name} still uses txn_ prefix"

    from multi_league.core.aggregate_ddl import HOMEPAGE_MANAGER_TXN_COLUMN_TYPES

    for col_name in HOMEPAGE_MANAGER_TXN_COLUMN_TYPES:
        assert not col_name.startswith(
            "txn_"
        ), f"Column {col_name} in HOMEPAGE_MANAGER_TXN_COLUMN_TYPES still uses txn_ prefix"


def test_homepage_top_rivalries_has_franchise_ids():
    from multi_league.core.aggregate_ddl import HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES

    assert "franchise_id_1" in HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES
    assert "franchise_id_2" in HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES


def test_draft_manager_season_pk_excludes_draft_category():
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS

    spec = AGGREGATE_TABLE_SPECS["draft_manager_season"]
    assert "draft_category" not in spec.primary_key
    # draft_manager_season has no primary_key defined (empty tuple before
    # _prepend_primary_key), so _prepend_primary_key returns () — db_name is
    # NOT prepended when the source PK is empty.
    assert spec.primary_key == ()
