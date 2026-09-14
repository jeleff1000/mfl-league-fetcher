"""Unit tests for LocalLeagueDB DDL wiring.

Verifies that ensure_table() creates draft and transactions tables
using canonical DDL (correct columns from canonical_draft.py and
canonical_transaction.py).
"""

import pytest
import pandas as pd
import polars as pl
from multi_league.core.local_db import LocalLeagueDB


@pytest.fixture
def tmp_db(tmp_path):
    db = LocalLeagueDB(tmp_path, "test_league")
    yield db
    db.close()


def test_ensure_table_creates_draft_with_canonical_ddl(tmp_db):
    tmp_db.ensure_table("draft")
    cols = {
        r[0]
        for r in tmp_db.connect()
        .execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'draft' AND table_schema = 'public'"
        )
        .fetchall()
    }
    assert "year" in cols
    assert "round" in cols
    assert "pick" in cols
    assert "NFL_player_id" in cols


def test_ensure_table_creates_transactions_with_canonical_ddl(tmp_db):
    tmp_db.ensure_table("transactions")
    cols = {
        r[0]
        for r in tmp_db.connect()
        .execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'transactions' AND table_schema = 'public'"
        )
        .fetchall()
    }
    assert "transaction_id" in cols
    assert "faab_bid" in cols
    assert "NFL_player_id" in cols


def test_ensure_table_creates_franchise_identity_registry(tmp_db):
    tmp_db.ensure_table("franchise_identity_registry")
    cols = {
        r[0]
        for r in tmp_db.connect()
        .execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'franchise_identity_registry' AND table_schema = 'public'"
        )
        .fetchall()
    }
    assert "db_name" in cols
    assert "resolved_franchise_id" in cols
    assert "identity_key" in cols
    assert "known_team_names" in cols


def test_execute_sql(tmp_db):
    tmp_db.connect().execute("CREATE TABLE public.test_tbl (id INTEGER, name VARCHAR)")
    tmp_db.execute_sql("INSERT INTO public.test_tbl VALUES (1, 'hello')")
    result = tmp_db.execute_sql("SELECT * FROM public.test_tbl").fetchdf()
    assert len(result) == 1


def test_list_tables(tmp_db):
    tmp_db.ensure_table("matchup")
    tmp_db.ensure_table("draft")
    tables = tmp_db.list_tables()
    assert "matchup" in tables
    assert "draft" in tables


def test_row_count(tmp_db):
    tmp_db.ensure_table("matchup")
    assert tmp_db.row_count("matchup") == 0
    tmp_db.connect().execute("INSERT INTO public.matchup (db_name, year, week) VALUES ('test_league', 2024, 1)")
    assert tmp_db.row_count("matchup") == 1


def test_save_table_schema_filter(tmp_db):
    """Verify save_table only uses public schema columns, not system tables."""
    df = pd.DataFrame({"year": [2024], "week": [1], "manager": ["Joe"]})
    tmp_db.save_table("matchup", df, year=2024)
    result = tmp_db.read_table("matchup")
    assert len(result) == 1
    assert result.iloc[0]["manager"] == "Joe"


def test_save_table_infers_platform_from_source_ids(tmp_db):
    """Sleeper source IDs should not silently normalize as Yahoo when platform is omitted."""
    df = pd.DataFrame(
        {
            "transaction_id": ["1"],
            "year": [2025],
            "week": [1],
            "transaction_type": ["add"],
            "player": ["Player A"],
            "sleeper_player_id": ["12345"],
        }
    )
    tmp_db.save_table("transactions", df, year=2025)
    result = tmp_db.read_table("transactions")
    assert len(result) == 1
    assert result.iloc[0]["platform"] == "sleeper"
    assert result.iloc[0]["sleeper_player_id"] == "12345"


def test_save_table_accepts_polars_frames(tmp_db):
    df = pl.DataFrame(
        {
            "year": [2025],
            "platform": ["yahoo"],
            "league_key": ["461.l.90939"],
            "num_teams": [10],
            "scoring_type": ["half_ppr"],
        }
    )

    tmp_db.save_table("league_settings", df, year=2025)
    result = tmp_db.read_table("league_settings")

    assert len(result) == 1
    assert int(result.iloc[0]["year"]) == 2025
    assert result.iloc[0]["league_key"] == "461.l.90939"


def test_save_table_fills_null_db_name_in_polars_frames(tmp_db):
    """Mixed staged/API settings must inherit the target league partition."""
    df = pl.DataFrame(
        {
            "db_name": ["test_league", None],
            "year": [2014, 2025],
            "platform": ["yahoo", "yahoo"],
            "league_key": ["331.l.381581", "461.l.90939"],
        }
    )

    tmp_db.save_table("league_settings", df)

    rows = tmp_db.connect().execute(
        "SELECT year, db_name FROM public.league_settings ORDER BY year"
    ).fetchall()
    assert rows == [(2014, "test_league"), (2025, "test_league")]


def test_ensure_table_migrates_stale_matchup_column_types(tmp_db):
    conn = tmp_db.connect()
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            final_playoff_seed VARCHAR,
            playoff_round INTEGER,
            consolation_round INTEGER,
            season_result INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup (year, week, manager, final_playoff_seed, playoff_round, consolation_round, season_result)
        VALUES (2025, 17, 'Paul', '1', 3, 2, 1)
        """
    )

    tmp_db.ensure_table("matchup")

    col_types = {r[0]: r[1] for r in conn.execute("DESCRIBE public.matchup").fetchall()}
    assert col_types["final_playoff_seed"] == "INTEGER"
    assert col_types["playoff_round"] == "VARCHAR"
    assert col_types["consolation_round"] == "VARCHAR"
    assert col_types["season_result"] == "VARCHAR"

    row = conn.execute(
        "SELECT final_playoff_seed, playoff_round, consolation_round, season_result FROM public.matchup"
    ).fetchone()
    assert row == (1, "3", "2", "1")


def test_validate_table_schema_reports_no_mismatches_after_migration(tmp_db):
    conn = tmp_db.connect()
    conn.execute("CREATE TABLE public.schedule (year VARCHAR, week VARCHAR, manager VARCHAR)")
    tmp_db.ensure_table("schedule")

    mismatches = tmp_db.validate_table_schema("schedule")
    assert mismatches == []


def test_validate_table_schema_reports_unexpected_columns(tmp_db):
    conn = tmp_db.connect()
    conn.execute("CREATE TABLE public.schedule (year VARCHAR, week VARCHAR, manager VARCHAR, stray VARCHAR)")
    tmp_db.ensure_table("schedule")

    mismatches = tmp_db.validate_table_schema("schedule")
    assert "schedule.stray unexpected" in mismatches


def test_recanonicalize_table_rewrites_canonical_draft_values(tmp_db):
    conn = tmp_db.connect()
    tmp_db.ensure_table("draft")
    conn.execute(
        """
        INSERT INTO public.draft (db_name, year, round, pick, manager, player, position, is_keeper, platform, league_id)
        VALUES ('test_league', 2025, 1, 1, 'Joe', 'Bills', NULL, 0, 'yahoo', NULL)
        """
    )

    row_count = tmp_db.recanonicalize_table("draft", platform="yahoo", league_id="461.l.90939")

    assert row_count == 1
    row = conn.execute("SELECT player, position, is_keeper, league_id FROM public.draft").fetchone()
    assert row == ("Bills DST", "DEF", 0, "461.l.90939")
