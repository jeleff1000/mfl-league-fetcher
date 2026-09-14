"""Unit tests for apply_keeper_rules SQL enrichment."""

import duckdb
import pytest
from unittest.mock import MagicMock, patch

import pandas as pd

from multi_league.transformations.sql_enrichments import SQLEnrichments
from multi_league.core.readers.fly_reader import FlyReaderTableNotFound, FlyReaderNetworkError


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA public")
    c.execute("SET schema = 'public'")
    c.execute("""
        CREATE TABLE player_fantasy (
            db_name VARCHAR, year INT, week INT, manager VARCHAR,
            NFL_player_id VARCHAR, round INT, pick INT, cost INT,
            max_faab_bid_to_date INT, keeper_year INT,
            base_keeper_cost INT, keeper_price INT
        )
    """)
    c.execute("""
        INSERT INTO player_fantasy VALUES
          ('foo', 2024, 1, 'Joe', 'p1', 5, 60, 0, 0, 0, NULL, NULL),
          ('foo', 2024, 1, 'Joe', 'p2', 0, 0, 20, 25, 1, NULL, NULL),
          ('foo', 2024, 1, 'Unrostered', 'p3', 0, 0, 0, 0, 0, NULL, NULL)
    """)
    from multi_league.core.keeper_config_schema import KEEPER_CONFIG_DDL

    c.execute(KEEPER_CONFIG_DDL)
    return c


def _make_enricher(conn):
    eng = SQLEnrichments(conn=conn, db_name="foo", data_dir="unused")
    return eng


def test_no_config_returns_zero(conn, caplog):
    eng = _make_enricher(conn)
    with caplog.at_level("INFO"):
        result = eng.apply_keeper_rules()
    assert result == 0
    assert "no config found" in caplog.text.lower() or "skip" in caplog.text.lower()


def test_disabled_config_returns_zero(conn, caplog):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds, snake_drafted_round_offset, snake_fa_pickup_source) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, FALSE, 'snake', 2, 1, 15, -1, 'last')"
    )
    eng = _make_enricher(conn)
    with caplog.at_level("INFO"):
        result = eng.apply_keeper_rules()
    assert result == 0


def test_snake_config_writes_base_cost(conn):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds, min_round, snake_drafted_round_offset, snake_fa_pickup_source, escalation_type, escalation_rounds_per_year) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 2, 1, 15, 1, -1, 'last', 'round_escalation', 1)"
    )
    eng = _make_enricher(conn)
    eng.apply_keeper_rules()
    # p1 (round 5, drafted): base_keeper_cost = max(round + offset, 1) = max(4, 1) = 4
    row = conn.execute("SELECT base_keeper_cost FROM player_fantasy WHERE NFL_player_id = 'p1'").fetchone()
    assert row[0] == 4
    # p2 (no draft round, snake): falls to fa_round = num_rounds = 15
    row2 = conn.execute("SELECT base_keeper_cost FROM player_fantasy WHERE NFL_player_id = 'p2'").fetchone()
    assert row2[0] == 15
    # p3 (Unrostered): not touched
    row3 = conn.execute("SELECT base_keeper_cost FROM player_fantasy WHERE NFL_player_id = 'p3'").fetchone()
    assert row3[0] is None


def test_free_snake_config_writes_zero_cost_forever(conn):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, max_years, min_price, num_rounds, min_round, escalation_type, escalation_rounds_per_year) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 5, NULL, 0, 15, 0, 'none', 0)"
    )
    eng = _make_enricher(conn)
    eng.apply_keeper_rules()
    rows = conn.execute(
        "SELECT NFL_player_id, base_keeper_cost, keeper_price "
        "FROM player_fantasy WHERE manager = 'Joe' ORDER BY NFL_player_id"
    ).fetchall()
    assert rows == [("p1", 0, None), ("p2", 0, 0)]


def test_free_auction_config_writes_zero_cost_forever(conn):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, max_years, min_price, "
        "auction_drafted_mult, auction_drafted_flat, auction_faab_mult, auction_faab_flat, auction_fa_value, escalation_type) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'auction', 5, NULL, 0, 0, 0, 0, 0, 0, 'none')"
    )
    eng = _make_enricher(conn)
    eng.apply_keeper_rules()
    rows = conn.execute(
        "SELECT NFL_player_id, base_keeper_cost, keeper_price "
        "FROM player_fantasy WHERE manager = 'Joe' ORDER BY NFL_player_id"
    ).fetchall()
    assert rows == [("p1", 0, None), ("p2", 0, 0)]


def test_idempotent_second_run(conn):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds, min_round, snake_drafted_round_offset, snake_fa_pickup_source, escalation_type, escalation_flat_per_year) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 2, 1, 15, 1, -1, 'last', 'from_base', 5)"
    )
    eng = _make_enricher(conn)
    eng.apply_keeper_rules()
    snapshot1 = conn.execute(
        "SELECT NFL_player_id, base_keeper_cost, keeper_price FROM player_fantasy ORDER BY NFL_player_id"
    ).fetchall()
    eng.apply_keeper_rules()
    snapshot2 = conn.execute(
        "SELECT NFL_player_id, base_keeper_cost, keeper_price FROM player_fantasy ORDER BY NFL_player_id"
    ).fetchall()
    assert snapshot1 == snapshot2


def test_unrostered_rows_never_touched(conn):
    """ROSTERED_FILTER excludes Unrostered/FA/free agent/waivers managers."""
    conn.execute(
        "INSERT INTO player_fantasy VALUES "
        "('foo', 2024, 1, 'fa', 'p4', 5, 60, 0, 0, 0, NULL, NULL), "
        "('foo', 2024, 1, '', 'p5', 5, 60, 0, 0, 0, NULL, NULL)"
    )
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds, min_round, snake_drafted_round_offset, snake_fa_pickup_source, escalation_type) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 2, 1, 15, 1, -1, 'last', 'none')"
    )
    eng = _make_enricher(conn)
    eng.apply_keeper_rules()
    # All non-rostered rows stay NULL
    rows = conn.execute(
        "SELECT NFL_player_id, base_keeper_cost FROM player_fantasy WHERE NFL_player_id IN ('p3','p4','p5') ORDER BY NFL_player_id"
    ).fetchall()
    for nfl_id, base_cost in rows:
        assert base_cost is None, f"{nfl_id} should not be touched"


# ────────────────────────────────────────────────────────────────────────────
# _sync_keeper_config_from_fly tests
# ────────────────────────────────────────────────────────────────────────────


def test_sync_skips_fly_in_corpus_mode(conn, monkeypatch):
    monkeypatch.setenv("CORPUS_MODE", "1")
    eng = _make_enricher(conn)

    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        side_effect=AssertionError("FlyReader must not be constructed in corpus mode"),
    ):
        assert eng._sync_keeper_config_from_fly() == 0


def test_apply_keeper_rules_targets_public_player_fantasy():
    """Canonical local league files keep data under the public schema."""
    c = duckdb.connect(":memory:")
    try:
        c.execute("CREATE SCHEMA public")
        c.execute(
            """
            CREATE TABLE public.player_fantasy (
                db_name VARCHAR, year INT, week INT, manager VARCHAR,
                NFL_player_id VARCHAR, round INT, pick INT, cost INT,
                max_faab_bid_to_date INT, keeper_year INT,
                base_keeper_cost INT, keeper_price INT
            )
            """
        )
        c.execute(
            "INSERT INTO public.player_fantasy VALUES "
            "('foo', 2024, 1, 'Joe', 'p1', 0, 0, 20, 0, 1, NULL, NULL)"
        )
        from multi_league.core.keeper_config_schema import KEEPER_CONFIG_DDL

        c.execute(KEEPER_CONFIG_DDL.replace("keeper_config", "public.keeper_config", 1))
        c.execute(
            "INSERT INTO public.keeper_config "
            "(db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, "
            "auction_drafted_mult, auction_drafted_flat, auction_faab_mult, auction_faab_flat, "
            "auction_fa_value, escalation_type) "
            "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'auction', 2, 1, 1, 0, 1, 0, 1, 'none')"
        )

        eng = SQLEnrichments(conn=c, db_name="foo", data_dir="unused")
        eng.apply_keeper_rules()

        assert c.execute(
            "SELECT base_keeper_cost, keeper_price FROM public.player_fantasy"
        ).fetchone() == (20, 20)
    finally:
        c.close()


def test_sync_handles_table_not_found_cleanly(conn, caplog):
    eng = _make_enricher(conn)
    fake_reader = MagicMock()
    fake_reader.query_df.side_effect = FlyReaderTableNotFound("Catalog Error: Table 'keeper_config' does not exist")
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        return_value=fake_reader,
    ):
        with caplog.at_level("INFO"):
            result = eng._sync_keeper_config_from_fly()
    assert result == 0
    assert "no keeper_config table on fly" in caplog.text.lower() or "skip" in caplog.text.lower()


def test_sync_handles_network_error_cleanly(conn, caplog):
    eng = _make_enricher(conn)
    fake_reader = MagicMock()
    fake_reader.query_df.side_effect = FlyReaderNetworkError("nope")
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        return_value=fake_reader,
    ):
        with caplog.at_level("INFO"):
            result = eng._sync_keeper_config_from_fly()
    assert result == 0


def test_sync_uses_local_config_without_fly(conn, caplog):
    conn.execute(
        "INSERT INTO keeper_config (db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 2, 1, 15)"
    )
    eng = _make_enricher(conn)
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        side_effect=AssertionError("FlyReader should not be called when local keeper_config exists"),
    ):
        with caplog.at_level("INFO"):
            result = eng._sync_keeper_config_from_fly()
    assert result == 0
    assert "using local keeper_config" in caplog.text.lower()


def test_sync_uses_an_empty_hydrated_keeper_snapshot_without_fly(conn):
    """An active refresh has already read the authoritative empty configuration."""
    eng = SQLEnrichments(conn=conn, db_name="foo", data_dir="unused", keeper_config_hydrated=True)

    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        side_effect=AssertionError("FlyReader should not be called after a keeper snapshot"),
    ):
        assert eng._sync_keeper_config_from_fly() == 0


def test_sync_prefers_public_keeper_config_without_fly(conn, caplog):
    from multi_league.core.keeper_config_schema import KEEPER_CONFIG_DDL

    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(KEEPER_CONFIG_DDL.replace("keeper_config", "public.keeper_config", 1))
    conn.execute(
        "INSERT INTO public.keeper_config "
        "(db_name, year, updated_at, enabled, draft_type, max_keepers, min_price, num_rounds) "
        "VALUES ('foo', 0, CURRENT_TIMESTAMP, TRUE, 'snake', 2, 1, 15)"
    )
    eng = _make_enricher(conn)
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        side_effect=AssertionError("FlyReader should not be called when public keeper_config exists"),
    ):
        with caplog.at_level("INFO"):
            result = eng._sync_keeper_config_from_fly()
    assert result == 0
    assert "using local keeper_config" in caplog.text.lower()


def test_sync_scopes_local_keeper_config_by_db_name(conn):
    from multi_league.core.keeper_config_schema import KEEPER_CONFIG_COLUMNS

    conn.execute(
        "INSERT INTO keeper_config "
        "(db_name, year, updated_at, enabled, draft_type, max_keepers, min_price) "
        "VALUES ('bar', 0, CURRENT_TIMESTAMP, TRUE, 'auction', 9, 1)"
    )
    sample = {col: None for col in KEEPER_CONFIG_COLUMNS}
    sample.update(
        {
            "db_name": "foo",
            "year": 0,
            "updated_at": pd.Timestamp("2026-01-01"),
            "enabled": True,
            "draft_type": "snake",
            "max_keepers": 2,
            "min_price": 1,
        }
    )
    fake_reader = MagicMock()
    fake_reader.query_df.return_value = pd.DataFrame([sample])
    eng = _make_enricher(conn)

    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        return_value=fake_reader,
    ):
        assert eng._sync_keeper_config_from_fly() == 1

    assert conn.execute(
        "SELECT db_name, draft_type, max_keepers FROM keeper_config ORDER BY db_name"
    ).fetchall() == [("bar", "auction", 9), ("foo", "snake", 2)]
    assert eng._read_keeper_config_default()["draft_type"] == "snake"


def test_sync_loads_rows_into_local(conn):
    eng = _make_enricher(conn)
    fake_reader = MagicMock()
    from multi_league.core.keeper_config_schema import KEEPER_CONFIG_COLUMNS

    sample = {col: None for col in KEEPER_CONFIG_COLUMNS}
    sample.update(
        {
            "db_name": "foo",
            "year": 0,
            "updated_at": pd.Timestamp("2026-01-01"),
            "enabled": True,
            "draft_type": "snake",
            "max_keepers": 2,
            "min_price": 1,
            "num_rounds": 15,
            "min_round": 1,
            "snake_drafted_round_offset": -1,
            "snake_fa_pickup_source": "last",
        }
    )
    fake_reader.query_df.return_value = pd.DataFrame([sample])
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        return_value=fake_reader,
    ):
        result = eng._sync_keeper_config_from_fly()
    assert result == 1
    query, = fake_reader.query_df.call_args.args
    assert "FROM public.keeper_config" in query
    assert "WHERE db_name = 'foo'" in query
    assert fake_reader.query_df.call_args.kwargs["database"] == "___leagues"
    row = conn.execute("SELECT enabled, draft_type, max_keepers FROM keeper_config WHERE year = 0").fetchone()
    assert row == (True, "snake", 2)


def test_sync_propagates_schema_mismatch_loudly(conn):
    """Schema mismatch (binder error) is NOT transient — propagate to surface migration bug."""
    eng = _make_enricher(conn)
    fake_reader = MagicMock()
    fake_reader.query_df.side_effect = RuntimeError("Binder Error: Referenced column 'foo' not found")
    with patch(
        "multi_league.transformations.player.sql_player_enrichments.FlyReader",
        return_value=fake_reader,
    ):
        with pytest.raises(RuntimeError, match="Binder Error"):
            eng._sync_keeper_config_from_fly()
