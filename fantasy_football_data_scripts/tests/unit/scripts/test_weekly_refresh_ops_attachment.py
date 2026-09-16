"""The shared weekly aggregate graph must see the disposable local OPS cache."""

from __future__ import annotations

import duckdb
from pathlib import Path
import logging


def test_weekly_shared_enrichment_attaches_the_patched_ops_cache_once(tmp_path, monkeypatch):
    """Rank enrichment runs before aggregates and must see the patched cache."""
    from scripts.refresh_yahoo_active_season import _attach_ops_cache_for_enrichment

    ops_path = tmp_path / "weekly_ops.duckdb"
    with duckdb.connect(str(ops_path)) as ops:
        ops.execute("CREATE SCHEMA nfl_historical")
        ops.execute(
            "CREATE TABLE nfl_historical.nfl_player_stats_all "
            "(NFL_player_id VARCHAR, rank_alltime_te_half INTEGER)"
        )
        ops.execute(
            "INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('00-0041395', 1026)"
        )
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_path))

    class LocalDB:
        def __init__(self):
            self.conn = duckdb.connect()

        def connect(self):
            return self.conn

    local_db = LocalDB()
    try:
        _attach_ops_cache_for_enrichment(local_db)
        _attach_ops_cache_for_enrichment(local_db)
        assert local_db.conn.execute(
            "SELECT rank_alltime_te_half FROM ___ops.nfl_historical.nfl_player_stats_all "
            "WHERE NFL_player_id = '00-0041395'"
        ).fetchone() == (1026,)
    finally:
        local_db.conn.close()

    source = (
        Path(__file__).resolve().parents[4] / "scripts" / "refresh_yahoo_active_season.py"
    ).read_text(encoding="utf-8")
    local_pipeline = source.split("def _run_local_pipeline(", 1)[1].split(
        "def _unresolved_provider_schedule_rows", 1
    )[0]
    assert local_pipeline.index("_attach_ops_cache_for_enrichment(local_db)") < local_pipeline.index(
        "enricher = SQLEnrichments("
    )


def test_shared_sql_enricher_accepts_an_existing_ops_attachment(tmp_path, monkeypatch, caplog):
    from multi_league.transformations.common.sql_base import SQLEnrichmentsBase

    ops_path = tmp_path / "weekly_ops.duckdb"
    with duckdb.connect(str(ops_path)) as ops:
        ops.execute("CREATE SCHEMA nfl_historical")
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_path))

    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS ___ops")
    enricher = SQLEnrichmentsBase("league_a", conn=conn)
    try:
        with caplog.at_level(logging.WARNING):
            assert enricher._get_connection() is conn
        assert "Could not attach local ops cache" not in caplog.text
        assert "No OPS_CACHE_PATH set" not in caplog.text
        assert sum(row[1] == "___ops" for row in conn.execute("PRAGMA database_list").fetchall()) == 1
    finally:
        conn.close()


def test_weekly_aggregates_attach_ops_before_season_and_career_queries(tmp_path, monkeypatch):
    from multi_league.transformations.aggregation import (
        aggregate_draft_context,
        aggregate_fantasy_context,
        aggregate_transaction_context,
        aggregation_utils,
    )
    from scripts.refresh_yahoo_active_season import _run_refresh_aggregates

    ops_path = tmp_path / "weekly_ops.duckdb"
    with duckdb.connect(str(ops_path)) as ops:
        ops.execute("CREATE SCHEMA nfl_historical")
        ops.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (NFL_player_id VARCHAR)")
        ops.execute("INSERT INTO nfl_historical.nfl_player_stats_all VALUES ('00-001')")
    monkeypatch.setenv("OPS_CACHE_PATH", str(ops_path))

    observed: list[str] = []
    for module in (aggregate_draft_context, aggregate_fantasy_context, aggregate_transaction_context):
        for name, value in vars(module).items():
            if callable(value) and (name.startswith("create_") or name.startswith("aggregate_")):
                monkeypatch.setattr(module, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(aggregation_utils, "configure_table_catalog", lambda conn: None)

    def assert_ops_visible(conn, db_name, *, year):
        assert db_name == "afi_data" and year == 2026
        assert conn.execute(
            "SELECT COUNT(*) FROM ___ops.nfl_historical.nfl_player_stats_all"
        ).fetchone()[0] == 1
        observed.append("season")

    def assert_career_ops_visible(conn, db_name):
        assert db_name == "afi_data"
        assert conn.execute(
            "SELECT COUNT(*) FROM ___ops.nfl_historical.nfl_player_stats_all"
        ).fetchone()[0] == 1
        observed.append("career")

    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_season", assert_ops_visible)
    monkeypatch.setattr(aggregate_fantasy_context, "aggregate_fantasy_career", assert_career_ops_visible)

    class LocalDB:
        def __init__(self):
            self.conn = duckdb.connect()

        def connect(self):
            return self.conn

    local_db = LocalDB()
    try:
        _run_refresh_aggregates(
            local_db,
            db_name="afi_data",
            active_year=2026,
            work_dir=tmp_path,
            has_finalized_matchups=False,
        )
        assert observed == ["season", "career"]
    finally:
        local_db.conn.close()
