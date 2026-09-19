"""The shared weekly enrichment graph must see the disposable local OPS cache."""

from __future__ import annotations

import duckdb
from pathlib import Path
import logging


def test_weekly_shared_enrichment_attaches_the_patched_ops_cache_once(tmp_path, monkeypatch):
    """Rank enrichment runs before publication and must see the patched cache."""
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
