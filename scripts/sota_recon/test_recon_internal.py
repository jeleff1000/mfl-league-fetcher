from __future__ import annotations

import duckdb

from . import recon_internal


def test_documented_null_attempt_override_is_loaded(tmp_path):
    db = tmp_path / "facts.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        """
        CREATE TABLE cell_override (
            fact_id VARCHAR, created_at TIMESTAMP, wave_id VARCHAR, reason VARCHAR,
            witness VARCHAR, source_snapshot_id VARCHAR, table_name VARCHAR,
            target_key VARCHAR, column_name VARCHAR, old_value VARCHAR, new_value VARCHAR
        )
        """
    )
    con.execute(
        """INSERT INTO cell_override VALUES
        ('fact-1', TIMESTAMP '2026-07-12', 'wave55', 'historical gap',
         'box confirms INT; attempts unrecorded', 'snap-1', 'nfl_player_stats_all',
         'PresGl20_1933_12', 'attempts', '1.0', 'NULL')"""
    )
    con.close()
    assert recon_internal._load_documented_attempt_gaps(db) == [
        ("PresGl20_1933_12", "fact-1", "wave55", "historical gap",
         "box confirms INT; attempts unrecorded", "snap-1")
    ]


def test_missing_facts_db_is_empty(tmp_path):
    assert recon_internal._load_documented_attempt_gaps(tmp_path / "missing.duckdb") == []
