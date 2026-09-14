import json
import duckdb
from multi_league.external_ingest.schema_conform._ledger import (
    create_schema_decisions_table,
    log_decision,
    query_decisions_for_run,
    create_external_identity_overrides_table,
    read_overrides_for_db,
    create_import_run_summary_table,
    log_run_summary,
)


def _conn():
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA staging")
    return c


def test_create_table_idempotent():
    c = _conn()
    create_schema_decisions_table(c)
    create_schema_decisions_table(c)  # second call must not error
    rows = c.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='staging' AND table_name='schema_decisions'"
    ).fetchone()
    assert rows[0] == 1


def test_log_decision_round_trips():
    c = _conn()
    create_schema_decisions_table(c)
    log_decision(
        c,
        dict(
            db_name="kmffl",
            run_id="run-1",
            table_name="matchup",
            source_col="manager_guid",
            ddl_slot="manager_guid",
            score=0.95,
            score_components={"name": 1.0, "value": 1.0, "dtype": 1.0, "shape": 1.0},
            second_best_score=0.30,
            second_best_slot="opponent_guid",
            pass_number=3,
            was_derived=False,
            derivation_rule=None,
            was_coalesced=False,
            coalesce_inputs=None,
            status="bound",
            invariant_failures=[],
            sample_values=["GUID_A", "GUID_B"],
        ),
    )
    rows = query_decisions_for_run(c, db_name="kmffl", run_id="run-1")
    assert len(rows) == 1
    assert rows[0]["status"] == "bound"
    assert rows[0]["score"] == 0.95
    assert rows[0]["score_components"]["name"] == 1.0


def test_create_import_run_summary_table_idempotent():
    c = _conn()
    create_import_run_summary_table(c)
    create_import_run_summary_table(c)  # second call must not error
    rows = c.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema='staging' AND table_name='import_run_summary'"
    ).fetchone()
    assert rows[0] == 1


def test_log_run_summary_round_trips():
    c = _conn()
    create_import_run_summary_table(c)
    failures = [
        {
            "table": "matchup",
            "gate": "UNMAPPED_REQUIRED",
            "ddl_slot": "manager_guid",
            "source_cols_considered": ["mgr_id"],
            "best_score": 0.3,
            "second_best_score": None,
            "second_best_slot": None,
            "suggested_action": "check column",
            "invariant_failures": [],
        }
    ]
    log_run_summary(
        c,
        db_name="kmffl",
        run_id="run-1",
        phase="schema_conform",
        status="failed",
        failures_json=failures,
        detail="1 failure",
    )
    row = c.execute(
        "SELECT db_name, run_id, phase, status, failures_json, detail "
        "FROM staging.import_run_summary WHERE db_name='kmffl'"
    ).fetchone()
    assert row is not None
    assert row[0] == "kmffl"
    assert row[1] == "run-1"
    assert row[2] == "schema_conform"
    assert row[3] == "failed"
    # failures_json may come back as str or dict depending on DuckDB version
    raw_json = row[4]
    parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    assert len(parsed) == 1
    assert parsed[0]["gate"] == "UNMAPPED_REQUIRED"
    assert row[5] == "1 failure"


def test_external_identity_overrides_create_and_read():
    c = _conn()
    create_external_identity_overrides_table(c)
    c.execute("""
        INSERT INTO staging.external_identity_overrides
            (db_name, source_manager_norm, decision, target_franchise_id,
             external_hash, confirmed_by, confirmed_at)
        VALUES ('kmffl', 'rubinstein', 'new_manager', NULL, 'external_abc12345',
                'joe', '2026-04-25 12:00:00'::TIMESTAMP)
    """)
    overrides = read_overrides_for_db(c, db_name="kmffl")
    assert "rubinstein" in overrides
    assert overrides["rubinstein"]["decision"] == "new_manager"
    assert overrides["rubinstein"]["external_hash"] == "external_abc12345"
