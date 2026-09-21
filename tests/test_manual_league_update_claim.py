import duckdb
import pytest


def test_prepare_manual_execution_reuses_existing_clients_for_probe_and_claim(tmp_path):
    from scripts import claim_manual_league_update as manual

    calls = []
    environment = {}
    output = tmp_path / "github-output.txt"

    result = manual.prepare_update_execution(
        object(),
        object(),
        db_name="paid_league",
        platform="espn",
        execute=True,
        observed_manifest_digest="",
        dispatch_token="",
        attempt_id="",
        claim_version=1,
        run_id=42,
        run_attempt=3,
        output_path=output,
        environment=environment,
        probe=lambda db_name: calls.append(("probe", db_name)) or "digest-123",
        claim=lambda reader, writer, **kwargs: calls.append(
            ("claim", reader, writer, kwargs)
        ) or {
            "dispatch_token": "manual-42-3",
            "attempt_id": "manual-42-3",
            "claim_version": 7,
        },
    )

    assert calls[0] == ("probe", "paid_league")
    assert calls[1][0] == "claim"
    assert calls[1][3] == {
        "db_name": "paid_league",
        "platform": "espn",
        "run_id": 42,
        "run_attempt": 3,
    }
    assert result == {
        "observed_manifest_digest": "digest-123",
        "dispatch_token": "manual-42-3",
        "attempt_id": "manual-42-3",
        "claim_version": 7,
    }
    assert output.read_text(encoding="utf-8").splitlines() == [
        "observed_manifest_digest=digest-123",
        "token=manual-42-3",
        "attempt_id=manual-42-3",
        "claim_version=7",
    ]
    assert environment == {
        "LEAGUE_UPDATE_TOKEN": "manual-42-3",
        "LEAGUE_UPDATE_ATTEMPT_ID": "manual-42-3",
        "LEAGUE_UPDATE_CLAIM_VERSION": "7",
    }


def test_prepare_ui_execution_preserves_supplied_manifest_and_claim(tmp_path):
    from scripts import claim_manual_league_update as manual

    def unexpected(*args, **kwargs):
        raise AssertionError("UI execution must not probe or replace its claim")

    result = manual.prepare_update_execution(
        object(), object(), db_name="paid_league", platform="sleeper",
        execute=True, observed_manifest_digest="ui-digest",
        dispatch_token="ui-token", attempt_id="ui-attempt", claim_version=9,
        run_id=42, run_attempt=1, output_path=tmp_path / "output.txt", environment={},
        probe=unexpected, claim=unexpected,
    )

    assert result == {
        "observed_manifest_digest": "ui-digest",
        "dispatch_token": "ui-token",
        "attempt_id": "ui-attempt",
        "claim_version": 9,
    }


def test_prepare_scheduled_demo_probes_without_claiming(tmp_path):
    from scripts import claim_manual_league_update as manual

    result = manual.prepare_update_execution(
        object(), object(), db_name="demo_league", platform="yahoo",
        execute=True, scheduled_demo=True, observed_manifest_digest="",
        dispatch_token="", attempt_id="", claim_version=1,
        run_id=42, run_attempt=1, output_path=tmp_path / "output.txt", environment={},
        probe=lambda db_name: "demo-digest",
        claim=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("scheduled demo must not create a paid claim")
        ),
    )

    assert result["observed_manifest_digest"] == "demo-digest"
    assert result["dispatch_token"] == ""


def test_paid_manual_run_reclaims_a_terminal_attempt_without_erasing_its_publication(monkeypatch):
    from scripts import claim_manual_league_update as manual

    monkeypatch.setattr(manual, "assert_league_update_entitled", lambda reader, *, database_name: None)
    monkeypatch.setattr(manual, "record_league_update_status", lambda writer, **kwargs: False)

    class Writer:
        def __init__(self):
            self.sql = []

        def execute(self, sql, database="___ops"):
            assert database == "___ops"
            self.sql.append(sql)
            return [{"database_name": "the_real_ff_league", "status": "dispatching",
                     "dispatch_token": "manual-42-2", "attempt_id": "manual-42-2",
                     "workflow_run_id": 42, "claim_version": 5}]

    writer = Writer()
    claim = manual.claim_manual_attempt(
        object(), writer, db_name="the_real_ff_league", platform="sleeper",
        run_id=42, run_attempt=2,
    )
    assert claim == {"dispatch_token": "manual-42-2", "attempt_id": "manual-42-2",
                     "claim_version": 5}
    sql = writer.sql[0]
    assert "status IN ('succeeded', 'failed', 'cancelled'" in sql
    assert "status IN ('dispatching', 'dispatched', 'running')" in sql
    assert "status IN ('committed', 'cache_verified')" not in sql
    assert "claim_version = COALESCE(claim_version, 0) + 1" in sql
    assert "publication_receipt_json = NULL" not in sql
    assert "bundle_id = NULL" not in sql
    assert "publish_generation = NULL" not in sql
    assert "healthy = FALSE" not in sql
    assert "workflow_run_id = 42" in sql
    assert "lease_expires_at = NOW() + INTERVAL '20 minutes'" in sql
    assert "AND (dispatch_token IS NULL OR dispatch_token <> 'manual-42-2')" in sql


def test_unpaid_manual_run_never_attempts_a_fly_claim(monkeypatch):
    from scripts import claim_manual_league_update as manual

    def unpaid(reader, *, database_name):
        raise PermissionError("Paid update required")

    monkeypatch.setattr(manual, "assert_league_update_entitled", unpaid)

    class Writer:
        def execute(self, *args, **kwargs):
            raise AssertionError("unpaid league must not write")

    with pytest.raises(PermissionError, match="Paid update required"):
        manual.claim_manual_attempt(object(), Writer(), db_name="unpaid_league",
                                    platform="espn", run_id=42, run_attempt=1)


def test_fresh_manual_claim_rejects_lost_lease_refresh(monkeypatch):
    from scripts import claim_manual_league_update as manual

    monkeypatch.setattr(manual, "assert_league_update_entitled", lambda reader, *, database_name: None)
    monkeypatch.setattr(manual, "record_league_update_status", lambda writer, **kwargs: True)

    class Reader:
        def query(self, sql, database="___ops"):
            return [{"database_name": "paid_league", "status": "dispatching",
                     "dispatch_token": "manual-42-1", "attempt_id": "manual-42-1",
                     "workflow_run_id": 42, "claim_version": 1}]

    class Writer:
        def execute(self, sql, database="___ops"):
            # Another caller took ownership between read and lease CAS.
            return []

    with pytest.raises(RuntimeError, match="no longer owns"):
        manual.claim_manual_attempt(Reader(), Writer(), db_name="paid_league",
                                    platform="yahoo", run_id=42, run_attempt=1)


def test_same_run_attempt_cannot_reclaim_its_own_terminal_publication(monkeypatch):
    from scripts import claim_manual_league_update as manual

    monkeypatch.setattr(manual, "assert_league_update_entitled", lambda reader, *, database_name: None)
    monkeypatch.setattr(manual, "record_league_update_status", lambda writer, **kwargs: False)

    class Writer:
        def execute(self, sql, database="___ops"):
            assert "dispatch_token <> 'manual-42-1'" in sql
            # Fly CAS refuses an already completed row with this exact token.
            return []

    with pytest.raises(RuntimeError, match="no longer owns"):
        manual.claim_manual_attempt(object(), Writer(), db_name="paid_league",
                                    platform="yahoo", run_id=42, run_attempt=1)


def test_partial_manual_claim_failure_only_closes_its_exact_uncommitted_owner():
    from scripts import claim_manual_league_update as manual

    connection = duckdb.connect(":memory:")
    connection.execute("""
      CREATE SCHEMA accounts;
      CREATE TABLE accounts.league_update_dispatches (
        database_name VARCHAR, platform VARCHAR, status VARCHAR,
        dispatch_token VARCHAR, attempt_id VARCHAR, workflow_run_id BIGINT,
        claim_version BIGINT, cache_state VARCHAR, lease_expires_at TIMESTAMP,
        completed_at TIMESTAMP, updated_at TIMESTAMP, error VARCHAR
      );
      INSERT INTO accounts.league_update_dispatches VALUES
        ('paid_league', 'yahoo', 'dispatching', 'manual-42-1', 'manual-42-1',
         42, 3, 'dispatching', NOW(), NULL, NOW(), NULL);
    """)

    class Database:
        def execute(self, sql, database="___ops"):
            assert database == "___ops"
            cursor = connection.execute(sql)
            columns = [description[0] for description in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []

        query = execute

    db = Database()
    assert manual.fail_partial_manual_attempt(db, db, db_name="paid_league",
                                               platform="yahoo", run_id=42,
                                               run_attempt=1) is True
    assert connection.execute(
        "SELECT status, lease_expires_at FROM accounts.league_update_dispatches"
    ).fetchone() == ("failed", None)
    assert manual.fail_partial_manual_attempt(db, db, db_name="paid_league",
                                               platform="yahoo", run_id=42,
                                               run_attempt=1) is False
    connection.execute("""
      UPDATE accounts.league_update_dispatches SET status='committed',
      dispatch_token='manual-42-2', attempt_id='manual-42-2', claim_version=4
    """)
    assert manual.fail_partial_manual_attempt(db, db, db_name="paid_league",
                                               platform="yahoo", run_id=42,
                                               run_attempt=2) is False
    assert connection.execute("SELECT status FROM accounts.league_update_dispatches").fetchone()[0] == "committed"


def test_real_duckdb_paid_claim_terminal_rerun_and_unpaid_rejection():
    from scripts import claim_manual_league_update as manual

    connection = duckdb.connect(":memory:")
    connection.execute("""
      CREATE SCHEMA accounts;
      CREATE TABLE accounts.league_inventory (
        database_name VARCHAR, tier VARCHAR, entitled_mode VARCHAR,
        expires_at TIMESTAMP, updated_at TIMESTAMP
      );
      CREATE TABLE accounts.league_update_manifests (
        database_name VARCHAR PRIMARY KEY, observed_manifest_json VARCHAR,
        observed_manifest_digest VARCHAR, published_manifest_json VARCHAR,
        published_manifest_digest VARCHAR, published_at TIMESTAMP,
        updated_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE accounts.league_update_dispatches (
        database_name VARCHAR PRIMARY KEY, platform VARCHAR NOT NULL, status VARCHAR NOT NULL,
        workflow_file VARCHAR, workflow_run_id BIGINT, dispatch_token VARCHAR,
        source_year INTEGER, source_week INTEGER, source_fingerprint VARCHAR,
        publish_generation VARCHAR, healthy BOOLEAN DEFAULT FALSE,
        dispatched_at TIMESTAMP, started_at TIMESTAMP, completed_at TIMESTAMP,
        lease_expires_at TIMESTAMP, updated_at TIMESTAMP DEFAULT NOW(), error VARCHAR,
        attempt_id VARCHAR, claim_version BIGINT DEFAULT 0, heartbeat_at TIMESTAMP,
        observed_manifest_digest VARCHAR, base_generation VARCHAR, bundle_id VARCHAR,
        cache_state VARCHAR, committed_at TIMESTAMP, cache_verified_at TIMESTAMP,
        publication_receipt_json VARCHAR
      );
      INSERT INTO accounts.league_inventory VALUES
        ('paid_league', 'paid', 'full', NOW() + INTERVAL '1 day', NOW()),
        ('unpaid_league', 'free', 'full', NOW() + INTERVAL '1 day', NOW());
    """)

    class Database:
        def execute(self, sql, database="___ops"):
            assert database == "___ops"
            cursor = connection.execute(sql)
            columns = [description[0] for description in (cursor.description or [])]
            return [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []

        query = execute

        def query_scalar(self, sql, database="___ops"):
            return connection.execute(sql).fetchone()[0]

    database = Database()
    first = manual.claim_manual_attempt(
        database, database, db_name="paid_league", platform="yahoo",
        run_id=42, run_attempt=1,
    )
    assert first == {"dispatch_token": "manual-42-1", "attempt_id": "manual-42-1",
                     "claim_version": 1}
    row = connection.execute("""
      SELECT status, lease_expires_at IS NOT NULL, publication_receipt_json
      FROM accounts.league_update_dispatches WHERE database_name='paid_league'
    """).fetchone()
    assert row == ("dispatching", True, None)

    connection.execute("""
      UPDATE accounts.league_update_dispatches
      SET status='succeeded', publication_receipt_json='old-commit',
          bundle_id='old-bundle', source_fingerprint='old-digest',
          publish_generation='old-generation', healthy=TRUE,
          committed_at=TIMESTAMP '2026-09-16 19:26:01',
          cache_verified_at=TIMESTAMP '2026-09-16 19:26:08'
      WHERE database_name='paid_league'
    """)
    with pytest.raises(RuntimeError, match="no longer owns"):
        manual.claim_manual_attempt(database, database, db_name="paid_league",
                                    platform="yahoo", run_id=42, run_attempt=1)
    second = manual.claim_manual_attempt(
        database, database, db_name="paid_league", platform="yahoo",
        run_id=42, run_attempt=2,
    )
    assert second == {"dispatch_token": "manual-42-2", "attempt_id": "manual-42-2",
                      "claim_version": 2}
    row = connection.execute("""
      SELECT status, publication_receipt_json, bundle_id, source_fingerprint,
             publish_generation, healthy, committed_at, cache_verified_at
      FROM accounts.league_update_dispatches WHERE database_name='paid_league'
    """).fetchone()
    assert row == (
        "dispatching", "old-commit", "old-bundle", "old-digest",
        "old-generation", True,
        duckdb.execute("SELECT TIMESTAMP '2026-09-16 19:26:01'").fetchone()[0],
        duckdb.execute("SELECT TIMESTAMP '2026-09-16 19:26:08'").fetchone()[0],
    )
    connection.execute("""
      UPDATE accounts.league_update_dispatches
      SET status='running', lease_expires_at=NULL,
          heartbeat_at=NOW(), updated_at=NOW() - INTERVAL '1 hour'
      WHERE database_name='paid_league'
    """)
    with pytest.raises(RuntimeError, match="no longer owns"):
        manual.claim_manual_attempt(database, database, db_name="paid_league",
                                    platform="yahoo", run_id=42, run_attempt=3)
    connection.execute("""
      UPDATE accounts.league_update_dispatches
      SET lease_expires_at=NOW() - INTERVAL '1 hour', heartbeat_at=NOW()
      WHERE database_name='paid_league'
    """)
    with pytest.raises(RuntimeError, match="no longer owns"):
        manual.claim_manual_attempt(database, database, db_name="paid_league",
                                    platform="yahoo", run_id=42, run_attempt=3)
    connection.execute("""
      UPDATE accounts.league_update_dispatches
      SET heartbeat_at=NOW() - INTERVAL '1 hour'
      WHERE database_name='paid_league'
    """)
    third = manual.claim_manual_attempt(database, database, db_name="paid_league",
                                        platform="yahoo", run_id=42, run_attempt=3)
    assert third["claim_version"] == 3
    with pytest.raises(PermissionError):
        manual.claim_manual_attempt(database, database, db_name="unpaid_league",
                                    platform="espn", run_id=43, run_attempt=1)
