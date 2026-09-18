import json

import duckdb
import pytest

from scripts import recover_league_update_cache as recovery


ROW = {
    "database_name": "the_league", "platform": "yahoo",
    "status": "committed_cache_pending",
    "dispatch_token": "original", "attempt_id": "attempt",
    "claim_version": 4, "workflow_run_id": 42,
    "source_year": 2026, "source_week": 1,
    "source_fingerprint": "digest", "bundle_id": "bundle",
    "base_generation": "3", "published_manifest_digest": "digest",
    "publication_receipt_json": json.dumps({
        "status": "COMMITTED", "executed": True, "source_year": 2026,
        "source_week": 1, "source_manifest_digest": "digest",
        "source_manifest_json": '{"schema_version":1}',
        "source_manifest_complete": True,
        "bundle_id": "bundle", "base_generation": 3,
    }),
}


def _reconcile_args(path):
    return ["--db", "the_league", "--platform", "yahoo", "--receipt", str(path),
            "--dispatch-token", "manual-42-1", "--attempt-id", "manual-42-1", "--claim-version", "3"]


@pytest.mark.parametrize("generations,expected_success", [([4, 4], True), ([4, 5], False)])
def test_cache_recovery_never_refetches_or_republishes_and_rechecks_generation(
    monkeypatch, generations, expected_success
):
    class Reader:
        def __init__(self):
            self.generations = iter(generations)

        def query(self, sql, *, database):
            assert database == "___ops"
            assert "d.status IN ('committed', 'committed_cache_pending')" in sql
            assert "LEFT JOIN accounts.league_update_manifests" in sql
            assert "original" in sql
            return [{**ROW, "status": "committed"}]

        def query_scalar(self, sql, *, database):
            assert database == "___leagues"
            assert "merge_admin.league_publish_generations" in sql
            return next(self.generations)

    warmed = []
    completed = []
    monkeypatch.setattr(recovery, "FlyReader", Reader)
    monkeypatch.setattr(recovery, "FlyWriter", lambda: object())
    monkeypatch.setattr(recovery, "assert_league_update_entitled", lambda reader, database_name: None)
    monkeypatch.setattr(recovery.subprocess, "run", lambda command, check: warmed.append(command))
    monkeypatch.setattr(recovery, "record_league_update_status", lambda writer, **kwargs: completed.append(kwargs) or True)
    monkeypatch.setenv("REVALIDATION_SECRET", "test-secret")

    argv = [
        "--db", "the_league", "--platform", "yahoo",
        "--dispatch-token", "original", "--attempt-id", "attempt",
        "--claim-version", "4",
    ]
    if expected_success:
        assert recovery.main(argv) == 0
        assert len(completed) == 1
        assert completed[0]["dispatch_token"] == "original"
        assert completed[0]["workflow_run_id"] == 42
        assert completed[0]["status"] == "succeeded"
        assert completed[0]["cache_verified"] is True
    else:
        with pytest.raises(ValueError, match="newer publication"):
            recovery.main(argv)
        assert completed == []
    assert len(warmed) == 1
    assert any("warm_vercel_cache.py" in str(value) for value in warmed[0])
    assert "--strict" in warmed[0]
    assert "--verify-hot" in warmed[0]
    assert "--required-only" in warmed[0]
    assert warmed[0][warmed[0].index("--timeout") + 1] == "5"
    assert warmed[0][warmed[0].index("--warm-attempts") + 1] == "1"
    assert warmed[0][warmed[0].index("--hot-verify-attempts") + 1] == "1"


def test_blank_token_recovers_only_an_exact_committed_manual_attempt(monkeypatch):
    manual = {**ROW, "dispatch_token": "manual-42-1", "attempt_id": "manual-42-1"}

    class Reader:
        def query(self, sql, *, database):
            assert database == "___ops"
            assert "manual-%" in sql
            assert "LEFT JOIN accounts.league_update_manifests" in sql
            return [manual]

        def query_scalar(self, sql, *, database):
            return 4

    settled = []
    monkeypatch.setattr(recovery, "FlyReader", Reader)
    monkeypatch.setattr(recovery, "FlyWriter", lambda: object())
    monkeypatch.setattr(recovery, "assert_league_update_entitled", lambda reader, database_name: None)
    monkeypatch.setattr(recovery.subprocess, "run", lambda command, check: None)
    monkeypatch.setattr(recovery, "record_league_update_status", lambda writer, **kwargs: settled.append(kwargs) or True)
    monkeypatch.setenv("REVALIDATION_SECRET", "test-secret")

    assert recovery.main(["--db", "the_league", "--platform", "yahoo"]) == 0
    assert settled[0]["dispatch_token"] == "manual-42-1"
    assert settled[0]["claim_version"] == 4
    assert settled[0]["receipt"]["bundle_id"] == "bundle"
    manual["dispatch_token"] = "ui-claim"
    with pytest.raises(RuntimeError, match="unavailable"):
        recovery.main(["--db", "the_league", "--platform", "yahoo"])


@pytest.fixture
def ambiguous_commit(tmp_path, monkeypatch):
    from multi_league.core.league_update_manifest import manifest_digest, source_manifest_from_mapping

    connection = duckdb.connect(":memory:")

    class Database:
        def execute(self, sql, database="___ops"):
            assert database in {"___ops", "___leagues"}
            cursor = connection.execute(sql)
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, values)) for values in cursor.fetchall()]

        query = execute

        def query_scalar(self, sql, database="___ops"):
            return self.query(sql, database=database)[0].popitem()[1]

    database = Database()
    source = json.dumps({
        "schema_version": 1, "database_name": "the_league", "active_season": 2026,
        "segments": [], "nfl_revisions": [], "provider_revisions": [],
    })
    digest = manifest_digest(source_manifest_from_mapping(json.loads(source)))
    claim = dict(database_name="the_league", platform="yahoo", dispatch_token="manual-42-1",
                 attempt_id="manual-42-1", claim_version=3, workflow_run_id=42)
    assert recovery.record_league_update_status(database, status="running", **claim)
    connection.execute("""
        CREATE TABLE accounts.league_inventory (
            database_name VARCHAR, tier VARCHAR, entitled_mode VARCHAR,
            expires_at TIMESTAMP, updated_at TIMESTAMP
        );
        INSERT INTO accounts.league_inventory VALUES
            ('the_league', 'paid', 'full', NOW() + INTERVAL '1 day', NOW());
        CREATE SCHEMA merge_admin;
        CREATE TABLE merge_admin.league_publish_generations (
            db_name VARCHAR, generation BIGINT, lane VARCHAR, run_id VARCHAR
        );
        INSERT INTO merge_admin.league_publish_generations VALUES ('the_league', 3, 'fleet', '42');
        CREATE TABLE merge_admin.league_delta_merge_state (
            db_name VARCHAR, bundle_id VARCHAR, bundle_hash VARCHAR, status VARCHAR,
            import_run_id VARCHAR, manifest_json VARCHAR, result_json VARCHAR
        );
    """)
    # A subsequent probe must not replace the source captured by the committed run.
    connection.execute("""
        INSERT INTO accounts.league_update_manifests
            (database_name, observed_manifest_json, observed_manifest_digest)
        VALUES ('the_league', '{}', 'newer-observation')
    """)
    bundle_hash = "a" * 64
    bundle_id = f"fleet-{bundle_hash}"
    manifest = {"db_name": "___fleet", "bundle_id": bundle_id, "bundle_hash": bundle_hash,
                "import_run_id": "42", "active_year": 2026, "db_names": ["the_league"],
                "league_generations": {"the_league": 2}, "schema_version": "fleet-partition-v3"}
    result = {"status": "COMMITTED", "bundle_id": bundle_id, "bundle_hash": bundle_hash,
              "db_name": "___fleet"}
    connection.execute(
        "INSERT INTO merge_admin.league_delta_merge_state VALUES ('___fleet', ?, ?, 'COMMITTED', '42', ?, ?)",
        [bundle_id, bundle_hash, json.dumps(manifest), json.dumps(result)],
    )
    receipt = {"status": "COMMITTED", "executed": True, "db_name": "the_league",
               "source_year": 2026, "source_week": 1, "source_manifest_digest": digest,
               "source_manifest_json": source, "source_manifest_complete": True,
               "bundle_id": bundle_id, "base_generation": 2}
    path = tmp_path / "original_receipt.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    warmed = []

    def warm(command, *, check):
        # The original claim must be recoverable before the external warm starts.
        state = connection.execute("""
            SELECT status, workflow_run_id, claim_version, bundle_id
            FROM accounts.league_update_dispatches WHERE database_name='the_league'
        """).fetchone()
        assert state == ("committed_cache_pending", 42, 3, bundle_id)
        warmed.append(command)

    monkeypatch.setattr(recovery, "FlyReader", lambda: database)
    monkeypatch.setattr(recovery, "FlyWriter", lambda: database)
    monkeypatch.setattr(recovery.subprocess, "run", warm)
    monkeypatch.setenv("REVALIDATION_SECRET", "test-secret")
    yield connection, path, receipt, warmed
    connection.close()


def test_reconcile_original_receipt_then_warm_without_republishing(ambiguous_commit):
    connection, path, receipt, warmed = ambiguous_commit
    assert recovery.main(_reconcile_args(path)) == 0
    assert len(warmed) == 1
    assert connection.execute("""
        SELECT status, workflow_run_id, claim_version, committed_at IS NOT NULL,
               cache_verified_at IS NOT NULL
        FROM accounts.league_update_dispatches WHERE database_name='the_league'
    """).fetchone() == ("succeeded", 42, 3, True, True)
    assert connection.execute("""
        SELECT observed_manifest_digest, published_manifest_digest
        FROM accounts.league_update_manifests WHERE database_name='the_league'
    """).fetchone() == ("newer-observation", receipt["source_manifest_digest"])
    assert connection.execute("SELECT generation FROM merge_admin.league_publish_generations").fetchone() == (3,)
    assert connection.execute("SELECT COUNT(*) FROM merge_admin.league_delta_merge_state").fetchone() == (1,)


@pytest.mark.parametrize("mutation", [
    "UPDATE merge_admin.league_delta_merge_state SET status='STAGED'",
    "UPDATE merge_admin.league_delta_merge_state SET bundle_hash='wrong'",
    "UPDATE merge_admin.league_delta_merge_state SET import_run_id='41'",
    "UPDATE merge_admin.league_delta_merge_state SET manifest_json='{}'",
    "UPDATE merge_admin.league_delta_merge_state SET result_json='{}'",
    "INSERT INTO merge_admin.league_delta_merge_state SELECT * FROM merge_admin.league_delta_merge_state",
    "UPDATE merge_admin.league_publish_generations SET generation=4",
    "UPDATE merge_admin.league_publish_generations SET run_id='41'",
    "UPDATE accounts.league_update_dispatches SET workflow_run_id=43",
    "UPDATE accounts.league_update_dispatches SET claim_version=4",
    "UPDATE accounts.league_update_dispatches SET dispatch_token='manual-42-2', attempt_id='manual-42-2'",
])
def test_reconciliation_rejects_mismatched_commit_before_status_or_warm(ambiguous_commit, mutation):
    connection, path, receipt, warmed = ambiguous_commit
    connection.execute(mutation)
    with pytest.raises((ValueError, RuntimeError)):
        recovery.main(_reconcile_args(path))
    assert warmed == []
    assert connection.execute("""
        SELECT status, publication_receipt_json FROM accounts.league_update_dispatches
        WHERE database_name='the_league'
    """).fetchone() == ("running", None)


def test_reconciliation_rejects_changed_source_payload(ambiguous_commit):
    connection, path, receipt, warmed = ambiguous_commit
    receipt["source_manifest_digest"] = "wrong"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(ValueError, match="digest"):
        recovery.main(_reconcile_args(path))
    assert warmed == []
    assert connection.execute("""
        SELECT status FROM accounts.league_update_dispatches WHERE database_name='the_league'
    """).fetchone() == ("running",)


def test_reconciliation_requires_explicit_original_claim(ambiguous_commit):
    connection, path, receipt, warmed = ambiguous_commit
    with pytest.raises(ValueError, match="original claim"):
        recovery.main(["--db", "the_league", "--platform", "yahoo", "--receipt", str(path)])
    assert warmed == []


def test_reconciliation_lost_claim_never_warms(ambiguous_commit, monkeypatch):
    connection, path, receipt, warmed = ambiguous_commit
    real_record = recovery.record_league_update_status

    def lose_claim(writer, **kwargs):
        connection.execute("UPDATE accounts.league_update_dispatches SET claim_version=4")
        return real_record(writer, **kwargs)

    monkeypatch.setattr(recovery, "record_league_update_status", lose_claim)
    with pytest.raises(RuntimeError, match="lost its publication claim"):
        recovery.main(_reconcile_args(path))
    assert warmed == []
    assert connection.execute("""
        SELECT status, publication_receipt_json FROM accounts.league_update_dispatches
        WHERE database_name='the_league'
    """).fetchone() == ("running", None)


def test_failed_reconciled_warm_resumes_existing_cache_path(ambiguous_commit, monkeypatch):
    connection, path, receipt, warmed = ambiguous_commit
    original_warm = recovery.subprocess.run

    def fail_warm(command, *, check):
        original_warm(command, check=check)
        raise RuntimeError("cache unavailable")

    monkeypatch.setattr(recovery.subprocess, "run", fail_warm)
    with pytest.raises(RuntimeError, match="cache unavailable"):
        recovery.main(_reconcile_args(path))
    committed_at = connection.execute("""
        SELECT committed_at FROM accounts.league_update_dispatches
        WHERE database_name='the_league' AND status='committed_cache_pending' AND cache_verified_at IS NULL
    """).fetchone()[0]
    assert committed_at is not None
    monkeypatch.setattr(recovery.subprocess, "run", original_warm)
    assert recovery.main(["--db", "the_league", "--platform", "yahoo"]) == 0
    assert connection.execute("""
        SELECT committed_at, status, workflow_run_id FROM accounts.league_update_dispatches
        WHERE database_name='the_league'
    """).fetchone() == (committed_at, "succeeded", 42)
    assert len(warmed) == 2
    assert connection.execute("SELECT generation FROM merge_admin.league_publish_generations").fetchone() == (3,)


def test_generation_change_during_reconciled_warm_cannot_mark_success(ambiguous_commit, monkeypatch):
    connection, path, receipt, warmed = ambiguous_commit
    original_warm = recovery.subprocess.run

    def newer_generation(command, *, check):
        original_warm(command, check=check)
        connection.execute("UPDATE merge_admin.league_publish_generations SET generation=4")

    monkeypatch.setattr(recovery.subprocess, "run", newer_generation)
    with pytest.raises(ValueError, match="newer publication"):
        recovery.main(_reconcile_args(path))
    assert connection.execute("""
        SELECT status, cache_verified_at FROM accounts.league_update_dispatches
        WHERE database_name='the_league'
    """).fetchone() == ("committed_cache_pending", None)
