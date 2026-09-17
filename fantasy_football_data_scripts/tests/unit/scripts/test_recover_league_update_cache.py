import json

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
