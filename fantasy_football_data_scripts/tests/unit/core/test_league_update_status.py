import json

import pandas as pd
import pytest
import duckdb

from multi_league.core.league_refresh import finalized_source_boundary
from multi_league.core.league_update_manifest import manifest_digest, source_manifest_from_mapping
from multi_league.core.league_update_status import assert_league_update_entitled, record_league_update_status


def test_finalized_source_boundary_is_deterministic_and_game_scoped():
    rows = pd.DataFrame([
        {"week": 2, "nfl_team": "KC", "opponent_nfl_team": "LV", "NFL_player_id": "p1", "source_revision": "11"},
        {"week": 1, "nfl_team": "MIA", "opponent_nfl_team": "BUF", "NFL_player_id": "p2", "source_revision": "22"},
        {"week": 2, "nfl_team": "LV", "opponent_nfl_team": "KC", "NFL_player_id": "p3", "source_revision": "33"},
    ])
    first = finalized_source_boundary(rows, year=2026)
    second = finalized_source_boundary(rows.iloc[::-1], year=2026)
    assert first == second
    assert first["source_year"] == 2026
    assert first["source_week"] == 2
    assert first["source_game_count"] == 2
    assert first["source_fingerprint"].startswith("2026:2:")


def test_same_week_new_finalized_game_changes_fingerprint():
    one = pd.DataFrame([{"week": 2, "nfl_team": "KC", "opponent_nfl_team": "LV", "NFL_player_id": "p1", "source_revision": "11"}])
    two = pd.concat([
        one,
        pd.DataFrame([{"week": 2, "nfl_team": "BUF", "opponent_nfl_team": "MIA", "NFL_player_id": "p2", "source_revision": "22"}]),
    ], ignore_index=True)
    assert finalized_source_boundary(one, year=2026)["source_fingerprint"] != \
        finalized_source_boundary(two, year=2026)["source_fingerprint"]


def test_same_player_stat_revision_changes_midweek_fingerprint():
    before = pd.DataFrame([{
        "week": 2, "nfl_team": "KC", "opponent_nfl_team": "LV",
        "NFL_player_id": "p1", "source_revision": "11",
    }])
    after = before.assign(source_revision="12")

    assert finalized_source_boundary(before, year=2026)["source_fingerprint"] != \
        finalized_source_boundary(after, year=2026)["source_fingerprint"]


class Writer:
    def __init__(self):
        self.sql = ""

    def execute(self, sql, *, database):
        self.sql = sql
        assert database == "___ops"
        return [("the_league",)]


def test_entitlement_uses_paid_fly_rows_and_only_explicit_grandfathers():
    class Reader:
        def __init__(self, value):
            self.value = value
            self.calls = 0

        def query_scalar(self, sql, *, database):
            self.calls += 1
            assert "tier, '')) = 'paid'" in sql
            assert "tier, '')) = 'grandfathered'" in sql
            assert database == "___ops"
            return self.value

    broad_legacy = Reader(0)
    with pytest.raises(PermissionError):
        assert_league_update_entitled(broad_legacy, database_name="nyu_ffl")
    explicit = Reader(0)
    assert_league_update_entitled(explicit, database_name="tfl_of_extraordinary_gentleman")
    assert explicit.calls == 0


def test_success_requires_committed_receipt_and_cache_verification():
    writer = Writer()
    with pytest.raises(ValueError, match="COMMITTED"):
        record_league_update_status(
            writer,
            database_name="the_league",
            platform="yahoo",
            status="succeeded",
            dispatch_token="opaque",
            receipt={"status": "DRY_RUN_READY"},
            cache_verified=True,
        )
    with pytest.raises(ValueError, match="cache verification"):
        record_league_update_status(
            writer,
            database_name="the_league",
            platform="yahoo",
            status="succeeded",
            dispatch_token="opaque",
            receipt={"status": "COMMITTED", "source_year": 2026, "source_week": 2,
                     "source_fingerprint": "fp", "bundle_id": "bundle"},
            cache_verified=False,
        )


def test_verified_success_advances_fingerprint_with_token_guard():
    writer = Writer()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="succeeded",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt={"status": "COMMITTED", "source_year": 2026, "source_week": 2,
                 "source_fingerprint": "fp", "bundle_id": "bundle"},
        cache_verified=True,
    )
    assert "observed_manifest_digest" in writer.sql
    assert "dispatch_token = 'opaque'" in writer.sql
    assert "claim_version" in writer.sql
    assert "status IN (" in writer.sql
    assert "cache_state = 'succeeded', healthy = TRUE" in writer.sql
    assert "TRUE" in writer.sql


def test_manifest_aware_commit_promotes_only_the_exact_observed_snapshot():
    writer = Writer()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="committed",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt={
            "status": "COMMITTED",
            "source_year": 2026,
            "source_week": 2,
            "source_manifest_digest": "manifest-digest",
            "bundle_id": "bundle",
        },
    )
    assert "published_manifest_json = observed_manifest_json" in writer.sql
    assert "published_manifest_digest = 'manifest-digest'" in writer.sql
    assert "observed_manifest_digest = 'manifest-digest'" in writer.sql
    assert "published_at = NOW()" in writer.sql


def test_running_claim_has_a_short_crash_recovery_lease():
    writer = Writer()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
    )
    assert "INTERVAL '20 minutes'" in writer.sql
    assert "135 minutes" not in writer.sql


class LocalWriter:
    def __init__(self):
        self.connection = duckdb.connect(":memory:")

    def execute(self, sql, *, database):
        assert database == "___ops"
        return self.connection.execute(sql)


def _seed_observed_manifest(writer: LocalWriter, *, digest: str = "manifest-digest") -> None:
    writer.connection.execute(
        "INSERT INTO accounts.league_update_manifests "
        "(database_name, observed_manifest_json, observed_manifest_digest) "
        "VALUES ('the_league', '{\"schema_version\":1}', ?)",
        [digest],
    )


def _committed_receipt():
    return {
        "status": "COMMITTED",
        "source_year": 2026,
        "source_week": 2,
        "source_fingerprint": "manifest-digest",
        "bundle_id": "bundle-1",
    }


def test_committed_publication_promotes_matching_observed_manifest():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
        workflow_run_id=42,
    )
    _seed_observed_manifest(writer)
    receipt = _committed_receipt() | {"source_manifest_digest": "manifest-digest"}

    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="committed",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=receipt,
    )
    assert writer.connection.execute(
        "SELECT published_manifest_json, published_manifest_digest "
        "FROM accounts.league_update_manifests WHERE database_name = 'the_league'"
    ).fetchone() == ('{"schema_version":1}', "manifest-digest")


def test_partial_espn_score_publication_does_not_mark_observed_manifest_current():
    """Sunday's safe rows may commit while Monday's Chiefs game remains live."""
    writer = LocalWriter()
    assert record_league_update_status(
        writer, database_name="the_league", platform="espn", status="running",
        dispatch_token="opaque", workflow_run_id=42,
    )
    _seed_observed_manifest(writer)
    receipt = _committed_receipt() | {
        "source_manifest_digest": "manifest-digest",
        "source_manifest_complete": False,
        "pending_nfl_teams": ["DEN", "KC"],
    }

    assert record_league_update_status(
        writer, database_name="the_league", platform="espn", status="committed",
        dispatch_token="opaque", workflow_run_id=42, receipt=receipt,
    )
    assert writer.connection.execute(
        "SELECT published_manifest_digest FROM accounts.league_update_manifests "
        "WHERE database_name = 'the_league'"
    ).fetchone() == (None,)
    assert writer.connection.execute(
        "SELECT status, publish_generation FROM accounts.league_update_dispatches "
        "WHERE database_name = 'the_league'"
    ).fetchone() == ("committed", "bundle-1")


def test_commit_rejects_manifest_that_changed_after_dispatch():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
        workflow_run_id=42,
    )
    _seed_observed_manifest(writer, digest="newer-probe")
    receipt = _committed_receipt() | {"source_manifest_digest": "dispatched-probe"}

    assert not record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="committed",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=receipt,
    )
    assert writer.connection.execute(
        "SELECT status FROM accounts.league_update_dispatches WHERE database_name = 'the_league'"
    ).fetchone()[0] == "running"
    assert writer.connection.execute(
        "SELECT published_manifest_digest FROM accounts.league_update_manifests "
        "WHERE database_name = 'the_league'"
    ).fetchone()[0] is None


def test_captured_manifest_can_publish_while_a_newer_probe_remains_observed():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
        workflow_run_id=42,
    )
    _seed_observed_manifest(writer, digest="newer-probe")
    source_json = (
        '{"active_season":2026,"database_name":"the_league",'
        '"nfl_revisions":[],"provider_revisions":[],"schema_version":1,'
        '"segments":[]}'
    )
    source_digest = manifest_digest(source_manifest_from_mapping(json.loads(source_json)))
    receipt = _committed_receipt() | {
        "source_manifest_digest": source_digest,
        "source_manifest_json": source_json,
    }
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="committed",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=receipt,
    )
    assert writer.connection.execute(
        "SELECT observed_manifest_digest, published_manifest_digest "
        "FROM accounts.league_update_manifests WHERE database_name = 'the_league'"
    ).fetchone() == ("newer-probe", source_digest)


def test_captured_manifest_digest_must_match_its_payload():
    writer = Writer()
    with pytest.raises(ValueError, match="digest does not match"):
        record_league_update_status(
            writer,
            database_name="the_league",
            platform="yahoo",
            status="committed",
            dispatch_token="opaque",
            receipt=_committed_receipt() | {
                "source_manifest_digest": "wrong",
                "source_manifest_json": (
                    '{"active_season":2026,"database_name":"the_league",'
                    '"nfl_revisions":[],"provider_revisions":[],"schema_version":1,'
                    '"segments":[]}'
                ),
            },
        )


def test_terminal_attempt_cannot_be_rewritten_by_same_token():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="succeeded",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=_committed_receipt(),
        cache_verified=True,
    )

    assert not record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="failed",
        dispatch_token="opaque",
        workflow_run_id=42,
        error="late callback",
    )
    status = writer.connection.execute(
        "SELECT status FROM accounts.league_update_dispatches WHERE database_name = 'the_league'"
    ).fetchone()[0]
    assert status == "succeeded"

    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="succeeded",
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=_committed_receipt(),
        cache_verified=True,
        error="must not replace the terminal receipt",
    )
    persisted_error = writer.connection.execute(
        "SELECT error FROM accounts.league_update_dispatches WHERE database_name = 'the_league'"
    ).fetchone()[0]
    assert persisted_error is None


def test_workflow_run_cannot_take_over_an_existing_attempt():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
        workflow_run_id=42,
    )

    assert not record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="failed",
        dispatch_token="opaque",
        workflow_run_id=43,
        error="wrong owner",
    )


def test_attempt_and_claim_version_are_both_compare_and_swap_guards():
    writer = LocalWriter()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="running",
        dispatch_token="opaque",
        attempt_id="attempt-2",
        claim_version=2,
        workflow_run_id=42,
    )
    assert not record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="failed",
        dispatch_token="opaque",
        attempt_id="old-attempt",
        claim_version=2,
        workflow_run_id=42,
    )
    assert not record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status="failed",
        dispatch_token="opaque",
        attempt_id="attempt-2",
        claim_version=1,
        workflow_run_id=42,
    )


def test_committed_cache_retry_can_finish_without_republishing():
    writer = LocalWriter()
    common = {
        "database_name": "the_league",
        "platform": "yahoo",
        "dispatch_token": "opaque",
        "attempt_id": "attempt-1",
        "claim_version": 1,
        "workflow_run_id": 42,
        "receipt": _committed_receipt(),
    }
    assert record_league_update_status(writer, status="committed", **common)
    assert record_league_update_status(writer, status="committed_cache_pending", **common)
    assert record_league_update_status(
        writer,
        status="cache_verified",
        cache_verified=True,
        **common,
    )
    assert record_league_update_status(
        writer,
        status="succeeded",
        cache_verified=True,
        **common,
    )


@pytest.mark.parametrize(
    "status",
    ["committed", "cache_verified", "committed_cache_pending", "cancelled"],
)
def test_attempt_lifecycle_accepts_recovery_and_cancellation_states(status):
    writer = Writer()
    assert record_league_update_status(
        writer,
        database_name="the_league",
        platform="yahoo",
        status=status,
        dispatch_token="opaque",
        workflow_run_id=42,
        receipt=_committed_receipt() if status in {"committed", "cache_verified", "committed_cache_pending"} else None,
        cache_verified=status == "cache_verified",
    )
