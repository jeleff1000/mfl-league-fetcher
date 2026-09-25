"""An active-year publication cannot acknowledge untouched chain corrections."""
from dataclasses import replace
from importlib import import_module

import pytest
import duckdb

from multi_league.core.league_update_manifest import (
    LeagueSegment, ResourceRevision, SourceManifest, canonical_manifest_json, manifest_digest,
)
from multi_league.core.league_update_plan import PersistedRefreshPlan, build_refresh_plan
from multi_league.core.league_update_status import record_league_update_status


@pytest.fixture(params=["yahoo", "espn", "sleeper"])
def completion(request):
    module = import_module(f"scripts.refresh_{request.param}_active_season")
    return getattr(module, f"{request.param}_source_manifest_complete")


def changed_plan(scope):
    old = SourceManifest(
        schema_version=1, database_name="coverage_league", active_season=2026,
        segments=(LeagueSegment("sleeper", "s26", (2025, 2026), ((2025, "s25"), (2026, "s26"))),),
        nfl_revisions=(ResourceRevision("nfl", "game", "2026:1:A@B", "same"),),
        provider_revisions=(ResourceRevision("sleeper", "matchups", scope, "old"),),
        base_generation=4,
    )
    observed = replace(old, provider_revisions=(
        ResourceRevision("sleeper", "matchups", scope, "corrected"),
    ))
    return PersistedRefreshPlan(
        build_refresh_plan(observed, old, {(2025, 17), (2026, 1)}),
        observed, old, manifest_digest(observed), manifest_digest(old),
    )


COMPLETE_FETCH = {"draft_validated": True, "pending_nfl_teams": [], "final_matchup_weeks": 1}


def test_completed_active_fetch_keeps_older_correction_pending(completion):
    assert completion(refresh_weeks=[1], fetch_rows=COMPLETE_FETCH,
                      plan=changed_plan("2025:17"), year=2026) is False


def test_complete_active_only_plan_can_advance_freshness(completion):
    assert completion(refresh_weeks=[1], fetch_rows=COMPLETE_FETCH,
                      plan=changed_plan("2026:1"), year=2026) is True


def test_unfetched_active_week_cannot_advance_freshness(completion):
    assert completion(refresh_weeks=[1], fetch_rows=COMPLETE_FETCH,
                      plan=changed_plan("2026:2"), year=2026) is False


def test_missing_plan_cannot_claim_complete_chain(completion):
    assert completion(refresh_weeks=[1], fetch_rows=COMPLETE_FETCH) is False


def test_pending_game_remains_partial_despite_complete_plan(completion):
    assert completion(refresh_weeks=[1], fetch_rows={**COMPLETE_FETCH, "pending_nfl_teams": ["KC"]},
                      plan=changed_plan("2026:1"), year=2026) is False


def test_wrong_publication_year_cannot_advance_freshness(completion):
    assert completion(refresh_weeks=[1], fetch_rows=COMPLETE_FETCH,
                      plan=changed_plan("2026:1"), year=2025) is False


def test_committed_active_update_and_cache_keep_historical_watermark_pending(completion):
    plan = changed_plan("2025:17")
    with duckdb.connect(":memory:") as conn:
        conn.execute("""
            CREATE SCHEMA accounts;
            CREATE TABLE accounts.league_update_manifests (
              database_name VARCHAR PRIMARY KEY,
              platform VARCHAR, active_season INTEGER, through_week INTEGER,
              observed_manifest_json VARCHAR, observed_manifest_digest VARCHAR,
              published_manifest_json VARCHAR, published_manifest_digest VARCHAR,
              published_at TIMESTAMP,
              probe_status VARCHAR NOT NULL DEFAULT 'unknown', probe_error_code VARCHAR,
              last_attempt_at TIMESTAMP, last_success_at TIMESTAMP,
              updated_at TIMESTAMP NOT NULL DEFAULT NOW()
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
            )
        """)

        class LocalWriter:
            def execute(self, sql, *, database):
                assert database == "___ops"
                return conn.execute(sql)

        writer = LocalWriter()
        owner = dict(database_name="coverage_league", platform="sleeper",
                     dispatch_token="owner", workflow_run_id=42)
        assert record_league_update_status(writer, **owner, status="running")
        conn.execute(
            "INSERT INTO accounts.league_update_manifests "
            "(database_name,observed_manifest_json,observed_manifest_digest,"
            "published_manifest_json,published_manifest_digest) VALUES (?,?,?,?,?)",
            ["coverage_league", plan.observed_manifest_json, plan.observed_manifest_digest,
             canonical_manifest_json(plan.published_manifest), plan.published_manifest_digest],
        )
        receipt = {
            "status": "COMMITTED", "source_year": 2026, "source_week": 1,
            "source_manifest_json": plan.observed_manifest_json,
            "source_manifest_digest": plan.observed_manifest_digest,
            "bundle_id": "actual-active-commit",
            "source_manifest_complete": completion(
                refresh_weeks=[1], fetch_rows=COMPLETE_FETCH, plan=plan, year=2026,
            ),
        }
        for status in ("committed", "cache_verified", "succeeded"):
            assert record_league_update_status(
                writer, **owner, status=status, receipt=receipt, cache_verified=True,
            )
        assert conn.execute(
            "SELECT published_manifest_digest FROM accounts.league_update_manifests "
            "WHERE database_name='coverage_league'"
        ).fetchone() == (plan.published_manifest_digest,)
        assert conn.execute(
            "SELECT status,bundle_id FROM accounts.league_update_dispatches "
            "WHERE database_name='coverage_league'"
        ).fetchone() == ("succeeded", "actual-active-commit")
