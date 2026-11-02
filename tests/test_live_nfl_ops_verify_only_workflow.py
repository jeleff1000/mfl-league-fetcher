"""Verify-only recovery cannot reach either production writer job."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/refresh_live_nfl_ops.yml"


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def test_verify_only_is_explicit_manual_and_excludes_writer_jobs():
    workflow = _workflow()
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["verify_only"]["default"] is False
    assert inputs["source_run_id"]["default"] == ""
    assert inputs["expected_candidate_sha256"]["default"] == ""
    jobs = workflow["jobs"]
    assert "!inputs.verify_only" in jobs["refresh"]["if"]
    assert "!inputs.verify_only" in jobs["materialize-revisions-only"]["if"]
    assert jobs["verify-saved-candidate"]["if"] == (
        "${{ github.event_name == 'workflow_dispatch' && inputs.verify_only }}"
    )


def test_verify_only_has_no_writer_credentials_or_commands():
    job = _workflow()["jobs"]["verify-saved-candidate"]
    assert job["permissions"] == {"actions": "read", "contents": "read"}
    text = yaml.safe_dump(job)
    for forbidden in (
        "DATABASE_ADMIN_TOKEN", "FLY_API_TOKEN", "FLY_PRIMARY_MACHINE_ID",
        "promote_complete_artifact", "materialize_league_update_nfl_revisions.py",
        "gh release upload", "gh release create", "gh api", "refresh_live_nfl_ops.py",
    ):
        assert forbidden not in text
    assert "secrets.DATABASE_READ_TOKEN" in text
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert 'live-nfl-ops-scope-$SOURCE_RUN_ID' in commands
    assert 'test "$manifest_hash" = "$EXPECTED_CANDIDATE_SHA256"' in commands
    assert 'test "$actual_hash" = "$EXPECTED_CANDIDATE_SHA256"' in commands
    assert "--scope recovery/scope/refresh_scope.json" in commands
    assert "--candidate recovery/ops_nfl.duckdb" in commands
    assert "date --date=yesterday" not in commands
    assert "discover_live_nfl_ops_refresh" not in commands


@pytest.mark.parametrize("overrides", [
    {"APPLY": "true"},
    {"REVISIONS_ONLY": "true"},
    {"SOURCE_RUN_ID": ""},
    {"SOURCE_RUN_ID": "123; echo unsafe"},
    {"EXPECTED_CANDIDATE_SHA256": ""},
    {"EXPECTED_CANDIDATE_SHA256": "not-a-digest"},
    {"BASELINE_RELEASE_TAG": ""},
])
def test_verify_only_guard_rejects_mutation_or_unpinned_inputs(overrides):
    command = _workflow()["jobs"]["verify-saved-candidate"]["steps"][0]["run"]
    env = dict(os.environ, APPLY="false", REVISIONS_ONLY="false", SOURCE_RUN_ID="41409897127",
               EXPECTED_CANDIDATE_SHA256="a" * 64, BASELINE_RELEASE_TAG="ops-nfl-live-current")
    env.update(overrides)
    assert subprocess.run(["bash", "-c", command], env=env, capture_output=True).returncode != 0


def test_verify_only_guard_accepts_bounded_readback():
    command = _workflow()["jobs"]["verify-saved-candidate"]["steps"][0]["run"]
    env = dict(os.environ, APPLY="false", REVISIONS_ONLY="false", SOURCE_RUN_ID="41409897127",
               EXPECTED_CANDIDATE_SHA256="a" * 64, BASELINE_RELEASE_TAG="ops-nfl-live-current")
    subprocess.run(["bash", "-c", command], env=env, check=True, capture_output=True)
