"""Exercise the hosted PFRA authorization boundary without dispatching a job."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


WORKFLOW = Path(__file__).resolve().parents[4] / ".github/workflows/pfra_page_evidence.yml"


def workflow():
    assert WORKFLOW.is_file(), "hosted PFRA workflow is missing"
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def authorize(**changes):
    config = workflow()
    step = next(s for s in config["jobs"]["prepare"]["steps"] if s.get("id") == "authorization")
    env = dict(os.environ, PFRA_PUBLIC_EVIDENCE_APPROVED="true", PFRA_MANIFEST_SHA256="a" * 64,
               PFRA_SOURCE_BASE_URL="https://private.example.test/pfra", GITHUB_REF="refs/heads/main",
               GITHUB_REPOSITORY="jeleff1000/mfl-league-fetcher")
    env.update(changes)
    return subprocess.run([sys.executable, "-c", step["run"]], env=env, capture_output=True, text=True)


@pytest.mark.parametrize("value", ["", "false", "True", "1"])
def test_public_artifact_consent_is_required_before_source_delivery(value):
    result = authorize(PFRA_PUBLIC_EVIDENCE_APPROVED=value)
    assert result.returncode != 0
    assert "approval" in result.stderr.lower()
    assert "private.example.test" not in result.stdout + result.stderr


@pytest.mark.parametrize("changes", [
    {"PFRA_MANIFEST_SHA256": "../wrong"},
    {"PFRA_SOURCE_BASE_URL": ""},
    {"GITHUB_REF": "refs/heads/unreviewed"},
    {"GITHUB_REPOSITORY": "someone/fork"},
])
def test_dispatch_preflight_rejects_wrong_identity_or_missing_delivery(changes):
    assert authorize(**changes).returncode != 0


def test_configured_approved_dispatch_passes_authorization_without_network():
    assert authorize().returncode == 0


def test_workflow_has_only_manual_read_only_ten_worker_execution():
    config = workflow()
    assert set(config["on"]) == {"workflow_dispatch"}
    assert config["on"]["workflow_dispatch"]["inputs"]["public_evidence_approved"]["default"] == "false"
    assert config["permissions"] == {"contents": "read"}
    jobs = config["jobs"]
    assert set(jobs) == {"prepare", "shard", "validate"}
    assert int(jobs["shard"]["strategy"]["max-parallel"]) == 10
    assert jobs["shard"]["strategy"]["fail-fast"] == "false"
    assert jobs["shard"]["needs"] == "prepare"
    assert set(jobs["validate"]["needs"]) == {"prepare", "shard"}
    for job in jobs.values():
        assert 0 < int(job["timeout-minutes"]) <= 60
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] == "false"
                assert "repository" not in step["with"]
            if step.get("uses", "").startswith("actions/upload-artifact@"):
                assert step["with"]["retention-days"] == "1"
                assert step["with"]["if-no-files-found"] == "error"
                assert ".pdf" not in step["with"]["path"]


def test_failed_worker_retry_can_reuse_successful_shards_from_same_run():
    """Changing run_attempt must not hide the earlier successful artifacts."""
    config = workflow()
    for job in config["jobs"].values():
        for step in job["steps"]:
            if "artifact@" not in step.get("uses", ""):
                continue
            parameters = step["with"]
            name = parameters.get("name", parameters.get("pattern"))
            first = name.replace("${{ github.run_id }}", "123").replace("${{ github.run_attempt }}", "1")
            retry = name.replace("${{ github.run_id }}", "123").replace("${{ github.run_attempt }}", "2")
            assert first == retry
            if "upload-artifact@" in step["uses"]:
                assert parameters["overwrite"] == "true"
