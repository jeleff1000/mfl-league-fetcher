"""Exercise the hosted PFRA authorization boundary without dispatching a job."""

import os
import email.message
import io
import json
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
import urllib.response
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
    assert set(jobs) == {"stage", "prepare", "shard", "validate"}
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


def test_worker_command_bounds_source_parallelism_to_four():
    steps = workflow()["jobs"]["shard"]["steps"]
    command = next(step["run"] for step in steps if "run-remote-shard" in step.get("run", ""))
    args = shlex.split(command.replace("\\\n", " "))
    assert "--source-workers" in args
    assert args.count("--source-workers") == 1
    assert args[args.index("--source-workers") + 1] == "4"


def staging_runner(monkeypatch, tmp_path, *, release=None, tag=None, redirect_status=None, **changes):
    jobs = workflow()["jobs"]
    assert "stage" in jobs, "staging-only job is missing"
    step = next(s for s in jobs["stage"]["steps"] if s.get("id") == "release")
    settings = dict(PFRA_PUBLIC_EVIDENCE_APPROVED="true", PFRA_MANIFEST_SHA256="a" * 64,
                    GITHUB_REF="refs/heads/main", GITHUB_REPOSITORY="jeleff1000/mfl-league-fetcher",
                    GITHUB_SHA="b" * 40, GH_TOKEN="sentinel-token", GITHUB_STEP_SUMMARY=str(tmp_path / "summary"))
    settings.update(changes)
    for key, value in settings.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("PFRA_SOURCE_BASE_URL", raising=False)
    calls = []

    def request(req, timeout):
        assert req.full_url.startswith("https://api.github.com/repos/jeleff1000/mfl-league-fetcher/")
        assert req.get_header("Authorization") == "Bearer sentinel-token"
        assert 0 < timeout <= 30
        body = json.loads(req.data) if req.data else None
        path = req.full_url.split("mfl-league-fetcher/", 1)[1]
        calls.append((req.method, path, body))
        if redirect_status:
            headers = email.message.Message()
            headers["Location"] = "https://untrusted.example/sentinel-token"
            response = urllib.response.addinfourl(io.BytesIO(b""),
                headers, req.full_url, redirect_status)
            response.msg = "Moved"
            return response
        if req.method == "GET":
            value = release if path.startswith("releases/tags/") else tag
            if value is None:
                raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {}, None)
        elif path == "git/refs":
            value = {"object": {"type": "commit", "sha": body["sha"]}}
        elif path == "releases":
            value = {**body, "id": 123, "upload_url": "https://uploads.github.com/repos/jeleff1000/mfl-league-fetcher/releases/123/assets{?name,label}"}
        else:
            pytest.fail(f"unexpected mutation: {req.method} {path}")
        response = urllib.response.addinfourl(io.BytesIO(json.dumps(value).encode()), {}, req.full_url, 200)
        response.msg = "OK"
        return response

    monkeypatch.setattr(urllib.request.HTTPSHandler, "https_open", lambda self, req: request(req, req.timeout))
    return lambda: exec(compile(step["run"], "staging-workflow", "exec"), {}), calls


def test_staging_is_opt_in_and_only_staging_has_write_permission():
    config = workflow()
    assert config["on"]["workflow_dispatch"]["inputs"]["staging_only"]["default"] == "false"
    jobs = config["jobs"]
    assert jobs["stage"]["if"] == "${{ inputs.staging_only }}"
    assert jobs["prepare"]["if"] == "${{ !inputs.staging_only }}"
    assert jobs["stage"]["permissions"] == {"contents": "write"}
    assert all("permissions" not in jobs[job] for job in ("prepare", "shard", "validate"))
    assert jobs["stage"]["steps"][0]["env"]["GH_TOKEN"] == "${{ github.token }}"


@pytest.mark.parametrize("changes", [
    {"PFRA_PUBLIC_EVIDENCE_APPROVED": "false"}, {"PFRA_PUBLIC_EVIDENCE_APPROVED": "True"},
    {"GITHUB_REF": "refs/heads/other"}, {"GITHUB_REPOSITORY": "other/repo"},
    {"PFRA_MANIFEST_SHA256": "bad"}, {"GITHUB_SHA": "main"}, {"GH_TOKEN": ""},
])
def test_staging_rejects_unauthorized_input_before_network(monkeypatch, tmp_path, changes):
    run, calls = staging_runner(monkeypatch, tmp_path, **changes)
    with pytest.raises(SystemExit):
        run()
    assert calls == []


def test_staging_creates_nonlatest_prerelease_and_exact_commit_tag(monkeypatch, tmp_path, capsys):
    run, calls = staging_runner(monkeypatch, tmp_path)
    run()
    posts = [(path, body) for method, path, body in calls if method == "POST"]
    tag = "pfra-staging-" + "a" * 24
    assert posts[0] == ("git/refs", {"ref": "refs/tags/" + tag, "sha": "b" * 40})
    assert posts[1] == ("releases", {"tag_name": tag, "target_commitish": "b" * 40, "name": tag,
                                    "body": "Temporary PFRA source staging for manifest " + "a" * 64 + ".",
                                    "draft": False, "prerelease": True, "make_latest": "false"})
    output = capsys.readouterr().out
    assert json.loads(output)["release_id"] == 123
    assert json.loads(output)["tag"] == tag
    assert "sentinel-token" not in output + (tmp_path / "summary").read_text()


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_staging_never_follows_authenticated_api_redirect(monkeypatch, tmp_path, status):
    run, calls = staging_runner(monkeypatch, tmp_path, redirect_status=status)
    with pytest.raises(SystemExit, match="^Staging API request failed\\.$") as error:
        run()
    assert len(calls) == 1
    assert "sentinel-token" not in str(error.value)
    assert not (tmp_path / "summary").exists()


@pytest.mark.parametrize("fault", [None, "commit", "tag_type", "name", "prerelease", "draft", "tag_name", "body"])
def test_staging_reuses_only_exact_existing_release(monkeypatch, tmp_path, fault):
    name = "pfra-staging-" + "a" * 24
    release = {"id": 123, "tag_name": name, "name": name, "prerelease": True, "draft": False,
               "body": "Temporary PFRA source staging for manifest " + "a" * 64 + ".",
               "upload_url": "https://uploads.github.com/repos/jeleff1000/mfl-league-fetcher/releases/123/assets{?name,label}"}
    tag = {"object": {"type": "commit", "sha": "b" * 40}}
    if fault == "commit":
        tag["object"]["sha"] = "c" * 40
    elif fault == "tag_type":
        tag["object"]["type"] = "tag"
    elif fault:
        release[fault] = False if fault == "prerelease" else True if fault == "draft" else "wrong"
    run, calls = staging_runner(monkeypatch, tmp_path, release=release, tag=tag)
    if fault:
        with pytest.raises(SystemExit):
            run()
    else:
        run()
    assert all(method == "GET" for method, _, _ in calls)
