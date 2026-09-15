from pathlib import Path

import subprocess
import sys

import pytest

import yaml


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("lock,target,expected", [
    ("kmffl", "kmffl", 0),
    ("other", "kmffl", 1),
    ("", "kmffl", 1),
    ("kmffl", "", 1),
])
def test_import_lock_validator_fails_closed(lock, target, expected):
    result = subprocess.run(
        [sys.executable, str(ROOT / ".github" / "scripts" / "verify_import_lock.py"),
         "--lock", lock, "--target", target],
        cwd=ROOT, capture_output=True, text=True,
    )
    assert result.returncode == expected
    assert "kmffl" not in result.stderr


@pytest.mark.parametrize("name", [
    "espn_quick_import_worker.yml", "espn_full_import_worker.yml",
    "sleeper_quick_import_worker.yml", "sleeper_full_import_worker.yml",
    "yahoo_quick_import_worker.yml", "yahoo_full_import_worker.yml",
    "multi_platform_full_import_worker.yml",
])
def test_public_import_worker_checks_lock_against_resolved_publication_target(name):
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
    check_job_name, check_job = next(
        (job_name, job) for job_name, job in workflow["jobs"].items()
        if any(step.get("name") == "Verify resolved import lock" for step in job.get("steps", []))
    )
    steps = check_job["steps"]
    check_indices = [i for i, step in enumerate(steps) if step.get("name") == "Verify resolved import lock"]
    assert len(check_indices) == 1
    index = check_indices[0]
    check = steps[index]
    assert "verify_import_lock.py" in check["run"]
    assert "IMPORT_LOCK_KEY" in check["env"]
    assert "RESOLVED_TARGET_DB" in check["env"]
    writes = [i for i, step in enumerate(steps)
              if "upload_to_fly" in step.get("run", "") or step.get("name") == "Run multi-platform import"]
    if writes:
        assert index < min(writes)
    for job_name, job in workflow["jobs"].items():
        if job_name != check_job_name and any("upload_to_fly" in step.get("run", "")
                                               for step in job.get("steps", [])):
            assert check_job_name in str(job.get("needs", ""))


def test_manual_sleeper_reimport_is_fly_only_and_passes_resolved_league_lock():
    text = (ROOT / "scripts" / "reimport_league.py").read_text(encoding="utf-8")
    assert 'os.environ["DATABASE_BACKEND"] = "fly"' in text
    assert '"import_lock_key": league["db_name"]' in text
    assert "MOTHERDUCK_TOKEN" not in text
    assert "MotherDuck" not in text


def test_admin_merge_and_single_league_draft_share_active_league_lock():
    merge = (ROOT / ".github" / "workflows" / "league_admin_merge_worker.yml").read_text(encoding="utf-8")
    assert "group: league-update-${{ github.event.inputs.import_lock_key || github.event.client_payload.import_lock_key }}" in merge
    assert "queue: max" in merge
    assert "Validate target league lock" in merge
    draft = (ROOT / ".github" / "workflows" / "offseason_draft_update_worker.yml").read_text(encoding="utf-8")
    assert "group: league-update-${{ inputs.db_names" in draft
    assert "queue: max" in draft
