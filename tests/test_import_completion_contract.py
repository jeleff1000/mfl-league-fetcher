"""Each existing worker binds cache completion to its own publication receipt."""
import importlib.util
from pathlib import Path
import re
import shlex
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_import_workflows_expose_commit_and_cache_states_separately(platform, mode):
    workflow = Path(__file__).resolve().parents[1] / ".github/workflows" / f"{platform}_{mode}_import_worker.yml"
    jobs = yaml.safe_load(workflow.read_text(encoding="utf-8"))["jobs"].values()
    job = next(job for job in jobs if any(step.get("name") == "Upload league data" for step in job["steps"]))
    steps = job["steps"]
    upload = next(step for step in steps if step.get("name") == "Upload league data")
    cache = next(step for step in steps if step.get("name") == "Revalidate and warm Vercel cache")
    for output in ("data_committed", "cache_verified", "completion_status"):
        expression = job["outputs"][output]
        step_id = re.search(r"steps\.([a-z_]+)\.outputs\.", expression)[1]
        assert any(step.get("id") == step_id for step in steps)
    assert steps.index(upload) < steps.index(cache)
    expression = cache["if"].replace("${{", "").replace("}}", "").strip()
    for committed, cancelled, expected in [(True, False, True), (False, False, False), (True, True, False)]:
        condition = expression.replace("!cancelled()", str(not cancelled)).replace("&&", "and")
        condition = condition.replace("steps.publication.outputs.data_committed", repr(str(committed).lower()))
        assert eval(condition, {"__builtins__": {}}, {}) is expected


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_worker_cache_command_reads_the_exact_uploaded_receipt(monkeypatch, tmp_path, platform, mode):
    document = yaml.safe_load((ROOT / ".github/workflows" / f"{platform}_{mode}_import_worker.yml").read_text(encoding="utf-8"))
    steps = next(job["steps"] for job in document["jobs"].values() if "cache_publish" in str(job))
    cache = next(step for step in steps if step.get("id") == "cache_publish")
    assert "data_committed" in cache["if"]
    command = cache["run"].replace("\\\n", " ")
    arguments = shlex.split(command)[2:]
    replacements = {
        "$DB_NAME": "target", "$REVALIDATION_SECRET": "test-secret",
        "${{ env.DATA_DIR }}": str(tmp_path),
        "${{ github.workspace }}/fantasy_football_data/sleeper_${{ steps.parse.outputs.database_name }}": str(tmp_path),
    }
    for index, argument in enumerate(arguments):
        for expression, value in replacements.items():
            argument = argument.replace(expression, value)
        arguments[index] = argument
    spec = importlib.util.spec_from_file_location("warmer_contract", ROOT / "scripts/warm_vercel_cache.py")
    warmer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = warmer
    spec.loader.exec_module(warmer)
    monkeypatch.setattr(sys, "argv", ["warmer", *arguments])
    args = warmer.parse_args()
    assert args.db == "target"
    assert args.manifest == str(tmp_path) + "/delta_publish_manifest.json"
    assert args.soft_warm_failures is True
    assert args.mode == mode


def test_existing_recovery_workflow_can_select_the_original_committed_run():
    document = yaml.safe_load((ROOT / ".github/workflows/refresh_league_cache.yml").read_text(encoding="utf-8"))
    trigger = document.get("on", document.get(True))
    inputs = trigger["workflow_dispatch"]["inputs"]
    assert inputs["publication_run_id"]["type"] == "string"
    job = document["jobs"]["refresh"]
    assert document["concurrency"]["group"] == "league-update-${{ inputs.database_name }}"
    assert document["concurrency"]["cancel-in-progress"] is False
    assert "DATABASE_READ_TOKEN" in job["env"]
    assert "DATABASE_ADMIN_TOKEN" in job["env"]
    step = next(step for step in job["steps"] if step["name"] == "Revalidate and warm Vercel cache")
    assert "--publication-run-id" in step["run"]
