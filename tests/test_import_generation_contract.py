from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
IMPORT_WORKFLOWS = (
    "yahoo_quick_import_worker.yml",
    "yahoo_full_import_worker.yml",
    "espn_quick_import_worker.yml",
    "espn_full_import_worker.yml",
    "sleeper_quick_import_worker.yml",
    "sleeper_full_import_worker.yml",
    "multi_platform_full_import_worker.yml",
)


@pytest.mark.parametrize("workflow_file", IMPORT_WORKFLOWS)
def test_import_capture_precedes_source_reads_and_delta_publish_requires_it(workflow_file):
    document = yaml.safe_load(
        (ROOT / ".github" / "workflows" / workflow_file).read_text(encoding="utf-8")
    )
    jobs = document["jobs"]
    sleeper = workflow_file.startswith("sleeper_")
    capture_job = jobs["resolve-db-lock"] if sleeper else next(iter(jobs.values()))
    capture_steps = capture_job["steps"]
    names = [step.get("name") for step in capture_steps]
    capture = next(step for step in capture_steps if step.get("name") == "Capture import publication generation")
    assert "python scripts/capture_import_generation.py" in capture["run"]
    assert names.index("Checkout code") < names.index("Capture import publication generation")
    source_step = "Resolve lock key" if sleeper else (
        "Parse import plan" if workflow_file.startswith("multi_platform_") else "Parse league data"
    )
    assert names.index("Capture import publication generation") < names.index(source_step)
    assert capture["env"]["IMPORT_LOCK_KEY"]

    publish_job = (
        jobs["import-sleeper-history" if "full" in workflow_file else "import-sleeper-league"]
        if sleeper else capture_job
    )
    assert str(publish_job["env"]["REQUIRE_IMPORT_BASE_GENERATION"]) == "1"
    if sleeper:
        assert "steps.snapshot.outputs.base_generation" in capture_job["outputs"]["base_generation"]
        assert "needs.resolve-db-lock.outputs.base_generation" in publish_job["env"][
            "LEAGUE_IMPORT_BASE_GENERATION"
        ]


def test_cookie_import_rebinds_generation_between_quick_and_full_publications():
    document = yaml.safe_load(
        (ROOT / ".github/workflows/yahoo_cookie_import_worker.yml").read_text(encoding="utf-8")
    )
    job = document["jobs"]["import"]
    steps = job["steps"]
    names = [step.get("name") for step in steps]
    assert str(job["env"]["REQUIRE_IMPORT_BASE_GENERATION"]) == "1"
    assert job["env"]["FLY_DELTA_FALLBACK_TO_DUCKDB"] == "0"
    assert names.index("Capture quick cookie import generation") < names.index("Run quick Yahoo cookie track")
    assert names.index("Upload quick cookie import") < names.index("Capture full cookie import generation")
    assert names.index("Capture full cookie import generation") < names.index("Run full Yahoo cookie track")
    full_capture = steps[names.index("Capture full cookie import generation")]
    assert "python scripts/capture_import_generation.py" in full_capture["run"]
    full_upload = steps[names.index("Upload full cookie import")]
    assert full_upload["env"]["LEAGUE_IMPORT_BASE_GENERATION"] == (
        "${{ steps.full_snapshot.outputs.base_generation }}"
    )


def test_manual_fleaflicker_import_is_generation_fenced_before_provider_fetch():
    document = yaml.safe_load(
        (ROOT / ".github/workflows/fleaflicker_full_import_worker.yml").read_text(encoding="utf-8")
    )
    job = document["jobs"]["import-fleaflicker"]
    names = [step.get("name") for step in job["steps"]]
    assert str(job["env"]["REQUIRE_IMPORT_BASE_GENERATION"]) == "1"
    assert document["concurrency"]["group"].startswith("league-update-")
    assert names.index("Capture Fleaflicker import generation") < names.index(
        "Restore canonical research public lake"
    )
    capture = job["steps"][names.index("Capture Fleaflicker import generation")]
    assert "python scripts/capture_import_generation.py" in capture["run"]


@pytest.mark.parametrize("platform", ("yahoo", "espn", "sleeper"))
def test_matrix_fleet_import_fences_each_league_before_source_work(platform):
    document = yaml.safe_load(
        (ROOT / f".github/workflows/{platform}_fleet_import.yml").read_text(encoding="utf-8")
    )
    job = document["jobs"]["import-league"]
    names = [step.get("name") for step in job["steps"]]
    assert str(job["env"]["REQUIRE_IMPORT_BASE_GENERATION"]) == "1"
    assert job["concurrency"]["group"] == "league-update-${{ matrix.database_name }}"
    assert names.index("Checkout code") < names.index("Capture fleet league generation")
    assert names.index("Capture fleet league generation") < names.index(
        "Restore canonical research public lake"
    )
    capture = job["steps"][names.index("Capture fleet league generation")]
    assert "python scripts/capture_import_generation.py" in capture["run"]


def test_playoff_recalculator_fences_existing_league_before_download():
    document = yaml.safe_load(
        (ROOT / ".github/workflows/playoff_odds_worker.yml").read_text(encoding="utf-8")
    )
    job = document["jobs"]["calculate-playoff-odds"]
    names = [step.get("name") for step in job["steps"]]
    assert str(job["env"]["REQUIRE_IMPORT_BASE_GENERATION"]) == "1"
    assert document["concurrency"]["group"] == "league-update-${{ inputs.database_name }}"
    assert names.index("Capture playoff recalculation generation") < names.index(
        "Download league data to local DuckDB"
    )


def test_every_executing_delta_writer_requires_a_pre_source_generation():
    for workflow_path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        document = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict) or job.get("env", {}).get("FLY_PUBLISH_FORMAT") != "delta":
                continue
            assert str(job["env"].get("REQUIRE_IMPORT_BASE_GENERATION")) == "1", (
                workflow_path.name,
                job_name,
            )


def test_every_executing_delta_writer_rejects_non_main_ref_before_checkout():
    for workflow_path in sorted((ROOT / ".github/workflows").glob("*.yml")):
        document = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        for job_name, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict) or job.get("env", {}).get("FLY_PUBLISH_FORMAT") != "delta":
                continue
            steps = job.get("steps") or []
            assert steps[0].get("name") == "Require canonical public main for publication", (
                workflow_path.name, job_name,
            )
            assert steps[0].get("env", {}).get("INPUT_WORKFLOW_REF") == "${{ github.ref }}"
            assert 'if [ "${INPUT_WORKFLOW_REF}" != "refs/heads/main" ]; then' in steps[0].get("run", "")
