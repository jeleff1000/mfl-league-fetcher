import subprocess
import sys
from pathlib import Path

import yaml


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "refresh_live_nfl_ops.yml"
GATE = Path(__file__).resolve().parents[1] / "scripts" / "live_nfl_ops_dispatch_gate.py"


def test_live_nfl_ops_workflow_accepts_only_manual_or_dedicated_scheduler_dispatches():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    triggers = workflow[True]

    assert "workflow_dispatch" in triggers
    assert triggers["repository_dispatch"] == {"types": ["live-nfl-ops-refresh"]}
    assert "schedule" not in triggers
    assert "push" not in triggers
    assert "workflow_run" not in triggers
    assert "repository_dispatch" in workflow["run-name"]


def test_live_nfl_ops_workflow_gates_release_and_fly_on_a_ready_scope():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["refresh"]["steps"]
    steps_by_name = {step["name"]: step for step in steps}
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "discover_live_nfl_ops_refresh.py" in text
    assert "Authorize dispatched refresh in the Eastern window" in text
    assert "scripts/live_nfl_ops_dispatch_gate.py" in text
    assert "game_date=$(TZ=America/New_York date --date=yesterday +%F)" in text
    assert "___leagues" not in text
    assert "repository_dispatch" in text
    assert steps_by_name["Resolve final live refresh scope"]["if"] == (
        "${{ steps.boundary.outputs.should_run == 'true' }}"
    )
    assert steps_by_name["Reconstruct and verify the durable baseline"]["if"] == (
        "${{ steps.scope.outputs.should_refresh == 'true' }}"
    )
    assert steps_by_name["Build and verify the local eight-table candidate"]["if"] == (
        "${{ steps.scope.outputs.should_refresh == 'true' }}"
    )
    assert "steps.scope.outputs.should_refresh == 'true'" in steps_by_name[
        "Publish the verified candidate as the next durable baseline"
    ]["if"]
    assert "steps.scope.outputs.should_refresh == 'true'" in steps_by_name[
        "Atomically promote the verified ops artifact"
    ]["if"]
    assert "github.event_name == 'repository_dispatch'" in steps_by_name[
        "Atomically promote the verified ops artifact"
    ]["if"]
    assert "Verify promoted Fly receipt" in steps_by_name
    assert "scripts/verify_live_nfl_ops_receipt.py" in text
    assert "output/ops_nfl.fly-receipt.json" in text


def test_ops_dispatch_gate_runs_only_after_its_public_source_checkout():
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["refresh"]["steps"]
    names = [step["name"] for step in steps]
    assert names.index("Checkout pipeline source") < names.index(
        "Authorize dispatched refresh in the Eastern window"
    )
    checkout = steps[names.index("Checkout pipeline source")]
    assert checkout["with"]["path"] == "code"
    assert "repository" not in checkout["with"]
    gate = steps[names.index("Authorize dispatched refresh in the Eastern window")]
    assert "python code/scripts/live_nfl_ops_dispatch_gate.py" in gate["run"]


def test_dispatch_gate_admits_manual_runs_and_only_the_0130_to_0500_et_dispatch_window():
    assert GATE.is_file(), "the workflow boundary must call a testable gate command"

    def authorize(event: str, moment: str) -> str:
        completed = subprocess.run(
            [sys.executable, str(GATE), "--event", event, "--now", moment],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip()

    assert authorize("workflow_dispatch", "2026-09-14T09:00:00-04:00") == "should_run=true"
    assert authorize("repository_dispatch", "2026-09-14T01:30:00-04:00") == "should_run=true"
    assert authorize("repository_dispatch", "2026-09-14T04:59:59-04:00") == "should_run=true"
    assert authorize("repository_dispatch", "2026-09-14T01:29:59-04:00") == "should_run=false"
    assert authorize("repository_dispatch", "2026-09-14T05:00:00-04:00") == "should_run=false"


def test_durable_baseline_checksum_uses_the_reconstructed_artifact_path() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    # Release manifests retain the original output/ops_nfl.duckdb filename.
    # The verifier must compare that digest with the reconstructed baseline,
    # rather than asking sha256sum to find the old filename locally.
    assert 'expected_hash=$(awk \'{print $1}\' baseline/ops_nfl.duckdb.sha256)' in text
    assert 'actual_hash=$(sha256sum "$BASELINE_PATH" | awk \'{print $1}\')' in text
    assert "durable baseline SHA-256 mismatch" in text
    assert "(cd baseline && sha256sum --check ops_nfl.duckdb.sha256)" not in text
