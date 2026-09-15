from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_promoted_ops_refresh_materializes_compact_league_update_revisions():
    """A promoted NFL week must refresh the compact manifest source before verification."""
    workflow = yaml.load(
        (ROOT / ".github" / "workflows" / "refresh_live_nfl_ops.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    steps = workflow["jobs"]["refresh"]["steps"]
    names = [step.get("name") for step in steps]

    promote_index = names.index("Atomically promote the verified ops artifact")
    revision_index = names.index("Materialize compact league-update NFL revisions")
    verify_index = names.index("Verify promoted Fly receipt")
    command = steps[revision_index]["run"]

    assert promote_index < revision_index < verify_index
    assert "scripts/materialize_league_update_nfl_revisions.py" in command
    assert '--year "${{ steps.scope.outputs.refresh_year }}"' in command
    assert '--week "${{ steps.scope.outputs.refresh_week }}"' in command
    assert steps[revision_index]["env"]["DATABASE_BACKEND"] == "fly"


def test_manual_revision_materialization_does_not_rebuild_or_replace_ops():
    workflow = yaml.load(
        (ROOT / ".github" / "workflows" / "refresh_live_nfl_ops.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert workflow["on"]["workflow_dispatch"]["inputs"]["revisions_only"]["default"] == "false"
    assert "!inputs.revisions_only" in workflow["jobs"]["refresh"]["if"]
    job = workflow["jobs"]["materialize-revisions-only"]
    assert "inputs.revisions_only" in job["if"]
    steps = job["steps"]
    commands = "\n".join(step.get("run", "") for step in steps)
    assert "inputs.apply" in commands
    assert "scripts/materialize_league_update_nfl_revisions.py" in commands
    assert "refresh_live_nfl_ops.py" not in commands
    assert "promote_complete_artifact" not in commands
    assert "ops_nfl.duckdb" not in commands
    assert "DATABASE_BACKEND" in job["env"] and job["env"]["DATABASE_BACKEND"] == "fly"
    assert "DATABASE_ADMIN_TOKEN" in job["env"]
    assert all(not step.get("with", {}).get("repository") for step in steps if isinstance(step, dict))
