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
