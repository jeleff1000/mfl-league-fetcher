from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOWS = (
    "yahoo_incremental_refresh_worker.yml",
    "espn_incremental_refresh_worker.yml",
    "sleeper_incremental_refresh_worker.yml",
)


@pytest.mark.parametrize("workflow_name", WORKFLOWS)
def test_weekly_update_workers_skip_virtualenv_and_pip_upgrade(workflow_name: str):
    workflow = (REPO_ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")

    assert "timeout-minutes: 2" in workflow
    assert "python -m venv" not in workflow
    assert "python -m pip install --upgrade pip" not in workflow
    assert (
        "python -m pip install --disable-pip-version-check --quiet "
        "--no-cache-dir --no-compile -r requirements-weekly-update.txt"
    ) in workflow
    assert "refresh_live_nfl_ops.py" not in workflow
    assert "replace_database" not in workflow
    assert "python scripts/warm_vercel_cache.py" in workflow
    assert "--strategy expire" in workflow
    assert "--verify-hot" in workflow
