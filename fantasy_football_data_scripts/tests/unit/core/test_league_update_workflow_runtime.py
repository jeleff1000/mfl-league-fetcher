from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[4]
WORKFLOWS = {
    "yahoo": "yahoo_incremental_refresh_worker.yml",
    "espn": "espn_incremental_refresh_worker.yml",
    "sleeper": "sleeper_incremental_refresh_worker.yml",
}


@pytest.mark.parametrize(("platform", "workflow_name"), WORKFLOWS.items())
def test_weekly_update_workers_skip_virtualenv_and_pip_upgrade(platform: str, workflow_name: str):
    workflow = (REPO_ROOT / ".github" / "workflows" / workflow_name).read_text(encoding="utf-8")

    assert "timeout-minutes: 3" in workflow
    assert "LEAGUE_UPDATE_DEADLINE_EPOCH=$(( $(date +%s) + 120 ))" in workflow
    assert 'timeout --signal=KILL "${remaining}s"' in workflow
    assert "python -m venv" not in workflow
    assert "python -m pip install --upgrade pip" not in workflow
    assert "uses: astral-sh/setup-uv@v5" in workflow
    assert "enable-cache: true" in workflow
    assert f'cache-dependency-glob: "requirements-weekly-update-{platform}.txt"' in workflow
    assert f"uv pip install --system --quiet -r requirements-weekly-update-{platform}.txt" in workflow
    assert "--no-cache-dir" not in workflow
    assert "refresh_live_nfl_ops.py" not in workflow
    assert "replace_database" not in workflow
    assert "python scripts/warm_vercel_cache.py" in workflow
    assert "--strategy expire" in workflow
    assert "--verify-hot" in workflow


def test_daily_demo_refresh_keeps_target_and_credential_owner_separate():
    workflow = (REPO_ROOT / ".github" / "workflows" / WORKFLOWS["yahoo"]).read_text(
        encoding="utf-8",
    )

    assert "- cron: '17 10 * * *'" in workflow
    assert "github.event_name == 'schedule' && 'demo_league' || inputs.db_name" in workflow
    assert "github.event_name == 'schedule' && 'kmffl' || inputs.credential_db_name || inputs.db_name" in workflow
    assert 'args=(--db "${DB_NAME}" --credential-db "${CREDENTIAL_DB_NAME}"' in workflow
