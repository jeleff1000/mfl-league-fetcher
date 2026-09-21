from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PARSE_ACTION = ROOT / ".github" / "actions" / "parse-league-data" / "action.yml"
YAHOO_REFRESH_WORKFLOW = ROOT / ".github" / "workflows" / "yahoo_incremental_refresh_worker.yml"


def test_shared_yahoo_credential_owner_is_exposed_to_workers():
    """A clone can read another league's credential without claiming ownership of it."""
    action = PARSE_ACTION.read_text(encoding="utf-8")

    assert "credential_database_name:" in action
    assert "credential_owner_db" in action
    assert 'f.write(f"credential_database_name={credential_owner_db}\\n")' in action
    assert "credential_owner_league_name" in action
    assert 'f.write(f"credential_league_name={credential_owner_league_name}\\n")' in action


def test_daily_demo_refresh_reuses_kmffl_oauth_but_publishes_demo_league():
    workflow = YAHOO_REFRESH_WORKFLOW.read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "cron:" in workflow
    assert "credential_db_name:" in workflow
    assert "required: false" in workflow
    assert "INPUT_DB_NAME: ${{ github.event_name == 'schedule' && 'demo_league' || inputs.db_name }}" in workflow
    assert "INPUT_CREDENTIAL_DB_NAME: ${{ github.event_name == 'schedule' && 'kmffl' || inputs.credential_db_name || inputs.db_name }}" in workflow
    assert "INPUT_EXECUTE: ${{ github.event_name == 'schedule' && 'true' || inputs.execute }}" in workflow
    assert 'args=(--db "${DB_NAME}" --credential-db "${CREDENTIAL_DB_NAME}"' in workflow
    assert "SYSTEM_DEMO_UPDATE: ${{ github.event_name == 'schedule' && 'true' || 'false' }}" in workflow
    refresh = workflow.split("- name: Refresh Yahoo active season", 1)[1].split("- name:", 1)[0]
    assert "SYSTEM_DEMO_UPDATE:" in workflow
    assert "--scheduled-demo" in refresh
    assert 'if [ "${SYSTEM_DEMO_UPDATE}" = "true" ]; then' in workflow
    assert "args+=(--scheduled-demo)" in workflow
    assert "LEAGUE_UPDATE_REQUIRE_CLAIM: ${{ github.event_name != 'schedule' && inputs.execute && '1' || '0' }}" in workflow
    assert "scheduled_demo:" not in workflow
