from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = (
    ROOT / ".github" / "workflows" / "fly_duckdb_recovery_diagnostics.yml",
    ROOT / ".github" / "workflows" / "fly_league_settings_recovery.yml",
)


def test_recovery_helper_machine_does_not_request_unsupported_json_output():
    for workflow in WORKFLOWS:
        source = workflow.read_text(encoding="utf-8")
        invocation_start = source.index("flyctl machine run")
        helper_invocation = source[
            invocation_start : source.index("| tee", invocation_start)
        ]

        assert "--json" not in helper_invocation, workflow
        assert any(
            f'--name "${variable}"' in helper_invocation
            for variable in ("recovery_machine_name", "inspect_machine_name")
        ), workflow
        assert "--file-local" in helper_invocation, workflow
        assert "--file-literal" not in helper_invocation, workflow
        assert "flyctl machines list" in source, workflow
        assert "select(.name == $name)" in source, workflow


def test_settings_recovery_derives_final_live_count_from_snapshot_and_overlay():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "recovery-overlay-manifest.json" in source
    assert "snapshot_overlay_rows" in source
    assert "calculated_final_rows" in source
    assert 'EXPECTED_FINAL_ROWS" != "auto"' in source
    assert "X-Expected-Rows: ${FINAL_EXPECTED_ROWS}" in source


def test_settings_recovery_requires_one_durable_checkpoint_after_all_replacements():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    settings_replace = source.index("X-Table-Name: league_settings")
    derived_replace = source.index("X-Table-Name: ${table}")
    final_checkpoint = source.index("checkpoint_body='", derived_replace)
    assert settings_replace < derived_replace < final_checkpoint
    assert "intermediate canonical replacement" in source
    assert "checksum" in source


def test_settings_recovery_reads_live_overlay_one_league_at_a_time():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "for db_name in names:" in source
    assert "WHERE db_name = {db_literal}" in source
    assert "WHERE db_name IN ({quoted})" not in source


def test_settings_recovery_can_reuse_a_retained_unattached_volume():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "existing_volume_id:" in source
    assert '[[ "$EXISTING_VOLUME_ID" =~ ^vol_[A-Za-z0-9]+$ ]]' in source
    assert 'test "$existing_attached_machine" = "null"' in source
    assert '[[ "$existing_name" == wkupd_rebuild_* ]]' in source
    assert 'if [ "$owns_recovery_volume" = "true" ]' in source


def test_derived_recovery_defers_changed_leagues_to_source_rebuild():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "recover_settings:" in source
    assert 'if [ "$RECOVER_SETTINGS" = "true" ]; then' in source
    assert '"snapshot_overlay_rows": snapshot_overlay_rows' in source
    assert '"calculated_final_rows"' in source
    assert '"live_overlay_rows": 0' in source
    replace = source.index("X-Table-Name: ${table}")
    derived_start = source.rfind(
        "for table in homepage_manager_rankings matchup_h2h_career", 0, replace
    )
    rebuild = source.index("/rebuild-league-derived")
    derived_replacement = source[derived_start:rebuild]
    assert "X-Recovery-Since" not in derived_replacement
    assert "X-Expected-Overlay-Rows" not in derived_replacement
    assert ".overlay_leagues == 0" in derived_replacement
    assert ".overlay_rows == 0" in derived_replacement
    assert replace < rebuild


def test_isolated_full_database_rebuild_workflow_is_not_dispatchable():
    assert not (
        ROOT / ".github" / "workflows" / "fly_duckdb_isolated_rebuild.yml"
    ).exists()


def test_recovery_probe_can_read_but_not_clean_up_retained_backup_volume():
    source = WORKFLOWS[0].read_text(encoding="utf-8")

    assert '[[ "$inspect_name" == wkupd_rebuild_* || "$inspect_name" == duckdb_backup_* ]]' in source
    cleanup = source.split('if [ "$CLEANUP_RECOVERY_RESOURCES" = "true" ]; then', 1)[1]
    cleanup = cleanup.split("exit 0", 1)[0]
    assert 'startswith("wkupd_")' in cleanup
    assert "duckdb_backup_" not in cleanup


def test_table_pilot_workflow_rejects_primary_before_calling_fly(tmp_path):
    import os
    import shutil
    import subprocess
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/fly_duckdb_reaggregate_recovery.yml").read_text())
    step = next(step for step in workflow["jobs"]["pilot"]["steps"] if step.get("name", "").startswith("One-table pilot"))
    bash = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else shutil.which("bash")
    env = dict(os.environ, RECOVERY_VOLUME_ID="vol_rkg7mmd17llez224", PILOT_ACTION="remove",
               TARGET_TABLE="player_fantasy_season", WITNESS_DB_NAME="nyu_ffl")
    result = subprocess.run(
        [bash], input="flyctl() { echo UNEXPECTED_FLY_CALL; return 99; }; export -f flyctl\n" + step["run"],
        cwd=tmp_path, env=env, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode != 0
    assert "stage=pilot_start" in result.stdout
    assert "UNEXPECTED_FLY_CALL" not in result.stdout


def test_table_pilot_rechecks_deadline_after_ssh_connect(tmp_path):
    import os
    import shutil
    import subprocess
    import time
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/fly_duckdb_reaggregate_recovery.yml").read_text())
    step = next(step for step in workflow["jobs"]["pilot"]["steps"] if step.get("name", "").startswith("One-table pilot"))
    ssh = step["run"].split('flyctl ssh console', 1)[1].split('echo "stage=pilot_finished', 1)[0]
    bash = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else shutil.which("bash")
    env = dict(os.environ, PILOT_DEADLINE=str(int(time.time()) - 1), remaining="35",
               FLY_APP="test", machine_id="test", PILOT_ACTION="remove", TARGET_TABLE="player_fantasy_season",
               WITNESS_DB_NAME="nyu_ffl", RECOVERY_VOLUME_ID="vol_test")
    # Execute exactly the remote command with a connection that arrived late.
    execute_remote = 'flyctl() { while [ "$1" != "-C" ]; do shift; done; bash -c "$2"; };\n'
    result = subprocess.run([bash], input=execute_remote + "flyctl ssh console" + ssh,
                            cwd=tmp_path, env=env, text=True, capture_output=True, timeout=5)
    assert result.returncode == 124, (result.stdout, result.stderr)
