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


def test_isolated_reaggregation_is_limited_to_the_retained_recovery_volume():
    source = (
        ROOT / ".github" / "workflows" / "fly_duckdb_reaggregate_recovery.yml"
    ).read_text(encoding="utf-8")

    assert "scripts/fly_reaggregate_derived.py" in source
    assert "scripts/fly_rebuild_duckdb.py" not in source
    assert "___leagues.clean.duckdb" not in source
    assert "--database /data/___leagues.duckdb" in source
    assert "--ops /data/___ops.duckdb" in source
    assert "--ops-nfl /data/___ops_nfl.duckdb" in source
    assert "wkupd_rebuild_" in source
    assert "--vm-cpus 2 --vm-memory 4096" in source
    assert '-C "env PYTHONPATH=/app python /tmp/fly_reaggregate_derived.py' in source
    assert "--drop-quarantined-targets-only" not in source
    assert "quarantine_drop" not in source
    assert "recovery_mode:" not in source
    assert "flyctl machine stop" not in source
    assert "flyctl machine clone" not in source
    assert "/replace-db" not in source
    assert "flyctl deploy" not in source
