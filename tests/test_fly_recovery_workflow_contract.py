from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = (
    ROOT / ".github" / "workflows" / "fly_duckdb_recovery_diagnostics.yml",
    ROOT / ".github" / "workflows" / "fly_league_settings_recovery.yml",
)


def test_recovery_helper_machine_does_not_request_unsupported_json_output():
    for workflow in WORKFLOWS:
        source = workflow.read_text(encoding="utf-8")
        helper_invocation = source[source.index("flyctl machine run") : source.index("recovery_machine_id=", source.index("flyctl machine run"))]

        assert "--json" not in helper_invocation, workflow
        assert '--name "$recovery_machine_name"' in helper_invocation, workflow
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


def test_settings_recovery_requires_a_durable_checkpoint():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert '.checkpointed == true' in source
    assert '.checkpoint_error == null' in source


def test_settings_recovery_can_reuse_a_retained_unattached_volume():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "existing_volume_id:" in source
    assert '[[ "$EXISTING_VOLUME_ID" =~ ^vol_[A-Za-z0-9]+$ ]]' in source
    assert 'test "$existing_attached_machine" = "null"' in source
    assert '[[ "$existing_name" == wkupd_rebuild_* ]]' in source
    assert 'if [ "$owns_recovery_volume" = "true" ]' in source


def test_derived_recovery_preserves_every_post_snapshot_publication_at_swap_time():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert "recover_settings:" in source
    assert 'if [ "$RECOVER_SETTINGS" = "true" ]; then' in source
    assert '"derived_overlay": derived_overlay' in source
    assert '"snapshot_overlay_rows": snapshot_overlay_rows' in source
    assert '"calculated_final_rows"' in source
    assert "X-Recovery-Since: ${SNAPSHOT_CREATED_AT}" in source
    assert "X-Expected-Overlay-Leagues: ${EXPECTED_OVERLAY_LEAGUES}" in source
    assert "X-Expected-Overlay-Rows: ${table_overlay_rows}" in source
    assert ".overlay_rows == $overlay_rows" in source


def test_isolated_rebuild_cannot_mutate_or_promote_the_primary_volume():
    source = (
        ROOT / ".github" / "workflows" / "fly_duckdb_isolated_rebuild.yml"
    ).read_text(encoding="utf-8")

    assert '--snapshot-id "$SNAPSHOT_ID"' in source
    assert '"wkupd_rebuild_${GITHUB_RUN_ID}"' in source
    assert "--target /data/___leagues.clean.duckdb" in source
    assert "--empty-table public.homepage_manager_rankings" in source
    assert "--empty-table public.standings_by_year" in source
    assert "flyctl machine stop" not in source
    assert "flyctl machine clone" not in source
    assert "/replace-db" not in source
    assert "flyctl deploy" not in source


def test_isolated_rebuild_can_fork_current_volume_without_snapshot_restore():
    source = (
        ROOT / ".github" / "workflows" / "fly_duckdb_isolated_rebuild.yml"
    ).read_text(encoding="utf-8")

    assert "source_mode:" in source
    assert 'flyctl volumes fork "$source_volume_id"' in source
    assert '--name "wkupd_rebuild_${GITHUB_RUN_ID}"' in source
    assert "--vm-cpus 2 --vm-memory 4096" in source


def test_isolated_rebuild_can_reuse_retained_unattached_volume():
    source = (
        ROOT / ".github" / "workflows" / "fly_duckdb_isolated_rebuild.yml"
    ).read_text(encoding="utf-8")

    assert "existing_volume_id:" in source
    assert '[[ "$EXISTING_VOLUME_ID" =~ ^vol_[A-Za-z0-9]+$ ]]' in source
    assert '[[ "$existing_name" == wkupd_rebuild_* ]]' in source
    assert 'test "$existing_state" = "created"' in source
    assert 'test "$existing_attachment" = "null"' in source


def test_isolated_rebuild_installs_clean_file_only_after_validation():
    source = (
        ROOT / ".github" / "workflows" / "fly_duckdb_isolated_rebuild.yml"
    ).read_text(encoding="utf-8")

    validation = source.index("python /tmp/validate_isolated_rebuild.py")
    install = source.index("python /tmp/install_isolated_rebuild.py")
    assert validation < install
    assert 'os.replace(source, backup)' in source
    assert 'os.replace(clean, source)' in source
    assert 'duckdb.connect(str(source), read_only=True)' in source
    assert 'backup.unlink()' in source


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
