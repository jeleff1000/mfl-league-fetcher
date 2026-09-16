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


def test_derived_recovery_preserves_every_post_snapshot_publication_at_swap_time():
    source = WORKFLOWS[1].read_text(encoding="utf-8")

    assert '"derived_overlay": derived_overlay' in source
    assert '"snapshot_overlay_rows": snapshot_overlay_rows' in source
    assert '"calculated_final_rows"' in source
    assert "X-Recovery-Since: ${SNAPSHOT_CREATED_AT}" in source
    assert "X-Expected-Overlay-Leagues: ${EXPECTED_OVERLAY_LEAGUES}" in source
    assert "X-Expected-Overlay-Rows: ${table_overlay_rows}" in source
    assert ".overlay_rows == $overlay_rows" in source
