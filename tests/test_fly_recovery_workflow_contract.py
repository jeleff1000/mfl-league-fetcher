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
        assert "flyctl machines list" in source, workflow
        assert "select(.name == $name)" in source, workflow
