from pathlib import Path


def test_deploy_waits_for_ready_after_machine_restart() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")
    post_update = workflow.split("timeout 90s flyctl machine update", 1)[1]

    assert "for attempt in $(seq 1 20)" in post_update
    assert "curl --fail --silent --max-time 5 https://league-history-duckdb.fly.dev/ready || true" in post_update
    assert 'test "$ready" = true' in post_update
