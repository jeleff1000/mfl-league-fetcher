from pathlib import Path


def test_deploy_waits_for_ready_after_machine_restart() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")
    post_update = workflow.split("timeout 90s flyctl machine update", 1)[1]

    assert "for attempt in $(seq 1 20)" in post_update
    assert "curl --fail --silent --max-time 5 https://league-history-duckdb.fly.dev/ready || true" in post_update
    assert 'test "$ready" = true' in post_update


def test_deploy_retries_transient_registry_manifest_race() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")

    assert "for update_attempt in 1 2 3" in workflow
    assert "flyctl machine update" in workflow
    assert "sleep 2" in workflow


def test_deploy_refuses_to_restart_with_pending_league_file_swap() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")
    pre_update = workflow.split("timeout 90s flyctl machine update", 1)[0]

    assert "test ! -e /data/___leagues.duckdb.incoming" in pre_update
    assert "test ! -e /data/___leagues.duckdb.prev" in pre_update
