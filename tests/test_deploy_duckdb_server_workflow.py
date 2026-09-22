from pathlib import Path
import tomllib


def test_deploy_waits_for_ready_after_machine_restart() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")
    post_update = workflow.split("timeout 330s flyctl machine update", 1)[1]

    assert "for attempt in $(seq 1 20)" in post_update
    assert "curl --fail --silent --max-time 5 https://league-history-duckdb.fly.dev/ready || true" in post_update
    assert 'test "$ready" = true' in post_update


def test_deploy_retries_transient_registry_manifest_race() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")

    assert "for update_attempt in 1 2 3" in workflow
    assert "flyctl machine update" in workflow
    assert "sleep 2" in workflow


def test_deploy_applies_committed_runtime_without_resetting_live_capacity() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")

    assert "group: fly-duckdb-server-deploy" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "scripts/build_fly_machine_update_config.py" in workflow
    assert "--machine-config machine-config.json" in workflow
    assert "flyctl secrets list --app league-history-duckdb --json" in workflow
    assert "flyctl secrets unset --app league-history-duckdb --stage" in workflow
    assert '"DUCKDB_MEMORY_LIMIT" "DUCKDB_THREADS"' in workflow
    assert 'stop_config.signal == "SIGTERM"' in workflow
    assert '.config.stop_config.timeout == "5m0s"' in workflow
    assert ".config.stop_config.timeout == 300000000000" in workflow
    assert ".config.mounts[0].extend_threshold_percent == 80" in workflow
    assert ".config.mounts[0].add_size_gb == 10" in workflow
    assert ".config.mounts[0].size_gb_limit == 200" in workflow
    assert '.runtime_contract.duckdb_version == "v1.5.5"' in workflow


def test_production_duckdb_memory_budget_fits_the_machine_and_supports_merges() -> None:
    config = tomllib.loads(Path("duckdb-server/fly.toml").read_text(encoding="utf-8"))

    assert config["env"]["DUCKDB_MEMORY_LIMIT"] == "4096MB"
    assert config["vm"][0]["memory"] == "16gb"


def test_deploy_refuses_to_restart_with_pending_league_file_swap() -> None:
    workflow = Path(".github/workflows/deploy_duckdb_server.yml").read_text(encoding="utf-8")
    pre_update = workflow.split("timeout 330s flyctl machine update", 1)[0]

    assert "test ! -e /data/___leagues.duckdb.incoming" in pre_update
    assert "test ! -e /data/___leagues.duckdb.prev" in pre_update
