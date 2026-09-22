import json
from pathlib import Path
import subprocess
import sys


SCRIPT = Path("scripts/build_fly_machine_update_config.py")


def test_build_preserves_live_machine_and_applies_storage_contract(tmp_path: Path) -> None:
    machine = {
        "config": {
            "image": "registry.fly.io/example:old",
            "env": {"KEEP_ME": "yes", "DUCKDB_MEMORY_LIMIT": "6GB"},
            "guest": {"cpu_kind": "shared", "cpus": 4, "memory_mb": 8192},
            "mounts": [{"volume": "vol_primary", "path": "/data"}],
            "services": [{"protocol": "tcp", "internal_port": 8080}],
        }
    }
    fly_toml = """
kill_signal = "SIGTERM"
kill_timeout = 300

[env]
DUCKDB_EXPECTED_VERSION = "1.5.5"
DUCKDB_MEMORY_LIMIT = "1024MB"

[[mounts]]
source = "duckdb_data"
destination = "/data"
auto_extend_size_threshold = 80
auto_extend_size_increment = "10GB"
auto_extend_size_limit = "200GB"
"""
    machine_path = tmp_path / "machine.json"
    toml_path = tmp_path / "fly.toml"
    output_path = tmp_path / "machine-config.json"
    machine_path.write_text(json.dumps(machine), encoding="utf-8")
    toml_path.write_text(fly_toml, encoding="utf-8")

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--machine-json",
            str(machine_path),
            "--fly-toml",
            str(toml_path),
            "--output",
            str(output_path),
        ],
        check=True,
    )

    config = json.loads(output_path.read_text(encoding="utf-8"))
    assert config["guest"] == machine["config"]["guest"]
    assert config["services"] == machine["config"]["services"]
    assert config["mounts"] == [{
        "volume": "vol_primary",
        "path": "/data",
        "extend_threshold_percent": 80,
        "add_size_gb": 10,
        "size_gb_limit": 200,
    }]
    assert config["env"]["KEEP_ME"] == "yes"
    assert config["env"]["DUCKDB_EXPECTED_VERSION"] == "1.5.5"
    assert config["env"]["DUCKDB_MEMORY_LIMIT"] == "1024MB"
    assert config["stop_config"] == {
        "signal": "SIGTERM",
        "timeout": 300_000_000_000,
    }


def test_build_rejects_a_machine_without_the_committed_mount(tmp_path: Path) -> None:
    machine_path = tmp_path / "machine.json"
    toml_path = tmp_path / "fly.toml"
    output_path = tmp_path / "machine-config.json"
    machine_path.write_text(json.dumps({"config": {"mounts": []}}), encoding="utf-8")
    toml_path.write_text(
        'kill_signal="SIGTERM"\nkill_timeout=300\n'
        '[[mounts]]\nsource="duckdb_data"\ndestination="/data"\n',
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--machine-json",
            str(machine_path),
            "--fly-toml",
            str(toml_path),
            "--output",
            str(output_path),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "exactly one /data mount" in result.stderr
    assert not output_path.exists()
