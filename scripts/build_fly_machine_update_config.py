"""Apply committed runtime/storage guardrails to a fresh Fly Machine config.

The live guest size, services, volume identity, and unrelated environment are
preserved.  Only settings that must match ``duckdb-server/fly.toml`` are
overlaid before the existing Machine is updated in place.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import tomllib


_GB = re.compile(r"^([1-9][0-9]*)\s*GB$", re.IGNORECASE)
_SIGNALS = {"SIGTERM", "SIGINT", "SIGQUIT", "SIGUSR1", "SIGUSR2"}


def _gb(value: object, field: str) -> int:
    match = _GB.fullmatch(str(value).strip())
    if not match:
        raise ValueError(f"{field} must be a positive whole-GB value")
    return int(match.group(1))


def build_machine_config(machine: dict, committed: dict) -> dict:
    config = deepcopy(machine.get("config"))
    if not isinstance(config, dict):
        raise ValueError("machine inventory is missing config")

    signal = str(committed.get("kill_signal", "")).strip().upper()
    timeout_seconds = committed.get("kill_timeout")
    if signal not in _SIGNALS:
        raise ValueError("committed kill_signal is not a supported graceful signal")
    if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 300:
        raise ValueError("committed kill_timeout must be between 1 and 300 seconds")
    config["stop_config"] = {
        "signal": signal,
        "timeout": timeout_seconds * 1_000_000_000,
    }

    committed_env = committed.get("env", {})
    if not isinstance(committed_env, dict):
        raise ValueError("committed env must be a table")
    live_env = config.setdefault("env", {})
    if not isinstance(live_env, dict):
        raise ValueError("machine env must be an object")
    live_env.update({str(key): str(value) for key, value in committed_env.items()})

    committed_mounts = committed.get("mounts", [])
    if not isinstance(committed_mounts, list) or len(committed_mounts) != 1:
        raise ValueError("fly.toml must define exactly one mount")
    mount_contract = committed_mounts[0]
    destination = str(mount_contract.get("destination", ""))
    mounts = config.get("mounts", [])
    matching_mounts = [mount for mount in mounts if mount.get("path") == destination]
    if destination != "/data" or len(matching_mounts) != 1:
        raise ValueError("machine must have exactly one /data mount")
    mount = matching_mounts[0]
    if not str(mount.get("volume", "")).startswith("vol_"):
        raise ValueError("/data mount is missing its existing volume identity")
    mount["extend_threshold_percent"] = int(
        mount_contract["auto_extend_size_threshold"]
    )
    mount["add_size_gb"] = _gb(
        mount_contract["auto_extend_size_increment"],
        "auto_extend_size_increment",
    )
    mount["size_gb_limit"] = _gb(
        mount_contract["auto_extend_size_limit"],
        "auto_extend_size_limit",
    )
    if not 1 <= mount["extend_threshold_percent"] <= 99:
        raise ValueError("auto_extend_size_threshold must be between 1 and 99")
    if mount["size_gb_limit"] <= mount["add_size_gb"]:
        raise ValueError("auto_extend_size_limit must exceed the increment")
    return config


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--machine-json", type=Path, required=True)
    parser.add_argument("--fly-toml", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    machine = json.loads(args.machine_json.read_text(encoding="utf-8"))
    with args.fly_toml.open("rb") as handle:
        committed = tomllib.load(handle)
    config = build_machine_config(machine, committed)
    args.output.write_text(
        json.dumps(config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
