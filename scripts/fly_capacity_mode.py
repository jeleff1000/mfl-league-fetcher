#!/usr/bin/env python3
"""Switch Fly DuckDB primary capacity between explicit operating modes.

This keeps infrastructure resizing out of import workflows. Use it before a
planned fleet import, public traffic spike, or emergency, then switch back to
normal when the event is over. The live DuckDB service is intentionally
primary-only.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


APP_DEFAULT = "league-history-duckdb"
PRIMARY_DEFAULT = "1781e011b69068"
PRIMARY_VOLUME_DEFAULT = "vol_rkg7mmd17llez224"
CAPACITY_PASSWORD_ENV = "DUCKDB_CAPACITY_MODE_PASSWORD"
CAPACITY_PASSWORD_INPUT_ENV = "DUCKDB_CAPACITY_MODE_PASSWORD_INPUT"
ENV_FILE_DEFAULT = ".env"

SHAPE_HOURLY = {
    ("shared", 4, 4096): 0.0329,
    ("shared", 4, 8192): 0.0617,
    ("shared", 8, 16384): 0.1189,
}
RAM_LEVELS = {
    "4gb": 4096,
    "8gb": 8192,
    "16gb": 16384,
}
CPU_LEVELS = {
    "shared4x": ("shared", 4),
    "shared8x": ("shared", 8),
}
CPU_MEMORY_LIMIT_MB = {
    "shared4x": 8192,
    "shared8x": 16384,
}
STORAGE_LEVELS = {
    "keep": None,
    "50gb": 50,
    "100gb": 100,
    "200gb": 200,
}
DEFAULT_TOTAL_VOLUME_GB = 100
VOLUME_GB_MONTHLY = 0.15
MONTH_HOURS = 730


@dataclass(frozen=True)
class CapacityMode:
    name: str
    description: str
    primary_memory_mb: int


MODES: dict[str, CapacityMode] = {
    "normal": CapacityMode(
        name="normal",
        description="Steady state: one hot primary with enough RAM for DuckDB plus attached league data.",
        primary_memory_mb=8192,
    ),
    "event": CapacityMode(
        name="event",
        description="Public traffic spike: primary-only headroom.",
        primary_memory_mb=8192,
    ),
    "fleet": CapacityMode(
        name="fleet",
        description="Fleet import: temporary primary-only max headroom for concurrent worker bursts.",
        primary_memory_mb=16384,
    ),
    "emergency": CapacityMode(
        name="emergency",
        description="Temporary primary-only max headroom for heavy traffic or debugging.",
        primary_memory_mb=16384,
    ),
}


def load_env_file(path: str = ENV_FILE_DEFAULT) -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            key, separator, value = line.partition("=")
            if not separator:
                continue
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value


def parse_machine_ids(raw: str) -> list[str]:
    seen: set[str] = set()
    ids: list[str] = []
    for part in raw.replace(";", ",").split(","):
        machine_id = part.strip()
        if machine_id and machine_id not in seen:
            seen.add(machine_id)
            ids.append(machine_id)
    return ids


def parse_volume_ids(raw: str) -> list[str]:
    return parse_machine_ids(raw)


def fly_cmd() -> str:
    configured = os.environ.get("FLYCTL_BIN")
    if configured:
        return configured
    return shutil.which("flyctl") or shutil.which("fly") or "flyctl"


def run(args: list[str], *, dry_run: bool, check: bool = True) -> subprocess.CompletedProcess[str] | None:
    print("+ " + " ".join(args), flush=True)
    if dry_run:
        return None
    result = subprocess.run(args, text=True, capture_output=True)
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)
    return result


def update_machine(
    machine_id: str,
    *,
    app: str,
    cpu_kind: str,
    cpus: int,
    memory_mb: int,
    autostop: str,
    mode: str,
    dry_run: bool,
) -> None:
    run(
        [
            fly_cmd(),
            "machine",
            "update",
            machine_id,
            "--app",
            app,
            "--vm-cpu-kind",
            cpu_kind,
            "--vm-cpus",
            str(cpus),
            "--vm-memory",
            str(memory_mb),
            "--autostart",
            f"--autostop={autostop}",
            "--env",
            f"FLY_CAPACITY_MODE={mode}",
            "--yes",
        ],
        dry_run=dry_run,
    )


def start_machine(machine_id: str, *, app: str, dry_run: bool) -> None:
    run([fly_cmd(), "machine", "start", machine_id, "--app", app], dry_run=dry_run, check=False)


def wait_ready(ready_url: str, *, app: str = APP_DEFAULT, timeout_seconds: int = 300) -> None:
    print(f"Waiting for {ready_url} ...")
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(ready_url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if payload.get("ready") is True:
                print(json.dumps(payload, indent=2))
                return
            last_error = json.dumps(payload)
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        print(f"Not ready yet: {last_error}")
        time.sleep(10)
    print("Recent Fly logs before readiness timeout:", flush=True)
    run([fly_cmd(), "logs", "--app", app, "--no-tail"], dry_run=False, check=False)
    raise SystemExit(f"DuckDB API did not become ready within {timeout_seconds}s: {last_error}")


def machine_status(app: str, *, dry_run: bool) -> None:
    run([fly_cmd(), "machines", "list", "--app", app], dry_run=dry_run)
    run([fly_cmd(), "checks", "list", "--app", app], dry_run=dry_run, check=False)
    run([fly_cmd(), "volumes", "list", "--app", app], dry_run=dry_run, check=False)


def volume_sizes(app: str, *, dry_run: bool) -> dict[str, int]:
    if dry_run:
        return {}
    result = run([fly_cmd(), "volumes", "list", "--app", app, "--json"], dry_run=False)
    if result is None or not result.stdout:
        return {}
    try:
        rows = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}
    sizes: dict[str, int] = {}
    for row in rows:
        volume_id = str(row.get("id") or "")
        size_gb = row.get("size_gb") or row.get("sizeGB") or row.get("size")
        if volume_id and size_gb is not None:
            try:
                sizes[volume_id] = int(size_gb)
            except (TypeError, ValueError):
                pass
    return sizes


def extend_volume(
    volume_id: str,
    *,
    app: str,
    target_gb: int,
    current_sizes: dict[str, int],
    dry_run: bool,
) -> None:
    current_gb = current_sizes.get(volume_id)
    if current_gb is not None:
        if current_gb > target_gb:
            print(f"Volume {volume_id} is already {current_gb}GB; not shrinking to {target_gb}GB")
            return
        if current_gb == target_gb:
            print(f"Volume {volume_id} is already {target_gb}GB")
            return
    run(
        [
            fly_cmd(),
            "volumes",
            "extend",
            volume_id,
            "--app",
            app,
            "--size",
            str(target_gb),
            "--yes",
        ],
        dry_run=dry_run,
    )


def estimated_total_volume_gb(
    *,
    volume_ids: list[str],
    storage_target_gb: int | None,
    current_sizes: dict[str, int],
) -> int:
    if not volume_ids:
        return DEFAULT_TOTAL_VOLUME_GB

    known_total_gb = 0
    unknown_count = 0
    for volume_id in volume_ids:
        current_gb = current_sizes.get(volume_id)
        if current_gb is None:
            unknown_count += 1
            if storage_target_gb is not None:
                known_total_gb += storage_target_gb
            continue
        known_total_gb += max(current_gb, storage_target_gb or current_gb)

    if unknown_count and storage_target_gb is None:
        fallback_per_volume_gb = DEFAULT_TOTAL_VOLUME_GB // len(volume_ids)
        known_total_gb += fallback_per_volume_gb * unknown_count
    return known_total_gb


def machine_hourly(*, cpu_kind: str, cpus: int, memory_mb: int) -> float:
    key = (cpu_kind, cpus, memory_mb)
    if key not in SHAPE_HOURLY:
        raise ValueError(f"No cost estimate configured for {cpu_kind}-{cpus}x/{memory_mb}MB")
    return SHAPE_HOURLY[key]


def estimated_cost(
    *,
    primary_memory_mb: int,
    cpu_kind: str,
    cpus: int,
    total_volume_gb: int,
) -> tuple[float, float]:
    compute_hourly = machine_hourly(cpu_kind=cpu_kind, cpus=cpus, memory_mb=primary_memory_mb)
    volume_hourly = (total_volume_gb * VOLUME_GB_MONTHLY) / MONTH_HOURS
    hourly = compute_hourly + volume_hourly
    monthly = hourly * MONTH_HOURS
    return hourly, monthly


def resolve_cpu_level(cpu_level: str, memory_mb: int) -> tuple[str, int, str]:
    if cpu_level == "auto":
        cpu_level = "shared8x" if memory_mb > CPU_MEMORY_LIMIT_MB["shared4x"] else "shared4x"
    limit_mb = CPU_MEMORY_LIMIT_MB[cpu_level]
    if memory_mb > limit_mb:
        raise SystemExit(
            f"{memory_mb}MB is too high for {cpu_level}; choose --cpu-level shared8x " "or lower the RAM level."
        )
    cpu_kind, cpus = CPU_LEVELS[cpu_level]
    return cpu_kind, cpus, cpu_level


def require_capacity_password(provided_password: str | None) -> None:
    expected_password = os.environ.get(CAPACITY_PASSWORD_ENV)
    if not expected_password:
        raise SystemExit(f"Missing {CAPACITY_PASSWORD_ENV}; refusing to scale")
    if not provided_password:
        raise SystemExit("Missing capacity mode password; refusing to scale")
    if not hmac.compare_digest(provided_password.encode("utf-8"), expected_password.encode("utf-8")):
        raise SystemExit("Invalid capacity mode password; refusing to scale")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=[*MODES.keys(), "status"], required=True)
    parser.add_argument("--app", default=os.environ.get("FLY_APP", APP_DEFAULT))
    parser.add_argument(
        "--primary-machine-id",
        default=os.environ.get("FLY_PRIMARY_MACHINE_ID") or PRIMARY_DEFAULT,
    )
    parser.add_argument(
        "--primary-volume-id",
        default=os.environ.get("FLY_PRIMARY_VOLUME_ID") or PRIMARY_VOLUME_DEFAULT,
    )
    parser.add_argument(
        "--ram-level",
        choices=RAM_LEVELS,
        help="Set primary RAM.",
    )
    parser.add_argument(
        "--cpu-level",
        choices=[*CPU_LEVELS.keys(), "auto"],
        default="auto",
        help="CPU shape for database machines. 16GB requires shared8x.",
    )
    parser.add_argument(
        "--machine-count",
        type=int,
        choices=[1],
        default=1,
        help="Total machines to keep active. Primary-only default is 1.",
    )
    parser.add_argument(
        "--storage-level",
        choices=STORAGE_LEVELS,
        default="keep",
        help="Optionally extend each configured Fly volume to this size.",
    )
    parser.add_argument(
        "--ready-url",
        default=(os.environ.get("DATABASE_SERVER_URL", "").rstrip("/") or f"https://{APP_DEFAULT}.fly.dev") + "/ready",
    )
    parser.add_argument(
        "--duration-minutes",
        type=int,
        choices=[30, 60, 120, 240],
        help="Optional event duration note for cost estimate.",
    )
    parser.add_argument("--duration-hours", type=float, help="Optional event duration note for cost estimate.")
    parser.add_argument("--confirm", help=argparse.SUPPRESS)
    parser.add_argument("--yes", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--password", default=os.environ.get(CAPACITY_PASSWORD_INPUT_ENV), help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-verify", action="store_true")
    return parser


def main() -> int:
    load_env_file()
    args = build_parser().parse_args()
    if args.machine_count != 1:
        raise SystemExit("DuckDB capacity mode is primary-only; --machine-count must be 1.")
    volume_ids = parse_volume_ids(args.primary_volume_id)

    if args.mode == "status":
        machine_status(args.app, dry_run=args.dry_run)
        if not args.skip_verify and not args.dry_run:
            wait_ready(args.ready_url, app=args.app, timeout_seconds=60)
        return 0

    require_capacity_password(args.password)

    mode = MODES[args.mode]
    primary_memory_mb = RAM_LEVELS[args.ram_level] if args.ram_level else mode.primary_memory_mb
    cpu_kind, cpus, resolved_cpu_level = resolve_cpu_level(args.cpu_level, primary_memory_mb)
    storage_target_gb = STORAGE_LEVELS[args.storage_level]
    current_sizes = volume_sizes(args.app, dry_run=args.dry_run)
    total_volume_gb = estimated_total_volume_gb(
        volume_ids=volume_ids,
        storage_target_gb=storage_target_gb,
        current_sizes=current_sizes,
    )
    hourly, monthly = estimated_cost(
        primary_memory_mb=primary_memory_mb,
        cpu_kind=cpu_kind,
        cpus=cpus,
        total_volume_gb=total_volume_gb,
    )
    print(f"Applying capacity mode: {mode.name}")
    print(mode.description)
    print(f"CPU shape: {resolved_cpu_level} ({cpu_kind}, {cpus} vCPU)")
    print(f"Primary: {args.primary_machine_id} -> {primary_memory_mb}MB, autostop=off")
    print("Topology: primary-only")
    if storage_target_gb is None:
        print("Storage: keep existing volume sizes")
    else:
        print(f"Storage: extend configured volumes to {storage_target_gb}GB each")
    print(f"Approx while active: ${hourly:.3f}/hr, ${monthly:.0f}/mo if left this way all month")
    duration_hours = args.duration_minutes / 60 if args.duration_minutes else args.duration_hours
    if duration_hours:
        duration_label = f"{args.duration_minutes:g} minutes" if args.duration_minutes else f"{duration_hours:g} hours"
        print(f"Approx for {duration_label}: ${hourly * duration_hours:.2f}")

    if storage_target_gb is not None:
        for volume_id in volume_ids:
            extend_volume(
                volume_id,
                app=args.app,
                target_gb=storage_target_gb,
                current_sizes=current_sizes,
                dry_run=args.dry_run,
            )

    update_machine(
        args.primary_machine_id,
        app=args.app,
        cpu_kind=cpu_kind,
        cpus=cpus,
        memory_mb=primary_memory_mb,
        autostop="off",
        mode=mode.name,
        dry_run=args.dry_run,
    )
    start_machine(args.primary_machine_id, app=args.app, dry_run=args.dry_run)

    machine_status(args.app, dry_run=args.dry_run)
    if not args.skip_verify and not args.dry_run:
        wait_ready(args.ready_url, app=args.app)
    print(f"Capacity mode applied: {mode.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
