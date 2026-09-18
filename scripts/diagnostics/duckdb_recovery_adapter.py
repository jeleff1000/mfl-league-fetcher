"""Guarded primitives for offline five-object recovery; no production CLI.

All mutation orchestration must first bind these checks to the isolated volume,
exact engine artifact, retained WAL and an externally enforced deadline.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import threading
import time

CANONICAL = ("homepage_manager_rankings", "matchup_h2h_career", "player_fantasy_season",
             "player_fantasy_season_all", "standings_by_year")
QUARANTINED = tuple("__corrupt_recovery_" + name for name in CANONICAL)
RECOVERY_VOLUME = "vol_4919j2m0wzg0xw5r"
STAGE_LIMITS = {"inspect": 5, "preserve": 10, "remove": 5, "verify": 15}


def run_stage(stage, command, *, deadline, env=None):
    """Parent-enforced stage cap; timeout never implies transaction rollback."""
    started = time.monotonic()
    remaining = min(STAGE_LIMITS[stage], deadline - time.time())
    if remaining <= 0:
        return {"stage": stage, "outcome": "NOT_STARTED", "exit_code": 124, "elapsed_s": 0}
    print(json.dumps({"stage": stage, "event": "start", "limit_s": remaining}), flush=True)
    child = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             start_new_session=os.name == "posix")
    captured = {"stdout": bytearray(), "stderr": bytearray()}
    limited = threading.Event()

    def stop_child():
        try:
            if os.name == "posix":
                os.killpg(child.pid, signal.SIGKILL)
            else:
                child.kill()
        except ProcessLookupError:
            pass

    def drain(name, pipe):
        with pipe:
            while chunk := pipe.read1(4096):
                free = 65536 - len(captured[name])
                captured[name].extend(chunk[:free])
                if len(chunk) > free:
                    limited.set()
                    stop_child()
                    return

    readers = [threading.Thread(target=drain, args=(name, getattr(child, name)), daemon=True)
               for name in captured]
    for reader in readers:
        reader.start()
    try:
        code = child.wait(timeout=max(0.001, remaining - (time.monotonic() - started)))
    except subprocess.TimeoutExpired:
        stop_child()
        child.wait(timeout=1)
        code = 124
    finally:
        for reader in readers:
            reader.join(timeout=1)
    if limited.is_set():
        code = 125
    stdout, stderr = (captured[name].decode("utf-8", errors="replace") for name in ("stdout", "stderr"))
    outcome = "PASS" if code == 0 else ("UNKNOWN" if stage in {"remove", "verify"} else "FAILED")
    result = {"stage": stage, "outcome": outcome, "exit_code": code,
              "elapsed_s": round(time.monotonic() - started, 3),
              "stdout": stdout, "stderr": stderr, "output_limited": limited.is_set()}
    print(json.dumps({"event": "end", **result}), flush=True)
    return result


def validate_inventory(machine, volume, runtime_machine_id, path):
    """Bind API inventory to the machine actually executing the pilot."""
    if (machine.get("id") == "1781e011b69068"
            or volume.get("id") == "vol_rkg7mmd17llez224"):
        raise ValueError("production target forbidden in pilot")
    if (not re.fullmatch(r"[0-9a-f]{14}", runtime_machine_id)
            or machine.get("id") != runtime_machine_id
            or machine.get("state") != "started"
            or not machine.get("name", "").startswith("wkupd-table-pilot-")):
        raise ValueError("machine is not this running isolated pilot")
    if (volume.get("id") != RECOVERY_VOLUME
            or volume.get("name") != "wkupd_rebuild_35166181636"
            or volume.get("attached_machine_id") != runtime_machine_id
            or str(path) != "/data/___leagues.duckdb"):
        raise ValueError("volume or database identity mismatch")
    mounts = machine.get("config", {}).get("mounts", [])
    if len(mounts) != 1 or mounts[0].get("volume") != RECOVERY_VOLUME or mounts[0].get("path") != "/data":
        raise ValueError("isolated volume mount mismatch")


def validate_file_binding(path, mountpoint):
    path, mountpoint = Path(path), Path(mountpoint)
    if (path.name != "___leagues.duckdb" or path.parent != mountpoint
            or path.is_symlink() or path.resolve() != path.absolute()
            or not mountpoint.is_mount()):
        raise ValueError("database is not the direct file on the approved mount")
    file_stat, mount_stat = path.stat(), mountpoint.stat()
    if (not stat.S_ISREG(file_stat.st_mode) or file_stat.st_nlink != 1
            or file_stat.st_dev != mount_stat.st_dev):
        raise ValueError("database inode/device does not belong to the approved mount")
    return {"device": file_stat.st_dev, "inode": file_stat.st_ino,
            "size": file_stat.st_size, "mtime_ns": file_stat.st_mtime_ns}


def preserve_wal(source, destination, *, max_bytes=64 * 1024 * 1024):
    """Retain only the bounded WAL, never the database; never replace evidence."""
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or destination.is_symlink():
        raise ValueError("WAL evidence cannot use symlinks")
    named_before = source.lstat()
    if not stat.S_ISREG(named_before.st_mode) or not 0 < max_bytes <= 64 * 1024 * 1024:
        raise ValueError("WAL requires a regular file and bounded preservation ceiling")
    with source.open("rb") as original:
        before = os.fstat(original.fileno())
        if (before.st_dev, before.st_ino) != (named_before.st_dev, named_before.st_ino):
            raise ValueError("WAL pathname changed before preservation")
        if before.st_nlink != 1 or not 0 < before.st_size <= max_bytes:
            raise ValueError("WAL exceeds preservation ceiling or is not a unique regular file")
        digest = hashlib.sha256()
        copied = 0
        with destination.open("xb") as retained:
            while chunk := original.read(min(1024 * 1024, max_bytes - copied + 1)):
                copied += len(chunk)
                if copied > max_bytes:
                    raise ValueError("WAL grew beyond preservation ceiling")
                retained.write(chunk)
                digest.update(chunk)
            retained.flush()
            os.fsync(retained.fileno())
        after = os.fstat(original.fileno())
    named_after = source.lstat()
    if ((named_after.st_dev, named_after.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(named_after.st_mode)
            or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
            or (named_after.st_size, named_after.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
            or copied != before.st_size):
        raise ValueError("WAL changed during preservation; do not open database for writing")
    if os.name == "posix":
        fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return {"bytes": copied, "sha256": digest.hexdigest()}


def validate_objects(rows):
    """Require both exact sets in the actual persistent catalog, no aliases."""
    expected = {("___leagues", "public", name) for name in CANONICAL + QUARANTINED}
    identities = [tuple(row[:3]) for row in rows]
    if len(identities) != len(expected) or set(identities) != expected:
        raise ValueError("exact quarantined objects and all healthy replacements are required")


def validate_block(block):
    expected = {"block_id": 346, "offset": 90714112, "block_size": 262144,
                "file_changed_during_read": False, "checksum_valid": False,
                "block_sha256": "7bbcf166a70b06eb12c19888577060bf17e867a6f8b81ac7b802bffb3cab1186",
                "stored_checksum": 18392342689821271652, "computed_checksum": 5168518579405463287}
    if any(block.get(key) != value for key, value in expected.items()):
        raise ValueError("database does not match the exact known damaged block")


def validate_engine(identity):
    # Isolated receipt 35358633104, same deployed image as production.
    # A matching version string alone is not an ABI/build identity.
    expected = {"duckdb": "1.5.4", "engine_revision": "08e34c447b",
                "engine_sha256": "9135828981e3d0bdc346c10f663e353eb12de486d352edd4af89651a926967a9",
                "engine_bytes": 60210744, "python": "3.11.16",
                "architecture": "x86_64", "libc": ["glibc", "2.41"]}
    normalized = {**identity, "libc": list(identity.get("libc", []))}
    if any(normalized.get(key) != value for key, value in expected.items()):
        raise ValueError("engine artifact or runtime does not match isolated identity receipt")


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def capture_witness(conn, db_name):
    """Small deterministic value receipts; league-filtered, explicit columns.

    Inspect both ordered ends, so current and historical matchup values enter
    the receipt. This is a sampled preservation witness, not a fleet audit.
    No raw values or credential material are logged by this function.
    """
    if not re.fullmatch(r"[a-z0-9_]+", db_name):
        raise ValueError("invalid witness league")
    result = {}
    for table in ("matchup",) + CANONICAL:
        columns = conn.execute("""SELECT column_name, data_type FROM duckdb_columns()
            WHERE database_name=current_database() AND schema_name='public' AND table_name=?
            ORDER BY column_index""", [table]).fetchall()
        if "db_name" not in {row[0] for row in columns}:
            raise ValueError(f"witness {table} is missing its league identity")
        names = ", ".join(_quote(row[0]) for row in columns)
        samples = []
        for direction in ("ASC", "DESC"):
            rows = conn.execute(f'SELECT {names} FROM public.{_quote(table)} WHERE db_name=? ORDER BY ALL {direction} NULLS LAST LIMIT 8', [db_name]).fetchall()
            samples.append(rows)
        if not samples[0]:
            raise ValueError(f"witness {table} has no rows for the selected league")
        payload = json.dumps({"schema": columns, "values": samples}, default=str, separators=(",", ":"))
        result[table] = {"rows": len(samples[0]), "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    return result


def compare_witness(before, after):
    if before != after:
        changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
        raise ValueError("preservation witness changed: " + ", ".join(changed))
