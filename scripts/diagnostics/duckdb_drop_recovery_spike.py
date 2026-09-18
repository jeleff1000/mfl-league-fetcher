"""Throwaway engine-removal feasibility experiment; NO existing database inputs.

Only synthetic, locally generated <=8 MiB files are eligible. No Fly access,
existing volumes, arbitrary SQL, checksum bypass, or database-page repair.
One intentional byte flip creates a failing test fixture, never repairs one.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time

START = time.monotonic()
LIMIT = 8 * 1024 * 1024
TABLE = "__corrupt_recovery_player_fantasy_season"
WITNESS = "SELECT db_name, year, franchise_id, manager, points FROM public.facts ORDER BY year"
EXPECTED = [("nyu_ffl", 2025, "fid-1", "Saved Alias", 112.5),
            ("nyu_ffl", 2026, "fid-1", "Saved Alias", 93.25)]


def emit(stage, **fields):
    print(json.dumps({"stage": stage, "elapsed_s": round(time.monotonic() - START, 3), **fields}), flush=True)


def connect(path, read_only=False):
    import duckdb
    config = {"threads": "1", "memory_limit": "256MB",
              "storage_compatibility_version": "v1.4.0",
              "temp_directory": str(Path(path).parent / "spill")}
    if not Path(path).exists() and not read_only:
        conn = duckdb.connect(":memory:", config=config)
        safe_path = Path(path).as_posix().replace("'", "''")
        conn.execute(f"ATTACH '{safe_path}' AS fixture (ROW_GROUP_SIZE 2048, STORAGE_VERSION 'v1.4.0')")
        conn.execute("USE fixture")
        return conn
    return duckdb.connect(str(path), read_only=read_only, config=config)


def assert_fixture(path):
    path = Path(path)
    parent = path.parent
    if (parent.parent.resolve() != Path(tempfile.gettempdir()).resolve()
            or not parent.name.startswith("lh_drop_spike_")
            or path.name != "candidate.duckdb" or path.is_symlink()
            or path.stat().st_nlink != 1 or path.stat().st_size > LIMIT):
        raise ValueError("only a tiny generated experiment fixture is permitted")
    return path


def child(path, mode):
    # The interposer's forwarding lookup needs Python's engine globally visible.
    if sys.platform == "linux":
        sys.setdlopenflags(os.RTLD_NOW | os.RTLD_GLOBAL)
    import duckdb
    path = assert_fixture(path)
    if mode == "verify":
        verify(path)
        return 0
    hook = ctypes.CDLL(None) if mode in {"hook", "crash_commit", "crash_flush", "recover"} else None
    if hook:
        hook.lh_spike_count.restype = ctypes.c_int
        hook.lh_spike_allocations.restype = ctypes.c_int
        hook.lh_spike_arm()
        if mode == "recover":
            hook.lh_spike_checkpoint(1)
    conn = None
    try:
        conn = connect(path)
        emit("connected", mode=mode)
        conn.execute("SET checkpoint_threshold='1GB'")
        conn.execute("PRAGMA disable_checkpoint_on_shutdown")
        if conn.execute(WITNESS).fetchall() != EXPECTED:
            raise ValueError("healthy witness changed before DROP")
        emit("healthy_witness_verified", mode=mode)
        if hook:
            before_unrelated = hook.lh_spike_count()
            conn.execute("CREATE TABLE public.forwarding_witness AS SELECT 9 AS n")
            conn.execute("DROP TABLE public.forwarding_witness")
            conn.execute("CREATE SCHEMA IF NOT EXISTS unrelated")
            conn.execute(f'CREATE TABLE unrelated."{TABLE}" AS SELECT 9 AS n')
            conn.execute(f'DROP TABLE unrelated."{TABLE}"')
            if hook.lh_spike_count() != before_unrelated:
                raise ValueError("unrelated table or schema DROP was intercepted")
            emit("ordinary_drop_forwarded")
        if mode == "locate":
            try:
                conn.execute(f"SELECT column_name FROM pragma_storage_info('public.{TABLE}')").fetchall()
            except duckdb.IOException as exc:
                if "checksum" not in str(exc).lower():
                    raise
                emit("fixture_corruption_located", error=str(exc).splitlines()[0])
                return 0
            raise ValueError("candidate did not affect target metadata")
        present = conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name=?", [TABLE]).fetchall()
        if present:
            if hook:
                before_rollback = hook.lh_spike_count()
                conn.execute("BEGIN TRANSACTION")
                conn.execute(f'DROP TABLE public."{TABLE}"')
                conn.execute("ROLLBACK")
                assert conn.execute(WITNESS).fetchall() == EXPECTED
                assert conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name=?", [TABLE]).fetchall()
                assert hook.lh_spike_count() == before_rollback
                emit("rollback_verified")
            conn.execute("BEGIN TRANSACTION")
            conn.execute(f'DROP TABLE public."{TABLE}"')
            conn.execute("COMMIT")
        elif mode != "recover":
            raise ValueError("aggregate absent before removal experiment")
        if hook:
            hook.lh_spike_disarm()
            if hook.lh_spike_count() != 1 and mode != "recover":
                raise ValueError("removal hook did not intercept exactly one call")
        emit("drop_committed", intercepted=hook.lh_spike_count() if hook else 0)
        if mode == "crash_commit":
            os._exit(23)
        if hook:
            hook.lh_spike_checkpoint(1)
            if mode == "crash_flush":
                hook.lh_spike_crash_flush()
        conn.execute("CHECKPOINT")
        if hook:
            # A second checkpoint retires metadata no longer referenced by
            # the new catalog, before a stock engine is asked to reuse space.
            conn.execute("CHECKPOINT")
            hook.lh_spike_checkpoint(0)
            if hook.lh_spike_allocations() < 1:
                raise ValueError("fresh metadata allocation hook was not exercised")
            emit("fresh_metadata_checkpoints", allocations=hook.lh_spike_allocations())
        conn.close()
        conn = None
        # New process without LD_PRELOAD verifies the actual persisted file.
        emit("checkpoint_finished")
        return 0
    except Exception as exc:
        emit("child_failed", mode=mode, error=str(exc).splitlines()[0])
        return 2
    finally:
        if hook:
            hook.lh_spike_disarm()
            hook.lh_spike_checkpoint(0)
        if conn is not None:
            conn.close()


def run_child(path, mode, library=None):
    env = dict(os.environ)
    env.pop("LD_PRELOAD", None)
    if library:
        env["LD_PRELOAD"] = str(library)
    result = subprocess.run([sys.executable, "-u", __file__, "--child", str(path), "--mode", mode],
                            env=env, capture_output=True, text=True, timeout=6)
    emit("child_result", mode=mode, exit_code=result.returncode,
         stdout=result.stdout[-5000:], stderr=result.stderr[-1000:])
    return result


def verify(path):
    with connect(path, read_only=True) as conn:
        if conn.execute(WITNESS).fetchall() != EXPECTED:
            raise ValueError("healthy facts/aliases changed after reopen")
        if conn.execute("SELECT table_name FROM duckdb_tables() WHERE table_name=?", [TABLE]).fetchall():
            raise ValueError("dropped aggregate still exists after reopen")
    # Exercise an ordinary independent write and a second checkpoint/reopen.
    with connect(path) as conn:
        conn.execute("CREATE OR REPLACE TABLE public.write_witness AS SELECT 7 AS value")
        conn.execute("CHECKPOINT")
    with connect(path, read_only=True) as conn:
        assert conn.execute("SELECT value FROM public.write_witness").fetchall() == [(7,)]
        assert conn.execute(WITNESS).fetchall() == EXPECTED
    emit("stock_engine_reopen_verified", fixture_bytes=path.stat().st_size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--mode", choices=["locate", "ordinary", "hook", "verify", "crash_commit", "crash_flush", "recover"], default="ordinary")
    parser.add_argument("--scenario", choices=["normal", "indexed", "shared"], default="normal")
    parser.add_argument("--fixture-only", action="store_true")
    parser.add_argument("--binding-only", action="store_true")
    args = parser.parse_args()
    timer = threading.Timer(35, lambda: os._exit(124))
    timer.daemon = True
    timer.start()
    try:
        if args.child:
            return child(args.child, args.mode)
        import duckdb
        emit("start", version=duckdb.__version__, scenario=args.scenario, production_access=False)
        if not args.fixture_only and (sys.platform != "linux" or duckdb.__version__ != "1.5.4"):
            raise ValueError("engine experiment requires Linux and production DuckDB 1.5.4")
        with tempfile.TemporaryDirectory(prefix="lh_drop_spike_") as temp:
            folder = Path(temp)
            baseline = folder / "baseline.duckdb"
            candidate = folder / "candidate.duckdb"
            with connect(baseline) as conn:
                conn.execute("CREATE SCHEMA public")
                cols = ", ".join(f"(i//2048)+{i} AS c{i}" for i in range(2 if args.binding_only or args.scenario == "shared" else 64))
                rows = 16 if args.binding_only or args.scenario == "shared" else 262144
                if args.scenario == "indexed":
                    cols += ", i AS row_id"
                conn.execute(f'CREATE TABLE public."{TABLE}" AS SELECT {cols} FROM range({rows}) t(i)')
                if args.scenario == "indexed":
                    conn.execute(f'CREATE UNIQUE INDEX target_idx ON public."{TABLE}" (row_id)')
                conn.execute("CHECKPOINT")
                conn.execute("CREATE TABLE public.facts (db_name VARCHAR, year INTEGER, franchise_id VARCHAR, manager VARCHAR, points DOUBLE)")
                conn.executemany("INSERT INTO public.facts VALUES (?, ?, ?, ?, ?)", EXPECTED)
                conn.execute("CHECKPOINT")
                old_blocks = [r[0] for r in conn.execute("SELECT block_id FROM pragma_metadata_info()").fetchall()]
            if baseline.stat().st_size > LIMIT:
                raise ValueError("fixture exceeded 8 MiB ceiling")
            baseline_hash = hashlib.sha256(baseline.read_bytes()).hexdigest()
            emit("fixture_built", bytes=baseline.stat().st_size, metadata_blocks=old_blocks)
            library = folder / "drop_spike.so"
            if not args.fixture_only:
                subprocess.run(["c++", "-shared", "-fPIC", "-O2", "-Wall", "-Werror",
                                str(Path(__file__).with_suffix(".cpp")), "-ldl", "-o", str(library)],
                               check=True, timeout=5)
                shutil.copyfile(baseline, candidate)
                binding = run_child(candidate, "hook", library)
                if binding.returncode:
                    raise ValueError("engine does not support this interposition; do not use on a volume")
                if run_child(candidate, "verify").returncode:
                    raise ValueError("stock-engine binding verification failed")
                emit("binding_proved", production_repair_authorized=False)
                if args.binding_only:
                    return 0
            if args.scenario == "shared":
                shutil.copyfile(baseline, candidate)
                with candidate.open("r+b") as stream:
                    offset = 12288 + old_blocks[0] * 262144 + 32
                    stream.seek(offset)
                    original = stream.read(1)
                    stream.seek(offset)
                    stream.write(bytes([original[0] ^ 1]))
                damaged_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
                rejected = run_child(candidate, "hook", library)
                if rejected.returncode == 0 or "checksum" not in rejected.stdout.lower():
                    raise ValueError("shared metadata corruption was not rejected")
                if hashlib.sha256(candidate.read_bytes()).hexdigest() != damaged_hash:
                    raise ValueError("rejected shared-metadata fixture changed")
                emit("shared_metadata_rejected_without_write", production_repair_authorized=False)
                return 0
            chosen = None
            for block in old_blocks[:8]:
                # This byte flip is exclusively fault injection in a synthetic file.
                shutil.copyfile(baseline, candidate)
                with candidate.open("r+b") as stream:
                    offset = 12288 + block * 262144 + 32
                    stream.seek(offset)
                    original = stream.read(1)
                    if not original:
                        raise ValueError("metadata offset outside fixture")
                    stream.seek(offset)
                    stream.write(bytes([original[0] ^ 1]))
                result = run_child(candidate, "locate")
                if result.returncode == 0:
                    chosen = candidate.read_bytes()
                    break
            if chosen is None:
                raise ValueError("no representative lazy-metadata fixture found")
            normal = run_child(candidate, "ordinary")
            if normal.returncode == 0 or "checksum" not in normal.stdout.lower():
                raise ValueError("ordinary removal did not reproduce checksum failure")
            emit("baseline_failure_proved")
            if args.fixture_only:
                return 0
            # Restore only the <=8 MiB generated fixture, never a real database.
            candidate.write_bytes(chosen)
            wal = Path(str(candidate) + ".wal")
            if wal.exists():
                wal.unlink()  # synthetic failed-test WAL only, under owned temp folder
            hooked = run_child(candidate, "hook", library)
            if hooked.returncode:
                raise ValueError("experimental removal failed; not safe for a real volume")
            if run_child(candidate, "verify").returncode:
                raise ValueError("stock-engine recovery verification failed")
            if args.scenario == "normal":
                for crash_mode, exit_code in [("crash_commit", 23), ("crash_flush", 24)]:
                    candidate.write_bytes(chosen)
                    for suffix in (".wal", ".checkpoint.wal"):
                        generated_wal = Path(str(candidate) + suffix)
                        if generated_wal.exists():
                            generated_wal.unlink()  # exclusively owned synthetic fixture
                    crashed = run_child(candidate, crash_mode, library)
                    if crashed.returncode != exit_code:
                        raise ValueError(f"{crash_mode} did not reach its injected boundary")
                    if run_child(candidate, "recover", library).returncode:
                        raise ValueError(f"{crash_mode} replay recovery failed")
                    if run_child(candidate, "verify").returncode:
                        raise ValueError(f"{crash_mode} stock-engine verification failed")
                    if run_child(candidate, "recover", library).returncode or run_child(candidate, "verify").returncode:
                        raise ValueError(f"{crash_mode} same-input recovery was not idempotent")
                    emit("crash_recovery_verified", boundary=crash_mode)
            if hashlib.sha256(baseline.read_bytes()).hexdigest() != baseline_hash:
                raise ValueError("baseline changed")
            emit("synthetic_pilot_passed", production_repair_authorized=False)
            return 0
    except Exception as exc:
        emit("blocked", error=str(exc).splitlines()[0])
        return 2
    finally:
        timer.cancel()


if __name__ == "__main__":
    raise SystemExit(main())
