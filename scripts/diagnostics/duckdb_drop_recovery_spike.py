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
REAL_TABLES = tuple("__corrupt_recovery_" + name for name in (
    "homepage_manager_rankings", "matchup_h2h_career", "player_fantasy_season",
    "player_fantasy_season_all", "standings_by_year"))
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
            or parent.is_symlink()
            or parent.resolve().parent != Path(tempfile.gettempdir()).resolve()
            or not parent.name.startswith("lh_drop_spike_")
            or path.name not in {"candidate.duckdb", "___leagues.duckdb"} or path.is_symlink()
            or path.stat().st_nlink != 1 or path.stat().st_size > LIMIT):
        raise ValueError("only a tiny generated experiment fixture is permitted")
    return path


def targets(path):
    return REAL_TABLES if Path(path).name == "___leagues.duckdb" else (TABLE,)


def add_metadata_density(conn):
    columns = ','.join(f'column_{i:03d}_with_a_long_but_ordinary_metadata_name VARCHAR' for i in range(64))
    for i in range(96):
        conn.execute(f'CREATE TABLE public.metadata_density_{i:03d} ({columns})')


def mark_origin_child(path, verify_only=False):
    """Healthy shared-block proof, never a corruption or recovery input."""
    if sys.platform != 'linux':
        raise ValueError('Mark-origin proof requires the stock Linux engine')
    if verify_only and os.environ.get('LD_PRELOAD'):
        raise ValueError('Mark-origin stock proof must not load the helper')
    marker = json.loads((path.parent / 'mark-origin.json').read_text())
    block_id = marker['block_id']
    expected = [(i, i + 100) for i in range(16)]
    if verify_only:
        with connect(path, read_only=True) as conn:
            if conn.execute('SELECT i, value FROM public.mark_survivor ORDER BY i').fetchall() != expected:
                raise ValueError('retained values changed after helper-free reopen')
            if conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name IN ('mark_keep','mark_drop')").fetchall():
                raise ValueError('retired shared tables survived stock reopen')
            if conn.execute('SELECT block_id FROM pragma_metadata_info() WHERE block_id=?', [block_id]).fetchall():
                raise ValueError('retired metadata block survived stock reopen')
        with connect(path) as conn:
            conn.execute('CREATE TABLE public.mark_stock_write AS SELECT 7 AS value')
            conn.execute('CHECKPOINT')
        assert_fixture(path)
        with connect(path, read_only=True) as conn:
            if conn.execute('SELECT value FROM public.mark_stock_write').fetchall() != [(7,)]:
                raise ValueError('helper-free checkpoint lost the stock write')
            if conn.execute('SELECT i, value FROM public.mark_survivor ORDER BY i').fetchall() != expected:
                raise ValueError('helper-free checkpoint changed retained values')
        emit('mark_origin_stock_verified', block_id=block_id, fixture_bytes=path.stat().st_size)
        return 0

    hook = ctypes.CDLL(None)
    hook.lh_spike_avoid_block.argtypes = [ctypes.c_int64]
    hook.lh_spike_test_mark_partial_mask.restype = ctypes.c_uint64
    hook.lh_spike_test_mark_calls.restype = ctypes.c_int
    hook.lh_spike_test_mark_partial_calls.restype = ctypes.c_int
    hook.lh_spike_reserved_masks.restype = ctypes.c_int
    hook.lh_spike_allocations.restype = ctypes.c_int
    hook.lh_spike_avoid_block(block_id)  # Must precede MetadataBlock::Read.
    hook.lh_spike_checkpoint(1)  # Retain the unchanged 128 physical-allocation cap.
    hook.lh_spike_disarm()  # All healthy table drops take the ordinary engine path.

    def reserved_state(conn):
        return conn.execute('SELECT free_list FROM pragma_metadata_info() WHERE block_id=?', [block_id]).fetchall()

    def block_digest():
        assert_fixture(path)
        with path.open('rb') as stream:
            stream.seek(12288 + block_id * 262144)
            raw = stream.read(262144)
        if len(raw) != 262144:
            raise ValueError('shared metadata block outside the tiny fixture')
        return hashlib.sha256(raw).hexdigest()

    try:
        with connect(path) as conn:
            conn.execute('PRAGMA disable_checkpoint_on_shutdown')
            conn.execute("SET checkpoint_threshold='1GB'")
            if hook.lh_spike_reserved_masks() < 1 or reserved_state(conn) != [([],)]:
                raise ValueError('healthy shared block was not reserved on engine Read')
            digest = block_digest()
            if conn.execute('SELECT i, value FROM public.mark_keep ORDER BY i').fetchall() != expected:
                raise ValueError('healthy retained reference is unreadable')
            # Both tables were persisted in the fixture's sole metadata block.
            # Drop one normally, but leave the other's row-group references live.
            conn.execute('DROP TABLE public.mark_drop')
            for index in (1, 2):
                calls = hook.lh_spike_test_mark_calls()
                partial_calls = hook.lh_spike_test_mark_partial_calls()
                conn.execute(f'CREATE TABLE public.mark_churn_{index} AS SELECT {index} AS value')
                conn.execute('CHECKPOINT')  # Only SQL drives the instrumented engine path.
                mask = hook.lh_spike_test_mark_partial_mask()
                if (hook.lh_spike_test_mark_calls() <= calls
                        or hook.lh_spike_test_mark_partial_calls() <= partial_calls
                        or not 0 < mask < (1 << 64) - 1):
                    raise ValueError('real MarkBlocksAsModified did not deliver a targeted partial mask')
                if reserved_state(conn) != [([],)]:
                    raise ValueError('reserved metadata subslots became allocatable')
                if block_digest() != digest:
                    raise ValueError('checkpoint allocated or rewrote reserved metadata subslots')
                if conn.execute('SELECT i, value FROM public.mark_keep ORDER BY i').fetchall() != expected:
                    raise ValueError('checkpoint changed the healthy retained reference')
                emit('mark_origin_partial_mask_verified', index=index, block_id=block_id,
                     incoming_mask=hex(mask), mark_calls=hook.lh_spike_test_mark_calls(),
                     partial_calls=hook.lh_spike_test_mark_partial_calls())
            # Copy the values to fresh storage, then release the final old reference.
            conn.execute('CREATE TABLE public.mark_survivor AS SELECT i,value FROM public.mark_keep')
            conn.execute('DROP TABLE public.mark_keep')
            conn.execute('CHECKPOINT')
            if reserved_state(conn):
                raise ValueError('unreferenced reserved metadata did not retire normally')
            assert_fixture(path)
            emit('mark_origin_retired', block_id=block_id, allocations=hook.lh_spike_allocations())
        return 0
    except Exception as exc:
        emit('mark_origin_failed', error=str(exc).splitlines()[0])
        return 2
    finally:
        hook.lh_spike_checkpoint(0)
        hook.lh_spike_avoid_block(-1)


def prove_mark_origin(library, mutant):
    """Run the same real-engine assertions against a broken and enabled mask."""
    for selected, should_fail in ((mutant, True), (library, False)):
        with tempfile.TemporaryDirectory(prefix='lh_drop_spike_') as temp:
            path = Path(temp) / 'candidate.duckdb'
            with connect(path) as conn:
                conn.execute('CREATE SCHEMA public')
                conn.execute('CREATE TABLE public.mark_keep AS SELECT i, i+100 AS value FROM range(16) t(i)')
                conn.execute('CREATE TABLE public.mark_drop AS SELECT i, i+200 AS value FROM range(16) t(i)')
                conn.execute('CHECKPOINT')
                blocks = conn.execute('SELECT block_id,free_list FROM pragma_metadata_info()').fetchall()
            assert_fixture(path)
            if len(blocks) != 1 or not blocks[0][1]:
                raise ValueError('healthy shared fixture must occupy one partially free metadata block')
            (path.parent / 'mark-origin.json').write_text(json.dumps({'block_id': blocks[0][0]}))
            result = run_child(path, 'mark_origin', selected)
            if should_fail:
                if result.returncode != 2 or 'reserved metadata subslots became allocatable' not in result.stdout:
                    raise ValueError('Mark-path mask mutant did not fail for exposed reserved subslots')
                emit('mark_origin_red_proved')
            else:
                if result.returncode:
                    raise ValueError('Mark-origin shared-reference proof failed')
                if run_child(path, 'mark_verify').returncode:
                    raise ValueError('Mark-origin helper-free stock proof failed')
                emit('mark_origin_behavior_proved', production_repair_authorized=False)


def child(path, mode):
    # The interposer's forwarding lookup needs Python's engine globally visible.
    if sys.platform == "linux":
        sys.setdlopenflags(os.RTLD_NOW | os.RTLD_GLOBAL)
    import duckdb
    path = assert_fixture(path)
    if mode in {'mark_origin', 'mark_verify'}:
        if duckdb.__version__ != '1.5.4':
            raise ValueError('Mark-origin proof requires stock DuckDB 1.5.4')
        return mark_origin_child(path, verify_only=mode == 'mark_verify')
    if mode in {"verify", "verify_wal"}:
        verify(path)
        if mode == "verify_wal":
            with connect(path, read_only=True) as conn:
                assert conn.execute("SELECT value FROM public.wal_fact").fetchall() == [("committed before recovery",)]
            emit("prior_committed_wal_preserved")
        return 0
    hook = ctypes.CDLL(None) if mode in {"hook", "crash_commit", "crash_flush", "recover", "budget", "forbidden"} else None
    if hook:
        hook.lh_spike_count.restype = ctypes.c_int
        hook.lh_spike_allocations.restype = ctypes.c_int
        hook.lh_spike_reserved_masks.restype = ctypes.c_int
        marker = path.parent / 'injected-block.json'
        if marker.exists():
            hook.lh_spike_avoid_block(ctypes.c_int64(json.loads(marker.read_text())['block_id']))
        hook.lh_spike_arm()
        if len(targets(path)) == 5:
            hook.lh_spike_replay()
        if mode == "budget":
            hook.lh_spike_allocation_limit(1)
        if mode == "recover":
            hook.lh_spike_checkpoint(1)
    conn = None
    try:
        conn = connect(path)
        if hook and marker.exists():
            block_id = json.loads(marker.read_text())['block_id']
            registered = bool(conn.execute('SELECT block_id FROM pragma_metadata_info() WHERE block_id=?', [block_id]).fetchall())
            if registered and hook.lh_spike_reserved_masks() < 1:
                raise ValueError('engine Read did not invoke the selective metadata reservation')
            emit('metadata_read_mask_bound', calls=hook.lh_spike_reserved_masks(), registered=registered)
        if hook:
            hook.lh_spike_arm()
        emit("connected", mode=mode)
        conn.execute("SET checkpoint_threshold='1GB'")
        conn.execute("PRAGMA disable_checkpoint_on_shutdown")
        if conn.execute(WITNESS).fetchall() != EXPECTED:
            raise ValueError("healthy witness changed before DROP")
        emit("healthy_witness_verified", mode=mode)
        if mode == 'budget':
            # Force real physical allocations under stock 64-subslot packing;
            # tiny catalogs can legitimately checkpoint without a fresh block.
            add_metadata_density(conn)
        if mode == "seed_wal":
            conn.execute("CREATE TABLE public.wal_fact AS SELECT 'committed before recovery' AS value")
            conn.execute("CREATE TABLE public.ordinary_wal_drop AS SELECT 8 AS n")
            conn.execute("DROP TABLE public.ordinary_wal_drop")
            emit("prior_wal_seeded")
            os._exit(25)
        if mode == "forbidden":
            conn.execute("CREATE TABLE public.must_keep AS SELECT 11 AS n")
            conn.execute("DROP TABLE public.must_keep")
            raise ValueError("real-file helper permitted an unrelated armed DROP")
        if hook and len(targets(path)) == 1:
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
        present = conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name IN (SELECT unnest(?))", [list(targets(path))]).fetchall()
        if present and mode == "recover":
            raise ValueError("committed aggregate DROP was not replayed; do not repeat it")
        if present:
            if hook:
                before_rollback = hook.lh_spike_count()
                conn.execute("BEGIN TRANSACTION")
                for table in targets(path):
                    conn.execute(f'DROP TABLE public."{table}"')
                conn.execute("ROLLBACK")
                assert conn.execute(WITNESS).fetchall() == EXPECTED
                assert conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name=?", [TABLE]).fetchall()
                assert hook.lh_spike_count() == before_rollback
                emit("rollback_verified")
            if len(targets(path)) == 5:
                from duckdb_recovery_adapter import remove_quarantined
                assert remove_quarantined(conn) == 5
            else:
                conn.execute("BEGIN TRANSACTION")
                for table in targets(path):
                    conn.execute(f'DROP TABLE public."{table}"')
                conn.execute("COMMIT")
        elif mode != "recover":
            raise ValueError("aggregate absent before removal experiment")
        if hook:
            hook.lh_spike_disarm()
            if hook.lh_spike_count() != len(targets(path)) and mode != "recover":
                raise ValueError(f"removal scope not bound: entries={hook.lh_spike_entry_calls()}, intercepted={hook.lh_spike_count()}")
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
            if hook.lh_spike_allocations() < 1 and mode != "recover" and len(targets(path)) == 5:
                raise ValueError("fresh metadata allocation hook was not exercised")
            emit("fresh_metadata_checkpoints", allocations=hook.lh_spike_allocations())
            emit('selective_metadata_reservation', calls=hook.lh_spike_reserved_masks())
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


def fixture_files(path):
    return {suffix: hashlib.sha256(file.read_bytes()).hexdigest()
            for suffix in ("", ".wal", ".wal.checkpoint")
            if (file := Path(str(path) + suffix)).exists()}


def verify(path):
    with connect(path, read_only=True) as conn:
        if conn.execute(WITNESS).fetchall() != EXPECTED:
            raise ValueError("healthy facts/aliases changed after reopen")
        if conn.execute("SELECT table_name FROM duckdb_tables() WHERE table_name IN (SELECT unnest(?))", [list(targets(path))]).fetchall():
            raise ValueError("dropped aggregate still exists after reopen")
        if len(targets(path)) == 5:
            for table in REAL_TABLES:
                canonical = table.removeprefix("__corrupt_recovery_")
                assert conn.execute(f'SELECT year, manager, points FROM public."{canonical}" ORDER BY year').fetchall() == [
                    (2025, "Saved Alias", 112.5), (2026, "Saved Alias", 93.25)]
    # Exercise an ordinary independent write and a second checkpoint/reopen.
    with connect(path) as conn:
        conn.execute("CREATE OR REPLACE TABLE public.write_witness AS SELECT 7 AS value")
        conn.execute("CHECKPOINT")
    with connect(path, read_only=True) as conn:
        assert conn.execute("SELECT value FROM public.write_witness").fetchall() == [(7,)]
        assert conn.execute(WITNESS).fetchall() == EXPECTED
        marker = path.parent / "injected-block.json"
        if marker.exists():
            block_id = json.loads(marker.read_text())["block_id"]
            registered = conn.execute("SELECT block_id FROM pragma_metadata_info() WHERE block_id=?", [block_id]).fetchall()
            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
            from fly_duckdb_block_probe import probe
            block = probe(path, 12288 + block_id * 262144)
            if registered and not block["checksum_valid"]:
                raise ValueError("damaged block remains registered as reusable metadata after stock reopen")
            emit("damaged_metadata_reference_retired", block_id=block_id,
                 registered=bool(registered), checksum_valid=block["checksum_valid"])
    emit("stock_engine_reopen_verified", fixture_bytes=path.stat().st_size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--mode", choices=["locate", "ordinary", "hook", "verify", "crash_commit", "crash_flush", "recover", "budget", "forbidden", "seed_wal", "verify_wal", "mark_origin", "mark_verify"], default="ordinary")
    parser.add_argument("--scenario", choices=["normal", "indexed", "shared", "budget", "real_scope"], default="normal")
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
            candidate = folder / ("___leagues.duckdb" if args.scenario == "real_scope" else "candidate.duckdb")
            with connect(baseline) as conn:
                conn.execute("CREATE SCHEMA public")
                cols = ", ".join(f"(i//2048)+{i} AS c{i}" for i in range(2 if args.binding_only or args.scenario == "shared" else 64))
                rows = 16 if args.binding_only or args.scenario == "shared" else 262144
                if args.scenario == "indexed":
                    cols += ", i AS row_id"
                conn.execute(f'CREATE TABLE public."{TABLE}" AS SELECT {cols} FROM range({rows}) t(i)')
                if args.scenario == "real_scope":
                    for table in REAL_TABLES:
                        if table != TABLE:
                            conn.execute(f'CREATE TABLE public."{table}" AS SELECT 7 AS obsolete')
                if args.scenario == "indexed":
                    conn.execute(f'CREATE UNIQUE INDEX target_idx ON public."{TABLE}" (row_id)')
                conn.execute("CHECKPOINT")
                conn.execute("CREATE TABLE public.facts (db_name VARCHAR, year INTEGER, franchise_id VARCHAR, manager VARCHAR, points DOUBLE)")
                conn.executemany("INSERT INTO public.facts VALUES (?, ?, ?, ?, ?)", EXPECTED)
                if args.scenario == "real_scope":
                    for table in REAL_TABLES:
                        canonical = table.removeprefix("__corrupt_recovery_")
                        conn.execute(f'CREATE TABLE public."{canonical}" AS SELECT * FROM public.facts')
                    # A real catalog contains many metadata handles. Reusing
                    # healthy sub-blocks must not become one 256KiB allocation
                    # per 4KiB handle just to avoid one damaged physical block.
                    add_metadata_density(conn)
                conn.execute("CHECKPOINT")
                old_blocks = [r[0] for r in conn.execute("SELECT block_id FROM pragma_metadata_info()").fetchall()]
            if baseline.stat().st_size > LIMIT:
                raise ValueError("fixture exceeded 8 MiB ceiling")
            baseline_hash = hashlib.sha256(baseline.read_bytes()).hexdigest()
            emit("fixture_built", bytes=baseline.stat().st_size, metadata_blocks=old_blocks)
            library = folder / "drop_spike.so"
            if not args.fixture_only:
                subprocess.run(["c++", "-shared", "-fPIC", "-O0", "-Wall", "-Werror",
                                *(["-DLH_REAL_FILE"] if args.scenario == "real_scope" else []),
                                *(["-DLH_TEST_MARK_ORIGIN"] if args.scenario == "shared" else []),
                                str(Path(__file__).with_suffix(".cpp")), "-ldl", "-o", str(library)],
                               check=True, timeout=5)
                shutil.copyfile(baseline, candidate)
                if args.scenario == "real_scope":
                    seeded = run_child(candidate, "seed_wal")
                    if seeded.returncode != 25:
                        raise ValueError("prior committed WAL fixture was not created")
                binding = run_child(candidate, "hook", library)
                if binding.returncode:
                    raise ValueError("engine does not support this interposition; do not use on a volume")
                if run_child(candidate, "verify_wal" if args.scenario == "real_scope" else "verify").returncode:
                    raise ValueError("stock-engine binding verification failed")
                emit("binding_proved", production_repair_authorized=False)
                if args.binding_only:
                    return 0
            if args.scenario == "budget":
                shutil.copyfile(baseline, candidate)
                bounded = run_child(candidate, "budget", library)
                if bounded.returncode != 97:
                    raise ValueError("allocation ceiling did not fail closed at the tested limit")
                if run_child(candidate, "verify").returncode:
                    raise ValueError("allocation-ceiling exit did not preserve stock-engine recovery")
                emit("allocation_ceiling_verified", metadata_block_budget=1)
                return 0
            if args.scenario == "shared":
                mutant = folder / 'mark_mask_mutant.so'
                subprocess.run(['c++', '-shared', '-fPIC', '-O0', '-Wall', '-Werror',
                                '-DLH_TEST_MARK_ORIGIN', '-DLH_TEST_SKIP_MARK_RESERVATION',
                                str(Path(__file__).with_suffix('.cpp')), '-ldl', '-o', str(mutant)],
                               check=True, timeout=5)
                prove_mark_origin(library, mutant)
                shutil.copyfile(baseline, candidate)
                with candidate.open("r+b") as stream:
                    offset = 12288 + old_blocks[0] * 262144 + 32
                    stream.seek(offset)
                    original = stream.read(1)
                    stream.seek(offset)
                    stream.write(bytes([original[0] ^ 1]))
                damaged_files = fixture_files(candidate)
                (folder / 'injected-block.json').write_text(json.dumps({'block_id': old_blocks[0]}))
                rejected = run_child(candidate, "hook", library)
                if rejected.returncode == 0 or "checksum" not in rejected.stdout.lower():
                    raise ValueError("shared metadata corruption was not rejected")
                if fixture_files(candidate) != damaged_files:
                    raise ValueError("rejected shared-metadata fixture or WAL changed")
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
                    (folder / "injected-block.json").write_text(json.dumps({"block_id": block}))
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
            if args.scenario in {"normal", "real_scope"}:
                for crash_mode, exit_code in [("crash_commit", 23), ("crash_flush", 24)]:
                    candidate.write_bytes(chosen)
                    for suffix in (".wal", ".wal.checkpoint"):
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
            if args.scenario == "real_scope":
                forbidden = run_child(candidate, "forbidden", library)
                if forbidden.returncode != 99:
                    raise ValueError("real-file mode did not refuse an unrelated armed DROP")
                emit("real_catalog_five_targets_verified", target_count=5)
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
