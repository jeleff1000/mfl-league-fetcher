"""Guarded offline five-object recovery; isolated-volume CLI only.

All mutation orchestration must first bind these checks to the isolated volume,
exact engine artifact, retained WAL and an externally enforced deadline.
"""
from __future__ import annotations

import hashlib
import argparse
import base64
import ctypes
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import threading
import time

CANONICAL = ("homepage_manager_rankings", "matchup_h2h_career", "player_fantasy_season",
             "player_fantasy_season_all", "standings_by_year")
QUARANTINED = tuple("__corrupt_recovery_" + name for name in CANONICAL)
RECOVERY_VOLUME = "vol_4919j2m0wzg0xw5r"
STAGE_LIMITS = {"inspect": 5, "preserve": 10, "replay": 10, "remove": 5, "verify": 15}


def charge_phase(spent, name, elapsed):
    spent[name] += elapsed
    if spent[name] > STAGE_LIMITS[name]:
        raise ValueError(f'{name} phase budget exceeded')


def protect_parent(expected_pid):
    """Arm Linux parent-death termination before opening any engine file."""
    if sys.platform != 'linux' or expected_pid < 2:
        raise ValueError('a Linux supervisor is required')
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    prctl.restype = ctypes.c_int
    if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # PR_SET_PDEATHSIG
        raise OSError(ctypes.get_errno(), 'cannot arm parent-death signal')
    if os.getppid() != expected_pid:
        os.kill(os.getpid(), signal.SIGKILL)  # Parent exited before prctl: close race.


def run_stage(stage, command, *, deadline, env=None, transitions=()):
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
    protocol_error = threading.Event()
    phase_lock = threading.Lock()
    phase = {"name": stage, "since": started, "index": 0, "mutating": stage in {'remove', 'verify'}}
    spent = {name: 0.0 for name in STAGE_LIMITS}

    def phase_event(line):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            return
        if not isinstance(event, dict) or event.get('event') != 'phase':
            return
        with phase_lock:
            index, next_phase = phase['index'], event.get('stage')
            if index >= len(transitions) or next_phase != transitions[index] or next_phase not in STAGE_LIMITS:
                protocol_error.set()
                stop_child()
                return
            now = time.monotonic()
            try:
                charge_phase(spent, phase['name'], now - phase['since'])
                if time.time() >= deadline:
                    raise ValueError('overall deadline exceeded')
            except ValueError:
                protocol_error.set()
                stop_child()
                return
            phase.update(name=next_phase, since=now, index=index+1,
                         mutating=phase['mutating'] or next_phase in {'replay', 'remove', 'verify'})
            print(json.dumps({'event': 'child_phase', 'stage': next_phase,
                              'elapsed_s': round(now-started, 3)}), flush=True)

    def stop_child():
        try:
            if os.name == "posix":
                os.killpg(child.pid, signal.SIGKILL)
            else:
                child.kill()
        except ProcessLookupError:
            pass

    def drain(name, pipe):
        pending = b''
        with pipe:
            while chunk := pipe.read1(4096):
                free = 65536 - len(captured[name])
                captured[name].extend(chunk[:free])
                if len(chunk) > free:
                    limited.set()
                    stop_child()
                    return
                if name == 'stdout' and transitions:
                    pending += chunk
                    while b'\n' in pending:
                        line, pending = pending.split(b'\n', 1)
                        phase_event(line)

    readers = [threading.Thread(target=drain, args=(name, getattr(child, name)), daemon=True)
               for name in captured]
    for reader in readers:
        reader.start()
    try:
        while child.poll() is None:
            with phase_lock:
                budget = STAGE_LIMITS[phase['name']] - spent[phase['name']] - (time.monotonic() - phase['since'])
            budget = min(budget, deadline-time.time())
            if budget <= 0:
                raise subprocess.TimeoutExpired(command, remaining)
            try:
                child.wait(timeout=min(0.02, budget))
            except subprocess.TimeoutExpired:
                continue
        code = child.returncode
    except subprocess.TimeoutExpired:
        stop_child()
        child.wait(timeout=1)
        code = 124
    finally:
        for reader in readers:
            reader.join(timeout=1)
    if limited.is_set():
        code = 125
    if protocol_error.is_set():
        code = 126
    if code == 0 and transitions and phase['index'] != len(transitions):
        code = 126
        protocol_error.set()
    if code == 0:
        try:
            charge_phase(spent, phase['name'], time.monotonic()-phase['since'])
            if time.time() >= deadline:
                raise ValueError('overall deadline exceeded before successful exit')
        except ValueError:
            code = 124
    stdout, stderr = (captured[name].decode("utf-8", errors="replace") for name in ("stdout", "stderr"))
    outcome = "PASS" if code == 0 else ("UNKNOWN" if phase['mutating'] else "FAILED")
    result = {"stage": stage, "outcome": outcome, "exit_code": code,
              "elapsed_s": round(time.monotonic() - started, 3),
              "stdout": stdout, "stderr": stderr, "output_limited": limited.is_set(),
              "last_phase": phase['name'], "protocol_error": protocol_error.is_set()}
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
        # Year must precede display names: alphabetical extremes alone can
        # silently miss every old season when names change between seasons.
        ordered = sorted((row[0] for row in columns), key=lambda name: (name not in {"year", "week"}, name != "year", name))
        samples = []
        for direction in ("ASC", "DESC"):
            ordering = ', '.join(f'{_quote(name)} {direction} NULLS LAST' for name in ordered)
            rows = conn.execute(f'SELECT {names} FROM public.{_quote(table)} WHERE db_name=? ORDER BY {ordering} LIMIT 8', [db_name]).fetchall()
            samples.append(rows)
        if not samples[0]:
            raise ValueError(f"witness {table} has no rows for the selected league")
        payload = json.dumps({"schema": columns, "values": samples}, default=str, separators=(",", ":"))
        result[table] = {"rows": len(samples[0]), "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    # Only public user preferences, never credentials or raw league_context.
    for table, wanted in (("league_context", ("manager_name_overrides_json", "franchise_merges_json")),
                          ("manager_overrides", None)):
        columns = conn.execute("""SELECT column_name FROM duckdb_columns()
            WHERE database_name=current_database() AND schema_name='public' AND table_name=?
            ORDER BY column_index""", [table]).fetchall()
        available = [r[0] for r in columns]
        if not available:
            result[table] = {"absent": True}
            continue
        if "db_name" not in available:
            raise ValueError(f"preference witness {table} missing league identity")
        selected = available if wanted is None else [name for name in wanted if name in available]
        if not selected:
            result[table] = {"columns": []}
            continue
        rows = conn.execute(f'SELECT {", ".join(_quote(n) for n in selected)} FROM public.{_quote(table)} WHERE db_name=? ORDER BY ALL LIMIT 257', [db_name]).fetchall()
        if len(rows) > 256:
            raise ValueError("preference witness exceeds 256-row ceiling")
        payload = json.dumps({"columns": selected, "rows": rows}, default=str, separators=(",", ":"))
        result[table] = {"rows": len(rows), "sha256": hashlib.sha256(payload.encode()).hexdigest()}
    return result


def compare_witness(before, after):
    if before != after:
        changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
        raise ValueError("preservation witness changed: " + ", ".join(changed))


def object_inventory(conn):
    return conn.execute("""SELECT database_name, schema_name, table_name FROM duckdb_tables()
        WHERE database_name=current_database() AND schema_name='public'
        AND table_name IN (SELECT unnest(?)) ORDER BY table_name""",
        [list(CANONICAL + QUARANTINED)]).fetchall()


def remove_quarantined(conn):
    """One transaction, exact five-name scope; partial absence is not a retry."""
    rows = object_inventory(conn)
    names = {row[2] for row in rows}
    if names == set(CANONICAL):
        return 0  # Only a complete prior removal can be idempotently verified.
    if names != set(CANONICAL + QUARANTINED):
        raise ValueError("partial object set: reconcile before any removal")
    validate_objects(rows)
    conn.execute('BEGIN TRANSACTION')
    try:
        for name in QUARANTINED:
            conn.execute(f'DROP TABLE public.{_quote(name)}')
        conn.execute('COMMIT')
    except BaseException:
        # Caller records UNKNOWN if it cannot distinguish commit from failure.
        # Never issue another DROP in this exception path.
        try:
            conn.execute('ROLLBACK')
        except Exception:
            pass
        raise
    return len(QUARANTINED)


def verify_stock(path, db_name, before):
    """Run in a fresh helper-free child. Ordinary write leaves no user rows."""
    if os.environ.get('LD_PRELOAD'):
        raise ValueError('stock verification must not load a recovery helper')
    import duckdb
    config = {'threads': '1', 'memory_limit': '1536MB', 'temp_directory': ''}
    with duckdb.connect(str(path), config=config) as conn:
        conn.execute('PRAGMA disable_checkpoint_on_shutdown')
        if {r[2] for r in object_inventory(conn)} != set(CANONICAL):
            raise ValueError('quarantined objects remain or healthy replacements are missing')
        compare_witness(before, capture_witness(conn, db_name))
        conn.execute('CREATE TABLE public.__lh_recovery_write_probe (value INTEGER)')
        conn.execute('INSERT INTO public.__lh_recovery_write_probe VALUES (7)')
        conn.execute('CHECKPOINT')
    with duckdb.connect(str(path), config=config) as conn:
        conn.execute('PRAGMA disable_checkpoint_on_shutdown')
        if conn.execute('SELECT value FROM public.__lh_recovery_write_probe').fetchall() != [(7,)]:
            raise ValueError('ordinary stock write did not survive reopen')
        conn.execute('DROP TABLE public.__lh_recovery_write_probe')
        conn.execute('CHECKPOINT')
        compare_witness(before, capture_witness(conn, db_name))


def write_receipt(path, payload):
    """Create-only durable small evidence. Never replace an earlier attempt."""
    data = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    if len(data) > 65536:
        raise ValueError('receipt exceeds 64 KiB ceiling')
    with Path(path).open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    if os.name == 'posix':
        fd = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def preflight_identity(encoded, runtime_machine_id):
    if len(encoded) > 16384:
        raise ValueError('inventory exceeds bounded identity receipt')
    data = json.loads(base64.b64decode(encoded, validate=True))
    path = Path('/data/___leagues.duckdb')
    validate_inventory(data['machine'], data['volume'], runtime_machine_id, path)
    return validate_file_binding(path, path.parent)


def phase(name):
    print(json.dumps({'event': 'phase', 'stage': name}), flush=True)


def recovery_child(args):
    """Never called in the web server; parent enforces every stage deadline."""
    if sys.platform != 'linux':
        raise ValueError('isolated recovery requires the verified Linux runtime')
    sys.setdlopenflags(os.RTLD_NOW | os.RTLD_GLOBAL)
    from fly_table_storage_pilot import engine_identity, inventory_connect_config, file_inventory
    from fly_duckdb_block_probe import probe
    import duckdb

    path = Path('/data/___leagues.duckdb')
    binding = preflight_identity(args.inventory_base64, os.environ.get('FLY_MACHINE_ID', ''))
    validate_engine(engine_identity())
    block = probe(path, 90714112)
    validate_block(block)
    if (binding['size'] != 15555375104 or binding['mtime_ns'] != 1789662410048242836
            or block['main_header_sha256'] != '1b47d141ed345a3a89371b6caffe8dc76db21a093b6438c444d22da461c01878'):
        raise ValueError('isolated source changed: reconcile, do not start another removal')
    library = Path('/tmp/lh_five_drop.so')
    if (os.environ.get('LD_PRELOAD') != str(library) or library.is_symlink()
            or library.stat().st_size > 1024 * 1024
            or hashlib.sha256(library.read_bytes()).hexdigest() != args.helper_sha256):
        raise ValueError('loaded helper does not match the bounded reviewed artifact')
    hook = ctypes.CDLL(None)
    hook.lh_spike_count.restype = ctypes.c_int
    hook.lh_spike_allocations.restype = ctypes.c_int
    hook.lh_spike_disarm()
    files = file_inventory(path)
    if ('.wal.checkpoint' in files or files.get('.wal', {}).get('size') != 45516621
            or files['.wal']['mtime_ns'] != 1789662378556231033):
        raise ValueError('retained WAL identity changed; reconcile before replay')

    phase('preserve')
    folder = Path('/data') / ('recovery_five_' + args.receipt_id)
    folder.mkdir()  # Create-only; an interrupted attempt is never overwritten.
    wal = preserve_wal(Path(str(path) + '.wal'), folder / 'original.wal')
    if wal['sha256'] != 'a4f7a2a20afdf2dc1cc218509c1f4052bf6f4df37924768fef518e8c53dace1f':
        raise ValueError('committed WAL differs from the retained first-attempt receipt')
    write_receipt(folder / 'input.json', {'binding': binding, 'files': files, 'wal': wal,
                                        'inventory_base64': args.inventory_base64,
                                        'helper_sha256': args.helper_sha256, 'db_name': args.db_name})
    if preflight_identity(args.inventory_base64, os.environ.get('FLY_MACHINE_ID', '')) != binding:
        raise ValueError('database identity changed while preserving WAL')
    print(json.dumps({'event': 'wal_preserved', **wal}), flush=True)

    phase('replay')
    hook.lh_spike_replay()
    hook.lh_spike_checkpoint(1)
    replay_started = time.monotonic()
    # Read-write replay can flush committed row groups normally; read-only
    # replay could not fit them in memory. WAL remains present for the engine.
    conn = duckdb.connect(str(path), config={**inventory_connect_config(), 'checkpoint_threshold': '1GB'})
    conn.execute('PRAGMA disable_checkpoint_on_shutdown')
    print(json.dumps({'event': 'wal_replayed', 'elapsed_s': round(time.monotonic()-replay_started, 3),
                      'wal_source_bytes': wal['bytes'], 'metadata_blocks': hook.lh_spike_allocations()}), flush=True)
    phase('preserve')
    validate_objects(object_inventory(conn))
    before = capture_witness(conn, args.db_name)
    write_receipt(folder / 'before.json', before)

    phase('remove')
    hook.lh_spike_arm()
    prior = hook.lh_spike_count()
    removed = remove_quarantined(conn)
    hook.lh_spike_disarm()
    if removed != 5 or hook.lh_spike_count() - prior != 5:
        raise ValueError('unexpected native removal count; outcome UNKNOWN')
    print(json.dumps({'event': 'commit_returned', 'removed': removed}), flush=True)

    phase('verify')
    conn.execute('CHECKPOINT')
    conn.execute('CHECKPOINT')
    conn.close()
    hook.lh_spike_checkpoint(0)
    print(json.dumps({'event': 'checkpoint_returned', 'metadata_blocks': hook.lh_spike_allocations(),
                      'metadata_bytes_ceiling': 32*1024*1024}), flush=True)
    env = dict(os.environ)
    env.pop('LD_PRELOAD', None)
    env['LH_RECOVERY_SUPERVISOR_PID'] = str(os.getpid())
    # This process is a member of the parent's killed process group. Its open,
    # checks, write, checkpoint and reopen share the SAME 15s verify budget.
    subprocess.run([sys.executable, '-u', __file__, '--stock-child', str(folder),
                    '--db-name', args.db_name, '--deadline', str(args.deadline)], env=env, check=True,
                   timeout=max(.001, min(15, args.deadline-time.time())))
    write_receipt(folder / 'completed.json', {'removed': removed, 'stock_verified': True,
                                             'metadata_blocks': hook.lh_spike_allocations()})
    print(json.dumps({'event': 'isolated_recovery_verified', 'receipt': folder.name}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory-base64')
    parser.add_argument('--receipt-id')
    parser.add_argument('--helper-sha256')
    parser.add_argument('--db-name', required=True)
    parser.add_argument('--deadline', type=float)
    parser.add_argument('--recovery-child', action='store_true')
    parser.add_argument('--stock-child')
    args = parser.parse_args()
    if args.recovery_child or args.stock_child:
        protect_parent(int(os.environ.get('LH_RECOVERY_SUPERVISOR_PID', '0')))
    if not re.fullmatch(r'[a-z0-9_]+', args.db_name):
        raise ValueError('invalid witness league')
    if args.stock_child:
        if not args.deadline or not 0 < args.deadline-time.time() <= 40:
            raise ValueError('stock verification requires the original bounded deadline')
        folder = Path(args.stock_child)
        if (folder.parent != Path('/data') or folder.is_symlink()
                or not re.fullmatch(r'recovery_five_[0-9]+_[0-9]+', folder.name)):
            raise ValueError('stock witness must be the isolated receipt folder')
        if os.environ.get('FLY_MACHINE_ID') == '1781e011b69068':
            raise ValueError('production stock pilot forbidden')
        manifest = json.loads((folder / 'input.json').read_text())
        binding = preflight_identity(manifest['inventory_base64'], os.environ.get('FLY_MACHINE_ID', ''))
        if any(binding[k] != manifest['binding'][k] for k in ('device', 'inode')):
            raise ValueError('stock verification database inode changed')
        if manifest['db_name'] != args.db_name:
            raise ValueError('stock witness league identity mismatch')
        verify_stock('/data/___leagues.duckdb', args.db_name, json.loads((folder / 'before.json').read_text()))
        from fly_duckdb_block_probe import probe
        import duckdb
        with duckdb.connect('/data/___leagues.duckdb', read_only=True,
                            config={'threads': '1', 'memory_limit': '1536MB', 'temp_directory': ''}) as conn:
            registered = bool(conn.execute('SELECT block_id FROM pragma_metadata_info() WHERE block_id=346').fetchall())
        if registered and not probe('/data/___leagues.duckdb', 90714112)['checksum_valid']:
            raise ValueError('damaged metadata remains eligible for stock reuse')
        print(json.dumps({'event': 'stock_reopen_write_verified', 'old_block_registered': registered}), flush=True)
        return 0
    if (not args.deadline or not 0 < args.deadline-time.time() <= 40
            or not re.fullmatch(r'[0-9]+_[0-9]+', args.receipt_id or '')
            or not re.fullmatch(r'[0-9a-f]{64}', args.helper_sha256 or '')):
        raise ValueError('exact receipt, helper fingerprint and bounded deadline required')
    if args.recovery_child:
        recovery_child(args)
        return 0
    env = dict(os.environ)
    env['LD_PRELOAD'] = '/tmp/lh_five_drop.so'
    env['LH_RECOVERY_SUPERVISOR_PID'] = str(os.getpid())
    result = run_stage('inspect', [sys.executable, '-u', __file__, *sys.argv[1:], '--recovery-child'],
                       deadline=args.deadline, env=env,
                       transitions=('preserve', 'replay', 'preserve', 'remove', 'verify'))
    return result['exit_code'] if result['exit_code'] >= 0 else 128-result['exit_code']


if __name__ == '__main__':
    try:
        exit_code = main()
    except BaseException as exc:
        print(json.dumps({'event': 'adapter_failed', 'error': type(exc).__name__ + ': ' + str(exc)[:1000],
                          'outcome': 'UNKNOWN' if '--recovery-child' in sys.argv else 'FAILED'}), flush=True)
        exit_code = 2
    # Never checkpoint implicitly during exception-path destructor cleanup.
    os._exit(exit_code)
