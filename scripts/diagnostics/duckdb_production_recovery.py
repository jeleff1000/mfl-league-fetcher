"""One bounded offline production application of proof35387584221.

Not an import path. No database copy, fixture seeding, checksum suppression,
automatic retries, unconditional restart, or removal outside the five old names.
"""
import argparse
import base64
import copy
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

import duckdb_recovery_adapter as a

MACHINE = '1781e011b69068'
VOLUME = 'vol_rkg7mmd17llez224'
IMAGE = 'registry.fly.io/league-history-duckdb:deployment-01M2S3CCAQK5KYEB807DK6C33Z'
PATH = Path('/data/___leagues.duckdb')
CONFIG = {'threads': '4', 'memory_limit': '6GB', 'temp_directory': '',
          'max_vacuum_tasks': '0', 'checkpoint_threshold': '2GB'}


def event(name, **fields):
    print(json.dumps({'event': name, **fields}), flush=True)


def maintenance_config(machine, files):
    config = copy.deepcopy(machine['config'])
    if (machine['id'] != MACHINE or config['image'] != IMAGE
            or len(config.get('mounts', [])) != 1
            or config['mounts'][0].get('volume') != VOLUME
            or config['mounts'][0].get('path') != '/data'):
        raise ValueError('production target/image/mount changed; no stop authorized')
    config['services'] = []
    config['checks'] = {}
    config['restart'] = {'policy': 'no'}
    # Confirmed host admission error35389697018: eight CPUs unavailable.
    # Fly's shared4CPU tier admits at most8GiB; reserve2GiB outside DuckDB.
    config['guest']['cpus'] = 4
    config['guest']['memory_mb'] = 8192
    config['init'] = {'swap_size_mb': config.get('init', {}).get('swap_size_mb', 0),
                      'exec': ['/bin/sleep', '600']}
    config['files'] = list(config.get('files') or []) + files
    return config


def require_verified(result):
    result = dict(result)
    signal = result.pop('exit_signal', 0)
    if type(signal) is not int or signal != 0:
        raise ValueError('remote process terminated by signal; outcome UNKNOWN')
    if a.emit_machine_exec_result(result, 'production_recovery_verified') != 0:
        raise ValueError('remote repair failed; keep maintenance, do not restart')


def file_input(path, guest_path):
    raw = Path(path).read_bytes()
    if len(raw) > 1024 * 1024:
        raise ValueError('repair input exceeds 1MiB')
    return {'guest_path': guest_path, 'raw_value': base64.b64encode(raw).decode()}


def production_binding():
    if os.environ.get('FLY_MACHINE_ID') != MACHINE:
        raise ValueError('production repair is bound to the original machine')
    if Path('/proc/1/cmdline').read_bytes().split(b'\0')[:2] != [b'/bin/sleep', b'600']:
        # Fly init is PID1; the service process is a child. Check all command
        # lines for the exact maintenance child, and refuse any live server.
        commands = [p.read_bytes().split(b'\0') for p in Path('/proc').glob('[0-9]*/cmdline')
                    if p.exists()]
        if not any(c[:2] == [b'/bin/sleep', b'600'] for c in commands):
            raise ValueError('maintenance entrypoint not running')
        if any(any(b'uvicorn' in arg for arg in c) for c in commands):
            raise ValueError('server writer still running')
    for suffix in ('.prev', '.wal.checkpoint', '.wal.recovery'):
        if Path(str(PATH) + suffix).exists():
            raise ValueError('pending startup recovery file: ' + suffix)
    return a.validate_file_binding(PATH, PATH.parent)


def stock(args, folder):
    import duckdb
    from fly_duckdb_block_probe import probe
    production_binding()
    a.verify_stock(PATH, args.db_name, a.read_receipt(folder / 'before.json'),
                   receipt_id=folder.name, config=CONFIG)
    with duckdb.connect(str(PATH), read_only=True, config=CONFIG) as conn:
        registered = bool(conn.execute('SELECT block_id FROM pragma_metadata_info() WHERE block_id=346').fetchall())
    if registered and not probe(PATH, 90714112)['checksum_valid']:
        raise ValueError('damaged metadata still registered for allocation')
    a.write_receipt(folder / 'stock.json', {'verified': True, 'old_block_registered': registered})
    event('stock_reopen_write_verified', old_block_registered=registered)


def repair(args, folder):
    sys.setdlopenflags(os.RTLD_NOW | os.RTLD_GLOBAL)
    from fly_table_storage_pilot import engine_identity
    from fly_duckdb_block_probe import probe
    import duckdb

    binding = production_binding()
    a.validate_engine(engine_identity())
    block = probe(PATH, 90714112)
    a.validate_block(block)
    library = Path('/tmp/lh_five_drop.so')
    if (os.environ.get('LD_PRELOAD') != str(library) or library.is_symlink()
            or library.stat().st_size > 1024*1024
            or hashlib.sha256(library.read_bytes()).hexdigest() != args.helper_sha256):
        raise ValueError('production helper artifact mismatch')
    hook = ctypes.CDLL(None)
    hook.lh_spike_count.restype = ctypes.c_int
    hook.lh_spike_allocations.restype = ctypes.c_int
    hook.lh_spike_reserved_masks.restype = ctypes.c_int
    hook.lh_spike_disarm()
    a.phase('preserve')
    wal = a.preserve_wal(Path(str(PATH) + '.wal'), folder / 'original.wal', max_bytes=1024**3)
    a.write_receipt(folder / 'input.json', {'binding': binding, 'block': block, 'wal': wal,
                                          'helper_sha256': args.helper_sha256, 'db_name': args.db_name})
    if production_binding() != binding:
        raise ValueError('production file changed during WAL preservation')
    event('wal_preserved', **wal)
    a.phase('replay')
    hook.lh_spike_replay()
    hook.lh_spike_avoid_block(ctypes.c_int64(346))
    hook.lh_spike_checkpoint(1)
    started = time.monotonic()
    conn = duckdb.connect(str(PATH), config=CONFIG)
    conn.execute('PRAGMA disable_checkpoint_on_shutdown')
    if hook.lh_spike_reserved_masks() < 1:
        raise ValueError('exact damaged metadata block was not reserved')
    event('wal_replayed', seconds=round(time.monotonic()-started, 3))
    # This first production attempt requires ALL old objects. Already absent
    # or partial objects need an earlier production receipt, not a new baseline.
    a.validate_objects(a.object_inventory(conn))
    a.phase('preserve')
    before = a.capture_witness(conn, args.db_name)
    a.write_receipt(folder / 'before.json', before)
    a.phase('remove')
    prior = hook.lh_spike_count()
    removed = a.remove_quarantined(conn, prepare=hook.lh_spike_arm)
    hook.lh_spike_disarm()
    if removed != 5 or hook.lh_spike_count()-prior != 5:
        raise ValueError('unexpected exact-object removal count')
    event('commit_returned', removed=removed)
    a.phase('verify')
    stopped = threading.Event()
    observer = threading.Thread(target=a.report_checkpoint_progress, args=(hook, stopped), daemon=True)
    observer.start()
    try:
        for index in (1, 2):
            event('checkpoint_start', index=index)
            conn.execute('CHECKPOINT')
            event('checkpoint_end', index=index)
    finally:
        stopped.set()
        observer.join(timeout=.1)
    conn.close()
    hook.lh_spike_checkpoint(0)
    env = dict(os.environ)
    env.pop('LD_PRELOAD', None)
    env['LH_RECOVERY_SUPERVISOR_PID'] = str(os.getpid())
    subprocess.run([sys.executable, '-u', __file__, '--mode', 'stock', '--receipt-id', args.receipt_id,
                    '--db-name', args.db_name], env=env, check=True, timeout=max(.1, args.deadline-time.time()))
    a.write_receipt(folder / 'completed.json', {'stock_verified': True, 'removed': removed,
                                             'metadata_blocks': hook.lh_spike_allocations()})
    event('production_recovery_verified', receipt=folder.name, metadata_blocks=hook.lh_spike_allocations())


def remote(args):
    if not re.fullmatch(r'[0-9]+_[0-9]+', args.receipt_id) or not re.fullmatch(r'[a-z0-9_]+', args.db_name):
        raise ValueError('invalid production receipt/league')
    folder = Path('/data') / ('production_recovery_' + args.receipt_id)
    if args.mode in ('repair', 'stock'):
        a.protect_parent(int(os.environ.get('LH_RECOVERY_SUPERVISOR_PID', '0')))
        return stock(args, folder) if args.mode == 'stock' else repair(args, folder)
    production_binding()
    if not args.deadline or not 0 < args.deadline-time.time() <= 150:
        raise ValueError('production repair requires a <=150s shared deadline')
    folder.mkdir()
    a.write_receipt(folder / 'original-machine.json', json.loads(Path('/tmp/original-machine.json').read_text()))
    env = dict(os.environ, LD_PRELOAD='/tmp/lh_five_drop.so', LH_RECOVERY_SUPERVISOR_PID=str(os.getpid()))
    a.STAGE_LIMITS.update(inspect=5, preserve=25, replay=65, remove=5, verify=50)
    with (folder / 'trace.jsonl').open('xb') as trace:
        result = a.run_stage('inspect', [sys.executable, '-u', __file__, '--mode', 'repair',
                    '--receipt-id', args.receipt_id, '--db-name', args.db_name,
                    '--helper-sha256', args.helper_sha256, '--deadline', str(args.deadline)],
                    env=env, deadline=args.deadline, trace=trace,
                    transitions=('preserve', 'replay', 'preserve', 'remove', 'verify'))
    finish_remote(result, folder)


def finish_remote(result, folder):
    if result['exit_code'] != 0:
        raise ValueError('repair outcome ' + result['outcome'] + '; retained production evidence in ' + str(folder))
    completed = a.read_receipt(folder / 'completed.json')
    stock_proof = a.read_receipt(folder / 'stock.json')
    if (completed.get('stock_verified') is not True or completed.get('removed') != 5
            or stock_proof.get('verified') is not True):
        raise ValueError('durable stock verification receipt missing')
    event('production_recovery_verified', receipt=folder.name)


class Fly:
    def __init__(self):
        self.headers = {'Authorization': 'Bearer ' + os.environ['FLY_API_TOKEN'], 'Content-Type': 'application/json'}

    def call(self, suffix='', data=None, method=None, timeout=20):
        url = 'https://api.machines.dev/v1/apps/league-history-duckdb/machines/' + MACHINE + suffix
        request = urllib.request.Request(url, data=None if data is None else json.dumps(data).encode(),
                                         headers=self.headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read(1024*1024+1)
        except urllib.error.HTTPError as exc:
            detail = exc.read(1024).decode(errors='replace')
            event('machine_api_error', endpoint=suffix, status=exc.code, detail=detail)
            raise
        if len(raw) > 1024*1024:
            raise ValueError('machine response exceeded 1MiB')
        return json.loads(raw) if raw else {}

    def wait(self, state, seconds=30):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            current = self.call()
            if current['state'] in ((state,) if isinstance(state, str) else state):
                return current
            time.sleep(1)
        raise TimeoutError('machine did not reach ' + str(state))


def handoff(args):
    fly = Fly()
    original = fly.call()
    event('handoff_inventory', state=original['state'], instance_id=original['instance_id'],
          init=original['config'].get('init'), cordoned=original.get('cordoned'))
    if original['config'].get('init', {}).get('exec'):
        raise ValueError('existing maintenance config must be reconciled, not saved as the original')
    root = Path(__file__).resolve().parents[2]
    files = [file_input(root / relative, '/tmp/' + Path(relative).name) for relative in
             ('scripts/diagnostics/duckdb_production_recovery.py', 'scripts/diagnostics/duckdb_recovery_adapter.py',
              'scripts/fly_table_storage_pilot.py', 'scripts/fly_duckdb_block_probe.py')]
    files.append(file_input('/tmp/lh_five_drop.so', '/tmp/lh_five_drop.so'))
    # Original config remains private: inject into maintenance and fsync to its
    # volume before repair. Never upload machine/env configuration publicly.
    raw = json.dumps(original).encode()
    files.append({'guest_path': '/tmp/original-machine.json', 'raw_value': base64.b64encode(raw).decode()})
    config = maintenance_config(original, files)
    # Cheap checks run while the original service is still up. Stop only after
    # confirming executable/API shape and absence of pending swap recovery.
    check = {'stdout': 'PREFLIGHT_OK'} if original['state'] == 'stopped' else fly.call('/exec', {'command': ['/usr/local/bin/python', '-c',
        'from pathlib import Path; p=Path("/data/___leagues.duckdb"); '
        'assert p.is_file(); assert all(not Path(str(p)+s).exists() for s in (".prev",".wal.checkpoint",".wal.recovery")); '
        'assert 0 < Path(str(p)+".wal").stat().st_size <= 1024**3; print("PREFLIGHT_OK")'], 'timeout': 5}, 'POST')
    if check.get('exit_code', 0) != 0 or check.get('exit_signal', 0) != 0 or 'PREFLIGHT_OK' not in check.get('stdout', ''):
        raise ValueError('production preflight refused before stop')
    helper_sha = hashlib.sha256(Path('/tmp/lh_five_drop.so').read_bytes()).hexdigest()
    run_handoff(fly, original, config, args, helper_sha)


def run_handoff(fly, original, config, args, helper_sha):
    lease = fly.call('/lease', {'description': 'bounded storage repair ' + args.receipt_id, 'ttl': 600})
    fly.headers['fly-machine-lease-nonce'] = lease['data']['nonce']
    try:
        current = fly.call()
        if current['instance_id'] != original['instance_id'] or current['config'] != original['config']:
            raise ValueError('machine changed before handoff')
        event('production_stop', machine=MACHINE)
        fly.call('/cordon', {}, 'POST')
        if current['state'] != 'stopped':
            fly.call('/stop', {'signal': 'SIGKILL', 'timeout': '1s'}, 'POST')
        stopped = fly.wait('stopped')
        if stopped['config'] != original['config']:
            raise ValueError('configuration changed during stop')
        event('writer_stopped', instance_id=stopped['instance_id'])
        fly.call('', {'config': config, 'current_version': stopped['instance_id'],
                      'skip_launch': True, 'skip_service_registration': True}, 'POST')
        fly.wait(('created', 'stopped'))
        fly.call('/start', {}, 'POST')
        fly.wait('started')
        event('maintenance_started', repair_limit_s=150)
        deadline = time.time()+150
        command = ['/usr/local/bin/python', '-u', '/tmp/duckdb_production_recovery.py', '--mode', 'remote',
                   '--receipt-id', args.receipt_id, '--db-name', args.db_name,
                   '--helper-sha256', helper_sha, '--deadline', str(deadline)]
        # Flyexec response is buffered; remote supervisor writes progress every
        # 2seconds to the retained trace and enforces all process-group limits.
        result = fly.call('/exec', {'command': command, 'timeout': 155}, 'POST', timeout=165)
        require_verified(result)
        current = fly.call()
        event('restore_original_service')
        restored = copy.deepcopy(original['config'])
        restored['guest']['cpus'] = 4
        restored['guest']['memory_mb'] = 8192
        restored.setdefault('env', {})['DUCKDB_MEMORY_LIMIT'] = '6GB'
        fly.call('', {'config': restored, 'current_version': current['instance_id'],
                      'skip_service_registration': True}, 'POST')
        fly.wait('started')
        # Probe the server locally while still cordoned; only route after ready.
        health = fly.call('/exec', {'command': ['/usr/local/bin/python', '-c',
            'import json,time,urllib.request;\nfor i in range(30):\n try:\n  d=json.load(urllib.request.urlopen("http://127.0.0.1:8080/ready",timeout=2));\n  if d.get("accepting_queries"): print("READY"); break\n except Exception: pass\n time.sleep(1)\nelse: raise SystemExit(1)'], 'timeout': 40}, 'POST', timeout=45)
        if health.get('exit_code', 0) != 0 or health.get('exit_signal', 0) != 0 or 'READY' not in health.get('stdout', ''):
            raise ValueError('restored service is not ready; still cordoned')
        fly.call('/uncordon', {}, 'POST')
        event('production_service_restored', temporary_cpus=4,
              config_sha256=hashlib.sha256(json.dumps(restored, sort_keys=True).encode()).hexdigest())
    finally:
        # No unconditional restore: an ambiguous repair must not invoke the
        # server's startup quarantine/.prev recovery against unverified data.
        fly.call('/lease', method='DELETE')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['handoff', 'remote', 'repair', 'stock'], required=True)
    parser.add_argument('--receipt-id', required=True)
    parser.add_argument('--db-name', default='nyu_ffl')
    parser.add_argument('--helper-sha256')
    parser.add_argument('--deadline', type=float)
    args = parser.parse_args()
    if args.mode == 'handoff':
        handoff(args)
    else:
        remote(args)


if __name__ == '__main__':
    try:
        main()
        code = 0
    except BaseException as exc:
        event('production_recovery_failed', error=type(exc).__name__ + ': ' + str(exc)[:1000])
        code = 2
    os._exit(code)  # Never implicitly checkpoint a failed mutable child.
