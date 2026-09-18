"""The production handoff must isolate the writer and retain its real config."""
import copy
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / 'scripts/diagnostics'))


def recovery():
    import duckdb_production_recovery
    return duckdb_production_recovery


def machine():
    return {'id': '1781e011b69068', 'state': 'started', 'instance_id': 'v1',
            'config': {'image': 'registry.fly.io/league-history-duckdb:deployment-01M2S3CCAQK5KYEB807DK6C33Z',
                       'init': {'swap_size_mb': 4096}, 'env': {'KEEP': 'yes'},
                       'guest': {'cpus': 8, 'memory_mb': 16384},
                       'mounts': [{'volume': 'vol_rkg7mmd17llez224', 'path': '/data'}],
                       'services': [{'autostart': True, 'internal_port': 8080}],
                       'restart': {'policy': 'on-failure', 'max_retries': 10}}}


def test_maintenance_disables_serving_without_mutating_saved_config():
    original = machine()
    saved = copy.deepcopy(original)
    config = recovery().maintenance_config(original, [])
    assert original == saved
    assert config['services'] == []
    assert config['restart'] == {'policy': 'no'}
    assert config['init']['exec'] == ['/bin/sleep', '600']
    assert config['init']['swap_size_mb'] == 4096
    assert config['env'] == {'KEEP': 'yes'}
    assert config['guest'] == {'cpus': 4, 'memory_mb': 8192}
    assert config['mounts'] == saved['config']['mounts']


@pytest.mark.parametrize('change', ['machine', 'mount', 'image'])
def test_handoff_rejects_wrong_target_before_stopping_anything(change):
    original = machine()
    if change == 'machine':
        original['id'] = 'unrelated'
    elif change == 'mount':
        original['config']['mounts'][0]['volume'] = 'vol_unrelated'
    else:
        original['config']['image'] = 'unreviewed-image'
    with pytest.raises(ValueError):
        recovery().maintenance_config(original, [])


def test_stock_proof_does_not_accept_an_ambiguous_remote_success():
    r = recovery()
    with pytest.raises(ValueError):
        r.require_verified({'stdout': '{"event":"checkpoint_end"}\n'})
    with pytest.raises(ValueError):
        r.require_verified({'exit_code': 1, 'stdout': '{"event":"production_recovery_verified"}\n'})
    r.require_verified({'stdout': '{"event":"production_recovery_verified"}\n'})


def test_machine_api_signal_is_validated_without_rejecting_normal_zero_signal():
    r = recovery()
    r.require_verified({'exit_signal': 0, 'stdout': '{"event":"production_recovery_verified"}\n'})
    with pytest.raises(ValueError):
        r.require_verified({'exit_signal': 9, 'stdout': '{"event":"production_recovery_verified"}\n'})


def test_supervisor_emits_success_only_after_durable_stock_receipt(tmp_path, capsys):
    r = recovery()
    import duckdb_recovery_adapter as a
    result = {'exit_code': 0, 'outcome': 'PASS'}
    with pytest.raises((ValueError, FileNotFoundError)):
        r.finish_remote(result, tmp_path)
    a.write_receipt(tmp_path / 'completed.json', {'stock_verified': True, 'removed': 5, 'metadata_blocks': 0})
    a.write_receipt(tmp_path / 'stock.json', {'verified': True, 'old_block_registered': False})
    r.finish_remote(result, tmp_path)
    output = capsys.readouterr().out
    r.require_verified({'stdout': output, 'exit_signal': 0})
    with pytest.raises(ValueError):
        r.finish_remote({'exit_code': 124, 'outcome': 'UNKNOWN'}, tmp_path)


def test_resume_maintenance_uses_embedded_original_not_sleep_config():
    import base64, json
    original = machine()
    current = copy.deepcopy(original)
    current['config'] = recovery().maintenance_config(original, [
        {'guest_path': '/tmp/original-machine.json', 'raw_value': base64.b64encode(json.dumps(original).encode()).decode()}])
    assert recovery().saved_original(current) == original
    current['config']['files'] = []
    with pytest.raises(ValueError):
        recovery().saved_original(current)


def test_reuse_wal_verifies_bytes_without_rewriting_retained_evidence(tmp_path):
    source, retained = tmp_path/'active.wal', tmp_path/'original.wal'
    source.write_bytes(b'valid-committed-wal')
    retained.write_bytes(source.read_bytes())
    before = retained.stat()
    receipt = recovery().verify_retained_wal(source, retained)
    assert receipt['bytes'] == 19
    assert (retained.stat().st_ino, retained.stat().st_mtime_ns) == (before.st_ino, before.st_mtime_ns)
    retained.write_bytes(b'bad!!-committed-wal')
    with pytest.raises(ValueError):
        recovery().verify_retained_wal(source, retained)
    with pytest.raises(ValueError):
        recovery().verify_retained_wal(source, source)


def test_wal_explicit_production_limit_preserves_bytes_and_default_stays_narrow(tmp_path):
    from duckdb_recovery_adapter import preserve_wal
    source = tmp_path / 'active.wal'
    source.write_bytes(b'committed-wal-records')
    result = preserve_wal(source, tmp_path / 'saved.wal', max_bytes=1024**3)
    assert (tmp_path / 'saved.wal').read_bytes() == source.read_bytes()
    assert result['bytes'] == 21
    with pytest.raises(ValueError):
        preserve_wal(source, tmp_path / 'too-large', max_bytes=1024**3 + 1)


@pytest.mark.parametrize('verified', [True, False])
def test_handoff_restores_exact_config_only_after_proof_and_uncordons_after_health(verified):
    """Catches unconditional restart, missing SIGKILL, and premature serving."""
    r = recovery()
    from types import SimpleNamespace
    original = machine()
    config = r.maintenance_config(original, [])

    class API:
        def __init__(self):
            self.current = copy.deepcopy(original)
            self.headers = {}
            self.calls = []

        def call(self, suffix='', data=None, method=None, timeout=20):
            self.calls.append((suffix, copy.deepcopy(data), method))
            if suffix == '/lease':
                return {'data': {'nonce': 'owned'}}
            if suffix == '':
                if data:
                    assert data['current_version'] == self.current['instance_id']
                    self.current['config'] = copy.deepcopy(data['config'])
                    self.current['instance_id'] = 'v2'
                return copy.deepcopy(self.current)
            if suffix == '/exec':
                if 'remote' in data['command']:
                    return {'stdout': '{"event":"production_recovery_verified"}\n' if verified else '{"event":"checkpoint_start"}\n'}
                return {'stdout': 'READY\n'}
            return {}

        def wait(self, state, seconds=30):
            self.calls.append(('wait', state, None))
            self.current['state'] = state
            if state == 'stopped':
                self.current['instance_id'] = 'stopped-version'
            return copy.deepcopy(self.current)

        def execute(self, command, seconds):
            return self.call('/exec', {'command': command})

    api = API()
    args = SimpleNamespace(receipt_id='1_1', db_name='nyu_ffl')
    if verified:
        r.run_handoff(api, original, config, args, 'a'*64)
        expected = copy.deepcopy(original['config'])
        expected['guest']['cpus'] = 4
        expected['guest']['memory_mb'] = 8192
        expected['env']['DUCKDB_MEMORY_LIMIT'] = '6GB'
        assert api.current['config'] == expected
        assert api.calls[-2][0] == '/uncordon'
    else:
        with pytest.raises(ValueError):
            r.run_handoff(api, original, config, args, 'a'*64)
        assert api.current['config']['services'] == []
        assert not any(c[0] == '/uncordon' for c in api.calls)
    assert next(c[1] for c in api.calls if c[0] == '/stop')['signal'] == 'SIGKILL'
    changes = [i for i, c in enumerate(api.calls) if c[0] == '' and c[1]]
    stopped = next(i for i,c in enumerate(api.calls) if c[:2] == ('wait', 'stopped'))
    assert stopped < changes[0]
    assert api.calls[-1] == ('/lease', None, 'DELETE')
