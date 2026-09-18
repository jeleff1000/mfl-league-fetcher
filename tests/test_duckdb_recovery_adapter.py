"""Real-file recovery gates: real small files and real DuckDB relations."""
import importlib.util
import sys
import time
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[1]


def adapter():
    path = ROOT / "scripts/diagnostics/duckdb_recovery_adapter.py"
    assert path.exists(), "real-file adapter is missing"
    spec = importlib.util.spec_from_file_location("recovery_adapter", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inventory():
    return ({"id": "abcdef01234567", "name": "wkupd-table-pilot-test", "state": "started",
             "config": {"mounts": [{"volume": "vol_4919j2m0wzg0xw5r", "path": "/data"}]}},
            {"id": "vol_4919j2m0wzg0xw5r", "name": "wkupd_rebuild_35166181636",
             "attached_machine_id": "abcdef01234567"})


def test_inventory_binds_runtime_machine_and_exact_mounted_volume():
    a = adapter()
    machine, volume = inventory()
    a.validate_inventory(machine, volume, "abcdef01234567", "/data/___leagues.duckdb")
    with pytest.raises(ValueError):
        a.validate_inventory(machine, volume, "different", "/data/___leagues.duckdb")
    machine["config"]["mounts"][0]["volume"] = "vol_other"
    with pytest.raises(ValueError):
        a.validate_inventory(machine, volume, "abcdef01234567", "/data/___leagues.duckdb")


@pytest.mark.parametrize("field,value", [("id", "1781e011b69068"), ("name", "production"),
                                         ("state", "stopped")])
def test_inventory_rejects_primary_or_unowned_machine(field, value):
    a = adapter()
    machine, volume = inventory()
    machine[field] = value
    with pytest.raises(ValueError):
        a.validate_inventory(machine, volume, machine["id"], "/data/___leagues.duckdb")


def test_wal_preservation_is_bounded_and_refuses_overwrite(tmp_path):
    a = adapter()
    source = tmp_path / "___leagues.duckdb.wal"
    source.write_bytes(b"committed WAL evidence")
    retained = tmp_path / "retained.wal"
    receipt = a.preserve_wal(source, retained, max_bytes=32)
    assert receipt["bytes"] == 22
    assert retained.read_bytes() == source.read_bytes() == b"committed WAL evidence"
    with pytest.raises(FileExistsError):
        a.preserve_wal(source, retained, max_bytes=32)
    too_small = tmp_path / "too_small.wal"
    with pytest.raises(ValueError, match="WAL.*ceiling"):
        a.preserve_wal(source, too_small, max_bytes=8)
    assert not too_small.exists()


def test_inventory_refuses_canonical_target_and_incomplete_replacements():
    a = adapter()
    rows = [("___leagues", "public", name, 0) for name in a.CANONICAL + a.QUARANTINED]
    a.validate_objects(rows)
    with pytest.raises(ValueError):
        a.validate_objects(rows[:-1])
    with pytest.raises(ValueError):
        a.validate_objects([(db, "other", name, n) for db, _, name, n in rows])


def test_witness_detects_score_alias_and_aggregate_changes_with_same_counts():
    a = adapter()
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INT, week INT, franchise_id VARCHAR, manager VARCHAR, points DOUBLE)")
        conn.execute("INSERT INTO public.matchup VALUES ('nyu_ffl',2025,17,'stable','Saved Alias',112.5),('nyu_ffl',2026,1,'stable','Saved Alias',93.25),('other',2026,1,'other','Unrelated',44)")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        before = a.capture_witness(conn, "nyu_ffl")
        assert before["matchup"]["rows"] == 2
        assert a.capture_witness(conn, "nyu_ffl") == before
        conn.execute("UPDATE public.matchup SET points=93.26 WHERE db_name='nyu_ffl' AND year=2026")
        with pytest.raises(ValueError, match="witness"):
            a.compare_witness(before, a.capture_witness(conn, "nyu_ffl"))
        conn.execute("UPDATE public.matchup SET points=93.25 WHERE db_name='nyu_ffl' AND year=2026")
        conn.execute("UPDATE public.matchup SET manager='Wrong Alias' WHERE db_name='nyu_ffl'")
        with pytest.raises(ValueError, match="witness"):
            a.compare_witness(before, a.capture_witness(conn, "nyu_ffl"))
        conn.execute("UPDATE public.matchup SET manager='Saved Alias' WHERE db_name='nyu_ffl'")
        conn.execute("UPDATE public.homepage_manager_rankings SET points=0 WHERE db_name='nyu_ffl'")
        with pytest.raises(ValueError, match="witness"):
            a.compare_witness(before, a.capture_witness(conn, "nyu_ffl"))


def test_commit_timeout_is_unknown_and_stops_child():
    a = adapter()
    started = time.monotonic()
    result = a.run_stage("remove", [sys.executable, "-c", "import time; time.sleep(20)"],
                         deadline=time.time()+0.3)
    assert result["outcome"] == "UNKNOWN"
    assert result["exit_code"] == 124
    assert time.monotonic() - started < 3


def test_expired_stage_does_not_execute_command(tmp_path):
    a = adapter()
    target = tmp_path / "must_not_exist"
    result = a.run_stage("inspect", [sys.executable, "-c", f"open({str(target)!r},'w').close()"],
                         deadline=time.time()-1)
    assert result["outcome"] == "NOT_STARTED"
    assert not target.exists()


def test_block_identity_requires_exact_known_damage_not_merely_a_checksum_error():
    a = adapter()
    block = {"block_id": 346, "offset": 90714112, "block_size": 262144,
             "file_changed_during_read": False, "checksum_valid": False,
             "block_sha256": "7bbcf166a70b06eb12c19888577060bf17e867a6f8b81ac7b802bffb3cab1186",
             "stored_checksum": 18392342689821271652, "computed_checksum": 5168518579405463287}
    a.validate_block(block)
    for field, value in [("block_id", 347), ("offset", 0), ("checksum_valid", True),
                         ("file_changed_during_read", True), ("block_sha256", "other")]:
        with pytest.raises(ValueError):
            a.validate_block({**block, field: value})


def test_capture_witness_does_not_hide_duplicate_sampled_rows():
    a = adapter()
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 2025 AS year, 'alias' AS manager, 44.0 AS points")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        before = a.capture_witness(conn, "nyu_ffl")
        conn.execute("INSERT INTO public.matchup SELECT * FROM public.matchup")
        with pytest.raises(ValueError, match="witness"):
            a.compare_witness(before, a.capture_witness(conn, "nyu_ffl"))


def test_engine_gate_rejects_same_version_different_binary():
    a = adapter()
    identity = {"duckdb": "1.5.4", "engine_revision": "08e34c447b",
                "engine_sha256": "9135828981e3d0bdc346c10f663e353eb12de486d352edd4af89651a926967a9",
                "engine_bytes": 60210744, "python": "3.11.16",
                "architecture": "x86_64", "libc": ["glibc", "2.41"]}
    a.validate_engine(identity)
    for field, value in [("engine_sha256", "other"), ("duckdb", "1.5.1"),
                         ("engine_revision", "other"), ("architecture", "aarch64"),
                         ("libc", ["glibc", "2.35"])]:
        with pytest.raises(ValueError):
            a.validate_engine({**identity, field: value})


@pytest.mark.skipif(sys.platform != "linux", reason="Linux permits rename over an open WAL; Windows locks it")
def test_wal_path_replacement_during_preservation_is_rejected(tmp_path, monkeypatch):
    a = adapter()
    source = tmp_path / "source.wal"
    source.write_bytes(b"original committed WAL")
    replacement = tmp_path / "replacement.wal"
    replacement.write_bytes(b"different committed WAL")
    real_fsync = a.os.fsync
    swapped = False

    def sync_then_swap(fd):
        nonlocal swapped
        real_fsync(fd)
        if not swapped:
            swapped = True
            source.rename(tmp_path / "original-retained.wal")
            replacement.rename(source)

    monkeypatch.setattr(a.os, "fsync", sync_then_swap)
    with pytest.raises(ValueError, match="WAL.*changed"):
        a.preserve_wal(source, tmp_path / "evidence.wal")


def test_timeout_retains_commit_marker():
    a = adapter()
    result = a.run_stage("remove", [sys.executable, "-u", "-c",
        "import time; print('COMMIT started', flush=True); time.sleep(20)"], deadline=time.time()+0.5)
    assert result["outcome"] == "UNKNOWN"
    assert "COMMIT started" in result["stdout"]


def test_excessive_output_terminates_child_with_bounded_capture():
    a = adapter()
    result = a.run_stage("preserve", [sys.executable, "-u", "-c",
        "import os; [os.write(1,b'x'*4096) for _ in range(1000)]"], deadline=time.time()+2)
    assert result["outcome"] == "FAILED"
    assert result["output_limited"] is True
    assert len(result["stdout"]) <= 65536


def test_file_binding_rejects_symlink_even_on_approved_mount(tmp_path, monkeypatch):
    a = adapter()
    path = tmp_path / "___leagues.duckdb"
    path.write_bytes(b"fixture")
    monkeypatch.setattr(Path, "is_mount", lambda self: self == tmp_path)
    before = a.validate_file_binding(path, tmp_path)
    assert before["size"] == 7
    with pytest.raises(ValueError):
        a.validate_file_binding(path, tmp_path / "wrong")
    # Symlink creation needs platform privileges; use real inode/path checks
    # above on every OS and the real symlink case on Linux Actions.
    if sys.platform == "linux":
        original = tmp_path / "original.duckdb"
        path.rename(original)
        path.symlink_to(original)
        with pytest.raises(ValueError):
            a.validate_file_binding(path, tmp_path)


def test_witness_orders_history_before_display_names_and_keeps_alias_config():
    a = adapter()
    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 'A' AS manager, 2026 AS year, i AS week, 1.0::DOUBLE AS points FROM range(20) t(i)")
        conn.execute("INSERT INTO public.matchup VALUES ('nyu_ffl','M',2011,1,123.45),('nyu_ffl','Z',2026,1,2)")
        conn.execute("INSERT INTO public.matchup SELECT 'nyu_ffl','Z',2026,i,2 FROM range(20) t(i)")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        conn.execute('CREATE TABLE public.league_context(db_name VARCHAR, manager_name_overrides_json VARCHAR, franchise_merges_json VARCHAR)')
        conn.execute("INSERT INTO public.league_context VALUES ('nyu_ffl','{\"M\":\"Saved Alias\"}','{}')")
        before = a.capture_witness(conn, 'nyu_ffl')
        conn.execute("UPDATE public.matchup SET points=0 WHERE year=2011")
        with pytest.raises(ValueError, match='witness'):
            a.compare_witness(before, a.capture_witness(conn, 'nyu_ffl'))
        conn.execute('UPDATE public.matchup SET points=123.45 WHERE year=2011')
        conn.execute("UPDATE public.league_context SET manager_name_overrides_json='{}'")
        with pytest.raises(ValueError, match='witness'):
            a.compare_witness(before, a.capture_witness(conn, 'nyu_ffl'))


def test_removal_uses_only_exact_objects_and_noops_only_after_all_five_absent(tmp_path):
    a = adapter()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT 7 AS value')
        assert a.remove_quarantined(conn) == 5
        assert a.remove_quarantined(conn) == 0
        for name in a.CANONICAL:
            assert conn.execute(f'SELECT value FROM public."{name}"').fetchall() == [(7,)]
        conn.execute(f'CREATE TABLE public."{a.QUARANTINED[0]}" AS SELECT 3 AS value')
        with pytest.raises(ValueError, match='partial'):
            a.remove_quarantined(conn)
        assert conn.execute(f'SELECT value FROM public."{a.QUARANTINED[0]}"').fetchall() == [(3,)]


def test_stock_verification_requires_no_quarantine_and_preserves_real_values(tmp_path):
    a = adapter()
    path = tmp_path / '___leagues.duckdb'
    with duckdb.connect(str(path)) as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 2011 AS year, 'Saved Alias' AS manager, 123.45 AS points")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        before = a.capture_witness(conn, 'nyu_ffl')
    a.verify_stock(path, 'nyu_ffl', before)
    with duckdb.connect(str(path)) as conn:
        assert a.capture_witness(conn, 'nyu_ffl') == before
        conn.execute(f'CREATE TABLE public."{a.QUARANTINED[0]}" AS SELECT 3 AS value')
    with pytest.raises(ValueError, match='quarantined'):
        a.verify_stock(path, 'nyu_ffl', before)


def test_parent_enforces_phase_deadlines_and_rejects_unexpected_transitions():
    a = adapter()
    a.STAGE_LIMITS['remove'] = .2
    code = "import json,time; print(json.dumps({'event':'phase','stage':'remove'}),flush=True); print('COMMIT started',flush=True); time.sleep(5)"
    result = a.run_stage('preserve', [sys.executable, '-u', '-c', code],
                         deadline=time.time()+3, transitions=('remove', 'verify'))
    assert result['outcome'] == 'UNKNOWN'
    assert result['last_phase'] == 'remove'
    assert 'COMMIT started' in result['stdout']
    bad = "import json,time; print(json.dumps({'event':'phase','stage':'verify'}),flush=True); time.sleep(5)"
    result = a.run_stage('preserve', [sys.executable, '-u', '-c', bad],
                         deadline=time.time()+2, transitions=('remove', 'verify'))
    assert result['exit_code'] == 126
    assert result['protocol_error']


def test_repeated_phase_cannot_reset_its_budget():
    a = adapter()
    a.STAGE_LIMITS['preserve'] = 0.35
    code = "import json,time; time.sleep(.2); print(json.dumps({'event':'phase','stage':'inspect'}),flush=True); print(json.dumps({'event':'phase','stage':'preserve'}),flush=True); time.sleep(.25)"
    result = a.run_stage('preserve', [sys.executable, '-u', '-c', code],
                         deadline=time.time()+3, transitions=('inspect', 'preserve'))
    assert result['exit_code'] == 124


def test_adapter_cli_refuses_wrong_machine_before_opening_any_file():
    import base64
    import json
    a = adapter()
    machine, volume = inventory()
    encoded = base64.b64encode(json.dumps({'machine': machine, 'volume': volume}).encode()).decode()
    with pytest.raises(ValueError, match='running isolated'):
        a.preflight_identity(encoded, '1781e011b69068')


def test_durable_receipt_is_create_only_and_preserves_before_values(tmp_path):
    a = adapter()
    path = tmp_path / 'before.json'
    a.write_receipt(path, {'values_sha': 'original'})
    with pytest.raises(FileExistsError):
        a.write_receipt(path, {'values_sha': 'replacement'})
    import json
    assert json.loads(path.read_text()) == {'values_sha': 'original'}


def test_early_success_without_required_phases_is_not_a_passing_repair():
    a = adapter()
    result = a.run_stage('inspect', [sys.executable, '-c', 'pass'],
                         deadline=time.time()+2, transitions=('preserve', 'remove', 'verify'))
    assert result['exit_code'] == 126
    assert result['outcome'] != 'PASS'


def test_writable_replay_timeout_is_unknown_not_failed():
    a = adapter()
    a.STAGE_LIMITS['replay'] = .2
    code = "import json,time; print(json.dumps({'event':'phase','stage':'replay'}),flush=True); time.sleep(5)"
    result = a.run_stage('preserve', [sys.executable, '-u', '-c', code],
                         deadline=time.time()+3, transitions=('replay',))
    assert result['exit_code'] == 124
    assert result['outcome'] == 'UNKNOWN'


def test_phase_transition_rejects_elapsed_budget_even_before_poll_observes_it():
    a = adapter()
    spent = {'preserve': 9.0}
    with pytest.raises(ValueError, match='budget'):
        a.charge_phase(spent, 'preserve', 1.1)


def test_successful_exit_charges_the_final_phase_budget(monkeypatch):
    a = adapter()
    charged = []
    real_charge = a.charge_phase
    def record_charge(spent, name, elapsed):
        charged.append(name)
        real_charge(spent, name, elapsed)
    monkeypatch.setattr(a, 'charge_phase', record_charge)
    result = a.run_stage('verify', [sys.executable, '-c', 'pass'], deadline=time.time()+2)
    assert result['outcome'] == 'PASS'
    assert charged == ['verify']


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux process counters')
def test_process_progress_uses_only_current_process_counters():
    import os
    a = adapter()
    usage = a.process_usage(os.getpid())
    assert usage['peak_rss_kib'] > 0
    assert usage['read_bytes'] >= 0
    assert usage['write_bytes'] >= 0
    assert usage['cpu_s'] >= 0


def test_process_usage_reads_bounded_fields(tmp_path):
    a = adapter()
    folder = tmp_path / '123'
    folder.mkdir()
    (folder / 'status').write_text('Name:\tpython\nVmHWM:\t128 kB\n')
    (folder / 'io').write_text('read_bytes: 11\nwrite_bytes: 12\nrchar: 13\n')
    (folder / 'stat').write_text('123 (python) ' + ' '.join(['0'] * 22))
    result = a.process_usage(123, proc_root=tmp_path)
    assert result == {'peak_rss_kib': 128, 'read_bytes': 11, 'write_bytes': 12, 'rchar': 13, 'cpu_s': 0.0}


@pytest.mark.skipif(sys.platform != 'linux', reason='Linux parent-death signal')
def test_recovery_child_dies_if_supervisor_is_killed(tmp_path):
    import os
    import subprocess
    marker = tmp_path / 'child-survived'
    armed = tmp_path / 'child-armed'
    child = (f"import sys,os,time; sys.path.insert(0,{str(ROOT / 'scripts/diagnostics')!r}); "
             "import duckdb_recovery_adapter as a; a.protect_parent(int(sys.argv[1])); "
             f"open({str(armed)!r},'w').close(); time.sleep(.7); open({str(marker)!r},'w').close()")
    parent = subprocess.Popen([sys.executable, '-c',
        "import subprocess,sys,os,time; subprocess.Popen([sys.executable,'-c',sys.argv[1],str(os.getpid())]); time.sleep(10)", child])
    try:
        deadline = time.monotonic()+3
        while not armed.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        assert armed.exists()
        parent.kill()
        parent.wait(timeout=1)
        time.sleep(.8)
        assert not marker.exists()
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=1)
