"""Real-file recovery gates: real small files and real DuckDB relations."""
import importlib.util
import sys
import time
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize('compile_exit', [0, 42])
def test_workflow_overlaps_independent_setup_but_requires_compiler_success(tmp_path, compile_exit):
    """Serial setup loses the window; an unchecked background failure is unsafe."""
    import shutil
    import subprocess
    import textwrap
    bash = shutil.which('bash')
    if not bash and Path('C:/Program Files/Git/bin/bash.exe').exists():
        bash = 'C:/Program Files/Git/bin/bash.exe'
    if not bash:
        pytest.skip('Bash required for the real workflow prelude')
    workflow = (ROOT / '.github/workflows/fly_duckdb_reaggregate_recovery.yml').read_text()
    prelude = workflow.split('          echo "stage=pilot_start', 1)[1]
    prelude = '          echo "stage=pilot_start' + prelude.split('          echo "stage=isolated_machine_start', 1)[0]
    prelude = textwrap.dedent(prelude).replace('/tmp/lh_five_drop.so', 'helper.so')
    # Only external API/compiler calls are replaced. Run the actual shell's
    # background execution, wait/error handling, file checks and SHA hashing.
    boundary = r'''
set -euo pipefail
export FLY_APP=test RECOVERY_VOLUME_ID=vol_4919j2m0wzg0xw5r PILOT_ACTION=engine_resume
export WITNESS_DB_NAME=nyu_ffl DIAGNOSTIC_RECEIPT_ID='' TARGET_TABLE=player_fantasy_season PILOT_DEADLINE=9999999999
c++() { : > compiler.started; printf tiny-helper > helper.so; return "$COMPILE_EXIT"; }
timeout() { shift 2; "$@"; }
flyctl() {
  for i in {1..50}; do
    if test -f compiler.started; then printf '{}'; return 0; fi
    sleep .01
  done
  echo 'compiler was serialized behind API reads' >&2
  return 61
}
jq() {
  if [ "$1" = -e ]; then return 0; fi
  if [ "$2" = .region ]; then printf iad; else printf test-image; fi
}
'''
    result = subprocess.run([bash, '--noprofile', '--norc'], cwd=tmp_path,
                            input=f'COMPILE_EXIT={compile_exit}\n' + boundary + prelude + '\necho READY_TO_START\n',
                            text=True, capture_output=True, timeout=10)
    assert result.returncode == compile_exit, result.stderr
    assert ('READY_TO_START' in result.stdout) is (compile_exit == 0)


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


def test_timeout_retains_bounded_stage_trace_on_disk(tmp_path):
    import json
    a = adapter()
    a.STAGE_LIMITS['verify'] = .3
    path = tmp_path / 'trace.jsonl'
    with path.open('xb') as trace:
        result = a.run_stage('verify', [sys.executable, '-u', '-c',
            "import time; print('unstructured detail', flush=True); time.sleep(20)"],
            deadline=time.time()+2, trace=trace)
    events = [json.loads(line) for line in path.read_text().splitlines()]
    assert events[0]['event'] == 'start'
    assert events[-1]['outcome'] == 'UNKNOWN'
    assert events[-1]['exit_code'] == 124
    assert all('stdout' not in event and 'stderr' not in event for event in events)
    assert path.stat().st_size <= 65536
    assert result['exit_code'] == 124


def test_delayed_exit_after_kill_still_reports_unknown(monkeypatch):
    import subprocess
    a = adapter()
    a.STAGE_LIMITS['verify'] = .05
    original = a.subprocess.Popen
    children = []

    def spawn(*args, **kwargs):
        child = original(*args, **kwargs)
        actual_wait = child.wait
        children.append((child, actual_wait))

        def delayed_reap(timeout=None):
            if timeout == 1:
                raise subprocess.TimeoutExpired(child.args, timeout)
            return actual_wait(timeout=timeout)

        child.wait = delayed_reap
        return child

    monkeypatch.setattr(a.subprocess, 'Popen', spawn)
    try:
        result = a.run_stage('verify', [sys.executable, '-c', 'import time; time.sleep(20)'],
                             deadline=time.time()+4)
    finally:
        for child, actual_wait in children:
            child.kill()
            actual_wait(timeout=2)
    assert result['outcome'] == 'UNKNOWN'
    assert result['exit_code'] == 124
    assert result['termination_unconfirmed'] is True


@pytest.mark.parametrize('stage,transitions', [('verify', ('verify',)), ('preserve', ('replay',))])
def test_trace_failure_terminates_mutable_child_and_returns_unknown(tmp_path, monkeypatch, stage, transitions):
    a = adapter()
    original = a.os.fsync
    syncs = 0

    def fail_after_start(fd):
        nonlocal syncs
        syncs += 1
        if syncs > 1:
            raise OSError('simulated trace disk failure')
        original(fd)

    monkeypatch.setattr(a.os, 'fsync', fail_after_start)
    with (tmp_path / 'trace.jsonl').open('xb') as trace:
        result = a.run_stage(stage, [sys.executable, '-u', '-c',
            "import time; print('{\"event\":\"checkpoint_start\"}', flush=True); time.sleep(20)"],
            deadline=time.time()+2, transitions=transitions, trace=trace)
    assert result['outcome'] == 'UNKNOWN'
    assert result['exit_code'] != 0
    assert result['elapsed_s'] < 2


def test_slow_trace_cannot_delay_replay_deadline(tmp_path, monkeypatch):
    a = adapter()
    a.STAGE_LIMITS['replay'] = .1
    original = a.os.fsync
    syncs = 0

    def slow_sync(fd):
        nonlocal syncs
        syncs += 1
        if syncs == 2:
            time.sleep(.65)
        original(fd)

    monkeypatch.setattr(a.os, 'fsync', slow_sync)
    marker = tmp_path / 'late-write'
    code = ("import pathlib,time; print('{\"event\":\"phase\",\"stage\":\"replay\"}',flush=True); "
            f"time.sleep(.3); pathlib.Path({str(marker)!r}).write_text('too late'); time.sleep(5)")
    with (tmp_path / 'trace.jsonl').open('xb') as trace:
        result = a.run_stage('preserve', [sys.executable, '-u', '-c', code],
                            deadline=time.time()+2, transitions=('replay',), trace=trace)
    assert result['outcome'] == 'UNKNOWN'
    assert result['exit_code'] != 0
    assert not marker.exists()


def test_final_fsync_failure_cannot_leave_authoritative_pass_in_trace(tmp_path, monkeypatch):
    import json
    a = adapter()
    original = a.os.fsync
    syncs = 0

    def fail_final(fd):
        nonlocal syncs
        syncs += 1
        if syncs == 2:
            raise OSError('final fsync failure')
        original(fd)

    monkeypatch.setattr(a.os, 'fsync', fail_final)
    path = tmp_path / 'trace.jsonl'
    with path.open('xb') as trace:
        result = a.run_stage('verify', [sys.executable, '-c', 'pass'],
                            deadline=time.time()+2, trace=trace)
    assert result['outcome'] == 'UNKNOWN'
    events = [json.loads(line) for line in path.read_text().splitlines()]
    assert events[-1]['outcome'] == 'PROVISIONAL'
    assert events[-1]['requires_completed_receipt'] is True


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


def test_single_removal_keeps_other_targets_and_resumes_only_known_progress(tmp_path):
    a = adapter()
    path = tmp_path / '___leagues.duckdb'
    first = '__corrupt_recovery_homepage_manager_rankings'
    second = '__corrupt_recovery_matchup_h2h_career'
    with duckdb.connect(str(path)) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT 7 AS value')
        assert a.remove_quarantined(conn, target=first) == 1
        conn.execute('CHECKPOINT')
    with duckdb.connect(str(path)) as conn:
        assert a.remove_quarantined(conn, target=first, prepare=lambda: pytest.fail('repeat mutation')) == 0
        with pytest.raises(ValueError, match='reconcile'):
            a.remove_quarantined(conn, target=second)

        def refused():
            raise ValueError('second preservation refused')

        with pytest.raises(ValueError, match='second preservation refused'):
            a.remove_quarantined(conn, target=second, prior_removed=[first], prepare=refused)
        remaining = {r[2] for r in a.object_inventory(conn)}
        assert first not in remaining
        assert second in remaining
        assert a.remove_quarantined(conn, target=second, prior_removed=[first]) == 1
        conn.execute('CHECKPOINT')
    with duckdb.connect(str(path), read_only=True) as conn:
        assert {r[2] for r in a.object_inventory(conn)} == set(a.CANONICAL + a.QUARANTINED) - {first, second}
        for name in a.CANONICAL + a.QUARANTINED[2:]:
            assert conn.execute(f'SELECT value FROM public."{name}"').fetchall() == [(7,)]


@pytest.mark.parametrize('target,prior', [
    ('homepage_manager_rankings', []),
    ('__corrupt_recovery_homepage_manager_rankings', ['matchup']),
    ('__corrupt_recovery_homepage_manager_rankings', ['__corrupt_recovery_matchup_h2h_career'] * 2),
])
def test_single_removal_rejects_wrong_scope_before_any_write(tmp_path, target, prior):
    a = adapter()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT 7 AS value')
        with pytest.raises(ValueError, match='scope'):
            a.remove_quarantined(conn, target=target, prior_removed=prior)
        assert len(a.object_inventory(conn)) == 10


def test_real_value_fixture_and_drops_rollback_together_and_preserve_other_leagues(tmp_path):
    a = adapter()
    machine, volume = inventory()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" (db_name VARCHAR, year INTEGER, manager VARCHAR, points DOUBLE)')
            conn.execute(f'INSERT INTO public."{name}" VALUES (?,?,?,?)', ['unrelated', 2011, 'Saved Alias', 123.45])
        bundle = {'db_name': 'nyu_ffl', 'tables': {name: {
            'columns': ['db_name', 'year', 'manager', 'points'],
            'types': ['VARCHAR', 'INTEGER', 'VARCHAR', 'DOUBLE'],
            'rows': [['nyu_ffl', 2018, 'Preferred Alias', 91.23], ['nyu_ffl', 2026, 'Preferred Alias', 110.01]],
        } for name in a.CANONICAL}}

        def prepare(fail=False):
            a.seed_witness_fixture(conn, bundle, 'nyu_ffl', machine, volume, machine['id'])
            if fail:
                raise ValueError('preservation refused')

        with pytest.raises(ValueError, match='preservation refused'):
            a.remove_quarantined(conn, prepare=lambda: prepare(True))
        assert len(a.object_inventory(conn)) == 10
        for name in a.CANONICAL:
            assert conn.execute(f'SELECT * FROM public."{name}"').fetchall() == [('unrelated', 2011, 'Saved Alias', 123.45)]
        assert a.remove_quarantined(conn, prepare=prepare) == 5
        # A repeated removal must not rerun fixture insertion or duplicate rows.
        assert a.remove_quarantined(conn, prepare=lambda: pytest.fail('repeated seed')) == 0
        for name in a.CANONICAL:
            assert conn.execute(f'SELECT year,manager,points FROM public."{name}" WHERE db_name=? ORDER BY year', ['nyu_ffl']).fetchall() == [(2018, 'Preferred Alias', 91.23), (2026, 'Preferred Alias', 110.01)]
            assert conn.execute(f'SELECT points FROM public."{name}" WHERE db_name=?', ['unrelated']).fetchall() == [(123.45,)]


def test_fixture_export_is_twenty_rows_at_most_with_old_and_recent_real_values():
    a = adapter()
    queries = []
    with duckdb.connect(':memory:') as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT \'nyu_ffl\' AS db_name, 2010+i AS year, \'Saved Alias\' AS manager, (90+i)::DOUBLE AS points FROM range(20) t(i)')
            conn.execute(f'INSERT INTO public."{name}" VALUES (?,?,?,?)', ['other', 1990, 'Private Other', 500])

        def query(sql):
            queries.append(sql)
            # Live Fly /query rejects a leading parenthesis as a non-read.
            if not sql.lstrip().lower().startswith('select '):
                raise ValueError('read endpoint requires SELECT')
            rows = conn.execute(sql).fetchall()
            names = [c[0] for c in conn.description]
            return [dict(zip(names, row)) for row in rows]

        bundle = a.export_witness_fixture('nyu_ffl', query)
        assert len(queries) <= 2, 'ten network round trips exceed the bounded read budget'
        assert sum(len(t['rows']) for t in bundle['tables'].values()) == 20
        for table in bundle['tables'].values():
            records = [dict(zip(table['columns'], r)) for r in table['rows']]
            assert sorted(r['year'] for r in records) == [2010, 2011, 2028, 2029]
            assert {r['manager'] for r in records} == {'Saved Alias'}
            assert {r['db_name'] for r in records} == {'nyu_ffl'}


@pytest.mark.parametrize('defect', ['primary', 'extra_table', 'other_league', 'too_many_rows', 'existing_rows'])
def test_fixture_refuses_wrong_scope_or_overwriting_existing_rows(tmp_path, defect):
    a = adapter()
    machine, volume = inventory()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" (db_name VARCHAR, points DOUBLE)')
        bundle = {'db_name': 'nyu_ffl', 'tables': {name: {'columns': ['db_name', 'points'], 'types': ['VARCHAR', 'DOUBLE'], 'rows': [['nyu_ffl', 91.23]]} for name in a.CANONICAL}}
        if defect == 'primary':
            machine['id'] = '1781e011b69068'
        elif defect == 'extra_table':
            bundle['tables']['matchup'] = bundle['tables'][a.CANONICAL[0]]
        elif defect == 'other_league':
            bundle['tables'][a.CANONICAL[-1]]['rows'][0][0] = 'other'
        elif defect == 'too_many_rows':
            bundle['tables'][a.CANONICAL[-1]]['rows'] *= 5
        else:
            conn.execute(f'INSERT INTO public."{a.CANONICAL[-1]}" VALUES (?,?)', ['nyu_ffl', 7])
        with pytest.raises(ValueError):
            a.remove_quarantined(conn, prepare=lambda: a.seed_witness_fixture(conn, bundle, 'nyu_ffl', machine, volume, machine['id']))
        assert len(a.object_inventory(conn)) == 10
        for name in a.CANONICAL[:-1]:
            assert conn.execute(f'SELECT COUNT(*) FROM public."{name}"').fetchone() == (0,)


@pytest.mark.parametrize('source_type', ['DOUBLE', 'INTEGER'])
def test_fixture_rejects_type_mismatch_and_silent_value_rounding(tmp_path, source_type):
    a = adapter()
    machine, volume = inventory()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        for name in a.CANONICAL + a.QUARANTINED:
            conn.execute(f'CREATE TABLE public."{name}" (db_name VARCHAR, points INTEGER)')
        bundle = {'db_name': 'nyu_ffl', 'tables': {name: {
            'columns': ['db_name', 'points'], 'types': ['VARCHAR', source_type],
            'rows': [['nyu_ffl', 91.23]],
        } for name in a.CANONICAL}}
        with pytest.raises(ValueError, match='type|value'):
            a.remove_quarantined(conn, prepare=lambda: a.seed_witness_fixture(conn, bundle, 'nyu_ffl', machine, volume, machine['id']))
        assert len(a.object_inventory(conn)) == 10
        for name in a.CANONICAL:
            assert conn.execute(f'SELECT COUNT(*) FROM public."{name}"').fetchone() == (0,)


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


def test_resume_binds_only_the_observed_postcommit_file_and_sidecars():
    a = adapter()
    binding = {'size': 15555375104, 'mtime_ns': 1789755176638185818, 'inode': 14}
    files = {'.wal': {'size': 45522288, 'mtime_ns': 1789755164142675724, 'inode': 64}}
    header = '1b47d141ed345a3a89371b6caffe8dc76db21a093b6438c444d22da461c01878'
    assert a.validate_recovery_baseline(binding, files, header, resume=True) == '81f5df9726e7a1e4009e3de53e8ee9d13e6c9369ae52618b6d75e3666f991af3'
    with pytest.raises(ValueError):
        a.validate_recovery_baseline(binding, files, header, resume=False)
    for changed in ({**binding, 'inode': 15}, {**binding, 'mtime_ns': 1789748756148253910}):
        with pytest.raises(ValueError):
            a.validate_recovery_baseline(changed, files, header, resume=True)
    for sidecar in ('.wal.checkpoint', '.wal.recovery'):
        with pytest.raises(ValueError):
            a.validate_recovery_baseline(binding, {**files, sidecar: {'size': 1}}, header, resume=True)


def test_resume_reconciliation_never_reseeds_or_reissues_drops(tmp_path):
    a = adapter()
    with duckdb.connect(str(tmp_path / '___leagues.duckdb')) as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 2018 AS year, 'Alias' AS manager, 91.23 AS points")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        before = a.capture_witness(conn, 'nyu_ffl')
        assert a.reconcile_committed_removal(conn, 'nyu_ffl', before) == 0
        assert a.reconcile_committed_removal(conn, 'nyu_ffl', before) == 0
        assert a.capture_witness(conn, 'nyu_ffl') == before
        conn.execute(f'CREATE TABLE public."{a.QUARANTINED[0]}" AS SELECT 7 AS value')
        with pytest.raises(ValueError, match='reconcile'):
            a.reconcile_committed_removal(conn, 'nyu_ffl', before)
        assert len(a.object_inventory(conn)) == 6
        conn.execute(f'DROP TABLE public."{a.QUARANTINED[0]}"')
        conn.execute("UPDATE public.matchup SET manager='Unmerged' WHERE db_name='nyu_ffl'")
        with pytest.raises(ValueError, match='witness'):
            a.reconcile_committed_removal(conn, 'nyu_ffl', before)


def test_resume_evidence_rebinds_same_volume_not_the_destroyed_machine():
    import base64
    import copy
    import json
    a = adapter()
    machine, volume = inventory()
    manifest = {'inventory_base64': base64.b64encode(json.dumps({'machine': machine, 'volume': volume}).encode()).decode(),
        'binding': {'size': 15555375104, 'mtime_ns': 1789662410048242836, 'inode': 14},
        'files': {'.wal': {'size': 45516621, 'mtime_ns': 1789662378556231033, 'inode': 64}},
        'db_name': 'nyu_ffl',
        'wal': {'bytes': 45516621, 'sha256': 'a4f7a2a20afdf2dc1cc218509c1f4052bf6f4df37924768fef518e8c53dace1f'},
        'fixture_sha256': '373b43909b047303815798e581b1d3c861562fbcbd3a3f2cf064de09949df4f6'}
    a.validate_resume_manifest(manifest, {'inode': 14}, 'nyu_ffl', manifest['fixture_sha256'])
    for key, value in [('db_name', 'other'), ('fixture_sha256', 'wrong'), ('binding', {**manifest['binding'], 'inode': 15}),
                       ('wal', {**manifest['wal'], 'sha256': 'wrong'})]:
        changed = copy.deepcopy(manifest)
        changed[key] = value
        with pytest.raises(ValueError):
            a.validate_resume_manifest(changed, {'inode': 14}, 'nyu_ffl', manifest['fixture_sha256'])


def test_read_evidence_refuses_large_files_and_never_replaces_them(tmp_path):
    a = adapter()
    path = tmp_path / 'before.json'
    path.write_bytes(b'{"unchanged":true}')
    assert a.read_receipt(path) == {'unchanged': True}
    path.write_bytes(b' ' * 65537)
    with pytest.raises(ValueError, match='bounded'):
        a.read_receipt(path)
    assert path.stat().st_size == 65537


def test_stock_probe_reconciles_its_owned_interrupted_write_and_repeats(tmp_path):
    a = adapter()
    path = tmp_path / '___leagues.duckdb'
    with duckdb.connect(str(path)) as conn:
        conn.execute('CREATE SCHEMA public')
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 2025 AS year, 'Alias' AS manager, 123.4 AS points")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        before = a.capture_witness(conn, 'nyu_ffl')
        conn.execute('CREATE TABLE public.__lh_recovery_write_probe (value INTEGER, receipt_id VARCHAR)')
        conn.execute("INSERT INTO public.__lh_recovery_write_probe VALUES (7,'35368369603_1')")
    a.verify_stock(path, 'nyu_ffl', before, receipt_id='35368369603_1')
    a.verify_stock(path, 'nyu_ffl', before, receipt_id='35368369603_1')
    with duckdb.connect(str(path)) as conn:
        assert conn.execute("SELECT table_name FROM duckdb_tables() WHERE table_name='__lh_recovery_write_probe'").fetchall() == []
        conn.execute('CREATE TABLE public.__lh_recovery_write_probe (value INTEGER, receipt_id VARCHAR)')
        conn.execute("INSERT INTO public.__lh_recovery_write_probe VALUES (7,'some-other-owner')")
    with pytest.raises(ValueError, match='probe ownership'):
        a.verify_stock(path, 'nyu_ffl', before, receipt_id='35368369603_1')
    with duckdb.connect(str(path)) as conn:
        assert conn.execute('SELECT receipt_id FROM public.__lh_recovery_write_probe').fetchall() == [('some-other-owner',)]


def test_stock_verification_does_not_compact_unrelated_pending_deletes(tmp_path):
    a = adapter()
    path = tmp_path / '___leagues.duckdb'
    with duckdb.connect(str(path), config={'threads': '1'}) as conn:
        conn.execute('PRAGMA disable_checkpoint_on_shutdown')
        conn.execute('CREATE SCHEMA public')
        conn.execute("CREATE TABLE public.matchup AS SELECT 'nyu_ffl' AS db_name, 2018 AS year, 'Saved Alias' AS manager, 91.23 AS points")
        for name in a.CANONICAL:
            conn.execute(f'CREATE TABLE public."{name}" AS SELECT * FROM public.matchup')
        conn.execute("CREATE TABLE public.unrelated_history AS SELECT 'other' AS db_name, i, i+100 AS score FROM range(245760)t(i)")
        conn.execute('CHECKPOINT')
        # Keep committed deletes in WAL, as the real recovery must do.
        conn.execute("DELETE FROM public.unrelated_history WHERE db_name='other' AND i%3!=0")
        before = a.capture_witness(conn, 'nyu_ffl')
    assert path.stat().st_size < 8*1024*1024
    a.verify_stock(path, 'nyu_ffl', before)
    with duckdb.connect(str(path), read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*),SUM(score) FROM public.unrelated_history WHERE db_name='other'").fetchone() == (81920, 10074398720)
        assert conn.execute("SELECT COUNT(DISTINCT row_group_id) FROM pragma_storage_info('public.unrelated_history')").fetchone() == (2,), 'ordinary proof must not vacuum/rewrite unrelated history'


def test_recovery_uses_the_four_available_threads_without_relaxing_memory_or_vacuum():
    a = adapter()
    with duckdb.connect(':memory:', config=a.recovery_connect_config()) as conn:
        assert conn.execute("SELECT current_setting('threads')").fetchone() == (4,)
        assert conn.execute("SELECT current_setting('max_vacuum_tasks')").fetchone() == (0,)
        assert a.recovery_connect_config()['memory_limit'] == '3072MB'
        assert conn.execute("SELECT current_setting('temp_directory')").fetchone() == ('',)


def test_recovery_refuses_startup_starvation_before_engine_open():
    a = adapter()
    a.require_recovery_window(140, now=110)
    for now in (110.1, 125, 141):
        with pytest.raises(ValueError, match='NOT_STARTED'):
            a.require_recovery_window(140, now=now)


def test_checkpoint_marker_is_forwarded_before_a_timeout(capsys):
    a = adapter()
    code = "import json,time; print(json.dumps({'event':'checkpoint_start','index':1}),flush=True); time.sleep(5)"
    result = a.run_stage('verify', [sys.executable, '-u', '-c', code], deadline=time.time()+.4,
                         transitions=('inspect',))
    output = capsys.readouterr().out
    assert output.index('child_event') < output.index('"event": "end"')
    assert result['outcome'] == 'UNKNOWN'


def test_checkpoint_table_progress_survives_remote_timeout(tmp_path):
    import json
    a = adapter()
    event = {'event': 'checkpoint_table_progress', 'table': '___leagues.public.player_fantasy',
             'active': True, 'completed_tables': 12, 'elapsed_ms': 2010}
    code = f"import time; print({json.dumps(event)!r},flush=True); time.sleep(5)"
    with (tmp_path / 'trace.jsonl').open('xb') as trace:
        result = a.run_stage('verify', [sys.executable, '-u', '-c', code],
                             deadline=time.time()+.4, transitions=('inspect',), trace=trace)
    saved = [json.loads(line) for line in (tmp_path / 'trace.jsonl').read_text().splitlines()]
    assert any(row.get('child') == event for row in saved)
    assert result['outcome'] == 'UNKNOWN'


def test_machine_exec_remote_failure_is_not_a_successful_cli_result(capsys):
    a = adapter()
    assert a.emit_machine_exec_result({'exit_code': 7, 'stdout': 'sentinel', 'stderr': 'failure'}) == 7
    assert capsys.readouterr().out == 'sentinel'
    assert a.emit_machine_exec_result({'stdout': '{"event":"isolated_recovery_verified"}\n'}) == 0
    for bad in ({'stdout': ''}, {'exit_code': False, 'stdout': '', 'stderr': ''},
                {'exit_code': 0, 'stdout': 'x'*131073, 'stderr': ''}):
        with pytest.raises(ValueError):
            a.emit_machine_exec_result(bad)


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
