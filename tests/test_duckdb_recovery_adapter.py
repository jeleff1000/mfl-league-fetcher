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
