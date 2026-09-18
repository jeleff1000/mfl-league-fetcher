"""Bounded storage pilots must refuse production and terminate on deadline."""
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


def test_pilot_refuses_primary_before_opening_a_database():
    result = subprocess.run(
        [sys.executable, "scripts/fly_table_storage_pilot.py", "--action", "remove",
         "--target-table", "player_fantasy_season", "--db-name", "nyu_ffl",
         "--machine-id", "1781e011b69068", "--volume-id", "vol_rkg7mmd17llez224",
         "--deadline", str(time.time() + 30)],
        cwd=ROOT, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode == 2
    assert "primary target forbidden" in result.stdout


@pytest.mark.parametrize("delay", [-1, 41])
def test_pilot_refuses_expired_or_overlong_deadline(delay):
    from scripts.fly_table_storage_pilot import arm_deadline
    with pytest.raises(ValueError):
        arm_deadline(time.time() + delay)


def test_deadline_kills_a_stuck_pilot_instead_of_waiting_for_completion():
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-u", "-c",
         "import time; from scripts.fly_table_storage_pilot import arm_deadline; "
         "arm_deadline(time.time()+0.2); time.sleep(20)"],
        cwd=ROOT, text=True, capture_output=True, timeout=5,
    )
    assert result.returncode == 124
    assert "deadline_armed" in result.stdout
    assert time.monotonic() - started < 3


def test_deadline_still_exits_when_logging_blocks():
    result = subprocess.run(
        [sys.executable, "-u", "-c",
         "import time; from scripts import fly_table_storage_pilot as p; "
         "p.emit=lambda *a, **k: time.sleep(20); "
         "p.arm_deadline(time.time()+0.2); time.sleep(20)"],
        cwd=ROOT, text=True, capture_output=True, timeout=3,
    )
    assert result.returncode == 124


def test_pilot_refuses_tables_outside_the_corrupt_aggregate_allowlist():
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="target table"):
        validate_target("remove", "matchup", "nyu_ffl", "isolated", "vol_test")


def test_pilot_refuses_a_nonisolated_path(tmp_path):
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="database path"):
        validate_target("remove", "player_fantasy_season", "nyu_ffl", "isolated", "vol_test", tmp_path / "anything.duckdb")


def test_inspect_reports_candidate_block_without_opening_database(monkeypatch, capsys):
    from types import SimpleNamespace
    import duckdb
    from scripts import fly_table_storage_pilot as pilot

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    import fly_duckdb_block_probe

    monkeypatch.setattr(fly_duckdb_block_probe, "probe", lambda path, offset: {
        "file_changed_during_read": False, "checksum_valid": True,
        "block_sha256": "different-candidate", "bytes_read": 274432,
    })
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("inspect opened DuckDB"))
    args = SimpleNamespace(action="inspect", target_table="player_fantasy_season",
        db_name="nyu_ffl", machine_id="isolated", volume_id="vol_test",
        deadline=time.time() + 5)
    assert pilot.run(args) == 0
    assert '"checksum_valid": true' in capsys.readouterr().out


def test_metadata_donor_probe_reads_checkpoint_without_touching_database_or_wal(tmp_path, monkeypatch):
    import hashlib
    import struct
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))

    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i FROM range(50) t(i)")
        conn.execute("CHECKPOINT")
        ids = [r[0] for r in conn.execute("SELECT block_id FROM pragma_metadata_info()").fetchall()]
    with path.open("rb") as stream:
        stream.seek(12288 + ids[0] * 262144)
        expected = struct.unpack("<Q", stream.read(8))[0]
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    wal = Path(str(path) + ".wal")
    wal.write_bytes(b"retained evidence - must not replay, move or delete")
    result = pilot.probe_metadata_donor(path, expected)
    assert result["checkpoint_only"] is True
    assert result["metadata_blocks_checked"] == len(ids)
    assert any(item["block_id"] == ids[0] and item["checksum_valid"] for item in result["candidates"])
    assert result["repair_authorized"] is False
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert wal.read_bytes() == b"retained evidence - must not replay, move or delete"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["source.duckdb", "source.duckdb.wal"]


def test_metadata_donor_action_rejects_unapproved_volume():
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="donor volume"):
        validate_target("donor_headers", "player_fantasy_season", "nyu_ffl", "isolated", "vol_test")


def test_retained_header_probe_finds_unregistered_block_without_opening_duckdb(tmp_path, monkeypatch):
    import hashlib
    import struct
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    path = tmp_path / "retained.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i FROM range(10) t(i)")
    with path.open("rb") as stream:
        stream.seek(12288)
        retained = stream.read(262144)
    offset = path.stat().st_size
    with path.open("ab") as stream:
        stream.write(retained)
    wal = Path(str(path) + ".wal")
    wal.write_bytes(b"retained WAL evidence")
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("header probe opened DuckDB"))
    result = pilot.probe_retained_donor(path, struct.unpack_from("<Q", retained)[0])
    assert any(c["offset"] == offset and c["checksum_valid"] for c in result["candidates"])
    assert result["header_bytes_read"] == ((path.stat().st_size - 12288) // 262144) * 8
    assert result["repair_authorized"] is False
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert wal.read_bytes() == b"retained WAL evidence"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["retained.duckdb", "retained.duckdb.wal"]


def test_retained_header_probe_stops_at_header_limit(tmp_path, monkeypatch):
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    path = tmp_path / "limit.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i FROM range(10) t(i)")
    monkeypatch.setattr(pilot, "RETAINED_BLOCK_LIMIT", 1, raising=False)
    with pytest.raises(ValueError, match="header ceiling"):
        pilot.probe_retained_donor(path, 1)


def test_retained_header_probe_rejects_ambiguous_candidates(tmp_path, monkeypatch):
    import struct
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    path = tmp_path / "ambiguous.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i FROM range(10) t(i)")
    with path.open("rb") as stream:
        stream.seek(12288)
        retained = stream.read(262144)
    with path.open("ab") as stream:
        stream.write(retained * 2)
    with pytest.raises(ValueError, match="more than two"):
        pilot.probe_retained_donor(path, struct.unpack_from("<Q", retained)[0])


def test_retained_header_action_requires_exact_isolated_witness():
    from scripts.fly_table_storage_pilot import validate_target
    with pytest.raises(ValueError, match="donor volume"):
        validate_target("retained_headers", "player_fantasy_season", "nyu_ffl", "isolated", "vol_test")
    validate_target("retained_headers", "player_fantasy_season", "nyu_ffl", "isolated", "vol_vp26dp2g9x3167j4")


def test_retained_headers_can_inspect_existing_damaged_witness_without_sql():
    from scripts.fly_table_storage_pilot import validate_target
    validate_target("retained_headers", "player_fantasy_season", "nyu_ffl", "isolated", "vol_4919j2m0wzg0xw5r")
    with pytest.raises(ValueError, match="donor volume"):
        validate_target("donor_headers", "player_fantasy_season", "nyu_ffl", "isolated", "vol_4919j2m0wzg0xw5r")


def test_engine_inventory_requires_exact_existing_isolated_volume():
    from scripts.fly_table_storage_pilot import validate_target
    validate_target("engine_inventory", "player_fantasy_season", "nyu_ffl", "isolated", "vol_4919j2m0wzg0xw5r")
    with pytest.raises(ValueError, match="isolated engine"):
        validate_target("engine_inventory", "player_fantasy_season", "nyu_ffl", "isolated", "vol_other")


def test_engine_identity_inspection_never_opens_database(monkeypatch):
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("identity inspection opened database"))
    identity = pilot.engine_identity()
    assert identity["duckdb"] == duckdb.__version__
    assert len(identity["engine_sha256"]) == 64
    assert identity["engine_bytes"] > 0
    pilot.validate_target("engine_identity", "player_fantasy_season", "nyu_ffl", "isolated", "vol_4919j2m0wzg0xw5r")
    with pytest.raises(ValueError, match="isolated engine"):
        pilot.validate_target("engine_identity", "player_fantasy_season", "nyu_ffl", "isolated", "vol_other")


def test_file_inventory_does_not_open_duckdb_or_hide_wal(tmp_path, monkeypatch):
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.setattr(duckdb, 'connect', lambda *a, **k: pytest.fail('stat inventory opened DuckDB'))
    path = tmp_path / '___leagues.duckdb'
    path.write_bytes(b'fixture')
    wal = Path(str(path) + '.wal')
    wal.write_bytes(b'committed')
    result = pilot.file_inventory(path)
    assert result['']['size'] == 7
    assert result['.wal']['size'] == 9
    assert result['.wal']['mtime_ns'] == wal.stat().st_mtime_ns
    assert wal.read_bytes() == b'committed'


def test_retained_headers_distinguish_bad_original_from_intact_duplicate(tmp_path, monkeypatch):
    import hashlib
    import struct
    import duckdb
    from scripts import fly_table_storage_pilot as pilot
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    path = tmp_path / "damaged.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i FROM range(10) t(i)")
    with path.open("r+b") as stream:
        stream.seek(12288)
        original = stream.read(262144)
        stream.seek(0, 2)
        duplicate_offset = stream.tell()
        stream.write(original)
        stream.seek(12288 + 24)
        stream.write(bytes([original[24] ^ 3]))
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(duckdb, "connect", lambda *a, **k: pytest.fail("header probe opened DuckDB"))
    result = pilot.probe_retained_donor(path, struct.unpack_from("<Q", original)[0])
    assert [(c["offset"], c["checksum_valid"]) for c in result["candidates"]] == [
        (12288, False), (duplicate_offset, True),
    ]
    assert result["repair_authorized"] is False
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
