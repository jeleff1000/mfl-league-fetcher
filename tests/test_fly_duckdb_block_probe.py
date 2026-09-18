"""A bounded diagnostic must not open DuckDB or alter the inspected file."""
import hashlib
import importlib.util
from pathlib import Path

import duckdb
import pytest


def probe(path, offset):
    script = Path(__file__).parents[1] / "scripts/fly_duckdb_block_probe.py"
    assert script.exists(), "Missing read-only single-block diagnostic"
    spec = importlib.util.spec_from_file_location("block_probe", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.probe(path, offset)


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "small.duckdb"
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE facts AS SELECT i, 'player-' || i AS name FROM range(100) t(i)")
    return path


def test_valid_block_matches_engine_checksum_without_mutation(database):
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    result = probe(database, 12288)
    assert result["checksum_valid"] is True
    assert result["block_size"] == 262144
    assert result["bytes_read"] == 12288 + 262144
    assert result["single_bit_payload_candidates"] == 0
    assert result["single_bit_checksum_candidate"] is False
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_corrupt_block_is_reported_without_rewriting_checksum(database):
    with database.open("r+b") as stream:
        stream.seek(12288 + 100)
        value = stream.read(1)[0]
        stream.seek(-1, 1)
        stream.write(bytes([value ^ 1]))
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    result = probe(database, 12288)
    assert result["checksum_valid"] is False
    assert result["stored_checksum"] != result["computed_checksum"]
    assert result["single_bit_payload_candidates"] >= 1
    assert result["repair_authorized"] is False
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_high_bit_fault_is_ambiguous_not_a_repair_instruction(database):
    # With the engine's XOR-of-word-hashes checksum, a high-bit flip in ANY
    # payload word produces the same checksum difference. Never pick one.
    with database.open("r+b") as stream:
        stream.seek(12288 + 15)
        value = stream.read(1)[0]
        stream.seek(-1, 1)
        stream.write(bytes([value ^ 128]))
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    result = probe(database, 12288)
    assert result["single_bit_payload_candidates"] == 32767
    assert result["single_bit_checksum_candidate"] is True
    assert result["repair_authorized"] is False
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_reports_both_checkpoint_headers_without_additional_reads(database):
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    result = probe(database, 12288)
    headers = result["checkpoint_headers"]
    assert [h["file_offset"] for h in headers] == [4096, 8192]
    assert sum(h["active"] for h in headers) == 1
    active = next(h for h in headers if h["active"])
    inactive = next(h for h in headers if not h["active"])
    assert active["iteration"] > inactive["iteration"]
    assert active["block_count"] == (database.stat().st_size - 12288) // 262144
    assert 0 <= (active["metadata_pointer"] & ((1 << 56) - 1)) < active["block_count"]
    assert result["bytes_read"] == 274432
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("offset", [-1, 0, 12289, 999999999999])
def test_invalid_offsets_are_rejected(database, offset):
    with pytest.raises(ValueError):
        probe(database, offset)
