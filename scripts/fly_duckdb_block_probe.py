"""Read one physical block; report checksums only, never repair or open DuckDB.

Checksum/layout reference: DuckDB v1.5.4 src/common/checksum.cpp and
src/storage/single_file_block_manager.cpp. This is a diagnostic, not proof
that any candidate block is safe to transplant into another database.
"""
import argparse
import hashlib
import json
import os
import struct
from pathlib import Path


def checksum(payload):
    if len(payload) % 8:
        raise ValueError("Expected an aligned DuckDB block payload")
    result = 5381
    for (word,) in struct.iter_unpack("<Q", payload):
        result ^= (word * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    return result


def probe(path, offset):
    with Path(path).open("rb") as stream:
        before = os.fstat(stream.fileno())
        headers = stream.read(12288)
        if len(headers) != 12288 or headers[8:12] != b"DUCK":
            raise ValueError("Not a supported DuckDB file header")
        pages = [headers[start:start + 4096] for start in (0, 4096, 8192)]
        if not all(struct.unpack_from("<Q", page)[0] == checksum(page[8:]) for page in pages):
            raise ValueError("Invalid or unsupported database header checksum")
        active = max(pages[1:], key=lambda page: struct.unpack_from("<Q", page, 8)[0])
        block_size = struct.unpack_from("<Q", active, 40)[0] or 262144
        if block_size != 262144:
            raise ValueError("Only the observed 256 KiB block size is supported")
        if offset < 12288 or (offset - 12288) % block_size or offset + block_size > before.st_size:
            raise ValueError("Block offset is outside the file or misaligned")
        stream.seek(offset)
        block = stream.read(block_size)
        after = os.fstat(stream.fileno())
    if len(block) != block_size:
        raise ValueError("Short block read")
    stored = struct.unpack_from("<Q", block)[0]
    computed = checksum(block[8:])
    syndrome = stored ^ computed
    # Multiplication by an odd constant preserves the lowest changed bit.
    # Thus a single-bit fault can only flip the lowest set bit of the syndrome.
    # Count every matching word: the XOR checksum cannot identify an offset
    # uniquely in general, and even a single candidate is not a repair witness.
    payload_candidates = 0
    if syndrome:
        bit = syndrome & -syndrome
        mask = (1 << 64) - 1
        multiplier = 0xBF58476D1CE4E5B9
        for (word,) in struct.iter_unpack("<Q", block[8:]):
            difference = ((word * multiplier) & mask) ^ (((word ^ bit) * multiplier) & mask)
            payload_candidates += difference == syndrome
    return {
        "offset": offset,
        "block_id": (offset - 12288) // block_size,
        "block_size": block_size,
        "bytes_read": len(headers) + len(block),
        "file_size": before.st_size,
        "file_changed_during_read": (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns),
        "main_header_sha256": hashlib.sha256(pages[0]).hexdigest(),
        "block_sha256": hashlib.sha256(block).hexdigest(),
        "stored_checksum": stored,
        "computed_checksum": computed,
        "checksum_valid": stored == computed,
        "single_bit_payload_candidates": payload_candidates,
        "single_bit_checksum_candidate": bool(syndrome and syndrome.bit_count() == 1),
        "repair_authorized": False,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path")
    parser.add_argument("--offset", type=int, default=90714112)
    args = parser.parse_args()
    print(json.dumps(probe(args.path, args.offset), sort_keys=True))
