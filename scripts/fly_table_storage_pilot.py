"""One-table, fail-closed storage pilot on an existing isolated recovery volume.

No production target, database copying, reaggregation, retry or checksum rewrite.
The caller also enforces an OS deadline (including provisioning) of 40 seconds.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import struct
import tempfile
import threading
import time
from pathlib import Path

STARTED = time.monotonic()
TARGETS = {"homepage_manager_rankings", "matchup_h2h_career",
           "player_fantasy_season", "player_fantasy_season_all", "standings_by_year"}
PRIMARY_MACHINE = "1781e011b69068"
PRIMARY_VOLUME = "vol_rkg7mmd17llez224"
DATABASE_PATH = Path("/data/___leagues.duckdb")
RETAINED_BLOCK_LIMIT = 65536


def emit(stage, **fields):
    print(json.dumps({"stage": stage, "elapsed_s": round(time.monotonic() - STARTED, 3), **fields}), flush=True)


def arm_deadline(deadline):
    remaining = deadline - time.time()
    if not 0 < remaining <= 40:
        raise ValueError("deadline must be in the next 40 seconds")

    def expire():
        # Never wait for stdout/stderr here: a broken pipe can block logging.
        os._exit(124)

    timer = threading.Timer(remaining, expire)
    timer.daemon = True
    timer.start()
    emit("deadline_armed", deadline_epoch=deadline, timeout_exit_code=124)
    return timer


def validate_target(action, table, db_name, machine, volume, path=DATABASE_PATH):
    if machine == PRIMARY_MACHINE or volume == PRIMARY_VOLUME:
        raise ValueError("primary target forbidden")
    if not machine or not volume.startswith("vol_"):
        raise ValueError("isolated machine and volume are required")
    if action in {"donor_headers", "retained_headers"} and volume != "vol_vp26dp2g9x3167j4":
        raise ValueError("donor volume is not the existing September 15 witness")
    if action not in {"inspect", "locate", "remove", "donor_headers", "retained_headers"} or table not in TARGETS:
        raise ValueError("target table/action is not allowlisted")
    if not re.fullmatch(r"[a-z0-9_]+", db_name):
        raise ValueError("invalid witness league")
    if path != DATABASE_PATH:
        raise ValueError("database path is not allowlisted")


def probe_metadata_donor(path, expected_checksum):
    """Inspect registered metadata only; retain the donor database and its WAL.

    The temporary hard link consumes no database copy. A read-only connection
    under that name inspects only the checkpoint, not the original name's WAL.
    This is forensic evidence, never a current publication or repair source.
    """
    import duckdb
    from fly_duckdb_block_probe import probe

    path = Path(path)
    before = path.stat()
    with tempfile.TemporaryDirectory(prefix="metadata_probe_", dir=path.parent) as folder:
        link = Path(folder) / "checkpoint.duckdb"
        os.link(path, link)
        if not os.path.samefile(path, link):
            raise ValueError("checkpoint probe must share the original inode")
        emit("donor_catalog_open", checkpoint_only=True, read_only=True)
        with duckdb.connect(str(link), read_only=True, config={"threads": "1", "memory_limit": "512MB"}) as conn:
            ids = [row[0] for row in conn.execute(
                "SELECT block_id FROM pragma_metadata_info() ORDER BY block_id LIMIT 4097"
            ).fetchall()]
        if len(ids) > 4096:
            raise ValueError("metadata header probe exceeds 4096-block ceiling")
        emit("donor_headers_start", metadata_blocks=len(ids))
        matches = []
        with path.open("rb", buffering=0) as stream:
            for block_id in ids:
                offset = 12288 + block_id * 262144
                if block_id < 0 or offset + 262144 > before.st_size:
                    raise ValueError("metadata pointer outside donor file")
                stream.seek(offset)
                data = stream.read(8)
                if len(data) != 8:
                    raise ValueError("short metadata checksum read")
                if struct.unpack("<Q", data)[0] == expected_checksum:
                    matches.append(offset)
        if len(matches) > 2:
            raise ValueError("ambiguous checksum has more than two candidate blocks")
        candidates = [probe(path, offset) for offset in matches]
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("donor file changed during inspection")
    return {"checkpoint_only": True, "metadata_blocks_checked": len(ids),
            "header_bytes_read": len(ids) * 8, "candidates": candidates, "repair_authorized": False}


def probe_retained_donor(path, expected_checksum):
    """Read eight-byte headers, including unregistered retained blocks; no SQL.

    Logical bytes read are reported, not filesystem physical I/O. Only up to two
    matching blocks receive a full checksum check. A match never authorizes repair.
    """
    from fly_duckdb_block_probe import probe
    path = Path(path)
    before = path.stat()
    count, remainder = divmod(before.st_size - 12288, 262144)
    if remainder or count < 1 or count > RETAINED_BLOCK_LIMIT:
        raise ValueError("donor file exceeds header ceiling or is not block aligned")
    initial = probe(path, 12288)
    if initial["file_changed_during_read"]:
        raise ValueError("donor file changed during initial inspection")
    matches = []
    emit("retained_headers_start", physical_blocks=count, header_bytes=count * 8)
    with path.open("rb", buffering=0) as stream:
        for block_id in range(count):
            offset = 12288 + block_id * 262144
            stream.seek(offset)
            data = stream.read(8)
            if len(data) != 8:
                raise ValueError("short retained checksum read")
            if struct.unpack("<Q", data)[0] == expected_checksum:
                matches.append(offset)
                if len(matches) > 2:
                    raise ValueError("ambiguous checksum has more than two candidate blocks")
    candidates = [probe(path, offset) for offset in matches]
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("donor file changed during inspection")
    return {"physical_blocks_checked": count, "header_bytes_read": count * 8,
            "validation_bytes_read": initial["bytes_read"] + sum(c["bytes_read"] for c in candidates),
            "candidates": candidates, "repair_authorized": False}


def run(args):
    validate_target(args.action, args.target_table, args.db_name, args.machine_id, args.volume_id)
    timer = arm_deadline(args.deadline)
    conn = None
    try:
        from fly_duckdb_block_probe import probe

        emit("block_probe_start", action=args.action, table=args.target_table)
        block = probe(DATABASE_PATH, 90714112)
        emit("block_probe", **block)
        if args.action == "inspect":
            if block["file_changed_during_read"]:
                raise ValueError("candidate changed during read")
            return 0
        if args.action == "retained_headers":
            if block["file_changed_during_read"]:
                raise ValueError("donor changed during initial block read")
            emit("retained_result", **probe_retained_donor(DATABASE_PATH, 18392342689821271652))
            return 0
        import duckdb

        if duckdb.__version__ != "1.5.4":
            raise ValueError("pilot must use production DuckDB 1.5.4")
        if args.action == "donor_headers":
            if block["file_changed_during_read"]:
                raise ValueError("donor changed during initial block read")
            emit("donor_result", **probe_metadata_donor(DATABASE_PATH, 18392342689821271652))
            return 0
        if block["file_changed_during_read"] or block["block_sha256"] != "7bbcf166a70b06eb12c19888577060bf17e867a6f8b81ac7b802bffb3cab1186":
            raise ValueError("target is not the exact previously observed damaged block")
        if args.action == "remove" and Path(str(DATABASE_PATH) + ".wal").exists():
            raise ValueError("retained WAL present: do not replay/checkpoint it as a table-removal pilot")
        emit("connect_start", read_only=args.action == "locate")
        conn = duckdb.connect(str(DATABASE_PATH), read_only=args.action == "locate",
                              config={"threads": "1", "memory_limit": "512MB"})
        quarantine = "__corrupt_recovery_" + args.target_table
        present = conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name IN (?, ?)",
                               [args.target_table, quarantine]).fetchall()
        names = {row[0] for row in present}
        target = quarantine if quarantine in names else args.target_table
        if target not in names:
            raise ValueError("expected target missing")
        emit("metadata_probe_start", table=target)
        try:
            conn.execute(f"SELECT column_name FROM pragma_storage_info('public.{target}') LIMIT 1").fetchall()
        except duckdb.IOException as exc:
            if "location 90714112" not in str(exc):
                raise
            emit("located", result="CONFIRMED", error=str(exc).splitlines()[0])
        else:
            raise ValueError("target did not reproduce the observed corruption")
        if args.action == "locate":
            return 0
        if names != {args.target_table, quarantine}:
            raise ValueError("removal requires both the quarantined old object and its canonical replacement")
        witness_sql = "SELECT year, COUNT(*) FROM public.matchup WHERE db_name=? GROUP BY year ORDER BY year"
        before = conn.execute(witness_sql, [args.db_name]).fetchall()
        if not before:
            raise ValueError("unrelated fact witness is empty")
        emit("remove_start", table=quarantine)
        conn.execute("BEGIN TRANSACTION")
        conn.execute(f'DROP TABLE public."{quarantine}"')
        emit("drop_statement_finished")
        conn.execute("COMMIT")
        emit("drop_committed")
        conn.execute("CHECKPOINT")
        emit("checkpoint_finished")
        conn.close()
        conn = duckdb.connect(str(DATABASE_PATH), read_only=True)
        after = conn.execute(witness_sql, [args.db_name]).fetchall()
        if after != before:
            raise ValueError("unrelated fact witness changed")
        if conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='public' AND table_name=?", [quarantine]).fetchall():
            raise ValueError("removed object remains after reopen")
        emit("remove_verified", result="PASS", witness_seasons=len(after))
        return 0
    finally:
        try:
            if conn is not None:
                conn.close()
        finally:
            timer.cancel()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", required=True, choices=["inspect", "locate", "remove", "donor_headers", "retained_headers"])
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--machine-id", required=True)
    parser.add_argument("--volume-id", required=True)
    parser.add_argument("--deadline", type=float, required=True)
    args = parser.parse_args()
    try:
        return run(args)
    except Exception as exc:
        emit("pilot", result="BLOCKED", error=type(exc).__name__ + ": " + str(exc).splitlines()[0])
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
