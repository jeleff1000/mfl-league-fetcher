"""One-table, fail-closed storage pilot on an existing isolated recovery volume.

No production target, database copying, reaggregation, retry or checksum rewrite.
The caller also enforces an OS deadline (including provisioning) of 40 seconds.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
from pathlib import Path

STARTED = time.monotonic()
TARGETS = {"homepage_manager_rankings", "matchup_h2h_career",
           "player_fantasy_season", "player_fantasy_season_all", "standings_by_year"}
PRIMARY_MACHINE = "1781e011b69068"
PRIMARY_VOLUME = "vol_rkg7mmd17llez224"
DATABASE_PATH = Path("/data/___leagues.duckdb")


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
    if action not in {"locate", "remove"} or table not in TARGETS:
        raise ValueError("target table/action is not allowlisted")
    if not re.fullmatch(r"[a-z0-9_]+", db_name):
        raise ValueError("invalid witness league")
    if path != DATABASE_PATH:
        raise ValueError("database path is not allowlisted")


def run(args):
    validate_target(args.action, args.target_table, args.db_name, args.machine_id, args.volume_id)
    timer = arm_deadline(args.deadline)
    conn = None
    try:
        import duckdb
        from fly_duckdb_block_probe import probe

        if duckdb.__version__ != "1.5.4":
            raise ValueError("pilot must use production DuckDB 1.5.4")
        emit("block_probe_start", action=args.action, table=args.target_table, engine=duckdb.__version__)
        block = probe(DATABASE_PATH, 90714112)
        emit("block_probe", **block)
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
    parser.add_argument("--action", required=True, choices=["locate", "remove"])
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
