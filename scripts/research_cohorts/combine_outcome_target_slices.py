"""Combine and validate deterministic outcome-target sidecar artifacts."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


KEY = ("db_name", "year", "week", "NFL_player_id", "manager")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--start-row", type=int, required=True)
    ap.add_argument("--end-row", type=int, required=True)
    args = ap.parse_args()
    files = sorted(args.input_dir.rglob("*.parquet"))
    if not files:
        raise SystemExit("no parquet target slices found")
    if args.start_row < 0 or args.end_row <= args.start_row:
        raise SystemExit("invalid ordinal range")
    paths = ", ".join("'" + str(p.resolve()).replace("'", "''") + "'" for p in files)
    con = duckdb.connect()
    cols = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{paths}])").fetchall()}
    missing = set(KEY) - cols
    if missing:
        raise SystemExit(f"target slices missing key columns: {sorted(missing)}")
    count, distinct, lo, hi = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT ({', '.join(KEY)})),
               MIN(target_ordinal), MAX(target_ordinal)
        FROM read_parquet([{paths}])
    """).fetchone()
    expected = args.end_row - args.start_row
    if count != distinct:
        raise SystemExit(f"duplicate target keys: {count - distinct}")
    if lo != args.start_row or hi != args.end_row - 1 or count != expected:
        raise SystemExit(
            f"non-contiguous target range: count={count}, ordinal={lo}..{hi}, "
            f"expected={args.start_row}..{args.end_row - 1}"
        )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM read_parquet([{paths}]) ORDER BY target_ordinal)
        TO '{str(args.out.resolve()).replace("'", "''")}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"combined {count:,} unique targets ({lo:,}..{hi:,}) -> {args.out}")


if __name__ == "__main__":
    main()
