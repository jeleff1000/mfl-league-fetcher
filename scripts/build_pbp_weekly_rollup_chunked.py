#!/usr/bin/env python3
"""
RAM-safe driver for the weekly PBP rollup.

The one-shot aggregate (aggregate_merged_pbp_for_supertable_audit.main) explodes every
play into per-role event rows and hash-groups all 48 years at once. On a 12 GB box with
~1.7 GB free that GROUP BY blows past memory and Windows fails the buffered temp write
("Insufficient system resources"). Each single year, by contrast, fits in RAM trivially.

This driver builds the bio lookup ONCE, then loops year-by-year: for each season it
rebuilds pbp_base + the weekly rollup and appends pbp_player_week_rollup to per-year
parquet shards. A final pass concatenates the shards into one weekly rollup identical in
schema to the one-shot output. Peak memory stays at one season's worth of plays.

    python scripts/build_pbp_weekly_rollup_chunked.py \
        --out-dir D:/league-history-data/nfl/raw/stathead/generated/pbp_weekly_rollup_1978_2025 \
        --temp-dir D:/league-history-data/nfl/tmp/duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts.aggregate_merged_pbp_for_supertable_audit import (  # noqa: E402
    DEFAULT_BIO,
    DEFAULT_BIO_REPAIRED,
    DEFAULT_PBP,
    create_bio_lookup,
    create_pbp_base,
    create_weekly_rollup,
    lit,
)

_ROLLUP_TEMPS = ["pbp_player_week_rollup", "pbp_player_events", "pbp_base"]


def build_year(con: duckdb.DuckDBPyConnection, pbp: Path, year: int, out_path: Path) -> int:
    for t in _ROLLUP_TEMPS:
        con.execute(f"DROP TABLE IF EXISTS {t}")
    create_pbp_base(con, pbp, year, year)
    create_weekly_rollup(con)
    con.execute(
        f"""COPY (SELECT * FROM pbp_player_week_rollup)
            TO '{lit(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    return int(con.execute("SELECT COUNT(*) FROM pbp_player_week_rollup").fetchone()[0])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--pbp", type=Path, default=DEFAULT_PBP)
    p.add_argument("--bio", type=Path, default=None)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--temp-dir", type=Path, default=None)
    p.add_argument("--year-min", type=int, default=1978)
    p.add_argument("--year-max", type=int, default=2025)
    p.add_argument("--memory-limit", default="3GB")
    p.add_argument("--threads", type=int, default=4)
    args = p.parse_args()

    bio = args.bio or (DEFAULT_BIO_REPAIRED if DEFAULT_BIO_REPAIRED.exists() else DEFAULT_BIO)
    if not args.pbp.exists():
        raise FileNotFoundError(f"PBP not found: {args.pbp}")
    if not bio.exists():
        raise FileNotFoundError(f"Bio not found: {bio}")

    shard_dir = args.out_dir / "by_year"
    shard_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    if args.temp_dir:
        args.temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{lit(args.temp_dir)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{args.memory_limit}'")
    con.execute(f"PRAGMA threads={max(1, int(args.threads))}")

    create_bio_lookup(con, bio)

    total = 0
    for year in range(args.year_min, args.year_max + 1):
        shard = shard_dir / f"weekly_{year}.parquet"
        n = build_year(con, args.pbp, year, shard)
        total += n
        print(f"  {year}: {n:,} player-weeks -> {shard.name}", flush=True)

    combined = args.out_dir / "pbp_player_week_rollup.parquet"
    con.execute(
        f"""COPY (SELECT * FROM read_parquet('{lit(shard_dir)}/weekly_*.parquet'))
            TO '{lit(combined)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
    )
    combined_rows = int(
        con.execute(f"SELECT COUNT(*) FROM read_parquet('{lit(combined)}')").fetchone()[0]
    )
    print(f"\nwrote {combined_rows:,} player-weeks -> {combined}")
    assert combined_rows == total, f"shard sum {total} != combined {combined_rows}"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
