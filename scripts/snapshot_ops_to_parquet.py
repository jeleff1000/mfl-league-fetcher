"""Snapshot the small ___ops tables from Fly to parquet files committed in repo.

Scope (MVP):
  * nfl_historical.player_bio  -> ops_data/nfl_historical/player_bio.parquet
  * public.yahoo_nfl_player_map  -> ops_data/public/yahoo_nfl_player_map.parquet
  * public.sleeper_nfl_player_map -> ops_data/public/sleeper_nfl_player_map.parquet
  * public.espn_nfl_player_map   -> ops_data/public/espn_nfl_player_map.parquet

These are the tables that actually change when we fix identity-resolution
bugs. Total snapshot is ~2.4 MB. Commits show row-level diffs.

The super_table (nfl_player_stats_all, ~800K rows × ~350 cols) intentionally
stays on Fly. It's too heavy to re-snapshot on each correction and rarely
changes. Workers continue to read it via the existing GH Actions ops cache
(restored from a prior build) — see fantasy_football_data_scripts/
build_ops_cache.py and the workflow.

After fixing identity-resolution data on Fly, run:

    python scripts/snapshot_ops_to_parquet.py

Then commit the parquet diff. PR diff stat shows what rows changed.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

import duckdb  # noqa: E402

from multi_league.core.fly_writer import FlyWriter  # noqa: E402

OUT_DIR = ROOT / "ops_data"

# Columns to KEEP for the super_table snapshot. The full table is ~350 cols;
# we don't need most of them in the import path. Confirm the actual list on
# first run and trim further if size is still a problem.
SUPER_TABLE_COLUMNS = "*"  # Start with everything, prune after measuring.


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def fetch_to_parquet(
    writer: FlyWriter,
    sql: str,
    database: str,
    out_path: Path,
    label: str,
    skip_existing: bool = True,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if skip_existing and out_path.exists() and out_path.stat().st_size > 0:
        size_mb = out_path.stat().st_size / 1024 / 1024
        print(f"  [{label}] SKIP (exists, {size_mb:.2f} MB)", flush=True)
        return

    # Retry on Fly /query-rw connection drops (ChunkedEncodingError) — Fly
    # terminates long large-result responses without warning.
    rows = None
    last_exc = None
    for attempt in range(4):
        try:
            t0 = time.time()
            rows = writer.execute(sql, database=database)
            fetch_s = time.time() - t0
            break
        except Exception as e:  # noqa: BLE001 — broad to also catch requests stack
            last_exc = e
            wait = 2 * (2**attempt)
            print(f"  [{label}] retry {attempt + 1}/4 after {wait}s — {type(e).__name__}: {e}", flush=True)
            time.sleep(wait)
    if rows is None:
        print(f"  [{label}] FAIL after retries — {last_exc}", flush=True)
        return
    if not rows:
        print(f"  [{label}] WARN: 0 rows from query", flush=True)
        return

    con = duckdb.connect()
    con.register("data", _rows_to_arrow(rows))
    con.execute(f"COPY (SELECT * FROM data) TO '{out_path.as_posix()}' " "(FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(
        f"  [{label}] {len(rows):>8d} rows -> {out_path.name}  {size_mb:6.2f} MB  (fetch {fetch_s:.1f}s)",
        flush=True,
    )


def _rows_to_arrow(rows: list[dict]):
    import pyarrow as pa

    return pa.Table.from_pylist(rows)


def snapshot_player_bio(writer: FlyWriter) -> None:
    print("[player_bio]")
    fetch_to_parquet(
        writer,
        "SELECT * FROM nfl_historical.player_bio",
        database="___ops",
        out_path=OUT_DIR / "nfl_historical" / "player_bio.parquet",
        label="player_bio",
    )


def snapshot_platform_maps(writer: FlyWriter) -> None:
    print("[platform maps]")
    for platform in ("yahoo", "sleeper", "espn"):
        table = f"{platform}_nfl_player_map"
        fetch_to_parquet(
            writer,
            f"SELECT * FROM public.{table}",
            database="___ops",
            out_path=OUT_DIR / "public" / f"{table}.parquet",
            label=table,
        )


def snapshot_super_table(writer: FlyWriter) -> None:
    """Partition by year. For 1998+ partition further by half-year (regular
    season + post-season) because Fly /query-rw 500s on a full modern year's
    response (~17K rows × ~350 cols)."""
    print("[super_table — partitioned by year]", flush=True)
    years = writer.execute(
        "SELECT DISTINCT year FROM nfl_historical.nfl_player_stats_all " "WHERE year IS NOT NULL ORDER BY year",
        database="___ops",
    )
    out_dir = OUT_DIR / "nfl_historical" / "nfl_player_stats_all"
    out_dir.mkdir(parents=True, exist_ok=True)

    for r in years:
        year = int(r["year"])
        # Pre-1999 fits in a single fetch (verified empirically).
        if year < 1999:
            fetch_to_parquet(
                writer,
                f"SELECT {SUPER_TABLE_COLUMNS} FROM nfl_historical.nfl_player_stats_all " f"WHERE year = {year}",
                database="___ops",
                out_path=out_dir / f"year={year}.parquet",
                label=f"super.{year}",
            )
            continue

        # 1999+: split per-week. Fly /query-rw 500s on full-year responses
        # (~17K-20K rows × ~350 cols). Per-week is ~600-1000 rows.
        for week in list(range(1, 23)) + [None]:
            if week is None:
                where = f"year = {year} AND week IS NULL"
                tag = "nullwk"
            else:
                where = f"year = {year} AND week = {week}"
                tag = f"wk{week:02d}"
            fetch_to_parquet(
                writer,
                f"SELECT {SUPER_TABLE_COLUMNS} FROM nfl_historical.nfl_player_stats_all " f"WHERE {where}",
                database="___ops",
                out_path=out_dir / f"year={year}_{tag}.parquet",
                label=f"super.{year}.{tag}",
            )

    # Year-NULL rows (pre-1920 / unmatched) — keep in a sentinel partition.
    fetch_to_parquet(
        writer,
        f"SELECT {SUPER_TABLE_COLUMNS} FROM nfl_historical.nfl_player_stats_all " "WHERE year IS NULL",
        database="___ops",
        out_path=out_dir / "year=null.parquet",
        label="super.null",
    )


def main() -> None:
    load_env()
    writer = FlyWriter()
    OUT_DIR.mkdir(exist_ok=True)
    snapshot_player_bio(writer)
    snapshot_platform_maps(writer)
    # NOTE: super_table NOT snapshotted here. See module docstring.

    # Summary
    print("\n[summary]", flush=True)
    total_size = 0
    file_count = 0
    for p in sorted(OUT_DIR.rglob("*.parquet")):
        size_mb = p.stat().st_size / 1024 / 1024
        total_size += size_mb
        file_count += 1
    print(f"  {file_count} files, total {total_size:.2f} MB", flush=True)


if __name__ == "__main__":
    main()
