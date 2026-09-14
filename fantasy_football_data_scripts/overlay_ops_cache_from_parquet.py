"""Overlay the small ___ops tables from committed parquets onto ops_cache.duckdb.

Runs in the workers AFTER `Restore ops cache` (which gets super_table from
GH Actions cache) and REPLACES `Apply ops_cache fixups`. The parquets are
the source of truth for player_bio + platform maps; whatever was in the
restored cache for those tables gets dropped + replaced.

Behaviour:
  * Drops + reloads nfl_historical.player_bio from
    ops_data/nfl_historical/player_bio.parquet
  * Drops + reloads public.{yahoo,sleeper,espn}_nfl_player_map from
    ops_data/public/*.parquet
  * Leaves nfl_historical.nfl_player_stats_all untouched (super_table)
  * Non-fatal: if a parquet file is missing, log + skip; the import still
    runs against the previous cache state. Ops cache writes MUST NEVER
    block an import (see feedback memory).

Usage (workers):
    python fantasy_football_data_scripts/overlay_ops_cache_from_parquet.py \
        --cache ops_cache/ops_cache.duckdb
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
OPS_DATA = ROOT / "ops_data"


OVERLAYS = [
    # (parquet_path_relative, schema, table)
    ("nfl_historical/player_bio.parquet", "nfl_historical", "player_bio"),
    ("public/yahoo_nfl_player_map.parquet", "public", "yahoo_nfl_player_map"),
    ("public/sleeper_nfl_player_map.parquet", "public", "sleeper_nfl_player_map"),
    ("public/espn_nfl_player_map.parquet", "public", "espn_nfl_player_map"),
]


def overlay_cache(cache_path: Path) -> None:
    if not cache_path.exists():
        # Cache file doesn't exist yet — workers will build it from
        # build_ops_cache.py first. Skip; this run will be a no-op.
        print(f"[overlay] WARN cache file missing, skipping: {cache_path}", flush=True)
        return

    conn = duckdb.connect(str(cache_path))
    ok = fail = 0

    for rel, schema, table in OVERLAYS:
        parquet = OPS_DATA / rel
        if not parquet.exists():
            print(f"[overlay] WARN parquet missing, skipping: {rel}", flush=True)
            fail += 1
            continue
        try:
            t0 = time.time()
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
            conn.execute(f"CREATE TABLE {schema}.{table} AS " f"SELECT * FROM read_parquet('{parquet.as_posix()}')")
            cnt = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
            print(
                f"[overlay] {schema}.{table}: {cnt} rows " f"({time.time() - t0:.1f}s) <- {rel}",
                flush=True,
            )
            ok += 1
        except Exception as e:  # noqa: BLE001 — overlay must never raise
            fail += 1
            print(f"[overlay] FAIL {schema}.{table}: {e}", flush=True)

    conn.close()
    print(f"[overlay] {ok} applied, {fail} skipped", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="ops_cache/ops_cache.duckdb")
    args = parser.parse_args()
    overlay_cache(Path(args.cache))


if __name__ == "__main__":
    main()
