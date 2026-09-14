"""build_local_ops_cache.py -- local ___ops cache for corpus-mode Sleeper imports.

The enrichment pipeline ATTACHes ___ops read-only via OPS_CACHE_PATH. This builds that cache
FULLY LOCAL for the big table (our LAMAR-fixed v26 super table -- no 1.9GB Fly download) and
pulls only the small supporting tables from Fly (bounded reads). Set OPS_CACHE_PATH to the
output and every corpus ingest runs with zero per-league Fly load.

    py -3 scripts/sleeper_corpus/build_local_ops_cache.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path
import duckdb
import pyarrow as pa

ROOT = Path("d:/yahoo_oauth")
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
sys.path.insert(0, str(ROOT / "scripts"))
from sota_recon import sources  # local v26 super table resolver

OUT = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb")
# small tables to pull from Fly ___ops (schema, table); skipped if absent
SMALL = [("nfl_historical", "player_bio"), ("nfl_historical", "player_prior_awards_by_year"),
         ("public", "sleeper_nfl_player_map"), ("public", "yahoo_nfl_player_map"),
         ("public", "espn_nfl_player_map")]


def main() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    from multi_league.core.readers.fly_reader import FlyReader
    fly = FlyReader()

    super_pq = str(sources.latest_v26())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if OUT.exists():
        OUT.unlink()
    con = duckdb.connect(str(OUT))
    con.execute("SET memory_limit='3000MB'")
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
    con.execute("CREATE SCHEMA IF NOT EXISTS public")

    # big table: local v26 parquet (LAMAR-fixed) -- no download
    con.execute(f"CREATE TABLE nfl_historical.nfl_player_stats_all AS SELECT * FROM read_parquet('{Path(super_pq).as_posix()}')")
    n, c = con.execute("SELECT COUNT(*), (SELECT COUNT(*) FROM (DESCRIBE nfl_historical.nfl_player_stats_all)) FROM nfl_historical.nfl_player_stats_all").fetchone()
    print(f"[ops-cache] nfl_player_stats_all (local v26): {n:,} rows x {c} cols")

    # small tables from Fly (bounded)
    for schema, tbl in SMALL:
        try:
            rows = fly.query(f"SELECT * FROM {schema}.{tbl}", "___ops")
        except Exception as e:
            print(f"[ops-cache] {schema}.{tbl}: SKIP ({str(e)[:70]})")
            continue
        if not rows:
            print(f"[ops-cache] {schema}.{tbl}: empty, skip")
            continue
        con.register("t", pa.Table.from_pylist(rows))
        con.execute(f"CREATE OR REPLACE TABLE {schema}.{tbl} AS SELECT * FROM t")
        con.unregister("t")
        print(f"[ops-cache] {schema}.{tbl}: {len(rows):,} rows (from Fly)")

    con.close()
    print(f"\n[done] OPS_CACHE_PATH={OUT}")


if __name__ == "__main__":
    main()
