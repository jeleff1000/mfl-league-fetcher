"""
sota_recon/build_season_career_v26.py  --  rebuild the season/career aggregate tables
LOCALLY on v26, reusing the CANONICAL Fly aggregation SQL (aggregate_nfl_stats_fly.py).

Why: the weekly v26 super table is now complete (corrected atoms + 28 fpts incl ppfd/tep +
pts_def_* + 65 weekly ranks + 56 LAMAR). The four NFL aggregate caches
  player_nfl_season / player_nfl_season_all / player_nfl_career / player_nfl_career_all
are derived FROM the weekly table and must be re-rolled so season/alltime ranks, ppg, and
the 56 LAMAR configs (incl the new ppfd/tep formats) propagate to season/career grain.

This is NOT a second code path: it imports the exact canonical functions
(discover_aggregate_columns, build_stage_table, recompute_ranks, rank_specs_for_scope,
verify_count, swap_stage_to_live) and only swaps the EXECUTOR -- FlyWriter (HTTP -> Fly
DuckDB) for a LocalWriter (a local duckdb.Connection where '___ops' is an attached catalog
and nfl_player_stats_all is a view over the v26 parquet). The generated SQL is identical to
what runs on Fly at deploy, so this both produces local artifacts and pre-verifies the ship.

Faithful inputs pulled read-only from Fly (so the result matches production exactly):
  * existing player_nfl_season / player_nfl_career schemas  -> discover_aggregate_columns()
    needs the curated set of numeric columns already living in the season/career tables.
  * nfl_player_season_stat_facts (9,022 season-only rows, 30 stats) -> season fact adjustments.

Outputs: writes the 4 rebuilt tables to parquet next to v26 (in a season_career_v26/ dir),
restores local enrich/position overlays, prints verify_count + populated-coverage of the
new format columns. Ships NOTHING to Fly.

    python -m scripts.sota_recon.build_season_career_v26 [--apply]
"""

from __future__ import annotations
import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

_FFS = Path(__file__).resolve().parent.parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))

import multi_league.data_fetchers.aggregate_nfl_stats_fly as AGG
from multi_league.core.readers.fly_reader import FlyReader


class LocalWriter:
    """FlyWriter-compatible executor backed by a local duckdb.Connection.

    Implements .execute(sql, database) -> list[dict]. The `database` kwarg is ignored:
    all canonical SQL uses 3-part names (___ops.schema.table), so the connection's default
    catalog is irrelevant. SELECTs return list of dicts keyed by column name; DDL/DML return [].
    """

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def execute(self, sql: str, database: str = "___ops") -> list[dict]:
        cur = self.con.execute(sql)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _pull_schema(reader: FlyReader, table: str) -> list[tuple[str, str]]:
    rows = reader.query(
        f"""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog='___ops' AND table_schema='nfl_historical' AND table_name='{table}'
        ORDER BY ordinal_position
        """,
        database="___ops",
    )
    return [(str(r["column_name"]), str(r["data_type"])) for r in rows]


def _seed_empty(con: duckdb.DuckDBPyConnection, full: str, schema: list[tuple[str, str]]) -> None:
    cols = ", ".join(f'"{n}" {t}' for n, t in schema)
    con.execute(f"CREATE OR REPLACE TABLE {full} ({cols})")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    AGG.load_env()
    reader = FlyReader()  # read-only; needs DATABASE_SERVER_URL + DATABASE_READ_TOKEN from .env

    print("[fly] pulling existing season/career schemas + season fact sidecar (read-only)", flush=True)
    season_schema = _pull_schema(reader, "player_nfl_season")
    career_schema = _pull_schema(reader, "player_nfl_career")
    fact_rows = reader.query("SELECT * FROM nfl_historical.nfl_player_season_stat_facts", database="___ops")
    print(
        f"  season cols={len(season_schema)} career cols={len(career_schema)} fact rows={len(fact_rows):,}", flush=True
    )

    if not apply:
        return {
            "v26": v26,
            "season_cols": len(season_schema),
            "career_cols": len(career_schema),
            "fact_rows": len(fact_rows),
            "season_specs": len(AGG.rank_specs_for_scope("season")),
            "alltime_specs": len(AGG.rank_specs_for_scope("alltime")),
        }

    stamp = utc_stamp()
    work = Path(os.path.dirname(v26)) / f".season_career_build_{stamp}"
    work.mkdir(parents=True, exist_ok=True)
    out = Path(os.path.dirname(v26)) / "season_career_v26"
    out.mkdir(parents=True, exist_ok=True)
    dbfile = work / "ops.duckdb"

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='10GB'")
    con.execute(f"SET temp_directory='{work.as_posix()}'")
    con.execute(f"ATTACH '{dbfile.as_posix()}' AS \"___ops\"")
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".nfl_historical')
    con.execute('CREATE SCHEMA IF NOT EXISTS "___ops".public')

    # super table as a view over v26 parquet (no physical copy)
    con.execute(
        f'CREATE OR REPLACE VIEW "___ops".nfl_historical.nfl_player_stats_all '
        f"AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')"
    )
    # seed existing season/career schemas (names/types) for discover_aggregate_columns
    _seed_empty(con, '"___ops".nfl_historical.player_nfl_season', season_schema)
    _seed_empty(con, '"___ops".nfl_historical.player_nfl_career', career_schema)
    # season-only fact sidecar (real data) for fact adjustments
    if fact_rows:
        import pandas as pd

        fact_df = pd.DataFrame(fact_rows)  # noqa: F841 -- referenced by name in SQL below
        con.execute(
            'CREATE OR REPLACE TABLE "___ops".nfl_historical.nfl_player_season_stat_facts AS SELECT * FROM fact_df'
        )

    writer = LocalWriter(con)

    print("[validate] rank specs vs canonical column constants", flush=True)
    AGG.validate_rank_specs()

    print("[discover] aggregate / lamar / fpts / bonus columns", flush=True)
    aggregate_cols, lamar_cols, fpts_cols, bonus_cols = AGG.discover_aggregate_columns(writer)
    use_fact = AGG.table_exists(writer, AGG.SEASON_FACT_TABLE)
    print(
        f"  aggregate={len(aggregate_cols)} lamar={len(lamar_cols)} fpts={len(fpts_cols)} "
        f"bonus={len(bonus_cols)} season_fact_adjustments={'on' if use_fact else 'off'}",
        flush=True,
    )

    targets = [
        (AGG.SEASON_TABLE, False, True),
        (AGG.SEASON_ALL_TABLE, True, True),
        (AGG.CAREER_TABLE, False, False),
        (AGG.CAREER_ALL_TABLE, True, False),
    ]
    for table, include_playoffs, season in targets:
        t0 = time.time()
        print(f"[build] {table.split('.')[-1]} (playoffs={include_playoffs})", flush=True)
        AGG.build_stage_table(
            writer,
            table,
            aggregate_cols=aggregate_cols,
            lamar_cols=lamar_cols,
            include_playoffs=include_playoffs,
            season=season,
            year=None,
            use_season_fact_adjustments=use_fact,
        )
        AGG.swap_stage_to_live(writer, table)
        print(f"  [{table.split('.')[-1]}] built in {time.time() - t0:.0f}s", flush=True)

    season_specs = AGG.rank_specs_for_scope("season")
    alltime_specs = AGG.rank_specs_for_scope("alltime")
    for table, _, season in targets:
        specs = season_specs if season else alltime_specs
        print(f"[rank] {table.split('.')[-1]} ({len(specs)} specs)", flush=True)
        AGG.recompute_ranks(writer, table, specs, "season" if season else "alltime")

    counts = {}
    for table, include_playoffs, season in targets:
        counts[table.split(".")[-1]] = AGG.verify_count(writer, table, include_playoffs=include_playoffs, season=season)
    print("[verify] counts:", counts, flush=True)

    # populated-coverage of the NEW format columns (ppfd / tep ranks) + a sample lamar
    new_rank_cols = [s.col for s in season_specs if "ppfd" in s.col or "tep" in s.col]
    cov = {}
    for table, _, _ in targets:
        tname = table.split(".")[-1]
        existing = {
            c["column_name"]
            for c in writer.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_catalog='___ops' "
                f"AND table_schema='nfl_historical' AND table_name='{tname}'"
            )
        }
        scope_cols = [
            c if tname.startswith("player_nfl_season") else c.replace("rank_season_", "rank_alltime_")
            for c in new_rank_cols
        ]
        present = [c for c in scope_cols if c in existing]
        nonnull = {}
        for c in present[:4]:
            nonnull[c] = writer.execute(f'SELECT COUNT(*) AS n FROM {table} WHERE "{c}" IS NOT NULL')[0]["n"]
        cov[tname] = {"new_rank_cols_present": len(present), "sample_nonnull": nonnull}
    print("[verify] new-format coverage:", cov, flush=True)

    # export the 4 tables to parquet artifacts
    exported = {}
    for table, _, _ in targets:
        tname = table.split(".")[-1]
        dest = out / f"{tname}.parquet"
        rb = con.execute(f"SELECT * FROM {table}").fetch_record_batch(50000)
        w = pq.ParquetWriter(str(dest), rb.schema)
        for b in rb:
            w.write_batch(b)
        w.close()
        exported[tname] = str(dest)
    con.close()
    print("[export] wrote artifacts:", exported, flush=True)

    print("[post] restoring season/career enrich overlay", flush=True)
    from . import build_enrich_season_career_v26 as ENRICH

    enrich = ENRICH.run(apply=True)
    if not enrich.get("gate_pass"):
        raise RuntimeError(f"season/career enrich overlay failed: {enrich}")

    print("[post] restoring compact position eligibility overlay", flush=True)
    from . import build_position_eligibility_v26 as POS

    position = POS.run(apply=True)
    if not position.get("gate_pass"):
        raise RuntimeError(f"position eligibility overlay failed: {position}")

    # The live Fly build adds 5 season-only columns this rebuild has no weekly source for
    # (adjusted/net/adjusted-net YPA, wins, games_started). Without them the promote's
    # 0-dropped-column preflight guard blocks. Preserve them from live HERE, structurally,
    # so no rollup can forget the step (idempotent: no-op when nothing is dropped).
    print("[post] preserving live-only season/career columns", flush=True)
    from . import preserve_dropped_from_live_v26 as PRESERVE

    preserved = PRESERVE.run(apply=True)
    for tbl, info in preserved.items():
        if info.get("dropped") and not info.get("swapped"):
            raise RuntimeError(f"live-column preservation failed for {tbl}: {info}")

    return {
        "preserved": {t: i.get("dropped", []) for t, i in preserved.items()},
        "counts": counts,
        "coverage": cov,
        "exported": exported,
        "work": str(work),
        "lamar_cols": len(lamar_cols),
        "season_specs": len(season_specs),
        "enrich": enrich,
        "position": position,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(apply=a.apply)
    if not a.apply:
        print("DRY RUN:", r)
        print("would rebuild player_nfl_season(_all) + player_nfl_career(_all) locally on v26")
    else:
        print("DONE:", {k: r[k] for k in ("counts", "season_specs", "lamar_cols")})
        print("coverage:", r["coverage"])
        print("artifacts:", r["exported"])
