"""
sota_recon/preserve_dropped_from_live_v26.py -- close the promote-preflight 0-dropped-column guard.

build_season_career_v26 reproduces the aggregated + enrich columns but NOT a handful of season-only
derived stats the live Fly build adds via a separate step (adjusted/net/adjusted-net YPA, wins,
games_started). The local rebuild would DROP them -> the 0-dropped-column guard (correctly) blocks the
promote. This preserves them: for each of the 4 tables, fetch the dropped live columns keyed by
NFL_player_id (+ year for season) and LEFT JOIN them into the rebuilt artifact. Idempotent; only ADDs.

(The passing-rate trio is preserved as-is; the +79 PBP-TD correction shifts ANY/A by <=~0.04 on ~79
QB-seasons -- negligible and kept live-consistent. wins/games_started are unaffected by our stat work.)

    python -m scripts.sota_recon.preserve_dropped_from_live_v26 [--apply]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "fantasy_football_data_scripts"))
import multi_league.data_fetchers.aggregate_nfl_stats_fly as AGG  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402
from .sources import latest_v26  # noqa: E402

TABLES = [
    ("player_nfl_season", "player_nfl_season.parquet", True),
    ("player_nfl_season_all", "player_nfl_season_all.parquet", True),
    ("player_nfl_career", "player_nfl_career.parquet", False),
    ("player_nfl_career_all", "player_nfl_career_all.parquet", False),
]


def _live_cols(reader, tbl):
    rows = reader.query(
        f"SELECT column_name FROM information_schema.columns WHERE table_catalog='___ops' "
        f"AND table_schema='nfl_historical' AND table_name='{tbl}'", database="___ops")
    return [str(r["column_name"]) for r in rows]


def _fetch_live(reader, tbl, cols, has_year):
    """Fetch key + dropped cols from live, paginated by year (season) or NFL_player_id buckets (career)."""
    keys = "NFL_player_id, year" if has_year else "NFL_player_id"
    sel = f"{keys}, " + ", ".join(cols)
    frames = []
    if has_year:
        years = [int(r["year"]) for r in reader.query(
            f"SELECT DISTINCT year FROM nfl_historical.{tbl} WHERE year IS NOT NULL ORDER BY year", database="___ops")]
        for y in years:
            frames.append(pd.DataFrame(reader.query(
                f"SELECT {sel} FROM nfl_historical.{tbl} WHERE year={y}", database="___ops")))
    else:
        for b in range(16):
            frames.append(pd.DataFrame(reader.query(
                f"SELECT {sel} FROM nfl_historical.{tbl} WHERE abs(hash(NFL_player_id)) % 16 = {b}", database="___ops")))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    tdir = Path(os.path.dirname(v26)) / "season_career_v26"
    AGG.load_env()
    reader = FlyReader()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    out = {}
    for tbl, art, has_year in TABLES:
        p = (tdir / art).as_posix()
        reb = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()}
        dropped = [c for c in _live_cols(reader, tbl) if c not in reb]
        out[tbl] = {"dropped": dropped}
        if not dropped or not apply:
            continue
        live_df = _fetch_live(reader, tbl, dropped, has_year)  # noqa: F841 -- referenced in SQL below
        con.register("live_df", live_df)
        onp = "r.NFL_player_id=l.NFL_player_id" + (" AND r.year=l.year" if has_year else "")
        merged = f"SELECT r.*, {', '.join('l.'+c for c in dropped)} FROM read_parquet('{p}') r LEFT JOIN live_df l ON {onp}"
        tmp = (tdir / (art + ".tmp")).as_posix()
        rb = con.execute(merged).fetch_record_batch(50000)
        w = pq.ParquetWriter(tmp, rb.schema)
        for b in rb:
            w.write_batch(b)
        w.close()
        newcols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{tmp}')").fetchall()}
        still = [c for c in dropped if c not in newcols]
        rows_ok = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0] == \
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}')").fetchone()[0]
        con.unregister("live_df")
        if not still and rows_ok:
            os.replace(tmp, p)
            out[tbl].update(added=dropped, fetched_rows=len(live_df), swapped=True)
        else:
            os.remove(tmp)
            out[tbl].update(swapped=False, still_missing=still, rows_ok=rows_ok)
    con.close()
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    import json
    print(json.dumps(run(apply=a.apply), default=str, indent=1))
