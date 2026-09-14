"""
sota_recon/build_denorm_ranks_from_super_v26.py  --  recompute + denormalize ALL season/career
rank columns on the weekly super table, using the SAME logic as the weekly-update system
(build_full_ops) so the two can never disagree.

Needed after the kicker (pts_k_std) and primary_position changes: the denormalized
rank_season_* / rank_alltime_* columns are stale. This rebuilds them by:
  1. building the season and career aggregates straight off the super table (career ranks by
     the stored primary_position, season by the per-year position) via build_full_ops helpers,
  2. running the canonical rank jobs on those small aggregates,
  3. streaming the super table and REPLACE-ing every rank_season_*/rank_alltime_* from the
     joined aggregates (build_denormalize_ranks_v26's pattern -- memory-bounded, no 1,010-col
     materialization).

PPG/consistency/weighted/next_year columns are untouched (unaffected by these changes).
Gated: golden holds, row count unchanged, every rank col constant within its scope,
season/career ranks monotonic in their aggregate points -> backup + swap.

    python -m scripts.sota_recon.build_denorm_ranks_from_super_v26          # dry-run
    python -m scripts.sota_recon.build_denorm_ranks_from_super_v26 --apply
"""
from __future__ import annotations
import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

_FFS = Path(__file__).resolve().parents[2] / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))

from multi_league.data_fetchers.build_full_ops import (  # noqa: E402
    _build_scoped_aggregate,
    _rank_scoped_aggregate,
    qident,
    scoped_rank_jobs,
)
from .sources import latest_v26  # noqa: E402
from .recon_common import utc_stamp  # noqa: E402

PROV = "wave52.denorm_ranks_primarypos"
PROV_COL = "recon_correction_log"


def _rank_cols(con, src: str, prefix: str) -> list[str]:
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    return sorted(c for c in cols if c.startswith(prefix))


def _build_ref(con, src: str, scope: str, ref_name: str, want: list[str]) -> list[str]:
    """Aggregate + rank via build_full_ops helpers; return (key + ranks) into ref_name."""
    _build_scoped_aggregate(con, src, scope, None)
    wanted = {c for c, _, _ in scoped_rank_jobs(scope)} & set(want)
    produced = _rank_scoped_aggregate(con, scope, wanted)
    missing = sorted(set(want) - set(produced))
    if missing:
        raise RuntimeError(f"{scope}: rank jobs did not cover {missing}")
    key = "NFL_player_id, year" if scope == "season" else "NFL_player_id"
    con.execute(f"DROP TABLE IF EXISTS {ref_name}")
    con.execute(f"CREATE TABLE {ref_name} AS SELECT {key}, {', '.join(qident(c) for c in produced)} FROM _scoped_agg")
    return produced


def run(apply: bool) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=1"); con.execute("SET memory_limit='9GB'")
    con.execute("SET preserve_insertion_order=false")
    # A VIEW (not a materialized table) so build_full_ops helpers can DESCRIBE it by name
    # without loading the 1,010-column table into memory.
    con.execute(f"CREATE VIEW _super AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    src = "_super"
    before = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

    season_want = _rank_cols(con, src, "rank_season_")
    career_want = _rank_cols(con, src, "rank_alltime_")
    # Career first (season rebuild drops _scoped_agg).
    career_cols = _build_ref(con, src, "alltime", "_car_ref", career_want)
    season_cols = _build_ref(con, src, "season", "_sea_ref", season_want)
    info = {"season_rank_cols": len(season_cols), "career_rank_cols": len(career_cols)}
    if not apply:
        con.close()
        return {"before": int(before), "info": info, "swapped": False}

    repl = [f"se.{qident(c)} AS {qident(c)}" for c in season_cols] + [f"ca.{qident(c)} AS {qident(c)}" for c in career_cols]
    repl.append(
        f"CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
        f"WHEN {PROV_COL} LIKE '%{PROV}%' THEN {PROV_COL} ELSE {PROV_COL}||',{PROV}' END AS {PROV_COL}"
    )
    out = f"""
        SELECT s.* REPLACE ({', '.join(repl)})
        FROM {src} s
        LEFT JOIN _sea_ref se ON s."NFL_player_id" = se."NFL_player_id" AND CAST(s.year AS INTEGER) = se.year
        LEFT JOIN _car_ref ca ON s."NFL_player_id" = ca."NFL_player_id"
    """
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_denomrank.parquet")
    rdr = con.execute(out).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close()
    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    # constancy gate: every rebuilt rank constant within its scope
    bad_const = 0
    for c in season_cols:
        bad_const += con.execute(f"""SELECT COUNT(*) FROM (SELECT "NFL_player_id", year FROM {tq}
            WHERE {qident(c)} IS NOT NULL GROUP BY 1,2 HAVING COUNT(DISTINCT {qident(c)})>1)""").fetchone()[0]
    for c in career_cols:
        bad_const += con.execute(f"""SELECT COUNT(*) FROM (SELECT "NFL_player_id" FROM {tq}
            WHERE {qident(c)} IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT {qident(c)})>1)""").fetchone()[0]
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before and bad_const == 0)
    res = {"before": int(before), "after": int(after), "info": info, "rank_not_constant": int(bad_const),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predenomrank_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN:", r["info"])
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | {r['info']} | not_constant {r['rank_not_constant']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
