"""
sota_recon/build_sota_batch_a_v26.py -- crawl-INDEPENDENT SOTA gap-fill batch (one streaming rewrite).

Adds the genuinely-missing derivable composites + fills the near-empty pre-1987 `fumbles` total. All are
PURE in-row derivations (no join) -> a single streaming COPY (scan+project) that stays memory-bounded
(the wide CREATE-TABLE path OOM'd on the local box; a COPY streams). Batched so it's ONE rewrite, not four.

Derivations (deterministic; the script is the provenance):
  - yds_from_scrimmage = rushing_yards + receiving_yards            (super had all_purpose_yards/scrimmage_tds
                                                                      but NOT scrimmage YARDS)
  - rush_receive_td    = rushing_tds + receiving_tds
  - total_tds_scored           = rush + rec + kick-ret + punt-ret + fum-ret + int-ret TDs (TDs SCORED)
  - fumbles (fill)     = split (rushing+receiving+sack fumbles) WHERE the total is currently empty. The split
                         is PBP-derived + validated (build_fumble_splits_pre1998, 1978-97); the total column was
                         near-empty pre-1987 (~40 rows/yr vs ~600 split). Fill EMPTIES only -> never touches an
                         audited nonzero total. (Return/ST-fumble residual, total>split, needs the name->pfr_id
                         mapping -> follow-up.)

Gated: row count unchanged, new cols populated, golden_points PASS on the temp before swap; backup + swap.

    python -m scripts.sota_recon.build_sota_batch_a_v26 [--apply]
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "sota_batch_a.composites_fumbles_fill"
NEW_COLS = ["yds_from_scrimmage", "rush_receive_td", "total_tds_scored"]
_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _select_sql(src: str) -> str:
    split = f"({_D('rushing_fumbles')}+{_D('receiving_fumbles')}+{_D('sack_fumbles')})"
    fumbles_fill = f"CASE WHEN {_D('fumbles')}=0 AND {split}>0 THEN {split} ELSE TRY_CAST(fumbles AS DOUBLE) END"
    scrimmage = f"({_D('rushing_yards')}+{_D('receiving_yards')})"
    rr_td = f"({_D('rushing_tds')}+{_D('receiving_tds')})"
    total_tds_scored = (f"({_D('rushing_tds')}+{_D('receiving_tds')}+{_D('kickoff_return_tds')}+{_D('punt_return_tds')}"
                f"+{_D('fum_ret_td')}+{_D('def_int_ret_td')})")
    return (f"SELECT * REPLACE (({fumbles_fill}) AS fumbles), "
            f"{scrimmage} AS yds_from_scrimmage, {rr_td} AS rush_receive_td, {total_tds_scored} AS total_tds_scored "
            f"FROM read_parquet('{src}')")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    # 3GB + record-batch STREAMING write (a direct COPY of the 1089-col wide table buffered -> OOM even at
    # 5GB; fetch_record_batch streams row groups, the pattern proven on this table in build_rescore_fpts).
    con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=2")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{vq}')").fetchall()}
    already = [c for c in NEW_COLS if c in cols]
    before_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{vq}')").fetchone()[0]
    fumbles_empty_fillable = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{vq}') WHERE {_D('fumbles')}=0 "
        f"AND ({_D('rushing_fumbles')}+{_D('receiving_fumbles')}+{_D('sack_fumbles')})>0"
    ).fetchone()[0]
    sample = con.execute(
        f"SELECT * FROM ({_select_sql(vq)}) WHERE year BETWEEN 1980 AND 1982 "
        f"AND {_D('yds_from_scrimmage')}>0 LIMIT 3"
    ).df()[["year", "yds_from_scrimmage", "rush_receive_td", "total_tds_scored", "fumbles"]].to_dict("records")
    if not apply:
        con.close()
        return {"already_present": already, "would_add": [c for c in NEW_COLS if c not in cols],
                "fumbles_empty_fillable_rows": fumbles_empty_fillable, "rows": before_rows, "sample": sample}

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_batcha.parquet")
    # streaming write (row-group batches) -- bounded memory on the wide table
    rb = con.execute(_select_sql(vq)).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    new_nonzero = {c: con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE {_D(c)}<>0").fetchone()[0] for c in NEW_COLS}
    fumbles_after = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE {_D('fumbles')}<>0 AND year<=1986").fetchone()[0]
    con.close()

    # gate on the temp: golden_points must PASS (offense recipe + DST + rates unaffected by additive cols/fill)
    from . import golden_points, sources as S
    old = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_points.run()
    finally:
        S.latest_v26 = old
    gate = (after_rows == before_rows and g["passed_overall"] and all(new_nonzero[c] > 0 for c in NEW_COLS))
    res = {"rows": after_rows, "new_nonzero": new_nonzero, "fumbles_pre1987_nonzero": fumbles_after,
           "golden_points_pass": g["passed_overall"], "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prebatcha_{stamp}.parquet")
        shutil.copy2(vp, backup)
        os.replace(tmp, vp)
        res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
