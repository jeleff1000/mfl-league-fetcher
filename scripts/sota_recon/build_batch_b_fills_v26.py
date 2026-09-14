"""
sota_recon/build_batch_b_fills_v26.py -- crawl-INDEPENDENT confirmed data fixes (one streaming rewrite).

Batches the fixes that are VERIFIED (not judgment calls) into a single streaming record-batch rewrite of
the weekly super table (the pattern proven memory-safe in build_rescore_fpts / build_sota_batch_a):

  1. NULL pre-2006 charted air-yards / YAC / aDOT / CPOE. Exhaustively verified 2026-07-15: no witness
     (PBP air_yards+YAC both 2006+; NGS 2016+; PFR-adv 2018+) and no derivation (yards_gained-YAC needs
     YAC, also 2006+) and no play-description catch-point pre-2006 -> these values are SPURIOUS (Marvin
     Harrison 2000 = 63 air-yds over 102 catches). Deletion discipline: proven-fabricated + backup on swap.

Adds more confirmed fixes here as they clear diagnosis (e.g. pts_def_team_pts 2014+ from nfl_team_games_all).

    python -m scripts.sota_recon.build_batch_b_fills_v26 [--apply]
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

PROV = "batch_b.null_pre2006_charted"
# charted air/YAC/adot/cpoe atoms whose true floor is 2006 -> NULL where year < 2006
CHARTED_2006 = [
    "receiving_air_yards", "passing_air_yards", "receiving_yac", "receiving_yards_after_catch",
    "passing_yards_after_catch", "receiving_completed_air_yards", "passing_completed_air_yards",
    "passing_cpoe", "receiving_adot", "adot",
]
_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _fl_split(cols) -> str | None:
    parts = [c for c in ("rushing_fumbles_lost", "sack_fumbles_lost", "receiving_fumbles_lost") if c in cols]
    return "(" + "+".join(_D(c) for c in parts) + ")" if parts else None


def _select_sql(src: str, cols: set) -> tuple[str, list]:
    repls = []
    targets = [c for c in CHARTED_2006 if c in cols]
    for c in targets:
        repls.append(f"CASE WHEN CAST(year AS INT) < 2006 THEN NULL ELSE {c} END AS {c}")
    # fumbles_lost TOTAL: fill from the PBP-derived split (1978-97) where the total is empty (was missed in
    # Batch A which only filled `fumbles`). Fill-empties-only; never lowers an existing nonzero total.
    split = _fl_split(cols)
    if split and "fumbles_lost" in cols:
        repls.append(f"CASE WHEN {_D('fumbles_lost')}=0 AND {split}>0 THEN {split} ELSE fumbles_lost END AS fumbles_lost")
        targets = targets + ["fumbles_lost"]
    return f"SELECT * REPLACE ({', '.join(repls)}) FROM read_parquet('{src}')", targets


def run(apply: bool = False) -> dict:
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=2")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{vq}')").fetchall()}
    sql, targets = _select_sql(vq, cols)
    null_targets = [c for c in targets if c != "fumbles_lost"]  # nulled pre-2006
    fill_fl = "fumbles_lost" in targets                          # filled from split pre-1994
    before = {c: con.execute(f"SELECT COUNT(*) FROM read_parquet('{vq}') WHERE CAST(year AS INT)<2006 AND {_D(c)}<>0").fetchone()[0] for c in null_targets}
    before_fl = con.execute(f"SELECT COUNT(*) FROM read_parquet('{vq}') WHERE year<1994 AND {_D('fumbles_lost')}<>0").fetchone()[0] if fill_fl else 0
    before_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{vq}')").fetchone()[0]
    if not apply:
        con.close()
        return {"null_targets": null_targets, "pre2006_nonzero_to_null": before,
                "fumbles_lost_fill": fill_fl, "fumbles_lost_pre1994_before": before_fl, "rows": before_rows}

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA disable_progress_bar"); con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_bfills.parquet")
    rb = con.execute(sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    after = {c: con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE CAST(year AS INT)<2006 AND {_D(c)}<>0").fetchone()[0] for c in null_targets}
    after_fl = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE year<1994 AND {_D('fumbles_lost')}<>0").fetchone()[0] if fill_fl else 0
    fl_2006plus = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE year>=1994 AND {_D('fumbles_lost')}<>0").fetchone()[0] if fill_fl else 1
    # keep 2006+ intact (a bug would zero those too)
    kept = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE CAST(year AS INT)>=2006 AND {_D('receiving_air_yards')}<>0").fetchone()[0]
    con.close()
    # gate: golden_points PASS + rows unchanged + null_targets pre-2006 now 0 + air 2006+ preserved
    #       + fumbles_lost pre-1994 now FILLED (>before) and 1994+ preserved
    from . import golden_points, sources as S
    old = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_points.run()
    finally:
        S.latest_v26 = old
    gate = (after_rows == before_rows and g["passed_overall"] and all(v == 0 for v in after.values())
            and kept > 0 and (not fill_fl or (after_fl > before_fl and fl_2006plus > 0)))
    res = {"null_targets": null_targets, "nulled_pre2006": {c: before[c] for c in null_targets}, "post_null": after,
           "fumbles_lost_pre1994": f"{before_fl} -> {after_fl}", "fumbles_lost_1994plus_kept": fl_2006plus,
           "recv_air_2006plus_kept": kept, "golden_pass": g["passed_overall"], "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_prebfills_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
