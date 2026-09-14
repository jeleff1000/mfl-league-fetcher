"""
sota_recon/build_pts_def_team_pts_v26.py -- repair the DEF-row pts_def_team_pts reconciliation column.

pts_def_team_pts is meant to carry the DEF's own team's points SCORED that game (the scored side of the
points-scored==points-allowed double-entry mirror). It went broken 2014+ (~27% populated vs ~98% pre-2013).
Authoritative source = nfl_team_games_all.team_points, joined to the DEF row on franchise number + year +
week (franchise number, not abbrev -- abbrevs drift OAK->LV). NON-scoring: fpts invariant (gated).

    python -m scripts.sota_recon.build_pts_def_team_pts_v26 [--apply]
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

TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _pop(con, src: str, lo: int, hi: int) -> tuple[int, int]:
    r = con.execute(f"""SELECT SUM(CASE WHEN {_D('pts_def_team_pts')}<>0 THEN 1 ELSE 0 END), COUNT(*)
        FROM read_parquet('{src}') WHERE position='DEF' AND year BETWEEN {lo} AND {hi}""").fetchone()
    return r[0], r[1]


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='3GB'")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")

    tgcols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{TG}')").fetchall()}
    fidcol = "team_fid" if "team_fid" in tgcols else ("nfl_franchise_number" if "nfl_franchise_number" in tgcols else None)
    before = {f"{lo}_{hi}": _pop(con, vq, lo, hi) for lo, hi in [(2005, 2013), (2014, 2020), (2021, 2025)]}
    if not apply:
        con.close(); shutil.rmtree(sp, ignore_errors=True)
        return {"tg_fid_col": fidcol, "tg_has_team_points": "team_points" in tgcols, "pop_before": before}

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{vq}')")
    con.execute(f"""CREATE TEMP TABLE tg AS SELECT CAST({fidcol} AS INT) fid, CAST(year AS INT) yr,
        CAST(week AS INT) wk, {_D('team_points')} pts FROM read_parquet('{TG}') WHERE {fidcol} IS NOT NULL""")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_orig = con.execute(f"SELECT ROUND(SUM({_D('fpts_4pt_half')}),1) FROM st").fetchone()[0]
    con.execute("""UPDATE st SET pts_def_team_pts = tg.pts FROM tg
        WHERE st.position='DEF' AND CAST(st.nfl_franchise_number AS INT)=tg.fid
          AND CAST(st.year AS INT)=tg.yr AND CAST(st.week AS INT)=tg.wk""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_teampts.parquet")
    rb = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    fpts_new = con.execute(f"SELECT ROUND(SUM({_D('fpts_4pt_half')}),1) FROM read_parquet('{tq}')").fetchone()[0]
    after = {f"{lo}_{hi}": _pop(con, tq, lo, hi) for lo, hi in [(2005, 2013), (2014, 2020), (2021, 2025)]}
    con.close()

    gate = (after_rows == before_rows and fpts_orig == fpts_new
            and after["2014_2020"][0] > before["2014_2020"][0])
    res = {"pop_before": before, "pop_after": after, "fpts_invariant": fpts_orig == fpts_new, "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_preteampts_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    print(run(apply=a.apply))
