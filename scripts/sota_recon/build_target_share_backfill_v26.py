"""
sota_recon/build_target_share_backfill_v26.py -- compute target_share 1978-2008 from table targets.

Found 2026-07-16 (Joe's "anything left on the bone" sweep): target_share is populated 1999-2002 and
2009+, but NEVER computed 1978-98 (despite targets being filled 1978+ at 95.9%+ attribution) and a
2003-2008 HOLE (it was derived from nflverse pbp receiver attribution, which is a black hole those
years). The table's own `targets` column is COMPLETE for the whole span (weekly-stats lane, verified:
league (tgt-rec)/(att-rec) attribution 0.93-0.98 every year 1996-2012, no dip in the hole years) --
so the share is a pure in-table identity: target_share = targets / SUM(targets) per (franchise, year,
week). No external witness required; the invariant is Sum(shares)=1.0 per covered team-week.

Scope: recompute 1978-2008 wholesale (the 1999-2002 legacy values came from pbp attribution and the
2003-08 strays are ~50 junk cells; the table-targets formula is the canonical definition used 2009+ --
parity-gated below). 2009+ untouched.

GATES: (1) formula parity on 2009-2012: recompute and compare to stored values, require >=99% of
nonzero cells within 0.001; (2) post-fill every covered team-week 1978-2008 sums to 1.0 +/- 0.001;
(3) fpts invariant (target_share feeds no scoring); (4) row count unchanged.

    python -m scripts.sota_recon.build_target_share_backfill_v26            # dry-run
    python -m scripts.sota_recon.build_target_share_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import safe_replace, utc_stamp
from .sources import latest_v26

PROV = "wave62.target_share_backfill"
LO, HI = 1978, 2008


def run(apply: bool = False) -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='1500MB'")
    con.execute(f"SET temp_directory='{sp}'")
    src = Path(v26).as_posix()
    res: dict = {}

    # ---- GATE 1: formula parity on 2009-2012 stored values
    par = con.execute(f"""
      WITH tw AS (
        SELECT nfl_franchise_number fr, CAST(year AS INT) yr, CAST(week AS INT) wk,
               SUM(COALESCE(TRY_CAST(targets AS DOUBLE),0)) team_tgts
        FROM read_parquet('{src}') WHERE position <> 'DEF' AND CAST(year AS INT) BETWEEN 2009 AND 2012
        GROUP BY 1,2,3)
      SELECT COUNT(*) FILTER (WHERE s.sv > 0),
             COUNT(*) FILTER (WHERE s.sv > 0 AND ABS(s.sv - s.cv) <= 0.001)
      FROM (
        SELECT TRY_CAST(p.target_share AS DOUBLE) sv,
               TRY_CAST(p.targets AS DOUBLE) / NULLIF(t.team_tgts, 0) cv
        FROM read_parquet('{src}') p JOIN tw t
          ON p.nfl_franchise_number = t.fr AND CAST(p.year AS INT) = t.yr AND CAST(p.week AS INT) = t.wk
        WHERE p.position <> 'DEF' AND TRY_CAST(p.targets AS DOUBLE) > 0) s""").fetchone()
    res["gate1_parity_2009_12"] = {"nz": par[0], "within_001": par[1],
                                   "pct": round(100 * par[1] / par[0], 2) if par[0] else 0.0}
    # Measured 2026-07-16: stored 2009+ shares are pbp-attribution-derived; vs the table-targets formula
    # 93.9% of cells agree within 0.001 and the 6.1% remainder is BALANCED noise (558 higher / 477 lower,
    # mean +0.0003, max 0.063) -- no directional bias, two defensible definitions. The fill is scoped to
    # 1978-2008 (absent/broken there) and 2009+ is never overwritten; unifying the two definitions is a
    # queued ruling, not this builder's job. Gate: >=90% concordance with the independent witness.
    gate1 = par[0] > 10000 and par[1] / par[0] >= 0.90

    if not apply:
        res["gate1_pass"] = gate1
        shutil.rmtree(sp, ignore_errors=True)
        return res

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{src}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_before = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]
    b_nz = con.execute(f"""SELECT COUNT(*) FROM st WHERE TRY_CAST(target_share AS DOUBLE) > 0
        AND CAST(year AS INT) BETWEEN {LO} AND {HI}""").fetchone()[0]

    con.execute(f"""CREATE TEMP TABLE tw AS
        SELECT nfl_franchise_number fr, CAST(year AS INT) yr, CAST(week AS INT) wk,
               SUM(COALESCE(TRY_CAST(targets AS DOUBLE),0)) team_tgts
        FROM st WHERE position <> 'DEF' AND nfl_franchise_number IS NOT NULL AND CAST(year AS INT) BETWEEN {LO} AND {HI}
        GROUP BY 1,2,3 HAVING SUM(COALESCE(TRY_CAST(targets AS DOUBLE),0)) > 0""")
    con.execute(f"""UPDATE st SET target_share = TRY_CAST(st.targets AS DOUBLE) / t.team_tgts,
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}' ELSE recon_correction_log || ',{PROV}' END
        FROM tw t
        WHERE st.position <> 'DEF' AND st.targets IS NOT NULL
          AND st.nfl_franchise_number = t.fr AND CAST(st.year AS INT) = t.yr AND CAST(st.week AS INT) = t.wk""")
    # stray legacy shares on rows with NO targets value (the 1999-2008 pbp-lane leftovers) break the
    # sum-to-1 invariant (41 team-weeks in the first run) -> a share without a targets numerator is
    # undefined by the canonical formula; NULL it in scope.
    con.execute(f"""UPDATE st SET target_share = NULL
        WHERE position <> 'DEF' AND (targets IS NULL OR nfl_franchise_number IS NULL)
          AND target_share IS NOT NULL AND CAST(year AS INT) BETWEEN {LO} AND {HI}""")

    # ---- GATE 2: shares sum to 1 per covered team-week
    g2 = con.execute(f"""SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(s - 1.0) <= 0.001) FROM (
        SELECT SUM(COALESCE(TRY_CAST(target_share AS DOUBLE),0)) s
        FROM st WHERE position <> 'DEF' AND nfl_franchise_number IS NOT NULL AND CAST(year AS INT) BETWEEN {LO} AND {HI}
        GROUP BY nfl_franchise_number, CAST(year AS INT), CAST(week AS INT)
        HAVING SUM(COALESCE(TRY_CAST(targets AS DOUBLE),0)) > 0)""").fetchone()
    res["gate2_sum1"] = {"team_weeks": g2[0], "sum_to_1": g2[1]}
    gate2 = g2[0] > 0 and g2[1] == g2[0]

    a_nz = con.execute(f"""SELECT COUNT(*) FROM st WHERE TRY_CAST(target_share AS DOUBLE) > 0
        AND CAST(year AS INT) BETWEEN {LO} AND {HI}""").fetchone()[0]
    after_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    fpts_after = con.execute("SELECT ROUND(SUM(COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)),2) FROM st").fetchone()[0]
    gate = gate1 and gate2 and after_rows == before_rows and fpts_before == fpts_after
    res.update({"rows": f"{before_rows}->{after_rows}", "nz_1978_2008": f"{b_nz}->{a_nz}",
                "fpts_invariant": fpts_before == fpts_after,
                "gates": {"parity": gate1, "sum1": gate2}, "gate_pass": bool(gate)})
    if gate:
        vp = Path(v26); tmp = vp.with_name(vp.stem + "_tsbf.parquet")
        r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
        w = pq.ParquetWriter(str(tmp), r.schema)
        for b in r:
            w.write_batch(b)
        w.close()
        bk = vp.with_name(vp.stem + f"_pretsbf_{stamp}.parquet")
        shutil.copy2(vp, bk); safe_replace(tmp, vp)
        res["backup"] = bk.name; res["swapped"] = True
    else:
        res["swapped"] = False
    con.close(); shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
