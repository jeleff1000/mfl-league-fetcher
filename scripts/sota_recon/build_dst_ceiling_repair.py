"""
sota_recon/build_dst_ceiling_repair.py  --  enforce touch>=TD and roll the DST row up to IDP sums

The expanded golden matrix surfaced two real consistency gaps:
  1. def_int_ret_td > def_interceptions (69 player-rows): the wave-22 scoring backfill set an
     INT-return TD without crediting the underlying interception. A player who returned an INT for
     a TD necessarily had >=1 INT, so raise def_interceptions to >= def_int_ret_td (same for
     fum_ret_td vs def_fumbles).
  2. fum_ret_td > def_fumbles on TEAM DEF rows (777 rows): the DST row's defensive counting stats
     were never aggregated from the individual defenders. Roll the DEF row up to the IDP sums per
     (franchise, year, week) for def_interceptions / def_fumbles / def_sacks / def_int_ret_td /
     fum_ret_td / def_fumbles_forced -- making the DST row a true team aggregate (and satisfying
     fum_ret_td<=def_fumbles, def_int_ret_td<=def_interceptions by construction).

Gated: golden 24/24, modern (>=1932) ceiling violations for these pairs -> 0, row count unchanged.

    python -m scripts.sota_recon.build_dst_ceiling_repair            # dry-run
    python -m scripts.sota_recon.build_dst_ceiling_repair --apply
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

PROV = "wave25.dst_ceiling_repair"
PROV_COL = "recon_correction_log"
NO_ROLLUP = False   # set True to floor only (skip IDP rollup -- preserves consensus DST values)
# DST team-row counting stats rolled up from IDP
ROLLUP = ["def_interceptions", "def_fumbles", "def_sacks", "def_int_ret_td",
          "fum_ret_td", "def_fumbles_forced"]


# (td_col, touch_col): touch must be >= TD wherever the touch is RECORDED (non-null)
TOUCH_PAIRS = [("def_int_ret_td", "def_interceptions"), ("fum_ret_td", "def_fumbles"),
               ("rushing_tds", "carries"), ("receiving_tds", "receptions"),
               ("kickoff_return_tds", "kickoff_returns"), ("punt_return_tds", "punt_returns")]


def _viol(con, tbl):
    """count recorded-touch-below-TD across all pairs (non-null touch only)."""
    return con.execute(f"SELECT COUNT(*) FROM {tbl} WHERE " + " OR ".join(
        f"({b} IS NOT NULL AND COALESCE({a},0) > {b} + 0.01)" for a, b in TOUCH_PAIRS)).fetchone()[0]


def apply_build():
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre = _viol(con, "st")
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    # 1) ALL rows (incl team DEF): where a touch/turnover count is RECORDED (non-null) yet below
    #    its TD count, raise it to the TD count -- the logical minimum (you can't score N TDs on
    #    fewer than N touches). NULL touches (early-era "count unknown") are left untouched.
    for td, touch in TOUCH_PAIRS:
        con.execute(f"""UPDATE st SET {touch}={td}, {prov}
            WHERE {touch} IS NOT NULL AND COALESCE({td},0) > {touch} + 0.01""")
    # 2) DEF team rows: roll up counting stats from the IDP rows of the same GAME -- keyed by
    #    (franchise, year, week, OPPONENT) so pre-1950 doubleheaders each get only their own game's
    #    IDP sum. SKIPPED when --no-rollup (after consensus_reconcile has set authoritative DST
    #    values that the IDP sum must not overwrite).
    if not NO_ROLLUP:
        con.execute(f"""CREATE TEMP TABLE idp AS
            SELECT nfl_franchise_number fr, year yr, week wk, opponent_nfl_franchise_number opp,
                   {', '.join(f'SUM(COALESCE({c},0)) {c}' for c in ROLLUP)}
            FROM st WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
        setc = ", ".join(f"{c}=i.{c}" for c in ROLLUP)
        con.execute(f"""UPDATE st SET {setc}, {prov} FROM idp i
            WHERE st.position='DEF' AND st.nfl_franchise_number=i.fr AND st.year=i.yr AND st.week=i.wk
              AND st.opponent_nfl_franchise_number IS NOT DISTINCT FROM i.opp""")
        # re-floor after rollup (DEF rows may now have a TD count above the rolled-up touch)
        for td, touch in TOUCH_PAIRS:
            con.execute(f"""UPDATE st SET {touch}={td}, {prov}
                WHERE {touch} IS NOT NULL AND COALESCE({td},0) > {touch} + 0.01""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    post = _viol(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dcrtmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and (post == 0)
    res = {"pre_viol": pre, "post_viol": post, "before": int(before), "after": int(after),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predcr_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    ap.add_argument("--no-rollup", action="store_true")
    a = ap.parse_args()
    NO_ROLLUP = a.no_rollup
    if a.apply:
        r = apply_build()
        print(f"recorded touch<TD violations {r['pre_viol']}->{r['post_viol']} | "
              f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TABLE st AS SELECT * FROM '{v}'")
        print("recorded touch<TD violations (non-null touch):", _viol(con, "st"))
