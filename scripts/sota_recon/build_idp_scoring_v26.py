"""
sota_recon/build_idp_scoring_v26.py  --  recompute the IDP scoring layer on v26 (modal-Sleeper weights
+ de-duped defensive TD).

(1) pts_idp_std uses the MOST COMMON real ruleset = modal Sleeper IDP (110 leagues, 2026-06-28):
    tackle (solo 1 / assist 0.5), sack 4, int 6, ff 3, fum_rec 2, tfl 2, pass_def 3, qb_hit 1,
    safety 2, td 6. premium / tackle_heavy / big_play follow fantasy_points_calculator (L1010-1069).
(2) DEFENSIVE-TD DE-DUP (wave49): the old TD term `def_tds + fum_ret_td` was WRONG for IDP rows --
    on most pick-six rows def_tds=0 while def_int_ret_td=1, so it DROPPED 771 pick-six TDs to 0 points;
    where def_tds IS populated it already includes fum returns, so adding fum_ret_td double-counts.
    Fixed to the disjoint-component de-dup (int-return + fum-return when present, else def_tds) --
    mirrors fantasy_points_calculator idp_td / pts_def_td. Applied to ALL 4 variants + pts_idp_td.

All four bundled variants + the pts_idp_td component are recomputed from atoms via streaming
SELECT * REPLACE (no 740-col materialization); non-IDP-eligible rows -> 0.

Gate: golden 56/56; rows unchanged; pts_idp_std changed for IDP rows; the 771 pick-six rows now >0.

    python -m scripts.sota_recon.build_idp_scoring_v26 [--apply]
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

PROV = "wave50.idp_total_tackles"


def D(c):
    return f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


# de-duped defensive TD (mirrors calculator idp_td): disjoint int-return + fum-return when present,
# else def_tds. Fixes the pick-six undercount (def_tds=0,int_ret>0) AND the fum double-count.
TD = (
    f"(CASE WHEN ({D('def_int_ret_td')}+{D('fum_ret_td')})>0 "
    f"THEN ({D('def_int_ret_td')}+{D('fum_ret_td')}) ELSE {D('def_tds')} END)"
)
# Tackle scoring: use split atoms when present; fall back to reported combined
# tackles only on rows with no split tackle data.
_SPLIT_TACKLES = f"({D('def_tackles_solo')}+{D('def_tackle_assists')})"
TACK = (
    f"(CASE WHEN {_SPLIT_TACKLES}<>0 "
    f"THEN ({D('def_tackles_solo')} + {D('def_tackle_assists')}*0.5) "
    f"ELSE {D('def_tackles_with_assist')} END)"
)
_SK, _IN, _FF, _FR = D("def_sacks"), D("def_interceptions"), D("def_fumbles_forced"), D("fum_rec")
_TFL, _PD, _QH, _SF = D("def_tackles_for_loss"), D("def_pass_defended"), D("def_qb_hits"), D("def_safeties")
# bundled variants -- weights MUST match fantasy_points_calculator pts_idp_* (L1010-1069)
VARIANTS = {
    "pts_idp_std": f"{TACK}*1.0+{_SK}*4.0+{_IN}*6.0+{_FF}*3.0+{_FR}*2.0+{_TFL}*2.0+{_PD}*3.0+{_QH}*1.0+{_SF}*2.0+{TD}*6.0",
    "pts_idp_premium": f"{TACK}*1.5+{_SK}*3.0+{_IN}*4.0+{_FF}*3.0+{_FR}*3.0+{_TFL}*1.5+{_PD}*1.0+{_QH}*1.0+{_SF}*3.0+{TD}*6.0",
    "pts_idp_tackle_heavy": f"{TACK}*2.0+{_SK}*2.0+{_IN}*3.0+{_FF}*2.0+{_FR}*2.0+{_TFL}*2.0+{_PD}*1.0+{_QH}*0.5+{_SF}*2.0+{TD}*6.0",
    "pts_idp_big_play": f"{TACK}*0.5+{_SK}*4.0+{_IN}*6.0+{_FF}*4.0+{_FR}*4.0+{_TFL}*2.0+{_PD}*1.5+{_QH}*1.0+{_SF}*4.0+{TD}*6.0",
    "pts_idp_td": TD,
}
IDP_POS = "('LB','DL','DB','DE','DT','CB','S','ILB','OLB','MLB','NT','SS','FS','EDGE','SAF')"
# Token match MUST cover golden's full IDP_POS set (not just the 3 base groups): a fumble-return-TD DE
# whose position='DE' (no 'DL' token) was skipped here but scored by golden -> 5 stale pts_idp_td rows.
# Keeps the position-comma-list basis (so two-way 'WR,DB' still scores IDP, unlike an nfl_position switch).
IDP_ELIG = ("len(list_intersect(string_split(UPPER(COALESCE(position, '')), ','), "
            "['DB','DL','LB','DE','DT','CB','S','ILB','OLB','MLB','NT','SS','FS','EDGE','SAF'])) > 0")


def component_formulas(cols):
    def s(c):
        return D(c) if c in cols else "0.0"

    fum_rec_yd = (
        f"(CASE WHEN ({s('fumble_recovery_yards_own')}+{s('fumble_recovery_yards_opp')})<>0 "
        f"THEN ({s('fumble_recovery_yards_own')}+{s('fumble_recovery_yards_opp')}) "
        f"ELSE {s('fum_rec_yds')} END)"
    )
    split_tkl = f"({s('def_tackles_solo')}+{s('def_tackle_assists')})"
    comb_tkl = f"(CASE WHEN {split_tkl}<>0 THEN {split_tkl} ELSE {s('def_tackles_with_assist')} END)"
    return {
        "pts_idp_tackle_solo": s("def_tackles_solo"),
        "pts_idp_tackle_assist": s("def_tackle_assists"),
        "pts_idp_sack": s("def_sacks"),
        "pts_idp_int": s("def_interceptions"),
        "pts_idp_ff": s("def_fumbles_forced"),
        "pts_idp_fr": s("fum_rec"),
        "pts_idp_pd": s("def_pass_defended"),
        "pts_idp_qb_hit": s("def_qb_hits"),
        "pts_idp_tfl": s("def_tackles_for_loss"),
        "pts_idp_safety": s("def_safeties"),
        "pts_idp_td": TD,
        "pts_idp_blk_kick": s("def_blk_kick"),
        "pts_idp_int_ret_yd": s("def_interception_yards"),
        "pts_idp_fum_rec_yd": fum_rec_yd,
        "pts_idp_tkl_combined": comb_tkl,
        "pts_idp_blk_kick_td": s("def_blk_kick_td"),
        "pts_idp_fum_ret_td": s("fum_ret_td"),
        "pts_idp_xpr": s("def_xpr"),
        "pts_idp_pass_def_3p": f"LEAST({s('def_pass_defended')},3.0)",
    }


def run(apply=False):
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    # Overridable for saturated boxes (RECON_MEMORY_LIMIT=1500MB forces spill to disk)
    # without making the constrained setting the default for every future run.
    con.execute(f"SET memory_limit='{os.environ.get('RECON_MEMORY_LIMIT', '4GB')}'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    if not apply:
        con.close()
        return {"rows": before}
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute(f"PRAGMA threads={os.environ.get('RECON_THREADS', '3')}")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'")
    cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    has_log = "recon_correction_log" in cols
    # Recompute every bundled variant and modular component, scoped to compact IDP eligibility.
    # Non-IDP rows may retain raw defensive atoms, but every fantasy-facing pts_idp_* lane is zero.
    formulas = {**component_formulas(cols), **VARIANTS}
    repl = []
    if "def_tackles_with_assist" in cols:
        repl.append(
            f"(CASE WHEN {_SPLIT_TACKLES}<>0 THEN {_SPLIT_TACKLES} ELSE {D('def_tackles_with_assist')} END) "
            "AS def_tackles_with_assist"
        )
    repl.extend(
        [f"(CASE WHEN {IDP_ELIG} THEN ({expr}) ELSE 0.0 END) AS {col}" for col, expr in formulas.items() if col in cols]
    )
    if has_log:
        repl.append(
            f"CASE WHEN {IDP_ELIG} THEN (CASE WHEN recon_correction_log IS NULL OR recon_correction_log='' "
            f"THEN '{PROV}' ELSE recon_correction_log||',{PROV}' END) ELSE recon_correction_log END AS recon_correction_log"
        )
    out_sql = f"SELECT * REPLACE ({', '.join(repl)}) FROM '{vq}'"
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_idpstd.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    idp_nonzero = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {IDP_ELIG} AND pts_idp_std<>0").fetchone()[0]
    non_idp_nonzero = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE NOT ({IDP_ELIG}) AND COALESCE(pts_idp_std,0)<>0"
    ).fetchone()[0]
    numeric_idp_cols = [
        r[0]
        for r in con.execute(f"DESCRIBE SELECT * FROM '{tq}'").fetchall()
        if r[0].startswith("pts_idp_")
        and str(r[1]).upper().split("(")[0]
        in {
            "DOUBLE",
            "FLOAT",
            "REAL",
            "DECIMAL",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "SMALLINT",
            "TINYINT",
            "UBIGINT",
            "UINTEGER",
        }
    ]
    component_expr = " + ".join(f"SUM(CASE WHEN COALESCE({col},0)<>0 THEN 1 ELSE 0 END)" for col in numeric_idp_cols)
    non_idp_any_component = con.execute(f"SELECT ({component_expr}) FROM '{tq}' WHERE NOT ({IDP_ELIG})").fetchone()[0]
    changed = con.execute(f"""SELECT COUNT(*) FROM '{vq}' o JOIN '{tq}' n USING (player_week)
        WHERE ABS(COALESCE(o.pts_idp_std,0)-COALESCE(n.pts_idp_std,0))>0.01""").fetchone()[0]
    changed_expr = " OR ".join(f"ABS(COALESCE(o.{col},0)-COALESCE(n.{col},0))>0.01" for col in numeric_idp_cols)
    changed_any_idp = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' o JOIN '{tq}' n USING (player_week) WHERE {changed_expr}"
    ).fetchone()[0]
    # the 771 pick-six undercount rows must now be > 0
    picksix = con.execute(f"""SELECT COUNT(*) FROM '{tq}' WHERE {IDP_ELIG}
        AND {D("def_int_ret_td")}>0 AND {D("def_tds")}=0 AND {D("fum_ret_td")}=0 AND pts_idp_std<=0""").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S

    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (
        (g["failed"] == 0)
        and (after == before)
        and (idp_nonzero > 0)
        and (non_idp_nonzero == 0)
        and (non_idp_any_component == 0)
        and (changed_any_idp > 0)
        and (picksix == 0)
    )
    res = {
        "before": before,
        "after": after,
        "idp_nonzero": idp_nonzero,
        "changed_rows": changed,
        "non_idp_nonzero": non_idp_nonzero,
        "picksix_still_zero": picksix,
        "non_idp_any_component": non_idp_any_component,
        "changed_any_idp": changed_any_idp,
        "golden": f"{g['passed']}/{g['total']}",
        "gate_pass": bool(gate),
        "temp": str(tmp),
    }
    if gate:
        bk = vp.with_name(vp.stem + f"_preidpstd_{stamp}.parquet")
        shutil.copy2(vp, bk)
        from .recon_common import safe_replace; safe_replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(
            f"rows {r['before']:,}->{r['after']:,} | idp_nonzero={r['idp_nonzero']:,} "
            f"non_idp_nonzero={r['non_idp_nonzero']:,} changed={r['changed_rows']:,} "
            f"changed_any_idp={r['changed_any_idp']:,} "
            f"non_idp_any_component={r['non_idp_any_component']:,} "
            f"picksix_still_zero={r['picksix_still_zero']} | golden {r['golden']}"
        )
        print(
            f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
            + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}")
        )
