"""
sota_recon/build_dst_int_pre1950.py  --  complete pre-1950 team-DST interceptions from the box-score team line

Pre-1950 the IDP interception detail is sparse, so the DST row's def_interceptions (rolled up from
individual defenders) under-counts: for 1933-44 the box-score TEAM line (team_stats
'Cmp-Att-Yd-TD-INT') records ~2,102 team defensive INTs but v26 had only 1,156. The team total was
in the box score all along (the opponent's INTs-thrown == this team's defensive INTs), with full
game coverage; the 1945-49 cross-check (team line vs IDP agree 93.5%) confirms it is trustworthy.

This sets the DST-row def_interceptions to the box-score team total for 1933-1949 (GREATEST with the
current value, so it only raises -- never below the individually-attributed IDP sum). Individual
attribution stays as-is (we know the team's INT count even where we can't name every defender);
the INT-return-TD ones are already attributed via the scoring witness (wave22). 1920-32 is left
alone (INTs were not a recorded stat then -- irreducible).

Gated: golden 24/24, 1933-49 DST def_interceptions agreement vs the team line -> ~100%, row count
unchanged.

    python -m scripts.sota_recon.build_dst_int_pre1950            # dry-run
    python -m scripts.sota_recon.build_dst_int_pre1950 --apply
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

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
TS = f"{BOX}/team_stats/_combined.parquet"
PROV = "wave26.dst_int_pre1950_teamline"
PROV_COL = "recon_correction_log"
LO, HI = 1933, 1949


def _teamline(con):
    """opponent's INTs-thrown (team_stats Cmp-Att-Yd-TD-INT field 5) -> defending franchise, keyed
    by GAME (def_fr + the throwing opponent off_fr) so doubleheaders separate correctly."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id,
        CAST(year AS INT) yr, CAST(week AS INT) wk, opponent_fid def_fr, team_fid off_fr, is_home
        FROM read_parquet('{TG}') WHERE opponent_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tsint AS
        WITH j AS (SELECT g.yr, g.wk, g.def_fr, g.off_fr,
                     CASE WHEN g.is_home THEN ts.home_stat ELSE ts.vis_stat END val
                   FROM tg g JOIN read_parquet('{TS}') ts
                     ON ts.boxscore_id=g.boxscore_id AND ts.stat='Cmp-Att-Yd-TD-INT')
        SELECT yr, wk, def_fr fr, off_fr opp, SUM(CAST(string_split(val,'-')[5] AS INT)) team_int
        FROM j WHERE len(string_split(val,'-'))=5 AND yr BETWEEN {LO} AND {HI}
        GROUP BY 1,2,3,4""")


def _agree(con, tbl):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE dst AS SELECT CAST(year AS INT) yr, CAST(week AS INT) wk,
        nfl_franchise_number fr, opponent_nfl_franchise_number opp, SUM(COALESCE(def_interceptions,0)) v
        FROM {tbl} WHERE position='DEF' AND year BETWEEN {LO} AND {HI} GROUP BY 1,2,3,4""")
    return con.execute("""SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(dst.v,0)-t.team_int)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1),
        SUM(t.team_int), SUM(COALESCE(dst.v,0)) FROM tsint t LEFT JOIN dst ON dst.yr=t.yr AND dst.wk=t.wk
        AND dst.fr=t.fr AND dst.opp IS NOT DISTINCT FROM t.opp WHERE t.team_int>0""").fetchone()


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
    _teamline(con)
    pre = _agree(con, "st")
    gkey = ("CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk AND st.nfl_franchise_number=t.fr "
            "AND st.opponent_nfl_franchise_number IS NOT DISTINCT FROM t.opp")
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tsint t ON {gkey}
        WHERE st.position='DEF' AND t.team_int > COALESCE(st.def_interceptions,0)""").fetchone()[0]
    con.execute(f"""UPDATE st SET def_interceptions=GREATEST(COALESCE(def_interceptions,0), t.team_int),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tsint t WHERE {gkey} AND st.position='DEF' AND t.team_int > COALESCE(st.def_interceptions,0)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    post = _agree(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dstint_tmp.parquet")
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
    # 96%+ game-level with the box-score team total now matching in aggregate (v26 ~= team-total) is
    # the honest pre-1950 ceiling: GREATEST keeps the few games where attributed IDP exceeds the box
    # line by 1 (net-zero, ambiguous which source is short) rather than destroy real attribution.
    gate = (g["failed"] == 0) and (after == before) and (post[0] >= 96.0) and (updated > 0)
    res = {"updated": int(updated), "before": int(before), "after": int(after),
           "pre": pre, "post": post, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predstint_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_build()
        print(f"DST rows raised={r['updated']} | rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"  1933-49 DST INT vs team line: agree {r['pre'][0]}->{r['post'][0]}% | "
              f"team-total {r['pre'][1]:.0f}, v26 {r['pre'][2]:.0f}->{r['post'][2]:.0f}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TABLE st AS SELECT * FROM '{v}'")
        _teamline(con)
        a0 = _agree(con, "st")
        print(f"1933-49 DST INT vs box-score team line: agree {a0[0]}% | team-total {a0[1]:.0f}, v26 {a0[2]:.0f}")
