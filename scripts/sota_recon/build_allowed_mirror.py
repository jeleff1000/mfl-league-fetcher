"""
sota_recon/build_allowed_mirror.py  --  defensive "allowed" stats = the opponent's (reconciled) offense

A team's defensive ALLOWED totals are, by definition, the opponent's offensive production in that
game. The opponent's offense is already reconciled to multi-source truth, so the cleanest possible
value for each DEF-row *_allowed column is a direct MIRROR of the opponent's offensive total for
that exact game (keyed by year, week, franchise, opponent). This makes the allowed-stats exact by
construction wherever the opponent offense is known.

Gated: golden 24/24, per-column DEF==opponent-mirror agreement -> ~100% (1950+), rows unchanged.

    python -m scripts.sota_recon.build_allowed_mirror            # dry-run
    python -m scripts.sota_recon.build_allowed_mirror --apply
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
PROV = "wave27.allowed_mirror"; PROV_COL = "recon_correction_log"
# DEF-row allowed col  <-  opponent offensive expression (summed over opponent's non-DEF rows)
MIRROR = {
    "passing_yds_allowed": "passing_yards", "passing_tds_allowed": "passing_tds",
    "rushing_yds_allowed": "rushing_yards", "rushing_tds_allowed": "rushing_tds",
    "receiving_yds_allowed": "receiving_yards", "receiving_tds_allowed": "receiving_tds",
    "def_completions_allowed": "completions", "def_completion_yards_allowed": "passing_yards",
    "def_sack_yards": "sack_yards_lost", "total_yds_allowed": "rushing_yards + receiving_yards",
}


def _agree(con, tbl):
    out = {}
    for col, expr in MIRROR.items():
        r = con.execute(f"""
            WITH o AS (SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,nfl_franchise_number fr,
                          SUM(COALESCE({expr},0)) v FROM {tbl} WHERE position<>'DEF'
                          AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3),
                 g AS (SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,team_fid fr,opponent_fid ofr FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL),
                 dv AS (SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,nfl_franchise_number fr,SUM(COALESCE({col},0)) v
                        FROM {tbl} WHERE position='DEF' GROUP BY 1,2,3)
            SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(dv.v,0)-COALESCE(o.v,0))<0.5 THEN 1 ELSE 0 END)/COUNT(*),1)
            FROM g JOIN dv ON dv.yr=g.yr AND dv.wk=g.wk AND dv.fr=g.fr
                   LEFT JOIN o ON o.yr=g.yr AND o.wk=g.wk AND o.fr=g.ofr
            WHERE g.yr>=1950""").fetchone()[0]
        out[col] = r
    return out


def apply_build(apply=False):
    v26 = latest_v26()
    have = set(pq.read_schema(v26).names)
    for k in [c for c in MIRROR if c not in have]:
        MIRROR.pop(k)
    if not apply:
        con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        return _agree(con, f"read_parquet('{v26}')")
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre = _agree(con, "st")
    def _term(c, e):
        if "+" in e:
            s = "+".join(f"COALESCE(o.{p.strip()},0)" for p in e.split("+"))
            return f"SUM({s}) m_{c}"
        return f"SUM(COALESCE(o.{e},0)) m_{c}"
    con.execute(f"""CREATE TEMP TABLE oppoff AS
        SELECT g.yr, g.wk, g.ofr fr, {', '.join(_term(c, e) for c, e in MIRROR.items())}
        FROM (SELECT * FROM st WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL) o
        JOIN (SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,team_fid offfr,opponent_fid ofr FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL) g
          ON CAST(o.year AS INT)=g.yr AND CAST(o.week AS INT)=g.wk AND o.nfl_franchise_number=g.offfr
        GROUP BY 1,2,3""")
    setc = ", ".join(f"{c}=m.m_{c}" for c in MIRROR)
    con.execute(f"""UPDATE st SET {setc},
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM oppoff m WHERE st.position='DEF' AND CAST(st.year AS INT)=m.yr AND CAST(st.week AS INT)=m.wk
          AND st.nfl_franchise_number=m.fr""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_amtmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); post = _agree(con, "st"); con.close(); shutil.rmtree(sp, ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and all(v >= 99.0 for v in post.values())
    res = {"before": before, "after": after, "pre": pre, "post": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_pream_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_build(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        for k in r["post"]:
            print(f"  {k:30s} {r['pre'][k]:6.1f} -> {r['post'][k]:6.1f}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED ({r['backup']})" if r["swapped"] else f"NOT swapped; {r['temp']}"))
    else:
        for k, val in apply_build().items():
            print(f"  {k:30s} {val:6.1f}%")
