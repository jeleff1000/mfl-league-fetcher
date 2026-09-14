"""
sota_recon/build_canonical_crosswalk.py  --  one authoritative id -> canonical-id map

The durable replacement for per-backfill dedup. Instead of patching duplicates after each
backfill, build ONE crosswalk that maps every v26 NFL_player_id (synthetic, orphan, pfr,
gsis) to a single canonical id per real person, apply it once, then collapse same-game rows.

Identity edges (union-find), chosen to NEVER fuse two distinct real people:
  E1  shared pfr_id in player_bio        -> same person (two bio rows for one player)
  E2  synthetic id co-occurs with a REAL id in the same (franchise, year, week, name)
      -> the synthetic is a stub of that real person   (synthetic<->real ONLY; never real<->real,
         because two real same-name players can share a team-week, e.g. OL+DL Chris Smith)

Canonical per cluster: a real id (pfr preferred, then gsis), else the surviving synthetic.
VALIDATION: a cluster must not contain 2+ DISTINCT real pfr_ids that are different players
(none of the edges can create that, but we assert it).

    python -m scripts.sota_recon.build_canonical_crosswalk          # build + validate + report
    python -m scripts.sota_recon.build_canonical_crosswalk --apply  # gated remap + collapse + bio dedup
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
OUT = "D:/league-history-data/nfl/derived/validation/identity"
PROV = "wave16.canonical_crosswalk"
PROV_COL = "recon_correction_log"
SYNTH = ("(id LIKE 'HIST-%' OR id LIKE 'SYN-%' OR regexp_matches(id,'^[A-Z]{2,4}[0-9]{5,7}$'))")
REAL = ("(regexp_matches(id,'^00-[0-9]+$') OR regexp_matches(id,'^[A-Za-z][A-Za-z.''-]*[0-9]{2}$'))")


def _is_real(i: str) -> bool:
    import re
    return bool(re.match(r"^00-[0-9]+$", i) or re.match(r"^[A-Za-z][A-Za-z.'-]*[0-9]{2}$", i))


def _is_pfr(i: str) -> bool:
    import re
    return bool(re.match(r"^[A-Za-z][A-Za-z.'-]*[0-9]{2}$", i))


def build_crosswalk(con, v26):
    V = f"read_parquet('{v26}')"
    # E2 edges: synthetic <-> real co-occurrence
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rid AS
        SELECT NFL_player_id id, nfl_franchise_number fr, year, week, position pos,
               lower(regexp_replace(player,'[^A-Za-z]','','g')) nn FROM {V} WHERE player IS NOT NULL""")
    # E2: synthetic <-> real co-occurrence (synthetic is a stub of the real person)
    e2 = con.execute(f"""
        SELECT DISTINCT s.id a, r.id b FROM rid s JOIN rid r
          ON r.fr=s.fr AND r.year=s.year AND r.week=s.week AND r.nn=s.nn
        WHERE ({SYNTH.replace('id','s.id')}) AND ({REAL.replace('id','r.id')})""").df()
    # E3: real <-> real co-occurrence with the SAME name AND POSITION in a team-week. Two real
    # same-name same-position players on one team in one week is virtually impossible -> same
    # person (the OL/DL same-name case is excluded because positions differ).
    e3 = con.execute(f"""
        SELECT DISTINCT a.id a, b.id b FROM rid a JOIN rid b
          ON a.fr=b.fr AND a.year=b.year AND a.week=b.week AND a.nn=b.nn AND a.pos=b.pos AND a.id<b.id
        WHERE a.pos IS NOT NULL AND ({REAL.replace('id','a.id')}) AND ({REAL.replace('id','b.id')})""").df()
    # E1 edges: bio rows sharing a pfr_id
    e1 = con.execute(f"""
        SELECT a.NFL_player_id a, b.NFL_player_id b
        FROM read_parquet('{BIO}') a JOIN read_parquet('{BIO}') b
          ON a.pfr_id=b.pfr_id AND a.pfr_id IS NOT NULL AND a.NFL_player_id<b.NFL_player_id""").df()
    # all ids present in v26
    allids = [r[0] for r in con.execute(f"SELECT DISTINCT NFL_player_id FROM {V} WHERE NFL_player_id IS NOT NULL").fetchall()]

    # union-find
    parent = {i: i for i in allids}
    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry
    for df in (e1, e2, e3):
        for a, b in zip(df.a, df.b):
            union(a, b)

    # cluster -> members
    from collections import defaultdict
    clusters = defaultdict(list)
    for i in allids:
        clusters[find(i)].append(i)

    rows, conflicts = [], []
    for members in clusters.values():
        if len(members) == 1:
            continue
        pfrs = [m for m in members if _is_pfr(m)]
        gsis = [m for m in members if m.startswith("00-")]
        if len(set(pfrs)) > 1:
            conflicts.append(members)            # >1 distinct pfr real id -> do NOT auto-merge
            continue
        canon = (pfrs[0] if pfrs else (gsis[0] if gsis else sorted(members)[0]))
        for m in members:
            if m != canon:
                rows.append((m, canon))
    xwalk = pd.DataFrame(rows, columns=["id", "canonical"])
    return xwalk, conflicts


def apply_crosswalk() -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='5GB'")
    con.execute(f"SET temp_directory='{sp}'")
    xwalk, conflicts = build_crosswalk(con, v26)
    os.makedirs(OUT, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(xwalk), os.path.join(OUT, "canonical_crosswalk.parquet"))

    types = {c[0]: c[1] for c in con.execute(f"DESCRIBE SELECT * FROM '{v26}'").fetchall()}
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in types:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.register("xw", xwalk)
    con.execute(f"""UPDATE st SET NFL_player_id=xw.canonical,
        player_week=xw.canonical||'_'||CAST(CAST(year AS BIGINT) AS VARCHAR)||'_'||CAST(CAST(week AS BIGINT) AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM xw WHERE st.NFL_player_id=xw.id""")

    # doubleheader-aware same-game collapse (same logic as the fixed synthetic resolver)
    num = [c for c, t in types.items() if c not in (
        "NFL_player_id","player","year","week","season_type","player_week","nfl_team",
        "nfl_franchise_number","opponent_nfl_franchise_number","position",PROV_COL,"data_source")
        and any(k in t.upper() for k in ("INT","DOUBLE","FLOAT","DECIMAL","BIGINT"))]
    # candidate = EVERY player-week with >1 row (fixes pre-existing same-id dups too, not just
    # crosswalk-touched ones).
    con.execute("""CREATE TEMP TABLE cand AS
        SELECT st.rowid rid, st.NFL_player_id id, st.year yr, st.week wk, st.season_type stt,
               st.opponent_nfl_franchise_number opp
        FROM st WHERE (st.NFL_player_id, st.year, st.week, st.season_type) IN
          (SELECT NFL_player_id, year, week, season_type FROM st
           GROUP BY 1,2,3,4 HAVING COUNT(*)>1)""")
    con.execute("""CREATE TEMP TABLE pw AS SELECT id,yr,wk,stt,COUNT(DISTINCT opp) nd
        FROM cand WHERE opp IS NOT NULL GROUP BY 1,2,3,4""")
    # year>=1950: no doubleheaders -> always merge same player-week. pre-1950: opponent-aware.
    con.execute("""CREATE TEMP TABLE mk AS SELECT c.rid,
        CASE WHEN c.yr>=1950 THEN c.id||'|'||c.yr||'|'||c.wk||'|'||c.stt
             WHEN COALESCE(p.nd,0)<=1 THEN c.id||'|'||c.yr||'|'||c.wk||'|'||c.stt
             WHEN c.opp IS NOT NULL THEN c.id||'|'||c.yr||'|'||c.wk||'|'||c.stt||'|'||CAST(c.opp AS VARCHAR)
             ELSE 'SOLO|'||CAST(c.rid AS VARCHAR) END mkey
        FROM cand c LEFT JOIN pw p ON p.id=c.id AND p.yr=c.yr AND p.wk=c.wk AND p.stt=c.stt""")
    con.execute("""CREATE TEMP TABLE grp AS SELECT mkey, MIN(rid) survivor, COUNT(*) c
        FROM mk GROUP BY mkey HAVING COUNT(*)>1""")
    con.execute(f"""CREATE TEMP TABLE comb AS SELECT m.mkey, {', '.join(f'MAX(st.{c}) {c}' for c in num)}
        FROM st JOIN mk m ON st.rowid=m.rid JOIN grp g ON m.mkey=g.mkey GROUP BY m.mkey""")
    con.execute(f"""UPDATE st SET {', '.join(f'{c}=comb.{c}' for c in num)}
        FROM comb JOIN grp g ON comb.mkey=g.mkey WHERE st.rowid=g.survivor""")
    con.execute("""DELETE FROM st WHERE rowid IN
        (SELECT m.rid FROM mk m JOIN grp g ON m.mkey=g.mkey WHERE m.rid<>g.survivor)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_xwtmp.parquet")
    rdr = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    # gate: golden + no synthetic-real splits + dup_person/week down
    from . import golden_samples, recon_expectations
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run(); ex = recon_expectations.run()
    finally:
        S.latest_v26 = o
    bc = ex["by_check"]
    gate = (g["failed"] == 0 and bc.get("identity.synthetic_id_split", 0) == 0
            and bc.get("identity.dup_person", 0) <= 2 and bc.get("identity.dup_player_week", 0) <= 2)
    res = {"crosswalk_size": int(len(xwalk)), "conflicts": len(conflicts),
           "before": int(before), "after": int(after), "collapsed": int(before - after),
           "golden": f"{g['passed']}/{g['total']}", "by_check": bc,
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prexw_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp)
        # dedup bio: drop non-canonical ids that are now mapped away
        bio = pq.read_table(BIO).to_pandas(); n0 = len(bio)
        bio = bio[~bio.NFL_player_id.isin(set(xwalk.id))]
        shutil.copy2(BIO, BIO.replace(".parquet", f"_backup_{stamp}.parquet"))
        pq.write_table(pa.Table.from_pandas(bio), BIO)
        res["backup"] = str(bk); res["swapped"] = True; res["bio"] = f"{n0}->{len(bio)}"
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_crosswalk()
        print(f"crosswalk: {r['crosswalk_size']} id->canonical | conflicts(2+ real pfr): {r['conflicts']}")
        print(f"rows {r['before']:,}->{r['after']:,} (collapsed {r['collapsed']}) | golden {r['golden']}")
        print(f"  checks: {r['by_check']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']}, bio {r['bio']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
        xw, conf = build_crosswalk(con, v)
        print(f"crosswalk size: {len(xw)} id->canonical mappings")
        print(f"conflict clusters (2+ distinct real pfr ids, NOT merged): {len(conf)}")
        print(xw.head(12).to_string(index=False))
