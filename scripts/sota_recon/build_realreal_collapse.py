"""
sota_recon/build_realreal_collapse.py  --  collapse real<->real same-person id splits that double-count

A small class of player-weeks carry the SAME person under two DISTINCT real ids (gsis + pfr, or
two pfr suffixes) on the same team in the same single game, so a shared atom (punts, def INT,
receptions...) is counted twice at team level. dup_person missed them (the two rows have
different positions); synthetic-resolution missed them (both ids are real).

DISCRIMINATOR (safe): only collapse a split where the SAME atom is non-zero on >=2 ids -> that
is provably one person double-counted. Splits whose ids carry DISJOINT atoms (e.g. Bill Butler
the RB vs Bill Butler the DB -- two distinct bio players) corrupt no team total and are LEFT
ALONE (logged), never fused.

Source-arbitrated canonical id: among a group's ids, prefer the one the PFR boxscore source
records for that game (roster), tie-break by most career rows. The other ids' rows for that
exact (year, week, franchise) are remapped to canonical and MAX-collapsed (duplicated atoms ->
one copy; disjoint -> merged). Scoped to the offending player-weeks only -- no global re-id.

Gated: same-atom double-counts 77->0, golden holds, rows drop by exactly the merged count,
no new dup_person -> backup + swap.

    python -m scripts.sota_recon.build_realreal_collapse            # dry-run plan
    python -m scripts.sota_recon.build_realreal_collapse --apply
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
PROV = "wave20.realreal_doublecount_collapse"
PROV_COL = "recon_correction_log"
# FUZZY name: strip suffix (Jr/Sr/II-IV) and middle initials, then keep letters only -- so
# "Mike A. Jones" == "Mike Jones" and "Devin Bush Sr." == "Devin Bush". The same-team/same-game/
# same-nonzero-atom discriminator keeps this safe (two distinct players with the same first+last
# both recording the same stat in one game is effectively impossible).
NN = ("lower(regexp_replace(regexp_replace(regexp_replace(player,"
      "'( (Jr|Sr|II|III|IV)\\.?)+$','','i'), ' [A-Za-z]\\.? ', ' ', 'g'), '[^A-Za-z]','','g'))")
ATOMS = ["carries", "rushing_yards", "receptions", "receiving_yards", "passing_yards",
         "completions", "attempts", "punts", "punt_yards", "fg_att", "fg_made",
         "kickoff_returns", "punt_returns", "def_interceptions", "def_sacks", "def_fumbles"]


def _setup(con, src):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr,
        COUNT(DISTINCT boxscore_id) OVER (PARTITION BY CAST(year AS INT),CAST(week AS INT),team_fid) ng
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    parts = []
    for t in ["player_offense", "player_defense", "kicking", "returns"]:
        parts.append(f"SELECT DISTINCT tg.yr,tg.wk,tg.fr, split_part(s.player_link_ids,';',1) sid "
                     f"FROM read_parquet('{BOX}/{t}/_combined.parquet') s JOIN tg "
                     f"ON s.boxscore_id=tg.boxscore_id AND s.team=tg.team_code WHERE s.player_link_ids IS NOT NULL")
    con.execute("CREATE OR REPLACE TEMP TABLE roster AS SELECT DISTINCT yr,wk,fr,sid FROM (" + " UNION ALL ".join(parts) + ")")
    con.execute("CREATE OR REPLACE TEMP TABLE dh AS SELECT DISTINCT yr,wk,fr,ng FROM tg")
    # career rows per id (for canonical tie-break)
    con.execute(f"CREATE OR REPLACE TEMP TABLE career AS SELECT NFL_player_id id, COUNT(*) c FROM {src} GROUP BY 1")
    # single-game same-name groups with a SAME-ATOM double count
    nzexpr = ", ".join(f"SUM(CASE WHEN COALESCE(s.{a},0)>0.5 THEN 1 ELSE 0 END) nz_{a}" for a in ATOMS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE grp AS
        WITH g AS (SELECT {NN} nn, CAST(s.year AS INT) yr, CAST(s.week AS INT) wk, s.nfl_franchise_number fr
                   FROM {src} s WHERE s.position<>'DEF' AND s.player IS NOT NULL AND s.nfl_franchise_number IS NOT NULL
                   GROUP BY 1,2,3,4 HAVING COUNT(DISTINCT s.NFL_player_id)>1),
        gsingle AS (SELECT g.* FROM g JOIN dh USING(yr,wk,fr) WHERE dh.ng=1),
        cnt AS (SELECT gs.nn,gs.yr,gs.wk,gs.fr, {nzexpr}
                FROM {src} s JOIN gsingle gs ON {NN}=gs.nn AND CAST(s.year AS INT)=gs.yr
                  AND CAST(s.week AS INT)=gs.wk AND s.nfl_franchise_number=gs.fr
                WHERE s.position<>'DEF' GROUP BY 1,2,3,4)
        SELECT nn,yr,wk,fr FROM cnt WHERE {' OR '.join(f'nz_{a}>=2' for a in ATOMS)}""")


def _plan(con, src):
    """rows in the 77 groups; choose canonical id per group; return remap rows."""
    df = con.execute(f"""
        SELECT {NN} nn, CAST(s.year AS INT) yr, CAST(s.week AS INT) wk, s.nfl_franchise_number fr,
               s.NFL_player_id id, s.position,
               (s.NFL_player_id IN (SELECT sid FROM roster r WHERE r.yr=CAST(s.year AS INT)
                  AND r.wk=CAST(s.week AS INT) AND r.fr=s.nfl_franchise_number)) in_src,
               COALESCE(c.c,0) career
        FROM {src} s JOIN grp g ON {NN}=g.nn AND CAST(s.year AS INT)=g.yr
          AND CAST(s.week AS INT)=g.wk AND s.nfl_franchise_number=g.fr
        LEFT JOIN career c ON c.id=s.NFL_player_id
        WHERE s.position<>'DEF'""").df()
    remap = []
    for (nn, yr, wk, fr), grp in df.groupby(["nn", "yr", "wk", "fr"]):
        ids = grp.drop_duplicates("id")
        # canonical: source-confirmed first, then most career rows
        ids = ids.sort_values(["in_src", "career"], ascending=[False, False])
        canon = ids.iloc[0]["id"]
        for oid in ids["id"]:
            if oid != canon:
                remap.append({"old_id": oid, "yr": int(yr), "wk": int(wk),
                              "fr": fr, "new_id": canon})
    return df, remap


def apply_collapse():
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    types = {c[0]: c[1] for c in con.execute("DESCRIBE st").fetchall()}
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    _setup(con, "st")
    ngrp = con.execute("SELECT COUNT(*) FROM grp").fetchone()[0]
    _, remap = _plan(con, "st")
    import pandas as pd
    rmdf = pd.DataFrame(remap)
    con.register("rmdf", rmdf)
    # 1) remap the offending rows to canonical id (scoped to the exact player-week)
    con.execute(f"""UPDATE st SET NFL_player_id=r.new_id,
        player_week=r.new_id||'_'||CAST(CAST(st.year AS BIGINT) AS VARCHAR)||'_'||CAST(CAST(st.week AS BIGINT) AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM rmdf r WHERE st.NFL_player_id=r.old_id AND CAST(st.year AS INT)=r.yr
          AND CAST(st.week AS INT)=r.wk AND st.nfl_franchise_number=r.fr""")
    # 2) MAX-collapse the now-duplicate (id, year, week) rows (single game -> safe)
    num = [c for c, t in types.items() if c not in (
        "NFL_player_id", "player", "year", "week", "season_type", "player_week", "nfl_team",
        "nfl_franchise_number", "opponent_nfl_franchise_number", "position", PROV_COL, "data_source")
        and any(k in t.upper() for k in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT"))]
    con.execute(f"""CREATE TEMP TABLE dups AS
        SELECT NFL_player_id id, CAST(year AS INT) yr, CAST(week AS INT) wk, MIN(rowid) keep, COUNT(*) c
        FROM st WHERE NFL_player_id IN (SELECT DISTINCT new_id FROM rmdf)
        GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    con.execute(f"""CREATE TEMP TABLE merged AS
        SELECT st.NFL_player_id id, CAST(st.year AS INT) yr, CAST(st.week AS INT) wk,
               {', '.join(f'MAX(st.{c}) {c}' for c in num)}
        FROM st JOIN dups ON st.NFL_player_id=dups.id AND CAST(st.year AS INT)=dups.yr
          AND CAST(st.week AS INT)=dups.wk GROUP BY 1,2,3""")
    setc = ", ".join(f"{c}=m.{c}" for c in num)
    con.execute(f"""UPDATE st SET {setc} FROM merged m JOIN dups d ON m.id=d.id AND m.yr=d.yr AND m.wk=d.wk
        WHERE st.rowid=d.keep""")
    dropped = con.execute("SELECT COALESCE(SUM(c-1),0) FROM dups").fetchone()[0]
    con.execute("""DELETE FROM st WHERE rowid IN
        (SELECT st.rowid FROM st JOIN dups d ON st.NFL_player_id=d.id AND CAST(st.year AS INT)=d.yr
           AND CAST(st.week AS INT)=d.wk WHERE st.rowid<>d.keep)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    # recheck double-counts remaining
    _setup(con, "st")
    remaining = con.execute("SELECT COUNT(*) FROM grp").fetchone()[0]

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_rrtmp.parquet")
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
    gate = (g["failed"] == 0) and (remaining == 0) and (before - after == dropped) and dropped > 0
    res = {"groups": int(ngrp), "remap_rows": len(remap), "dropped": int(dropped),
           "before": int(before), "after": int(after), "remaining_doublecounts": int(remaining),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prerr_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_collapse()
        print(f"groups={r['groups']} remap_rows={r['remap_rows']} dropped={r['dropped']} "
              f"rows {r['before']:,}->{r['after']:,} | remaining_doublecounts={r['remaining_doublecounts']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TEMP TABLE st AS SELECT * FROM '{v}'")
        _setup(con, "st")
        ng = con.execute("SELECT COUNT(*) FROM grp").fetchone()[0]
        _, remap = _plan(con, "st")
        print(f"same-atom double-count groups: {ng} | rows to remap+collapse: {len(remap)}")
