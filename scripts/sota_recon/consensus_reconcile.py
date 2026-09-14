"""
sota_recon/consensus_reconcile.py  --  drive each team-level non-derived stat to MULTI-SOURCE consensus

The mandate: every non-derived column provably perfect for every game, reconciled against ALL source
docs -- not one source with GREATEST, not "96.5% is fine". Where >=2 INDEPENDENT records agree on a
value, that value is the truth; v26's team row is set to it. Only games where the sources themselves
have no >=2 agreement (or no witness) are residual -- the genuine source ceiling, reported per era.

Each stat lists its independent witnesses at team-game grain (year, week, franchise, opponent).
Witness kinds:
  teamline(field)  team_stats packed line for THIS team (parsed) -- e.g. our sacks-allowed
  teamline_opp(f)  team_stats packed line for the OPPONENT -> our defensive stat (their INT-thrown
                   == our def INT; their fumbles-lost ~ our recoveries; their sacked == our sacks)
  oppsum(tbl,col)  sum the OPPONENT's player rows (their pass_int == our def INT, etc.)
  idp(col)         sum v26's own IDP rows (the individual-defender attribution)
  pbp(field)       play-by-play reconstruction (pbpsk etc.) [reserved]

consensus(game) = the value shared by >=2 independent witnesses (mode); None if no 2 agree.
apply: set the v26 TEAM/DST row's column to the consensus where it exists (insert DST row if the
game has a consensus but no DST row). IDP detail rows are left as-is (team total is the target;
pre-1950 individual attribution is a separate, documented limit).

Gated: golden 24/24, post agreement-with-consensus == the resolvable ceiling, row count delta ==
inserted DST rows only.

    python -m scripts.sota_recon.consensus_reconcile --stat def_interceptions          # dry-run
    python -m scripts.sota_recon.consensus_reconcile --stat def_interceptions --apply
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
PROV_COL = "recon_correction_log"
ERAS = [("<1933", 0, 1932), ("1933-49", 1933, 1949), ("1950-77", 1950, 1977), ("1978+", 1978, 2025)]

# O.5 lineage honesty (lineage_roots.v1.json): every EXTERNAL witness kind here is the
# PFR boxscore scrape (one root) -- teamline/oppsum/scoring/tg_auth are correlated copies,
# so ">=2 agreement" is intra-root copy-consistency, not cross-root confirmation. The
# 'idp'/'idp_expr' kinds sum the SUBJECT's own rows: under LOROO the subject may not vote
# for itself -- re-typing that participant is QUEUED as OQ-LR-4 in lineage_roots.v1.json
# (apply-lane semantics change; adjudication required, never an improvised edit).
WITNESS_KIND_ROOT = {"teamline": "pfr", "teamline_opp": "pfr", "oppsum": "pfr",
                     "scoring": "pfr", "tg_auth": "pfr", "pbp": "pfr",
                     "idp": "internal_subject", "idp_expr": "internal_subject"}

# stat -> list of witnesses; each witness builds (yr,wk,fr,opp,val) for the DEFENDING/own team.
# teamline field index is into Cmp-Att-Yd-TD-INT (1..5) or Sacked-Yards (1..2) or Fumbles-Lost(1..2)
WITNESS_REG = {
    "def_interceptions": [
        ("teamline_opp", "Cmp-Att-Yd-TD-INT", 5),     # opp INTs thrown = our def INT
        ("oppsum", "player_offense", "pass_int"),      # opp QBs' INTs thrown
        ("idp", "def_interceptions"),                  # our IDP def_int sum
    ],
    "def_sacks": [
        ("teamline_opp", "Sacked-Yards", 1),           # opp times sacked = our sacks
        ("oppsum", "player_offense", "pass_sacked"),   # opp QBs sacked
        ("idp", "def_sacks"),                          # our IDP sacks sum
    ],
    "def_fumbles": [
        ("teamline_opp", "Fumbles-Lost", 2),           # opp fumbles LOST ~ our recoveries
        ("idp", "def_fumbles"),                        # our IDP recoveries sum
    ],
    # points_allowed is the TOTAL points the opponent scored = the official scoreboard, so it is
    # enforced directly against the game record (tg_auth authority = opponent_points). This guards
    # against reconstruction drift (the 2014+ inflation that came from rebuilding the score from
    # component TD/FG/PAT counts instead of reading the scoreboard). The fantasy-adjusted value that
    # excludes points scored on THIS team's turnovers (pick-6 / fumble-6) lives in dst_points_allowed,
    # derived deterministically as points_allowed - 6*(opp int/fum-ret TDs) - 2*(opp safeties) by
    # build_pa_reconciled_v26 / wave4c -- not a raw witness, so it is not reconciled here.
    "points_allowed": [
        ("tg_auth", "opponent_points"),                # official scoreboard: what this defense allowed
    ],
    # team-DST scoring atoms: scoring-play classification + IDP sums (2 independent witnesses)
    "def_tds": [
        ("scoring", "sc_def_td"),                       # scoring-play classification (int/fum return)
        ("idp_expr", "def_int_ret_td + fum_ret_td"),    # the individual scorers
    ],
    "special_teams_tds": [
        ("scoring", "sc_st_td"),                        # scoring-play classification (kick/punt return)
        ("idp_expr", "kickoff_return_tds + punt_return_tds"),
    ],
}


def _tg(con):
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr, opponent_fid ofr, is_home,
        team_points, opponent_points
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")


def _scoring_witness(con):
    """per (yr,wk,fr) scoring-play counts via score-delta team attribution (sc_def_td/sc_st_td...)."""
    SC = f"read_parquet('{BOX}/scoring/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scd AS
        SELECT boxscore_id, lower(COALESCE(description,'')) d,
          TRY_CAST(vis_team_score AS INT) - LAG(TRY_CAST(vis_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dvis,
          TRY_CAST(home_team_score AS INT) - LAG(TRY_CAST(home_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dhome
        FROM {SC} WHERE description IS NOT NULL AND description<>''""")
    con.execute("""CREATE OR REPLACE TEMP TABLE scs AS
        SELECT boxscore_id, d, (dhome>0 AND dhome>=dvis) is_home FROM scd WHERE dvis>0 OR dhome>0""")
    con.execute("""CREATE OR REPLACE TEMP TABLE scw AS
        SELECT g.yr, g.wk, g.fr, g.ofr opp,
          SUM(CASE WHEN d LIKE '%interception return%' OR d LIKE '%fumble return%'
                    OR (d LIKE '%fumble recovery%' AND d LIKE '%end zone%') THEN 1 ELSE 0 END) sc_def_td,
          SUM(CASE WHEN d LIKE '%kickoff return%' OR d LIKE '%punt return%'
                    OR d LIKE '%blocked%return%' OR d LIKE '%missed field goal return%' THEN 1 ELSE 0 END) sc_st_td
        FROM scs JOIN tg g ON scs.boxscore_id=g.boxscore_id AND scs.is_home=g.is_home GROUP BY 1,2,3,4""")


def _witness(con, name, spec, V):
    kind = spec[0]
    if kind == "teamline_opp":
        statname, idx = spec[1], spec[2]
        # opponent's packed line attributed to the DEFENDING team (ofr); opp = the throwing team (fr)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            WITH j AS (SELECT g.yr, g.wk, g.ofr fr, g.fr opp,
                         CASE WHEN g.is_home THEN ts.home_stat ELSE ts.vis_stat END val
                       FROM tg g JOIN read_parquet('{BOX}/team_stats/_combined.parquet') ts
                         ON ts.boxscore_id=g.boxscore_id AND ts.stat='{statname}')
            SELECT yr, wk, fr, opp, SUM(TRY_CAST(string_split(val,'-')[{idx}] AS INT)) v
            FROM j WHERE len(string_split(val,'-'))={5 if statname.startswith('Cmp') else 2}
            GROUP BY 1,2,3,4""")
    elif kind == "oppsum":
        tbl, col = spec[1], spec[2]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            SELECT tg.yr, tg.wk, tg.ofr fr, tg.fr opp, SUM(TRY_CAST(s.{col} AS DOUBLE)) v
            FROM read_parquet('{BOX}/{tbl}/_combined.parquet') s JOIN tg
              ON s.boxscore_id=tg.boxscore_id AND s.team=tg.team_code
            WHERE tg.ofr IS NOT NULL GROUP BY 1,2,3,4""")
    elif kind == "idp":
        col = spec[1]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
                   opponent_nfl_franchise_number opp, SUM(COALESCE({col},0)) v
            FROM {V} WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
    elif kind == "idp_expr":
        expr = spec[1]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
                   opponent_nfl_franchise_number opp, SUM(COALESCE({expr},0)) v
            FROM {V} WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
    elif kind == "scoring":
        field = spec[1]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            SELECT yr, wk, fr, opp, CAST({field} AS DOUBLE) v FROM scw""")
    elif kind == "tg_auth":
        field = spec[1]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS
            SELECT yr, wk, fr, ofr opp, CAST({field} AS DOUBLE) v FROM tg""")
    return name


def _consensus_table(con, stat, V):
    """build cons(yr,wk,fr,opp,consensus,nagree) from all witnesses of stat.
    If a witness is an authority (tg_auth = official game record), the consensus IS that value
    (no >=2 vote needed). Otherwise consensus = value shared by >=2 independent witnesses."""
    specs = WITNESS_REG[stat]
    if any(s[0] == "scoring" for s in specs):
        _scoring_witness(con)
    # authority short-circuit (e.g. points_allowed == official opponent_points)
    auth = next((i for i, s in enumerate(specs) if s[0] == "tg_auth"), None)
    if auth is not None:
        nm = _witness(con, "wA", specs[auth], V)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cons AS
            SELECT yr, wk, fr, opp, v consensus, 99 nagree FROM {nm} WHERE v IS NOT NULL""")
        return 1
    names = [_witness(con, f"w{i}", s, V) for i, s in enumerate(specs)]
    # full-outer-join all witnesses
    base = names[0]
    sel = [f"{base}.yr", f"{base}.wk", f"{base}.fr", f"{base}.opp"] + [f"{base}.v v0"]
    frm = base
    for i, n in enumerate(names[1:], 1):
        sel.append(f"{n}.v v{i}")
        frm += f" FULL JOIN {n} USING(yr,wk,fr,opp)"
    # coalesce keys across the full joins
    keys = "COALESCE(" + ",".join(f"{n}.yr" for n in names) + ") yr, " \
         + "COALESCE(" + ",".join(f"{n}.wk" for n in names) + ") wk, " \
         + "COALESCE(" + ",".join(f"{n}.fr" for n in names) + ") fr, " \
         + "COALESCE(" + ",".join(f"{n}.opp" for n in names) + ") opp"
    vcols = ", ".join(f"{n}.v v{i}" for i, n in enumerate(names))
    con.execute(f"CREATE OR REPLACE TEMP TABLE allw AS SELECT {keys}, {vcols} FROM {frm}")
    nv = len(names)
    vlist = [f"v{i}" for i in range(nv)]
    # consensus = a value shared by >=2 non-null witnesses (pick the most-agreed); else NULL
    # build via pairwise: for each value, count how many witnesses equal it
    pair_terms = []
    for i in range(nv):
        cnt = " + ".join(f"(CASE WHEN {vlist[j]} IS NOT NULL AND abs({vlist[i]}-{vlist[j]})<0.5 THEN 1 ELSE 0 END)" for j in range(nv))
        pair_terms.append(f"CASE WHEN {vlist[i]} IS NOT NULL THEN ({cnt}) ELSE 0 END")
    # the consensus value: the vi with the highest agreement count (>=2)
    agree_exprs = ", ".join(f"{pair_terms[i]} a{i}" for i in range(nv))
    con.execute(f"CREATE OR REPLACE TEMP TABLE agr AS SELECT *, {agree_exprs} FROM allw")
    # pick consensus = the vi whose ai is max and >=2
    case_cons = "CASE "
    for i in range(nv):
        others = " AND ".join(f"a{i} >= a{j}" for j in range(nv) if j != i)
        case_cons += f"WHEN a{i} >= 2{(' AND ' + others) if others else ''} THEN v{i} "
    case_cons += "ELSE NULL END consensus"
    maxa = "GREATEST(" + ",".join(f"a{i}" for i in range(nv)) + ") nagree"
    con.execute(f"CREATE OR REPLACE TEMP TABLE cons AS SELECT yr,wk,fr,opp, {case_cons}, {maxa} FROM agr")
    return nv


def _era(y):
    for nm, lo, hi in ERAS:
        if lo <= y <= hi:
            return nm
    return "?"


def run(stat, apply=False):
    v26 = latest_v26()
    if not apply:
        con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        _tg(con); _consensus_table(con, stat, f"read_parquet('{v26}')")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE dst AS SELECT CAST(year AS INT) yr,
            CAST(week AS INT) wk, nfl_franchise_number fr, opponent_nfl_franchise_number opp,
            SUM(COALESCE({stat},0)) v FROM read_parquet('{v26}') WHERE position='DEF'
            AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
        df = con.execute("""SELECT c.yr, c.consensus, d.v dstv FROM cons c
            LEFT JOIN dst d USING(yr,wk,fr,opp) WHERE c.consensus IS NOT NULL""").df()
        df["era"] = df.yr.map(_era)
        import pandas as pd
        print(f"[{stat}] consensus availability + current v26-DST match, by era:")
        for nm, lo, hi in ERAS:
            dd = df[df.era == nm]
            if dd.empty: continue
            match = (abs(dd.dstv.fillna(-1) - dd.consensus) < 0.5).mean() * 100
            print(f"  {nm:9s} consensus-games={len(dd):6d}  v26 matches consensus now={match:5.1f}%")
        con.close(); return
    # ---- apply ----
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
    _tg(con); _consensus_table(con, stat, "st")
    PROV = f"consensus_reconcile.{stat}"
    prov = f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END"
    # set the DST team-row column to the consensus where it exists
    con.execute(f"""UPDATE st SET {stat}=c.consensus, {prov} FROM cons c
        WHERE st.position='DEF' AND CAST(st.year AS INT)=c.yr AND CAST(st.week AS INT)=c.wk
          AND st.nfl_franchise_number=c.fr AND st.opponent_nfl_franchise_number IS NOT DISTINCT FROM c.opp
          AND c.consensus IS NOT NULL AND COALESCE(st.{stat},0) IS DISTINCT FROM c.consensus""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    # post agreement
    con.execute(f"""CREATE OR REPLACE TEMP TABLE dst AS SELECT CAST(year AS INT) yr, CAST(week AS INT) wk,
        nfl_franchise_number fr, opponent_nfl_franchise_number opp, SUM(COALESCE({stat},0)) v
        FROM st WHERE position='DEF' AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3,4""")
    post = con.execute("""SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(d.v,-1)-c.consensus)<0.5 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM cons c LEFT JOIN dst d USING(yr,wk,fr,opp) WHERE c.consensus IS NOT NULL""").fetchone()[0]

    vp = Path(v26); tmp = vp.with_name(vp.stem + f"_cons_{stat}_tmp.parquet")
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
    gate = (g["failed"] == 0) and (after == before) and (post >= 99.9)
    res = {"stat": stat, "before": before, "after": after, "post_consensus_match": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_precons_{stat}_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stat", required=True, choices=list(WITNESS_REG))
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = run(a.stat, apply=True)
        print(f"[{r['stat']}] rows {r['before']:,}->{r['after']:,} | post v26-DST==consensus {r['post_consensus_match']}% | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        run(a.stat, apply=False)
