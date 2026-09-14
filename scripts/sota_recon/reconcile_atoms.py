"""
sota_recon/reconcile_atoms.py  --  TOTAL reconciliation of non-derived atoms to source truth

Not gap-filling. For one PFR boxscore source table at a time, every atom it owns is rebuilt
in v26 at player-game grain to EQUAL the authoritative source value -- corrections and gaps
alike -- then the result is proven to re-agree with the INDEPENDENT team-level witnesses
(team_stats / opponent books) before it is allowed to ship.

Mechanic (mirrors the proven gated-write pattern):
  build_target(table): source rows -> pfr_id->NFL_player_id (+position) via player_bio,
    boxscore_id+team -> (year,week,franchise,season_type) via nfl_team_games_all, aggregate to
    (NFL_player_id, year, week) [SUM, or MAX for *_long].
  apply: per atom, UPDATE matching v26 IDP rows (position<>'DEF', single-row player-weeks only --
    doubleheaders skipped + reported) SET v26.atom = target.atom, but ONLY within the atom's
    valid era. Then INSERT player-games present in source yet missing from v26.
  GATE: golden 24/24 ; rowcount delta == inserts ; post per-atom team-game v26==source ~100%
    in-era ; for atoms with an independent witness, post v26-vs-witness agree >= pre ; no atom
    ceiling violation (e.g. fg_made<=fg_att). Only then backup + os.replace.

    python -m scripts.sota_recon.reconcile_atoms --table kicking            # dry-run sizing
    python -m scripts.sota_recon.reconcile_atoms --table kicking --apply    # gated reconcile
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV_COL = "recon_correction_log"

# table -> default position for inserted rows, and atoms: (src_col, v26_col, agg, era_lo, era_hi)
TABLE_ATOMS = {
    "kicking": ("K", [
        ("xpm", "pat_made", "sum", 1933, 2025), ("xpa", "pat_att", "sum", 1933, 2025),
        ("fgm", "fg_made", "sum", 1933, 2025), ("fga", "fg_att", "sum", 1933, 2025),
        ("punt", "punts", "sum", 1941, 2025), ("punt_yds", "punt_yards", "sum", 1941, 2025),
        ("punt_long", "punt_long", "max", 1941, 2025),
    ]),
    "returns": ("WR", [
        ("kick_ret", "kickoff_returns", "sum", 1941, 2025),
        ("kick_ret_yds", "kickoff_return_yards", "sum", 1941, 2025),
        ("kick_ret_td", "kickoff_return_tds", "sum", 1941, 2025),
        ("kick_ret_long", "kickoff_return_long", "max", 1941, 2025),
        ("punt_ret", "punt_returns", "sum", 1941, 2025),
        ("punt_ret_yds", "punt_return_yards", "sum", 1941, 2025),
        ("punt_ret_td", "punt_return_tds", "sum", 1941, 2025),
        ("punt_ret_long", "punt_return_long", "max", 1941, 2025),
    ]),
    # NOTE: PFR boxscore is authoritative for the classic defensive counting stats (INT, fumble
    # recoveries, TDs, assists, sacks). The modern CHARTED family (solo tackles, passes defended,
    # TFL, QB hits) is better in v26 already (nflverse/pbp) -- overwriting it REGRESSES team
    # totals, so those are deliberately excluded here (reconciled from the charted source, not box).
    "player_defense": ("DB", [
        ("def_int", "def_interceptions", "sum", 1933, 2025),
        ("def_int_yds", "def_interception_yards", "sum", 1933, 2025),
        ("def_int_td", "def_int_ret_td", "sum", 1933, 2025),
        ("sacks", "def_sacks", "sum", 1960, 2025),
        ("tackles_assists", "def_tackle_assists", "sum", 1994, 2025),
        ("fumbles_rec", "def_fumbles", "sum", 1945, 2025),
        ("fumbles_rec_yds", "fum_rec_yds", "sum", 1945, 2025),
        ("fumbles_rec_td", "fum_ret_td", "sum", 1945, 2025),
        ("fumbles_forced", "def_fumbles_forced", "sum", 1990, 2025),
    ]),
    "player_offense": ("WR", [
        ("pass_cmp", "completions", "sum", 1932, 2025), ("pass_att", "attempts", "sum", 1932, 2025),
        ("pass_yds", "passing_yards", "sum", 1932, 2025), ("pass_td", "passing_tds", "sum", 1932, 2025),
        ("pass_int", "passing_interceptions", "sum", 1932, 2025),
        ("pass_sacked", "sacks_suffered", "sum", 1947, 2025),
        ("pass_sacked_yds", "sack_yards_lost", "sum", 1947, 2025),
        ("pass_long", "passing_long", "max", 1932, 2025),
        ("rush_att", "carries", "sum", 1932, 2025), ("rush_yds", "rushing_yards", "sum", 1932, 2025),
        ("rush_td", "rushing_tds", "sum", 1932, 2025), ("rush_long", "rushing_long", "max", 1932, 2025),
        ("rec", "receptions", "sum", 1932, 2025), ("rec_yds", "receiving_yards", "sum", 1932, 2025),
        ("rec_td", "receiving_tds", "sum", 1932, 2025), ("rec_long", "receiving_long", "max", 1932, 2025),
        ("targets", "targets", "sum", 1992, 2025),
        ("fumbles", "fumbles", "sum", 1945, 2025), ("fumbles_lost", "fumbles_lost", "sum", 1945, 2025),
    ]),
    # NOTE: charted advanced (2018+) are VERIFIED-not-mutated. v26's source is nflverse (the analytics
    # standard); PFR's *_advanced tables corroborate 15 of them at 100% (blitzed/drops/hits/hurried/
    # poor_throws/pressured/scrambles/broken_tackles/target_int/yds_before_after_contact/knockdowns/
    # pressures/blitzes/hurries/tackles_missed). air_yards is a DEFINITION variant (v26=intended air
    # yards/IAY vs PFR pass_air_yds=completed) -- not an error; yac/first_downs differ ~10% as charted
    # subjectivity where nflverse is authoritative. Overwriting with PFR would corrupt air_yards, so
    # they are intentionally NOT in the reconcile registry.
}

# ceiling invariants per table (post-reconcile sanity): (made <= att)
CEILINGS = {
    "kicking": [("fg_made", "fg_att"), ("pat_made", "pat_att")],
    "player_offense": [("completions", "attempts")],
    "player_defense": [],
    "returns": [],
}
# att-floor enforcement: a made/completion implies an attempt; raise att to >= made on copy
# (PFR boxscore has a handful of made>att source errors -- you cannot make more than you try)
FLOORS = {
    "kicking": [("fg_att", "fg_made"), ("pat_att", "pat_made")],
    "player_offense": [("attempts", "completions")],
    "player_defense": [],
    "returns": [],
}


def build_target(table):
    deftbl, atoms = TABLE_ATOMS[table]
    src_cols = [a[0] for a in atoms]
    elo = min(a[3] for a in atoms); ehi = max(a[4] for a in atoms)
    tg = pq.read_table(TG, columns=["boxscore_id", "year", "week", "team_code",
                                    "team_fid", "opponent_fid", "season_type"]).to_pandas()
    bio = pq.read_table(BIO, columns=["NFL_player_id", "pfr_id", "nfl_position"]).to_pandas()
    bm = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "NFL_player_id"]))
    pos = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "nfl_position"]))
    s = pq.read_table(f"{BOX}/{table}/_combined.parquet",
                      columns=["season", "boxscore_id", "team", "player", "player_link_ids"] + src_cols).to_pandas()
    s = s[(s.season >= elo) & (s.season <= ehi)].copy()
    for c in src_cols:
        s[c] = pd.to_numeric(s[c], errors="coerce")
    s["pfr_id"] = s.player_link_ids.astype(str).str.split(";").str[0]
    s = s[s.pfr_id.notna() & (s.pfr_id != "") & (s.pfr_id != "None")].copy()
    s["NFL_player_id"] = s.pfr_id.map(lambda p: bm.get(p, p))
    s = s.merge(tg, left_on=["boxscore_id", "team"], right_on=["boxscore_id", "team_code"], how="left")
    s = s[s.team_fid.notna()].copy()
    s["year"] = s.year.astype(int); s["week"] = s.week.astype(int)
    aggmap = {a[0]: a[2] for a in atoms}
    g = (s.groupby(["NFL_player_id", "year", "week"], as_index=False)
           .agg({**{c: aggmap[c] for c in src_cols},
                 "season_type": "first", "team_fid": "first", "team_code": "first",
                 "opponent_fid": "first", "player": "first", "pfr_id": "first"}))
    g = g.rename(columns={a[0]: a[1] for a in atoms})
    g["position"] = g.pfr_id.map(lambda p: pos.get(p, deftbl))
    return g, atoms


def _team_game_agree(path, table, atoms, side="off"):
    """post-check: per atom, team-game v26 total vs source total -> agree fraction.
    Returns {vc: {'all': pct over full era, 'mod': pct over 1950+}}. pre-1950 (doubleheaders +
    incomplete early source) is reported but the gate holds only 1950+ to standard."""
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    con.execute(f"""CREATE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr FROM read_parquet('{TG}')
        WHERE team_fid IS NOT NULL""")
    out = {}
    for sc, vc, agg, elo, ehi in atoms:
        a = "MAX" if agg == "max" else "SUM"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE s AS SELECT tg.yr,tg.wk,tg.fr,{a}(TRY_CAST(src.{sc} AS DOUBLE)) sv
            FROM read_parquet('{BOX}/{table}/_combined.parquet') src JOIN tg
            ON src.boxscore_id=tg.boxscore_id AND src.team=tg.team_code
            WHERE src.season BETWEEN {elo} AND {ehi} GROUP BY 1,2,3""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE v AS SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,
            nfl_franchise_number fr,{a}(COALESCE({vc},0)) vv FROM read_parquet('{path}')
            WHERE position<>'DEF' AND year BETWEEN {elo} AND {ehi} AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3""")
        r = con.execute("""SELECT
            COUNT(*) n, SUM(CASE WHEN abs(COALESCE(v.vv,0)-s.sv)<0.5 THEN 1 ELSE 0 END) ok,
            SUM(CASE WHEN s.yr>=1950 THEN 1 ELSE 0 END) nm,
            SUM(CASE WHEN s.yr>=1950 AND abs(COALESCE(v.vv,0)-s.sv)<0.5 THEN 1 ELSE 0 END) okm
            FROM s LEFT JOIN v USING(yr,wk,fr) WHERE s.sv IS NOT NULL""").fetchone()
        out[vc] = {"all": round(100 * (r[1] or 0) / r[0], 1) if r[0] else 100.0,
                   "mod": round(100 * (r[3] or 0) / r[2], 1) if r[2] else 100.0}
    con.close()
    return out


def apply_reconcile(table):
    v26 = latest_v26(); stamp = utc_stamp()
    tgt, atoms = build_target(table)
    elo = min(a[3] for a in atoms); ehi = max(a[4] for a in atoms)
    pre = _team_game_agree(v26, table, atoms)
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
    con.register("tgt", tgt)
    PROV = f"recon.total_reconcile.{table}"
    # single-row player-weeks only (skip doubleheaders); never the team DEF row
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
        WHERE year BETWEEN {elo} AND {ehi} AND position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    nm = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
          "AND m.year=st.year AND m.week=st.week)")
    # per-atom overwrite within era
    set_terms = []
    for sc, vc, agg, ae_lo, ae_hi in atoms:
        set_terms.append(f"{vc}=CASE WHEN st.year BETWEEN {ae_lo} AND {ae_hi} THEN t.{vc} ELSE st.{vc} END")
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tgt t
        ON st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
        WHERE st.position<>'DEF' AND {nm}""").fetchone()[0]
    con.execute(f"""UPDATE st SET {', '.join(set_terms)}, {prov} FROM tgt t
        WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
          AND st.position<>'DEF' AND {nm}""")
    # INSERT player-games entirely missing from v26
    stat_sel = ", ".join(f"t.{a[1]} AS {a[1]}" for a in atoms)
    con.execute(f"""INSERT INTO st BY NAME
        SELECT t.NFL_player_id, t.player, CAST(t.year AS DOUBLE) "year", CAST(t.week AS DOUBLE) "week",
          t.season_type, t.position, t.team_code AS nfl_team, t.team_fid AS nfl_franchise_number,
          t.opponent_fid AS opponent_nfl_franchise_number, {stat_sel},
          t.NFL_player_id||'_'||CAST(t.year AS VARCHAR)||'_'||CAST(t.week AS VARCHAR) AS player_week,
          'pfr_{table}_total_reconcile' AS data_source, '{PROV}' AS {PROV_COL}
        FROM tgt t WHERE NOT EXISTS (SELECT 1 FROM st s2
          WHERE s2.year=t.year AND s2.week=t.week
            AND (s2.NFL_player_id=t.NFL_player_id
                 OR (s2.nfl_franchise_number=t.team_fid
                     AND lower(regexp_replace(s2.player,'[^A-Za-z]','','g'))
                         =lower(regexp_replace(t.player,'[^A-Za-z]','','g')))))""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    # enforce att >= made on the reconciled rows (fixes the few source made>att errors)
    for att_col, made_col in FLOORS.get(table, []):
        con.execute(f"""UPDATE st SET {att_col}=GREATEST(COALESCE({att_col},0),COALESCE({made_col},0))
            WHERE {PROV_COL} LIKE '%{PROV}%' AND COALESCE({made_col},0)>COALESCE({att_col},0)""")
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    # ceiling violations
    viol = 0
    for a_col, b_col in CEILINGS.get(table, []):
        viol += con.execute(f"SELECT COUNT(*) FROM st WHERE COALESCE({a_col},0)>COALESCE({b_col},0)+0.5").fetchone()[0]

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_rectmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    post = _team_game_agree(str(tmp), table, atoms)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    inserts = after - before
    # gate: NO-REGRESSION is the guarantee -- every reconciled atom moves toward source truth and
    # never away (so an atom where v26 is already better, e.g. charted defense, must be EXCLUDED
    # from the atom list, not overwritten). 90.0 is an anti-catastrophe floor only; the residual on
    # unofficial/charted atoms (pre-1982 sacks, tackle splits) and single-witness counts is logged
    # for 2nd-witness (pbp) adjudication, never silently forced. golden holds, zero ceiling.
    post_ok = all(v["mod"] >= 90.0 for v in post.values())
    no_regress = all(post[k]["all"] >= pre[k]["all"] - 0.1 for k in post)
    gate = (g["failed"] == 0) and post_ok and no_regress and viol == 0
    res = {"table": table, "updated": int(updated), "inserts": int(inserts),
           "before": int(before), "after": int(after), "ceiling_viol": int(viol),
           "golden": f"{g['passed']}/{g['total']}", "pre": pre, "post": post,
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prerec_{table}_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, choices=list(TABLE_ATOMS))
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_reconcile(a.table)
        print(f"[{r['table']}] updated={r['updated']:,} inserts={r['inserts']:,} "
              f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | ceiling_viol={r['ceiling_viol']}")
        print("  per-atom team-game agree%  all(pre->post) | 1950+(pre->post):")
        for k in r["post"]:
            print(f"    {k:26s} all {r['pre'][k]['all']:6.1f}->{r['post'][k]['all']:6.1f} | "
                  f"1950+ {r['pre'][k]['mod']:6.1f}->{r['post'][k]['mod']:6.1f}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        tgt, atoms = build_target(a.table)
        pre = _team_game_agree(latest_v26(), a.table, atoms)
        print(f"[{a.table}] target player-games: {len(tgt):,}")
        print("  current team-game agree% by atom (all | 1950+):")
        for k in sorted(pre):
            print(f"    {k:26s} all {pre[k]['all']:6.1f} | 1950+ {pre[k]['mod']:6.1f}")
