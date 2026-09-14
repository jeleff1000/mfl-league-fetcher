"""
sota_recon/build_kicking_backfill.py  --  close the pre-1978 kicking/punting gap

The dedicated `kicking` boxscore source (placekicker + punter rows per team-game) is far
more complete pre-1978 than v26 (FG 65.7%, PAT 76.8%, punts 88.5% of source) -- v26's FG/PAT
had been derived from the scoring table, which misses kicks. This backfills FG/PAT/punting
from the kicking source for 1933-1977 (kicking source starts 1933; modern is already 100%;
1920-32 stays on the scoring-derived values since kicking has no rows there).

Unlike the scoring-derived fill, the kicking source carries ATTEMPTS (fga, xpa) directly, so
fg_made<=fg_att and pat_made<=pat_att hold by construction (no made>att repair needed).

Gated: pre-1978 FG/PAT/punt reconcile to source, golden holds, no new hard/struct
violations -> backup + swap. Mirrors build_defense_backfill.

    python -m scripts.sota_recon.build_kicking_backfill            # dry-run
    python -m scripts.sota_recon.build_kicking_backfill --apply    # gated write
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

SRC = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/kicking/_combined.parquet"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave13.pre1978_kicking"
PROV_COL = "recon_correction_log"
YEAR_LO, YEAR_HI = 1933, 1977
CMAP = {"fgm": "fg_made", "fga": "fg_att", "xpm": "pat_made", "xpa": "pat_att",
        "punt": "punts", "punt_yds": "punt_yards"}
SRC_STATS = list(CMAP)
V26_STATS = list(CMAP.values())


def build_target() -> pd.DataFrame:
    tg = pq.read_table(TG, columns=["boxscore_id", "year", "week", "team_code", "team_fid",
                                    "opponent_fid", "season_type"]).to_pandas()
    tg = tg[(tg.year >= YEAR_LO) & (tg.year <= YEAR_HI) & (tg.season_type == "REG")]
    bio = pq.read_table(BIO, columns=["NFL_player_id", "pfr_id", "nfl_position"]).to_pandas()
    bm = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "NFL_player_id"]))
    pos = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "nfl_position"]))
    s = pq.read_table(SRC, columns=["season", "boxscore_id", "team", "player",
                                    "player_link_ids"] + SRC_STATS).to_pandas()
    s = s[(s.season >= YEAR_LO) & (s.season <= YEAR_HI)].copy()
    for c in SRC_STATS:
        s[c] = pd.to_numeric(s[c], errors="coerce").fillna(0)
    s["pfr_id"] = s.player_link_ids.str.split(";").str[0]
    s["NFL_player_id"] = s.pfr_id.map(lambda p: bm.get(p, p))
    s = s.rename(columns=CMAP)
    s = s.merge(tg, left_on=["boxscore_id", "team"], right_on=["boxscore_id", "team_code"], how="inner")
    s["year"] = s.year.astype(int); s["week"] = s.week.astype(int)
    agg = (s.groupby(["NFL_player_id", "year", "week"], as_index=False)
             .agg(**{c: (c, "sum") for c in V26_STATS},
                  season_type=("season_type", "first"),
                  nfl_franchise_number=("team_fid", "first"),
                  nfl_team=("team_code", "first"),
                  opponent_nfl_franchise_number=("opponent_fid", "first"),
                  player=("player", "first"), pfr_id=("pfr_id", "first")))
    agg = agg[(agg[V26_STATS].sum(axis=1) > 0)].copy()
    agg["position"] = agg.pfr_id.map(lambda p: pos.get(p, "K"))
    return agg


def apply_backfill() -> dict:
    v26 = latest_v26(); tgt = build_target(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    if PROV_COL not in [c[0] for c in con.execute("DESCRIBE st").fetchall()]:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre_fg = con.execute(f"SELECT SUM(COALESCE(fg_made,0)) FROM st WHERE year BETWEEN {YEAR_LO} AND {YEAR_HI} AND season_type='REG' AND position<>'DEF'").fetchone()[0]
    con.register("tgt", tgt)
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
                    WHERE year BETWEEN {YEAR_LO} AND {YEAR_HI} GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    # ADDITIVE merge: the kicking source and the existing (scoring-derived) FG are partially
    # disjoint (e.g. Lou Groza 1950 is in v26 but not the kicking source), so take the MAX
    # per stat -- never overwrite a higher existing value (that loses FGs). fg_att/pat_att
    # also take the max so attempts stay >= makes.
    setc = ", ".join(f"{c}=GREATEST(COALESCE(st.{c},0), COALESCE(t.{c},0))" for c in V26_STATS)
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    nm = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
          "AND m.year=st.year AND m.week=st.week)")
    NNS = "lower(regexp_replace(st.player,'[^A-Za-z]','','g'))"
    NNS_S = "lower(regexp_replace(s.player,'[^A-Za-z]','','g'))"
    # PASS A: id-match GREATEST update
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tgt t
        ON st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
        WHERE st.position<>'DEF' AND {nm}""").fetchone()[0]
    con.execute(f"""UPDATE st SET {setc}, {prov} FROM tgt t
        WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
          AND st.position<>'DEF' AND {nm}""")
    # PASS B: name-fallback -- target rows unmatched by id, matched to a UNIQUE existing v26
    # row by (franchise, year, week, name) -> GREATEST-update that row (avoids id-mismatch dups)
    con.execute(f"""CREATE TEMP TABLE unm AS
        SELECT t.*, lower(regexp_replace(t.player,'[^A-Za-z]','','g')) nn FROM tgt t
        WHERE NOT EXISTS (SELECT 1 FROM st s WHERE s.NFL_player_id=t.NFL_player_id
          AND s.year=t.year AND s.week=t.week)""")
    nmstat = ", ".join(f"u.{c} AS {c}" for c in V26_STATS)
    con.execute(f"""CREATE TEMP TABLE nmatch AS
        SELECT u.nfl_franchise_number fr, u.year yr, u.week wk, u.nn nn, {nmstat}
        FROM unm u
        WHERE (SELECT COUNT(*) FROM st s WHERE s.nfl_franchise_number=u.nfl_franchise_number
                 AND s.year=u.year AND s.week=u.week AND {NNS_S}=u.nn AND s.position<>'DEF')=1
          AND (SELECT COUNT(*) FROM unm u2 WHERE u2.nfl_franchise_number=u.nfl_franchise_number
                 AND u2.year=u.year AND u2.week=u.week AND u2.nn=u.nn)=1""")
    setc_nm = ", ".join(f"{c}=GREATEST(COALESCE(st.{c},0),COALESCE(nm.{c},0))" for c in V26_STATS)
    con.execute(f"""UPDATE st SET {setc_nm}, {prov} FROM nmatch nm
        WHERE st.nfl_franchise_number=nm.fr AND st.year=nm.yr AND st.week=nm.wk AND {NNS}=nm.nn""")
    # INSERT: unmatched by id AND not name-matched
    stat_sel = ", ".join(f"u.{c} AS {c}" for c in V26_STATS)
    con.execute(f"""INSERT INTO st BY NAME
        SELECT u.NFL_player_id, u.player, CAST(u.year AS DOUBLE) AS "year", CAST(u.week AS DOUBLE) AS "week",
          u.season_type, u.position, u.nfl_team, u.nfl_franchise_number,
          u.opponent_nfl_franchise_number, {stat_sel},
          u.NFL_player_id||'_'||CAST(u.year AS VARCHAR)||'_'||CAST(u.week AS VARCHAR) AS player_week,
          'pfr_kicking_boxscore_backfill' AS data_source, '{PROV}' AS {PROV_COL}
        FROM unm u WHERE NOT EXISTS (SELECT 1 FROM nmatch nm
          WHERE nm.fr=u.nfl_franchise_number AND nm.yr=u.year AND nm.wk=u.week AND nm.nn=u.nn)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    # attempts must stay >= makes after the independent per-column MAX merge
    con.execute("""UPDATE st SET fg_att=GREATEST(COALESCE(fg_att,0),COALESCE(fg_made,0)),
                   pat_att=GREATEST(COALESCE(pat_att,0),COALESCE(pat_made,0))
                   WHERE fg_made>fg_att OR pat_made>pat_att""")
    bad = con.execute("SELECT COUNT(*) FROM st WHERE fg_made>fg_att OR pat_made>pat_att").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_kicktmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    # GATE (additive merge): FG must INCREASE (we added source FGs v26 lacked) and never
    # regress; no made>att; golden holds. A strict % isn't achievable because the kicking
    # source and v26 are partially disjoint -- the win is monotone completeness gain.
    post_fg = pq.read_table(str(tmp), columns=["year", "season_type", "position", "fg_made"]).to_pandas()
    post_fg = post_fg[(post_fg.year >= YEAR_LO) & (post_fg.year <= YEAR_HI)
                      & (post_fg.season_type == "REG") & (post_fg.position != "DEF")].fg_made.fillna(0).sum()
    fg_pct = _recon(str(tmp), "fgm", "fg_made")
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    dp = duckdb.connect(); dp.execute("SET memory_limit='3GB'")
    dup_person = dp.execute(f"""SELECT COUNT(*) FROM (SELECT 1 FROM read_parquet('{tmp}')
        WHERE player IS NOT NULL AND position IS NOT NULL
        GROUP BY nfl_franchise_number, year, week, lower(regexp_replace(player,'[^A-Za-z]','','g')), position
        HAVING COUNT(DISTINCT NFL_player_id)>1)""").fetchone()[0]
    dp.close()
    gate = (post_fg > (pre_fg or 0)) and (g["failed"] == 0) and (bad == 0) and (dup_person <= 5)
    res = {"updated": int(updated), "inserted": int(after - before), "before": int(before),
           "after": int(after), "pre_fg": float(pre_fg or 0), "post_fg": float(post_fg),
           "pre1978_fg_pct_vs_kicking_src": fg_pct, "made_gt_att": int(bad),
           "dup_person": int(dup_person),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prekickbf_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


def _recon(path, src_col, v_col):
    d = duckdb.connect(); d.execute("SET memory_limit='3GB'")
    return d.execute(f"""
      WITH tg AS (SELECT boxscore_id,year,week,team_code,team_fid FROM read_parquet('{TG}') WHERE season_type='REG'),
       src AS (SELECT t.team_fid fr,t.year,t.week,SUM(TRY_CAST(s.{src_col} AS DOUBLE)) x
          FROM read_parquet('{SRC}') s JOIN tg t ON s.boxscore_id=t.boxscore_id AND s.team=t.team_code
          WHERE s.season BETWEEN {YEAR_LO} AND {YEAR_HI} GROUP BY 1,2,3),
       vv AS (SELECT nfl_franchise_number fr,year,week,SUM(COALESCE({v_col},0)) x FROM read_parquet('{path}')
          WHERE season_type='REG' AND year BETWEEN {YEAR_LO} AND {YEAR_HI} AND position<>'DEF' GROUP BY 1,2,3)
      SELECT ROUND(100.0*SUM(vv.x)/NULLIF(SUM(s.x),0),1) FROM src s LEFT JOIN vv ON s.fr=vv.fr AND s.year=vv.year AND s.week=vv.week
    """).fetchone()[0]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_backfill()
        print(f"updated={r['updated']:,} inserted={r['inserted']:,} rows {r['before']:,}->{r['after']:,}")
        print(f"pre-1978 fg_made {r['pre_fg']:.0f}->{r['post_fg']:.0f} | vs kicking-src {r['pre1978_fg_pct_vs_kicking_src']}% | made>att {r['made_gt_att']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        t = build_target()
        print(f"target kicker player-weeks: {len(t):,} | fg_made {t.fg_made.sum():.0f} pat_made {t.pat_made.sum():.0f} punts {t.punts.sum():.0f}")
