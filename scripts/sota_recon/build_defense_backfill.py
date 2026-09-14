"""
sota_recon/build_defense_backfill.py  --  close the pre-1978 IDP interception gap

The player_defense boxscore source (1933-2025) was never integrated into v26, so pre-1978
INDIVIDUAL defensive interceptions are incomplete (Night Train Lane 1952: v26 had 2, the
record is 14). The modern era (1978+) is corroborated by the PBP oracle, so this backfill
is scoped to 1933-1977 and to the cleanly-official INT family (def_interceptions,
def_interception_yards). Sacks (unofficial pre-1982), tackles (charted later), and the
def_tds double-count with the team DST row are deliberately OUT of scope here.

CRITICAL: this writes IDP (individual player) rows only -- matched by NFL_player_id, which
never hits the team DEF/DST row -- so IDP and DST stay separate.

Gated: Night Train Lane 1952 == 14, golden holds, per-team-game IDP INT reconciles to the
source, no new hard/struct violations -> backup + swap. Mirrors build_pre1978_backfill.

    python -m scripts.sota_recon.build_defense_backfill            # dry-run sizing
    python -m scripts.sota_recon.build_defense_backfill --apply    # gated write
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

SRC = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/player_defense/_combined.parquet"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave11.pre1978_idp_int"
PROV_COL = "recon_correction_log"
YEAR_LO, YEAR_HI = 1933, 1977
CMAP = {"def_int": "def_interceptions", "def_int_yds": "def_interception_yards"}
SRC_STATS = list(CMAP)
V26_STATS = list(CMAP.values())


def build_target() -> pd.DataFrame:
    tg = pq.read_table(TG, columns=["boxscore_id", "year", "week", "team_code",
                                    "team_fid", "opponent_fid", "season_type"]).to_pandas()
    tg = tg[(tg.year >= YEAR_LO) & (tg.year <= YEAR_HI)]
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
    s = s.merge(tg, left_on=["boxscore_id", "team"],
                right_on=["boxscore_id", "team_code"], how="left")
    s = s[s.team_fid.notna()].copy()
    s["year"] = s.year.astype(int); s["week"] = s.week.astype(int)
    agg = (s.groupby(["NFL_player_id", "year", "week"], as_index=False)
             .agg(**{c: (c, "sum") for c in V26_STATS},
                  season_type=("season_type", "first"),
                  nfl_franchise_number=("team_fid", "first"),
                  nfl_team=("team_code", "first"),
                  opponent_nfl_franchise_number=("opponent_fid", "first"),
                  player=("player", "first"), pfr_id=("pfr_id", "first")))
    agg = agg[agg.def_interceptions > 0].copy()   # only rows that add INT signal
    agg["position"] = agg.pfr_id.map(lambda p: pos.get(p, "DB"))
    return agg


def _idp_franchise_week(path, tgt):
    """per-(franchise,year,week) IDP INT (exclude the DST team row) vs source."""
    v = pq.read_table(path, columns=["year", "week", "position", "nfl_franchise_number",
                                     "def_interceptions"]).to_pandas()
    v = v[(v.year >= YEAR_LO) & (v.year <= YEAR_HI) & (v.position != "DEF")]
    vf = v.groupby(["nfl_franchise_number", "year", "week"], as_index=False).def_interceptions.sum()
    sf = tgt.groupby(["nfl_franchise_number", "year", "week"], as_index=False).def_interceptions.sum()
    m = sf.merge(vf, on=["nfl_franchise_number", "year", "week"], how="left",
                 suffixes=("_src", "_v")).fillna(0)
    return m


def apply_backfill() -> dict:
    v26 = latest_v26(); tgt = build_target(); stamp = utc_stamp()
    tmp_spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(tmp_spill, exist_ok=True)
    con = duckdb.connect(os.path.join(tmp_spill, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{tmp_spill}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.register("tgt", tgt)
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
                    WHERE year BETWEEN {YEAR_LO} AND {YEAR_HI} GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    setc = ", ".join(f"{c}=t.{c}" for c in V26_STATS)
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    nm = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
          "AND m.year=st.year AND m.week=st.week)")
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tgt t
        ON st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
        WHERE st.position<>'DEF' AND {nm}""").fetchone()[0]
    con.execute(f"""UPDATE st SET {setc}, {prov} FROM tgt t
        WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
          AND st.position<>'DEF' AND {nm}""")
    stat_sel = ", ".join(f"t.{c} AS {c}" for c in V26_STATS)
    con.execute(f"""INSERT INTO st BY NAME
        SELECT t.NFL_player_id, t.player, CAST(t.year AS DOUBLE) AS "year", CAST(t.week AS DOUBLE) AS "week",
          t.season_type, t.position, t.nfl_team, t.nfl_franchise_number,
          t.opponent_nfl_franchise_number, {stat_sel},
          t.NFL_player_id||'_'||CAST(t.year AS VARCHAR)||'_'||CAST(t.week AS VARCHAR) AS player_week,
          'pfr_player_defense_boxscore_backfill' AS data_source, '{PROV}' AS {PROV_COL}
        FROM tgt t WHERE NOT EXISTS (SELECT 1 FROM st s2
          WHERE s2.NFL_player_id=t.NFL_player_id AND s2.year=t.year AND s2.week=t.week)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_deftmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(tmp_spill, ignore_errors=True)

    # GATE: Lane 1952 == 14 ; team-game IDP INT reconciles ; golden holds
    lane = pq.read_table(str(tmp), columns=["player", "year", "season_type", "def_interceptions"]).to_pandas()
    lane_v = lane[(lane.player.fillna("").str.contains("Night Train Lane")) & (lane.year == 1952)
                  & (lane.season_type == "REG")].def_interceptions.sum()
    m = _idp_franchise_week(str(tmp), tgt)
    pct = round(100 * m.def_interceptions_v.sum() / m.def_interceptions_src.sum(), 1)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (abs(lane_v - 14) < 0.5) and (99.0 <= pct <= 101.0) and (g["failed"] == 0)
    res = {"updated": int(updated), "inserted": int(after - before), "before": int(before),
           "after": int(after), "lane_1952_int": float(lane_v), "team_game_pct": pct,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predef_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_backfill()
        print(f"updated={r['updated']:,} inserted={r['inserted']:,} rows {r['before']:,}->{r['after']:,}")
        print(f"Lane 1952 INT={r['lane_1952_int']:.0f} (want 14) | team-game IDP INT {r['team_game_pct']}% | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        t = build_target()
        print(f"target IDP-INT player-weeks: {len(t):,} | total def_int: {t.def_interceptions.sum():.0f}")
        print("Lane 1952 in target:", t[(t.player.str.contains('Lane',na=False)) & (t.year==1952)].def_interceptions.sum())
