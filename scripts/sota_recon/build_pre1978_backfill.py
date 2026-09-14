"""
sota_recon/build_pre1978_backfill.py  --  close the pre-1978 offensive completeness gap

v26's pre-1978 offense was built from PFA player-gamelogs (partial rosters). The full PFR
`player_offense` box scores (1932+) were scraped but never integrated, so v26 was missing
~6% of pre-1978 player-game OFFENSE — both absent players AND undercounted existing rows
(see finding_pre1978_completeness_gap). This ingestion reconciles v26's offensive counting
stats to the authoritative PFR box score at the player-week grain:

  UPDATE  existing single-row player-weeks -> set offense cols = source totals
  INSERT  player-weeks present in the box score but absent from v26

Authoritative source validated vs independent PUBLISHED records (Brown '63 1863, Baugh '47
2938, Graham '47 REG 2753, Simpson '73 2003) and includes postseason (season_type from
nfl_team_games_all). Scope 1932-1977 only (player_offense has no rows before 1932; 1978+ is
already complete at 0.0% diff). Derived columns (pts_*/rank_*/lamar) intentionally left
untouched -- a separate recompute owns those.

SAFETY: builds the new table in DuckDB, writes a TEMP parquet, runs the team-game
reconciliation GATE against the temp, and only backs up + swaps the live v26 if the gate
passes. A failing build never reaches the canonical file.

    python -m scripts.sota_recon.build_pre1978_backfill            # dry-run sizing + projection
    python -m scripts.sota_recon.build_pre1978_backfill --apply    # gated write
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

SRC = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/player_offense/_combined.parquet"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"

PROV = "wave8.pre1978_offense_reconcile"
PROV_COL = "recon_correction_log"
YEAR_LO, YEAR_HI = 1932, 1977

# PFR box-score column -> v26 column. targets excluded (not recorded pre-1978);
# fumbles_lost excluded (box score carries only total fumbles).
CMAP = {
    "pass_cmp": "completions", "pass_att": "attempts", "pass_yds": "passing_yards",
    "pass_td": "passing_tds", "pass_int": "passing_interceptions",
    "rush_att": "carries", "rush_yds": "rushing_yards", "rush_td": "rushing_tds",
    "rec": "receptions", "rec_yds": "receiving_yards", "rec_td": "receiving_tds",
    "fumbles": "fumbles",
}
SRC_STATS = list(CMAP)
V26_STATS = list(CMAP.values())


def build_target(year_lo: int = YEAR_LO, year_hi: int = YEAR_HI) -> pd.DataFrame:
    """Authoritative player-week offense from the PFR box score, mapped to v26 identity/keys."""
    tg = pq.read_table(TG, columns=["boxscore_id", "year", "week", "team_code",
                                    "team_fid", "opponent_fid", "season_type"]).to_pandas()
    tg = tg[(tg.year >= year_lo) & (tg.year <= year_hi)]

    bio = pq.read_table(BIO, columns=["NFL_player_id", "pfr_id"]).to_pandas()
    bm = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "NFL_player_id"]))

    s = pq.read_table(SRC, columns=["season", "boxscore_id", "team", "player",
                                    "player_link_ids"] + SRC_STATS).to_pandas()
    s = s[(s.season >= year_lo) & (s.season <= year_hi)].copy()
    for c in SRC_STATS:
        s[c] = pd.to_numeric(s[c], errors="coerce").fillna(0)
    s["pfr_id"] = s.player_link_ids.str.split(";").str[0]
    s["NFL_player_id"] = s.pfr_id.map(lambda p: bm.get(p, p))
    s = s.merge(tg, left_on=["boxscore_id", "team"],
                right_on=["boxscore_id", "team_code"], how="left")
    s = s[s.team_fid.notna()].copy()
    s["year"] = s.year.astype(int)
    s["week"] = s.week.astype(int)
    s = s.rename(columns=CMAP)

    agg = (s.groupby(["NFL_player_id", "year", "week"], as_index=False)
             .agg(**{c: (c, "sum") for c in V26_STATS},
                  season_type=("season_type", "first"),
                  nfl_franchise_number=("team_fid", "first"),
                  nfl_team=("team_code", "first"),
                  opponent_nfl_franchise_number=("opponent_fid", "first"),
                  player=("player", "first")))

    # position for inserted rows (the box score carries none): bio first, then infer from
    # the player's dominant stat so position-filtered analysis still sees these players.
    pos = pq.read_table(BIO, columns=["NFL_player_id", "nfl_position"]).to_pandas()
    pos = pos.dropna(subset=["nfl_position"]).drop_duplicates("NFL_player_id")
    agg = agg.merge(pos.rename(columns={"nfl_position": "position"}),
                    on="NFL_player_id", how="left")
    need = agg.position.isna()
    infer = np.where(agg.passing_yards >= agg[["rushing_yards", "receiving_yards"]].max(axis=1),
                     "QB", np.where(agg.rushing_yards >= agg.receiving_yards, "RB", "WR"))
    agg.loc[need, "position"] = infer[need.to_numpy()]
    return agg


def _v26_franchise_week(year_lo=YEAR_LO, year_hi=YEAR_HI, path=None) -> pd.DataFrame:
    path = path or latest_v26()
    v = pq.read_table(path, columns=["year", "week", "nfl_franchise_number",
                                     "rushing_yards", "passing_yards",
                                     "receiving_yards"]).to_pandas()
    v = v[(v.year >= year_lo) & (v.year <= year_hi)]
    return (v.groupby(["nfl_franchise_number", "year", "week"], as_index=False)
              [["rushing_yards", "passing_yards", "receiving_yards"]].sum())


def _source_franchise_week(tgt: pd.DataFrame) -> pd.DataFrame:
    return (tgt.groupby(["nfl_franchise_number", "year", "week"], as_index=False)
               [["rushing_yards", "passing_yards", "receiving_yards"]].sum())


def gate(tgt: pd.DataFrame, v26_path: str) -> dict:
    """Team-game (franchise,year,week) reconciliation of a v26 file vs the source."""
    sf = _source_franchise_week(tgt)
    vf = _v26_franchise_week(path=v26_path)
    m = sf.merge(vf, on=["nfl_franchise_number", "year", "week"],
                 how="left", suffixes=("_src", "")).fillna(0)
    out = {}
    for src_col, v_col in [("rushing_yards_src", "rushing_yards"),
                           ("passing_yards_src", "passing_yards"),
                           ("receiving_yards_src", "receiving_yards")]:
        tot_s, tot_v = m[src_col].sum(), m[v_col].sum()
        out[v_col] = {
            "source_total": float(tot_s), "v26_total": float(tot_v),
            "pct": round(100 * tot_v / tot_s, 3) if tot_s else 0.0,
            "team_games_off_by_gt1": int((m[src_col] - m[v_col]).abs().gt(1).sum()),
        }
    out["team_games"] = int(len(m))
    return out


def apply_backfill() -> dict:
    v26 = latest_v26()
    tgt = build_target()
    stamp = utc_stamp()

    tmp_spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(tmp_spill, exist_ok=True)
    # disk-backed database so the 1.18M x 740-col table lives on disk, not RAM (avoids OOM)
    db_file = os.path.join(tmp_spill, "work.duckdb")
    con = duckdb.connect(db_file)
    con.execute("PRAGMA threads=1")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{tmp_spill}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.register("tgt_raw", tgt)
    NN = "lower(regexp_replace(player, '[^A-Za-z]', '', 'g'))"  # normalized name
    con.execute(f"CREATE TEMP TABLE t AS SELECT *, {NN} AS nn FROM tgt_raw")

    # exclude player-weeks that already have >1 v26 row (doubleheader splits): a single
    # player-week total must not be written onto both rows.
    con.execute(f"""
        CREATE TEMP TABLE multi AS
        SELECT NFL_player_id, year, week FROM st
        WHERE year BETWEEN {YEAR_LO} AND {YEAR_HI}
        GROUP BY 1,2,3 HAVING COUNT(*) > 1
    """)
    multi_n = con.execute("SELECT COUNT(*) FROM multi").fetchone()[0]

    set_clause = ", ".join(f"{c} = t.{c}" for c in V26_STATS)
    prov_set = (f"{PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' "
                f"THEN '{PROV}' ELSE {PROV_COL} || ',{PROV}' END")

    # PASS 1 -- match by NFL_player_id (single-row player-weeks only)
    updated_id = con.execute(f"""
        SELECT COUNT(*) FROM st s JOIN t ON s.NFL_player_id=t.NFL_player_id
          AND s.year=t.year AND s.week=t.week
        WHERE NOT EXISTS (SELECT 1 FROM multi m
          WHERE m.NFL_player_id=s.NFL_player_id AND m.year=s.year AND m.week=s.week)
    """).fetchone()[0]
    con.execute(f"""
        UPDATE st SET {set_clause}, {prov_set}
        FROM t
        WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year AND st.week=t.week
          AND NOT EXISTS (SELECT 1 FROM multi m
            WHERE m.NFL_player_id=st.NFL_player_id AND m.year=st.year AND m.week=st.week)
    """)

    # target rows still unmatched by id -> candidates for name-fallback / insert
    con.execute("""
        CREATE TEMP TABLE unmatched AS
        SELECT * FROM t
        WHERE NOT EXISTS (SELECT 1 FROM st s
          WHERE s.NFL_player_id=t.NFL_player_id AND s.year=t.year AND s.week=t.week)
    """)
    # PASS 2 -- same player under a DIFFERENT id: match by (franchise, year, week, name)
    # where exactly one v26 row and one target row share that name in that game (avoids
    # ambiguity), so an id mismatch updates the existing row instead of duplicating it.
    con.execute(f"""
        CREATE TEMP TABLE name_match AS
        SELECT u.nfl_franchise_number AS fr, u.year AS yr, u.week AS wk, u.nn AS nn,
               {', '.join(f'u.{c} AS {c}' for c in V26_STATS)}
        FROM unmatched u
        WHERE (SELECT COUNT(*) FROM st s WHERE s.nfl_franchise_number=u.nfl_franchise_number
                 AND s.year=u.year AND s.week=u.week AND {NN.replace('player','s.player')}=u.nn) = 1
          AND (SELECT COUNT(*) FROM unmatched u2 WHERE u2.nfl_franchise_number=u.nfl_franchise_number
                 AND u2.year=u.year AND u2.week=u.week AND u2.nn=u.nn) = 1
    """)
    updated_name = con.execute("SELECT COUNT(*) FROM name_match").fetchone()[0]
    con.execute(f"""
        UPDATE st SET {', '.join(f'{c} = nm.{c}' for c in V26_STATS)}, {prov_set}
        FROM name_match nm
        WHERE st.nfl_franchise_number=nm.fr AND st.year=nm.yr AND st.week=nm.wk
          AND {NN.replace('player','st.player')}=nm.nn
    """)

    # INSERT -- target rows matched by neither id nor a unique name in the game
    stat_sel = ", ".join(f"u.{c} AS {c}" for c in V26_STATS)
    con.execute(f"""
        INSERT INTO st BY NAME
        SELECT u.NFL_player_id, u.player, CAST(u.year AS DOUBLE) AS year,
               CAST(u.week AS DOUBLE) AS week, u.season_type, u.position, u.nfl_team,
               u.nfl_franchise_number, u.opponent_nfl_franchise_number, {stat_sel},
               u.NFL_player_id || '_' || CAST(u.year AS VARCHAR) || '_' || CAST(u.week AS VARCHAR) AS player_week,
               'pfr_player_offense_boxscore_backfill' AS data_source,
               '{PROV}' AS {PROV_COL}
        FROM unmatched u
        WHERE NOT EXISTS (SELECT 1 FROM name_match nm
          WHERE nm.fr=u.nfl_franchise_number AND nm.yr=u.year AND nm.wk=u.week AND nm.nn=u.nn)
    """)
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    inserted = after - before
    updated = int(updated_id) + int(updated_name)

    con.execute(f"""
        UPDATE st SET {PROV_COL} = array_to_string(list_distinct(string_split({PROV_COL}, ',')), ',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'
    """)

    # write TEMP, gate against temp, swap only on pass
    vpath = Path(v26)
    tmp = vpath.with_name(vpath.stem + "_pre1978tmp.parquet")
    reader = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for batch in reader:
        writer.write_batch(batch)
    writer.close()
    con.close()
    shutil.rmtree(tmp_spill, ignore_errors=True)

    g = gate(tgt, str(tmp))
    pcts = [g[c]["pct"] for c in ("rushing_yards", "passing_yards", "receiving_yards")]
    # two-sided: undercount (missing) AND overcount (duplicate inserts) both fail
    gate_pass = all(99.5 <= p <= 100.5 for p in pcts)

    result = {"updated": int(updated), "updated_id": int(updated_id),
              "updated_name": int(updated_name), "inserted": int(inserted),
              "multi_row_excluded": int(multi_n), "before": int(before),
              "after": int(after), "gate": g, "gate_pass": bool(gate_pass),
              "temp": str(tmp)}

    if gate_pass:
        backup = vpath.with_name(vpath.stem + f"_prebackfill_backup_{stamp}.parquet")
        shutil.copy2(vpath, backup)
        os.replace(tmp, vpath)
        result["backup"] = str(backup)
        result["swapped"] = True
    else:
        result["swapped"] = False
    return result


def dry_run() -> dict:
    tgt = build_target()
    v26 = latest_v26()
    vkeys = set(map(tuple, pq.read_table(v26, columns=["NFL_player_id", "year", "week"])
                    .to_pandas().query(f"{YEAR_LO} <= year <= {YEAR_HI}")
                    [["NFL_player_id", "year", "week"]].astype({"year": int, "week": int})
                    .itertuples(index=False, name=None)))
    tgt["key"] = list(zip(tgt.NFL_player_id.astype(str), tgt.year, tgt.week))
    in_v26 = tgt.key.map(lambda k: (str(k[0]), k[1], k[2]) in vkeys)
    return {"target_player_weeks": int(len(tgt)),
            "would_update": int(in_v26.sum()),
            "would_insert": int((~in_v26).sum()),
            "current_gate": gate(tgt, v26)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="gated write to canonical v26")
    args = ap.parse_args()

    if args.apply:
        r = apply_backfill()
        print(f"updated={r['updated']:,}  inserted={r['inserted']:,}  "
              f"multi_row_excluded={r['multi_row_excluded']:,}")
        print(f"rows {r['before']:,} -> {r['after']:,}")
        for c in ["rushing_yards", "passing_yards", "receiving_yards"]:
            d = r["gate"][c]
            print(f"  {c}: v26 {d['v26_total']:,.0f} / source {d['source_total']:,.0f} "
                  f"= {d['pct']}%  (off>1: {d['team_games_off_by_gt1']}/{r['gate']['team_games']})")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"]
                 else f"NOT swapped; temp kept at {r['temp']}"))
    else:
        r = dry_run()
        print(f"target player-weeks: {r['target_player_weeks']:,}")
        print(f"would UPDATE: {r['would_update']:,}  would INSERT: {r['would_insert']:,}")
        print("current franchise-week reconciliation (pre-write):")
        for c in ["rushing_yards", "passing_yards", "receiving_yards"]:
            d = r["current_gate"][c]
            print(f"  {c}: {d['pct']}%  (off>1: {d['team_games_off_by_gt1']}/{r['current_gate']['team_games']})")
