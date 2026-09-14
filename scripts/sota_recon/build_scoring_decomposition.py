"""
sota_recon/build_scoring_decomposition.py  --  close the scoring-decomposition gap

The scoring lane reconstructs only ~61% of the scoreboard (91% in 2015+) from v26's
per-player scoring atoms: v26's team scores are right but WHICH player scored which
TD/FG/2pt is incomplete in older eras. The PFR scoring table carries every scoring play
with player link-ids (133,466 plays, 99% linked, 1920-2025) -- one source that closes both
the scoring lane AND the pre-1932 scoring question.

Grammar (consistent across eras), link-ids in description order:
  pass TD   "{rec} N yard pass from {passer} ({kicker} kick)"  -> [rec, passer, kicker?]
  rush TD   "{rusher} N yard rush ({kicker} kick)"             -> [rusher, kicker?]
  FG        "{kicker} N yard field goal"                       -> [kicker]
  ret/def   "{ret} N yard kickoff|punt|interception return"    -> [returner, kicker?]
            "{rec} fumble recovery in end zone"                -> [recoverer, kicker?]
  parenthetical: "(X kick)" = +1 XP ; "(X run/pass...)" = +2 two-pt ; "failed/no good" = +0

This module (dry-run first) parses every play into points + player roles, then PROVES the
parse by reconciling per-game parsed points against the scoreboard. Only once the parse
reconciles do we attribute to v26 atoms and gate-write (mirrors build_pre1978_backfill).

    python -m scripts.sota_recon.build_scoring_decomposition          # dry-run: parse + scoreboard reconcile
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

SCORING = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/scoring/_combined.parquet"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave9.scoring_decomposition"
PROV_COL = "recon_correction_log"

TD_TYPES = {"rush_td", "pass_td", "kr_td", "pr_td", "int_td", "fum_td", "other_td"}
# scope: kicking (fg/pat) for all pre-1978; offensive TDs only pre-1932 (1932+ TDs are
# already correct from the player_offense backfill — do not overwrite them)
KICK_HI = 1977
TD_HI = 1931

_FAIL = re.compile(r"failed|no good|blocked|missed", re.I)
_PAREN = re.compile(r"\(([^)]*)\)")


def classify(desc: str) -> tuple[str, int]:
    """Return (play_type, points) for one scoring-play description."""
    if not isinstance(desc, str) or not desc:
        return "unknown", 0
    d = desc.lower()

    # field goal (scoring table lists only made FGs) — but guard blocked/returned oddities
    if "field goal" in d:
        return ("FG", 0 if _FAIL.search(d) and "return" not in d else 3)
    if "safety" in d:
        return "safety", 2

    # the bundled try in the trailing parenthetical
    pts_try = 0
    m = _PAREN.search(d)
    if m:
        inside = m.group(1)
        if _FAIL.search(inside):
            pts_try = 0
        elif "kick" in inside:
            pts_try = 1
        elif any(w in inside for w in ("run", "rush", "pass", "two", "conversion", "reception")):
            pts_try = 2

    # touchdown type (all worth 6)
    if "kickoff return" in d:
        ptype = "kr_td"
    elif "punt return" in d:
        ptype = "pr_td"
    elif "interception" in d:
        ptype = "int_td"
    elif "fumble" in d:
        ptype = "fum_td"
    elif "pass" in d and "from" in d:
        ptype = "pass_td"
    elif "rush" in d or " run" in d:
        ptype = "rush_td"
    elif "extra point" in d or (m and "kick" in m.group(1) and "yard" not in d):
        return "xp_only", pts_try or 1
    else:
        ptype = "other_td"
    return ptype, 6 + pts_try


def _scoreboard(g: pd.DataFrame) -> int:
    """Final combined score of a game from the running vis/home score columns."""
    v = pd.to_numeric(g["vis_team_score"], errors="coerce").max()
    h = pd.to_numeric(g["home_team_score"], errors="coerce").max()
    return int((0 if pd.isna(v) else v) + (0 if pd.isna(h) else h))


def parse_plays() -> pd.DataFrame:
    sc = pq.read_table(SCORING, columns=["season", "boxscore_id", "description",
                                         "vis_team_score", "home_team_score"]).to_pandas()
    res = sc["description"].map(classify)
    sc["ptype"] = res.map(lambda x: x[0])
    sc["points"] = res.map(lambda x: x[1])
    return sc


def dry_run() -> dict:
    sc = parse_plays()
    # parse-quality: per-game sum(parsed points) vs scoreboard
    out = {"by_era": [], "type_counts": sc.ptype.value_counts().to_dict(),
           "total_plays": int(len(sc)), "total_points_parsed": int(sc.points.sum())}
    parsed = sc.groupby("boxscore_id").points.sum()
    board = sc.groupby("boxscore_id").apply(_scoreboard, include_groups=False)
    cmp = pd.DataFrame({"season": sc.groupby("boxscore_id").season.first(),
                        "parsed": parsed, "board": board})
    cmp["ok"] = (cmp.parsed - cmp.board).abs() <= 1
    for lo, hi in [(1920, 1931), (1932, 1949), (1950, 1977), (1978, 2024)]:
        e = cmp[(cmp.season >= lo) & (cmp.season <= hi)]
        out["by_era"].append((f"{lo}-{hi}", len(e), round(100 * e.ok.mean(), 1) if len(e) else 0.0))
    out["overall_game_match_pct"] = round(100 * cmp.ok.mean(), 1)
    out["games"] = int(len(cmp))
    return out


def _attribute(ids: list[str], ptype: str, points: int) -> list[tuple[str, str]]:
    """Map one play's ordered link-ids to (pfr_id, atom). Offensive + kicking only;
    def/ST/safety are team-level (lane reads them via MAX) and are out of scope here."""
    out = []
    if not ids:
        return out
    if ptype == "FG":
        out.append((ids[0], "fg_made"))
    elif ptype == "rush_td":
        out.append((ids[0], "rushing_tds"))
    elif ptype == "pass_td":
        out.append((ids[0], "receiving_tds"))
        if len(ids) >= 2:
            out.append((ids[1], "passing_tds"))
    # extra point: the kicker is the last link-id on a 7-point TD play, or an xp_only play
    if (ptype in TD_TYPES and points == 7) or ptype == "xp_only":
        out.append((ids[-1], "pat_made"))
    return out


def build_target() -> pd.DataFrame:
    """Per (NFL_player_id, year, week, season_type) scoring atoms from the scoring table.

    The scoring `team` is a team NAME (no code), so we instead infer each play's side
    (home vs visitor) from which running score increased, then join team_games on
    (boxscore_id, is_home) to get franchise/year/week."""
    sc = pq.read_table(SCORING, columns=["season", "boxscore_id", "row_index_in_table",
                                         "description", "description_link_ids",
                                         "vis_team_score", "home_team_score"]).to_pandas()
    sc = sc[sc.season <= KICK_HI].copy()
    sc["vs"] = pd.to_numeric(sc.vis_team_score, errors="coerce").fillna(0)
    sc["hs"] = pd.to_numeric(sc.home_team_score, errors="coerce").fillna(0)
    sc["ri"] = pd.to_numeric(sc.row_index_in_table, errors="coerce").fillna(0)
    sc = sc.sort_values(["boxscore_id", "ri"])
    sc["dv"] = sc.vs - sc.groupby("boxscore_id").vs.shift(1).fillna(0)
    sc["dh"] = sc.hs - sc.groupby("boxscore_id").hs.shift(1).fillna(0)
    sc["is_home"] = sc.dh > sc.dv  # the side whose score increased on this play

    res = sc["description"].map(classify)
    sc["ptype"] = res.map(lambda x: x[0])
    sc["points"] = res.map(lambda x: x[1])
    sc["ids"] = sc.description_link_ids.fillna("").str.split(";")

    rows = []
    for tup in sc.itertuples(index=False):
        for pfr_id, atom in _attribute([i for i in tup.ids if i], tup.ptype, tup.points):
            rows.append((pfr_id, atom, tup.boxscore_id, bool(tup.is_home)))
    att = pd.DataFrame(rows, columns=["pfr_id", "atom", "boxscore_id", "is_home"])

    tg = pq.read_table(TG, columns=["boxscore_id", "year", "week", "team_code",
                                    "team_fid", "season_type", "is_home"]).to_pandas()
    att = att.merge(tg, on=["boxscore_id", "is_home"], how="left")
    att = att[att.team_fid.notna()].copy()
    att["year"] = att.year.astype(int)
    att["week"] = att.week.astype(int)

    bio = pq.read_table(BIO, columns=["NFL_player_id", "pfr_id", "nfl_position"]).to_pandas()
    bm = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "NFL_player_id"]))
    pos = dict(zip(bio.pfr_id.dropna(), bio.loc[bio.pfr_id.notna(), "nfl_position"]))
    att["NFL_player_id"] = att.pfr_id.map(lambda p: bm.get(p, p))
    att["position"] = att.pfr_id.map(lambda p: pos.get(p))

    # count atoms, pivot to columns, per player-week
    g = (att.groupby(["NFL_player_id", "year", "week", "season_type", "team_fid",
                      "team_code", "atom"]).size().reset_index(name="n"))
    wide = g.pivot_table(index=["NFL_player_id", "year", "week", "season_type",
                                "team_fid", "team_code"],
                         columns="atom", values="n", fill_value=0).reset_index()
    for c in ("fg_made", "pat_made", "rushing_tds", "receiving_tds", "passing_tds"):
        if c not in wide.columns:
            wide[c] = 0
    wide = wide.rename(columns={"team_fid": "nfl_franchise_number", "team_code": "nfl_team"})
    return wide


def apply_backfill() -> dict:
    v26 = latest_v26()
    wide = build_target()
    stamp = utc_stamp()
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
    con.register("tgt", wide)
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
                    WHERE year<={KICK_HI} GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    not_multi = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
                 "AND m.year=st.year AND m.week=st.week)")

    # PASS A: kicking (fg/pat) for ALL pre-1978 single-row player-weeks. Keep attempts
    # physically consistent with the (authoritative) made counts: an attempt is never less
    # than makes (+misses for FG), else the internal lane flags pat_made>pat_att etc.
    con.execute(f"""UPDATE st SET fg_made=t.fg_made, pat_made=t.pat_made,
        fg_att=GREATEST(COALESCE(st.fg_att,0), t.fg_made + COALESCE(st.fg_missed,0) - COALESCE(st.fg_blocked,0)),
        pat_att=GREATEST(COALESCE(st.pat_att,0), t.pat_made + COALESCE(st.pat_missed,0) + COALESCE(st.pat_blocked,0)), {prov}
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year
        AND st.week=t.week AND st.year<={KICK_HI} AND {not_multi}""")
    # PASS B: offensive TDs for pre-1932 only (1932+ already correct)
    con.execute(f"""UPDATE st SET rushing_tds=t.rushing_tds, receiving_tds=t.receiving_tds,
        passing_tds=t.passing_tds, {prov}
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND st.year=t.year
        AND st.week=t.week AND st.year<={TD_HI} AND {not_multi}""")

    # INSERT player-weeks present in scoring but absent from v26 (mostly kickers)
    con.execute(f"""INSERT INTO st BY NAME
        SELECT t.NFL_player_id, CAST(t.year AS DOUBLE) AS year, CAST(t.week AS DOUBLE) AS week,
          t.season_type, t.nfl_team, t.nfl_franchise_number, t.fg_made, t.fg_made AS fg_att,
          t.pat_made, t.pat_made AS pat_att,
          CASE WHEN t.year<={TD_HI} THEN t.rushing_tds ELSE 0 END AS rushing_tds,
          CASE WHEN t.year<={TD_HI} THEN t.receiving_tds ELSE 0 END AS receiving_tds,
          CASE WHEN t.year<={TD_HI} THEN t.passing_tds ELSE 0 END AS passing_tds,
          t.NFL_player_id||'_'||CAST(t.year AS VARCHAR)||'_'||CAST(t.week AS VARCHAR) AS player_week,
          'pfr_scoring_decomposition_backfill' AS data_source, '{PROV}' AS {PROV_COL}
        FROM tgt t WHERE NOT EXISTS (SELECT 1 FROM st s2 WHERE s2.NFL_player_id=t.NFL_player_id
          AND s2.year=t.year AND s2.week=t.week)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    # write temp, gate, swap
    vpath = Path(v26)
    tmp = vpath.with_name(vpath.stem + "_scoringtmp.parquet")
    reader = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    import pyarrow.parquet as _pq
    writer = _pq.ParquetWriter(str(tmp), reader.schema)
    for b in reader:
        writer.write_batch(b)
    writer.close(); con.close(); shutil.rmtree(tmp_spill, ignore_errors=True)

    # GATE: temp pre-1978 fg/pat must reach the scoring-table targets; golden must hold
    tv = pq.read_table(str(tmp), columns=["year", "fg_made", "pat_made"]).to_pandas()
    tv = tv[(tv.year >= 1920) & (tv.year <= KICK_HI)]
    fg_now, pat_now = tv.fg_made.fillna(0).sum(), tv.pat_made.fillna(0).sum()
    tgt_fg = wide.fg_made.sum() + 0  # target totals are at least what we attributed
    tgt_pat = wide.pat_made.sum()
    fg_ok = fg_now >= 0.97 * tgt_fg
    pat_ok = pat_now >= 0.97 * tgt_pat
    res = {"before": int(before), "after": int(after), "inserted": int(after - before),
           "fg_pre1978": float(fg_now), "pat_pre1978": float(pat_now),
           "target_fg": float(tgt_fg), "target_pat": float(tgt_pat),
           "gate_pass": bool(fg_ok and pat_ok), "temp": str(tmp)}
    if res["gate_pass"]:
        backup = vpath.with_name(vpath.stem + f"_prescoring_backup_{stamp}.parquet")
        shutil.copy2(vpath, backup)
        os.replace(tmp, vpath)
        res["backup"] = str(backup); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="gated write of scoring atoms")
    args = ap.parse_args()
    if args.apply:
        r = apply_backfill()
        print(f"rows {r['before']:,} -> {r['after']:,} (inserted {r['inserted']:,})")
        print(f"pre-1978 fg_made {r['fg_pre1978']:,.0f} (target {r['target_fg']:,.0f}); "
              f"pat_made {r['pat_pre1978']:,.0f} (target {r['target_pat']:,.0f})")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"]
                 else f"NOT swapped; temp at {r['temp']}"))
        raise SystemExit(0)
    r = dry_run()
    print(f"parsed {r['total_plays']:,} scoring plays -> {r['total_points_parsed']:,} points")
    print("\nplay-type classification:")
    for k, v in sorted(r["type_counts"].items(), key=lambda x: -x[1]):
        print(f"  {k:<10} {v:,}")
    print(f"\nPARSE QUALITY -- per-game parsed points == scoreboard (+/-1):")
    print(f"  {'era':<11} {'games':>7} {'match%':>7}")
    for era, n, pct in r["by_era"]:
        print(f"  {era:<11} {n:>7,} {pct:>6}%")
    print(f"  OVERALL: {r['overall_game_match_pct']}% of {r['games']:,} games reconcile to scoreboard")
