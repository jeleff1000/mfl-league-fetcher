"""
sota_recon/build_appearance_backfill_v26.py  --  wave50: era-tiered appearance backfill.

Pre-flight #7 (Joe, 2026-07-10): the weekly table is APPEARANCE-INCLUSIVE -- players with
confirmed participation get rows even with no stat atoms (NULL atoms, never fake zeros;
pts_* = 0 is correct semantics for played-but-scoreless). Measured gaps (2026-07-11):

  snap tier    2012-2025 : 348,434 snap lines ->     38 missing rows (v26 already carries
                           modern rosters; this tier is a mop-up)
  starter tier 1957-2011 : 515,815 starter lines -> 20,667 missing rows (4.0%) -- THE gap

Inserted rows carry: identity/keys/teams/date, game-accurate position from the starters
or snap table, NULL stat atoms, and a witness citation in recon_correction_log. Never
fabricated from season counts (era-tier law).

SAFETY RAILS (wave49's):
  * INSERT-ONLY; existing rows never touched; player_week must be new and unique.
  * Doubleheader team-weeks excluded; pre-1978 POST excluded (week-vocabulary drift).
  * Unmapped pids -> identity queue CSV, never guessed.
  * Every inserted row emits a row_add FACT with the boxscore + table citation.
  * Gates: row count == before + inserted; zero new duplicate player_weeks.

    python -m scripts.sota_recon.build_appearance_backfill_v26            # DRY RUN
    python -m scripts.sota_recon.build_appearance_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from .recon_common import utc_stamp
from .sources import PLAYER_BIO, TEAM_GAMES, latest_v26, registry

WAVE = "wave50.appearance_backfill"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"
PREVIEW = os.path.join(SWEEPS, "wave50_appearance_preview.csv")
IDQ = os.path.join(SWEEPS, "wave50_identity_queue.csv")

# (source_key, witness label, activity predicate on the raw table, year bounds)
TIERS = [
    ("pfr_box_home_starters", "starter", "TRUE", 1957, 2011),
    ("pfr_box_vis_starters", "starter", "TRUE", 1957, 2011),
    ("pfr_box_home_snaps", "snaps",
     "COALESCE(TRY_CAST(offense AS INT),0)+COALESCE(TRY_CAST(defense AS INT),0)"
     "+COALESCE(TRY_CAST(special_teams AS INT),0) > 0", 2012, 2025),
    ("pfr_box_vis_snaps", "snaps",
     "COALESCE(TRY_CAST(offense AS INT),0)+COALESCE(TRY_CAST(defense AS INT),0)"
     "+COALESCE(TRY_CAST(special_teams AS INT),0) > 0", 2012, 2025),
]


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build(con, vq: str) -> None:
    tg, bio = _q(TEAM_GAMES), _q(PLAYER_BIO)
    arms = []
    for key, label, activity, y0, y1 in TIERS:
        side = "home" if "home" in key else "vis"
        arms.append(f"""
        SELECT regexp_extract(s.player_link_ids, '^([^,]+)', 1) AS pfr_id,
               s.boxscore_id, s.pos AS box_pos, '{label}' AS witness_tier,
               '{side}' AS side, {y0} AS y0, {y1} AS y1
        FROM '{_q(registry()[key].path)}' s
        WHERE s.player_link_ids IS NOT NULL AND ({activity})""")
    union = " UNION ALL ".join(arms)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE app AS
    WITH dh AS (SELECT team_fid, year, CAST(week AS INT) AS week, season_type
                FROM '{tg}' GROUP BY 1,2,3,4 HAVING COUNT(*) > 1),
    g AS (
      SELECT boxscore_id, year, CAST(week AS INT) AS week, season_type, game_date,
             team_fid, opponent_fid, team_code, opponent_code,
             COALESCE(is_home, FALSE) AS is_home
      FROM '{tg}'),
    lines AS (
      SELECT a.pfr_id, a.boxscore_id, a.box_pos, a.witness_tier,
             g.year, g.week, g.season_type, g.game_date,
             g.team_fid, g.opponent_fid, g.team_code, g.opponent_code
      FROM ({union}) a
      JOIN g ON g.boxscore_id = a.boxscore_id AND g.is_home = (a.side = 'home')
      WHERE g.year BETWEEN a.y0 AND a.y1
        AND NOT (g.season_type = 'POST' AND g.year < 1978)
        AND NOT EXISTS (SELECT 1 FROM dh WHERE dh.team_fid = g.team_fid
                        AND dh.year = g.year AND dh.week = CAST(g.week AS INT)
                        AND dh.season_type = g.season_type))
    SELECT DISTINCT * FROM lines""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE idq AS
    SELECT pfr_id, COUNT(*) AS lines, MIN(year) AS y0, MAX(year) AS y1
    FROM app WHERE pfr_id NOT IN (SELECT pfr_id FROM '{bio}' WHERE pfr_id IS NOT NULL)
    GROUP BY 1""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE new_rows AS
    WITH abbrev AS (
      SELECT nfl_franchise_number AS fid, year, MODE(nfl_team) AS abbr
      FROM '{vq}' WHERE nfl_team IS NOT NULL GROUP BY 1, 2)
    SELECT bio.NFL_player_id,
           bio.NFL_player_id || '_' || CAST(a.year AS INT) || '_' || a.week AS player_week,
           a.year, a.week, a.season_type,
           COALESCE(a1.abbr, a.team_code) AS nfl_team,
           COALESCE(a2.abbr, a.opponent_code) AS opponent_nfl_team,
           a.team_fid AS nfl_franchise_number,
           a.opponent_fid AS opponent_nfl_franchise_number,
           TRY_CAST(a.game_date AS TIMESTAMP) AS game_date,
           COALESCE(a.box_pos, bio.nfl_position) AS nfl_position,
           COALESCE(a.box_pos, bio.nfl_position) AS position,
           bio.player AS player,
           '{WAVE}:' || a.witness_tier || ':' || a.boxscore_id AS recon_correction_log,
           a.boxscore_id, a.pfr_id, a.witness_tier
    FROM app a
    JOIN '{bio}' bio ON bio.pfr_id = a.pfr_id
    LEFT JOIN abbrev a1 ON a1.fid = a.team_fid AND a1.year = a.year
    LEFT JOIN abbrev a2 ON a2.fid = a.opponent_fid AND a2.year = a.year
    WHERE NOT EXISTS (
        SELECT 1 FROM '{vq}' v WHERE v.NFL_player_id = bio.NFL_player_id
          AND v.year = a.year AND CAST(v.week AS INT) = a.week
          AND v.season_type = a.season_type)
    QUALIFY COUNT(*) OVER (PARTITION BY player_week) = 1""")


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    vq = _q(latest_v26())
    _build(con, vq)
    n, games = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT boxscore_id) FROM new_rows").fetchone()
    by_tier = dict(con.execute(
        "SELECT witness_tier, COUNT(*) FROM new_rows GROUP BY 1").fetchall())
    idq_n = con.execute("SELECT COUNT(*) FROM idq").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM new_rows) TO '{Path(PREVIEW).as_posix()}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM idq) TO '{Path(IDQ).as_posix()}' (HEADER)")
    diag = dict(inserted_rows=n, games_touched=games, by_tier=by_tier,
                identity_queue_pids=idq_n, preview_csv=PREVIEW, identity_csv=IDQ)
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    v26_p = Path(latest_v26())
    stamp = utc_stamp()
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    tmp = v26_p.with_name(v26_p.stem + "_w50.parquet")
    con.execute(f"""
        COPY (
          SELECT * FROM '{vq}'
          UNION ALL BY NAME
          SELECT * EXCLUDE (boxscore_id, pfr_id, witness_tier) FROM new_rows
        ) TO '{Path(tmp).as_posix()}' (FORMAT PARQUET)""")
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    dup_pw = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT player_week FROM '{tq}'
          WHERE player_week IN (SELECT player_week FROM new_rows)
          GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    gate = (after == before + n) and dup_pw == 0
    res = {"mode": "APPLY", **diag, "rows_before": before, "rows_after": after,
           "dup_player_weeks_after": dup_pw, "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        rows = con.execute("""
            SELECT player_week, boxscore_id, NFL_player_id, year, week, season_type,
                   witness_tier FROM new_rows""").fetchall()
        for pw, bx, nid, yr, wk, st, tier in rows:
            facts.emit_fact("row_add", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw,
                            row_json=json.dumps({"player_week": pw, "NFL_player_id": nid,
                                                 "year": int(yr), "week": int(wk),
                                                 "season_type": st, "boxscore_id": bx,
                                                 "appearance_only": True}),
                            wave_id=WAVE,
                            reason=f"confirmed participation ({tier}); appearance-"
                                   "inclusive doctrine, NULL atoms",
                            witness=f"pfr {tier} table, boxscore {bx}",
                            source_snapshot_id=snap)
        fc.close()
        bk = v26_p.with_name(v26_p.stem + f"_prew50_{stamp}.parquet")
        shutil.copy2(v26_p, bk)
        os.replace(tmp, v26_p)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
