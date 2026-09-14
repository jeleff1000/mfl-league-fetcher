"""
sota_recon/build_ancient_scoring_backfill_v26.py  --  wave51: event-witnessed scorers.

The scoring-event bijection (recon_scoring_events) found event-witnessed scorers with NO
v26 row -- concentrated 1920s + AAFC 1946-48 + AFL 1960-68, where the scoring log is the
ONLY player-grain witness we hold. Pre-1957 the event counts ARE the atoms: a man
credited with "2 rushing TD, 1 XP" by the per-play log gets exactly those cells and
nothing else. This wave inserts those rows with their event-derived scoring atoms and a
per-event citation.

Recomputed live (never from a stale CSV -- waves 49/50 may have absorbed some scorers).

SAFETY RAILS (wave49/50's):
  * INSERT-ONLY; bio-linked pids only (others -> identity queue CSV).
  * Doubleheader team-weeks excluded; pre-1978 POST excluded (week-vocabulary drift) ->
    named queue. NOTE: ancient championship games are POST -- they stay queued until the
    week-vocab mapping design lands; never inserted with the wrong week number.
  * player_week must be new and unique.
  * Only scoring atoms the events witness are set; everything else stays NULL.
  * Every inserted row emits a row_add FACT citing the boxscore.
  * Gates: row count == before + inserted; zero new duplicate player_weeks.

    python -m scripts.sota_recon.build_ancient_scoring_backfill_v26            # DRY RUN
    python -m scripts.sota_recon.build_ancient_scoring_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from .recon_common import utc_stamp
from .recon_scoring_events import build_event_aggregates
from .sources import PLAYER_BIO, TEAM_GAMES, latest_v26

WAVE = "wave51.ancient_scoring_backfill"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"
PREVIEW = os.path.join(SWEEPS, "wave51_preview.csv")
POST_QUEUE = os.path.join(SWEEPS, "wave51_pre1978_post_queue.csv")
IDQ = os.path.join(SWEEPS, "wave51_identity_queue.csv")

# event stat -> v26 column (scoring atoms only; fg_long/fg_yards ride along when present)
EV_TO_V26 = {
    "pass_td": "passing_tds", "rec_td": "receiving_tds", "rush_td": "rushing_tds",
    "fg_made": "fg_made", "pat_made": "pat_made", "pat_att": "pat_att",
    "int_ret_td": "def_int_ret_td", "punt_ret_td": "punt_return_tds",
    "kick_ret_td": "kickoff_return_tds",
    "pass_2pt": "passing_2pt_conversions", "rush_2pt": "rushing_2pt_conversions",
    "rec_2pt": "receiving_2pt_conversions",
    "fum_td": "fum_ret_td", "blocked_kick_td": "special_teams_tds",
    "fg_long": "fg_long",
    "fg_made_0_19": "fg_made_0_19", "fg_made_20_29": "fg_made_20_29",
    "fg_made_30_39": "fg_made_30_39", "fg_made_40_49": "fg_made_40_49",
    "fg_made_50_59": "fg_made_50_59",
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build(con, vq: str) -> None:
    from . import sources as S
    tg, bio = _q(TEAM_GAMES), _q(PLAYER_BIO)
    scoring = Path(S.registry()["pfr_box_scoring"].path).as_posix()
    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    mapping = {s: c for s, c in EV_TO_V26.items() if c in have}
    build_event_aggregates(con)  # -> TEMP TABLE ev(pid, boxscore_id, stat, val)
    piv = ", ".join(f"MAX(CASE WHEN stat = '{s}' THEN val END) AS {c}"
                    for s, c in mapping.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE evp AS
    SELECT pid, boxscore_id, {piv} FROM ev GROUP BY 1, 2""")
    # SIDE ATTRIBUTION: the event's team column is a nickname; the RUNNING SCORE is
    # exact by construction -- the side whose score increased is the scorer's side
    # (defensive TDs credit the defense, which is the scorer's own team).
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ev_side AS
    WITH s AS (
      SELECT boxscore_id, row_index_in_table,
             regexp_extract(description_link_ids, '^([^;]+)', 1) AS pid,
             TRY_CAST(vis_team_score AS INT) AS v, TRY_CAST(home_team_score AS INT) AS h
      FROM '{scoring}' WHERE description_link_ids IS NOT NULL),
    d AS (
      SELECT *, v - COALESCE(LAG(v) OVER w, 0) AS dv,
                h - COALESCE(LAG(h) OVER w, 0) AS dh
      FROM s WINDOW w AS (PARTITION BY boxscore_id ORDER BY row_index_in_table))
    SELECT pid, boxscore_id, MODE(dh > dv) AS is_home
    FROM d WHERE pid IS NOT NULL AND (dh > 0 OR dv > 0)
    GROUP BY 1, 2""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cand AS
    WITH dh AS (SELECT team_fid, year, CAST(week AS INT) AS week, season_type
                FROM '{tg}' GROUP BY 1,2,3,4 HAVING COUNT(*) > 1),
    g AS (
      SELECT g0.boxscore_id, g0.year, CAST(g0.week AS INT) AS week,
             g0.season_type, g0.game_date, g0.team_fid, g0.opponent_fid,
             g0.team_code, g0.opponent_code,
             COALESCE(g0.is_home, FALSE) AS is_home
      FROM '{tg}' g0
      LEFT JOIN dh ON dh.team_fid = g0.team_fid AND dh.year = g0.year
        AND dh.week = CAST(g0.week AS INT) AND dh.season_type = g0.season_type
      WHERE dh.team_fid IS NULL),
    abbrev AS (
      SELECT nfl_franchise_number AS fid, year, MODE(nfl_team) AS abbr
      FROM '{vq}' WHERE nfl_team IS NOT NULL GROUP BY 1, 2)
    SELECT bio.NFL_player_id,
           bio.NFL_player_id || '_' || CAST(g.year AS INT) || '_' || g.week AS player_week,
           g.year, g.week, g.season_type,
           COALESCE(a1.abbr, g.team_code) AS nfl_team,
           COALESCE(a2.abbr, g.opponent_code) AS opponent_nfl_team,
           g.team_fid AS nfl_franchise_number,
           g.opponent_fid AS opponent_nfl_franchise_number,
           TRY_CAST(g.game_date AS TIMESTAMP) AS game_date,
           bio.nfl_position, bio.nfl_position AS position, bio.player AS player,
           '{WAVE}:' || e.boxscore_id AS recon_correction_log,
           e.boxscore_id, e.pid AS pfr_id,
           (g.season_type = 'POST' AND g.year < 1978) AS pre1978_post,
           {", ".join(f"e.{c}" for c in mapping.values())}
    FROM evp e
    JOIN ev_side es ON es.pid = e.pid AND es.boxscore_id = e.boxscore_id
    JOIN g ON g.boxscore_id = e.boxscore_id AND g.is_home = es.is_home
    JOIN '{bio}' bio ON bio.pfr_id = e.pid
    LEFT JOIN abbrev a1 ON a1.fid = g.team_fid AND a1.year = g.year
    LEFT JOIN abbrev a2 ON a2.fid = g.opponent_fid AND a2.year = g.year
    WHERE NOT EXISTS (
        SELECT 1 FROM '{vq}' v WHERE v.NFL_player_id = bio.NFL_player_id
          AND v.year = g.year AND CAST(v.week AS INT) = g.week
          AND v.season_type = g.season_type)""")
    con.execute("CREATE OR REPLACE TEMP TABLE post_queue AS "
                "SELECT * FROM cand WHERE pre1978_post")
    # a row must CARRY its witnessed atom: scorers whose only credits live in stats
    # the release has no column for are queued, never inserted empty
    atom_any = " OR ".join(f"{c} IS NOT NULL" for c in mapping.values())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE new_rows AS
    SELECT * EXCLUDE (pre1978_post) FROM cand
    WHERE NOT pre1978_post AND ({atom_any})
    QUALIFY COUNT(*) OVER (PARTITION BY player_week) = 1""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE idq AS
    SELECT e.pid, COUNT(*) AS games FROM evp e
    WHERE e.pid NOT IN (SELECT pfr_id FROM '{bio}' WHERE pfr_id IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM '{bio}' b2 WHERE b2.pfr_id = e.pid)
    GROUP BY 1""")


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    vq = _q(latest_v26())
    _build(con, vq)
    n, games = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT boxscore_id) FROM new_rows").fetchone()
    post_q = con.execute("SELECT COUNT(*) FROM post_queue").fetchone()[0]
    idq_n = con.execute("SELECT COUNT(*) FROM idq").fetchone()[0]
    by_era = con.execute("""
        SELECT year//10*10 AS decade, COUNT(*) FROM new_rows
        GROUP BY 1 ORDER BY 1""").fetchall()
    con.execute(f"COPY (SELECT * FROM new_rows) TO '{Path(PREVIEW).as_posix()}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM post_queue) TO '{Path(POST_QUEUE).as_posix()}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM idq) TO '{Path(IDQ).as_posix()}' (HEADER)")
    diag = dict(inserted_rows=n, games_touched=games, pre1978_post_queued=post_q,
                identity_queue_pids=idq_n,
                by_decade=[(int(d), c) for d, c in by_era])
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag, "preview_csv": PREVIEW}

    v26_p = Path(latest_v26())
    stamp = utc_stamp()
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    tmp = v26_p.with_name(v26_p.stem + "_w51.parquet")
    con.execute(f"""
        COPY (
          SELECT * FROM '{vq}'
          UNION ALL BY NAME
          SELECT * EXCLUDE (boxscore_id, pfr_id) FROM new_rows
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
            SELECT player_week, boxscore_id, NFL_player_id, year, week, season_type
            FROM new_rows""").fetchall()
        for pw, bx, nid, yr, wk, st in rows:
            facts.emit_fact("row_add", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw,
                            row_json=json.dumps({"player_week": pw, "NFL_player_id": nid,
                                                 "year": int(yr), "week": int(wk),
                                                 "season_type": st,
                                                 "boxscore_id": bx}),
                            wave_id=WAVE,
                            reason="event-witnessed scorer with no v26 row; atoms are "
                                   "the per-play scoring log's own credits",
                            witness=f"pfr scoring log, boxscore {bx}",
                            source_snapshot_id=snap)
        fc.close()
        bk = v26_p.with_name(v26_p.stem + f"_prew51_{stamp}.parquet")
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
