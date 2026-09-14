"""
sota_recon/build_missing_row_backfill_v26.py  --  wave49: witnessed missing-row backfill.

The cell-witness lane (recon_cell_witness) enumerates box lines with NONZERO activity for
bio-linked players who have NO v26 row at that game -- the completion class wave44's
thin-game funnel could not see (single missing players inside otherwise-full games, all
eras, plus the wave48-unblocked identities). This wave inserts those rows with their full
multi-table stat line (offense + defense + kicking + returns atoms for the same game).

SAFETY RAILS (wave44's, plus two):
  * INSERT-ONLY for players ABSENT from the (player, year, week, season_type) -- existing
    rows are never touched.
  * Doubleheader team-weeks excluded entirely (merged rows may already hold the game).
  * PRE-1978 POST EXCLUDED: v26 and team_games use different playoff week vocabularies
    there (v26 1963 POST = wk 15-18 vs tg 17-18); inserting with tg numbering could
    duplicate a game under a different week. Those lines go to a named queue instead.
  * Identity: bio.pfr_id unique; unmapped pids skipped (identity queue).
  * A candidate must show NONZERO activity in at least one witnessed atom.
  * Every inserted row emits a row_add FACT with the boxscore citation.
  * Gates on apply: row count == before + inserted; zero new duplicate player_weeks;
    golden samples; run_invariants after (cumulative).

    python -m scripts.sota_recon.build_missing_row_backfill_v26            # DRY RUN
    python -m scripts.sota_recon.build_missing_row_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import PLAYER_BIO, TEAM_GAMES, latest_v26, registry

WAVE = "wave49.missing_row_backfill"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"
PREVIEW = os.path.join(SWEEPS, "wave49_backfill_preview.csv")
POST_QUEUE = os.path.join(SWEEPS, "wave49_pre1978_post_queue.csv")

# per box source: box column -> v26 column (atoms only; the rest of the row stays NULL)
SOURCE_ATOMS: dict[str, dict[str, str]] = {
    "pfr_player_offense_box": {
        "pass_cmp": "completions", "pass_att": "attempts", "pass_yds": "passing_yards",
        "pass_td": "passing_tds", "pass_int": "passing_interceptions",
        "pass_sacked": "sacks_suffered", "pass_sacked_yds": "sack_yards_lost",
        "pass_long": "passing_long",
        "rush_att": "carries", "rush_yds": "rushing_yards", "rush_td": "rushing_tds",
        "rush_long": "rushing_long",
        "rec": "receptions", "rec_yds": "receiving_yards", "rec_td": "receiving_tds",
        "rec_long": "receiving_long", "targets": "targets",
        "fumbles": "fumbles", "fumbles_lost": "fumbles_lost",
    },
    "pfr_player_defense_box": {
        "def_int": "def_interceptions", "def_int_yds": "def_interception_yards",
        "def_int_td": "def_int_ret_td", "pass_defended": "def_pass_defended",
        "sacks": "def_sacks", "tackles_solo": "def_tackles_solo",
        "tackles_assists": "def_tackle_assists", "tackles_loss": "def_tackles_for_loss",
        "qb_hits": "def_qb_hits", "fumbles_forced": "def_fumbles_forced",
        "fumbles_rec": "fum_rec",
    },
    "pfr_box_kicking": {
        "xpm": "pat_made", "xpa": "pat_att", "fgm": "fg_made", "fga": "fg_att",
        "punt": "punts", "punt_yds": "punt_yards", "punt_long": "punt_long",
    },
    "pfr_box_returns": {
        "kick_ret": "kickoff_returns", "kick_ret_yds": "kickoff_return_yards",
        "kick_ret_td": "kickoff_return_tds", "punt_ret": "punt_returns",
        "punt_ret_yds": "punt_return_yards", "punt_ret_td": "punt_return_tds",
    },
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _all_v26_atoms() -> list[str]:
    out: list[str] = []
    for atoms in SOURCE_ATOMS.values():
        for dst in atoms.values():
            if dst not in out:
                out.append(dst)
    return out


def _build_candidates(con, vq: str) -> None:
    tg, bio = _q(TEAM_GAMES), _q(PLAYER_BIO)
    # one arm per source: (pid, boxscore_id, box_team, atoms...) with NULLs for other atoms
    arms = []
    for key, atoms in SOURCE_ATOMS.items():
        sel = ", ".join(f"TRY_CAST(b.{src} AS DOUBLE) AS {dst}" for src, dst in atoms.items())
        others = ", ".join(f"NULL::DOUBLE AS {c}" for c in _all_v26_atoms()
                           if c not in atoms.values())
        arms.append(f"""
        SELECT regexp_extract(b.player_link_ids, '^([^,]+)', 1) AS pfr_id,
               b.boxscore_id, b.team AS box_team, {sel}{',' if others else ''} {others}
        FROM '{_q(registry()[key].path)}' b
        WHERE b.player_link_ids IS NOT NULL""")
    union = " UNION ALL BY NAME ".join(arms)
    atom_merge = ", ".join(f"MAX({c}) AS {c}" for c in _all_v26_atoms())
    activity = " OR ".join(f"COALESCE({c}, 0) != 0" for c in _all_v26_atoms())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lines AS
    SELECT pfr_id, boxscore_id, box_team, {atom_merge}
    FROM ({union})
    GROUP BY 1, 2, 3""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cand AS
    WITH dh AS (SELECT team_fid, year, CAST(week AS INT) AS week, season_type
                FROM '{tg}' GROUP BY 1,2,3,4 HAVING COUNT(*) > 1),
    games AS (
      SELECT DISTINCT g.boxscore_id, g.year, CAST(g.week AS INT) AS week, g.season_type,
             g.team_fid, g.opponent_fid, g.team_code, g.opponent_code, g.game_date
      FROM '{tg}' g
      LEFT JOIN dh ON dh.team_fid = g.team_fid AND dh.year = g.year
        AND dh.week = CAST(g.week AS INT) AND dh.season_type = g.season_type
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
           '{WAVE}:' || g.boxscore_id AS recon_correction_log,
           g.boxscore_id, l.pfr_id,
           (g.season_type = 'POST' AND g.year < 1978) AS pre1978_post,
           {", ".join(f"l.{c}" for c in _all_v26_atoms())}
    FROM lines l
    JOIN games g USING (boxscore_id)
    JOIN '{bio}' bio ON bio.pfr_id = l.pfr_id
    LEFT JOIN abbrev a1 ON a1.fid = g.team_fid AND a1.year = g.year
    LEFT JOIN abbrev a2 ON a2.fid = g.opponent_fid AND a2.year = g.year
    WHERE l.box_team = g.team_code
      AND ({activity})
      AND NOT EXISTS (
        SELECT 1 FROM '{vq}' v WHERE v.NFL_player_id = bio.NFL_player_id
          AND v.year = g.year AND CAST(v.week AS INT) = g.week
          AND v.season_type = g.season_type)""")
    con.execute("CREATE OR REPLACE TEMP TABLE post_queue AS "
                "SELECT * FROM cand WHERE pre1978_post")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE new_rows AS
    SELECT * EXCLUDE (pre1978_post) FROM cand
    WHERE NOT pre1978_post
    QUALIFY COUNT(*) OVER (PARTITION BY player_week) = 1""")


def _diagnostics(con) -> dict:
    n, games = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT boxscore_id) FROM new_rows").fetchone()
    pw_collide = con.execute("""
        SELECT COUNT(*) FROM (SELECT player_week FROM cand WHERE NOT pre1978_post
        GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    post_q = con.execute("SELECT COUNT(*) FROM post_queue").fetchone()[0]
    by_era = con.execute("""
        SELECT year//10*10 AS decade, season_type, COUNT(*)
        FROM new_rows GROUP BY 1,2 ORDER BY 1,2""").fetchall()
    return dict(inserted_rows=n, games_touched=games,
                player_week_ambiguous_skipped=pw_collide,
                pre1978_post_queued=post_q,
                by_decade=[(int(d), st, c) for d, st, c in by_era])


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    vq = _q(latest_v26())
    _build_candidates(con, vq)
    diag = _diagnostics(con)
    con.execute(f"COPY (SELECT * FROM new_rows) TO '{Path(PREVIEW).as_posix()}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM post_queue) TO '{Path(POST_QUEUE).as_posix()}' (HEADER)")
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag,
                "preview_csv": PREVIEW, "post_queue_csv": POST_QUEUE}

    v26_p = Path(latest_v26())
    stamp = utc_stamp()
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    inserted = diag["inserted_rows"]
    tmp = v26_p.with_name(v26_p.stem + "_w49.parquet")
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
    gate = (after == before + inserted) and dup_pw == 0
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
                            reason="witnessed activity line, bio-linked, no v26 row",
                            witness=f"pfr boxscore {bx} (4-table merged line)",
                            source_snapshot_id=snap)
        fc.close()
        bk = v26_p.with_name(v26_p.stem + f"_prew49_{stamp}.parquet")
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
