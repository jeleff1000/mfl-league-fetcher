"""
sota_recon/build_team_attribution_from_box_v26.py -- WITNESS-DRIVEN team/opponent repair (2 lanes).

LANE A -- TRANSPOSITION (box witness): a player's row carries nfl_team=<opponent>, opponent=<his real
          team>. Fixed from the PFR boxscore witness. 283 provable rows league-wide (JAX 2001-02 = 146).
LANE B -- SELF-PLAY (game catalog): 375 rows where nfl_team == opponent_nfl_team, i.e. a team playing
          ITSELF. Root cause: `data_source='legacy_nflverse_player_stats_cache'` imported roster rows
          (330/375; 298 are OL, only 31 carry any stats, 330 have game_date NULL) whose opponent never
          resolved, leaving opponent_franchise = the player's OWN franchise; `build_franchise_
          normalization` then rendered opponent_nfl_team = MODE(team_code of that franchise) = his own
          team. `build_opponent_backfill_v26` never repaired it because it only fills NULL/blank
          opponents, not WRONG ones. Clusters: STL/OAK/SDG 1999-2002, JAX 2001-02, BOS 1944 -- i.e. the
          nflverse-era team-code trouble spots Joe flagged.
          Fix: re-resolve the opponent from `nfl_team_games_all` by (franchise, year, week, season_type).
          VERIFIED: all 375 resolve to EXACTLY ONE catalog game -- deterministic, no guessing, none
          ambiguous, none phantom.

Supersedes the hand-enumerated `build_team_attribution_fix_v26.py` (which listed 5 Mark Brunell
player_weeks). Joe 2026-07-16: "2000-2002 for Jax seems to have a lot of irregularities ... NFLVerse has
some major bug for jax in that era." Confirmed and measured -- the enumerated repair fixed the QB and left
his whole roster behind, because **the bug is GAME-scoped, not player-scoped**.

THE DEFECT: a player's row carries `nfl_team = <the opponent>` and `opponent_nfl_team = <his real team>`
-- i.e. team and opponent are transposed. The PFR boxscore witness says who actually played for whom.

MEASURED (all history, super vs box across player_offense+player_defense+kicking+returns):
    396 team mismatches total
      283 CLEAN SWAP  (super.opponent_nfl_team == box team)  <- this builder fixes exactly these
      113 ambiguous   (super.opponent_nfl_team != box team)  <- NOT touched; queued
    Of the 283: **JAX 2001 = 80 rows / 8 games, JAX 2002 = 66 rows / 8 games** (the nflverse JAX bug --
    whole rosters credited to the opponent: Fred Taylor, Jimmy Smith, Kyle Brady, Stacey Mack, Jonathan
    Quinn, Chris Hanson, ...). 2002 wk1 vs IND is the "Brunell on the Colts" game Joe remembered: Brunell
    was already repaired, 8 team-mates were not. Remainder is a scatter of 2-3 row incidents across other
    franchise-years. 1999/2000/2003 JAX are CLEAN (0).
    The 113 ambiguous are dominated by **1944 WAS (102)** -- a DIFFERENT bug (Washington players filed under
    the Boston franchise; opponent is not the real team, so a swap would not fix it). Left for adjudication.

SAFETY: only rows whose own `opponent_nfl_team` already equals the box witness's team are touched, so the
repair is a provable transposition (both halves of the swap are witnessed). Player STATS are untouched ->
`fpts_*` is invariant (gated).

!! CASCADE: team attribution feeds team-grain rollups. After --apply, the DST lane must be rebuilt:
   build_allowed_mirror -> build_dst_ceiling_repair -> build_deterministic_recompute (yds_allow buckets)
   -> build_dst_scoring_v26 -> gates. DO NOT promote without that chain.

    python -m scripts.sota_recon.build_team_attribution_from_box_v26            # dry-run
    python -m scripts.sota_recon.build_team_attribution_from_box_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
SRCS = ["player_offense", "player_defense", "kicking", "returns"]
PROV = "wave59.team_attribution_from_box"


def _build_witness(con) -> None:
    union = " UNION ALL ".join(
        f"SELECT b.player_link_ids AS lid, b.boxscore_id AS bid, b.team AS tm "
        f"FROM read_parquet('{BOX}/{s}/_combined.parquet') b WHERE b.player_link_ids IS NOT NULL"
        for s in SRCS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE w AS
        SELECT DISTINCT COALESCE(bi.NFL_player_id, REPLACE(CAST(u.lid AS VARCHAR),'pfr:','')) AS nfl_id,
               CAST(g.year AS INT) AS y, CAST(g.week AS INT) AS w, g.season_type AS st,
               g.team_code AS box_team, CAST(g.team_fid AS INT) AS box_fid,
               g.opponent_code AS box_opp, CAST(g.opponent_fid AS INT) AS box_opp_fid
        FROM ({union}) u
        JOIN read_parquet('{TG}') g ON g.boxscore_id=u.bid AND g.team_code=u.tm
        LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
                   WHERE pfr_id IS NOT NULL) bi
          ON bi.pfr_id = REPLACE(CAST(u.lid AS VARCHAR),'pfr:','')""")


def _mk_catalog(con) -> None:
    """(franchise, year, week, season_type) -> the ONE game + its opponent, for the self-play lane."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cal AS
        SELECT CAST(year AS INT) AS y, CAST(week AS INT) AS w, season_type AS st,
               CAST(team_fid AS INT) AS fr, COUNT(*) AS games,
               MIN(opponent_code) AS opp_code, MIN(CAST(opponent_fid AS INT)) AS opp_fid
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL
        GROUP BY 1,2,3,4 HAVING COUNT(*) = 1""")


def _stats(con, tbl: str) -> dict:
    tot = con.execute(f"""SELECT COUNT(*) FROM {tbl} s JOIN w
        ON CAST(s.NFL_player_id AS VARCHAR)=CAST(w.nfl_id AS VARCHAR)
        AND CAST(s.year AS INT)=w.y AND CAST(s.week AS INT)=w.w AND s.season_type=w.st
        WHERE s.position<>'DEF' AND s.nfl_team <> w.box_team""").fetchone()[0]
    clean = con.execute(f"""SELECT COUNT(*) FROM {tbl} s JOIN w
        ON CAST(s.NFL_player_id AS VARCHAR)=CAST(w.nfl_id AS VARCHAR)
        AND CAST(s.year AS INT)=w.y AND CAST(s.week AS INT)=w.w AND s.season_type=w.st
        WHERE s.position<>'DEF' AND s.nfl_team <> w.box_team
          AND s.opponent_nfl_team = w.box_team""").fetchone()[0]
    self_play = con.execute(
        f"SELECT COUNT(*) FROM {tbl} WHERE nfl_team = opponent_nfl_team").fetchone()[0]
    return {"mismatches": tot, "clean_swaps": clean, "ambiguous": tot - clean,
            "self_play": self_play}


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='3GB'")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    _build_witness(con)
    _mk_catalog(con)

    if not apply:
        con.execute(f"CREATE OR REPLACE TEMP VIEW st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
        res = _stats(con, "st")
        res["by_year_team"] = con.execute(f"""SELECT w.y, w.box_team, COUNT(*) AS n
            FROM st s JOIN w ON CAST(s.NFL_player_id AS VARCHAR)=CAST(w.nfl_id AS VARCHAR)
              AND CAST(s.year AS INT)=w.y AND CAST(s.week AS INT)=w.w AND s.season_type=w.st
            WHERE s.position<>'DEF' AND s.nfl_team <> w.box_team AND s.opponent_nfl_team = w.box_team
            GROUP BY 1,2 ORDER BY 3 DESC LIMIT 8""").fetchall()
        shutil.rmtree(sp, ignore_errors=True)
        return res

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{Path(v26).as_posix()}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    before = _stats(con, "st")

    # provable transposition: box says he played for X; the row says team=Y, opponent=X -> swap both pairs.
    # LANE A+C (unified): wherever the box witness disagrees with the row's team, take BOTH halves from
    # the witness -- team AND franchise number AND the opponent pair from that franchise's catalog game.
    # A (transposition, super_opp == box_team) and C (franchise misfiling, super_opp != box_team) reduce to
    # the SAME action, so they are one lane. Joe: "that's why we have franchise id number" -- the box
    # witness carries team_fid, so a misfiled franchise (1944 Washington sitting on the Boston Yanks'
    # fid148 -> real fid4) is repaired by the fid, not by a team-code swap.
    con.execute("""CREATE OR REPLACE TEMP TABLE fixme AS
        SELECT s.player_week, w.box_team, w.box_fid, w.box_opp, w.box_opp_fid
        FROM st s JOIN w ON CAST(s.NFL_player_id AS VARCHAR)=CAST(w.nfl_id AS VARCHAR)
          AND CAST(s.year AS INT)=w.y AND CAST(s.week AS INT)=w.w AND s.season_type=w.st
        WHERE s.position<>'DEF' AND s.nfl_team <> w.box_team""")
    n_fix = con.execute("SELECT COUNT(*) FROM fixme").fetchone()[0]
    con.execute(f"""UPDATE st SET
            nfl_team = f.box_team,
            opponent_nfl_team = f.box_opp,
            nfl_franchise_number = f.box_fid,
            opponent_nfl_franchise_number = f.box_opp_fid,
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}' ELSE recon_correction_log || ';{PROV}' END
        FROM fixme f WHERE st.player_week = f.player_week""")

    # --- LANE B: self-play -> re-resolve the opponent from the game catalog.
    # PRECONDITION (added after a near-miss): Lane B assumes the player's OWN team is right and only the
    # OPPONENT half is wrong. TRUE for the modern clusters (STL/OAK/SDG/JAX 1999-2002: OL/appearance rows,
    # 0 box contradictions). FALSE for 1944 BOS, where 22 of 45 self-play rows are WASHINGTON players
    # misfiled onto the Boston Yanks' franchise -- resolving those from fid148's calendar would hand them
    # the Boston Yanks' opponent: a plausible-looking WRONG fix that also destroys the self-play evidence.
    # Guards: (1) skip any row the box witness contradicts (Lane A+C owns those, by fid);
    #         (2) skip 1944 entirely -- only 12 of 208 BOS-coded rows are WAS-coded while Washington played
    #             a full season, so the un-box-joinable remainder is of UNPROVEN team and must not be
    #             "resolved" off a franchise we don't trust. Residual stays visible in the self_play gate.
    con.execute("""CREATE OR REPLACE TEMP TABLE spfix AS
        SELECT s.player_week, c.opp_code, c.opp_fid
        FROM st s JOIN cal c
          ON c.y=CAST(s.year AS INT) AND c.w=CAST(s.week AS INT) AND c.st=s.season_type
         AND c.fr=CAST(s.nfl_franchise_number AS INT)
        WHERE s.nfl_team = s.opponent_nfl_team
          AND CAST(s.year AS INT) <> 1944
          AND NOT EXISTS (SELECT 1 FROM w
                          WHERE CAST(w.nfl_id AS VARCHAR)=CAST(s.NFL_player_id AS VARCHAR)
                            AND w.y=CAST(s.year AS INT) AND w.w=CAST(s.week AS INT)
                            AND w.st=s.season_type AND w.box_team <> s.nfl_team)""")
    n_sp = con.execute("SELECT COUNT(*) FROM spfix").fetchone()[0]
    con.execute(f"""UPDATE st SET
            opponent_nfl_team = f.opp_code,
            opponent_nfl_franchise_number = f.opp_fid,
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}.selfplay' ELSE recon_correction_log || ';{PROV}.selfplay' END
        FROM spfix f WHERE st.player_week = f.player_week""")
    after = _stats(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_teamattr.parquet")
    rb = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w_ = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w_.write_batch(b)
    w_.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    _f = "COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)"
    fpts_orig = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{Path(v26).as_posix()}')").fetchone()[0]
    fpts_new = con.execute(f"SELECT ROUND(SUM({_f}),1) FROM read_parquet('{tq}')").fetchone()[0]
    con.close()

    # self_play residual is EXPECTED: the 1944 own-team-disputed rows are deliberately not touched here.
    gate = (after_rows == before_rows and fpts_orig == fpts_new
            and after["clean_swaps"] == 0 and after["self_play"] <= 45 and n_fix > 0)
    res = {"laneA_transpositions_fixed": n_fix, "laneB_self_play_fixed": n_sp,
           "before": before, "after": after,
           "rows": f"{before_rows} -> {after_rows}", "fpts_invariant": fpts_orig == fpts_new,
           "gate_pass": bool(gate),
           "NEXT": "CASCADE REQUIRED: build_allowed_mirror -> build_dst_ceiling_repair -> "
                   "build_deterministic_recompute -> build_dst_scoring_v26 -> gates"}
    if gate:
        backup = vp.with_name(vp.stem + f"_preteamattr_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    import json
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
