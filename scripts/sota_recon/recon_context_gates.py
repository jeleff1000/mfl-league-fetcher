"""
sota_recon/recon_context_gates.py -- the CONTEXT/COVERAGE gates the lattice never had.

Joe 2026-07-16: "we have all those era gates to check completeness though. the witnesses were supposed to
check matchups and teams and identities. wire those all in."

The 2026-07-16 audit found three defect classes that ~20 existing lanes all slept through, and the reason
was structural, not accidental (runbook 10):
  * `pick6` sat 100% EMPTY for the table's whole life. recon_column_census DOES detect it (CONSTANT), but
    run_all classifies ALL_NULL/CONSTANT as *informational* -> it can never fail.
  * 146 JAX 2001-02 player-rows had nfl_team/opponent TRANSPOSED. recon_bounce PASSES at >=99% of
    team-games and 16 bad games is 0.065% -> 99.93% = PASS. recon_schedule anchors GAMES (existence/
    date/opponent/score), never a PLAYER's team. Every individual row is perfectly valid.
  * 45 rows of 1944 have team == opponent (a team playing ITSELF). recon_internal's HARD tier is entirely
    intra-row STAT arithmetic (completions>attempts...) -- no CONTEXT invariant exists.

So the gap is: value-agreement was gated; EXISTENCE, ENTITY-BINDING and CONTEXT were not. This lane adds
them as **absolute-count FAIL gates** (never percentages -- a rate gate cannot see a localized cluster).

GATES (any nonzero count FAILS):
  CONTEXT.self_play         a team cannot play itself (nfl_team == opponent_nfl_team).
  CONTEXT.team_vs_box       a player's team must equal the PFR boxscore witness's team for that game
                            (the JAX 2001-02 class). Only rows joinable to the box witness are judged.
  CONTEXT.targets_gt_att    team-grain: a team cannot target more receivers than it threw passes.
  COVERAGE.dead_column      a stat column that a registered witness covers is entirely empty/constant in
                            an era the witness spans (the pick6 class).
  IDENTITY.dup_player_week  same player, same game, >1 row (excluding true doubleheaders).
  IDENTITY.clone_pair       TWO DIFFERENT ids, same franchise+game, IDENTICAL nonzero stat line = one
                            human double-counted. This catches what every identity lane structurally
                            cannot: `recon_identity_splits` links ids only via shared pfr_id or DOB+name,
                            and the worst dups have NEITHER (one side lacks a DOB, the other lacks a
                            pfr_id) AND different display names -- nickname vs real name. Measured:
                            **147 id-pairs / 241 clone games**, incl. Raghib/Rocket Ismail (31 games),
                            Trevor/T.J. Graham (14), Cameron/Cam Cleeland (13), Karim Abdul-Jabbar (11).
                            Report-only threshold note: 1-game pairs at low stat mass CAN be coincidence
                            (two players with the same line); >=2 games is effectively proof.

    python -m scripts.sota_recon.recon_context_gates            # summary
    python -m scripts.sota_recon.recon_context_gates --csv DIR  # dump offending rows
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from .sources import latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
BOX_SRCS = ["player_offense", "player_defense", "kicking", "returns"]

_D = lambda c: f'COALESCE(TRY_CAST("{c}" AS DOUBLE),0)'

# COVERAGE.dead_column: column -> (era_lo, era_hi) that a registered witness demonstrably covers.
# A column empty across its whole witnessed era is a build that never ran (not an availability gap).
# Keep to atoms with an UNAMBIGUOUS witness; era floors are the measured witness floors (runbook 5b/5d).
WITNESSED_ERAS = {
    "pick6": (1978, 2025),                      # pbp interception+return_touchdown, passer 100% attributed
    "def_int_ret_td": (1978, 2025),
    "fumbles": (1978, 2025),
    "def_sacks": (1982, 2025),
    "def_tackles_solo": (1978, 2025),
    "targets": (1992, 2025),
    "passing_air_yards": (2006, 2025),
    "receiving_air_yards": (2006, 2025),
    "passing_epa": (1999, 2025),
    "rushing_first_downs": (1978, 2025),
    "receiving_first_downs": (1978, 2025),
    "penalties": (1999, 2025),
    "special_teams_tds": (1978, 2025),
}


def _mk_box_witness(con) -> None:
    union = " UNION ALL ".join(
        f"SELECT b.player_link_ids AS lid, b.boxscore_id AS bid, b.team AS tm "
        f"FROM read_parquet('{BOX}/{s}/_combined.parquet') b WHERE b.player_link_ids IS NOT NULL"
        for s in BOX_SRCS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE boxw AS
        SELECT DISTINCT COALESCE(bi.NFL_player_id, REPLACE(CAST(u.lid AS VARCHAR),'pfr:','')) AS nfl_id,
               CAST(g.year AS INT) AS y, CAST(g.week AS INT) AS w, g.season_type AS st,
               g.team_code AS box_team
        FROM ({union}) u
        JOIN read_parquet('{TG}') g ON g.boxscore_id=u.bid AND g.team_code=u.tm
        LEFT JOIN (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}')
                   WHERE pfr_id IS NOT NULL) bi
          ON bi.pfr_id = REPLACE(CAST(u.lid AS VARCHAR),'pfr:','')""")


def run(csv_dir: str | None = None) -> dict:
    v26 = Path(latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'"); con.execute("PRAGMA threads=4")
    con.execute("PRAGMA disable_progress_bar")
    con.execute(f"CREATE OR REPLACE TEMP VIEW st AS SELECT * FROM read_parquet('{v26}')")
    _mk_box_witness(con)
    gates: dict[str, dict] = {}

    # --- CONTEXT.franchise_code_collision (the GENERATOR guard) -----------------------------
    # `build_franchise_normalization` sets nfl_team := MODE(team_code) PER (year, franchise). That is only
    # safe while each franchise-year maps to exactly ONE real team code. It once did not: the PFR schedule
    # authority merged the 1944-48 Boston Yanks INTO the Redskins' fid4, so MODE over (1944, fid4) picked
    # 'BOS' and stamped it onto Washington's players (Sammy Baugh, Frank Filchock) -- after which
    # build_boston_franchise_repair dutifully moved every BOS/1944-48/fr4 row to fid148, carrying the
    # Redskins' roster onto the Boston Yanks. The catalog has since been split (BOS=fid148, WAS=fid4) so
    # this reads 0 today; this gate exists so the generator can never silently come back.
    n = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT CAST(year AS INT) AS y, CAST(team_fid AS INT) AS fid
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL
        GROUP BY 1,2 HAVING COUNT(DISTINCT team_code) > 1)""").fetchone()[0]
    gates["CONTEXT.franchise_code_collision"] = {"violations": n,
        "note": "catalog franchise-year with >1 team code -> MODE(team_code) would smear one code "
                "across two real franchises (the 1944 WAS/BOS generator)"}

    # --- CONTEXT.self_play -----------------------------------------------------------------
    n = con.execute("""SELECT COUNT(*) FROM st
        WHERE nfl_team IS NOT NULL AND opponent_nfl_team IS NOT NULL
          AND nfl_team = opponent_nfl_team""").fetchone()[0]
    gates["CONTEXT.self_play"] = {"violations": n, "note": "a team cannot play itself"}

    # --- CONTEXT.team_vs_box ---------------------------------------------------------------
    r = con.execute("""SELECT COUNT(*) AS n,
            COUNT(*) FILTER (WHERE s.opponent_nfl_team = b.box_team) AS transposed
        FROM st s JOIN boxw b
          ON CAST(s.NFL_player_id AS VARCHAR)=CAST(b.nfl_id AS VARCHAR)
         AND CAST(s.year AS INT)=b.y AND CAST(s.week AS INT)=b.w AND s.season_type=b.st
        WHERE s.position<>'DEF' AND s.nfl_team <> b.box_team""").fetchone()
    gates["CONTEXT.team_vs_box"] = {"violations": r[0], "of_which_transposed": r[1],
                                    "note": "player's team must match the PFR boxscore witness"}

    # --- CONTEXT.targets_gt_att (team grain) -----------------------------------------------
    n = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT CAST(year AS INT) AS y, CAST(week AS INT) AS wk, season_type, nfl_franchise_number,
               SUM({_D('targets')}) AS tgt, SUM({_D('attempts')}) AS att
        FROM st WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL AND year>=1978
        GROUP BY 1,2,3,4) t WHERE t.tgt > t.att""").fetchone()[0]
    gates["CONTEXT.targets_gt_att"] = {"violations": n,
                                       "note": "team cannot target more receivers than passes thrown"}

    # --- COVERAGE.dead_column --------------------------------------------------------------
    have = {r[0] for r in con.execute("DESCRIBE st").fetchall()}
    dead = []
    for col, (lo, hi) in WITNESSED_ERAS.items():
        if col not in have:
            dead.append({"column": col, "era": f"{lo}-{hi}", "reason": "COLUMN_MISSING"})
            continue
        nz = con.execute(f"""SELECT COUNT(*) FROM st
            WHERE {_D(col)} <> 0 AND CAST(year AS INT) BETWEEN {lo} AND {hi}""").fetchone()[0]
        if nz == 0:
            dead.append({"column": col, "era": f"{lo}-{hi}", "reason": "EMPTY_IN_WITNESSED_ERA"})
    gates["COVERAGE.dead_column"] = {"violations": len(dead), "dead": dead,
                                     "note": "a witnessed column empty across its witnessed era = a build that never ran"}

    # --- IDENTITY.dup_player_week ----------------------------------------------------------
    n = con.execute("""SELECT COUNT(*) FROM (
        SELECT NFL_player_id, year, week, season_type, COUNT(*) AS c,
               COUNT(DISTINCT opponent_nfl_franchise_number) AS opps
        FROM st WHERE position<>'DEF' AND NFL_player_id IS NOT NULL
        GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
           AND COUNT(*) > COUNT(DISTINCT opponent_nfl_franchise_number))""").fetchone()[0]
    gates["IDENTITY.dup_player_week"] = {"violations": n,
                                         "note": "same player+game twice (doubleheaders excluded via distinct opponents)"}

    # --- IDENTITY.clone_pair ---------------------------------------------------------------
    # one human under two ids: identical nonzero stat fingerprint in the same team-game.
    _I = lambda c: f'CAST(COALESCE(TRY_CAST("{c}" AS DOUBLE),0) AS INT)'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fp AS
        SELECT CAST(year AS INT) AS y, CAST(week AS INT) AS w, season_type AS st,
               CAST(nfl_franchise_number AS INT) AS fr, NFL_player_id AS id, player,
               {_I('receptions')}||'-'||{_I('receiving_yards')}||'-'||{_I('targets')}||'-'||
               {_I('carries')}||'-'||{_I('rushing_yards')}||'-'||{_I('attempts')}||'-'||
               {_I('passing_yards')}||'-'||{_I('def_tackles_solo')} AS sig,
               {_I('receptions')}+{_I('receiving_yards')}+{_I('carries')}+{_I('rushing_yards')}+
               {_I('attempts')}+{_I('passing_yards')}+{_I('def_tackles_solo')} AS mass
        FROM st WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE clones AS
        SELECT a.id AS id_a, MIN(a.player) AS player_a, b.id AS id_b, MIN(b.player) AS player_b,
               COUNT(*) AS games, MIN(a.y) AS y0, MAX(a.y) AS y1
        FROM fp a JOIN fp b
          ON a.y=b.y AND a.w=b.w AND a.st=b.st AND a.fr=b.fr AND a.sig=b.sig AND a.id < b.id
        WHERE a.mass >= 20 GROUP BY a.id, b.id""")
    r = con.execute("""SELECT COUNT(*) FILTER (WHERE games >= 2), COUNT(*), SUM(games)
        FROM clones""").fetchone()
    gates["IDENTITY.clone_pair"] = {"violations": r[0], "all_pairs_incl_single_game": r[1],
                                    "clone_games": int(r[2] or 0),
                                    "note": "one human under 2 ids (>=2 identical games = proof); "
                                            "invisible to identity lanes (no shared pfr_id/DOB, names differ)"}

    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)
        con.execute(f"""COPY (SELECT * FROM clones ORDER BY games DESC)
            TO '{Path(csv_dir).as_posix()}/gate_clone_pairs.csv' (HEADER)""")
        con.execute(f"""COPY (SELECT player_week, player, year, week, nfl_team, opponent_nfl_team
            FROM st WHERE nfl_team = opponent_nfl_team)
            TO '{Path(csv_dir).as_posix()}/gate_self_play.csv' (HEADER)""")
        con.execute(f"""COPY (SELECT s.player_week, s.player, b.y AS year, b.w AS week,
                s.nfl_team AS super_team, b.box_team, s.opponent_nfl_team AS super_opp
            FROM st s JOIN boxw b ON CAST(s.NFL_player_id AS VARCHAR)=CAST(b.nfl_id AS VARCHAR)
             AND CAST(s.year AS INT)=b.y AND CAST(s.week AS INT)=b.w AND s.season_type=b.st
            WHERE s.position<>'DEF' AND s.nfl_team <> b.box_team ORDER BY b.y, b.w)
            TO '{Path(csv_dir).as_posix()}/gate_team_vs_box.csv' (HEADER)""")
    con.close()

    total = sum(g["violations"] for g in gates.values())
    return {"status": "fail" if total else "pass", "total_violations": total, "gates": gates}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None, help="dump offending rows to this dir")
    a = ap.parse_args()
    res = run(csv_dir=a.csv)
    print(f"{'GATE':30}{'VIOLATIONS':>12}   note")
    print("-" * 100)
    for k, v in res["gates"].items():
        print(f"{k:30}{v['violations']:>12}   {v['note']}")
        for d in v.get("dead", []):
            print(f"{'':30}{'':>12}   -> {d['column']} {d['era']} {d['reason']}")
    print(f"\nVERDICT: {res['status'].upper()} ({res['total_violations']} total violations)")
