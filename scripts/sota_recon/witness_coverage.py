"""
sota_recon/witness_coverage.py  --  legacy source-capability map: every atom -> source tables
that can observe it. Promotion policy now lives in witness_gate admissibility contracts.

This is the glossary the reconciliation rests on. For every non-derived atom in the super table
it lists each source path (a source table + the method by which that table produces the atom at
team-game grain) and its era of validity. Counts are inventory context only: they do not imply
independence and can never authorize promotion.

Methods (how a table yields the atom at team-game grain):
  self        sum the team's own players in the boxscore table
  opp         sum the OPPONENT's players (their offense IS our defense allowed/forced)
  ts_self     parse this team's team_stats packed line (Cmp-Att-Yd-TD-INT, Rush-Yds-TDs, ...)
  ts_opp      parse the opponent's team_stats line (their pass_int IS our def INT, etc.)
  scoring     classify+count the scoring table's play descriptions (TD type / FG / safety)
  pbp         reconstruct from play-by-play detail text (1966+). Link order is reliable:
              "Kicker kicks off, returned by RETURNER ... (tackle by T)" -> returner = 2nd
              detail_link_id; "QB sacked by SACKER" -> sacker = 2nd link. Attribute to franchise
              by joining that pfr_id to the player_offense/defense roster for the boxscore.
              CAVEAT (accepted penalties): plays voided by an accepted penalty do NOT count toward
              official stats. In THIS pbp the marker is lowercase "(no play)" (NOT "No Play"):
              ~1,735 of 134,246 return rows are nullified. The witness MUST exclude
              lower(detail) LIKE '%no play%' before counting, or it over-counts vs the boxscore.
              (Most penalty-on-return rows still COUNT -- only the "(no play)" ones are voided.)
  advanced    the *_advanced charted tables (2018+)
  game        nfl_team_games_all game-level fields (points)

    python -m scripts.sota_recon.witness_coverage            # full map -> stdout
    python -m scripts.sota_recon.witness_coverage --md        # markdown table
    python -m scripts.sota_recon.witness_coverage --family defense
"""

from __future__ import annotations

import argparse
from collections import Counter

DEPRECATED_FOR_PROMOTION = True

# atom -> (family, [ (table, method, era_lo, era_hi) ... ])
# era bounds reflect when that table actually carries the atom (yardage 1932+, sacks pbp 1966+,
# charted 2018+, etc.). "impl" markers in stat_consensus note which are wired into the live engine.
COVERAGE = {
    # ---- PASSING (player_offense primary; team_stats Cmp-Att-Yd-TD-INT 1920+) ----
    "completions":            ("passing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("passing_advanced","advanced",2018,2025)]),
    "attempts":               ("passing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("passing_advanced","advanced",2018,2025)]),
    "passing_yards":          ("passing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("passing_advanced","advanced",2018,2025)]),
    "passing_tds":            ("passing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "passing_interceptions":  ("passing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("player_defense","opp",1933,2025),("pbp","pbp",1966,2025)]),
    "sacks_suffered":         ("passing", [("player_offense","self",1947,2025),("team_stats","ts_self",1952,2025),("player_defense","opp",1982,2025),("pbp","pbp",1966,2025)]),
    "sack_yards_lost":        ("passing", [("player_offense","self",1947,2025),("team_stats","ts_self",1952,2025),("pbp","pbp",1966,2025)]),
    "passing_long":           ("passing", [("player_offense","self",1932,2025),("pbp","pbp",1966,2025)]),
    "passing_air_yards":      ("passing", [("passing_advanced","advanced",2018,2025),("pbp","pbp",2006,2025)]),
    "passing_yards_after_catch":("passing",[("passing_advanced","advanced",2018,2025)]),
    "passing_pressured":      ("passing", [("passing_advanced","advanced",2018,2025)]),
    # ---- RUSHING ----
    "carries":                ("rushing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("rushing_advanced","advanced",2018,2025)]),
    "rushing_yards":          ("rushing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("rushing_advanced","advanced",2018,2025)]),
    "rushing_tds":            ("rushing", [("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "rushing_long":           ("rushing", [("player_offense","self",1932,2025),("pbp","pbp",1966,2025)]),
    "rushing_yards_before_contact":("rushing",[("rushing_advanced","advanced",2018,2025)]),
    "rushing_broken_tackles": ("rushing", [("rushing_advanced","advanced",2018,2025)]),
    # ---- RECEIVING (team rec==team cmp, team rec_yds==team pass_yds in team_stats) ----
    "receptions":             ("receiving",[("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("receiving_advanced","advanced",2018,2025)]),
    "receiving_yards":        ("receiving",[("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("pbp","pbp",1966,2025),("receiving_advanced","advanced",2018,2025)]),
    "receiving_tds":          ("receiving",[("player_offense","self",1932,2025),("team_stats","ts_self",1920,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "targets":                ("receiving",[("player_offense","self",1992,2025),("pbp","pbp",1994,2025),("receiving_advanced","advanced",2018,2025)]),
    "receiving_long":         ("receiving",[("player_offense","self",1932,2025),("pbp","pbp",1966,2025)]),
    "receiving_air_yards":    ("receiving",[("receiving_advanced","advanced",2018,2025),("pbp","pbp",2006,2025)]),
    "receiving_yards_after_catch":("receiving",[("receiving_advanced","advanced",2018,2025),("pbp","pbp",2006,2025)]),
    "receiving_broken_tackles":("receiving",[("receiving_advanced","advanced",2018,2025)]),
    # ---- FUMBLES (offense) ----
    "fumbles":                ("fumbles",  [("player_offense","self",1945,2025),("team_stats","ts_self",1934,2025),("pbp","pbp",1966,2025)]),
    "fumbles_lost":           ("fumbles",  [("player_offense","self",1945,2025),("team_stats","ts_self",1934,2025),("pbp","pbp",1966,2025)]),
    # ---- DEFENSE ----
    "def_interceptions":      ("defense",  [("player_defense","self",1933,2025),("player_offense","opp",1932,2025),("team_stats","ts_opp",1920,2025),("pbp","pbp",1966,2025)]),
    "def_interception_yards": ("defense",  [("player_defense","self",1933,2025),("pbp","pbp",1966,2025)]),
    "def_int_ret_td":         ("defense",  [("player_defense","self",1933,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "def_sacks":              ("defense",  [("player_defense","self",1982,2025),("player_offense","opp",1947,2025),("team_stats","ts_opp",1952,2025),("pbp","pbp",1966,2025)]),
    "def_fumbles":            ("defense",  [("player_defense","self",1945,2025),("pbp","pbp",1966,2025)]),
    "fum_rec_yds":            ("defense",  [("player_defense","self",1945,2025),("pbp","pbp",1966,2025)]),
    "fum_ret_td":             ("defense",  [("player_defense","self",1945,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "def_fumbles_forced":     ("defense",  [("player_defense","self",1990,2025),("pbp","pbp",1994,2025)]),
    "def_tackles_solo":       ("defense",  [("player_defense","self",1994,2025),("defense_advanced","advanced",2018,2025),("pbp","pbp",1994,2025)]),
    "def_tackle_assists":     ("defense",  [("player_defense","self",1994,2025),("defense_advanced","advanced",2018,2025)]),
    "def_pass_defended":      ("defense",  [("player_defense","self",1999,2025),("defense_advanced","advanced",2018,2025)]),
    "def_tackles_for_loss":   ("defense",  [("player_defense","self",1999,2025),("pbp","pbp",1999,2025)]),
    "def_qb_hits":            ("defense",  [("player_defense","self",1999,2025),("defense_advanced","advanced",2018,2025)]),
    # ---- KICKING / PUNTING ----
    "fg_made":                ("kicking",  [("kicking","self",1933,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "fg_att":                 ("kicking",  [("kicking","self",1933,2025),("pbp","pbp",1966,2025)]),
    "pat_made":               ("kicking",  [("kicking","self",1933,2025),("scoring","scoring",1932,2025),("pbp","pbp",1966,2025)]),
    "pat_att":                ("kicking",  [("kicking","self",1933,2025),("pbp","pbp",1966,2025)]),
    "punts":                  ("kicking",  [("kicking","self",1941,2025),("pbp","pbp",1966,2025)]),
    "punt_yards":             ("kicking",  [("kicking","self",1941,2025),("pbp","pbp",1966,2025)]),
    "punt_long":              ("kicking",  [("kicking","self",1941,2025),("pbp","pbp",1966,2025)]),
    # ---- RETURNS / SPECIAL TEAMS ----
    "kickoff_returns":        ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "kickoff_return_yards":   ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "kickoff_return_tds":     ("special_teams",[("returns","self",1941,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "kickoff_return_long":    ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "punt_returns":           ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "punt_return_yards":      ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "punt_return_tds":        ("special_teams",[("returns","self",1941,2025),("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "punt_return_long":       ("special_teams",[("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    # ---- TEAM DST scoring (the def/ST decomposition) ----
    "def_tds":                ("team_dst", [("scoring","scoring",1920,2025),("player_defense","self",1933,2025),("pbp","pbp",1966,2025)]),
    "special_teams_tds":      ("team_dst", [("scoring","scoring",1920,2025),("returns","self",1941,2025),("pbp","pbp",1966,2025)]),
    "def_safeties":           ("team_dst", [("scoring","scoring",1920,2025),("pbp","pbp",1966,2025)]),
    "points_allowed":         ("team_dst", [("nfl_team_games_all","game",1920,2025),("team_stats","ts_opp",1920,2025),("scoring","scoring",1920,2025)]),
}

# witnesses wired into the LIVE consensus engine (stat_consensus.WITNESS_MAP) today.
# self/opp/ts_self/ts_opp/scoring/pbp implemented; advanced (2018+ charted) still to add.
IMPL = {"self", "opp", "ts_self", "ts_opp", "scoring", "pbp"}


def run(family=None):
    rows = []
    for atom, (fam, wits) in COVERAGE.items():
        if family and fam != family:
            continue
        n = len(wits)
        n_impl = sum(1 for w in wits if w[1] in IMPL)
        tables = ",".join(w[0] for w in wits)
        earliest = min(w[2] for w in wits)
        rows.append((fam, atom, n, n_impl, earliest, tables))
    rows.sort(key=lambda r: (r[0], -r[2], r[1]))
    return rows


def _print(rows, md=False):
    if md:
        print("| family | atom | #source paths | impl | from year | witness tables |")
        print("|---|---|---|---|---|---|")
        for r in rows:
            print(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]} | {r[5]} |")
    else:
        print(f"{'family':<13} {'atom':<26} {'#src':>5} {'impl':>5} {'since':>6}  witness tables")
        print("-" * 120)
        for r in rows:
            print(f"{r[0]:<13} {r[1]:<26} {r[2]:>5d} {r[3]:>5d} {r[4]:>6d}  {r[5]}")
    cnt = Counter(r[2] for r in rows)
    print(f"\n{len(rows)} atoms mapped | corroboration depth: " +
          ", ".join(f"{k}-witness:{v}" for k, v in sorted(cnt.items(), reverse=True)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", default=None)
    ap.add_argument("--md", action="store_true")
    a = ap.parse_args()
    _print(run(family=a.family), md=a.md)
