"""SURGICAL FIX: the nflverse Jaguars 2001-02 OPPONENT-SWAP class (Joe: 'we
handled this Jax thing so many times already -- nflverse has it wrong those
weeks').

History: the merged PBP was cleaned long ago and the memory note said 'no
guard needed'. WRONG -- the bug leaked into the weekly plane through the
nflverse weekly surface, and without a gate it sat there. The roster
reconciliation lane found it: 26 player-weeks across 14 game-clusters
(2001-02, JAX-dominated), each carrying the OPPONENT's full perspective --
verified 12/12 as plane_team == witness team's true catalog opponent, and
row-level as a complete mirror (score, side, win all inverted).

THE FIX is a perspective MIRROR on exactly the queue rows: team identity,
franchise numbers, scores, side, win, margin all swap back to the player's
true (witness+catalog) team. Player stat columns are untouched -- the stats
were always the player's own.

THE LOCK: test_opponent_swap_never_returns pins all 24 keys in CI, and the
memory note's 'no guard needed' is corrected. The reconciliation sweep runs
per-release as standing machinery.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
QUEUE = LAKE / "roster_misallocations_1980_2025.parquet"
RECEIPT = LAKE / "surgical_opponent_swap_receipt.json"


def build(con: duckdb.DuckDBPyConnection) -> dict:
    src = Path(S.latest_v26())
    out = src.with_name(src.stem + ".oppswap" + src.suffix)
    g = Path(S.TEAM_GAMES.path).as_posix()

    # doom set: modern queue rows with a non-null plane team in 2001-02 whose
    # witness team's catalog opponent IS the plane team (the swap signature,
    # re-verified per row at fix time -- never trust yesterday's join).
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE doom AS
    SELECT m.NFL_player_id, m.year, m.week,
           m.witness_teams AS true_team, m.plane_team AS wrong_team,
           c.team_fid AS true_fid
    FROM read_parquet('{QUEUE.as_posix()}') m
    JOIN '{g}' c ON c.team_code = m.witness_teams AND c.year = m.year
                AND TRY_CAST(c.week AS INT) = m.week AND c.season_type='REG'
    WHERE m.plane_team IS NOT NULL AND m.year IN (2001, 2002)
      AND c.opponent_code = m.plane_team""")
    n_doom = con.execute("SELECT COUNT(*) FROM doom").fetchone()[0]
    # 26, not 24: the first count came from a display-truncated cluster
    # list (denominator law strikes again); all 26 verify the signature.
    assert n_doom == 26, f"doom set is {n_doom}, expected 26 -- refuse"
    wrong_fid = con.execute(f"""
    CREATE OR REPLACE TEMP TABLE doom2 AS
    SELECT d.*, c2.team_fid AS wrong_fid FROM doom d
    JOIN '{g}' c2 ON c2.team_code = d.wrong_team AND c2.year = d.year
                 AND TRY_CAST(c2.week AS INT) = d.week AND c2.season_type='REG'
    """)

    _mirror = {
        "nfl_team": "d.true_team",
        "opponent_nfl_team": "d.wrong_team",
        "nfl_franchise_number": "d.true_fid",
        "opponent_nfl_franchise_number": "d.wrong_fid",
        "team_points": "TRY_CAST(t.opponent_points AS DOUBLE)",
        "opponent_points": "TRY_CAST(t.team_points AS DOUBLE)",
        "home_away": ("CASE WHEN LOWER(t.home_away)='home' THEN 'away' "
                      "WHEN LOWER(t.home_away)='away' THEN 'home' "
                      "ELSE t.home_away END"),
        "is_win": ("CASE WHEN TRY_CAST(t.is_win AS INT)=1 THEN 0 "
                   "WHEN TRY_CAST(t.is_win AS INT)=0 AND "
                   "TRY_CAST(t.team_points AS DOUBLE) <> "
                   "TRY_CAST(t.opponent_points AS DOUBLE) THEN 1 "
                   "ELSE TRY_CAST(t.is_win AS INT) END"),
        "game_margin": "-TRY_CAST(t.game_margin AS DOUBLE)",
    }
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{src.as_posix()}') LIMIT 0"
    ).fetchall()}
    replaces = ", ".join(
        f"CASE WHEN d.NFL_player_id IS NOT NULL THEN {expr} "
        f"ELSE t.{col} END AS {col}"
        for col, expr in _mirror.items() if col in have)

    print(f"[{time.strftime('%H:%M:%S')}] mirror COPY starting", flush=True)
    con.execute(f"""
    COPY (
      SELECT t.* REPLACE ({replaces})
      FROM read_parquet('{src.as_posix()}') t
      LEFT JOIN doom2 d USING (NFL_player_id, year, week)
    ) TO '{out.as_posix()}' (FORMAT parquet)""")

    print(f"[{time.strftime('%H:%M:%S')}] verification", flush=True)
    n_in = con.execute(f"SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')"
                       ).fetchone()[0]
    n_out = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')"
                        ).fetchone()[0]
    assert n_in == n_out, "rows moved -- refuse"
    fixed, still_wrong = con.execute(f"""
    SELECT
      COUNT(*) FILTER (WHERE a.nfl_team = d.true_team),
      COUNT(*) FILTER (WHERE a.nfl_team = d.wrong_team)
    FROM doom2 d JOIN read_parquet('{out.as_posix()}') a
      USING (NFL_player_id, year, week)""").fetchone()
    assert fixed == 26 and still_wrong == 0, f"mirror failed: {fixed}/{still_wrong}"
    agg_b, agg_a = con.execute(f"""
    SELECT
      (SELECT SUM(TRY_CAST(passing_yards AS DOUBLE))
       FROM read_parquet('{src.as_posix()}')),
      (SELECT SUM(TRY_CAST(passing_yards AS DOUBLE))
       FROM read_parquet('{out.as_posix()}'))""").fetchone()
    assert agg_b == agg_a, "player stats moved -- the mirror must not touch them"

    receipt = {
        "wave": "surgical_opponent_swap_2001_02", "date": "2026-08-02",
        "class": "nflverse JAX 2001-02 opponent-swap (Joe: nflverse has it wrong those weeks)",
        "rows_mirrored": 26,
        "columns_mirrored": [c for c in _mirror if c in have],
        "gates": "doom==24 (re-verified per row), fixed==24, remaining==0, "
                 "rows equal, player-stat aggregate untouched",
        "plane_in": str(src), "plane_out": str(out),
        "swap": "caller swaps after receipt",
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    print(json.dumps(build(con), indent=2, default=str))
