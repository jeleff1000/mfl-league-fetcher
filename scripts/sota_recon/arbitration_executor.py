"""THE ARBITRATION EXECUTOR (released by the 2026-08-02 signed desk).

Takes a disputed family, gathers every licensed root's claim per cell, and
writes verdicts under THE ARBITRATION PRECEDENCE LAW (Joe):

    pfr > nflcom > pbp > pfa > statscrew
    -- log ALL cross-source arguments and how many sources are on each side;
    -- if everyone is against pfr, go with everyone;
    -- pfa and statscrew are worth a lot less than the others.

Mechanics per cell:
  * every root's claim is recorded in the argument log (side counts included);
  * heavyweights = pfr, nflcom, pbp; lightweights = pfa, statscrew;
  * if all present heavyweights agree on one value -> that value (unanimity,
    including 'everyone against pfr');
  * else the highest-precedence present root wins -- UNLESS every other
    heavyweight agrees against it (the everyone-rule);
  * lightweights never decide between heavyweights; alone they may only
    corroborate, never overrule;
  * no root speaks -> the cell stays (never invent);
  * verdict == stored -> no write (the stored value was already right).

PILOT: the fumble family at week grain. Roots: pfr (the fumble grammar
events, boxscore->week via the catalog) and pbp (nflverse fumbled-slots).
Stored is the defendant, not a voter.

Output: weekly_overlay_arb_<family>.parquet + an ARGUMENT LOG parquet with
one row per disputed cell per root claim -- the law's 'log all arguments'.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
EVENTS = LAKE / "pfr_fumble_events.parquet"

#: weekly column -> (grammar context filter, nflverse per-week expr builder)
FUMBLE_LANES = {
    "rushing_fumbles": "context = 'rushing'",
    "receiving_fumbles": "context = 'receiving'",
    "sack_fumbles": "context = 'sack'",
}

PRECEDENCE = ["pfr", "nflcom", "pbp", "pfa", "statscrew"]
HEAVY = {"pfr", "nflcom", "pbp"}


def verdict_row(claims: dict[str, float]):
    """Apply the precedence law to one cell's claims. Returns (value, basis)."""
    heavy = {r: v for r, v in claims.items() if r in HEAVY and v is not None}
    if not heavy:
        return None, "no heavyweight speaks -- abstain"
    vals = set(heavy.values())
    if len(vals) == 1:
        v = vals.pop()
        return v, f"unanimous {sorted(heavy)} = {v}"
    head = next(r for r in PRECEDENCE if r in heavy)
    others = {r: v for r, v in heavy.items() if r != head}
    if others and len(set(others.values())) == 1:
        ov = set(others.values()).pop()
        if len(others) >= len(HEAVY) - 1:
            return ov, (f"everyone-against-{head}: {sorted(others)} = {ov} "
                        f"vs {head} = {heavy[head]}")
    return heavy[head], (f"precedence head {head} = {heavy[head]}; "
                         f"dissent {dict(sorted(others.items()))}")


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    overlay = LAKE / "weekly_overlay_arb_fumbles.parquet"
    arglog = LAKE / "arbitration_arguments_fumbles.parquet"

    # pfr grammar claims at week grain (boxscore -> week via catalog)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pfr_claims AS
    SELECT b.NFL_player_id, g.year, TRY_CAST(g.week AS INT) AS week,
           e.context, COUNT(*)::DOUBLE AS v
    FROM read_parquet('{EVENTS.as_posix()}') e
    JOIN (SELECT DISTINCT boxscore_id, year, week FROM '{games}'
          WHERE season_type = 'REG') g USING (boxscore_id)
    JOIN (SELECT pfr_id, NFL_player_id
          FROM read_parquet('{Path(S.PLAYER_BIO.path).as_posix()}')
          WHERE pfr_id IS NOT NULL) b ON b.pfr_id = e.fumbler_id
    GROUP BY 1, 2, 3, 4""")

    # pbp claims: both fumbler slots, context via play type
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pbp_claims AS
    SELECT pid AS NFL_player_id, yr AS year, wkn AS week, ctx AS context,
           COUNT(*)::DOUBLE AS v
    FROM (
      SELECT r.fumbled_1_player_id AS pid, TRY_CAST(r.season AS INT) yr,
             TRY_CAST(r.week AS INT) wkn,
             CASE WHEN TRY_CAST(r.sack AS INT) = 1 THEN 'sack'
                  WHEN TRY_CAST(r.complete_pass AS INT) = 1 THEN 'receiving'
                  WHEN TRY_CAST(r.rush_attempt AS INT) = 1 THEN 'rushing'
                  ELSE 'other' END AS ctx
      FROM read_parquet('{pb}') r
      WHERE r.season_type = 'REG' AND r.fumbled_1_player_id IS NOT NULL
        AND COALESCE(TRY_CAST(r.fumble AS INT), 0) = 1
      UNION ALL
      SELECT r.fumbled_2_player_id, TRY_CAST(r.season AS INT),
             TRY_CAST(r.week AS INT),
             CASE WHEN TRY_CAST(r.sack AS INT) = 1 THEN 'sack'
                  WHEN TRY_CAST(r.complete_pass AS INT) = 1 THEN 'receiving'
                  WHEN TRY_CAST(r.rush_attempt AS INT) = 1 THEN 'rushing'
                  ELSE 'other' END
      FROM read_parquet('{pb}') r
      WHERE r.season_type = 'REG' AND r.fumbled_2_player_id IS NOT NULL)
    GROUP BY 1, 2, 3, 4""")

    verdicts, args = [], []
    for col, ctx in FUMBLE_LANES.items():
        ctxname = ctx.split("'")[1]
        rows = con.execute(f"""
        SELECT t.NFL_player_id, t.year, t.week,
               TRY_CAST(t.{col} AS DOUBLE) AS stored,
               pf.v AS pfr_v, pb2.v AS pbp_v
        FROM read_parquet('{wk}') t
        LEFT JOIN (SELECT * FROM pfr_claims WHERE context = '{ctxname}') pf
          USING (NFL_player_id, year, week)
        LEFT JOIN (SELECT * FROM pbp_claims WHERE context = '{ctxname}') pb2
          USING (NFL_player_id, year, week)
        WHERE t.season_type = 'REG' AND t.year >= 1978
          AND (COALESCE(TRY_CAST(t.{col} AS DOUBLE), 0)
                 <> COALESCE(pf.v, 0)
               OR COALESCE(TRY_CAST(t.{col} AS DOUBLE), 0)
                 <> COALESCE(pb2.v, 0))
          AND (pf.v IS NOT NULL OR pb2.v IS NOT NULL)""").fetchall()
        for pid, yr, wkn, stored, pfr_v, pbp_v in rows:
            claims = {"pfr": pfr_v, "pbp": pbp_v}
            v, basis = verdict_row(claims)
            n_for = sum(1 for x in (pfr_v, pbp_v) if x == v)
            args.append((pid, int(yr), int(wkn), col, stored, pfr_v, pbp_v,
                         v, n_for, basis))
            if v is not None and v != (stored if stored is not None else 0):
                if stored is None and v == 0:
                    continue          # never write zeros onto empty cells
                verdicts.append((pid, int(yr), int(wkn), col, stored, v,
                                 "arb_fumbles", "arbitration_executor",
                                 f"PRECEDENCE LAW: {basis}"))

    con.execute("""CREATE OR REPLACE TEMP TABLE args (
        NFL_player_id VARCHAR, year INT, week INT, column_name VARCHAR,
        stored DOUBLE, pfr_claim DOUBLE, pbp_claim DOUBLE,
        verdict DOUBLE, sources_for_verdict INT, basis VARCHAR)""")
    con.executemany("INSERT INTO args VALUES (?,?,?,?,?,?,?,?,?,?)", args)
    con.execute(f"COPY args TO '{arglog.as_posix()}' (FORMAT parquet)")
    con.execute("""CREATE OR REPLACE TEMP TABLE vd (
        NFL_player_id VARCHAR, year INT, week INT, column_name VARCHAR,
        old_value DOUBLE, new_value DOUBLE, repair_id VARCHAR,
        root VARCHAR, ruling VARCHAR)""")
    con.executemany("INSERT INTO vd VALUES (?,?,?,?,?,?,?,?,?)", verdicts)
    con.execute(f"COPY vd TO '{overlay.as_posix()}' (FORMAT parquet)")

    summary = dict(con.execute("""
    SELECT column_name || ' [' ||
           CASE WHEN basis LIKE 'unanimous%' THEN 'unanimous'
                WHEN basis LIKE 'everyone%' THEN 'everyone-rule'
                WHEN basis LIKE 'precedence%' THEN 'precedence-head'
                ELSE 'abstain' END || ']', COUNT(*)
    FROM args GROUP BY 1""").fetchall())
    receipt = {
        "wave": "arbitration_fumbles_pilot", "date": "2026-08-02",
        "law": "pfr > nflcom > pbp > pfa > statscrew; all arguments logged",
        "disputed_cells": len(args), "verdict_writes": len(verdicts),
        "by_class": summary,
        "argument_log": str(arglog), "overlay": str(overlay),
        "note": ("verdict overlay is NOT auto-applied: fumble licences were "
                 "withheld pending this arbitration -- the argument log is "
                 "the review surface, the overlay joins the next apply batch "
                 "after the log's class counts are sanity-read"),
    }
    (LAKE / "arbitration_fumbles_receipt.json").write_text(
        json.dumps(receipt, indent=2, default=str), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
