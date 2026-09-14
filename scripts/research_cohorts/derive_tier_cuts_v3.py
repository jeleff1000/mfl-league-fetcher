"""Tier cuts as MIDPOINTS between tier centres, with a landing test that must pass.

THE BUG THIS FIXES. v2 set each cut with `quantile_cont(capacity, P(teams <= N))`, which
returns the TOP of that team count's own distribution, not the midpoint to the next one. So
the boundary landed exactly ON a tier centre: a non-superflex 10-team league starts exactly
10 QBs, the 10tm cut came out 10.0, and `capacity >= 10.0` threw every 10-team league into
12tm. Every one of the 18 v2 cut sets is shifted a tier high the same way.

THE FIX. Tier centres are 8, 10, 12, 14 teams. A position that runs at rate `r` spots per
team has centres at 8r, 10r, 12r, 14r, so the boundaries are the midpoints:

    c1 = 9r    c2 = 11r    c3 = 13r

`r` is MEASURED (observed spots / num_teams, median over the lane), never assumed.

THE GATE, AND IT DIFFERS BY STAT.

  STARTED  the landing test binds. Starting lineups are pinned by the rules -- a non-superflex
           league starts exactly one QB per team -- so a literal N-team league MUST land in the
           N tier. Under 70% means the cuts are misplaced. This is the check that would have
           caught the v2 off-by-one instantly and was never run.

  ROSTERED the landing test does NOT bind, and a low rate is the tier WORKING. Bench depth
           varies enormously within a team count, so a deep 10-team league genuinely has
           12-team rostering demand and should land in 12tm. Demanding high landing here would
           be demanding the tier reproduce num_teams, which is the thing it exists to improve
           on. Rostered is gated on DRIFT alone.

Lanes matter and are not optional. QB is derived separately for flx and sflx because superflex
IS the QB axis (R1): started goes 1.0 -> 1.8 per team and the rostered c2 boundary moves 65%.
Every lane is managed redraft only -- dynasty stashes and best-ball depth inflate rostered and
drift as their corpus share grows, which is what made flx QB read 2.3 instead of 1.8.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

CENTRES = (8, 10, 12, 14)
MID = (9, 11, 13)                     # midpoints between the centres, in team units
IDP = ("DL", "LB", "DB")
# ELIGIBILITY-GATED positions. A position that only exists in some leagues must be measured
# ONLY in those leagues. K sits in ~40% of leagues and DST in ~53%, so the median league has
# zero K slots -- pooling over everyone made K read 0.00 started and 0.00 rostered, which
# would have passed the mechanism check as a false green.
GATED = {"K": "roster_K", "DEF": "roster_DEF",
         "DL": "roster_DL", "LB": "roster_LB", "DB": "roster_DB"}
SLOT_COL = {"QB": "roster_QB", "RB": "roster_RB", "WR": "roster_WR", "TE": "roster_TE",
            "K": "roster_K", "DEF": "roster_DEF", "DL": "roster_DL", "LB": "roster_LB",
            "DB": "roster_DB"}
# QB is split because superflex is the QB axis; the others get the mainstream lane only until
# a superflex read is done for them too.
LANES = {"QB": ("flx", "sflx")}


def lane_sql(pos: str, lane: str | None) -> str:
    idp_free = ("COALESCE(s.roster_IDP,0)=0 AND COALESCE(s.roster_DL,0)=0 "
                "AND COALESCE(s.roster_LB,0)=0 AND COALESCE(s.roster_DB,0)=0")
    # MANAGED REDRAFT ONLY -- omitting this is what inflated flx QB rostered to 2.3
    base = ("COALESCE(s.sleeper_best_ball,false)=false "
            "AND COALESCE(s.is_dynasty,false)=false")
    if pos in IDP:
        # IDP slots are not just roster_DL/LB/DB -- roster_IDP is a catch-all and DB_LB/DL_LB
        # are combo slots. A league fields this position if ANY of them is populated, which is
        # why LB overshot by +1.10 when only its own column was counted.
        combo = {"DL": ["roster_DL", "roster_DL_LB", "roster_IDP"],
                 "LB": ["roster_LB", "roster_DL_LB", "roster_DB_LB", "roster_IDP"],
                 "DB": ["roster_DB", "roster_DB_LB", "roster_IDP"]}[pos]
        any_slot = " + ".join(f"COALESCE(s.{c},0)" for c in combo)
        return f"{base} AND ({any_slot}) > 0"
    if pos in GATED:
        return f"{base} AND {idp_free} AND COALESCE(s.{GATED[pos]},0) > 0"
    sfx = "COALESCE(s.roster_SUPER_FLEX,0)"
    if lane == "sflx":
        return f"{base} AND {idp_free} AND {sfx} > 0"
    return f"{base} AND {idp_free} AND {sfx} = 0"


def run(snapshot: Path, ops: Path, pos: str, lane: str | None) -> list[dict]:
    out = []
    for yr in range(2021, 2026):
        con = duckdb.connect(config={
            "memory_limit": "1400MB", "threads": 2,
            "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/v3{pos}{yr}"})
        try:
            for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                      "PRAGMA max_temp_directory_size='3GB'"):
                con.execute(s)
            con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
            con.execute(f"ATTACH '{ops.as_posix()}' AS o (READ_ONLY)")
            con.execute('CREATE OR REPLACE TEMP TABLE pos AS SELECT NFL_player_id pid, '
                        'MAX(UPPER(TRIM(position))) p FROM o.nfl_historical.nfl_player_stats_all '
                        f'WHERE "year"={yr} AND NFL_player_id IS NOT NULL '
                        "AND position NOT LIKE '%,%' GROUP BY 1")
            con.execute(f"""CREATE OR REPLACE TEMP TABLE cap AS
              SELECT c.db_name, s.num_teams, c.ros, c.st
              FROM (SELECT db_name, AVG(nros) ros, AVG(nst) st FROM
                     (SELECT pf.db_name, pf.week, COUNT(*) nros,
                             SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END) nst
                      FROM l.public.player_fantasy pf JOIN pos ON pos.pid=pf.NFL_player_id
                      WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17 AND pos.p='{pos}'
                      GROUP BY 1,2) GROUP BY 1) c
              JOIN l.public.league_settings s ON s.db_name=c.db_name AND s.year={yr}
              WHERE s.num_teams BETWEEN 4 AND 24 AND {lane_sql(pos, lane)}""")
            n = con.execute("SELECT COUNT(*) FROM cap").fetchone()[0]
            if n < 200:
                continue
            for stat, col in (("rostered", "ros"), ("started", "st")):
                # r = MEASURED spots per team, taken on the mode team count so a skewed
                # league-size mix cannot drag it
                r = con.execute(f"""SELECT MEDIAN({col}/num_teams) FROM cap
                                    WHERE num_teams BETWEEN 8 AND 14""").fetchone()[0]
                if not r or r <= 0:
                    continue
                cuts = tuple(round(m * r, 1) for m in MID)
                # LANDING TEST: does a literal N-team league land in the N tier?
                land = con.execute(f"""
                  SELECT SUM(CASE WHEN
                      (num_teams<=8  AND {col} <  {cuts[0]}) OR
                      (num_teams=10  AND {col} >= {cuts[0]} AND {col} < {cuts[1]}) OR
                      (num_teams=12  AND {col} >= {cuts[1]} AND {col} < {cuts[2]}) OR
                      (num_teams>=14 AND {col} >= {cuts[2]})
                    THEN 1 ELSE 0 END)::DOUBLE / COUNT(*)
                  FROM cap WHERE num_teams IN (8,10,12,14)""").fetchone()[0]
                out.append({"position": pos, "lane": lane or "-", "stat": stat, "year": yr,
                            "leagues": n, "per_team": round(r, 3),
                            "c1": cuts[0], "c2": cuts[1], "c3": cuts[2],
                            "landing": round(100 * land, 0)})
        finally:
            con.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--positions", default="QB,RB,WR,TE,K,DEF,DL,LB,DB")
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for pos in a.positions.split(","):
        for lane in LANES.get(pos, (None,)):
            print(f"  {pos}{'/' + lane if lane else ''} ...", flush=True)
            rows.extend(run(a.snapshot, a.ops, pos, lane))
    d = pd.DataFrame(rows)
    d.to_parquet(a.out, index=False)
    pd.set_option("display.width", 220)
    g = (d.groupby(["position", "lane", "stat"])
           .agg(years=("year", "size"), leagues=("leagues", "median"),
                per_team=("per_team", "median"), c1=("c1", "median"), c2=("c2", "median"),
                c3=("c3", "median"), landing=("landing", "median"),
                drift=("c2", lambda s: round(100 * (s.max() - s.min()) / s.mean())))
           .round(2))
    def _verdict(r):
        stat = r.Index[2]
        if r.drift > 20:
            return f"PROVISIONAL (drift {r.drift:.0f}%)"
        if stat == "started" and r.landing < 70:
            return f"PROVISIONAL (landing {r.landing:.0f}%, cuts misplaced)"
        return "LOCK"
    g["verdict"] = [_verdict(r) for r in g.itertuples()]
    print("\nMIDPOINT CUTS, landing test, 2021-2025 pooled\n")
    print(g.to_string())


if __name__ == "__main__":
    main()
