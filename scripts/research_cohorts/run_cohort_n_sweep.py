"""THE N SWEEP. Executes docs/runbooks/cohort-n-sweep-plan-2026-08-04.md. 216 boxes.

9 positions x 8 (stat, grain) x 3 cohort ranks. Every box is hit or written BLOCKED with a
reason; none is silently skipped or narrowed.

Per box:
  1. the Nth-thickest fully-specified 7-dimension cohort for that position
  2. the 35 player-weeks (or player-seasons) in it CLOSEST TO p -- not a band, which breaks
     at low p where +/-5 points around .10 spans half to 1.5x the value
  3. every comparison is the SAME PLAYER against himself, one cell versus another
  4. per dimension, over all 64 starting subsets: the move it makes, the precision it costs,
     and min_n_child = (z*sd/move)^2 -- leagues the child cell needs before it pays
  5. with everything pooled, leagues needed at +/-5 @85%, +/-5 @95%, +/-3 @85%, +/-3 @95%

p is 0.50 for roster/start/win. For playoff% and champ% it comes from the LITERAL team count
(playoff_teams/num_teams and 1/num_teams), so p varies inside a cell and the cell's reference
p is the mean over its leagues -- the derived-capacity `teams` axis cannot supply it, since a
20-team league can sit in the 12tm tier.

WEEKLY spread is forced: a league's weekly value is 0/1, so sd = sqrt(p(1-p)). SEASON
roster/start/win are shares, whose spread must be MEASURED separately for each player-unit
from that player's own league values. A panel-wide SD is never used. Season playoff/champ
are 0/1 per league, so they use sqrt(p(1-p)) again.

RESUMABLE: one row group per box, appended as it finishes. A rerun reads what is done and
skips it.
"""
from __future__ import annotations

import argparse
import itertools
from pathlib import Path
from statistics import NormalDist

import duckdb
import numpy as np
import pandas as pd

import position_slots_contract as PS
from cohort_format_sql import cohort_league_settings_sql

AXES = ("teams", "roster", "ppr", "td", "bracket", "lineup_mode", "league_type")
_EXTRAS = ("COALESCE(s.roster_K, 0) AS k_slots", "COALESCE(s.roster_DEF, 0) AS def_slots",
           "s.num_teams", "s.playoff_teams")
PANEL_N = 35
TARGETS = {"pm5_85": (.05, .85), "pm5_95": (.05, .95),
           "pm3_85": (.03, .85), "pm3_95": (.03, .95)}
# (stat, grain) -> the per-league value, and whether the grain exists
COMBOS = [("roster", "weekly"), ("roster", "season"), ("start", "weekly"), ("start", "season"),
          ("win", "weekly"), ("win", "season"), ("playoff", "season"), ("champ", "season")]


def value_sql(stat: str, grain: str) -> str:
    """Per (league, player[, week]) value. Weekly is 0/1; season is a share or a 0/1 flag."""
    if grain == "weekly":
        return {"roster": "MAX(CAST(pf.is_rostered AS INT))::DOUBLE",
                "start": "MAX(CAST(pf.is_started AS INT))::DOUBLE",
                "win": "MAX(CASE WHEN CAST(pf.is_started AS INT)=1 THEN CAST(pf.win AS INT) END)::DOUBLE"
                }[stat]
    return {"roster": "SUM(CAST(pf.is_rostered AS INT))::DOUBLE / MAX(w.wks)",
            "start": "SUM(CAST(pf.is_started AS INT))::DOUBLE / MAX(w.wks)",
            "win": "SUM(CASE WHEN CAST(pf.is_started AS INT)=1 THEN CAST(pf.win AS INT) ELSE 0 END)::DOUBLE"
                   " / NULLIF(SUM(CAST(pf.is_started AS INT)),0)",
            "playoff": "MAX(CAST(pf.made_playoffs AS INT))::DOUBLE",
            "champ": "MAX(CAST(pf.champion AS INT))::DOUBLE"}[stat]


def load_year(snap: Path, ops: Path, pos: str, year: int, stat: str, grain: str, tmp: Path):
    con = duckdb.connect(config={"memory_limit": "2500MB", "threads": 3,
                                 "temp_directory": str(tmp / f"{pos}{year}")})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='20GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snap.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=year,
                                               extra_select=_EXTRAS)
                    .replace("public.", "lake.public."))
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lgc AS
          SELECT f.db_name, f.teams_{pos} AS teams, f.roster, f.ppr, f.td, f.bracket,
                 f.lineup_mode, f.league_type,
                 f.num_teams, f.playoff_teams
          FROM fmt f
          JOIN (SELECT DISTINCT db_name FROM lake.public.player_fantasy WHERE year={year}) lv
            ON lv.db_name=f.db_name
          WHERE f.year={year} AND f.teams_{pos} <> 'ALL'
            AND f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
            AND f.bracket IS NOT NULL AND f.lineup_mode IS NOT NULL
            AND f.league_type IS NOT NULL
            AND ({PS.position_eligibility_sql(pos, 'f')})""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lw AS
          SELECT db_name, COUNT(DISTINCT week) AS wks FROM lake.public.player_fantasy
          WHERE year={year} AND week BETWEEN 1 AND 17 GROUP BY 1""")
        grp = "pf.NFL_player_id, pf.db_name" + (", pf.week" if grain == "weekly" else "")
        sel = "pf.week," if grain == "weekly" else ""
        val = value_sql(stat, grain)
        vals = con.execute(f"""
          SELECT pf.NFL_player_id AS pid, pf.db_name, {sel} {val} AS v
          FROM lake.public.player_fantasy pf
          JOIN (SELECT NFL_player_id pid FROM ops.nfl_historical.nfl_player_stats_all
                WHERE "year"={year} AND NFL_player_id IS NOT NULL
                  AND UPPER(TRIM(position))='{'DEF' if pos == 'DEF' else pos}'
                  AND position NOT LIKE '%,%' GROUP BY 1) n ON n.pid=pf.NFL_player_id
          JOIN lgc g ON g.db_name=pf.db_name
          JOIN lw  w ON w.db_name=pf.db_name
          WHERE pf.year={year} AND pf.week BETWEEN 1 AND 17
          GROUP BY {grp}""").fetchdf()
        lg = con.execute(f"""SELECT {','.join(AXES)}, COUNT(*) n,
              AVG(1.0/NULLIF(num_teams,0)) p_champ,
              AVG(playoff_teams::DOUBLE/NULLIF(num_teams,0)) p_po,
              MIN(db_name) anydb FROM lgc GROUP BY ALL""").fetchdf()
        keys = con.execute(f"SELECT db_name, {','.join(AXES)} FROM lgc").fetchdf()
        return lg, keys, vals
    finally:
        con.close()


def run_box(lg, keys, vals, pos, stat, grain, rank, year, z_by, out_rows):
    lgv = lg.sort_values("n", ascending=False)
    if len(lgv) < rank:
        out_rows.append({"position": pos, "stat": stat, "grain": grain, "cohort_rank": rank,
                         "year": year, "status": "BLOCKED", "reason": "fewer cohorts than rank"})
        return
    tgt = lgv.iloc[rank - 1]
    TGT = {a: tgt[a] for a in AXES}
    p = {"roster": .5, "start": .5, "win": .5,
         "playoff": float(tgt.p_po), "champ": float(tgt.p_champ)}[stat]

    v = vals.merge(keys, on="db_name")
    unit = ["pid", "week"] if grain == "weekly" else ["pid"]
    LM = {d: (lg[d] == TGT[d]).values for d in AXES}
    VM = {d: (v[d] == TGT[d]).values for d in AXES}
    lgn = lg.n.values
    vv = v.v.fillna(0).values

    def cell(S):
        ml = np.ones(len(lg), bool); mv = np.ones(len(v), bool)
        for d in S:
            ml &= LM[d]; mv &= VM[d]
        n = int(lgn[ml].sum())
        sub = v.loc[mv, unit + ["db_name"]].copy()
        sub["v"] = vv[mv]
        ser = sub.groupby(unit, observed=True)["v"].sum()
        # player_fantasy is sparse: absence means zero, not a missing league.
        # For roster/start/playoff/champ, the denominator is the eligible league
        # population in the cell. Win% is intentionally conditional on starting,
        # so only non-null win observations (started leagues) enter its denominator.
        if stat == "win":
            den = sub.groupby(unit, observed=True)["v"].count()
        else:
            den = pd.Series(float(n), index=ser.index)
        return n, ser, den, sub

    full_n, full_s, full_obs, full_rows = cell(AXES)
    if full_n == 0:
        out_rows.append({"position": pos, "stat": stat, "grain": grain, "cohort_rank": rank,
                         "year": year, "status": "BLOCKED", "reason": "empty cohort"})
        return
    rate = (full_s / full_obs.replace(0, np.nan))
    panel_idx = (rate - p).abs().nsmallest(PANEL_N).index
    if len(panel_idx) == 0:
        out_rows.append({"position": pos, "stat": stat, "grain": grain, "cohort_rank": rank,
                         "year": year, "status": "BLOCKED", "reason": "no players in cohort"})
        return
    cache = {}
    def R(S):
        k = tuple(sorted(S))
        if k not in cache:
            n, ser, obs, sub = cell(S)
            cache[k] = {"n": n,
                        "rate": (ser / obs.replace(0, np.nan)).reindex(panel_idx).to_numpy(dtype=float),
                        "obs": obs.reindex(panel_idx).fillna(0).to_numpy(dtype=float),
                        "rows": sub}
        return cache[k]

    base = {"position": pos, "stat": stat, "grain": grain, "cohort_rank": rank, "year": year,
            "cohort": "|".join(str(TGT[a]) for a in AXES), "cohort_n": full_n,
            "panel_n": len(panel_idx), "p": p, "status": "OK"}
    # step 5 -- pooled minimums
    n0 = R(())["n"]
    row = dict(base, comparison_level="box_summary", dimension="(pooled)", n_parent=n0,
               n_child=n0, moves_pts=0.0)
    sd_pool = float(np.sqrt(p*(1-p)))
    for name, (m, c) in TARGETS.items():
        row[name] = float((NormalDist().inv_cdf(1-(1-c)/2)*sd_pool/m)**2)
    out_rows.append(row)
    # step 4 -- per dimension over all 64 starting subsets
    for d in AXES:
        for r in range(len(AXES)):
            for S in itertools.combinations([x for x in AXES if x != d], r):
                parent, child = R(S), R(tuple(S) + (d,))
                rp, rc = parent["rate"], child["rate"]
                np_, nc = parent["obs"], child["obs"]
                valid = np.isfinite(rp) & np.isfinite(rc) & (np_ > 0) & (nc > 0)
                if not valid.any():
                    continue
                for i, key in enumerate(panel_idx):
                    if not valid[i]:
                        continue
                    pid_key = key[0] if grain == "weekly" else key
                    if grain == "season" and stat in ("roster", "start", "win"):
                        pvals = parent["rows"].loc[parent["rows"]["pid"] == pid_key, "v"].to_numpy(dtype=float)
                        sd_self = float(np.std(pvals, ddof=1)) if len(pvals) > 1 else 0.0
                        if not np.isfinite(sd_self) or sd_self == 0:
                            sd_self = float(np.sqrt(max(0.0, rp[i]*(1-rp[i]))))
                    else:
                        sd_self = float(np.sqrt(max(0.0, p*(1-p))))
                    move = abs(float(rc[i] - rp[i]))
                    cost = z_by * sd_self * (1/np.sqrt(nc[i]) - 1/np.sqrt(np_[i]))
                    rec = dict(base, comparison_level="self", dimension=d,
                               subset="|".join(S) or "(pooled)", player_id=pid_key,
                               unit_week=int(key[1]) if grain == "weekly" else None,
                               parent_rate=float(rp[i]), child_rate=float(rc[i]),
                               n_parent=float(np_[i]), n_child=float(nc[i]), sd_self=sd_self,
                               moves_pts=100*move, costs_pts=100*cost,
                               net_pts=100*(move-cost),
                               min_n_child=float((z_by*sd_self/move)**2) if move > 0 else np.inf)
                    for name, (m, c) in TARGETS.items():
                        zz = NormalDist().inv_cdf(1-(1-c)/2)
                        rec[name] = float((zz*sd_self/m)**2)
                    out_rows.append(rec)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--years", type=int, nargs="+", default=[2023, 2024, 2025])
    ap.add_argument("--positions", nargs="+", default=list(PS.TIER_POSITIONS))
    ap.add_argument("--stats", nargs="+", choices=["roster", "start", "win", "playoff", "champ"],
                    default=["roster", "start", "win", "playoff", "champ"])
    ap.add_argument("--grains", nargs="+", choices=["weekly", "season"],
                    default=["weekly", "season"])
    ap.add_argument("--tmp", type=Path, default=Path("D:/tmp/ddbtmp/sweep"))
    a = ap.parse_args()
    a.tmp.mkdir(parents=True, exist_ok=True)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    z_by = NormalDist().inv_cdf(1 - .15/2)

    done = set()
    if a.out.exists():
        d = pd.read_parquet(a.out)
        done = set(zip(d.position, d.stat, d.grain, d.cohort_rank, d.year))
        print(f"resuming: {len(done)} boxes already done")
    rows = []
    for pos in a.positions:
        for stat, grain in COMBOS:
            if stat not in a.stats or grain not in a.grains:
                continue
            for year in a.years:
                if all((pos, stat, grain, r, year) in done for r in (1, 2, 3)):
                    continue
                try:
                    lg, keys, vals = load_year(a.snapshot, a.ops, pos, year, stat, grain, a.tmp)
                except Exception as e:
                    for r in (1, 2, 3):
                        rows.append({"position": pos, "stat": stat, "grain": grain,
                                     "cohort_rank": r, "year": year, "status": "BLOCKED",
                                     "reason": str(e)[:180]})
                    continue
                for r in (1, 2, 3):
                    if (pos, stat, grain, r, year) in done:
                        continue
                    try:
                        run_box(lg, keys, vals, pos, stat, grain, r, year, z_by, rows)
                    except Exception as e:
                        rows.append({"position": pos, "stat": stat, "grain": grain,
                                     "cohort_rank": r, "year": year, "status": "BLOCKED",
                                     "reason": str(e)[:180]})
                print(f"  {pos:4s} {stat:8s} {grain:6s} {year}  done", flush=True)
                # write through after every (pos, stat, grain, year) so a crash costs one unit
                cur = pd.DataFrame(rows)
                if a.out.exists():
                    cur = pd.concat([pd.read_parquet(a.out), cur], ignore_index=True)
                cur.to_parquet(a.out, index=False)
                rows = []
    print(f"\nSWEEP COMPLETE -> {a.out}")


if __name__ == "__main__":
    main()
