#!/usr/bin/env python3
"""Fleet backtest -> draft-predictability registry.

Leave-future-out: for every manager-year with >=2 prior drafts, predict from
prior years only and check the held-out year. For each behavior we report the
base rate, the next-year rate GIVEN the manager was flagged (prior rate >=0.5),
the out-of-sample lift, and the point-biserial correlation. Behaviors that don't
clear a real lift get dropped; the rest get a reliability tier that drives the
confidence word in the dossier. Writes docs/draft-predictability.json.
"""
import json
from pathlib import Path

import pandas as pd
from multi_league.core.readers.fly_reader import FlyReader

reader = FlyReader()

SQL = """
WITH picks AS (
  SELECT db_name, year, COALESCE(franchise_id, manager) AS mkey,
         UPPER(position) AS pos, pick, round, COALESCE(cost,0) AS cost
  FROM public.draft
  WHERE position IS NOT NULL AND year IS NOT NULL
    AND COALESCE(is_keeper,0)=0 AND pick IS NOT NULL AND manager IS NOT NULL
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY db_name, year, mkey ORDER BY pick) AS seq
  FROM picks
)
SELECT db_name, year, mkey,
  MAX(CASE WHEN seq=1 THEN pos END) AS opening,
  SUM(CASE WHEN seq<=5 AND pos='RB' THEN 1 ELSE 0 END) AS rb5,
  SUM(CASE WHEN seq<=3 AND pos='WR' THEN 1 ELSE 0 END) AS wr3,
  MIN(CASE WHEN pos='QB' THEN round END) AS qb_rd,
  MIN(CASE WHEN pos='TE' THEN round END) AS te_rd,
  SUM(cost) AS total_cost, MAX(cost) AS top_cost,
  AVG(CASE WHEN cost>0 THEN 1.0 ELSE 0.0 END) AS auction_share
FROM ranked
GROUP BY db_name, year, mkey
"""

print("querying fleet draft features...", flush=True)
df = pd.DataFrame(reader.query(SQL, database="___leagues"))
df["year"] = df["year"].astype(int)
for c in ["rb5", "wr3", "total_cost", "top_cost"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
print(f"rows: {len(df):,}  leagues: {df['db_name'].nunique():,}", flush=True)

auction = df["auction_share"].fillna(0) >= 0.25
df["snake"] = (~auction).astype(int)
df["top_share"] = df["top_cost"] / df["total_cost"].replace(0, float("nan"))

BEHAVIORS = {
    # key: (predicate series, snake_only)
    "zero_rb":        ((df["rb5"] == 0) & df["snake"].astype(bool), True),
    "robust_rb":      ((df["rb5"] >= 2) & df["snake"].astype(bool), True),
    "hero_rb":        ((df["opening"] == "RB") & (df["rb5"] == 1) & df["snake"].astype(bool), True),
    "wr_heavy_start": ((df["wr3"] >= 2) & df["snake"].astype(bool), True),
    "early_qb":       (df["qb_rd"].fillna(99) <= 3, False),
    "late_qb":        (df["qb_rd"].fillna(99) >= 8, False),
    "te_early":       (df["te_rd"].fillna(99) <= 4, False),
    "opens_rb":       ((df["opening"] == "RB") & df["snake"].astype(bool), True),
    "opens_wr":       ((df["opening"] == "WR") & df["snake"].astype(bool), True),
    "opens_qb":       ((df["opening"] == "QB") & df["snake"].astype(bool), True),
    "opens_te":       ((df["opening"] == "TE") & df["snake"].astype(bool), True),
    "stars_and_scrubs": ((df["top_share"] >= 0.35) & auction, False),
}


def backtest(flag_col: str, snake_only: bool):
    d = df.copy()
    d["flag"] = df[flag_col] if isinstance(flag_col, str) else flag_col
    if snake_only:
        d = d[d["snake"] == 1]
    d = d.sort_values(["db_name", "mkey", "year"])
    recs = []
    for _, g in d.groupby(["db_name", "mkey"]):
        vals = list(g["flag"].astype(int))
        for i in range(2, len(vals)):
            recs.append((sum(vals[:i]) / i, vals[i]))
    if not recs:
        return None
    r = pd.DataFrame(recs, columns=["prior", "actual"])
    base = r["actual"].mean()
    flagged = r[r["prior"] >= 0.5]["actual"]
    p_flag = flagged.mean() if len(flagged) else float("nan")
    return {
        "n": int(len(r)), "base_rate": round(float(base), 4),
        "p_next_if_flagged": round(float(p_flag), 4), "n_flagged": int(len(flagged)),
        "lift": round(float(p_flag / base), 3) if base else None,
        "corr": round(float(r["prior"].corr(r["actual"])), 3),
    }


def tier(res):
    """Reliability tier from out-of-sample lift + correlation."""
    if not res or res["lift"] is None or res["n_flagged"] < 40:
        return "insufficient"
    lift, corr = res["lift"], res["corr"]
    if lift >= 1.8 and corr >= 0.15:
        return "strong"
    if lift >= 1.3 and corr >= 0.10:
        return "lean"
    if lift >= 1.12:
        return "slight"
    return "none"


results = {}
print(f"\n{'behavior':16} {'n':>6} {'base':>6} {'if_flag':>7} {'lift':>5} {'corr':>5}  tier")
for key, (pred, snake) in BEHAVIORS.items():
    res = backtest(pred, snake)
    if not res:
        continue
    t = tier(res)
    res["tier"] = t
    results[key] = res
    print(f"{key:16} {res['n']:>6} {res['base_rate']:>6.3f} {res['p_next_if_flagged']:>7.3f} "
          f"{res['lift']:>5.2f} {res['corr']:>5.2f}  {t}")

out = Path(r"D:/yahoo_oauth/docs/draft-predictability.json")
out.write_text(json.dumps({
    "method": "leave-future-out; predict from years<Y, test year Y; >=2 prior drafts",
    "fleet_manager_years": int(len(df)), "leagues": int(df["db_name"].nunique()),
    "behaviors": results,
}, indent=2), encoding="utf-8")
print(f"\nwrote {out}")
