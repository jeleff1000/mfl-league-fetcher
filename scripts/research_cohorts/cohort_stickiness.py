"""Measure league stickiness properly: the intraclass correlation, per stat and position.

Two players can share a 50% season rate and need wildly different sample sizes:

  * 50% of leagues roster him all 17 weeks, 50% never  -> each league is ONE observation.
    The 17 weeks carry no extra information. Maximally sticky.
  * every league rosters him half the weeks             -> each league carries ~17 usable
    observations. Not sticky at all.

rho = Var(per-league season rate) / (p * (1 - p)) separates them: 1 is all-or-nothing,
0 is every league sitting at the same rate. It is what converts a weekly sample size into
an honest season one, via  N_season = N_weekly * (1 + (m-1) * rho) / m.

Measured ONLY on the contested band. A player at 100% everywhere has p(1-p) = 0 and yields
0/0 -- he cannot tell you how many leagues you need, because any number gives the same
answer. Earlier I inferred rho by dividing two aggregate sample sizes, which averages over
every player and every structure and cannot see this distinction at all.
"""
from __future__ import annotations

import argparse
import glob
import math
from pathlib import Path

import numpy as np
import pandas as pd

# stat -> (per-league season value, is it a rate bounded 0..1?)
STATS = {
    "roster_pct":  ("rostered_wks / wks", True),
    "start_pct":   ("starts / wks",       True),
    "playoff_pct": ("po",                 True),
    "champ_pct":   ("champ",              True),
}
MIN_LEAGUES = 25          # leagues a player needs in a cohort to decompose his variance
BAND = (0.15, 0.85)       # the contested band; outside it p(1-p) is too small to divide by


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--glob", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    files = [p for p in glob.glob(a.glob) if "pop" not in p and "samp" not in p]
    c = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    c = c[c.position.isin(["QB", "RB", "WR", "TE", "K", "DEF"])].copy()

    # Best ball sets the lineup automatically, so start% is a constant there and any rho
    # measured on it is an artifact of the format rather than manager behaviour. The
    # extracts should already exclude it; verified here rather than assumed.
    import duckdb
    con = duckdb.connect(config={"memory_limit": "600MB", "threads": 2})
    con.execute("SET enable_progress_bar=false")
    con.execute(f"ATTACH '{a.snapshot}' AS lake (READ_ONLY)")
    bb = con.execute("SELECT DISTINCT db_name, year FROM lake.public.league_settings "
                     "WHERE COALESCE(sleeper_best_ball, FALSE)").fetchdf()
    con.close()
    before = len(c)
    c = c.merge(bb.assign(_bb=1), on=["db_name", "year"], how="left")
    c = c[c._bb.isna()].drop(columns="_bb")
    print(f"best-ball rows removed: {before - len(c):,} of {before:,}")
    c["pid"] = c.player.astype(str) + "@" + c.year.astype(str)
    c["roster_pct"] = 1.0                       # a row exists only where rostered
    c["start_pct"] = (c.starts / c.wks.replace(0, np.nan)).clip(upper=1)
    c["playoff_pct"] = c.po.astype(float)
    c["champ_pct"] = c.champ.astype(float)

    # rho must be WITHIN a cohort. Pooling a player's leagues across cohorts folds the
    # cohort differences into the variance, which inflates rho, understates the effective
    # weeks and overstates the season sample size. The whole point of the cohort grid is
    # that those differences are signal, not noise.
    con2 = duckdb.connect(config={"memory_limit": "800MB", "threads": 2})
    con2.execute("SET enable_progress_bar=false")
    con2.execute(f"ATTACH '{a.snapshot}' AS lake (READ_ONLY)")
    import sys; sys.path.insert(0, ".")
    from cohort_format_sql import cohort_league_settings_sql
    con2.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                 cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public."))
    ck = con2.execute("""
        SELECT db_name, year,
               concat_ws('|', teams_RB, roster, ppr, td, bracket, lineup_mode) AS cohort
        FROM fmt WHERE roster IS NOT NULL AND ppr IS NOT NULL AND td IS NOT NULL
          AND bracket IS NOT NULL""").fetchdf()
    con2.close()
    c = c.merge(ck, on=["db_name", "year"], how="inner")
    c = c[~c.cohort.str.startswith("ALL")]
    # dense integer cohort id -- never group on the string (P1)
    c["cid"] = pd.factorize(c.cohort)[0]
    print(f"joined cohorts: {c.cid.nunique()} distinct, {len(c):,} rows")

    rows = []
    for stat in STATS:
        for pos, g in c.groupby("position"):
            # per (player, COHORT): his leagues inside one cohort only
            v = g.groupby(["pid", "cid"])[stat]
            agg = v.agg(["mean", "var", "size"]).reset_index()
            agg = agg[(agg["size"] >= MIN_LEAGUES) & agg["var"].notna()]
            agg = agg[agg["mean"].between(*BAND)]     # contested band only
            if len(agg) < 10:
                continue
            # rho = between-league variance / the Bernoulli ceiling p(1-p)
            agg["rho"] = (agg["var"] / (agg["mean"] * (1 - agg["mean"]))).clip(0, 1)
            for m in (17,):
                disc = (1 + (m - 1) * agg["rho"]) / m   # season N = weekly N * disc
                rows.append({
                    "stat": stat, "pos": pos, "players": len(agg),
                    "rho_p50": float(agg["rho"].median()),
                    "rho_p90": float(agg["rho"].quantile(0.90)),
                    "season_discount_p50": float(disc.median()),
                    "effective_weeks_p50": float(1 / disc.median()),
                })
    r = pd.DataFrame(rows)
    r.to_parquet(a.out, index=False)
    pd.set_option("display.width", 250)
    print("rho = 1 -> all-or-nothing (a league is ONE sample). rho = 0 -> weeks are samples.")
    print("effective_weeks = how many of the 17 weeks a season actually earns.\n")
    for stat, d in r.groupby("stat"):
        print(f"--- {stat} ---")
        print(d[["pos", "players", "rho_p50", "rho_p90", "season_discount_p50",
                 "effective_weeks_p50"]].round(3).to_string(index=False))
        print()


if __name__ == "__main__":
    main()
