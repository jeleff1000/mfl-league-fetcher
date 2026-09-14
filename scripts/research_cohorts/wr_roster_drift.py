"""WR roster% drift vs precision. Executes docs/runbooks/wr-roster-drift-measurement-spec-2026-08-02.md

Take one WR, one home cohort, one week. Walk him up the collapse ladder and record, at each
level, how many leagues must be sampled to pin his rate, and how far that rate DRIFTS from
his home-cohort value. Drift is the bias, measured -- it replaces every estimated bias in
this program.

    L0  fmtx|tier|roster|ppr|td     home cohort
    L1  fmtx|tier|roster|ppr        drop td
    L2  fmtx|tier|ppr               drop roster
    L3  fmtx|tier                   drop ppr
    L4  fmtx                        drop tier
    L5  (whole week)                drop format

Format is carried through L4 because A2 says it never pools; L5 exists only to show what
ignoring A2 would cost.

Binding rules from the spec, each traceable to a defect already made:
  M1 intervals centred on the player's OWN rate at that level, never 50%
  M2 panel FIXED at L0 (home-cohort rate in [0.40,0.60]) and tracked upward, NEVER re-selected
  M3 denominator is the eligible LIVE league set; absence inside a live league is a real zero
  M4 every printed number carries its filter
  M5 drift (centre offset) and MoE (interval width) reported separately, never silently added
  M6 the row table is written BEFORE any summary is printed

OOM discipline (P1-P6): one year per pass, per-run temp dir, bounded memory, and ONE
aggregation over player_fantasy per year -- every level is then a sum over that small table
rather than a fresh scan. Never group the corpus by db_name.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import hypergeom

from cohort_format_sql import cohort_league_settings_sql

C1, C2, C3 = 54.2, 79.4, 134.5          # R22/R23 observed-capacity cutoffs
AXES = ["fmtx", "tier", "ppr", "roster", "td"]
LEVELS = [                               # (name, axes that still define the pool)
    ("L0_home",      ["fmtx", "tier", "ppr", "roster", "td"]),
    ("L1_drop_td",   ["fmtx", "tier", "ppr", "roster"]),
    ("L2_drop_rost", ["fmtx", "tier", "ppr"]),
    ("L3_drop_ppr",  ["fmtx", "tier"]),
    ("L4_drop_tier", ["fmtx"]),
    ("L5_all",       []),
]
BAND = (0.40, 0.60)
TOLS = [("5", 0.05), ("3", 0.03)]
CONFS = [("85", 0.85), ("95", 0.95)]


def year_tables(snapshot: Path, ops: Path, yr: int):
    """Return (league map with cohort axes, per week x player x cohort roster counts)."""
    con = duckdb.connect(config={
        "memory_limit": "1400MB", "threads": 2,
        "temp_directory": f"C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp/drift{yr}"})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  # P6: C: runs ~11GB free. The cap must sit FAR below that -- an 8GB cap
                  # against 11GB free is the configuration that took the host down 2026-07-21.
                  "PRAGMA max_temp_directory_size='3GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS l (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS o (READ_ONLY)")
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True).replace("public.", "l.public."))
        con.execute(
            'CREATE OR REPLACE TEMP TABLE wr AS SELECT NFL_player_id pid, MAX(player) pname '
            'FROM o.nfl_historical.nfl_player_stats_all WHERE "year"=' + str(yr) +
            " AND NFL_player_id IS NOT NULL AND UPPER(TRIM(position))='WR' "
            "AND position NOT LIKE '%,%' GROUP BY 1")
        # M3: live leagues only -- a league-year with settings but no player rows is not eligible
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lg AS
          SELECT c.db_name,
            CASE WHEN c.wr_spots<{C1} THEN '08tm' WHEN c.wr_spots<{C2} THEN '10tm'
                 WHEN c.wr_spots<{C3} THEN '12tm' ELSE '14tm' END tier,
            f.roster, f.ppr, f.td,
            CASE WHEN f.lineup_mode='best_ball' THEN 'bestball'
                 WHEN f.league_type='dynasty' THEN 'dynasty' ELSE 'redraft' END fmtx
          FROM (SELECT db_name, AVG(nwr) wr_spots FROM
                  (SELECT pf.db_name, pf.week, COUNT(*) nwr FROM l.public.player_fantasy pf
                   JOIN wr ON wr.pid=pf.NFL_player_id
                   WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17 GROUP BY 1,2)
                GROUP BY 1) c
          JOIN fmt f ON f.db_name=c.db_name AND f.year={yr}
          WHERE f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL""")
        pools = con.execute(
            "SELECT fmtx,tier,ppr,roster,td, COUNT(*) n FROM lg GROUP BY ALL").fetchdf()
        # ONE aggregation over player_fantasy for the whole year (P1)
        counts = con.execute(f"""
          SELECT pf.week, pf.NFL_player_id pid, w.pname,
                 g.fmtx, g.tier, g.ppr, g.roster, g.td, COUNT(*) k
          FROM l.public.player_fantasy pf
          JOIN lg g ON g.db_name=pf.db_name
          JOIN wr w ON w.pid=pf.NFL_player_id
          WHERE pf.year={yr} AND pf.week BETWEEN 1 AND 17
          GROUP BY ALL""").fetchdf()
        return pools, counts
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--first-year", type=int, default=2021)
    ap.add_argument("--last-year", type=int, default=2025)
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for yr in range(a.first_year, a.last_year + 1):
        pools, counts = year_tables(a.snapshot, a.ops, yr)
        # M2: the panel is fixed by the HOME-cohort rate, computed once
        home_n = pools.groupby(AXES, as_index=False).n.sum()
        home = counts.merge(home_n, on=AXES, how="left")
        home["rate"] = home.k / home.n
        panel = home[home.rate.between(*BAND)][["week", "pid", "pname"] + AXES].copy()
        panel["home_rate"] = home.loc[home.rate.between(*BAND), "rate"].values
        rows = []
        for lname, keep in LEVELS:
            if keep:
                pn = pools.groupby(keep, as_index=False).n.sum().rename(columns={"n": "N"})
                kk = counts.groupby(["week", "pid"] + keep, as_index=False).k.sum()
                m = panel.merge(kk, on=["week", "pid"] + keep, how="left") \
                         .merge(pn, on=keep, how="left")
                m["pool_key"] = m[keep].astype(str).agg("|".join, axis=1)
            else:
                pn = int(pools.n.sum())
                kk = counts.groupby(["week", "pid"], as_index=False).k.sum()
                m = panel.merge(kk, on=["week", "pid"], how="left")
                m["N"] = pn
                m["pool_key"] = "ALL"
            m["k"] = m.k.fillna(0)
            m["level"] = lname
            rows.append(m)
        d = pd.concat(rows, ignore_index=True)
        d["year"] = yr
        d["rate"] = d.k / d.N
        d["drift_pts"] = (100 * (d.rate - d.home_rate)).round(3)
        frames.append(d)
        print(f"  {yr}: panel {len(panel):,} (player,week) at {BAND[0]:.0%}-{BAND[1]:.0%}, "
              f"{len(d):,} level-rows")

    d = pd.concat(frames, ignore_index=True)

    # exact hypergeometric, cached by (N,K,tol,conf) -- M1: centred on HIS rate at this level
    def need(N, K, tol, conf):
        N, K = int(N), int(K)
        if N <= 1 or K <= 0:
            return np.nan
        for n in list(range(5, 100, 5)) + list(range(100, 600, 20)) + list(range(600, 6001, 50)):
            if n > N:
                return np.nan
            lo = int(np.ceil((K / N - tol) * n))
            hi = int(np.floor((K / N + tol) * n))
            if hypergeom.cdf(hi, N, K, n) - hypergeom.cdf(lo - 1, N, K, n) >= conf:
                return n
        return np.nan

    uniq = d[["N", "k"]].drop_duplicates()
    print(f"\nsolving {len(uniq):,} unique (N,K) pairs x {len(TOLS)*len(CONFS)} settings...")
    for tname, tol in TOLS:
        for cname, conf in CONFS:
            cache = {(r.N, r.k): need(r.N, r.k, tol, conf) for r in uniq.itertuples()}
            d[f"n_{tname}_{cname}"] = [cache[(r.N, r.k)] for r in d.itertuples()]

    # M5: does the pooled interval still cover his home rate? (separate from drift)
    z = {"85": 1.4395, "95": 1.9600}
    for cname in ("85", "95"):
        hw = z[cname] * np.sqrt(d.rate * (1 - d.rate) / d.N.clip(lower=1))
        d[f"covers_L0_{cname}"] = (d.rate - hw <= d.home_rate) & (d.home_rate <= d.rate + hw)

    d["home_cohort"] = d[AXES].astype(str).agg("|".join, axis=1)
    cols = ["year", "week", "pid", "pname", "home_cohort", "level", "pool_key",
            "N", "k", "rate", "home_rate", "drift_pts",
            "n_5_85", "n_5_95", "n_3_85", "n_3_95", "covers_L0_85", "covers_L0_95"]
    # M6: write BEFORE any summary
    d[cols].to_parquet(a.out, index=False)
    print(f"\nWROTE {len(d):,} rows -> {a.out}")


if __name__ == "__main__":
    main()
