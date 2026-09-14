#!/usr/bin/env python3
"""Year-by-year: is route depth getting worse for WR fantasy?

For each season (2009-2024, air-yards era, WR targets>=50):
  * Spearman(ADOT, fantasy PPG) in 0ppr / half / ppr  -> does deeper = more
    points, and is that relationship trending down?
  * deep vs short tercile (by within-year ADOT) mean PPG, and the deep-short gap.
  * per-target efficiency by depth (points per target).
Then a linear trend (slope/decade) on each series.

    python scripts/wr_depth_trend.py [--min-targets 50]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
import wr_depth_common as C

pd.set_option("display.width", 220)


def slope_per_decade(years, vals):
    m = np.isfinite(years) & np.isfinite(vals)
    if m.sum() < 5:
        return np.nan
    b = np.polyfit(years[m], vals[m], 1)[0]
    return b * 10


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-targets", type=int, default=50)
    args = ap.parse_args()

    df = C.load_wr()
    df = df[(df["targets"] >= args.min_targets) & df["year"].between(2009, 2025)
            & df["adot"].notna() & df["ppg_season_4pt_half"].notna()].copy()
    print(f"WR seasons (targets>={args.min_targets}, 2009-2024): {len(df):,}\n")

    rows = []
    for yr, g in df.groupby("year"):
        g = g.copy()
        # within-year depth terciles
        g["depth_tier"] = pd.qcut(g["adot"], 3, labels=["short", "interm", "deep"])
        gap = (g.loc[g.depth_tier == "deep", "ppg_season_4pt_half"].mean()
               - g.loc[g.depth_tier == "short", "ppg_season_4pt_half"].mean())
        # points per target by depth tier (efficiency)
        ppt_deep = (g.loc[g.depth_tier == "deep", "fpts_4pt_half"].sum()
                    / g.loc[g.depth_tier == "deep", "targets"].sum())
        ppt_short = (g.loc[g.depth_tier == "short", "fpts_4pt_half"].sum()
                     / g.loc[g.depth_tier == "short", "targets"].sum())
        rows.append({
            "year": yr, "n": len(g), "adot": round(g.adot.mean(), 2),
            "r_adot_0ppr": round(C.spearman(g.adot, g.ppg_season_4pt_0ppr), 2),
            "r_adot_half": round(C.spearman(g.adot, g.ppg_season_4pt_half), 2),
            "r_adot_ppr": round(C.spearman(g.adot, g.ppg_season_4pt_ppr), 2),
            "r_ayshare_half": round(C.spearman(g.air_yards_share, g.ppg_season_4pt_half), 2),
            "r_yac_half": round(C.spearman(g.yac_share, g.ppg_season_4pt_half), 2),
            "deep-short_half": round(gap, 2),
            "ppt_deep": round(ppt_deep, 3), "ppt_short": round(ppt_short, 3),
        })
    t = pd.DataFrame(rows)
    print("=" * 150)
    print("YEAR-BY-YEAR: route depth vs WR fantasy  (r = Spearman with PPG; deep-short = "
          "deep-tercile minus short-tercile mean half-PPR PPG)")
    print("=" * 150)
    print(t.to_string(index=False))

    print("\n" + "=" * 80)
    print("TREND (linear slope per decade, 2009-2024):")
    print("=" * 80)
    yrs = t["year"].to_numpy(float)
    for col in ["adot", "r_adot_0ppr", "r_adot_half", "r_adot_ppr",
                "r_ayshare_half", "r_yac_half", "deep-short_half",
                "ppt_deep", "ppt_short"]:
        s = slope_per_decade(yrs, t[col].to_numpy(float))
        early = t[t.year <= 2012][col].mean()
        late = t[t.year >= 2021][col].mean()
        print(f"  {col:18} slope/decade {s:+.3f}   |  2009-12 avg {early:+.2f}  ->  2021-24 avg {late:+.2f}")

    t.to_json(C.SCRATCH / "wr_depth_trend.json", orient="records", indent=2)
    print(f"\nwrote wr_depth_trend.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
