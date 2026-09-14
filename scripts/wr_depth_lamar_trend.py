#!/usr/bin/env python3
"""Deep-threat value trend with TIME as the axis (years = rows) and value in
LAMAR/G (league-adjusted, replacement-relative) alongside raw PPG.

LAMAR/G scales with the league: raw PPG drifts up with passing inflation, but
LAMAR/G holds replacement level constant across eras -- the honest way to ask
"is a deep threat worth more or less *relative to the league* over time?"

Rows = seasons 2009-2025 (WR, targets>=50, corrected 2025). Columns compare the
ADOT->value relationship and deep-vs-short value gap in PPG vs LAMAR/G.

    python scripts/wr_depth_lamar_trend.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import wr_depth_common as C
from multi_league.core.readers.fly_reader import FlyReader

pd.set_option("display.width", 240)

LAMAR = {"std": "lamar_ppg_12t_flx_std_4pt", "half": "lamar_ppg_12t_flx_half_4pt",
         "ppr": "lamar_ppg_12t_flx_ppr_4pt"}


def main() -> int:
    wr = C.load_wr()   # corrected 2025 ADOT
    # pull LAMAR/G (12-team flex) for WRs and join
    cols = ", ".join(f'"{c}"' for c in ["NFL_player_id", "year"] + list(LAMAR.values()))
    lam = FlyReader().query_df(
        f"SELECT {cols} FROM nfl_historical.player_nfl_season_all WHERE position='WR' "
        f"AND year BETWEEN 2009 AND 2025", "___ops")
    for c in LAMAR.values():
        lam[c] = pd.to_numeric(lam[c], errors="coerce")
    wr = wr.merge(lam, on=["NFL_player_id", "year"], how="left")

    df = wr[(wr.targets >= 50) & wr.year.between(2009, 2025) & wr.adot.notna()
            & wr[LAMAR["half"]].notna()].copy()

    rows = []
    for yr, g in df.groupby("year"):
        g = g.copy()
        g["tier"] = pd.qcut(g["adot"], 3, labels=["short", "interm", "deep"])
        def gap(col):
            return (g.loc[g.tier == "deep", col].mean() - g.loc[g.tier == "short", col].mean())
        H = LAMAR["half"]
        deep, short = g[g.tier == "deep"], g[g.tier == "short"]
        rows.append({
            "year": int(yr), "n": len(g), "ADOT": round(g.adot.mean(), 1),
            # deep-tercile LEVEL: raw PPG (inflates) vs LAMAR/G (league-adjusted)
            "deep_PPG": round(deep.ppg_season_4pt_half.mean(), 1),
            "deep_LAMARg": round(deep[H].mean(), 2),
            "short_PPG": round(short.ppg_season_4pt_half.mean(), 1),
            "short_LAMARg": round(short[H].mean(), 2),
            # deep-minus-short (identical in PPG vs LAMAR/G within-year, by construction)
            "deepShort_LAMARg": round(gap(H), 2),
            # does depth predict value this year? (same rank in PPG or LAMAR/G)
            "r_ADOT_LAMARg": round(C.spearman(g.adot, g[H]), 2),
            "r_ADOT_LAMARg_ppr": round(C.spearman(g.adot, g[LAMAR["ppr"]]), 2),
            "r_ADOT_LAMARg_std": round(C.spearman(g.adot, g[LAMAR["std"]]), 2),
        })
    t = pd.DataFrame(rows)
    print("=" * 150)
    print("DEEP-THREAT VALUE BY SEASON — rows=years; PPG vs LAMAR/G (league-adjusted).  "
          "r_* = Spearman(ADOT, value); deepShort = deep-tercile minus short-tercile mean")
    print("=" * 150)
    print(t.to_string(index=False))

    print("\nSLOPE / DECADE (2009-2025) and era averages:")
    yrs = t["year"].to_numpy(float)
    for col in ["ADOT", "deep_PPG", "deep_LAMARg", "short_PPG", "short_LAMARg",
                "deepShort_LAMARg", "r_ADOT_LAMARg", "r_ADOT_LAMARg_ppr", "r_ADOT_LAMARg_std"]:
        v = t[col].to_numpy(float); m = np.isfinite(v)
        s = np.polyfit(yrs[m], v[m], 1)[0] * 10
        early = t[t.year <= 2012][col].mean(); late = t[t.year >= 2022][col].mean()
        print(f"  {col:20} slope/dec {s:+.2f}   2009-12 {early:+.2f} -> 2022-25 {late:+.2f}")

    t.to_csv(r"D:/yahoo_oauth/docs/runbooks/wr-depth-lamar-trend-by-year.csv",
             index=False, encoding="utf-8-sig")
    print("\nwrote docs/runbooks/wr-depth-lamar-trend-by-year.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
