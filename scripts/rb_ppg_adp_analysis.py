#!/usr/bin/env python3
"""Fleet ADP analysis for RBs (Joe's ask).

Two questions:
  (1) SAME-YEAR (as asked): how well does the year-Y preseason ADP predict
      year-Y RB PPG, and does predictability differ by ADP tier
      (top-3 / RB1 / RB2 / RB3 / late; and by draft round)?
  (2) NEXT-YEAR: as a feature for Y+1 PPG, does the (Y+1)-draft ADP -- the
      market's own projection -- beat / add to our persistence + model?

    python scripts/rb_ppg_adp_analysis.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import rb_ppg_common as C


def load_with_adp() -> pd.DataFrame:
    df = C.load_dataset()
    adp = pd.read_parquet(C.SCRATCH / "rb_adp.parquet")
    # same-year ADP: draft in year Y informs year Y
    a_y = adp.rename(columns={"draft_year": "year", "adp_overall": "adp_y",
                              "adp_n": "adp_y_n"})[["NFL_player_id", "year", "adp_y", "adp_y_n"]]
    df = df.merge(a_y, on=["NFL_player_id", "year"], how="left")
    # next-year ADP: draft in year Y+1 is the market projection for Y+1
    a_n = adp.copy()
    a_n["year"] = a_n["draft_year"] - 1
    a_n = a_n.rename(columns={"adp_overall": "adp_next", "adp_n": "adp_next_n"})
    df = df.merge(a_n[["NFL_player_id", "year", "adp_next", "adp_next_n"]],
                  on=["NFL_player_id", "year"], how="left")
    # positional RB ADP rank within year (1 = first RB off the board)
    df["rb_adp_rank"] = df.groupby("year")["adp_y"].rank(method="min")
    return df


def tier_of(rank: float) -> str:
    if not np.isfinite(rank):
        return "undrafted/na"
    if rank <= 3:
        return "1 top-3"
    if rank <= 12:
        return "2 RB1 (4-12)"
    if rank <= 24:
        return "3 RB2 (13-24)"
    if rank <= 36:
        return "4 RB3 (25-36)"
    return "5 late (37+)"


def main() -> int:
    df = C.load_dataset_adp = load_with_adp()
    g6 = df[df["games_played"] >= 6].copy()

    # ---------- (1) SAME-YEAR: ADP_Y vs PPG_Y --------------------------------
    sy = g6[g6["adp_y"].notna() & g6[C.PERSIST].notna()].copy()
    sy = sy[sy["year"] >= 2010]
    print("=" * 78)
    print("(1) SAME-YEAR: preseason ADP (year Y) vs realized year-Y RB PPG (half)")
    print(f"    n={len(sy):,}  years 2010-{int(sy.year.max())}")
    print("=" * 78)
    sp = C.spearman(-sy["adp_y"].to_numpy(), sy[C.PERSIST].to_numpy())
    print(f"Spearman(earlier ADP, higher PPG) overall = {sp:.3f}\n")
    sy["tier"] = sy["rb_adp_rank"].map(tier_of)
    rows = []
    for t, gg in sy.groupby("tier"):
        # within-tier: does ADP still discriminate PPG?
        within = C.spearman(-gg["adp_y"].to_numpy(), gg[C.PERSIST].to_numpy())
        rows.append({
            "ADP tier": t, "n": len(gg),
            "mean_PPG": round(gg[C.PERSIST].mean(), 2),
            "sd_PPG": round(gg[C.PERSIST].std(), 2),
            "p25": round(gg[C.PERSIST].quantile(.25), 2),
            "median": round(gg[C.PERSIST].median(), 2),
            "p75": round(gg[C.PERSIST].quantile(.75), 2),
            "within_tier_spear": round(within, 3) if np.isfinite(within) else None,
            "bust<8ppg%": round((gg[C.PERSIST] < 8).mean() * 100, 0),
        })
    print(pd.DataFrame(rows).sort_values("ADP tier").to_string(index=False))
    print("\n  -> 'does ADP tier matter?' mean PPG should fall monotonically by tier;")
    print("     within_tier_spear shows how much ADP still separates INSIDE a tier.")

    # ---------- (2) NEXT-YEAR: ADP_{Y+1} as a projection of Y+1 PPG ----------
    ny = g6[g6["adp_next"].notna() & df[C.LABEL].notna()].copy()
    ny = ny[ny["year"] >= 2009]
    print("\n" + "=" * 78)
    print("(2) NEXT-YEAR: market (Y+1 draft) ADP as a projection of Y+1 PPG")
    print(f"    n={len(ny):,}  (subset with a Y+1 draft ADP)")
    print("=" * 78)
    y = ny[C.LABEL].to_numpy()
    persist = ny[C.PERSIST].to_numpy()
    adp_next = ny["adp_next"].to_numpy()
    print(f"Spearman(persistence  Y PPG , Y+1 PPG) = {C.spearman(persist, y):.3f}")
    print(f"Spearman(market -ADP_Y+1  , Y+1 PPG) = {C.spearman(-adp_next, y):.3f}")
    # partial: does ADP add beyond persistence, and vice versa?
    def resid(a, b):
        m = np.isfinite(a) & np.isfinite(b)
        s = np.polyfit(b[m], a[m], 1)
        out = np.full_like(a, np.nan); out[m] = a[m] - (s[0]*b[m]+s[1]); return out
    print(f"partial Spearman(market ADP | persistence)     = "
          f"{C.pearson(resid(-adp_next, persist), resid(y, persist)):.3f}")
    print(f"partial Spearman(persistence | market ADP)     = "
          f"{C.pearson(resid(persist, -adp_next), resid(y, -adp_next)):.3f}")
    print("  -> if market-ADP partial >> persistence partial, the draft market")
    print("     already prices in most of what our season stats know (+ more).")

    df.to_parquet(C.SCRATCH / "rb_seasons_adp.parquet", index=False)
    print(f"\nwrote rb_seasons_adp.parquet (dataset + adp_y/adp_next/rb_adp_rank)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
