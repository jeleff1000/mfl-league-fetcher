#!/usr/bin/env python3
"""Stratified predictor table for RB next-year PPG -- the core deliverable.

For each stratum cell report: n, mean Y+1 PPG, how PREDICTABLE it is
(persistence Spearman + market-ADP Spearman), how RELIABLE (residual sd of Y+1
PPG after regression-to-mean = a prediction-interval proxy), and WHICH FEATURES
matter (top partial-over-persistence signals within the cell).

Strata: age band, experience, workload (carries), draft pedigree, current PPG
tier, and next-year ADP tier.

    python scripts/rb_ppg_strata.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import rb_ppg_common as C

pd.set_option("display.width", 200)


def partial_spear(x, p, y):
    """Spearman of x with y after removing persistence p (all pct-normed)."""
    def resid(a, b):
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() < 25 or b[m].std() < 1e-9:
            return None
        s = np.polyfit(b[m], a[m], 1)
        out = np.full_like(a, np.nan, float)
        out[m] = a[m] - (s[0] * b[m] + s[1])
        return out
    rx, ry = resid(x, p), resid(y, p)
    if rx is None or ry is None:
        return np.nan
    return C.pearson(rx, ry)


def top_features(sub, dfp, feats, p_col, k=4):
    y = sub[C.LABEL].to_numpy(float)
    p = dfp.loc[sub.index, p_col].to_numpy(float)
    scored = []
    for f in feats:
        x = dfp.loc[sub.index, f].to_numpy(float)
        if np.isfinite(x).sum() < max(40, 0.5 * len(sub)):
            continue
        ps = partial_spear(x, p, y)
        if ps is not None and np.isfinite(ps):
            scored.append((f, ps))
    scored.sort(key=lambda t: -abs(t[1]))
    return ", ".join(f"{f}{'+' if v > 0 else '-'}{abs(v):.2f}" for f, v in scored[:k])


def main() -> int:
    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    df = df[(df["games_played"] >= 6) & df[C.LABEL].notna() & df[C.PERSIST].notna()].copy()

    # candidate features for the per-stratum sweep (exclude persistence proxies
    # so the "which features matter" column shows signal BEYOND persistence)
    feats = [f for f in C.all_features()
             if f in df.columns and f not in C.PERSISTENCE_PROXIES]
    dfp = C.pctnorm_within_year(df, feats + [C.PERSIST])

    df["exp"] = df["experience"]
    # strata definitions
    def age_band(a):
        return ("<=23" if a <= 23 else "24-26" if a <= 26 else
                "27-28" if a <= 28 else "29+" if a >= 29 else "na")
    def exp_band(e):
        if not np.isfinite(e):
            return "na"
        return ("1 rook-Y2" if e <= 1 else "2 asc(2-4)" if e <= 4 else
                "3 prime(5-7)" if e <= 7 else "4 vet(8+)")
    def work_band(c):
        return "bellcow(>=200)" if c >= 200 else "committee(100-199)" if c >= 100 else "thin(<100)"
    def pedigree(row):
        if row.get("is_undrafted") == 1:
            return "UDFA"
        r = row.get("draft_round")
        if not np.isfinite(r):
            return "na"
        return "R1" if r == 1 else "day2(2-3)" if r <= 3 else "day3(4-7)"
    def ppg_tier(rk):
        if not np.isfinite(rk):
            return "na"
        return "RB1(1-12)" if rk <= 12 else "RB2(13-24)" if rk <= 24 else \
               "RB3(25-36)" if rk <= 36 else "depth(37+)"

    df["s_age"] = df["age"].map(age_band)
    df["s_exp"] = df["exp"].map(exp_band)
    df["s_work"] = df["carries"].fillna(0).map(work_band)
    df["s_ped"] = df.apply(pedigree, axis=1)
    df["s_ppg"] = df["rank_season_rb_half"].map(ppg_tier)
    df["rb_adp_next_rank"] = df.groupby("year")["adp_next"].rank(method="min")
    def adp_tier(rk):
        if not np.isfinite(rk):
            return "undrafted"
        return "top-3" if rk <= 3 else "RB1(4-12)" if rk <= 12 else \
               "RB2(13-24)" if rk <= 24 else "RB3(25-36)" if rk <= 36 else "late(37+)"
    df["s_adp"] = df["rb_adp_next_rank"].map(adp_tier)

    strata = [("AGE", "s_age", ["<=23", "24-26", "27-28", "29+"]),
              ("EXPERIENCE", "s_exp", ["1 rook-Y2", "2 asc(2-4)", "3 prime(5-7)", "4 vet(8+)"]),
              ("WORKLOAD (carries yr Y)", "s_work", ["bellcow(>=200)", "committee(100-199)", "thin(<100)"]),
              ("DRAFT PEDIGREE", "s_ped", ["R1", "day2(2-3)", "day3(4-7)", "UDFA"]),
              ("CURRENT PPG TIER", "s_ppg", ["RB1(1-12)", "RB2(13-24)", "RB3(25-36)", "depth(37+)"]),
              ("NEXT-YR ADP TIER", "s_adp", ["top-3", "RB1(4-12)", "RB2(13-24)", "RB3(25-36)", "late(37+)"])]

    for title, col, order in strata:
        print("\n" + "=" * 140)
        print(f"STRATUM: {title}")
        print("=" * 140)
        print(f"{'cell':16}{'n':>5}{'meanPPG':>8}{'persist_sp':>11}{'adp_sp':>8}"
              f"{'resid_sd':>9}   top features beyond persistence (partial Spearman)")
        for cell in order:
            sub = df[df[col] == cell]
            if len(sub) < 40:
                print(f"{cell:16}{len(sub):>5}  (too few)")
                continue
            y = sub[C.LABEL].to_numpy(float)
            p = sub[C.PERSIST].to_numpy(float)
            ps = C.spearman(p, y)
            # market adp spearman where available
            an = sub["adp_next"].to_numpy(float)
            asp = C.spearman(-an, y) if np.isfinite(an).sum() >= 40 else np.nan
            # residual sd after regression-to-mean (reliability)
            m = np.isfinite(p) & np.isfinite(y)
            b = np.polyfit(p[m], y[m], 1)
            resid_sd = float((y[m] - (b[0] * p[m] + b[1])).std())
            tf = top_features(sub, dfp, feats, C.PERSIST)
            asp_s = f"{asp:>8.3f}" if np.isfinite(asp) else f"{'--':>8}"
            print(f"{cell:16}{len(sub):>5}{y.mean():>8.2f}{ps:>11.3f}{asp_s}"
                  f"{resid_sd:>9.2f}   {tf}")

    print("\nlegend: persist_sp = Spearman(Y PPG, Y+1 PPG) within cell (higher = more")
    print("predictable by inertia); adp_sp = Spearman(-market ADP, Y+1 PPG); resid_sd =")
    print("sd of Y+1 PPG after regression-to-mean (lower = tighter prediction interval).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
