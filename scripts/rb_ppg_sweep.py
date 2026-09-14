#!/usr/bin/env python3
"""Single-column predictive sweep for RB next-year PPG.

For every season-Y feature, rank how well it predicts realized Y+1 PPG:
  spearman   : Spearman(feature, Y+1 PPG), features percentile-normed within year
  partial    : same but residualized on the PERSISTENCE baseline (Y PPG) -> the
               signal a column adds BEYOND "he was already good this year"
  z_null     : (spearman - shuffled_mean)/shuffled_sd, label permuted within year
  cov / yr0  : non-null coverage and earliest season with >=50% coverage (era)

Population: RB seasons with games_played>=6 and a realized Y+1 label.

    python scripts/rb_ppg_sweep.py [--test-from 2015]
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

import rb_ppg_common as C


def residualize(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    """Return residuals of y ~ 1 + x over finite rows (NaN elsewhere)."""
    out = np.full_like(y, np.nan, dtype=float)
    m = np.isfinite(y) & np.isfinite(x)
    if m.sum() < 20 or x[m].std() < 1e-9:
        return out
    b1 = np.cov(x[m], y[m])[0, 1] / np.var(x[m])
    b0 = y[m].mean() - b1 * x[m].mean()
    out[m] = y[m] - (b0 + b1 * x[m])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--test-from", type=int, default=2015,
                    help="also report Spearman on label-years >= this")
    ap.add_argument("--null-iters", type=int, default=300)
    ap.add_argument("--min-games", type=int, default=6)
    args = ap.parse_args()

    df = C.load_dataset()
    df = df[(df["games_played"] >= args.min_games) & df[C.LABEL].notna()].copy()
    print(f"population: {len(df):,} RB seasons (games>={args.min_games}, has Y+1 label)")
    print(f"label years: {int(df['year'].min())}-{int(df['year'].max())}\n")

    feats = [f for f in C.all_features() if f in df.columns and f != C.PERSIST]
    dfp = C.pctnorm_within_year(df, feats + [C.PERSIST])
    y = df[C.LABEL].to_numpy(float)
    p = dfp[C.PERSIST].to_numpy(float)                 # persistence, pct-normed
    yr = df["year"].to_numpy()
    rng = np.random.default_rng(0)

    # persistence reference line
    persist_sp = C.spearman(p, y)

    # precompute label residual on persistence once
    y_res = residualize(y, p)

    # --- vectorized within-year shuffle null -------------------------------
    # spearman(x, perm(y)) == pearson(rank(x), perm(rank(y))); ranks of y are
    # fixed, so precompute ry once and just permute it within year per iter.
    ry_full = pd.Series(y).rank().to_numpy()
    year_groups = [np.where(yr == u)[0] for u in np.unique(yr)]
    ry_perms = np.tile(ry_full, (args.null_iters, 1))   # (iters, n)
    for gi in year_groups:
        if len(gi) > 1:
            for k in range(args.null_iters):
                ry_perms[k, gi] = ry_full[gi[rng.permutation(len(gi))]]

    rec = df["year"].to_numpy() >= args.test_from
    rows = []
    for f in feats:
        x = dfp[f].to_numpy(float)
        cov = int(np.isfinite(x).sum())
        if cov < 200:
            continue
        sp = C.spearman(x, y)
        sp_rec = C.spearman(x[rec], y[rec])
        # partial (beyond persistence): corr of residuals
        x_res = residualize(x, p)
        partial = C.pearson(x_res, y_res)
        # earliest era with >=50% coverage
        by = df.assign(_f=np.isfinite(x)).groupby("year")["_f"].mean()
        good = by[by >= 0.5]
        yr0 = int(good.index.min()) if len(good) else None
        # within-year shuffle null on Spearman (vectorized over finite rows)
        m = np.isfinite(x)
        rx = pd.Series(x[m]).rank().to_numpy()
        rx = (rx - rx.mean()) / (rx.std() + 1e-12)
        rp = ry_perms[:, m]
        rp = (rp - rp.mean(axis=1, keepdims=True)) / (rp.std(axis=1, keepdims=True) + 1e-12)
        nulls = rp @ rx / len(rx)
        z = (sp - nulls.mean()) / nulls.std() if nulls.std() > 1e-9 else np.nan
        rows.append({
            "feature": f, "cov": cov, "yr0": yr0,
            "spearman": round(sp, 3), "sp_recent": round(sp_rec, 3),
            "partial": round(partial, 3), "z_null": round(z, 1),
            "proxy": "P" if f in C.PERSISTENCE_PROXIES else "",
        })

    lb = pd.DataFrame(rows)
    lb["absp"] = lb["partial"].abs()
    lb = lb.sort_values("absp", ascending=False).drop(columns="absp").reset_index(drop=True)

    print("=" * 92)
    print(f"RB NEXT-YEAR PPG ({C.VARIANT}) — single-column sweep")
    print(f"persistence baseline  Spearman(Y PPG, Y+1 PPG) = {persist_sp:.3f}")
    print("ranked by |partial| = signal beyond persistence.  P = current-year finish proxy")
    print("=" * 92)
    print(f"{'feature':34}{'cov':>6} {'yr0':>5} {'spear':>7} {'sp_rec':>7} {'partial':>8} {'z_null':>7}  proxy")
    for _, r in lb.iterrows():
        yr0 = "-" if pd.isna(r["yr0"]) else str(int(r["yr0"]))
        print(f"{r['feature']:34}{int(r['cov']):>6} {yr0:>5} {r['spearman']:>7.3f} "
              f"{r['sp_recent']:>7.3f} {r['partial']:>8.3f} {r['z_null']:>7.1f}  {r['proxy']}")

    lb.to_json(C.SCRATCH / "rb_ppg_sweep.json", orient="records", indent=2)
    print(f"\nwrote {C.SCRATCH / 'rb_ppg_sweep.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
