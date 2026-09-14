#!/usr/bin/env python3
"""Apples-to-apples: our season-Y model vs the market (Y+1 ADP) vs persistence,
on the IDENTICAL set of rows that have a Y+1 ADP. Answers: can a projection
computed right after season Y (no offseason info) match the summer draft market?

    python scripts/rb_ppg_vs_market.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import r2_score

import rb_ppg_common as C


def main() -> int:
    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    feats = [f for f in C.all_features() if f in df.columns]
    dfp = C.pctnorm_within_year(df, feats)

    reg = df[(df["games_played"] >= 6) & df[C.LABEL].notna()
             & df[C.PERSIST].notna()].copy()
    X = dfp.loc[reg.index, feats].to_numpy(float)
    y = reg[C.LABEL].to_numpy(float)
    persist = reg[C.PERSIST].to_numpy(float)
    adp_next = reg["adp_next"].to_numpy(float)
    yr = reg["year"].to_numpy()

    preds = {k: [] for k in ("y", "persist", "adp", "gbm", "yr")}
    for ty in range(2010, int(yr.max()) + 1):
        tr = (yr < ty) & (yr >= 1990)
        te = yr == ty
        if tr.sum() < 500 or te.sum() < 20:
            continue
        gbm = HistGradientBoostingRegressor(
            max_depth=3, learning_rate=0.05, max_iter=400, l2_regularization=1.0,
            min_samples_leaf=30, random_state=0).fit(X[tr], y[tr])
        pg = gbm.predict(X[te])
        for arr, k in [(y[te], "y"), (persist[te], "persist"),
                       (adp_next[te], "adp"), (pg, "gbm")]:
            preds[k].extend(arr)
        preds["yr"].extend([ty] * te.sum())
    D = {k: np.array(v) for k, v in preds.items()}

    # restrict to rows with a market ADP for a fair 3-way compare
    m = np.isfinite(D["adp"])
    print(f"identical-rows comparison  n={m.sum():,}  (rows with Y+1 ADP, test 2010+)\n")
    print(f"{'predictor':22}{'Spearman':>10}{'R2*':>8}")
    # ADP is a rank, not a PPG scale -> report Spearman for all; R2 only for
    # PPG-scaled preds (persist, gbm) which are in points.
    for name, key, scaled in [("persistence (Y PPG)", "persist", True),
                              ("market ADP (Y+1)", "adp", False),
                              ("our GBM (season-Y)", "gbm", True)]:
        sp = C.spearman(D[key][m] * (-1 if key == "adp" else 1), D["y"][m])
        r2 = f"{r2_score(D['y'][m], D[key][m]):>8.3f}" if scaled else f"{'--':>8}"
        print(f"{name:22}{sp:>10.3f}{r2}")

    # blend: does adding market ADP to our model help? (rank-average)
    from scipy.stats import rankdata
    rg = rankdata(D["gbm"][m]); ra = rankdata(-D["adp"][m])
    blend = rg + ra
    print(f"{'GBM + ADP (rank-avg)':22}{C.spearman(blend, D['y'][m]):>10.3f}{'--':>8}")
    print("\n* R2 only meaningful for PPG-scaled predictors (ADP is a draft rank).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
