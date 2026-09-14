#!/usr/bin/env python3
"""Combo/model stage for RB next-year PPG.

Rolling leave-future-out out-of-sample:
  * baselines : persistence (Y PPG) and regression-to-mean (OLS label~Y PPG,
                fit on train) -- the honest bars to beat.
  * models    : HistGradientBoostingRegressor (all features, native NaN) and
                ElasticNetCV (coverage-filtered, imputed+scaled).
Reports pooled test R^2 / Spearman / MAE and LIFT over the baselines, per-fold
R^2, GBM permutation importance, and the ElasticNet minimal feature set.

Also fits an ATTRITION classifier: P(no Y+1 season | RB played >=6 games in Y).

    python scripts/rb_ppg_model.py [--test-from 2008] [--min-games 6]
"""
from __future__ import annotations

import argparse
import warnings

import numpy as np
import pandas as pd
from sklearn.ensemble import (HistGradientBoostingRegressor,
                              HistGradientBoostingClassifier)
from sklearn.linear_model import ElasticNetCV
from sklearn.metrics import r2_score, mean_absolute_error, roc_auc_score
from sklearn.inspection import permutation_importance

import rb_ppg_common as C

warnings.filterwarnings("ignore")


def r2(y, p):
    return r2_score(y, p)


def spear(y, p):
    return C.spearman(np.asarray(y, float), np.asarray(p, float))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--test-from", type=int, default=2008)
    ap.add_argument("--min-games", type=int, default=6)
    ap.add_argument("--train-from", type=int, default=1990)
    args = ap.parse_args()

    raw = C.load_dataset()
    feats = [f for f in C.all_features() if f in raw.columns]
    # era-fair: percentile-norm features within season (known post-season-Y)
    dfp = C.pctnorm_within_year(raw, feats)

    reg = raw[(raw["games_played"] >= args.min_games) & raw[C.LABEL].notna()
              & raw[C.PERSIST].notna()].copy()
    reg_idx = reg.index
    X_all = dfp.loc[reg_idx, feats].to_numpy(float)
    y_all = reg[C.LABEL].to_numpy(float)
    persist_all = reg[C.PERSIST].to_numpy(float)      # raw Y PPG for baselines
    yr_all = reg["year"].to_numpy()

    # coverage-filtered feature set for the linear model
    cov = np.isfinite(X_all).mean(axis=0)
    lin_feats = [f for f, c in zip(feats, cov) if c >= 0.80]
    lin_ix = [i for i, c in enumerate(cov) if c >= 0.80]
    print(f"features: {len(feats)} total, {len(lin_feats)} with >=80% coverage (linear)")

    test_years = [y for y in range(args.test_from, int(yr_all.max()) + 1)]
    P = {k: [] for k in ("y", "persist", "rtm", "gbm", "enet", "yr")}
    fold_rows = []
    for ty in test_years:
        tr = (yr_all < ty) & (yr_all >= args.train_from)
        te = yr_all == ty
        if tr.sum() < 500 or te.sum() < 20:
            continue
        ytr, yte = y_all[tr], y_all[te]
        # --- baselines ---
        b1, b0 = np.polyfit(persist_all[tr], ytr, 1)     # regression-to-mean
        rtm = b0 + b1 * persist_all[te]
        # --- GBM (all feats, native NaN) ---
        gbm = HistGradientBoostingRegressor(
            max_depth=3, learning_rate=0.05, max_iter=400,
            l2_regularization=1.0, min_samples_leaf=30, random_state=0)
        gbm.fit(X_all[tr], ytr)
        pgbm = gbm.predict(X_all[te])
        # --- ElasticNet (coverage-filtered, impute .5, standardize) ---
        Xl_tr = X_all[np.ix_(np.where(tr)[0], lin_ix)].copy()
        Xl_te = X_all[np.ix_(np.where(te)[0], lin_ix)].copy()
        med = np.nanmedian(np.where(np.isfinite(Xl_tr), Xl_tr, np.nan), axis=0)
        med = np.where(np.isfinite(med), med, 0.5)
        for j in range(Xl_tr.shape[1]):
            Xl_tr[~np.isfinite(Xl_tr[:, j]), j] = med[j]
            Xl_te[~np.isfinite(Xl_te[:, j]), j] = med[j]
        mu, sd = Xl_tr.mean(0), Xl_tr.std(0) + 1e-9
        enet = ElasticNetCV(l1_ratio=[.2, .5, .8, .95], n_alphas=40, cv=4,
                            random_state=0, max_iter=5000)
        enet.fit((Xl_tr - mu) / sd, ytr)
        penet = enet.predict((Xl_te - mu) / sd)

        for arr, k in [(yte, "y"), (persist_all[te], "persist"), (rtm, "rtm"),
                       (pgbm, "gbm"), (penet, "enet")]:
            P[k].extend(arr)
        P["yr"].extend([ty] * te.sum())
        fold_rows.append({
            "year": ty, "n": int(te.sum()),
            "r2_persist": round(r2(yte, persist_all[te]), 3),
            "r2_rtm": round(r2(yte, rtm), 3),
            "r2_gbm": round(r2(yte, pgbm), 3),
            "r2_enet": round(r2(yte, penet), 3),
        })

    D = {k: np.array(v) for k, v in P.items()}
    print("\n" + "=" * 78)
    print(f"RB NEXT-YEAR PPG ({C.VARIANT}) — pooled out-of-sample "
          f"(test {test_years[0]}-{test_years[-1]}, n={len(D['y']):,})")
    print("=" * 78)
    hdr = f"{'model':16}{'R2':>8}{'Spearman':>10}{'MAE':>8}{'dR2 vs persist':>16}"
    print(hdr)
    base_r2 = r2(D["y"], D["persist"])
    for name, key in [("persistence", "persist"), ("regress-to-mean", "rtm"),
                      ("ElasticNet", "enet"), ("HistGBM", "gbm")]:
        rr = r2(D["y"], D[key])
        print(f"{name:16}{rr:>8.3f}{spear(D['y'], D[key]):>10.3f}"
              f"{mean_absolute_error(D['y'], D[key]):>8.3f}{rr - base_r2:>+16.3f}")

    print("\nper-fold test R^2:")
    fr = pd.DataFrame(fold_rows)
    print(fr.to_string(index=False))

    # --- permutation importance (GBM refit train<last, test=last window) ---
    cut = test_years[max(0, len(test_years) - 6)]     # last ~6 seasons as test
    tr = (yr_all < cut) & (yr_all >= args.train_from)
    te = yr_all >= cut
    gbm = HistGradientBoostingRegressor(
        max_depth=3, learning_rate=0.05, max_iter=400, l2_regularization=1.0,
        min_samples_leaf=30, random_state=0).fit(X_all[tr], y_all[tr])
    pi = permutation_importance(gbm, X_all[te], y_all[te], n_repeats=8,
                                random_state=0, scoring="r2")
    imp = (pd.DataFrame({"feature": feats, "imp": pi.importances_mean,
                         "sd": pi.importances_std})
           .sort_values("imp", ascending=False).reset_index(drop=True))
    imp["proxy"] = imp["feature"].map(lambda f: "P" if f in C.PERSISTENCE_PROXIES else "")
    print(f"\nGBM permutation importance (test {int(cut)}+, drop in R^2):")
    print(f"{'feature':34}{'imp':>9}{'sd':>8}  proxy")
    for _, r in imp.head(25).iterrows():
        print(f"{r['feature']:34}{r['imp']:>9.4f}{r['sd']:>8.4f}  {r['proxy']}")

    # ElasticNet minimal set (last full fit)
    keep = [(f, c) for f, c in zip(lin_feats, enet.coef_) if abs(c) > 1e-4]
    keep.sort(key=lambda t: -abs(t[1]))
    print(f"\nElasticNet non-zero coefs ({len(keep)}/{len(lin_feats)}), "
          f"l1_ratio={enet.l1_ratio_}, standardized:")
    for f, c in keep[:20]:
        print(f"  {f:34}{c:>+8.3f}")

    # ---- attrition classifier: P(no Y+1 | played >=6 games) ---------------
    att = raw[(raw["games_played"] >= args.min_games) & (raw["year"] < raw["year"].max())].copy()
    Xa = dfp.loc[att.index, feats].to_numpy(float)
    ya = 1 - att["has_next"].to_numpy()               # 1 = attrition (no Y+1)
    yra = att["year"].to_numpy()
    aucs, preds, labs = [], [], []
    for ty in test_years:
        tr = (yra < ty) & (yra >= args.train_from)
        te = yra == ty
        if tr.sum() < 500 or te.sum() < 20 or ya[te].sum() < 3:
            continue
        clf = HistGradientBoostingClassifier(
            max_depth=3, learning_rate=0.05, max_iter=300,
            min_samples_leaf=30, random_state=0).fit(Xa[tr], ya[tr])
        pp = clf.predict_proba(Xa[te])[:, 1]
        preds.extend(pp); labs.extend(ya[te])
    auc = roc_auc_score(labs, preds)
    print("\n" + "=" * 78)
    print(f"ATTRITION classifier — P(no Y+1 season | RB >= {args.min_games} games)")
    print(f"  base rate {np.mean(ya):.3f}   pooled OOS AUC {auc:.3f}   n={len(labs):,}")
    # attrition importances
    cut = test_years[max(0, len(test_years) - 6)]
    tr = (yra < cut) & (yra >= args.train_from); te = yra >= cut
    clf = HistGradientBoostingClassifier(
        max_depth=3, learning_rate=0.05, max_iter=300, min_samples_leaf=30,
        random_state=0).fit(Xa[tr], ya[tr])
    pia = permutation_importance(clf, Xa[te], ya[te], n_repeats=6,
                                 random_state=0, scoring="roc_auc")
    impa = (pd.DataFrame({"feature": feats, "imp": pia.importances_mean})
            .sort_values("imp", ascending=False).head(12))
    print("  top attrition predictors (drop in AUC):")
    for _, r in impa.iterrows():
        print(f"    {r['feature']:32}{r['imp']:>9.4f}")

    imp.to_json(C.SCRATCH / "rb_ppg_gbm_importance.json", orient="records", indent=2)
    fr.to_json(C.SCRATCH / "rb_ppg_folds.json", orient="records", indent=2)
    print(f"\nwrote importance + folds json to scratchpad")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
