#!/usr/bin/env python3
"""Do columns naturally STACK into tight 1-4 column bundles?

Cluster the FEATURES (not the rows) by |Spearman correlation|: hierarchical
clustering on distance = 1 - |corr|, cut so each bundle's members genuinely move
together. Each discovered bundle is a data-found "combo." For each, build a
sign-aligned composite (mean of z-scored members) and score its correlation with
Y+1 PPG -- so we see both WHICH columns stack and HOW predictive each combo is.

These bundles can then serve as cleaner cluster AXES (rows) or as composite
PREDICTOR columns (columns).

    python scripts/rb_ppg_column_combos.py [--since 2010] [--tight 0.35]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

import rb_ppg_common as C


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", type=int, default=2010)
    ap.add_argument("--tight", type=float, default=0.35,
                    help="cut distance; smaller = tighter bundles (0.35 => |corr|>=.65)")
    args = ap.parse_args()

    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    df = df[(df["games_played"] >= 6) & df[C.LABEL].notna() & df[C.PERSIST].notna()].copy()
    if args.since:
        df = df[df["year"] >= args.since].copy()

    feats = [f for f in C.all_features() if f in df.columns]
    feats = [f for f in feats if df[f].notna().mean() >= 0.80]
    for a in ("adp_next", "adp_y"):
        if a in df.columns and df[a].notna().mean() >= 0.40:
            feats.append(a)
    # include persistence itself so we see the "production level" bundle
    feats = list(dict.fromkeys(feats + [C.PERSIST]))
    print(f"clustering {len(feats)} columns on |Spearman| (year>={args.since}, n={len(df):,})\n")

    # percentile-norm within year, then Spearman corr = Pearson on ranks
    Xp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in feats})
    Xp = Xp.fillna(Xp.median())
    corr = np.corrcoef(Xp.rank().to_numpy().T)
    corr = np.nan_to_num(corr, nan=0.0)
    dist = 1.0 - np.abs(corr)
    np.fill_diagonal(dist, 0.0)
    Z = linkage(squareform(dist, checks=False), method="average")
    lab = fcluster(Z, t=args.tight, criterion="distance")

    # composite per bundle + predictive score
    y = df[C.LABEL].to_numpy(float)
    p = df.groupby("year")[C.PERSIST].rank(pct=True).to_numpy(float)

    def partial(x):
        def resid(a, b):
            m = np.isfinite(a) & np.isfinite(b)
            s = np.polyfit(b[m], a[m], 1)
            o = np.full_like(a, np.nan, float); o[m] = a[m]-(s[0]*b[m]+s[1]); return o
        return C.pearson(resid(x, p), resid(y, p))

    bundles = []
    for cl in np.unique(lab):
        members = [feats[i] for i in range(len(feats)) if lab[i] == cl]
        M = Xp[members].to_numpy(float)
        anchor = M[:, 0]
        signs = [1.0] + [np.sign(np.corrcoef(M[:, 0], M[:, j])[0, 1] or 1) for j in range(1, M.shape[1])]
        comp = np.mean(M * np.array(signs), axis=1)
        # mean intra |corr|
        if len(members) > 1:
            sub = np.abs(corr[np.ix_([feats.index(m) for m in members],
                                     [feats.index(m) for m in members])])
            intra = (sub.sum() - len(members)) / (len(members)**2 - len(members))
        else:
            intra = 1.0
        sp = C.spearman(comp, y)
        pa = partial(comp)
        bundles.append({"members": members, "size": len(members),
                        "intra_corr": round(float(intra), 2),
                        "spearman": round(sp, 3), "partial": round(pa, 3)})

    bundles.sort(key=lambda b: -abs(b["partial"]))
    print("=" * 100)
    print(f"DATA-FOUND COLUMN BUNDLES (cut |corr|>={1-args.tight:.2f}) — combos that stack together")
    print("ranked by |partial| = composite's signal beyond persistence")
    print("=" * 100)
    print(f"{'sz':>3}{'intra':>7}{'spear':>7}{'partial':>8}   members")
    for b in bundles:
        star = "  <" if abs(b["partial"]) >= 0.12 and C.PERSIST not in b["members"] else ""
        print(f"{b['size']:>3}{b['intra_corr']:>7.2f}{b['spearman']:>7.3f}"
              f"{b['partial']:>8.3f}   {', '.join(b['members'])}{star}")
    print("\nsingletons (size 1) = columns that don't tightly stack with anything.")
    print("<  = a multi-column combo that adds real signal beyond just-repeat-last-year.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
