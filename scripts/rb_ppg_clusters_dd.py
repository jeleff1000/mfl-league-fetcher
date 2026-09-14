#!/usr/bin/env python3
"""Fully data-driven version: let the data choose BOTH the cluster axes AND the
predictor columns.

  axes  : PCA over ALL well-covered season-Y features (>=80% coverage) -> the
          data's own axes of variation; cluster (KMeans) on the PC scores.
  cols  : within each discovered cluster, sweep EVERY feature and rank by
          |partial-Spearman with Y+1 PPG, controlling for persistence|. The
          reported columns are the union of each cluster's data-selected top-k
          -- nothing hand-picked.

Only two stated methodological choices (not cherry-picking): (1) coverage>=80%
so a column is present enough to matter; (2) pure persistence proxies (rank /
weighted-ppg / lamar / Y PPG) are excluded from the *predictor* candidates
because they mechanically equal the persistence baseline we control for (they
remain in the clustering axes as the natural "level" dimension).

    python scripts/rb_ppg_clusters_dd.py [--k N] [--topk 4]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

import rb_ppg_common as C

pd.set_option("display.width", 260)


def partial(x, p, y):
    def resid(a, b):
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() < 40 or b[m].std() < 1e-9:
            return None
        s = np.polyfit(b[m], a[m], 1)
        o = np.full_like(a, np.nan, float); o[m] = a[m] - (s[0]*b[m]+s[1]); return o
    rx, ry = resid(x, p), resid(y, p)
    if rx is None or ry is None:
        return np.nan
    return C.pearson(rx, ry)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--k", type=int, default=0)
    ap.add_argument("--topk", type=int, default=4)
    ap.add_argument("--since", type=int, default=0, help="restrict to year >= this")
    args = ap.parse_args()

    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    df = df[(df["games_played"] >= 6) & df[C.LABEL].notna()
            & df[C.PERSIST].notna()].copy()
    if args.since:
        df = df[df["year"] >= args.since].copy()
        print(f"restricted to year >= {args.since}: {len(df):,} RB-seasons\n")

    # ---- feature universe: everything with >=80% coverage (data-defined) ----
    cand = [f for f in C.all_features() if f in df.columns]
    cov = {f: df[f].notna().mean() for f in cand}
    axis_feats = [f for f in cand if cov[f] >= 0.80]      # clustering axes (realized)
    pred_feats = [f for f in axis_feats if f not in C.PERSISTENCE_PROXIES]
    # ADP is a forward market signal: NOT a clustering axis (would be circular),
    # but forced into the predictor candidates so it competes head-to-head.
    for a in ("adp_next", "adp_y"):
        if a in df.columns and df[a].notna().mean() >= 0.40:
            pred_feats.append(a)
    print(f"{len(axis_feats)} features >=80% coverage feed PCA axes; "
          f"{len(pred_feats)} predictor candidates (proxies removed, ADP forced in)")
    print(f"  ADP coverage in population: adp_next={df['adp_next'].notna().mean():.0%}, "
          f"adp_y={df['adp_y'].notna().mean():.0%}\n")

    # era-fair: percentile within year, then standardize, then PCA
    Xp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in axis_feats})
    Xp = Xp.fillna(Xp.median())
    Z = StandardScaler().fit_transform(Xp)
    pca = PCA(n_components=0.90, random_state=0).fit(Z)
    S = pca.transform(Z)
    print(f"PCA: {S.shape[1]} components explain 90% of variance.")
    print("data's own axes (top loadings per PC):")
    for i in range(min(4, S.shape[1])):
        load = pd.Series(pca.components_[i], index=axis_feats)
        top = load.reindex(load.abs().sort_values(ascending=False).index).head(6)
        ev = pca.explained_variance_ratio_[i]
        print(f"  PC{i+1} ({ev*100:4.1f}% var): " +
              ", ".join(f"{n}{'+' if v>0 else '-'}{abs(v):.2f}" for n, v in top.items()))

    if not args.k:
        print("\nsilhouette by k (on PCA scores):")
        for k in range(3, 9):
            km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(S)
            s = silhouette_score(S, km.labels_, sample_size=4000, random_state=0)
            print(f"  k={k}  silhouette={s:.3f}")
        print("\n(re-run with --k to lock a choice)")
        return 0

    km = KMeans(n_clusters=args.k, n_init=20, random_state=0).fit(S)
    df["cl"] = km.labels_

    def label(age, car, ppg):
        role = "workhorse" if car >= 140 else "committee" if car >= 60 else "depth"
        stage = "aging" if age >= 27.5 else "young/prime"
        return f"{stage} {role}"
    p0 = df.groupby("cl").agg(age=("age", "mean"), car=("carries", "mean"),
                              ppg=(C.PERSIST, "mean"))
    names = {cl: label(r.age, r.car, r.ppg) for cl, r in p0.iterrows()}
    for cl in p0.sort_values("ppg", ascending=False).index:
        if list(names.values()).count(names[cl]) > 1:
            names[cl] += f" #{sum(1 for c in p0.index if c<=cl and names.get(c,'').startswith(names[cl].split(' #')[0]))}"
    df["arch"] = df["cl"].map(names)

    prof = df.groupby("arch").agg(
        n=("cl", "size"), age=("age", "mean"), carries=("carries", "mean"),
        recept=("receptions", "mean"), ppg_Y=(C.PERSIST, "mean"),
        ppg_Y1=(C.LABEL, "mean"), draft_ov=("draft_overall", "median"),
    ).round(1).sort_values("ppg_Y", ascending=False)
    print("\n" + "=" * 70)
    print(f"DATA-DISCOVERED CLUSTERS (k={args.k}) — profile")
    print("=" * 70)
    print(prof.to_string())

    # ---- per-cluster: data picks the top predictors (full sweep) ----
    dfp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in pred_feats})
    dfp["_p"] = df.groupby("year")[C.PERSIST].rank(pct=True)
    dfp[C.LABEL] = df[C.LABEL].values
    dfp["arch"] = df["arch"].values

    print("\n" + "=" * 70)
    print("Each cluster's OWN top predictors (data-selected: max |partial vs")
    print("persistence|); nothing hand-picked.")
    print("=" * 70)
    per_cluster_top = {}
    for arch in prof.index:
        sub = dfp[dfp["arch"] == arch]
        y = sub[C.LABEL].to_numpy(float)
        p = sub["_p"].to_numpy(float)
        scored = []
        for f in pred_feats:
            if sub[f].notna().mean() < 0.4:
                continue
            v = partial(sub[f].to_numpy(float), p, y)
            if v is not None and np.isfinite(v):
                scored.append((f, v))
        scored.sort(key=lambda t: -abs(t[1]))
        per_cluster_top[arch] = scored[:args.topk]
        txt = ", ".join(f"{f}{'+' if v>0 else '-'}{abs(v):.2f}" for f, v in scored[:args.topk])
        print(f"\n  [{arch}]  n={len(sub)}\n     {txt}")

    # ---- rectangular matrix on the DATA-SELECTED columns that recur in >=2
    # clusters' tops (keeps it compact & robust). blank cells with <40% cluster
    # coverage so survivorship-thin ADP on depth tiers isn't shown as signal. --
    from collections import Counter
    freq = Counter(f for lst in per_cluster_top.values() for f, _ in lst)
    colset = [f for f, c in freq.most_common() if c >= 2]
    print("\n" + "=" * 70)
    print(f"MATRIX: partial-Spearman (vs persistence) on the {len(colset)} columns the")
    print("data picked in >=2 clusters. rows=clusters. '.'=<40% coverage in cell.")
    print("=" * 70)
    rows = []
    for arch in prof.index:
        sub = dfp[dfp["arch"] == arch]
        y = sub[C.LABEL].to_numpy(float); p = sub["_p"].to_numpy(float)
        rec = {"cluster": arch, "n": len(sub)}
        for f in colset:
            if sub[f].notna().mean() < 0.4:
                rec[f] = None; continue
            v = partial(sub[f].to_numpy(float), p, y)
            rec[f] = round(v, 2) if v is not None and np.isfinite(v) else None
        rows.append(rec)
    mat = pd.DataFrame(rows).set_index("cluster")
    print(mat.fillna(".").to_string())
    mat.to_json(C.SCRATCH / "rb_ppg_clusters_dd.json", orient="index", indent=2)
    print(f"\nwrote rb_ppg_clusters_dd.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
