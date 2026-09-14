#!/usr/bin/env python3
"""Discover NATURAL RB-season strata (unsupervised) and tabulate predictor->
next-year-PPG correlations within each discovered cluster.

Rows  = data-discovered RB archetypes (KMeans on era-fair role/age/pedigree
        features; k chosen by silhouette).
Cols  = Spearman(predictor, Y+1 PPG) within the cluster, for a fixed predictor
        panel + persistence R^2 + residual sd (reliability).

    python scripts/rb_ppg_clusters.py [--k N]   (N omitted -> pick by silhouette)
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

import rb_ppg_common as C

pd.set_option("display.width", 240)

# features that DEFINE an RB's season-type (role / age / pedigree / level) --
# deliberately NOT the target. era-fair ones are percentile-within-year.
PCT_FEATS = ["carries", "receptions", "rushing_yards", "receiving_yards",
             C.PERSIST]                     # volume, receiving, production level
RAW_FEATS = ["age", "experience", "draft_overall_f"]   # standardized as-is

# predictor panel for the correlation columns (Spearman with Y+1 PPG)
PANEL = ["persist", "adp_next", "age", "experience", "carries", "receptions",
         "target_share", "yards_per_carry", "rush_explosive_10",
         "draft_overall", "pro_bowl"]


def build(df):
    df = df.copy()
    df["draft_overall_f"] = df["draft_overall"].fillna(262.0)   # UDFA -> after last pick
    df["experience"] = df["experience"].fillna(df["experience"].median())
    X = pd.DataFrame(index=df.index)
    for f in PCT_FEATS:
        X[f] = df.groupby("year")[f].rank(pct=True)
    for f in RAW_FEATS:
        X[f] = df[f]
    X = X.fillna(X.median())
    Z = StandardScaler().fit_transform(X)
    return df, Z


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--k", type=int, default=0)
    args = ap.parse_args()

    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    df = df[(df["games_played"] >= 6) & df[C.LABEL].notna()
            & df[C.PERSIST].notna()].copy()
    df, Z = build(df)
    print(f"clustering {len(df):,} RB-seasons on {Z.shape[1]} era-fair features "
          f"({', '.join(PCT_FEATS + RAW_FEATS)})\n")

    if not args.k:
        print("silhouette by k:")
        for k in range(3, 9):
            km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(Z)
            s = silhouette_score(Z, km.labels_, sample_size=4000, random_state=0)
            print(f"  k={k}  silhouette={s:.3f}")
        print("\n(re-run with --k to lock a choice)")
        return 0

    km = KMeans(n_clusters=args.k, n_init=20, random_state=0).fit(Z)
    df["cl"] = km.labels_

    # auto-label each cluster from its centroid profile (age/carries/ppg)
    def label(age, car, ppg):
        role = ("workhorse" if car >= 140 else "committee" if car >= 60 else "depth")
        stage = "aging" if age >= 27.5 else "young/prime"
        tag = f"{stage} {role}"
        if role == "workhorse" and ppg >= 11.5 and stage.startswith("young"):
            tag = "prime workhorse (elite)"
        return tag
    prof0 = df.groupby("cl").agg(age=("age", "mean"), car=("carries", "mean"),
                                 ppg=(C.PERSIST, "mean"))
    names = {cl: f"{label(r.age, r.car, r.ppg)}" for cl, r in prof0.iterrows()}
    # disambiguate duplicate names with the ppg rank suffix
    seen = {}
    for cl in prof0.sort_values("ppg", ascending=False).index:
        nm = names[cl]
        if list(names.values()).count(nm) > 1:
            seen[nm] = seen.get(nm, 0) + 1
            names[cl] = f"{nm} #{seen[nm]}"
    df["archetype"] = df["cl"].map(names)

    # ---- cluster profiles (original units) to name the archetypes ----
    prof = df.groupby("archetype").agg(
        n=("cl", "size"),
        age=("age", "mean"),
        exp=("experience", "mean"),
        carries=("carries", "mean"),
        recept=("receptions", "mean"),
        ppg_Y=(C.PERSIST, "mean"),
        ppg_Y1=(C.LABEL, "mean"),
        draft_ov=("draft_overall", "median"),
        udfa=("is_undrafted", "mean"),
    ).round(1)
    prof = prof.sort_values("ppg_Y", ascending=False)
    print("=" * 110)
    print(f"DISCOVERED CLUSTERS (k={args.k}) — profile in original units "
          "(sorted by current PPG)")
    print("=" * 110)
    print(prof.to_string())

    # ---- correlation table: rows = clusters, cols = Spearman(predictor, Y+1) ----
    df["persist"] = df[C.PERSIST]
    df["draft_overall"] = df["draft_overall_f"]
    rows = []
    for cl in prof.index:
        sub = df[df["archetype"] == cl]
        y = sub[C.LABEL].to_numpy(float)
        rec = {"archetype": cl, "n": len(sub),
               "meanPPGy1": round(float(np.nanmean(y)), 1)}
        for f in PANEL:
            if f not in sub.columns:
                rec[f] = None; continue
            x = sub[f].to_numpy(float)
            # sign so that "better next year" is +: invert cost-like predictors
            sgn = -1 if f in ("adp_next", "draft_overall", "age", "experience") else 1
            sp = C.spearman(sgn * x, y)
            rec[f] = round(sp, 2) if np.isfinite(sp) else None
        # reliability
        p = sub[C.PERSIST].to_numpy(float)
        m = np.isfinite(p) & np.isfinite(y)
        b = np.polyfit(p[m], y[m], 1)
        rec["persistR2"] = round(float(np.corrcoef(p[m], y[m])[0, 1] ** 2), 2)
        rec["resid_sd"] = round(float((y[m] - (b[0]*p[m]+b[1])).std()), 2)
        rows.append(rec)

    tab = pd.DataFrame(rows).set_index("archetype")
    print("\n" + "=" * 110)
    print("Spearman(predictor, Y+1 PPG) WITHIN each discovered cluster")
    print("(sign flipped so + = predicts a better next year; ADP/draft/age/exp inverted)")
    print("=" * 110)
    print(tab.to_string())
    print("\nlegend: persist=Y PPG, adp_next=market Y+1 ADP; persistR2/resid_sd = reliability")

    tab.to_json(C.SCRATCH / "rb_ppg_clusters.json", orient="index", indent=2)
    prof.to_json(C.SCRATCH / "rb_ppg_cluster_profiles.json", orient="index", indent=2)
    print(f"\nwrote rb_ppg_clusters.json + rb_ppg_cluster_profiles.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
