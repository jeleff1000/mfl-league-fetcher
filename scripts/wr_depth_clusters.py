#!/usr/bin/env python3
"""WR route-depth archetypes (clusters) and their fantasy value over time.

Cluster WR seasons on era-fair role/depth features, name the archetypes, then
show each archetype's fantasy PPG and share-of-population in the EARLY (2009-12)
vs LATE (2021-24) era -- to see if deep-threat archetypes are declining in both
value and prevalence.

    python scripts/wr_depth_clusters.py [--k 5]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import wr_depth_common as C

pd.set_option("display.width", 220)

FEATS = ["adot", "air_yards_share", "yac_share", "target_share", "catch_rate",
         "deep_catch_rate", "yards_per_reception", "explosive_catch_rate"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--k", type=int, default=0)
    args = ap.parse_args()

    df = C.load_wr()
    df = df[(df["targets"] >= 50) & df["year"].between(2009, 2025)
            & df["adot"].notna() & df["ppg_season_4pt_half"].notna()].copy()

    Xp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in FEATS})
    Xp = Xp.fillna(Xp.median())
    Z = StandardScaler().fit_transform(Xp)
    S = PCA(n_components=0.90, random_state=0).fit_transform(Z)

    if not args.k:
        for k in range(3, 8):
            km = KMeans(n_clusters=k, n_init=10, random_state=0).fit(S)
            print(f"  k={k} silhouette={silhouette_score(S, km.labels_):.3f}")
        return 0

    df["cl"] = KMeans(n_clusters=args.k, n_init=20, random_state=0).fit_predict(S)

    def name(a, ays, yac, ts, cr):
        # a=adot, ays=air_yards_share, yac=yac_share, ts=target_share, cr=catch_rate
        if a >= 13 and cr < 0.60:
            return "deep threat"
        if yac >= 0.42:
            return "YAC / slot"
        if ts >= 0.24 and ays >= 0.30:
            return "alpha X (volume)"
        if a <= 9.5:
            return "short/possession"
        return "balanced"
    prof = df.groupby("cl").agg(adot=("adot","mean"), ays=("air_yards_share","mean"),
        yac=("yac_share","mean"), ts=("target_share","mean"), cr=("catch_rate","mean"),
        ppg=("ppg_season_4pt_half","mean"))
    names = {cl: name(r.adot, r.ays, r.yac, r.ts, r.cr) for cl, r in prof.iterrows()}
    # de-dupe
    seen = {}
    for cl in prof.sort_values("adot", ascending=False).index:
        nm = names[cl]
        if list(names.values()).count(nm) > 1:
            seen[nm] = seen.get(nm, 0)+1; names[cl] = f"{nm} #{seen[nm]}"
    df["arch"] = df["cl"].map(names)

    print("=" * 130)
    print(f"WR DEPTH ARCHETYPES (k={args.k}) — profile + fantasy value by era")
    print("=" * 130)
    rows = []
    tot_early = len(df[df.year.between(2009,2012)])
    tot_late = len(df[df.year.between(2021,2024)])
    for arch, g in df.groupby("arch"):
        e = g[g.year.between(2009,2012)]; l = g[g.year.between(2021,2024)]
        rows.append({
            "archetype": arch, "n": len(g),
            "adot": round(g.adot.mean(),1), "ay_share": round(g.air_yards_share.mean(),2),
            "yac_sh": round(g.yac_share.mean(),2), "tgt_sh": round(g.target_share.mean(),2),
            "catch%": round(g.catch_rate.mean(),2),
            "ppg_half": round(g.ppg_season_4pt_half.mean(),1),
            "ppg_ppr": round(g.ppg_season_4pt_ppr.mean(),1),
            "ppg_0ppr": round(g.ppg_season_4pt_0ppr.mean(),1),
            "PPG_09-12": round(e.ppg_season_4pt_half.mean(),1) if len(e) else None,
            "PPG_21-24": round(l.ppg_season_4pt_half.mean(),1) if len(l) else None,
            "share_09-12": f"{len(e)/tot_early:.0%}",
            "share_21-24": f"{len(l)/tot_late:.0%}",
        })
    tab = pd.DataFrame(rows).sort_values("adot", ascending=False)
    print(tab.to_string(index=False))
    print("\nshare_* = this archetype's % of all qualifying WRs in that era "
          "(does the deep-threat share shrink? does its PPG fall?)")
    tab.to_json(C.SCRATCH / "wr_depth_clusters.json", orient="records", indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
