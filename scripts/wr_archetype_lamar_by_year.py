#!/usr/bin/env python3
"""WR natural archetypes (columns) x seasons (rows), valued in LAMAR/G.

Cluster corrected WR-seasons into natural depth/role archetypes (PCA->KMeans),
then pivot: rows = years 2009-2025, columns = archetype, cell = that archetype's
mean LAMAR/G (league-adjusted value above replacement). Shows which receiver
TYPES gain/lose league-relative value over time -- time is the axis.

    python scripts/wr_archetype_lamar_by_year.py [--k 5]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import wr_depth_common as C
from multi_league.core.readers.fly_reader import FlyReader

pd.set_option("display.width", 260)

FEATS = ["adot", "air_yards_share", "yac_share", "target_share", "catch_rate",
         "deep_catch_rate", "yards_per_reception", "explosive_catch_rate"]
LAMARG = "lamar_ppg_12t_flx_half_4pt"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--k", type=int, default=5)
    args = ap.parse_args()

    wr = C.load_wr()
    lam = FlyReader().query_df(
        f'SELECT "NFL_player_id","year","{LAMARG}", "ppg_season_4pt_half" AS ppg2 '
        f"FROM nfl_historical.player_nfl_season_all WHERE position='WR' AND year BETWEEN 2009 AND 2025",
        "___ops")
    lam[LAMARG] = pd.to_numeric(lam[LAMARG], errors="coerce")
    wr = wr.merge(lam[["NFL_player_id", "year", LAMARG]], on=["NFL_player_id", "year"], how="left")

    df = wr[(wr.targets >= 50) & wr.year.between(2009, 2025) & wr.adot.notna()
            & wr[LAMARG].notna()].copy()

    # cluster (era-fair within-year percentile -> PCA -> KMeans)
    Xp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in FEATS}).fillna(0.5)
    S = PCA(n_components=0.90, random_state=0).fit_transform(StandardScaler().fit_transform(Xp))
    df["cl"] = KMeans(n_clusters=args.k, n_init=20, random_state=0).fit_predict(S)

    # clean archetype names from each cluster's profile
    prof = df.groupby("cl").agg(adot=("adot", "mean"), ts=("target_share", "mean"),
                                yac=("yac_share", "mean"), ppg=("ppg_season_4pt_half", "mean"))
    def name(r):
        if r.adot >= 13: return "Deep threat"
        if r.ts >= 0.22: return "Alpha (volume)"
        if r.adot <= 9: return "Slot / YAC"
        if r.yac >= 0.36: return "Balanced-YAC"
        return "Mid (low-vol)"
    names = {cl: name(r) for cl, r in prof.iterrows()}
    # de-dup if two clusters map to same name
    seen = {}
    for cl in prof.sort_values("ppg", ascending=False).index:
        n = names[cl]
        if list(names.values()).count(n) > 1:
            seen[n] = seen.get(n, 0) + 1; names[cl] = f"{n} {seen[n]}"
    df["arch"] = df["cl"].map(names)

    order = df.groupby("arch")[LAMARG].mean().sort_values(ascending=False).index.tolist()

    # profile legend
    print("ARCHETYPES (natural clusters):")
    pl = df.groupby("arch").agg(n=("arch","size"), ADOT=("adot","mean"),
        tgt_share=("target_share","mean"), yac=("yac_share","mean"),
        LAMARg=(LAMARG,"mean"), PPG=("ppg_season_4pt_half","mean")).round(2).reindex(order)
    print(pl.to_string())

    # pivot: rows=year, cols=archetype, cell=mean LAMAR/G
    piv = df.pivot_table(index="year", columns="arch", values=LAMARG, aggfunc="mean").round(2)
    piv = piv.reindex(columns=order)
    print("\n" + "=" * 130)
    print("LAMAR/G by SEASON x WR ARCHETYPE  (league-adjusted value above replacement; rows=years)")
    print("=" * 130)
    print(piv.to_string())

    print("\nSLOPE / DECADE per archetype (LAMAR/G, 2009-2025):")
    yrs = piv.index.to_numpy(float)
    for a in order:
        v = piv[a].to_numpy(float); m = np.isfinite(v)
        s = np.polyfit(yrs[m], v[m], 1)[0] * 10 if m.sum() >= 5 else np.nan
        early = piv.loc[piv.index <= 2012, a].mean(); late = piv.loc[piv.index >= 2022, a].mean()
        print(f"  {a:16} slope {s:+.2f}   2009-12 {early:+.2f} -> 2022-25 {late:+.2f}")

    piv.to_csv(r"D:/yahoo_oauth/docs/runbooks/wr-archetype-lamar-by-year.csv", encoding="utf-8-sig")
    print("\nwrote docs/runbooks/wr-archetype-lamar-by-year.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
