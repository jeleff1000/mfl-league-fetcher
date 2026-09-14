#!/usr/bin/env python3
"""Which TYPES of receiver buff/nerf over the years?

Step 1 (data-driven): cluster the receiver STYLE columns (depth / share /
efficiency / YAC / catch-distribution rates) into bundles by |correlation| ->
each bundle is a "receiver type." Raw production totals and fantasy points are
excluded so bundles describe HOW a receiver plays, not how much.

Step 2: build a sign-aligned composite per bundle and, for each season, correlate
it with fantasy PPG. Report the EARLY (2009-12) vs LATE (2021-24) correlation and
the slope/decade -> buff (rising) or nerf (falling). Done for half / ppr / 0ppr.

    python scripts/wr_depth_bundles.py [--tight 0.45]
"""
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
import wr_depth_common as C

pd.set_option("display.width", 220)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tight", type=float, default=0.45)
    args = ap.parse_args()

    df = C.load_wr()
    df = df[(df["targets"] >= 50) & df["year"].between(2009, 2025)
            & df["adot"].notna() & df["ppg_season_4pt_half"].notna()].copy()
    rec = df["receptions"].replace(0, np.nan)
    # catch-depth distribution rates (share of catches by yardage gained)
    df["short_catch_rate"] = (df["receptions_0_4"] + df["receptions_5_9"]) / rec
    df["mid_catch_rate"] = (df["receptions_10_19"] + df["receptions_20_29"]) / rec
    df["long_catch_rate"] = (df["receptions_30_39"] + df["receptions_40plus"]) / rec

    STYLE = ["adot", "air_yards_share", "target_share", "wopr", "racr",
             "catch_rate", "yards_per_target", "yards_per_reception", "yac_share",
             "deep_catch_rate", "explosive_catch_rate", "short_catch_rate",
             "mid_catch_rate", "long_catch_rate"]
    STYLE = [c for c in STYLE if df[c].notna().mean() >= 0.8]
    print(f"WR seasons {len(df):,} (targets>=50, 2009-24); {len(STYLE)} style columns\n")

    # era-fair: percentile within year
    Xp = pd.DataFrame({c: df.groupby("year")[c].rank(pct=True) for c in STYLE})
    Xp = Xp.fillna(Xp.median())

    # ---- cluster the columns by |Spearman| into bundles ("types") ----
    corr = np.corrcoef(Xp.rank().to_numpy().T)
    dist = 1 - np.abs(np.nan_to_num(corr))
    np.fill_diagonal(dist, 0)
    Zc = linkage(squareform(dist, checks=False), method="average")
    lab = fcluster(Zc, t=args.tight, criterion="distance")
    bundles = {}
    for b in np.unique(lab):
        members = [STYLE[i] for i in range(len(STYLE)) if lab[i] == b]
        bundles[b] = members
    print("=" * 90)
    print(f"DATA-FOUND RECEIVER-TYPE BUNDLES (cut |corr|>={1-args.tight:.2f})")
    print("=" * 90)
    names = {}
    for b, m in bundles.items():
        s = set(m)
        if s & {"target_share", "air_yards_share", "wopr"}:
            nm = "Volume / target-hog"
        elif s & {"yards_per_reception", "explosive_catch_rate", "long_catch_rate",
                  "deep_catch_rate"}:
            nm = "Big-play / vertical"
        elif "adot" in s:
            nm = "Downfield orientation"
        elif s & {"short_catch_rate", "mid_catch_rate", "yac_share"}:
            nm = "Short / underneath"
        else:
            nm = "type-" + "+".join(m[:2])
        while nm in names.values():
            nm += " #2"
        names[b] = nm
        print(f"  [{nm}]  {', '.join(m)}")

    # ---- composite per bundle, sign-aligned to adot-direction if present ----
    def composite(members):
        M = Xp[members].to_numpy(float)
        signs = [1.0] + [np.sign(np.corrcoef(M[:, 0], M[:, j])[0, 1] or 1)
                         for j in range(1, M.shape[1])]
        return (M * np.array(signs)).mean(axis=1)
    for b, m in bundles.items():
        df[f"_b{b}"] = composite(m)

    # ---- trend of each bundle's correlation with fantasy PPG ----
    for score, lbl in [("ppg_season_4pt_half", "HALF PPR"),
                       ("ppg_season_4pt_ppr", "FULL PPR"),
                       ("ppg_season_4pt_0ppr", "0 PPR (std)")]:
        rows = []
        for b, m in bundles.items():
            yrly = []
            for yr, g in df.groupby("year"):
                yrly.append((yr, C.spearman(g[f"_b{b}"], g[score])))
            yr = np.array([y for y, _ in yrly], float)
            rr = np.array([r for _, r in yrly], float)
            ok = np.isfinite(rr)
            slope = np.polyfit(yr[ok], rr[ok], 1)[0] * 10
            early = np.nanmean(rr[yr <= 2012]); late = np.nanmean(rr[yr >= 2021])
            rows.append({"receiver type": names[b],
                         "r_2009-12": round(early, 2), "r_2021-24": round(late, 2),
                         "change": round(late - early, 2),
                         "slope/dec": round(slope, 2),
                         "verdict": "BUFF" if late - early > 0.05 else
                                    "NERF" if late - early < -0.05 else "flat"})
        t = pd.DataFrame(rows).sort_values("change", ascending=False)
        print("\n" + "=" * 90)
        print(f"{lbl}: each receiver-type bundle's Spearman with fantasy PPG, early vs late")
        print("=" * 90)
        print(t.to_string(index=False))
        if lbl == "HALF PPR":
            t.to_json(C.SCRATCH / "wr_type_buff_nerf.json", orient="records", indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
