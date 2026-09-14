#!/usr/bin/env python3
"""The definitive 6x6: clearest strata (rows) x clearest predictor axes (cols).

Rows  = the 6 data-discovered RB archetypes (PCA->KMeans on 2010+ season
        features; volume-tier x receiving-role).
Cols  = the 6 distinct predictor bundles the feature-clustering surfaced, each a
        sign-aligned composite so **+ = predicts a BETTER next year**:
          Prior Yr PPG = last season's PPG (the "how good this year" axis / persistence)
          ADP          = -(next-year fleet ADP)
          youth       = -(age)
          pedigree    = -(draft_overall)   (UDFA treated as latest pick)
          receiving      = target_share + receptions + wopr
          explosive runs = rush_explosive_10 + rushing_40plus + rushing_long
Cell  = Spearman(composite, Y+1 PPG) within the archetype. Top value per row **bold**
        in spirit (marked *). This says, definitively, what is most predictive where.

    python scripts/rb_ppg_summary_matrix.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

import rb_ppg_common as C

pd.set_option("display.width", 200)


def zscore_cols(df, cols):
    """percentile-within-year then combine; returns a single composite series."""
    parts = [df.groupby("year")[c].rank(pct=True) for c in cols]
    return pd.concat(parts, axis=1).mean(axis=1)


def main() -> int:
    df = pd.read_parquet(C.SCRATCH / "rb_seasons_adp.parquet")
    df = df[(df["games_played"] >= 6) & df[C.LABEL].notna()
            & df[C.PERSIST].notna() & (df["year"] >= 2010)].copy()

    # ---- rediscover the 6 archetypes (PCA -> KMeans on realized features) ----
    axis_feats = [f for f in C.all_features()
                  if f in df.columns and df[f].notna().mean() >= 0.80]
    Xp = pd.DataFrame({f: df.groupby("year")[f].rank(pct=True) for f in axis_feats})
    Xp = Xp.fillna(Xp.median())
    S = PCA(n_components=0.90, random_state=0).fit_transform(StandardScaler().fit_transform(Xp))
    df["cl"] = KMeans(n_clusters=6, n_init=20, random_state=0).fit_predict(S)

    prof = df.groupby("cl").agg(car=("carries", "mean"), rec=("receptions", "mean"),
                                ppg=(C.PERSIST, "mean"))

    def name(car, rec):
        base = ("bellcow" if car >= 180 else "workhorse" if car >= 120
                else "committee" if car >= 55 else "backup")
        share = rec / (car + rec + 1e-9)
        if base in ("bellcow", "workhorse") and rec >= 40:
            return f"dual-threat {base}"
        if share >= 0.34:
            return f"receiving {base}"
        if share >= 0.20 and base == "committee":
            return f"pass-catching {base}"
        return f"early-down {base}" if base in ("bellcow", "workhorse", "committee") else f"deep {base}"
    names = {cl: name(r.car, r.rec) for cl, r in prof.iterrows()}
    df["arch"] = df["cl"].map(names)
    order = prof.sort_values("ppg", ascending=False).index
    row_order = [names[cl] for cl in order]

    # ---- build the 6 predictor composites (sign so + = better next year) ----
    df["draft_overall_f"] = df["draft_overall"].fillna(262.0)
    comps = {
        "Prior Yr PPG": df[C.PERSIST],
        "ADP": -df["adp_next"],
        "youth": -df["age"],
        "pedigree": -df["draft_overall_f"],
        "receiving": zscore_cols(df, ["target_share", "receptions", "wopr"]),
        "explosive runs": zscore_cols(df, ["rush_explosive_10", "rushing_40plus", "rushing_long"]),
    }
    for k, v in comps.items():
        df[f"_c_{k}"] = v.values if hasattr(v, "values") else v

    # ---- cells: Spearman(composite, Y+1 PPG) within each archetype ----
    rows = []
    for arch in row_order:
        sub = df[df["arch"] == arch]
        y = sub[C.LABEL].to_numpy(float)
        rec = {"archetype": arch, "n": len(sub),
               "car": int(prof.loc[df[df.arch==arch].cl.iloc[0], "car"]),
               "recU": int(prof.loc[df[df.arch==arch].cl.iloc[0], "rec"]),
               "ppgY1": round(float(np.nanmean(y)), 1)}
        for k in comps:
            x = sub[f"_c_{k}"].to_numpy(float)
            sp = C.spearman(x, y)
            rec[k] = round(sp, 2) if np.isfinite(sp) else np.nan
        rows.append(rec)
    tab = pd.DataFrame(rows).set_index("archetype")

    # ALL-RB reference row
    allrec = {"n": len(df), "car": int(df.carries.mean()), "recU": int(df.receptions.mean()),
              "ppgY1": round(df[C.LABEL].mean(), 1)}
    for k in comps:
        allrec[k] = round(C.spearman(df[f"_c_{k}"].to_numpy(float),
                                     df[C.LABEL].to_numpy(float)), 2)
    tab.loc["— ALL RBs —"] = allrec

    # mark the top predictor per row with *
    disp = tab.copy()
    pred_cols = list(comps)
    disp[pred_cols] = disp[pred_cols].astype(object)
    for idx in disp.index:
        vals = {c: abs(tab.loc[idx, c]) for c in pred_cols if pd.notna(tab.loc[idx, c])}
        if vals:
            top = max(vals, key=vals.get)
            disp.loc[idx, top] = f"{tab.loc[idx, top]:+.2f}*"
    for c in pred_cols:
        disp[c] = disp[c].map(lambda v: f"{v:+.2f}" if isinstance(v, (int, float)) and pd.notna(v) else v)

    print("=" * 108)
    print("DEFINITIVE 6x6 — clearest RB strata (rows) x clearest predictor axes (cols)")
    print("cells = Spearman(predictor, Y+1 PPG) within stratum;  + = predicts a better")
    print("next year;  * = the single most predictive axis for that stratum.  (2010+, half PPR)")
    print("=" * 108)
    print(disp.to_string())
    print("\ncolumn strength (avg |Spearman| across the 6 archetypes):")
    strong = {c: round(tab.loc[row_order, c].abs().mean(), 2) for c in pred_cols}
    for c, v in sorted(strong.items(), key=lambda t: -t[1]):
        print(f"   {c:12} {v:.2f}")
    tab.to_json(C.SCRATCH / "rb_ppg_summary_matrix.json", orient="index", indent=2)
    print(f"\nwrote rb_ppg_summary_matrix.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
