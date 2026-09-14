#!/usr/bin/env python3
"""Discover archetype AXES from column combos — not hand-designed — and test
which ones a manager persists on relative to his LEAGUEMATES.

Pipeline:
  1. each pick's advanced attributes -> percentile within (position, season)
  2. center each attribute within (league, season) => deviation from LEAGUEMATES
     (same draft pool / ADP / rules), so we isolate manager idiosyncrasy
  3. manager-preference matrix (manager x attribute) = mean deviation over their
     TRAIN-year picks; PCA on it => the latent axes of how managers DIFFER, each
     a combination of columns (read the loadings to interpret the archetype)
  4. validate: does a manager's TRAIN loading on axis k predict where their
     held-out TEST picks land on axis k? corr vs a shuffled-manager null.

The point: let column combos we'd never invent surface, and only trust the ones
that separate managers AND persist out of sample.

    python scripts/draft_archetype_factors.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_attribute_sweep import (  # noqa: E402  (reuse feature source + aggregation)
    FEATURES, latest_v26_weekly, load_season_features,
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--test-from", type=int, default=2021)
    ap.add_argument("--components", type=int, default=8)
    ap.add_argument("--min-train-picks", type=int, default=12)
    ap.add_argument("--null-iters", type=int, default=20)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    weekly = latest_v26_weekly()
    print("aggregating season features...", flush=True)
    feat, feats = load_season_features(weekly)
    feat = feat.rename(columns={"year": "season"}).drop(columns=["position"])

    picks = pd.read_parquet(args.data, columns=["chosen", "year", "db_name",
                                                "manager_key", "cand_nflid", "cand_pos"])
    picks = picks[picks["chosen"] == 1].rename(columns={"cand_nflid": "NFL_player_id"})
    picks["season"] = picks["year"] - 1
    m = picks.merge(feat, on=["NFL_player_id", "season"], how="left")

    # 1) percentile within (position, season); 2) deviation from leaguemates
    dev_cols = []
    for f in feats:
        pct = m.groupby(["cand_pos", "season"])[f].rank(pct=True)
        league_mean = pct.groupby([m["db_name"], m["season"]]).transform("mean")
        m[f + "_dev"] = (pct - league_mean)          # >0 = more than leaguemates that year
        dev_cols.append(f + "_dev")
    m[dev_cols] = m[dev_cols].fillna(0.0)             # missing attr = neutral vs leaguemates

    train = m[m["year"] < args.test_from]
    test = m[m["year"] >= args.test_from]
    # manager-preference matrix from TRAIN picks
    counts = train.groupby("manager_key").size()
    keep_mgr = counts[counts >= args.min_train_picks].index
    pref = (train[train["manager_key"].isin(keep_mgr)]
            .groupby("manager_key")[dev_cols].mean())
    print(f"{len(pref):,} managers x {len(dev_cols)} attrs -> PCA", flush=True)

    scaler = StandardScaler()
    X = scaler.fit_transform(pref.to_numpy())
    pca = PCA(n_components=args.components, random_state=0)
    S_train = pca.fit_transform(X)                    # manager train scores per axis
    loadings = pca.components_ / scaler.scale_        # back to attr space (per-unit-dev)

    # manager train score per axis, indexed by manager
    mgr_scores = {k: pd.Series(S_train[:, k], index=pref.index) for k in range(args.components)}

    # project each TEST pick's deviation vector onto each axis (raw attr loadings)
    Dtest = test[dev_cols].to_numpy()
    L = pca.components_.T                              # attrs x k (standardized space)
    Dtest_std = (Dtest - scaler.mean_) / scaler.scale_
    test_scores = Dtest_std @ L                        # test pick x k
    test_mgr = test["manager_key"].to_numpy()

    rng = np.random.default_rng(0)
    results = []
    for k in range(args.components):
        s = mgr_scores[k]
        mask = np.array([mm in s.index for mm in test_mgr])
        if mask.sum() < 1000:
            continue
        pred = np.array([s.get(mm, np.nan) for mm in test_mgr[mask]])
        target = test_scores[mask, k]
        ok = ~np.isnan(pred)
        pred, target = pred[ok], target[ok]
        if pred.std() < 1e-9:
            continue
        persistence = float(np.corrcoef(pred, target)[0, 1])
        # shuffle-null: permute manager->train-score
        keys = s.index.to_numpy(); vals = s.to_numpy(); nulls = []
        for _ in range(args.null_iters):
            perm = dict(zip(keys, rng.permutation(vals)))
            p = np.array([perm[mm] for mm in test_mgr[mask][ok]])
            nulls.append(np.corrcoef(p, target)[0, 1])
        z = (persistence - np.mean(nulls)) / (np.std(nulls) + 1e-12)
        # interpret: top +/- loadings (attributes, dev-space)
        lk = pd.Series(loadings[k], index=[c[:-4] for c in dev_cols]).sort_values()
        top_neg = [(n, round(v, 2)) for n, v in lk.head(4).items()]
        top_pos = [(n, round(v, 2)) for n, v in lk.tail(4).items()][::-1]
        results.append({
            "axis": k, "explained_var": round(float(pca.explained_variance_ratio_[k]), 3),
            "persistence": round(persistence, 4), "z_vs_null": round(float(z), 2),
            "n_test": int(len(pred)), "pos_pole": top_pos, "neg_pole": top_neg,
        })

    results.sort(key=lambda r: r["z_vs_null"], reverse=True)
    print("\n" + "=" * 82)
    print("DISCOVERED ARCHETYPE AXES (PCA of manager-vs-leaguemate preference vectors)")
    print("ranked by out-of-sample persistence vs shuffle-null")
    print("=" * 82)
    for r in results:
        star = "  <== real & persistent" if r["z_vs_null"] >= 3 else ""
        print(f"\nAxis {r['axis']}  var={r['explained_var']:.0%}  persist={r['persistence']:+.3f}  "
              f"z={r['z_vs_null']:+.1f}{star}")
        print(f"   + pole: {', '.join(f'{n} ({v:+})' for n, v in r['pos_pole'])}")
        print(f"   - pole: {', '.join(f'{n} ({v:+})' for n, v in r['neg_pole'])}")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_archetype_factors.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_archetype_factors.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
