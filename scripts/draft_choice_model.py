#!/usr/bin/env python3
"""Step 3: does manager identity lift draft-pick prediction over the market?

Trains two rankers on the SAME candidate sets (draft_choice_dataset.py output)
and compares them out-of-sample (train year < --test-from, test on/after):

  market   : market + player + context + roster features   (NO manager identity)
  manager  : market model + the prior-years manager features
             (mgr_pos_rate, mgr_mean_reach, mgr_n_prior)

Both see identical candidates per pick, so the delta is a clean LIFT from
manager identity — the brief's success metric. We also recompute the ADP and
roster-need baselines on the very same candidate sets, so every number is
comparable. (Absolute top-k here is on the sampled pool = chosen + top-20 by
ADP, so it runs higher than the full-board floor in draft_choice_baselines.py;
the LIFT between models is what matters and is unaffected.)

Model = sklearn HistGradientBoostingClassifier (chosen vs not); candidates are
ranked within each pick by predicted P(chosen). No lightgbm dependency.

Memory-lean: reads only needed columns, filtered into train/test, builds
float32 numpy designs and frees the frames (the full parquet is ~10M rows).

    python scripts/draft_choice_model.py --data <dir>/draft_candidates.parquet
    python scripts/draft_choice_model.py --test-from 2023
"""
from __future__ import annotations

import argparse
import gc
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

NUMERIC = [
    "adp_stage", "reach", "adp_rank", "adp_known", "prior_pos_pctl", "is_rookie",
    "experience", "age", "nfl_draft_overall", "ras_score", "pick_stage",
    "pos_is_need", "superflex", "num_teams", "roster_have_pos",
]
CATEG = ["cand_pos", "round_bucket"]
MANAGER = ["mgr_pos_rate", "mgr_mean_reach", "mgr_n_prior"]


def _design(df: pd.DataFrame, cat_levels: dict[str, list]) -> tuple[np.ndarray, list[str], int]:
    """float32 matrix ordered NUMERIC, one-hot(CATEG), MANAGER (manager last so
    the market model is a contiguous left slice). Returns (X, colnames, n_market)."""
    blocks = [df[NUMERIC].to_numpy(dtype="float32")]
    names = list(NUMERIC)
    for c in CATEG:
        col = df[c].to_numpy()
        for lvl in cat_levels[c]:
            blocks.append((col == lvl).astype("float32")[:, None])
            names.append(f"{c}={lvl}")
    n_market = len(names)
    blocks.append(df[MANAGER].to_numpy(dtype="float32"))
    names += list(MANAGER)
    return np.concatenate(blocks, axis=1), names, n_market


def _rank_metrics(pick_codes: np.ndarray, chosen: np.ndarray, score: np.ndarray) -> tuple[float, float]:
    """top-1 / top-3 player accuracy: rank candidates within each pick by score
    (desc) and check the chosen row's rank."""
    tmp = pd.DataFrame({"g": pick_codes, "chosen": chosen, "s": score})
    tmp["rank"] = tmp.groupby("g")["s"].rank(ascending=False, method="first")
    ch = tmp.loc[tmp["chosen"] == 1, "rank"]
    return float((ch == 1).mean()), float((ch <= 3).mean())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True, help="draft_candidates.parquet")
    ap.add_argument("--test-from", type=int, default=2023, help="first test year")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    feat = NUMERIC + CATEG + MANAGER
    train = pd.read_parquet(args.data, columns=feat + ["chosen", "year"],
                            filters=[("year", "<", args.test_from)])
    test = pd.read_parquet(args.data, columns=feat + ["chosen", "pick_id"],
                           filters=[("year", ">=", args.test_from)])
    print(f"train rows {len(train):,}  test rows {len(test):,}  "
          f"test picks {test['pick_id'].nunique():,}", flush=True)
    cat_levels = {c: sorted(set(train[c].dropna().unique()) | set(test[c].dropna().unique()))
                  for c in CATEG}

    ytr = train["chosen"].to_numpy()
    Xtr, names, n_market = _design(train, cat_levels)
    del train
    gc.collect()

    yte = test["chosen"].to_numpy()
    pick_codes = pd.factorize(test["pick_id"], sort=False)[0]
    adp_rank = test["adp_rank"].to_numpy(dtype="float32")
    pos_need = test["pos_is_need"].to_numpy(dtype="float32")
    mgr_n_prior = test["mgr_n_prior"].to_numpy()
    Xte, _, _ = _design(test, cat_levels)
    del test
    gc.collect()

    results: dict[str, dict] = {}
    # --- baselines on the identical candidate sets ---
    results["adp"] = dict(zip(("top1", "top3"),
                              [round(v, 4) for v in _rank_metrics(pick_codes, yte, -adp_rank)]))
    results["roster_need"] = dict(zip(("top1", "top3"),
                                  [round(v, 4) for v in _rank_metrics(pick_codes, yte, -adp_rank + 1000.0 * pos_need)]))

    def fit_eval(ncols: int, tag: str) -> tuple[dict, np.ndarray]:
        clf = HistGradientBoostingClassifier(
            max_iter=250, learning_rate=0.06, max_leaf_nodes=63,
            min_samples_leaf=200, l2_regularization=1.0, random_state=0)
        clf.fit(Xtr[:, :ncols], ytr)
        s = clf.predict_proba(Xte[:, :ncols])[:, 1]
        t1, t3 = _rank_metrics(pick_codes, yte, s)
        print(f"  {tag:8} top1={t1:.4f} top3={t3:.4f}", flush=True)
        return {"top1": round(t1, 4), "top3": round(t3, 4), "clf": clf}, s

    print("fitting market model...", flush=True)
    market, s_market = fit_eval(n_market, "market")
    print("fitting manager model...", flush=True)
    manager, s_manager = fit_eval(Xtr.shape[1], "manager")
    results["market"] = {k: market[k] for k in ("top1", "top3")}
    results["manager"] = {k: manager[k] for k in ("top1", "top3")}

    lift1 = manager["top1"] - market["top1"]
    lift3 = manager["top3"] - market["top3"]
    results["lift_manager_over_market"] = {
        "top1_abs": round(lift1, 4), "top1_rel": round(lift1 / market["top1"], 4),
        "top3_abs": round(lift3, 4), "top3_rel": round(lift3 / market["top3"], 4),
    }

    # where the manager signal should help most: picks by managers with real history
    hist = mgr_n_prior >= 2
    if hist.any():
        m1, m3 = _rank_metrics(pick_codes[hist], yte[hist], s_market[hist])
        g1, g3 = _rank_metrics(pick_codes[hist], yte[hist], s_manager[hist])
        results["lift_on_managers_with_history"] = {
            "n_rows": int(hist.sum()),
            "market_top1": round(m1, 4), "manager_top1": round(g1, 4), "top1_abs": round(g1 - m1, 4),
            "market_top3": round(m3, 4), "manager_top3": round(g3, 4), "top3_abs": round(g3 - m3, 4),
        }

    # manager-feature importances (permutation on a test sample)
    try:
        from sklearn.inspection import permutation_importance
        rng = np.random.default_rng(0)
        idx = rng.choice(len(yte), size=min(40000, len(yte)), replace=False)
        pi = permutation_importance(
            manager["clf"], Xte[idx], yte[idx], n_repeats=3, random_state=0,
            scoring="average_precision", n_jobs=-1)
        imp = sorted(zip(names, pi.importances_mean), key=lambda kv: kv[1], reverse=True)
        results["top_importances"] = [{"feat": f, "imp": round(float(v), 5)} for f, v in imp[:14]]
    except Exception as e:  # pragma: no cover
        results["top_importances"] = f"skipped: {e}"

    _report(results)
    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_choice_model.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_choice_model.json'}")
    return 0


def _report(r: dict) -> None:
    print("\n" + "=" * 60)
    print("DRAFT-CHOICE MODEL  (player prediction on identical candidate sets)")
    print("=" * 60)
    print(f"{'predictor':22} {'top1':>7} {'top3':>7}")
    for name in ("adp", "roster_need", "market", "manager"):
        print(f"{name:22} {r[name]['top1']:>7.4f} {r[name]['top3']:>7.4f}")
    lm = r["lift_manager_over_market"]
    print(f"\nLIFT manager over market:  top1 {lm['top1_abs']:+.4f} "
          f"({lm['top1_rel']:+.1%})   top3 {lm['top3_abs']:+.4f} ({lm['top3_rel']:+.1%})")
    if "lift_on_managers_with_history" in r:
        h = r["lift_on_managers_with_history"]
        print(f"  managers w/ >=2 prior drafts ({h['n_rows']:,} rows): "
              f"top1 {h['market_top1']:.4f}->{h['manager_top1']:.4f} ({h['top1_abs']:+.4f}) | "
              f"top3 {h['market_top3']:.4f}->{h['manager_top3']:.4f} ({h['top3_abs']:+.4f})")
    if isinstance(r.get("top_importances"), list):
        print("\ntop feature importances (permutation, avg-precision):")
        for row in r["top_importances"]:
            print(f"  {row['feat']:22} {row['imp']:.5f}")


if __name__ == "__main__":
    import sys
    sys.exit(main())
