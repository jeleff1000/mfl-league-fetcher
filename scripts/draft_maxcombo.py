#!/usr/bin/env python3
"""Definitive test: what is the MOST manager-separating linear combination of the
whole advanced column set, and how reliable is even that — out of sample?

Rather than hand-hunt combos, solve for the direction that maximizes between-
manager vs within-manager variance (a generalized eigenproblem = the best combo
the data can form). Learn it on train drafts, then measure the one-way ICC of
that FIXED direction on held-out drafts. If even the optimal combo's out-of-
sample reliability doesn't beat age (ICC 0.26), no linear combo of these columns
carries meaningful per-manager signal we're missing.

    python scripts/draft_maxcombo.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from scipy.linalg import eigh

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_reliability import icc1, n_for  # noqa: E402
from draft_attribute_sweep import latest_v26_weekly, load_season_features  # noqa: E402

EARLY = {"r1", "r2", "r3", "r4_5"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--test-from", type=int, default=2021)
    ap.add_argument("--ridge", type=float, default=0.05)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    feat, feats = load_season_features(latest_v26_weekly())
    feat = feat.rename(columns={"year": "season"}).drop(columns=["position"])
    p = pd.read_parquet(args.data, columns=["chosen", "year", "db_name", "manager_key",
                                            "cand_nflid", "cand_pos", "round_bucket",
                                            "prior_pos_pctl", "is_rookie", "reach", "age"])
    p = p[(p["chosen"] == 1) & p["round_bucket"].isin(EARLY)].rename(columns={"cand_nflid": "NFL_player_id"})
    p["season"] = p["year"] - 1
    m = p.merge(feat, on=["NFL_player_id", "season"], how="left")
    # widen: advanced cols + age + production + value (so the solver could rediscover age)
    m["floor"] = np.where(m["is_rookie"] == 1, 0.0, m["prior_pos_pctl"])
    cols = feats + ["floor", "age", "reach"]
    # percentile within (pos, season), leaguemate-center within (db, year)
    dev = []
    for c in cols:
        pct = m.groupby(["cand_pos", "season"])[c].rank(pct=True)
        lm = pct.groupby([m["db_name"], m["season"]]).transform("mean")
        m[c + "_d"] = (pct - lm)
        dev.append(c + "_d")
    # per (manager, draft) mean vector over early picks
    M = m.groupby(["db_name", "manager_key", "year"])[dev].mean().reset_index()
    M[dev] = M[dev].fillna(0.0)
    M["mgr"] = M["db_name"] + "|" + M["manager_key"]

    tr = M[M["year"] < args.test_from]
    te = M[M["year"] >= args.test_from]
    # standardize on train
    mu, sd = tr[dev].mean().to_numpy(), tr[dev].std().replace(0, 1).to_numpy()
    Xtr = (tr[dev].to_numpy() - mu) / sd
    Xte = (te[dev].to_numpy() - mu) / sd
    tr = tr.assign(**{f"z{i}": Xtr[:, i] for i in range(len(dev))})

    # between vs within manager covariance on train (managers with >=2 drafts)
    cnt = tr.groupby("mgr").size()
    keep = cnt[cnt >= 2].index
    t2 = tr[tr["mgr"].isin(keep)]
    Z = t2[[f"z{i}" for i in range(len(dev))]].to_numpy()
    mkey = t2["mgr"].to_numpy()
    mean_by = pd.DataFrame(Z, index=mkey).groupby(level=0).transform("mean").to_numpy()
    within = Z - mean_by
    mmeans = pd.DataFrame(Z, index=mkey).groupby(level=0).mean().to_numpy()
    Sb = np.cov(mmeans, rowvar=False)
    Sw = np.cov(within, rowvar=False) + args.ridge * np.eye(len(dev))

    # generalized eig: maximize w'Sb w / w'Sw w
    evals, evecs = eigh(Sb, Sw)
    order = np.argsort(evals)[::-1]

    print("\n" + "=" * 72)
    print("MAX-SEPARATION LINEAR COMBO — best combo the data can form, OOS")
    print("=" * 72)
    rows = []
    for rank in range(3):
        w = evecs[:, order[rank]]
        te_val = Xte @ w
        d = te.assign(v=te_val)[["db_name", "manager_key", "year", "v"]].copy()
        d["dev"] = d["v"]  # already leaguemate-centered upstream
        d["mgr"] = d["db_name"] + "|" + d["manager_key"]
        icc, nm, n = icc1(d, "mgr", "dev")
        n7 = n_for(icc, 0.7)
        load = pd.Series(w, index=[c[:-2] for c in dev]).abs().sort_values(ascending=False).head(5)
        top = ", ".join(f"{k}" for k in load.index)
        print(f"\ncombo #{rank+1}: OOS reliability (ICC) = {icc:.3f}   "
              f"N@0.7 = {'inf' if not np.isfinite(n7) else int(np.ceil(n7))}   (#mgrs {nm:,})")
        print(f"  top columns: {top}")
        rows.append({"rank": rank + 1, "oos_icc": round(icc, 4),
                     "n_for_0.7": None if not np.isfinite(n7) else int(np.ceil(n7)),
                     "top_columns": list(load.index)})
    print("\ncompare: age alone 0.261 (N@0.7=7); best auction axis spend-timing 0.197 (10)")
    print("if these combos' OOS ICC <= ~age, no linear combo of these columns is a")
    print("per-manager tell we're missing.")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_maxcombo.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_maxcombo.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
