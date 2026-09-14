#!/usr/bin/env python3
"""Last sweep: which 3- and 4-COLUMN combos form an archetype managers persist
on (vs leaguemates) — sparse and interpretable, unlike the dense PCA axes.

For every 3- and 4-column bundle from a role/efficiency column set (raw volume
EXCLUDED so it can't just re-discover the production/style axis), we take the
bundle's dominant direction (PC1 of how managers differ on those columns),
score each manager's leaguemate-relative lean, and measure whether that lean
predicts their held-out picks. Ranked by z vs a shared shuffle-manager null;
with ~6k combos tested, the bar is Bonferroni-scale (z>=4).

    python scripts/draft_combo_sweep.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
from itertools import combinations
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_attribute_sweep import latest_v26_weekly, load_season_features  # noqa: E402

# role / efficiency / advanced only — NO raw volume (that IS the style axis)
COLS = [
    "ngs_avg_separation", "ngs_avg_cushion", "ngs_avg_time_to_throw",
    "ngs_rush_yards_over_expected", "ngs_avg_yac_above_expectation",
    "ngs_pct_share_intended_air_yards",
    "air_yards_share", "offense_snap_pct", "rushing_scrambles", "rz_targets",
    "receiving_adot", "receiving_yards_per_target", "rushing_yards_per_carry",
    "rushing_yards_before_contact", "receiving_yards_after_catch", "passing_cpoe",
    "receiving_epa", "rushing_epa", "passing_epa", "rec_success",
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--test-from", type=int, default=2021)
    ap.add_argument("--min-train-picks", type=int, default=12)
    ap.add_argument("--sizes", default="3,4")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    feat, feats = load_season_features(latest_v26_weekly())
    feat = feat.rename(columns={"year": "season"}).drop(columns=["position"])
    cols = [c for c in COLS if c in feats]

    picks = pd.read_parquet(args.data, columns=["chosen", "year", "db_name",
                                                "manager_key", "cand_nflid", "cand_pos"])
    picks = picks[picks["chosen"] == 1].rename(columns={"cand_nflid": "NFL_player_id"})
    picks["season"] = picks["year"] - 1
    m = picks.merge(feat, on=["NFL_player_id", "season"], how="left")

    dev = []
    for f in cols:
        pct = m.groupby(["cand_pos", "season"])[f].rank(pct=True)
        lm = pct.groupby([m["db_name"], m["season"]]).transform("mean")
        m[f + "_d"] = (pct - lm).fillna(0.0)
        dev.append(f + "_d")

    train = m[m["year"] < args.test_from]
    test = m[m["year"] >= args.test_from]
    cnt = train.groupby("manager_key").size()
    keep = cnt[cnt >= args.min_train_picks].index
    M = train[train["manager_key"].isin(keep)].groupby("manager_key")[dev].mean()
    mu, sd = M.mean().to_numpy(), M.std().replace(0, 1).to_numpy()
    Mz = (M.to_numpy() - mu) / sd                                   # managers x cols
    Dt = ((test[dev].to_numpy() - mu) / sd)                         # test picks x cols
    tmgr = test["manager_key"].to_numpy()
    in_m = np.array([mm in M.index for mm in tmgr])
    Dt, tmgr = Dt[in_m], tmgr[in_m]
    mrow = {mm: i for i, mm in enumerate(M.index)}
    tidx = np.array([mrow[mm] for mm in tmgr])
    rng = np.random.default_rng(0)
    perm = rng.permutation(len(M))                                  # shared null shuffle

    print(f"{len(M):,} managers, {len(Dt):,} test picks, {len(cols)} cols", flush=True)
    rows = []
    for k in [int(s) for s in args.sizes.split(",")]:
        for combo in combinations(range(len(cols)), k):
            c = list(combo)
            sub = Mz[:, c]
            # PC1 = dominant direction managers differ on within this bundle
            w, V = np.linalg.eigh(sub.T @ sub)
            pc = V[:, -1]
            mscore = sub @ pc                                       # per manager
            tscore = Dt[:, c] @ pc                                  # per test pick
            pred = mscore[tidx]
            if pred.std() < 1e-9:
                continue
            persist = float(np.corrcoef(pred, tscore)[0, 1])
            null = float(np.corrcoef(mscore[perm][tidx], tscore)[0, 1])
            rows.append({"cols": [cols[i] for i in c], "loading": np.round(pc, 2).tolist(),
                         "persist": persist, "null": null, "k": k})

    df = pd.DataFrame(rows)
    null_sd = df["null"].std() or 1e-9
    df["z"] = (df["persist"] - df["null"].mean()) / null_sd
    df = df.reindex(df["z"].abs().sort_values(ascending=False).index).reset_index(drop=True)

    print(f"\nnull_sd={null_sd:.4f}  Bonferroni bar for {len(df):,} combos ~ z>=4\n" + "=" * 84)
    for _, r in df.head(args.top).iterrows():
        sign = {c: ("+" if l >= 0 else "-") for c, l in zip(r["cols"], r["loading"])}
        combo = "  ".join(f"{sign[c]}{c}" for c in r["cols"])
        flag = "  <== robust" if abs(r["z"]) >= 4 else ""
        print(f"z={r['z']:+5.1f}  persist={r['persist']:+.3f}  [{r['k']}] {combo}{flag}")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    df.head(200).to_json(out_dir / "draft_combo_sweep.json", orient="records", indent=2)
    print(f"\nwrote {out_dir / 'draft_combo_sweep.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
