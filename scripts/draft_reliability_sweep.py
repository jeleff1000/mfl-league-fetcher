#!/usr/bin/env python3
"""Reliability of the BIG-SWEEP discovered axes — same ICC / drafts-needed lens
as draft_reliability.py, applied to the sweep + factor + combo findings.

Those axes cleared a shuffle-null (real at POPULATION scale across thousands of
managers), but their persistence was tiny (0.01-0.018). This asks the product
question: how many drafts would ONE manager need before their lean on these axes
is reliable? Expectation: many more than age/value — i.e. these are
population/research signals, not per-manager product signatures.

    python scripts/draft_reliability_sweep.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_reliability import icc1, leaguemate_center, n_for  # noqa: E402
from draft_attribute_sweep import latest_v26_weekly, load_season_features  # noqa: E402

NEED = ["ngs_avg_separation", "ngs_avg_cushion", "ngs_avg_yac_above_expectation",
        "air_yards_share", "receiving_adot", "rushing_scrambles", "passing_cpoe",
        "receiving_epa", "total_epa"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    feat, feats = load_season_features(latest_v26_weekly())
    feat = feat.rename(columns={"year": "season"}).drop(columns=["position"])
    picks = pd.read_parquet(args.data, columns=["chosen", "year", "db_name",
                                                "manager_key", "cand_nflid", "cand_pos"])
    picks = picks[picks["chosen"] == 1].rename(columns={"cand_nflid": "NFL_player_id"})
    picks["season"] = picks["year"] - 1
    m = picks.merge(feat, on=["NFL_player_id", "season"], how="left")
    for c in [c for c in NEED if c in feats]:
        m["p_" + c] = m.groupby(["cand_pos", "season"])[c].rank(pct=True)

    def col(c):
        return m["p_" + c] if ("p_" + c) in m else pd.Series(np.nan, index=m.index)

    # discovered axes (single columns + the PCA/combo directions)
    m["receiving_epa"] = col("receiving_epa")
    m["total_epa"] = col("total_epa")
    m["sep_vs_deep"] = (np.nanmean(np.c_[col("ngs_avg_separation"), col("ngs_avg_cushion"),
                                         col("ngs_avg_yac_above_expectation")], axis=1)
                        - np.nanmean(np.c_[col("air_yards_share"), col("receiving_adot")], axis=1))
    m["mobile_eff_qb"] = np.nanmean(np.c_[col("rushing_scrambles"), col("passing_cpoe"),
                                          col("receiving_epa")], axis=1)

    rows = []
    for axis in ("receiving_epa", "total_epa", "sep_vs_deep", "mobile_eff_qb"):
        s = m[["db_name", "manager_key", "year", axis]].rename(columns={axis: "v"}).copy()
        s["dev"] = leaguemate_center(s, "v")
        md = s.dropna(subset=["dev"]).groupby(["db_name", "manager_key", "year"])["dev"].mean().reset_index()
        md["mgr"] = md["db_name"] + "|" + md["manager_key"]
        icc, mgr, n = icc1(md, "mgr", "dev")
        rows.append((axis, mgr, icc, n_for(icc, 0.7), n_for(icc, 0.8)))

    print("\n" + "=" * 70)
    print("RELIABILITY OF BIG-SWEEP AXES (snake; same ICC lens as the curated axes)")
    print("=" * 70)
    print(f"{'sweep axis':16} {'#mgrs':>6} {'1-draft rel':>12} {'N@0.7':>7} {'N@0.8':>7}")
    for axis, mgr, icc, n7, n8 in rows:
        f7 = "  inf" if not np.isfinite(n7) else f"{int(np.ceil(n7)):>5}"
        f8 = "  inf" if not np.isfinite(n8) else f"{int(np.ceil(n8)):>5}"
        print(f"{axis:16} {mgr:>6,} {icc:>12.3f} {f7:>7} {f8:>7}")
    print("\ncompare: snake age ICC=0.261 (N@0.7=7); value 0.140 (15); floor 0.032 (70)")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_reliability_sweep.json").write_text(json.dumps(
        [{"axis": a, "n_mgrs": mgr, "icc_1draft": round(i, 4),
          "n_for_0.7": None if not np.isfinite(n7) else int(np.ceil(n7)),
          "n_for_0.8": None if not np.isfinite(n8) else int(np.ceil(n8))}
         for a, mgr, i, n7, n8 in rows], indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_reliability_sweep.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
