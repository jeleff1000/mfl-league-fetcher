#!/usr/bin/env python3
"""Auction's own tells: (A) per-manager SPEND SHAPE, (B) league-wide NOMINATION
ORDER. Snake tells are about which player at a pick slot; auction identity is
about how you allocate a budget and how the room nominates.

A. SPEND SHAPE (manager, leaguemate-relative, leave-future-out persistence vs
   shuffle-null): stars-and-scrubs concentration (top-3 share, HHI), positional
   budget allocation (RB/WR/QB/TE share of spend), number of big buys.
B. NOMINATION ORDER (league personality): in an auction the pick sequence is the
   order players come off the board. studs-first index = corr(order, cost)
   (negative => expensive won early); plus which positions get nominated early,
   each league vs the auction fleet.

    python scripts/draft_auction_signatures.py
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import draft_choice_baselines  # noqa: E402,F401  (preamble: multi_league path + .env)
from draft_choice_baselines import canon_pos  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def fetch_auction(reader: FlyReader) -> pd.DataFrame:
    sql = """
    WITH d AS (
        SELECT db_name, COALESCE(franchise_id, manager) AS manager_key, manager, year,
               pick, COALESCE(cost,0) AS cost, UPPER(COALESCE(position,'')) AS position,
               NFL_player_id,
               AVG(CASE WHEN COALESCE(cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY db_name, year) AS auction_share
        FROM public.draft
        WHERE COALESCE(is_keeper,0)=0 AND pick IS NOT NULL AND manager IS NOT NULL AND year IS NOT NULL
    )
    SELECT db_name, manager_key, manager, year, pick, cost, position, NFL_player_id
    FROM d WHERE auction_share >= 0.25
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


# --------------------------------------------------------------------------- #
# A. manager spend-shape persistence
# --------------------------------------------------------------------------- #

def spend_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["cost"] > 0].copy()
    df["cpos"] = df["position"].map(canon_pos)
    rows = []
    for (db, mk, yr), g in df.groupby(["db_name", "manager_key", "year"], sort=False):
        total = g["cost"].sum()
        if total <= 0 or len(g) < 4:
            continue
        c = np.sort(g["cost"].to_numpy())[::-1]
        share = c / total
        pos_share = g.groupby("cpos")["cost"].sum() / total
        rows.append({
            "db_name": db, "manager_key": mk, "year": yr,
            "top1_share": share[0], "top3_share": share[:3].sum(),
            "hhi": float((share ** 2).sum()), "n_big": int((share >= 0.15).sum()),
            "rb_share": pos_share.get("RB", 0.0), "wr_share": pos_share.get("WR", 0.0),
            "qb_share": pos_share.get("QB", 0.0), "te_share": pos_share.get("TE", 0.0),
        })
    return pd.DataFrame(rows)


def persistence_sweep(feat: pd.DataFrame, cols: list[str], test_from: int, min_prior: int) -> list[dict]:
    # leaguemate-center each feature within (league, year) leave-one-out
    for c in cols:
        gsum = feat.groupby(["db_name", "year"])[c].transform("sum")
        gn = feat.groupby(["db_name", "year"])[c].transform("count")
        feat[c + "_d"] = np.where(gn >= 3, feat[c] - (gsum - feat[c]) / (gn - 1), np.nan)
    train = feat[feat["year"] < test_from]
    test = feat[feat["year"] >= test_from]
    rng = np.random.default_rng(0)
    out = []
    for c in cols:
        d = c + "_d"
        tr = train.dropna(subset=[d]); te = test.dropna(subset=[d])
        mgr = tr.groupby("manager_key")[d].agg(["mean", "size"])
        mgr = mgr[mgr["size"] >= min_prior]
        te = te[te["manager_key"].isin(mgr.index)]
        if len(te) < 200:
            continue
        pred = te["manager_key"].map(mgr["mean"]).to_numpy()
        target = te[d].to_numpy()
        if pred.std() < 1e-9:
            continue
        persist = float(np.corrcoef(pred, target)[0, 1])
        keys = mgr.index.to_numpy(); vals = mgr["mean"].to_numpy()
        nulls = [np.corrcoef(te["manager_key"].map(dict(zip(keys, rng.permutation(vals)))).to_numpy(), target)[0, 1]
                 for _ in range(30)]
        z = (persist - np.mean(nulls)) / (np.std(nulls) + 1e-12)
        out.append({"feature": c, "n_test": int(len(te)), "persist": round(persist, 3), "z": round(float(z), 1)})
    return sorted(out, key=lambda r: abs(r["z"]), reverse=True)


# --------------------------------------------------------------------------- #
# B. league nomination-order personality
# --------------------------------------------------------------------------- #

def nomination_personality(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df[df["cost"] > 0].copy()
    df["cpos"] = df["position"].map(canon_pos)
    league_year = []
    for (db, yr), g in df.groupby(["db_name", "year"], sort=False):
        if len(g) < 20:
            continue
        order = g["pick"].rank()
        # studs-first: Spearman(order, cost) — negative = expensive came off early
        sf = float(pd.Series(order.to_numpy()).corr(pd.Series(g["cost"].to_numpy()), method="spearman"))
        league_year.append({"db_name": db, "year": yr, "studs_first_corr": sf})
    ly = pd.DataFrame(league_year)
    league = ly.groupby("db_name").agg(studs_first=("studs_first_corr", "mean"),
                                       yrs=("studs_first_corr", "size")).reset_index()
    # position nomination timing: avg pick-stage per (league, position)
    df["stage"] = df.groupby(["db_name", "year"])["pick"].transform(lambda s: s.rank(pct=True))
    postime = (df[df["cpos"].isin(["QB", "RB", "WR", "TE"])]
               .groupby(["db_name", "cpos"])["stage"].mean().reset_index())
    return league, postime


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--test-from", type=int, default=2020)
    ap.add_argument("--min-prior", type=int, default=2)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    reader = FlyReader()
    print("fetching auction drafts...", flush=True)
    df = fetch_auction(reader)
    print(f"  {len(df):,} picks in auction drafts; "
          f"{df.groupby(['db_name','year']).ngroups:,} auction drafts, "
          f"{df['db_name'].nunique():,} leagues", flush=True)

    # ---- A ----
    feat = spend_features(df)
    cols = ["top1_share", "top3_share", "hhi", "n_big", "rb_share", "wr_share", "qb_share", "te_share"]
    sweep = persistence_sweep(feat, cols, args.test_from, args.min_prior)
    print("\n" + "=" * 66)
    print("A. MANAGER SPEND-SHAPE persistence (leaguemate-relative, OOS vs null)")
    print("=" * 66)
    print(f"{'feature':14} {'n_test':>7} {'persist':>8} {'z':>6}")
    for r in sweep:
        star = "  <-- real" if abs(r["z"]) >= 3 else ""
        print(f"{r['feature']:14} {r['n_test']:>7,} {r['persist']:>8.3f} {r['z']:>6.1f}{star}")

    # ---- B ----
    league, postime = nomination_personality(df)
    print("\n" + "=" * 66)
    print("B. LEAGUE NOMINATION ORDER (studs-first index = corr(order, cost))")
    print("   negative = expensive players come off the board EARLY")
    print("=" * 66)
    lg = league[league["yrs"] >= 2]
    print(f"fleet mean studs-first corr: {lg['studs_first'].mean():+.3f}  "
          f"(across {len(lg)} auction leagues, >=2 yrs)")
    print("\nmost STUDS-FIRST leagues (expensive early):")
    for _, r in lg.nsmallest(6, "studs_first").iterrows():
        print(f"  {r['db_name'][:34]:34} {r['studs_first']:+.3f} ({int(r['yrs'])}yr)")
    print("\nmost BUDGET-DRAIN leagues (cheap/scrubs nominated early):")
    for _, r in lg.nlargest(6, "studs_first").iterrows():
        print(f"  {r['db_name'][:34]:34} {r['studs_first']:+.3f} ({int(r['yrs'])}yr)")
    print("\nposition nomination timing (fleet avg pick-stage, lower = earlier):")
    pt = postime.groupby("cpos")["stage"].mean()
    for pos in ["QB", "RB", "WR", "TE"]:
        if pos in pt:
            print(f"  {pos:4} {pt[pos]:.3f}")

    out_dir = Path(args.out) if args.out else Path(".")
    (out_dir / "draft_auction_signatures.json").write_text(json.dumps(
        {"spend_shape": sweep, "fleet_studs_first": float(lg["studs_first"].mean()),
         "position_nomination_stage": {k: round(float(v), 3) for k, v in pt.items()}}, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_auction_signatures.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
