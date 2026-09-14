#!/usr/bin/env python3
"""Archetypes by ROSTER SLOT: what type does a manager take at RB1 vs RB2, WR1
vs WR2 vs WR3 — and who consistently chases which slot-type.

Instead of conditioning on draft-capital tier, we label each pick by the
manager's Nth player at that position (their WR1, WR2, ...), then ask two
things:
  1. STRUCTURAL: fleet-average type by slot — do people take a high-floor WR1
     and a deeper / younger WR2? (the shape everyone shares)
  2. MANAGER: is the type at a given slot a persistent manager tell (leave-
     future-out vs the fleet-at-slot baseline, vs a shuffle-null)? i.e. can we
     say "this manager's WR2 is always a deep threat"?

Axes: floor (prior-season production tier), deep (prior air-yards share, WR/TE),
value posture (reach vs wait), age. Reads picks from the built candidate set;
air-yards from the local v26 table.

    python scripts/draft_slot_archetypes.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_archetype_lift import leave_future_out_lift  # noqa: E402
from draft_attribute_sweep import latest_v26_weekly  # noqa: E402
import duckdb  # noqa: E402

SLOT_POS = ("QB", "RB", "WR", "TE")


def slot_label(pos: str, idx: int) -> str:
    cap = {"QB": 2, "TE": 2, "RB": 3, "WR": 4}.get(pos, 3)
    return f"{pos}{cap}+" if idx >= cap else f"{pos}{idx}"


def load_airyards(weekly: str) -> pd.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT NFL_player_id, year AS season,
               AVG(air_yards_share) AS air_yards_share, AVG(receiving_adot) AS adot
        FROM read_parquet('{weekly}')
        WHERE NFL_player_id IS NOT NULL AND year IS NOT NULL
        GROUP BY NFL_player_id, year
    """).fetchdf()
    con.close()
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--test-from", type=int, default=2021)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    p = pd.read_parquet(args.data, columns=["chosen", "year", "db_name", "manager_key",
                                            "cand_nflid", "cand_pos", "seq",
                                            "prior_pos_pctl", "is_rookie", "reach", "age"])
    p = p[(p["chosen"] == 1) & p["cand_pos"].isin(SLOT_POS)].copy()
    p["season"] = p["year"] - 1
    ay = load_airyards(latest_v26_weekly())
    p = p.merge(ay, left_on=["cand_nflid", "season"], right_on=["NFL_player_id", "season"], how="left")

    # roster-slot index: manager's Nth player at this position, in draft order
    p = p.sort_values(["db_name", "manager_key", "year", "cand_pos", "seq"])
    p["idx"] = p.groupby(["db_name", "manager_key", "year", "cand_pos"]).cumcount() + 1
    p["slot"] = [slot_label(pos, i) for pos, i in zip(p["cand_pos"], p["idx"])]
    p["tier"] = p["slot"]                         # leave_future_out_lift groups on `tier`

    # percentiles for continuous axes (within position, season)
    p["ays_pct"] = p.groupby(["cand_pos", "season"])["air_yards_share"].rank(pct=True)

    # ---- STRUCTURAL: fleet-average type by slot ----
    def floor_num(r):
        return np.nan if r["is_rookie"] == 1 else r["prior_pos_pctl"]
    p["floor_num"] = p.apply(floor_num, axis=1)
    struct = (p.groupby("slot")
              .agg(n=("slot", "size"),
                   rookie_rate=("is_rookie", "mean"),
                   floor_pctl=("prior_pos_pctl", "mean"),
                   deep_air_pctl=("ays_pct", "mean"),
                   mean_age=("age", "mean"),
                   reach_rate=("reach", lambda s: (s >= 0.05).mean()))
              .reset_index())
    order = ["QB1", "QB2+", "RB1", "RB2", "RB3+", "WR1", "WR2", "WR3", "WR4+", "TE1", "TE2+"]
    struct["o"] = struct["slot"].map({s: i for i, s in enumerate(order)})
    struct = struct.sort_values("o").drop(columns="o")
    print("\n" + "=" * 78)
    print("STRUCTURAL: fleet-average player TYPE by roster slot")
    print("=" * 78)
    print(f"{'slot':6} {'n':>8} {'rookie%':>8} {'floor':>7} {'deep_air':>9} {'age':>6} {'reach%':>7}")
    for _, r in struct.iterrows():
        print(f"{r['slot']:6} {int(r['n']):>8,} {r['rookie_rate']:>8.2f} {r['floor_pctl']:>7.2f} "
              f"{r['deep_air_pctl']:>9.2f} {r['mean_age']:>6.1f} {r['reach_rate']:>7.2f}")

    # ---- MANAGER: is slot-type a persistent tell? ----
    def cls_floor(i):
        if p["is_rookie"].iat[i] == 1:
            return "rookie"
        v = p["prior_pos_pctl"].iat[i]
        return "unknown" if np.isnan(v) else ("high_floor" if v >= 0.6 else ("mid" if v >= 0.35 else "low"))

    def cls_deep(i):
        v = p["ays_pct"].iat[i]
        if np.isnan(v):
            return "na"
        return "deep" if v >= 0.66 else ("short" if v <= 0.33 else "balanced")

    def cls_value(i):
        r = p["reach"].iat[i]
        return "unknown" if np.isnan(r) else ("reach" if r >= 0.05 else ("value" if r <= -0.05 else "market"))

    def cls_age(i):
        a = p["age"].iat[i]
        return "unknown" if np.isnan(a) else ("young" if a < 24 else ("prime" if a < 28 else "vet"))

    n = len(p)
    axes = {
        "floor": np.array([cls_floor(i) for i in range(n)]),
        "deep_WRTE": np.array([cls_deep(i) for i in range(n)]),
        "value": np.array([cls_value(i) for i in range(n)]),
        "age": np.array([cls_age(i) for i in range(n)]),
    }
    pr = p.reset_index(drop=True)
    out = _lift_by_slot(pr, axes, order, args.test_from)

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_slot_archetypes.json").write_text(
        json.dumps({"structural": struct.to_dict("records"), "persistence": out}, indent=2, default=str),
        encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_slot_archetypes.json'}")
    return 0


def _lift_by_slot(df, axes, order, test_from):
    """Leave-future-out manager-modal vs fleet-modal accuracy per (axis, slot)."""
    res = leave_future_out_lift(df, axes)  # keyed by axis -> slot -> metrics
    print("\n" + "=" * 78)
    print("MANAGER TELL by slot: does knowing the manager beat the fleet-at-slot?")
    print("(leave-future-out, managers with prior history; lift = manager - fleet acc)")
    print("=" * 78)
    for axis, slots in res.items():
        rows = [(s, slots[s]) for s in order if s in slots and slots[s].get("naive_acc_hist") is not None]
        rows = [(s, m) for s, m in rows if m["n_hist"] >= 400]
        if not rows:
            continue
        print(f"\n{axis.upper()}")
        print(f"  {'slot':6} {'n_hist':>8} {'fleet':>7} {'manager':>8} {'lift':>7}")
        for s, m in rows:
            flag = "  <--" if m["lift_hist"] and m["lift_hist"] >= 0.05 else ""
            print(f"  {s:6} {m['n_hist']:>8,} {m['naive_acc_hist']:>7.3f} "
                  f"{m['mgr_acc_hist']:>8.3f} {m['lift_hist']:>+7.3f}{flag}")
    return res


if __name__ == "__main__":
    sys.exit(main())
