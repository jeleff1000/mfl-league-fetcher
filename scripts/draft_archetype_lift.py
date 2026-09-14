#!/usr/bin/env python3
"""Reframed question: not "Josh Allen or Lamar Jackson," but "at a given draft
capital, does this manager reliably take a certain ARCHETYPE of player?"

The player-level model found manager identity adds ~0 to predicting the exact
player. This tests the coarser target: reduce each pick to archetype classes on
several axes, split by draft-capital tier, and measure the out-of-sample LIFT of
the MANAGER's own tendency over the LEAGUE base rate (leave-future-out, the same
method as draft_predictability_backtest.py).

For each (axis, capital tier) we predict the class of a manager's next pick:
  naive   = the league's modal class at that tier (prior years)
  manager = this manager's modal class at that tier (prior years; else naive)
The lift is manager-accuracy minus naive-accuracy on the subset where the
manager actually has history — i.e., does knowing WHO is picking beat knowing
only the round?

Reads the chosen picks from the built candidate dataset (draft_choice_dataset).

    python scripts/draft_archetype_lift.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

import numpy as np
import pandas as pd

TIERS = {"r1": "early", "r2": "early", "r3": "early",
         "r4_5": "mid", "r6_8": "mid", "r9plus": "late"}


def archetypes(df: pd.DataFrame) -> dict[str, np.ndarray]:
    """Class label per pick on each archetype axis."""
    pos = df["cand_pos"].to_numpy()
    pctl = pd.to_numeric(df["prior_pos_pctl"], errors="coerce").to_numpy()
    rook = df["is_rookie"].to_numpy()
    age = pd.to_numeric(df["age"], errors="coerce").to_numpy()
    ndo = pd.to_numeric(df["nfl_draft_overall"], errors="coerce").to_numpy()
    reach = pd.to_numeric(df["reach"], errors="coerce").to_numpy()

    def prod(i):
        if rook[i] == 1:
            return "rookie"
        p = pctl[i]
        if np.isnan(p):
            return "unknown"
        return "proven" if p >= 0.65 else ("middling" if p >= 0.35 else "bounceback")

    def age_c(i):
        a = age[i]
        if np.isnan(a):
            return "unknown"
        return "young" if a < 24 else ("prime" if a < 28 else "vet")

    def ped(i):
        d = ndo[i]
        if np.isnan(d):
            return "undrafted"
        return "r1_pedigree" if d <= 32 else ("early_pick" if d <= 100 else "late_pick")

    def val(i):
        r = reach[i]
        if np.isnan(r):
            return "unknown"
        return "reach" if r >= 0.05 else ("value" if r <= -0.05 else "market")

    n = len(df)
    return {
        "position": pos,
        "production": np.array([prod(i) for i in range(n)]),
        "age": np.array([age_c(i) for i in range(n)]),
        "pedigree": np.array([ped(i) for i in range(n)]),
        "value_vs_adp": np.array([val(i) for i in range(n)]),
    }


def _modal(counter: Counter) -> str | None:
    return counter.most_common(1)[0][0] if counter else None


def run(df: pd.DataFrame) -> dict:
    df = df[df["chosen"] == 1].copy()
    df["tier"] = df["round_bucket"].map(TIERS)
    df = df[df["tier"].notna()].reset_index(drop=True)
    return leave_future_out_lift(df, archetypes(df))


def leave_future_out_lift(df: pd.DataFrame, axes: dict[str, np.ndarray]) -> dict:
    """Manager-modal vs league-modal class accuracy per (axis, capital tier),
    leave-future-out. `df` needs columns year, manager_key, tier aligned to the
    `axes` class arrays. Shared by the archetype scripts (common util)."""
    year = df["year"].to_numpy()
    tier = df["tier"].to_numpy()
    mkey = df["manager_key"].to_numpy()
    order = np.argsort(year, kind="stable")   # ascending years for leave-future-out

    axis_names = list(axes)
    # prior-years-only counters
    league: dict = {a: defaultdict(Counter) for a in axis_names}          # [axis][tier]
    mgr: dict = {a: defaultdict(Counter) for a in axis_names}             # [axis][(mkey,tier)]
    # tallies: [axis][tier] -> dict of counts
    T = {a: defaultdict(lambda: Counter()) for a in axis_names}
    cur_year = None
    pending: list[int] = []

    def flush(idxs):
        for i in idxs:
            t = tier[i]
            for a in axis_names:
                cls = axes[a][i]
                league[a][t][cls] += 1
                mgr[a][(mkey[i], t)][cls] += 1

    for pos_i in order:
        i = int(pos_i)
        y = year[i]
        if cur_year is None:
            cur_year = y
        if y != cur_year:
            flush(pending)
            pending = []
            cur_year = y
        t = tier[i]
        for a in axis_names:
            actual = axes[a][i]
            lg_pred = _modal(league[a][t])
            mg_counter = mgr[a].get((mkey[i], t))
            has_hist = bool(mg_counter)
            mg_pred = _modal(mg_counter) if has_hist else lg_pred
            c = T[a][t]
            c["n"] += 1
            c["naive_hit"] += int(lg_pred == actual) if lg_pred is not None else 0
            c["mgr_hit"] += int(mg_pred == actual) if mg_pred is not None else 0
            if has_hist:
                c["n_hist"] += 1
                c["naive_hit_h"] += int(lg_pred == actual) if lg_pred is not None else 0
                c["mgr_hit_h"] += int(mg_pred == actual) if mg_pred is not None else 0
        pending.append(i)
    flush(pending)

    out: dict = {}
    for a in axis_names:
        out[a] = {}
        for t in list(T[a].keys()):          # whatever tiers were seen (capital or slot)
            c = T[a][t]
            if not c.get("n"):
                continue
            nh = c.get("n_hist", 0)
            out[a][t] = {
                "n": c["n"], "n_hist": nh,
                "naive_acc": round(c["naive_hit"] / c["n"], 4),
                "mgr_acc": round(c["mgr_hit"] / c["n"], 4),
                "naive_acc_hist": round(c["naive_hit_h"] / nh, 4) if nh else None,
                "mgr_acc_hist": round(c["mgr_hit_h"] / nh, 4) if nh else None,
                "lift_hist": round((c["mgr_hit_h"] - c["naive_hit_h"]) / nh, 4) if nh else None,
            }
    return out


def _report(out: dict) -> None:
    print("\n" + "=" * 78)
    print("ARCHETYPE-AT-CAPITAL: does the MANAGER beat the LEAGUE base rate?")
    print("(leave-future-out; accuracy on picks where the manager has prior history)")
    print("=" * 78)
    for axis, tiers in out.items():
        print(f"\n{axis.upper()}")
        print(f"  {'tier':6} {'n_hist':>8} {'league':>8} {'manager':>8} {'lift':>8}")
        for t in ("early", "mid", "late"):
            if t not in tiers:
                continue
            r = tiers[t]
            if r["naive_acc_hist"] is None:
                continue
            flag = "  <--" if r["lift_hist"] and r["lift_hist"] >= 0.03 else ""
            print(f"  {t:6} {r['n_hist']:>8,} {r['naive_acc_hist']:>8.3f} "
                  f"{r['mgr_acc_hist']:>8.3f} {r['lift_hist']:>+8.3f}{flag}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    df = pd.read_parquet(args.data, columns=[
        "chosen", "year", "manager_key", "round_bucket", "cand_pos",
        "prior_pos_pctl", "is_rookie", "age", "nfl_draft_overall", "reach"])
    out = run(df)
    _report(out)
    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_archetype_lift.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_archetype_lift.json'}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
