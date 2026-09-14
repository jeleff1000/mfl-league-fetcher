#!/usr/bin/env python3
"""WHO consistently chases which slot-type — vs their own leaguemates.

For every manager x roster-slot x axis, we center the pick against the manager's
LEAGUEMATES at the SAME slot and season (leave-one-out mean), then keep only
signatures that are consistent across >=4 of the manager's drafts. Output is a
ranked, plain-language roster of manager slot signatures:

  "<manager> (<league>): his WR1 skews deep-threat vs his league — 6 drafts"

Axes: floor (prior-season production; rookies = 0 floor), deep (prior air-yards
share, WR/TE/receiving-back), age, value posture (reach vs wait).

    python scripts/draft_slot_signatures.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_slot_archetypes import load_airyards, slot_label, SLOT_POS  # noqa: E402
from draft_attribute_sweep import latest_v26_weekly  # noqa: E402
import draft_choice_baselines  # noqa: E402,F401  (preamble: multi_league path + .env)
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402

MIN_YEARS = 4
CONSISTENCY = 0.70
THRESH = {"floor": 0.12, "deep": 0.12, "age": 1.5, "value": 0.03}


def name_map() -> dict[tuple[str, str], str]:
    sql = """
    SELECT db_name, COALESCE(franchise_id, manager) AS manager_key,
           ARG_MAX(manager, year) AS nm
    FROM public.draft WHERE manager IS NOT NULL
    GROUP BY db_name, COALESCE(franchise_id, manager)
    """
    out = {}
    for r in FlyReader().query(sql, database="___leagues"):
        out[(str(r["db_name"]), str(r["manager_key"]))] = str(r["nm"])
    return out


def label(axis: str, slot: str, dev: float) -> str:
    pos = slot.rstrip("+0123456789")
    if axis == "floor":
        return (f"his {slot} is more proven than his league"
                if dev > 0 else f"gambles on {slot} — lower floor than his league")
    if axis == "deep":
        if dev > 0:
            return (f"his {slot} is a pass-catching back vs his league"
                    if pos == "RB" else f"his {slot} skews deep-threat vs his league")
        return (f"his {slot} is a between-the-tackles back vs his league"
                if pos == "RB" else f"his {slot} skews possession/short vs his league")
    if axis == "age":
        return (f"drafts the most veteran {slot}s in his league"
                if dev > 0 else f"drafts the youngest {slot}s in his league")
    return (f"reaches for {slot} vs his league"
            if dev > 0 else f"waits for value at {slot} vs his league")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    p = pd.read_parquet(args.data, columns=["chosen", "year", "db_name", "manager_key",
                                            "cand_nflid", "cand_pos", "seq",
                                            "prior_pos_pctl", "is_rookie", "reach", "age"])
    p = p[(p["chosen"] == 1) & p["cand_pos"].isin(SLOT_POS)].copy()
    p["season"] = p["year"] - 1
    ay = load_airyards(latest_v26_weekly())
    p = p.merge(ay, left_on=["cand_nflid", "season"], right_on=["NFL_player_id", "season"], how="left")
    p = p.sort_values(["db_name", "manager_key", "year", "cand_pos", "seq"])
    p["idx"] = p.groupby(["db_name", "manager_key", "year", "cand_pos"]).cumcount() + 1
    p["slot"] = [slot_label(pos, i) for pos, i in zip(p["cand_pos"], p["idx"])]

    # axis raw values (percentiles within pos/season where continuous)
    p["floor"] = np.where(p["is_rookie"] == 1, 0.0, p["prior_pos_pctl"])
    p["deep"] = p.groupby(["cand_pos", "season"])["air_yards_share"].rank(pct=True)
    p["value"] = p["reach"]
    # 'age' used raw

    names = name_map()
    rows = []
    for axis in ("floor", "deep", "age", "value"):
        d = p[["db_name", "manager_key", "year", "slot", axis]].dropna(subset=[axis]).copy()
        if axis == "deep":  # air-yards only meaningful for pass catchers
            d = d[d["slot"].str.startswith(("WR", "TE", "RB"))]
        # leave-one-out leaguemate mean at (league, season, slot)
        key = ["db_name", "year", "slot"]
        gsum = d.groupby(key)[axis].transform("sum")
        gn = d.groupby(key)[axis].transform("count")
        d = d[gn >= 3].copy()
        loo = (gsum[d.index] - d[axis]) / (gn[d.index] - 1)
        d["dev"] = d[axis] - loo
        scale = float(d["dev"].std()) or 1.0     # per-axis spread -> comparable strength
        # per manager-slot-year mean, then across years
        yr = d.groupby(["db_name", "manager_key", "slot", "year"])["dev"].mean().reset_index()
        agg = yr.groupby(["db_name", "manager_key", "slot"]).agg(
            mean_dev=("dev", "mean"), n_years=("dev", "size"),
            pos_years=("dev", lambda s: int((s > 0).sum())),
        ).reset_index()
        agg["consistency"] = agg.apply(
            lambda r: max(r["pos_years"], r["n_years"] - r["pos_years"]) / r["n_years"], axis=1)
        agg = agg[(agg["n_years"] >= MIN_YEARS) & (agg["consistency"] >= CONSISTENCY)
                  & (agg["mean_dev"].abs() >= THRESH[axis])]
        for _, r in agg.iterrows():
            rows.append({
                "manager": names.get((r["db_name"], r["manager_key"]), r["manager_key"]),
                "league": r["db_name"], "slot": r["slot"], "axis": axis,
                "statement": label(axis, r["slot"], r["mean_dev"]),
                "mean_dev": round(float(r["mean_dev"]), 3), "n_years": int(r["n_years"]),
                "consistency": round(float(r["consistency"]), 2),
                "sd_vs_league": round(float(r["mean_dev"]) / scale, 2),
                "strength": round(abs(float(r["mean_dev"])) / scale * float(np.sqrt(r["n_years"])), 3),
            })

    lb = pd.DataFrame(rows).sort_values("strength", ascending=False).reset_index(drop=True)
    print(f"\n{len(lb):,} consistent (>= {MIN_YEARS} drafts, >= {int(CONSISTENCY*100)}% one-way) "
          f"slot signatures vs leaguemates\n" + "=" * 82)
    for _, r in lb.head(args.top).iterrows():
        print(f"{r['manager'][:20]:20} [{r['league'][:18]:18}] {r['statement']}  "
              f"({r['n_years']}yr, {int(r['consistency']*100)}%, {r['sd_vs_league']:+.1f}sd)")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    lb.to_json(out_dir / "draft_slot_signatures.json", orient="records", indent=2)
    print(f"\n{len(lb):,} signatures -> {out_dir / 'draft_slot_signatures.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
