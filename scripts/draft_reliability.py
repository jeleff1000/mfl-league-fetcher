#!/usr/bin/env python3
"""How many drafts until a manager's archetype signature is statistically
reliable? Answer per axis AND per draft type.

Each of a manager's drafts gives one leaguemate-relative estimate of an axis.
Decompose the variance of those estimates into BETWEEN-manager (real, stable
tendency) vs WITHIN-manager (draft-to-draft noise). The single-draft
reliability is the one-way ICC = var_between / (var_between + var_within).
Spearman-Brown then gives the reliability of an N-draft average:
    rel(N) = N*ICC / (1 + (N-1)*ICC)
and the drafts needed to clear a target T:
    N = T*(1-ICC) / (ICC*(1-T))

    python scripts/draft_reliability.py --data <dir>/draft_candidates.parquet
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_slot_archetypes import load_airyards  # noqa: E402
from draft_attribute_sweep import latest_v26_weekly  # noqa: E402
from draft_auction_signatures import fetch_auction, spend_features  # noqa: E402
import draft_choice_baselines  # noqa: E402,F401  (preamble: multi_league path + .env)

EARLY = {"r1", "r2", "r3", "r4_5"}


def icc1(df: pd.DataFrame, group: str, val: str) -> tuple[float, int, int]:
    """One-way ICC(1) via ANOVA on `group`; groups need >=2 rows."""
    d = df[[group, val]].dropna()
    sizes = d.groupby(group)[val].size()
    keep = sizes[sizes >= 2].index
    d = d[d[group].isin(keep)]
    if d[group].nunique() < 30:
        return float("nan"), 0, 0
    m = d[group].nunique()
    N = len(d)
    grand = d[val].mean()
    gm = d.groupby(group)[val]
    ssb = (gm.mean().sub(grand).pow(2) * gm.size()).sum()
    ssw = gm.apply(lambda s: ((s - s.mean()) ** 2).sum()).sum()
    msb, msw = ssb / (m - 1), ssw / (N - m)
    n_i = d.groupby(group)[val].size().to_numpy()
    k0 = (N - (n_i ** 2).sum() / N) / (m - 1)
    icc = (msb - msw) / (msb + (k0 - 1) * msw) if (msb + (k0 - 1) * msw) > 0 else 0.0
    return float(max(icc, 0.0)), m, N


def n_for(icc: float, target: float) -> float:
    if icc <= 0 or icc >= 1:
        return float("inf")
    return target * (1 - icc) / (icc * (1 - target))


def leaguemate_center(df: pd.DataFrame, val: str, keys=("db_name", "year")) -> pd.Series:
    gsum = df.groupby(list(keys))[val].transform("sum")
    gn = df.groupby(list(keys))[val].transform("count")
    return np.where(gn >= 3, df[val] - (gsum - df[val]) / (gn - 1), np.nan)


def snake_axes(data: str) -> dict[str, pd.DataFrame]:
    p = pd.read_parquet(data, columns=["chosen", "year", "db_name", "manager_key",
                                        "cand_nflid", "cand_pos", "round_bucket",
                                        "prior_pos_pctl", "is_rookie", "reach", "age"])
    p = p[(p["chosen"] == 1) & p["round_bucket"].isin(EARLY)].copy()
    p["season"] = p["year"] - 1
    ay = load_airyards(latest_v26_weekly())
    p = p.merge(ay, left_on=["cand_nflid", "season"], right_on=["NFL_player_id", "season"], how="left")
    p["floor"] = np.where(p["is_rookie"] == 1, 0.0, p["prior_pos_pctl"])
    p["deep"] = p.groupby(["cand_pos", "season"])["air_yards_share"].rank(pct=True)
    p["value"] = p["reach"]
    out = {}
    for axis, sub in (("floor", p), ("age", p), ("value", p),
                      ("deep", p[p["cand_pos"].isin(["WR", "TE", "RB"])])):
        s = sub.copy()
        s["dev"] = leaguemate_center(s, axis)
        md = s.dropna(subset=["dev"]).groupby(["db_name", "manager_key", "year"])["dev"].mean().reset_index()
        md["mgr"] = md["db_name"] + "|" + md["manager_key"]
        out[axis] = md
    return out


def auction_axes(reader) -> dict[str, pd.DataFrame]:
    feat = spend_features(fetch_auction(reader))
    out = {}
    for axis in ("top3_share", "hhi", "rb_share", "n_big"):
        s = feat.copy()
        s["dev"] = leaguemate_center(s, axis)
        md = s.dropna(subset=["dev"])[["db_name", "manager_key", "year", "dev"]].copy()
        md["mgr"] = md["db_name"] + "|" + md["manager_key"]
        out[axis] = md
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    from multi_league.core.readers.fly_reader import FlyReader

    rows = []
    print("computing snake axes...", flush=True)
    for axis, md in snake_axes(args.data).items():
        icc, mgr, n = icc1(md, "mgr", "dev")
        rows.append(("snake", axis, mgr, n, icc, n_for(icc, 0.7), n_for(icc, 0.8)))
    print("computing auction axes...", flush=True)
    for axis, md in auction_axes(FlyReader()).items():
        icc, mgr, n = icc1(md, "mgr", "dev")
        rows.append(("auction", axis, mgr, n, icc, n_for(icc, 0.7), n_for(icc, 0.8)))

    print("\n" + "=" * 74)
    print("DRAFTS NEEDED FOR A RELIABLE SIGNATURE (one-way ICC + Spearman-Brown)")
    print("=" * 74)
    print(f"{'type':8} {'axis':12} {'#mgrs':>6} {'1-draft rel':>12} {'N@0.7':>7} {'N@0.8':>7}")
    for t, axis, mgr, n, icc, n7, n8 in rows:
        f7 = "  inf" if not np.isfinite(n7) else f"{int(np.ceil(n7)):>5}"
        f8 = "  inf" if not np.isfinite(n8) else f"{int(np.ceil(n8)):>5}"
        print(f"{t:8} {axis:12} {mgr:>6,} {icc:>12.3f} {f7:>7} {f8:>7}")

    out_dir = Path(args.out) if args.out else Path(args.data).parent
    (out_dir / "draft_reliability.json").write_text(json.dumps(
        [{"type": t, "axis": a, "n_mgrs": m, "icc_1draft": round(i, 4),
          "n_for_0.7": None if not np.isfinite(n7) else int(np.ceil(n7)),
          "n_for_0.8": None if not np.isfinite(n8) else int(np.ceil(n8))}
         for t, a, m, _, i, n7, n8 in rows], indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_reliability.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
