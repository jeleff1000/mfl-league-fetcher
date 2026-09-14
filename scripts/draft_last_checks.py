#!/usr/bin/env python3
"""Two last type checks through the reliability lens:
A. DEEPER AUCTION axes (positional budget allocation, concentration, spend
   timing, roster size) — auction is the reliable substrate (no slot noise).
B. PLAYER LOYALTY (fraction of picks that re-draft a player the manager drafted
   in a PRIOR year) — slot-independent, so it may clear the bar snake
   construction couldn't. Computed for snake AND auction.

    python scripts/draft_last_checks.py
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
from draft_auction_signatures import fetch_auction  # noqa: E402
from draft_choice_baselines import canon_pos  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def auction_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["cost"] > 0].copy()
    df["cpos"] = df["position"].map(canon_pos)
    rows = []
    for (db, mk, yr), g in df.groupby(["db_name", "manager_key", "year"], sort=False):
        total = g["cost"].sum()
        if total <= 0 or len(g) < 4:
            continue
        c = np.sort(g["cost"].to_numpy())[::-1]
        share = c / total
        pos_share = (g.groupby("cpos")["cost"].sum() / total)
        mx = g["pick"].max() or 1
        stage = g["pick"] / mx
        rows.append({
            "db_name": db, "manager_key": mk, "year": yr,
            "top3_share": share[:3].sum(), "hhi": float((share ** 2).sum()),
            "n_big": int((share >= 0.15).sum()), "n_players": len(g),
            "rb_share": pos_share.get("RB", 0.0), "wr_share": pos_share.get("WR", 0.0),
            "qb_share": pos_share.get("QB", 0.0), "te_share": pos_share.get("TE", 0.0),
            "max_pos_share": float(pos_share.max()),
            "spend_stage": float((g["cost"] * stage).sum() / total),  # low = spends early
        })
    return pd.DataFrame(rows)


def reliability_table(feat: pd.DataFrame, cols: list[str], label: str) -> list:
    print(f"\n{label}")
    print(f"{'axis':16} {'#mgrs':>6} {'1-draft rel':>12} {'N@0.7':>6} {'N@0.8':>6}")
    out = []
    for ax in cols:
        s = feat[["db_name", "manager_key", "year", ax]].rename(columns={ax: "v"}).copy()
        s["dev"] = leaguemate_center(s, "v")
        md = s.dropna(subset=["dev"]).groupby(["db_name", "manager_key", "year"])["dev"].mean().reset_index()
        md["mgr"] = md["db_name"] + "|" + md["manager_key"]
        icc, m, n = icc1(md, "mgr", "dev")
        n7, n8 = n_for(icc, 0.7), n_for(icc, 0.8)
        f = lambda x: "inf" if not np.isfinite(x) else str(int(np.ceil(x)))
        print(f"{ax:16} {m:>6,} {icc:>12.3f} {f(n7):>6} {f(n8):>6}")
        out.append({"axis": ax, "n_mgrs": m, "icc": round(icc, 4),
                    "n07": None if not np.isfinite(n7) else int(np.ceil(n7))})
    return out


def loyalty_frame(reader: FlyReader) -> pd.DataFrame:
    sql = """
    SELECT db_name, COALESCE(franchise_id, manager) AS manager_key, year, NFL_player_id,
           AVG(CASE WHEN COALESCE(cost,0)>0 THEN 1.0 ELSE 0.0 END)
               OVER (PARTITION BY db_name, year) AS auc
    FROM public.draft
    WHERE COALESCE(is_keeper,0)=0 AND NFL_player_id IS NOT NULL AND manager IS NOT NULL
      AND year IS NOT NULL AND pick IS NOT NULL
    """
    df = pd.DataFrame(reader.query(sql, database="___leagues"))
    df["year"] = df["year"].astype(int)
    rows = []
    for (db, mk), g in df.groupby(["db_name", "manager_key"], sort=False):
        seen: set = set()
        for yr in sorted(g["year"].unique()):
            gy = g[g["year"] == yr]
            if seen:  # need a prior year to have a loyalty rate
                loyalty = gy["NFL_player_id"].isin(seen).mean()
                rows.append({"db_name": db, "manager_key": mk, "year": yr,
                             "loyalty": float(loyalty),
                             "is_auction": float(gy["auc"].iloc[0]) >= 0.25})
            seen |= set(gy["NFL_player_id"])
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    reader = FlyReader()

    print("A. DEEPER AUCTION AXES", flush=True)
    af = auction_features(fetch_auction(reader))
    print(f"   {len(af):,} auction manager-drafts")
    a_out = reliability_table(af, ["top3_share", "hhi", "n_big", "max_pos_share",
                                   "rb_share", "wr_share", "qb_share", "te_share",
                                   "spend_stage", "n_players"],
                              "DEEPER AUCTION reliability (leaguemate-relative):")

    print("\nB. PLAYER LOYALTY (re-drafting own prior-year players)", flush=True)
    lf = loyalty_frame(reader)
    print(f"   {len(lf):,} manager-years with a prior pool; "
          f"mean loyalty snake {lf[~lf.is_auction].loyalty.mean():.3f} "
          f"auction {lf[lf.is_auction].loyalty.mean():.3f}")
    l_out = {}
    for typ, mask in (("snake", ~lf["is_auction"]), ("auction", lf["is_auction"])):
        sub = lf[mask]
        l_out[typ] = reliability_table(sub, ["loyalty"], f"LOYALTY reliability ({typ}):")

    print("\ncompare: snake age 0.261(7) | auction stars-scrubs 0.168(12) | "
          "snake construction ~0.06(35) | snake floor 0.032(70)")
    out_dir = Path(args.out) if args.out else Path(".")
    (out_dir / "draft_last_checks.json").write_text(
        json.dumps({"auction_axes": a_out, "loyalty": l_out}, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_last_checks.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
