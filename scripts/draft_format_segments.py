#!/usr/bin/env python3
"""Do the manager type-tells hold across league types / formats — and does
snake differ from auction?

Everything else in this workstream was snake-only and pooled. This pulls ALL
drafts (auction included), tags each pick with its league format, and re-runs
the two cleanest format-agnostic archetype axes — PRODUCTION (proven-season vs
rookie/upside) and AGE (young/prime/vet) — segmented by draft type and format,
using a capital tier that works for both mechanics (snake = pick order, auction
= dollars spent). Reports the early-capital manager lift per segment.

    python scripts/draft_format_segments.py
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_archetype_lift import leave_future_out_lift  # noqa: E402
import draft_choice_baselines  # noqa: E402,F401  (preamble: multi_league path + .env)
from draft_choice_baselines import canon_pos  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def fetch_year(reader: FlyReader, year: int) -> pd.DataFrame:
    sql = f"""
    WITH picks AS (
        SELECT d.db_name, COALESCE(d.franchise_id, d.manager) AS manager_key, d.year,
               d.pick, COALESCE(d.cost,0) AS cost, UPPER(COALESCE(d.position,'')) AS position,
               d.NFL_player_id,
               AVG(CASE WHEN COALESCE(d.cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY d.db_name) AS auction_share
        FROM public.draft d
        WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL
          AND COALESCE(d.is_keeper,0)=0 AND d.pick IS NOT NULL AND d.manager IS NOT NULL
    )
    SELECT p.*, ps.ppg_season_4pt_ppr AS prior_ppg, pb.birth_date
    FROM picks p
    LEFT JOIN ___ops.nfl_historical.player_nfl_season ps
        ON p.NFL_player_id = ps.NFL_player_id AND ps.year = {year - 1}
    LEFT JOIN ___ops.nfl_historical.player_bio pb ON p.NFL_player_id = pb.NFL_player_id
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


def fetch_formats(reader: FlyReader) -> dict:
    sql = """
    SELECT db_name, year, COALESCE(scoring_rec,0) AS rec, COALESCE(num_teams,0) AS nt,
           COALESCE("roster_SUPER_FLEX",0) AS sf, COALESCE(scoring_bonus_rec_te,0) AS tep,
           COALESCE(max_keepers,0) AS keep
    FROM public.league_settings WHERE year IS NOT NULL
    """
    return {(str(r["db_name"]), int(r["year"])): r for r in reader.query(sql, database="___leagues")}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    reader = FlyReader()
    fmt = fetch_formats(reader)
    frames = []
    for y in range(2003, 2027):
        d = fetch_year(reader, y)
        if not d.empty:
            frames.append(d)
        print(f"  {y}: {0 if d.empty else len(d):>6}", flush=True)
    df = pd.concat(frames, ignore_index=True)
    df["cpos"] = df["position"].map(canon_pos)
    df["is_auction"] = df["auction_share"] >= 0.25

    # capital tier: snake = pick asc, auction = cost desc -> manager's Nth most-valuable
    df["cap"] = np.where(df["is_auction"], df["cost"], -df["pick"])
    df = df.sort_values(["db_name", "manager_key", "year", "cap"], ascending=[True, True, True, False])
    df["rank"] = df.groupby(["db_name", "manager_key", "year"]).cumcount() + 1
    df["tier"] = np.where(df["rank"] <= 3, "early", np.where(df["rank"] <= 8, "mid", "late"))

    # production percentile within (position, season); age
    df["ppctl"] = df.groupby(["cpos", "year"])["prior_ppg"].rank(pct=True)
    by = pd.to_datetime(df["birth_date"], errors="coerce").dt.year
    df["age"] = (df["year"] - by).astype(float)

    def prod_cls(r):
        if pd.isna(r["prior_ppg"]):
            return "rookie"
        p = r["ppctl"]
        return "proven" if p >= 0.6 else ("middling" if p >= 0.35 else "upside")

    def age_cls(a):
        return "unknown" if pd.isna(a) else ("young" if a < 24 else ("prime" if a < 28 else "vet"))

    df["prod"] = df.apply(prod_cls, axis=1)
    df["agecls"] = df["age"].map(age_cls)

    # format flags per pick
    for k in ("rec", "nt", "sf", "tep", "keep"):
        df[k] = [None if (v := fmt.get((db, int(y)))) is None else v[k]
                 for db, y in zip(df["db_name"], df["year"])]

    segments = {
        "SNAKE": df["is_auction"] == False,                       # noqa: E712
        "AUCTION": df["is_auction"] == True,                      # noqa: E712
        "  snake PPR(1)": (~df["is_auction"]) & (df["rec"] >= 1),
        "  snake half(.5)": (~df["is_auction"]) & (df["rec"] >= 0.5) & (df["rec"] < 1),
        "  snake standard(0)": (~df["is_auction"]) & (df["rec"] < 0.5),
        "  snake superflex": (~df["is_auction"]) & (df["sf"].fillna(0) > 0),
        "  snake 1QB": (~df["is_auction"]) & (df["sf"].fillna(0) == 0),
        "  snake TEP": (~df["is_auction"]) & (df["tep"].fillna(0) > 0),
        "  snake keeper": (~df["is_auction"]) & (df["keep"].fillna(0) > 0),
        "  snake redraft": (~df["is_auction"]) & (df["keep"].fillna(0) == 0),
        "  snake <=10 tm": (~df["is_auction"]) & (df["nt"].fillna(0).between(1, 10)),
        "  snake 12 tm": (~df["is_auction"]) & (df["nt"] == 12),
        "  snake >=14 tm": (~df["is_auction"]) & (df["nt"] >= 14),
    }

    print("\n" + "=" * 74)
    print("EARLY-CAPITAL manager lift by format (does the type-tell hold?)")
    print("baseline vs manager, leave-future-out; n = early picks w/ mgr history")
    print("=" * 74)
    print(f"{'segment':22} {'n_early':>9} {'PROD base':>9} {'PROD mgr':>9} {'lift':>6}   "
          f"{'AGE base':>8} {'AGE mgr':>8} {'lift':>6}")
    out = {}
    for name, mask in segments.items():
        sub = df[mask]
        if len(sub) < 3000:
            continue
        res = leave_future_out_lift(sub[["year", "manager_key", "tier"]].reset_index(drop=True),
                                    {"prod": sub["prod"].to_numpy(), "age": sub["agecls"].to_numpy()})
        pe = res.get("prod", {}).get("early")
        ae = res.get("age", {}).get("early")
        if not pe or not ae or pe["naive_acc_hist"] is None:
            continue
        out[name.strip()] = {"prod": pe, "age": ae}
        print(f"{name:22} {pe['n_hist']:>9,} {pe['naive_acc_hist']:>9.3f} {pe['mgr_acc_hist']:>9.3f} "
              f"{pe['lift_hist']:>+6.3f}   {ae['naive_acc_hist']:>8.3f} {ae['mgr_acc_hist']:>8.3f} "
              f"{ae['lift_hist']:>+6.3f}")

    out_dir = Path(args.out) if args.out else Path(".")
    (out_dir / "draft_format_segments.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_format_segments.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
