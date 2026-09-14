#!/usr/bin/env python3
"""Broaden the archetype vocabulary beyond age/production/value.

Pulls the actual (chosen) snake picks joined to PRE-DRAFT-SAFE signals only —
combine/athleticism, size, college pedigree, NFL draft capital, and PRIOR-season
workload — and runs each candidate archetype axis through the same leave-future-
out lift harness as draft_archetype_lift.py (manager-modal vs league-modal class
accuracy per capital tier). The point: which player TYPES does a manager reveal
a persistent preference for, at a given draft capital?

Leakage guard: bio career totals (seasons_started, years_active, probowls,
allpro, career_games) count the player's WHOLE career through today and would
leak the future for a historical pick — they are deliberately excluded. Only
fixed pre-draft facts (RAS, forty, size, college, NFL draft round) and the
season BEFORE the draft are used.

    python scripts/draft_archetype_explore.py
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from draft_archetype_lift import _report, leave_future_out_lift  # noqa: E402
# draft_choice_baselines preamble sets multi_league path + loads .env
import draft_choice_baselines  # noqa: E402,F401
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402

BLUE_CHIP = {
    "Alabama", "Ohio State", "Georgia", "LSU", "Clemson", "Oklahoma",
    "Notre Dame", "Michigan", "USC", "Florida", "Texas", "Penn State",
    "Oregon", "Auburn", "Florida State", "Miami (FL)", "Miami", "Wisconsin",
    "Tennessee", "Texas A&M", "Nebraska",
}


def fetch_year(reader: FlyReader, year: int) -> pd.DataFrame:
    sql = f"""
    WITH picks AS (
        SELECT d.db_name, COALESCE(d.franchise_id, d.manager) AS manager_key,
               d.year, d.round, UPPER(COALESCE(d.position,'')) AS position,
               d.NFL_player_id,
               AVG(CASE WHEN COALESCE(d.cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY d.db_name) AS auction_share
        FROM public.draft d
        WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL
          AND COALESCE(d.is_keeper,0)=0 AND d.pick IS NOT NULL AND d.manager IS NOT NULL
    )
    SELECT p.db_name, p.manager_key, p.year, p.round, p.position,
           ps.games_played AS prior_games, ps.carries, ps.receptions, ps.targets,
           pb.birth_date, pb.rookie_year, pb.ras_score, pb.forty,
           pb.height, pb.weight, pb.college, pb.draft_round, pb.is_undrafted
    FROM picks p
    LEFT JOIN ___ops.nfl_historical.player_nfl_season ps
        ON p.NFL_player_id = ps.NFL_player_id AND ps.year = {year - 1}
    LEFT JOIN ___ops.nfl_historical.player_bio pb ON p.NFL_player_id = pb.NFL_player_id
    WHERE p.auction_share < 0.25
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


def build_axes(df: pd.DataFrame) -> dict[str, np.ndarray]:
    pos = df["position"].str.split(",").str[0].fillna("").to_numpy()
    ras = pd.to_numeric(df["ras_score"], errors="coerce").to_numpy()
    wt = pd.to_numeric(df["weight"], errors="coerce")
    forty = pd.to_numeric(df["forty"], errors="coerce")
    dr = pd.to_numeric(df["draft_round"], errors="coerce").to_numpy()
    udf = pd.to_numeric(df["is_undrafted"], errors="coerce").to_numpy()
    pg = pd.to_numeric(df["prior_games"], errors="coerce").to_numpy()
    exp = (pd.to_numeric(df["year"], errors="coerce")
           - pd.to_numeric(df["rookie_year"], errors="coerce")).to_numpy()
    college = df["college"].fillna("").to_numpy()
    # position-relative percentiles (size/speed mean different things by position)
    wt_pct = wt.groupby(df["position"]).rank(pct=True).to_numpy()
    forty_pct = forty.groupby(df["position"]).rank(pct=True).to_numpy()  # lower forty = faster
    n = len(df)

    def athlete(i):
        r = ras[i]
        return "unknown" if np.isnan(r) else ("elite" if r >= 8 else ("solid" if r >= 5 else "limited"))

    def size(i):
        p = wt_pct[i]
        return "unknown" if np.isnan(p) else ("big" if p >= 0.66 else ("small" if p <= 0.33 else "medium"))

    def speed(i):
        p = forty_pct[i]
        return "unknown" if np.isnan(p) else ("burner" if p <= 0.33 else ("plodder" if p >= 0.66 else "average"))

    def pedigree(i):
        d = dr[i]
        if not np.isnan(d):
            return "r1" if d == 1 else ("day2" if d <= 3 else "day3")
        return "udfa" if udf[i] == 1 else "unknown"

    def college_tier(i):
        c = college[i]
        return "bluechip" if c in BLUE_CHIP else ("other" if c else "unknown")

    def starter(i):
        g = pg[i]
        if np.isnan(g):
            return "rookie_or_new"
        return "featured" if g >= 14 else ("partial" if g >= 6 else "thin")

    def career(i):
        e = exp[i]
        if np.isnan(e):
            return "unknown"
        return "rookie" if e <= 0 else ("ascending" if e <= 3 else ("prime" if e <= 8 else "aging"))

    return {
        "position": pos,
        "athleticism": np.array([athlete(i) for i in range(n)]),
        "size": np.array([size(i) for i in range(n)]),
        "speed": np.array([speed(i) for i in range(n)]),
        "nfl_pedigree": np.array([pedigree(i) for i in range(n)]),
        "college_tier": np.array([college_tier(i) for i in range(n)]),
        "prior_workload": np.array([starter(i) for i in range(n)]),
        "career_stage": np.array([career(i) for i in range(n)]),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    reader = FlyReader()
    frames = []
    for year in range(2003, 2027):
        d = fetch_year(reader, year)
        if not d.empty:
            frames.append(d)
        print(f"  fetched {year}: {0 if d.empty else len(d):>6} snake picks", flush=True)
    df = pd.concat(frames, ignore_index=True)
    df["tier"] = np.where(df["round"] <= 3, "early",
                          np.where(df["round"] <= 8, "mid", "late"))
    df = df[df["tier"].notna()].reset_index(drop=True)
    print(f"\ntotal {len(df):,} snake picks; running lift on {len(build_axes(df.head(1)))} axes...")
    out = leave_future_out_lift(df, build_axes(df))
    _report(out)
    out_dir = Path(args.out) if args.out else Path(
        os.environ.get("TMP", ".")) / "dcb_dataset"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "draft_archetype_explore.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_archetype_explore.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
