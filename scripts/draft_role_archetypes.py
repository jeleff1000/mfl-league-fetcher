#!/usr/bin/env python3
"""Role / usage archetypes: does a manager reveal a persistent taste for HOW a
player is used — mobile QB, pass-catching back, alpha receiver — beyond the
proven-vs-upside style axis?

These are measured WITHIN position (the honest framing: "when this manager takes
a RB, do they take a receiving back?"), from the player's PRIOR-season usage
(leak guard), aggregated from the weekly super table. Same leave-future-out lift
harness as the other archetype scripts.

Coverage: snap%/target_share are ~2012+; usage archetypes only apply to
NON-ROOKIE picks (rookies have no prior NFL usage). Slot-vs-wide needs PFF/NGS
we don't have — not attempted here.

    python scripts/draft_role_archetypes.py
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
import draft_choice_baselines  # noqa: E402,F401  (preamble: multi_league path + .env)
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


def fetch_year(reader: FlyReader, year: int) -> pd.DataFrame:
    """Snake picks in `year` joined to the player's PRIOR-season (Y-1) usage,
    aggregated from the weekly super table."""
    sql = f"""
    WITH usage AS (
        SELECT NFL_player_id,
               SUM(carries) AS car, SUM(receptions) AS rec, SUM(targets) AS tgt,
               SUM(CASE WHEN offense_snaps > 0 THEN 1 ELSE 0 END) AS games,
               AVG(offense_snap_pct) AS snap_pct, AVG(target_share) AS tgt_share
        FROM ___ops.nfl_historical.nfl_player_stats_all
        WHERE year = {year - 1}
        GROUP BY NFL_player_id
    ),
    picks AS (
        SELECT d.db_name, COALESCE(d.franchise_id, d.manager) AS manager_key,
               d.year, d.round, UPPER(COALESCE(d.position,'')) AS position, d.NFL_player_id,
               AVG(CASE WHEN COALESCE(d.cost,0)>0 THEN 1.0 ELSE 0.0 END)
                   OVER (PARTITION BY d.db_name) AS auction_share
        FROM public.draft d
        WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL
          AND COALESCE(d.is_keeper,0)=0 AND d.pick IS NOT NULL AND d.manager IS NOT NULL
    )
    SELECT p.db_name, p.manager_key, p.year, p.round, p.position,
           u.car, u.rec, u.tgt, u.games, u.snap_pct, u.tgt_share
    FROM picks p LEFT JOIN usage u ON p.NFL_player_id = u.NFL_player_id
    WHERE p.auction_share < 0.25
    """
    return pd.DataFrame(reader.query(sql, database="___leagues"))


def qb_mobility(df: pd.DataFrame) -> np.ndarray:
    car = pd.to_numeric(df["car"], errors="coerce").to_numpy()
    games = pd.to_numeric(df["games"], errors="coerce").to_numpy()
    out = []
    for i in range(len(df)):
        g = games[i]
        if np.isnan(g) or g < 1:
            out.append("rookie")
            continue
        rpg = car[i] / g if not np.isnan(car[i]) else 0.0
        out.append("mobile" if rpg >= 5 else ("dual" if rpg >= 2.5 else "pocket"))
    return np.array(out)


def rb_role(df: pd.DataFrame) -> np.ndarray:
    """Backfield ROLE keyed on receiving share of touches — orthogonal to how
    proven the back is (a receiving back can be a star or a rotational third-
    down specialist). Scale-free, so unaffected by snap_pct being 0-100."""
    car = pd.to_numeric(df["car"], errors="coerce").to_numpy()
    rec = pd.to_numeric(df["rec"], errors="coerce").to_numpy()
    games = pd.to_numeric(df["games"], errors="coerce").to_numpy()
    out = []
    for i in range(len(df)):
        g = games[i]
        if np.isnan(g) or g < 1:
            out.append("rookie")
            continue
        c = 0.0 if np.isnan(car[i]) else car[i]
        r = 0.0 if np.isnan(rec[i]) else rec[i]
        touches = c + r
        if touches < 20:                       # barely used last year
            out.append("fringe")
        elif r / touches >= 0.28:
            out.append("receiving")
        elif r / touches <= 0.12:
            out.append("early_down")
        else:
            out.append("balanced")
    return np.array(out)


def wr_role(df: pd.DataFrame) -> np.ndarray:
    ts = pd.to_numeric(df["tgt_share"], errors="coerce").to_numpy()
    games = pd.to_numeric(df["games"], errors="coerce").to_numpy()
    out = []
    for i in range(len(df)):
        g = games[i]
        if np.isnan(g) or g < 1:
            out.append("rookie")
        elif np.isnan(ts[i]):
            out.append("depth")
        else:
            out.append("alpha" if ts[i] >= 0.22 else ("secondary" if ts[i] >= 0.12 else "depth"))
    return np.array(out)


AXES = {  # axis name -> (position filter, class fn)
    "qb_mobility": ("QB", qb_mobility),
    "rb_backfield_role": ("RB", rb_role),
    "wr_target_role": ("WR", wr_role),
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-year", type=int, default=2013, help="snap%/tgt_share era")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    reader = FlyReader()
    frames = []
    for year in range(args.from_year, 2027):
        d = fetch_year(reader, year)
        if not d.empty:
            frames.append(d)
        print(f"  fetched {year}: {0 if d.empty else len(d):>6} snake picks", flush=True)
    df = pd.concat(frames, ignore_index=True)
    df["pos1"] = df["position"].str.split(",").str[0]
    df["tier"] = np.where(df["round"] <= 3, "early", np.where(df["round"] <= 8, "mid", "late"))

    out: dict = {}
    for axis, (pos, fn) in AXES.items():
        sub = df[df["pos1"] == pos].reset_index(drop=True)
        if sub.empty:
            continue
        res = leave_future_out_lift(sub, {axis: fn(sub)})
        out.update(res)
        mix = pd.Series(fn(sub)).value_counts(normalize=True).round(3).to_dict()
        print(f"\n[{axis}] {len(sub):,} {pos} picks — class mix: {mix}")
    _report(out)
    out_dir = Path(args.out) if args.out else Path(os.environ.get("TMP", ".")) / "dcb_dataset"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "draft_role_archetypes.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nwrote {out_dir / 'draft_role_archetypes.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
