"""
sota_recon/build_scoring_summary.py

Builds an AUTHORITATIVE per-team-game scoring summary from the PFR scoring tables
(raw/pfr/boxscores/tables/scoring, 1957-2026). Each scoring play's points come from
the running-score delta, so the summary reconciles to the final score BY CONSTRUCTION.

Implemented in pandas/pyarrow (DuckDB crashes on the 11k-file read + window ops on this
box). Output: derived/scoring_summary/scoring_summary.parquet + reconciliation report.
"""

from __future__ import annotations

import glob
import os
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .sources import DATA_LAKE

SCORING_DIR = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables", "scoring")
TEAM_GAMES = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "nfl_team_games_all.parquet")
OUT_DIR = os.path.join(DATA_LAKE, "derived", "scoring_summary")


_COLS = ["boxscore_id", "season", "row_index_in_table", "description",
         "vis_team_score", "home_team_score"]


def _summarize_batch(df: pd.DataFrame) -> pd.DataFrame:
    """play rows for a set of whole games -> per (boxscore_id, season, side) scoring summary."""
    df = df[df.description.notna() & df.vis_team_score.notna()].copy()
    if df.empty:
        return df
    df["vs"] = pd.to_numeric(df.vis_team_score, errors="coerce").fillna(0).astype(int)
    df["hs"] = pd.to_numeric(df.home_team_score, errors="coerce").fillna(0).astype(int)
    df["ri"] = pd.to_numeric(df.row_index_in_table, errors="coerce").fillna(0).astype(int)
    df["tot"] = df.vs + df.hs
    df = df.sort_values(["boxscore_id", "ri"])
    g = df.groupby("boxscore_id", sort=False)
    df["pts"] = (df.tot - g.tot.shift(1).fillna(0)).astype(int)
    df["hd"] = (df.hs - g.hs.shift(1).fillna(0)).astype(int)
    df = df[df.pts > 0].copy()
    # Classify by the POINT DELTA (exact by construction), not description text:
    # 3=FG, 2=safety, 6=TD, 7=TD+PAT, 8=TD+2pt. Text is only needed later for player
    # attribution / TD-type, not for the points breakdown.
    df["side"] = np.where(df.hd > 0, "home", "vis")
    df["is_fg"] = df.pts == 3
    df["is_safety"] = df.pts == 2
    df["is_td"] = df.pts.isin([6, 7, 8])
    df["pat_made"] = df.pts == 7
    df["two_pt"] = df.pts == 8
    return df.groupby(["boxscore_id", "season", "side"], sort=False).agg(
        td=("is_td", "sum"), fg=("is_fg", "sum"), pat_made=("pat_made", "sum"),
        two_pt=("two_pt", "sum"), safeties=("is_safety", "sum"), points=("pts", "sum"),
    ).reset_index()


def build_shard(start: int, count: int, out_path: str) -> int:
    """Summarize files[start:start+count] and write the partial summary. Run in a FRESH
    process per shard: repeated pyarrow dataset reads leak file handles on Windows and
    crash the process ~5-6k files in, so each shard stays well under that."""
    files = sorted(glob.glob(os.path.join(SCORING_DIR, "*.parquet")))[start:start+count]
    # minimize ds.dataset() calls (each leaks a handle on Windows; ~20 calls crashes the
    # process). Batch-read 200 files/call -> 10 calls for a 2000-file shard.
    summaries, skipped, batch = [], 0, 200
    for i in range(0, len(files), batch):
        chunk = files[i:i+batch]
        try:
            raw = ds.dataset(chunk, format="parquet").to_table(columns=_COLS).to_pandas()
            summaries.append(_summarize_batch(raw))
        except Exception:
            skipped += len(chunk)
    summ = pd.concat([s for s in summaries if not s.empty], ignore_index=True)
    summ.to_parquet(out_path, index=False)
    return len(summ)


SCORING_COMBINED = os.path.join(SCORING_DIR, "_combined.parquet")


def build() -> dict:
    """Read the pre-consolidated _combined.parquet (all 18,154 games 1920-2025, one file —
    no multi-file pyarrow handle leak), summarize, map to franchise, reconcile."""
    os.makedirs(OUT_DIR, exist_ok=True)
    raw = pq.read_table(SCORING_COMBINED, columns=_COLS).to_pandas()
    summ = _summarize_batch(raw)
    return _finish(summ)


def combine_and_map() -> dict:
    """Combine shard summaries + map side->franchise via team_games -> final scoring_summary."""
    shards = sorted(glob.glob(os.path.join(OUT_DIR, "shard_*.parquet")))
    summ = pd.concat([pd.read_parquet(s) for s in shards], ignore_index=True)
    return _finish(summ)


def _finish(summ: pd.DataFrame) -> dict:

    # reconcile (by construction): 6*td+3*fg+pat+2*2pt+2*saf == points
    summ["computed"] = 6*summ.td + 3*summ.fg + summ.pat_made + 2*summ.two_pt + 2*summ.safeties
    exact = int((summ.computed == summ.points).sum())

    # map side -> franchise via team_games
    tg = pq.read_table(TEAM_GAMES, columns=[
        "boxscore_id", "year", "week", "season_type", "team_fid", "team_code", "is_home"]).to_pandas()
    tg["side"] = np.where(tg.is_home, "home", "vis")
    out = summ.merge(tg[["boxscore_id", "side", "year", "week", "season_type", "team_fid", "team_code"]],
                     on=["boxscore_id", "side"], how="left")
    mapped = int(out.team_fid.notna().sum())

    out_path = os.path.join(OUT_DIR, "scoring_summary.parquet")
    out.to_parquet(out_path, index=False)

    return {
        "team_game_sides": int(len(summ)),
        "reconcile_to_points_exact": exact,
        "reconcile_pct": round(100.0*exact/len(summ), 2) if len(summ) else None,
        "mapped_to_franchise": mapped,
        "mapped_pct": round(100.0*mapped/len(out), 2) if len(out) else None,
        "years": f"{int(summ.season.min())}-{int(summ.season.max())}",
        "output": out_path,
    }


if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", nargs=2, type=int, metavar=("START", "COUNT"))
    ap.add_argument("--combine", action="store_true")
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    if args.shard:
        start, count = args.shard
        out = os.path.join(OUT_DIR, f"shard_{start:06d}.parquet")
        n = build_shard(start, count, out)
        print(f"shard {start}+{count}: {n} summary rows -> {out}", flush=True)
    elif args.combine:
        print(json.dumps(combine_and_map(), indent=2))
    else:
        print(json.dumps(build(), indent=2))  # default: read _combined.parquet (one file)
