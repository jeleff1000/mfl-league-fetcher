#!/usr/bin/env python3
"""Build the RB next-year-PPG modeling frame -> local parquet.

Pulls RB seasons from Fly `player_nfl_season_all` (features + realized Y+1 label)
and joins time-invariant `player_bio` fields. Derives experience and BMI.
Writes rb_seasons.parquet to the scratchpad for fast local iteration.

    python scripts/rb_ppg_dataset.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import rb_ppg_common as C
from multi_league.core.readers.fly_reader import FlyReader


def main() -> int:
    r = FlyReader()

    meta = ["NFL_player_id", "year", "player", "position", "nfl_position"]
    pull = meta + [C.LABEL, C.PERSIST] + C.SEASON_FEATURES
    pull = list(dict.fromkeys(pull))  # dedup, keep order
    cols = ", ".join(f'"{c}"' for c in pull)
    sql = f"""
        SELECT {cols}
        FROM nfl_historical.player_nfl_season_all
        WHERE (position = 'RB' OR nfl_position = 'RB')
          AND year >= 1970
    """
    print("pulling RB seasons from player_nfl_season_all ...", flush=True)
    df = r.query_df(sql, "___ops")
    print(f"  {len(df):,} RB-season rows", flush=True)

    bio_cols = ", ".join(f'"{c}"' for c in ["NFL_player_id"] + C.BIO_FIELDS)
    bio = r.query_df(
        f"SELECT {bio_cols} FROM nfl_historical.player_bio", "___ops"
    )
    print(f"  {len(bio):,} player_bio rows", flush=True)
    df = df.merge(bio, on="NFL_player_id", how="left")

    # numeric coercion
    for c in pull[5:] + C.BIO_FIELDS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # derived, non-leaky bio features
    df["experience"] = df["year"] - df["rookie_year"]          # seasons of NFL exp
    df.loc[df["experience"] < 0, "experience"] = np.nan
    df["bmi"] = np.where(
        (df["height"] > 0) & (df["weight"] > 0),
        df["weight"] / (df["height"] ** 2) * 703.0, np.nan,
    )

    # label availability = has a Y+1 season (attrition target is its inverse)
    df["has_next"] = df[C.LABEL].notna().astype(int)

    out = C.DATASET
    df.to_parquet(out, index=False)
    print(f"\nwrote {out}  ({len(df):,} rows, {df.shape[1]} cols)")

    # quick coverage report
    yr = df.groupby("year").agg(
        n=("NFL_player_id", "size"),
        n_ge6=("games_played", lambda s: int((s >= 6).sum())),
        has_label=(C.LABEL, lambda s: int(s.notna().sum())),
        ppg=(C.PERSIST, "mean"),
    )
    print("\nby year (tail):")
    print(yr.tail(12).to_string())
    print(f"\ntotal with games>=6: {(df['games_played'] >= 6).sum():,}")
    print(f"total with Y+1 label: {df['has_next'].sum():,}")
    print(f"attrition rate (games>=6, no Y+1): "
          f"{1 - df.loc[df['games_played'] >= 6, 'has_next'].mean():.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
