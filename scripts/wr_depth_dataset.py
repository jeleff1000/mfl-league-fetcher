#!/usr/bin/env python3
"""Pull WR + QB season data (2006-2024) from v26 for the depth-vs-fantasy study."""
from __future__ import annotations

import pandas as pd
import wr_depth_common as C
from multi_league.core.readers.fly_reader import FlyReader


def main() -> int:
    r = FlyReader()
    wc = ", ".join(f'"{c}"' for c in C.WR_COLS)
    wr = r.query_df(f"""
        SELECT {wc} FROM nfl_historical.player_nfl_season_all
        WHERE position='WR' AND year BETWEEN 2006 AND 2025
    """, "___ops")
    for c in C.WR_COLS[4:]:
        wr[c] = pd.to_numeric(wr[c], errors="coerce")
    wr.to_parquet(C.WR, index=False)
    print(f"WR: {len(wr):,} rows -> {C.WR}")

    qc = ", ".join(f'"{c}"' for c in C.QB_COLS)
    qb = r.query_df(f"""
        SELECT {qc} FROM nfl_historical.player_nfl_season_all
        WHERE position='QB' AND year BETWEEN 2006 AND 2025
    """, "___ops")
    for c in C.QB_COLS[4:]:
        qb[c] = pd.to_numeric(qb[c], errors="coerce")
    qb.to_parquet(C.QB, index=False)
    print(f"QB: {len(qb):,} rows -> {C.QB}")

    # coverage sanity: WR ADOT (computed) by year, targets>=50
    d = C.add_depth_metrics(wr)
    d = d[(d["targets"] >= 50) & (d["year"] <= 2024)]
    yr = d.groupby("year").agg(
        n=("player", "size"),
        adot=("adot", "mean"),
        ay_share=("air_yards_share", "mean"),
        yac_share=("yac_share", "mean"),
        ppg_half=("ppg_season_4pt_half", "mean"),
    ).round(2)
    print("\nWR (targets>=50) by year — computed ADOT should be ~11-13 & stable:")
    print(yr.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
