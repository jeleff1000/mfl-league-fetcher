#!/usr/bin/env python3
"""Build fleet fantasy ADP per (player, draft-year) from ___leagues.draft.

Fleet ADP = mean overall pick across our leagues' SNAKE drafts (cost=0). It is a
PRESEASON market signal: the ADP from the year-Y draft is set before year Y is
played, so it is the market's projection of year Y.  For the next-year framework
a season-Y row's ADP feature is the (Y+1)-draft ADP; for the same-year analysis
(Joe's ask) it is the year-Y draft ADP vs year-Y PPG.

Writes rb_adp.parquet: NFL_player_id, draft_year, adp_overall, adp_n.

    python scripts/rb_ppg_adp.py
"""
from __future__ import annotations

import rb_ppg_common as C
from multi_league.core.readers.fly_reader import FlyReader


def main() -> int:
    r = FlyReader()
    sql = """
        SELECT NFL_player_id,
               year               AS draft_year,
               AVG(CAST(pick AS DOUBLE)) AS adp_overall,
               MEDIAN(CAST(pick AS DOUBLE)) AS adp_median,
               MIN(pick)          AS adp_min,
               COUNT(*)           AS adp_n
        FROM public.draft
        WHERE NFL_player_id IS NOT NULL
          AND COALESCE(cost, 0) = 0          -- snake picks only
          AND pick IS NOT NULL
          AND year BETWEEN 2008 AND 2025
        GROUP BY NFL_player_id, year
        HAVING COUNT(*) >= 3                  -- drafted in >=3 leagues
    """
    df = r.query_df(sql, "___leagues")
    print(f"fleet ADP rows: {len(df):,}  years {df.draft_year.min()}-{df.draft_year.max()}")
    out = C.SCRATCH / "rb_adp.parquet"
    df.to_parquet(out, index=False)
    print(f"wrote {out}")
    # coverage sanity: RB ADP counts per recent year
    print(df.groupby("draft_year")["NFL_player_id"].size().tail(10).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
