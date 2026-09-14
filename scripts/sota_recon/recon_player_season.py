"""
sota_recon/recon_player_season.py  --  EVERY player-season vs the record book

season_authority checks the LEAGUE-YEAR aggregate against the PFR player-page season
tables; that can pass while individual players are wrong and offset each other. This lane
closes that hole: for EVERY (player, season) it compares v26's REG totals to the published
player-page season totals and logs every player-season that disagrees.

Join: v26.NFL_player_id -> pfr_id (via player_bio, or the id itself for old players) ==
authority pfr_id + year. Authority rush/rec is split across two player-page tables (union);
multi-team seasons use the 'NTM' combined row (reuses season_authority helpers).

    python -m scripts.sota_recon.recon_player_season
      -> derived/validation/expectations/player_season_discrepancies.parquet
"""

from __future__ import annotations

import os

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .sources import latest_v26, SEASON_AUTH_PASS, SEASON_AUTH_RUSHREC, SEASON_AUTH_RECRUSH
from .recon_season_authority import _load_season, _season_totals

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
OUT_DIR = "D:/league-history-data/nfl/derived/validation/expectations"
TOL_YDS = 3          # rounding / lateral / sack-yard attribution slack
TOL_TD = 0           # touchdowns should be exact


def _authority_player_season() -> pd.DataFrame:
    """Per (pfr_id, year): pass_yds, rush_yds, rec_yds, pass_td, rush_td, rec_td (REG)."""
    pa_ = _season_totals(_load_season(SEASON_AUTH_PASS.path, ["pass_yds", "pass_td"]),
                         ["pass_yds", "pass_td"])
    rr = _season_totals(_load_season(SEASON_AUTH_RUSHREC.path,
                                     ["rush_yds", "rush_td", "rec_yds", "rec_td"]),
                        ["rush_yds", "rush_td", "rec_yds", "rec_td"])
    ar = _season_totals(_load_season(SEASON_AUTH_RECRUSH.path,
                                     ["rush_yds", "rush_td", "rec_yds", "rec_td"]),
                        ["rush_yds", "rush_td", "rec_yds", "rec_td"])
    rushrec = pd.concat([rr, ar]).groupby(["pfr_id", "yr"], as_index=False).sum()
    a = pa_.merge(rushrec, on=["pfr_id", "yr"], how="outer").fillna(0)
    return a.rename(columns={"yr": "year"})


def _v26_player_season() -> pd.DataFrame:
    """Per (pfr_id, year): v26 REG totals, mapping NFL_player_id->pfr_id via bio else self."""
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'")
    V = f"read_parquet('{latest_v26()}')"; B = f"read_parquet('{BIO}')"
    df = con.execute(f"""
        WITH v AS (
          SELECT COALESCE(b.pfr_id, v.NFL_player_id) AS pfr_id, v.year,
                 COALESCE(passing_yards,0) py, COALESCE(passing_tds,0) pt,
                 COALESCE(rushing_yards,0) ry, COALESCE(rushing_tds,0) rt,
                 COALESCE(receiving_yards,0) cy, COALESCE(receiving_tds,0) ct
          FROM {V} v LEFT JOIN {B} b ON b.NFL_player_id = v.NFL_player_id
          WHERE v.season_type = 'REG' AND v.NFL_player_id IS NOT NULL)
        SELECT pfr_id, year, SUM(py) pass_yds, SUM(pt) pass_td, SUM(ry) rush_yds,
               SUM(rt) rush_td, SUM(cy) rec_yds, SUM(ct) rec_td
        FROM v GROUP BY pfr_id, year
    """).df()
    con.close()
    return df


def run() -> dict:
    auth = _authority_player_season()
    v = _v26_player_season()
    m = auth.merge(v, on=["pfr_id", "year"], how="inner", suffixes=("_a", "_v"))
    disc = []
    pairs = [("pass_yds", TOL_YDS), ("rush_yds", TOL_YDS), ("rec_yds", TOL_YDS),
             ("pass_td", TOL_TD), ("rush_td", TOL_TD), ("rec_td", TOL_TD)]
    for col, tol in pairs:
        d = (m[f"{col}_a"] - m[f"{col}_v"]).abs()
        bad = m[d > tol]
        for r in bad.itertuples(index=False):
            disc.append((r.pfr_id, int(r.year), col,
                         float(getattr(r, f"{col}_a")), float(getattr(r, f"{col}_v"))))
    dd = pd.DataFrame(disc, columns=["pfr_id", "year", "stat", "record_book", "v26"])
    os.makedirs(OUT_DIR, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dd),
                   os.path.join(OUT_DIR, "player_season_discrepancies.parquet"))

    # summary: % of player-seasons clean on all six stats, by era
    m2 = m.copy()
    m2["ok"] = True
    for col, tol in pairs:
        m2["ok"] &= (m2[f"{col}_a"] - m2[f"{col}_v"]).abs() <= tol
    by_era = []
    for lo, hi in [(1932, 1949), (1950, 1977), (1978, 2009), (2010, 2025)]:
        e = m2[(m2.year >= lo) & (m2.year <= hi)]
        if len(e):
            by_era.append((f"{lo}-{hi}", len(e), round(100 * e.ok.mean(), 2)))
    return {"player_seasons_compared": int(len(m)),
            "discrepant_cells": int(len(dd)),
            "clean_player_seasons_pct": round(100 * m2.ok.mean(), 2),
            "by_era": by_era,
            "by_stat": dd.stat.value_counts().to_dict()}


if __name__ == "__main__":
    r = run()
    print(f"player-seasons compared: {r['player_seasons_compared']:,}")
    print(f"clean on all 6 stats: {r['clean_player_seasons_pct']}%")
    print(f"discrepant cells: {r['discrepant_cells']:,}  by stat: {r['by_stat']}")
    print("\n  era         player-seasons   clean%")
    for era, n, pct in r["by_era"]:
        print(f"  {era:<11} {n:>14,}   {pct}%")
