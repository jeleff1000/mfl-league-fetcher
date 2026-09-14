#!/usr/bin/env python3
"""Shared utils for the WR/receiver route-depth vs fantasy trend study.

Question: are deep threats getting worse for fantasy, year by year? Same-season
(current-year) relationship between route depth and fantasy scoring, tracked
across 2006-2024 (the air-yards era).

DATA-QUALITY NOTES (verified 2026-07-08):
  * the precomputed `adot` column is BROKEN (bad weekly average) -> we compute
    ADOT = receiving_air_yards / targets at the season grain.
  * air-yards exist 2006+; `air_yards_share` only stabilizes ~2009+.
  * receiving NGS (separation/cushion/yac) is 2016+.
  * 2025 is a partial season -> excluded from trend fits.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "fantasy_football_data_scripts"))
_ENV = _REPO / ".env"
if _ENV.exists():
    for _l in _ENV.read_text().splitlines():
        _l = _l.strip()
        if _l and not _l.startswith("#") and "=" in _l:
            _k, _v = _l.split("=", 1)
            os.environ.setdefault(_k, _v.strip())

SCRATCH = Path(
    r"C:/Users/joeye/AppData/Local/Temp/claude/d--yahoo-oauth/"
    r"03fb2042-2335-4f17-87ec-4d7c4002d157/scratchpad"
)
WR = SCRATCH / "wr_seasons.parquet"
QB = SCRATCH / "qb_seasons.parquet"

# raw receiver columns to pull (compute depth metrics ourselves)
WR_COLS = [
    "NFL_player_id", "year", "player", "position", "games_played",
    "targets", "receptions", "receiving_yards", "receiving_air_yards",
    "receiving_yards_after_catch", "receiving_tds", "receiving_first_downs",
    "receiving_long", "air_yards_share", "target_share", "wopr", "racr",
    "catch_rate", "yards_per_target", "yards_per_reception",
    "receptions_0_4", "receptions_5_9", "receptions_10_19",
    "receptions_20_29", "receptions_30_39", "receptions_40plus",
    "rec_explosive_20", "receiving_epa",
    "ngs_avg_separation", "ngs_avg_cushion", "ngs_avg_yac",
    "ngs_avg_expected_yac", "ngs_avg_yac_above_expectation",
    "ngs_pct_share_intended_air_yards",
    "ppg_season_4pt_0ppr", "ppg_season_4pt_half", "ppg_season_4pt_ppr",
    "fpts_4pt_0ppr", "fpts_4pt_half", "fpts_4pt_ppr",
    "rank_season_wr_0ppr", "rank_season_wr_half", "rank_season_wr_ppr",
]
QB_COLS = [
    "NFL_player_id", "year", "player", "position", "games_played",
    "attempts", "completions", "passing_yards", "passing_air_yards",
    "passing_yards_after_catch", "passing_tds", "passing_interceptions",
    "passing_cpoe", "passing_epa", "passer_rating", "comp_pct",
    "ppg_season_4pt_0ppr", "ppg_season_4pt_half",
    "ppg_season_6pt_0ppr", "fpts_4pt_half", "fpts_6pt_0ppr",
]


def add_depth_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the honest depth metrics from raw components."""
    df = df.copy()
    t = df["targets"].replace(0, np.nan)
    df["adot"] = df["receiving_air_yards"] / t                    # route depth
    df["ay_per_game"] = df["receiving_air_yards"] / df["games_played"].replace(0, np.nan)
    df["yac_share"] = (df["receiving_yards_after_catch"]
                       / df["receiving_yards"].replace(0, np.nan))
    # deep-catch rate: share of receptions that went 20+ / 40+ yards
    rec = df["receptions"].replace(0, np.nan)
    df["deep_catch_rate"] = df["receptions_40plus"] / rec
    df["explosive_catch_rate"] = df["rec_explosive_20"] / rec
    return df


def spearman(a, b) -> float:
    a, b = np.asarray(a, float), np.asarray(b, float)
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 15 or a[m].std() < 1e-9 or b[m].std() < 1e-9:
        return np.nan
    ar = pd.Series(a[m]).rank().to_numpy()
    br = pd.Series(b[m]).rank().to_numpy()
    return float(np.corrcoef(ar, br)[0, 1])


def load_wr() -> pd.DataFrame:
    return add_depth_metrics(pd.read_parquet(WR))
